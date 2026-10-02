"""
REAL-MONEY copy trading.

Mirrors followed wallets with real SOL from a dedicated trading wallet whose key
lives only on this computer (data/trading_wallet.json). Every limit here is a hard
cap; the wallet's own balance is the final one - fund it only with what you can lose.

Flow per trade:
  PumpPortal Local API builds the transaction -> signed here -> sent through your
  Helius RPC -> confirmed -> the real SOL/token amounts are read back from the chain.
Buys: fixed USD size. Sells: the same % of our tokens as the followed wallet sold.
After a full exit the empty token account is closed to reclaim its ~0.002 SOL rent.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time

import aiohttp
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

from memebot.chain import JUP_PRICE, curve_address, parse_curve, spot_price, trade_from_tx
from memebot.names import is_placeholder

log = logging.getLogger("memebot")
TRADE_LOCAL = os.environ.get("MOMENTUM_PUMPPORTAL_TRADE", "https://pumpportal.fun/api/trade-local")
RPC = os.environ.get("MOMENTUM_HELIUS_RPC", "https://mainnet.helius-rpc.com/?api-key={key}")
PUBLIC_RPC = os.environ.get("MOMENTUM_PUBLIC_RPC", "https://api.mainnet-beta.solana.com")
TOKEN_PROGRAMS = ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"]
for _p in TOKEN_PROGRAMS:          # fail loudly at startup, not with a cryptic RPC error later
    Pubkey.from_string(_p)
LAMPORTS = 1_000_000_000
WSOL_MINT = "So11111111111111111111111111111111111111112"
JUP_QUOTE = os.environ.get("MOMENTUM_JUP_QUOTE", "https://lite-api.jup.ag/swap/v1/quote")
JUP_SWAP = os.environ.get("MOMENTUM_JUP_SWAP", "https://lite-api.jup.ag/swap/v1/swap")
RESERVE_SOL = 0.01          # always keep this much for fees / rent


STRATEGY_TAGS = {"lookalike": "Lookalike", "reclaim": "Reclaim", "lookalike_grad": "Lookalike·grad", "survivor": "Survivor", "calls": "Discord calls"}      # real positions opened by our own strategies (not copies)


def _short(w):
    if w in STRATEGY_TAGS:
        return STRATEGY_TAGS[w]
    return "untracked" if w == "unknown" else w[:4] + "…" + w[-4:]


class LiveTrader:
    def __init__(self, data_dir, cfg_getter, helius_key_getter, sol_usd_getter):
        self.dir = data_dir
        os.makedirs(self.dir, exist_ok=True)
        self.key_path = os.path.join(self.dir, "trading_wallet.json")
        self.state_path = os.path.join(self.dir, "live_state.json")
        self._cfg = cfg_getter
        self._hkey = helius_key_getter
        self._usd = sol_usd_getter
        self.kp: Keypair | None = self._load_key()
        self.positions: dict = {}          # mint -> position dict
        self.closed: list = []
        self.paused = False
        self.events: list = []             # recent log lines for the dashboard
        self.balance: float | None = None
        self.balance_ts = 0.0
        self.locks: dict[str, asyncio.Lock] = {}
        self.session: aiohttp.ClientSession | None = None
        self.price_of = lambda mint: None  # set by the app: current SOL price per token (live feed)
        self.prices: dict = {}             # mint -> (SOL per token, ts): our own on-chain price reads
        self.active = False                # true only while a live session is running
        self.late_waits = (45, 120)        # re-check a timed-out buy after these many seconds
        self.dip_poll_s = 1.5              # waiting for a dip: check the price this often (live feed)...
        self.dip_rpc_every_s = 12          # ...and read it on-chain this often (Helius credits)
        self.external_seen: set = set()    # wallet transactions already checked for outside sells
        self.ignored: set = set()          # tokens that showed up without us buying them (airdrops / spam)
        self.fill_cache: dict = {}         # our own tx signature -> parsed trade (or None)
        self.gone_confirm_s = 25           # a token must read as gone twice, this far apart, before we close it
        self._load_state()
        self._repair_history()

    # ------------------------------------------------------------------ wallet & state
    def _load_key(self):
        try:
            with open(self.key_path, encoding="utf-8", errors="replace") as fh:
                return Keypair.from_base58_string(json.load(fh)["secret"])
        except (OSError, KeyError, ValueError, json.JSONDecodeError):
            return None

    def create_wallet(self):
        if self.kp is not None:
            raise ValueError("A trading wallet already exists")
        kp = Keypair()
        with open(self.key_path, "w", encoding="utf-8", errors="replace") as fh:
            json.dump({"secret": str(kp), "public": str(kp.pubkey()), "created": time.time(),
                       "note": "Momentum trading wallet. Anyone with this file can spend the funds. Never share it."}, fh)
        self.kp = kp
        self._event("info", f"Created trading wallet {kp.pubkey()}")
        return str(kp.pubkey())

    @property
    def address(self):
        return str(self.kp.pubkey()) if self.kp else None

    def _load_state(self):
        try:
            with open(self.state_path, encoding="utf-8", errors="replace") as fh:
                s = json.load(fh)
            self.positions = s.get("positions", {})
            self.closed = s.get("closed", [])
            self.paused = s.get("paused", False)
            self.ignored = set(s.get("ignored", []))
            for m in [m for m, p in self.positions.items() if p["status"] == "waiting"]:
                self.positions.pop(m)                    # was only waiting for a dip - nothing was bought
            for p in self.positions.values():            # anything mid-flight when we stopped
                if p["status"] in ("buying", "selling"):
                    p["status"] = "check"
        except (OSError, json.JSONDecodeError):
            pass

    def _save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8", errors="replace") as fh:
            json.dump({"positions": self.positions, "closed": self.closed[-500:], "paused": self.paused,
                       "ignored": sorted(self.ignored)[-2000:]}, fh, indent=1)
        os.replace(tmp, self.state_path)

    CSV_HEAD = "closed_utc,symbol,mint,wallet,sol_in,sol_out,pnl_sol,pnl_pct,pnl_usd,signatures\n"

    def _csv_path(self):
        return os.path.join(self.dir, "live_trades.csv")

    def _rewrite_csv(self, fix):
        """fix(list of row-lists) -> new list. Keeps live_trades.csv in step with a history repair."""
        try:
            with open(self._csv_path(), encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError:
            return
        if not lines:
            return
        rows = fix([ln.split(",") for ln in lines[1:] if ln.strip()])
        tmp = self._csv_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8", errors="replace") as fh:
            fh.write(self.CSV_HEAD)
            for r in rows:
                fh.write(",".join(str(x) for x in r) + "\n")
        os.replace(tmp, self._csv_path())

    def _repair_history(self):
        """One sell split into two records (a "-100%" close, then the same coin sold as 'untracked'):
        merge them back into one trade."""
        merged = []
        for i, c in enumerate(self.closed):
            if c.get("wallet") != "unknown" or c.get("sol_in"):
                continue
            for e in reversed(self.closed[:i]):
                if e["mint"] == c["mint"] and e.get("wallet") != "unknown" and e.get("sol_out", 0) <= e.get("sol_in", 0) * 0.02:
                    e["sol_out"] = round(e.get("sol_out", 0) + c["sol_out"], 6)
                    e["pnl_sol"] = round(e["sol_out"] - e["sol_in"], 6)
                    e["pnl_pct"] = round(e["pnl_sol"] / e["sol_in"] * 100, 1) if e["sol_in"] else 0.0
                    e["pnl_usd"] = round(e.get("pnl_usd", 0) + c.get("pnl_usd", 0), 2)
                    e["sigs"] = list(e.get("sigs", [])) + list(c.get("sigs", []))
                    e["closed"] = c["closed"]
                    merged.append((c, e))
                    break
        if not merged:
            return
        drop = {id(c) for c, _ in merged}
        self.closed = [c for c in self.closed if id(c) not in drop]

        def fix(rows):
            for c, e in merged:
                un = next((r for r in rows if len(r) >= 10 and r[2] == c["mint"] and r[3] == "unknown"), None)
                orig = next((r for r in reversed(rows) if len(r) >= 10 and r[2] == e["mint"] and r[3] != "unknown"), None)
                if un is not None:
                    rows.remove(un)
                if orig is not None:
                    orig[5], orig[6], orig[7], orig[8] = e["sol_out"], e["pnl_sol"], e["pnl_pct"], e["pnl_usd"]
                    orig[9] = " ".join(e["sigs"])
            return rows
        self._rewrite_csv(fix)
        self._save()
        log.info("LIVE merged %d split trade record(s) in the history", len(merged))

    def _event(self, kind, text, **extra):
        self.events.append(dict(ts=time.time(), kind=kind, text=text, **extra))
        self.events = self.events[-150:]
        (log.warning if kind == "error" else log.info)("LIVE %s", text)
        n = getattr(self, "notifier", None)
        if n is not None and (kind in ("buy", "sell", "close") or (kind == "error" and "couldn't sell" in text)):
            usd = self._usd() or 0
            import re as _re
            msg = text if "$" in text else _re.sub(r"(-?\d+\.\d{4}) SOL", lambda m: f"{m.group(1)} SOL (${float(m.group(1)) * usd:,.2f})" if usd else m.group(0), text)
            tags = {"buy": "shopping_cart", "sell": "moneybag", "error": "warning"}.get(kind) or (
                "chart_with_upwards_trend" if "+" in text.split("(")[-1][:3] or ": +" in text else "chart_with_downwards_trend")
            n.send(f"{getattr(self, 'notify_label', 'Real wallet')}: {kind.upper()}", msg, tags, 4 if kind == "error" else 3)

    # ------------------------------------------------------------------ config helpers
    def cfg(self):
        return self._cfg().get("live") or {}

    def enabled(self):
        return bool(self.cfg().get("enabled")) and self.kp is not None and bool(self._hkey())

    def _sol(self, usd):
        px = self._usd()
        return usd / px if px else None

    def per_wallet(self, wallet):
        op = [p for p in self.positions.values() if p["wallet"] == wallet]
        cl = [c for c in self.closed if c["wallet"] == wallet]
        deployed = sum(p.get("sol_in", 0) or p.get("size", 0) for p in op)
        realized = sum(c["pnl_sol"] for c in cl)
        return op, cl, deployed, realized

    # ------------------------------------------------------------------ rpc
    async def _session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def rpc(self, method, params):
        """Helius RPC with retries. When Helius is busy or out of credits it answers with a non-JSON page
        (HTTP 429 / 401 / 5xx) - retry a few times, then say plainly which it was."""
        s = await self._session()
        last = "Helius not answering"
        for attempt in range(3):
            try:
                async with s.post(RPC.format(key=self._hkey()), json={"jsonrpc": "2.0", "id": 1, "method": method,
                                                                      "params": params},
                                  timeout=aiohttp.ClientTimeout(total=20)) as r:
                    status = r.status
                    body = await r.read()
                try:
                    j = json.loads(body)
                except ValueError:
                    j = None
                if isinstance(j, dict):
                    if "error" in j:
                        raise RuntimeError(str(j["error"].get("message") if isinstance(j["error"], dict) else j["error"])[:160])
                    return j.get("result")
                txt = body[:80].decode(errors="ignore").strip()
                last = (f"Helius rate limit (HTTP 429) - too many requests" if status == 429 else
                        f"Helius refused the key (HTTP {status}) - out of credits or key invalid? Check your Helius dashboard"
                        if status in (401, 402, 403) else f"Helius busy (HTTP {status}{': ' + txt if txt else ''})")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = f"Helius connection problem ({type(e).__name__})"
            await asyncio.sleep(0.7 * (attempt + 1))
        raise RuntimeError(last)

    async def refresh_balance(self, force=False):
        if not self.kp or not self._hkey():
            return None
        if force or time.time() - self.balance_ts > 20:
            try:
                r = await self.rpc("getBalance", [self.address, {"commitment": "confirmed"}])
                self.balance = (r["value"] if isinstance(r, dict) else r) / LAMPORTS
                self.balance_ts = time.time()
            except Exception as e:
                self._event("error", f"Balance check failed: {e}")
        return self.balance

    async def _send(self, tx: VersionedTransaction):
        b64 = base64.b64encode(bytes(tx)).decode()
        return await self.rpc("sendTransaction", [b64, {"encoding": "base64", "skipPreflight": False,
                                                        "preflightCommitment": "confirmed", "maxRetries": 0}])

    async def _rebroadcast(self, b64):
        """Re-send an already-signed tx (no simulation) through Helius and the public RPC."""
        s = await self._session()
        body = {"jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                "params": [b64, {"encoding": "base64", "skipPreflight": True, "maxRetries": 0}]}
        for url in (RPC.format(key=self._hkey()), PUBLIC_RPC):
            try:
                async with s.post(url, json=body, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    await r.read()
            except Exception:
                pass

    async def _send_and_confirm(self, tx: VersionedTransaction, timeout=None, resend_for=None):
        """Send, then keep re-broadcasting every 2s until it confirms, fails, or its blockhash expires.
        Returns (ok, why, sig): ok is True / False / None (unknown after timeout)."""
        timeout = timeout or self.cfg().get("confirm_timeout_s", 75)
        resend_for = resend_for or min(60, timeout)
        sig = await self._send(tx)                      # first send simulates: bad trades fail here, free
        b64 = base64.b64encode(bytes(tx)).decode()
        t0, last = time.time(), 0.0
        while time.time() - t0 < timeout:
            try:
                r = await self.rpc("getSignatureStatuses", [[sig], {"searchTransactionHistory": False}])
                st = (r or {}).get("value", [None])[0]
                if st:
                    if st.get("err"):
                        return False, f"failed on-chain: {st['err']}", sig
                    if st.get("confirmationStatus") in ("confirmed", "finalized"):
                        return True, "", sig
            except Exception:
                pass
            if time.time() - last >= 2 and time.time() - t0 < resend_for:
                last = time.time()
                await self._rebroadcast(b64)
            await asyncio.sleep(1.0)
        return None, f"not confirmed after {timeout}s (network busy - try a higher priority fee)", sig

    async def _confirm(self, sig, timeout=45):
        t0 = time.time()
        while time.time() - t0 < timeout:
            r = await self.rpc("getSignatureStatuses", [[sig], {"searchTransactionHistory": False}])
            st = (r or {}).get("value", [None])[0]
            if st:
                if st.get("err"):
                    return False, f"failed on-chain: {st['err']}"
                if st.get("confirmationStatus") in ("confirmed", "finalized"):
                    return True, ""
            await asyncio.sleep(1.0)
        return None, "not confirmed in time"

    async def _fill(self, sig):
        if sig in self.fill_cache:
            return self.fill_cache[sig]
        for attempt in range(6):
            tx = await self.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                                                         "maxSupportedTransactionVersion": 0}])
            if tx:
                self.fill_cache[sig] = f = trade_from_tx(tx, self.address, sig)
                if len(self.fill_cache) > 3000:
                    self.fill_cache.pop(next(iter(self.fill_cache)))
                return f
            await asyncio.sleep(1.5)
        return None

    async def _owner_accounts(self, flt):
        """All of our token accounts matching `flt` ({"mint": ..} or {"programId": ..}).
        Helius' paginated V2 method first (the old one fails with "index service overloaded"
        under load), the classic method as a fallback, with short retries."""
        last = None
        for attempt in range(3):
            try:
                out, key = [], None
                for _ in range(20):
                    cfg = {"encoding": "jsonParsed", "commitment": "confirmed", "limit": 1000}
                    if key:
                        cfg["paginationKey"] = key
                    r = await self.rpc("getTokenAccountsByOwnerV2", [self.address, flt, cfg]) or {}
                    v = r.get("value")
                    if isinstance(v, dict):                       # withContext-style shape
                        key, page = v.get("paginationKey"), v.get("accounts") or []
                    else:
                        key, page = r.get("paginationKey"), v or []
                    out += page
                    if not page or not key:
                        return out
                return out
            except Exception as e:
                last = e
            try:
                r = await self.rpc("getTokenAccountsByOwner", [self.address, flt, {"encoding": "jsonParsed",
                                                                                "commitment": "confirmed"}])
                return (r or {}).get("value", [])
            except Exception as e:
                last = e
            await asyncio.sleep(1.5 * (attempt + 1))
        raise last

    async def _token_accounts(self, mint):
        out = []
        for a in await self._owner_accounts({"mint": mint}):
            info = a["account"]["data"]["parsed"]["info"]
            out.append(dict(address=a["pubkey"], program=a["account"]["owner"], lamports=a["account"].get("lamports", 0),
                            amount=int(info["tokenAmount"]["amount"]),
                            ui=float(info["tokenAmount"].get("uiAmountString") or 0)))
        return out

    async def _pumpportal_only(self, action, mint, amount, denom_sol, slippage):
        s = await self._session()
        body = {"publicKey": self.address, "action": action, "mint": mint, "amount": amount,
                "denominatedInSol": "true" if denom_sol else "false", "slippage": slippage,
                "priorityFee": self.cfg().get("priority_fee_sol", 0.0003), "pool": "auto"}
        async with s.post(TRADE_LOCAL, data=body, timeout=aiohttp.ClientTimeout(total=8)) as r:
            raw = await r.read()
            if r.status != 200:
                raise RuntimeError(f"PumpPortal HTTP {r.status}: {raw[:120].decode(errors='ignore')}")
        tx = VersionedTransaction.from_bytes(raw)
        return VersionedTransaction(tx.message, [self.kp])

    async def _jupiter(self, action, mint, amount, denom_sol, slippage):
        """Backup route: Jupiter's swap API builds the transaction (it routes pump.fun curves and pools too)."""
        s = await self._session()
        if action == "buy":
            in_mint, out_mint = WSOL_MINT, mint
            raw_amount = int(float(amount) * LAMPORTS)
        else:
            in_mint, out_mint = mint, WSOL_MINT
            accts = await self._token_accounts(mint)
            have = sum(a["amount"] for a in accts)
            pct = float(str(amount).rstrip("%")) / 100 if isinstance(amount, str) and amount.endswith("%") else None
            raw_amount = int(have * pct) if pct is not None else int(float(amount))
            if raw_amount <= 0:
                raise RuntimeError("nothing to sell")
        q = {"inputMint": in_mint, "outputMint": out_mint, "amount": str(raw_amount),
             "slippageBps": str(int(float(slippage) * 100)), "restrictIntermediateTokens": "true"}
        async with s.get(JUP_QUOTE, params=q, timeout=aiohttp.ClientTimeout(total=8)) as r:
            quote = await r.json(content_type=None)
            if r.status != 200 or not isinstance(quote, dict) or "outAmount" not in quote:
                raise RuntimeError(f"Jupiter quote: {str(quote)[:100]}")
        prio = int(float(self.cfg().get("priority_fee_sol", 0.0003)) * LAMPORTS)
        body = {"quoteResponse": quote, "userPublicKey": self.address, "wrapAndUnwrapSol": True,
                "dynamicComputeUnitLimit": True, "prioritizationFeeLamports": prio}
        async with s.post(JUP_SWAP, json=body, timeout=aiohttp.ClientTimeout(total=8)) as r:
            j = await r.json(content_type=None)
            if r.status != 200 or not isinstance(j, dict) or not j.get("swapTransaction"):
                raise RuntimeError(f"Jupiter swap: {str(j)[:100]}")
        tx = VersionedTransaction.from_bytes(base64.b64decode(j["swapTransaction"]))
        return VersionedTransaction(tx.message, [self.kp])

    async def _pumpportal(self, action, mint, amount, denom_sol, slippage):
        """Build a buy/sell: PumpPortal first (one retry), Jupiter if PumpPortal is down or refuses."""
        errs = []
        for attempt in range(2):
            try:
                return await self._pumpportal_only(action, mint, amount, denom_sol, slippage)
            except Exception as e:
                errs.append(f"PumpPortal {type(e).__name__}{': ' + str(e)[:80] if str(e) else ''}")
                if "HTTP 400" in str(e):
                    break                                 # PumpPortal can't route this coin: go straight to Jupiter
        try:
            tx = await self._jupiter(action, mint, amount, denom_sol, slippage)
            self._event("info", f"{action} of {mint[:5]} built by Jupiter (PumpPortal: {errs[-1][:60]})")
            return tx
        except Exception as e:
            errs.append(f"Jupiter {type(e).__name__}{': ' + str(e)[:80] if str(e) else ''}")
        raise RuntimeError(" / ".join(errs))

    def _lock(self, mint):
        return self.locks.setdefault(mint, asyncio.Lock())

    # ------------------------------------------------------------------ copy hooks (called by the engine)
    def on_copy_buy(self, wallet, mint, wallet_sol, symbol="", wallet_px=None):
        if not (self.active and self.enabled()) or self.paused:
            return
        c = self.cfg()
        if mint in self.positions:
            return                                        # one live position per token
        size = self._sol(c.get("trade_usd", 2.5))
        budget = self._sol(c.get("budget_usd_per_wallet", 10))
        if not size or not budget:
            return self._event("skip", f"{symbol or mint[:5]}: no SOL price yet, can't size a USD trade")
        op, cl, deployed, realized = self.per_wallet(wallet)
        if realized <= -budget:
            return self._event("skip", f"{_short(wallet)} hit its loss limit (${c.get('budget_usd_per_wallet', 10)}); not copying")
        if len(op) >= c.get("max_open_per_wallet", 4):
            return self._event("skip", f"{symbol or mint[:5]}: {_short(wallet)} already has {len(op)} live positions")
        if deployed + size > budget * 1.0001:
            return self._event("skip", f"{symbol or mint[:5]}: {_short(wallet)} budget in use ({deployed:.3f}/{budget:.3f} SOL)")
        if self.balance is not None and self.balance < size + RESERVE_SOL:
            return self._event("skip", f"{symbol or mint[:5]}: trading wallet balance too low ({self.balance:.4f} SOL)")
        self.positions[mint] = dict(mint=mint, symbol=symbol or mint[:5], wallet=wallet, status="buying", size=size,
                                    sol_in=0.0, sol_out=0.0, tokens=0.0, tokens_bought=0.0, opened=time.time(),
                                    queued_sell=0.0, sigs=[], wallet_sol=wallet_sol, wallet_px=wallet_px)
        self.ignored.discard(mint)
        self._save()
        asyncio.get_running_loop().create_task(self._buy(mint))

    # ------------------------------------------------------------------ our own strategies (lookalike)
    def realized_today_usd(self, tag):
        day = time.strftime("%Y-%m-%d")
        return sum(c.get("pnl_usd") or 0 for c in self.closed
                   if c.get("wallet") == tag and time.strftime("%Y-%m-%d", time.localtime(c["closed"])) == day)

    def open_strategy(self, tag, mint, symbol, usd, max_open, daily_loss_usd):
        """Real buy for a strategy signal. Returns None if placed, else the reason it was skipped."""
        if not (self.active and self.kp is not None and self._hkey()):
            return "real money not ready (no wallet / Helius key / not running)"
        if self.paused:
            return "real buys are paused"
        if mint in self.positions:
            return "already holding this coin"
        size = self._sol(usd)
        if not size:
            return "no SOL price yet"
        op = [p for p in self.positions.values() if p["wallet"] == tag]
        if len(op) >= max_open:
            return f"{len(op)} real {STRATEGY_TAGS.get(tag, tag)} positions open (max {max_open})"
        lost = self.realized_today_usd(tag)
        if daily_loss_usd and lost <= -daily_loss_usd:
            return f"daily loss limit hit (${lost:.2f} today, limit ${daily_loss_usd:g})"
        if self.balance is not None and self.balance < size + RESERVE_SOL:
            return f"trading wallet balance too low ({self.balance:.4f} SOL)"
        self.positions[mint] = dict(mint=mint, symbol=symbol or mint[:5], wallet=tag, status="buying", size=size,
                                    sol_in=0.0, sol_out=0.0, tokens=0.0, tokens_bought=0.0, opened=time.time(),
                                    queued_sell=0.0, sigs=[], wallet_sol=0.0, wallet_px=None)
        self.ignored.discard(mint)
        self._save()
        asyncio.get_running_loop().create_task(self._buy(mint))
        return None

    def add_strategy(self, tag, mint, usd, daily_loss_usd):
        """Buy MORE of a coin a strategy already holds for real (e.g. a manual re-buy of a call). None if placed."""
        if not (self.active and self.kp is not None and self._hkey()):
            return "real money not ready (no wallet / Helius key / not running)"
        if self.paused:
            return "real buys are paused"
        p = self.positions.get(mint)
        if not p or p["wallet"] != tag or p["status"] != "open":
            return "no open position to add to"
        size = self._sol(usd)
        if not size:
            return "no SOL price yet"
        lost = self.realized_today_usd(tag)
        if daily_loss_usd and lost <= -daily_loss_usd:
            return f"daily loss limit hit (${lost:.2f} today)"
        if self.balance is not None and self.balance < size + RESERVE_SOL:
            return f"trading wallet balance too low ({self.balance:.4f} SOL)"
        asyncio.get_running_loop().create_task(self._add_buy(mint, size))
        return None

    async def _add_buy(self, mint, size):
        sig = None
        async with self._lock(mint):
            p = self.positions.get(mint)
            if not p or p["status"] != "open":
                return
            try:
                tx = await self._pumpportal("buy", mint, round(size, 6), True, self.cfg().get("buy_slippage_pct", 20))
                ok, why, sig = await self._send_and_confirm(tx)
                p["sigs"].append(sig)
                if not ok:
                    raise RuntimeError(why)
                fill = await self._fill(sig)
                if fill and fill["txType"] == "buy":
                    sol, tok = fill["solAmount"], fill["tokenAmount"]
                else:
                    before = p["tokens"]
                    tok = max(0.0, sum(a["ui"] for a in await self._token_accounts(mint)) - before)
                    sol = size
                p["sol_in"] += sol
                p["tokens"] += tok
                p["tokens_bought"] += tok
                self._event("buy", f"Bought more {p['symbol']} for {sol:.4f} SOL (added to the position)", sig=sig, mint=mint)
            except Exception as e:
                self._event("error", f"Add-buy {p['symbol']} failed: {e or type(e).__name__}", mint=mint, sig=sig)
            self._save()
        await self.refresh_balance(force=True)

    def strategy_sell(self, tag, mint, frac, reason):
        """Mirror a strategy's sale (same share of what's left) on the real position."""
        p = self.positions.get(mint)
        if not p or p["wallet"] != tag:
            return
        if p["status"] in ("buying", "waiting"):
            p["queued_sell"] = 1 - (1 - p["queued_sell"]) * (1 - frac)
            self._save()
            return
        asyncio.get_running_loop().create_task(self._sell_until_done(mint, frac, reason))

    async def _sell_until_done(self, mint, frac, reason, tries=12):
        """A strategy's sale must happen: the strategy has already counted it, so a real sale that fails (e.g. the
        coin graduated that second - error 0x1775, or slippage) would leave the coin with no exit plan at all.
        Retry with growing pauses for ~15 min until the tokens actually leave the wallet."""
        for attempt in range(tries):
            p = self.positions.get(mint)
            if not p:
                return
            before = p["tokens"]
            await self._sell(mint, frac, reason if attempt == 0 else f"{reason} - retry {attempt}")
            p = self.positions.get(mint)
            if not p or p["tokens"] < before * 0.98:
                return
            await asyncio.sleep(min(15 * (attempt + 1), 120))
        self._event("error", f"{(self.positions.get(mint) or {}).get('symbol', mint[:5])}: still couldn't sell after "
                             f"{tries} tries - sell it by hand (Sell button)", mint=mint)

    def on_copy_sell(self, wallet, mint, frac):
        p = self.positions.get(mint)
        if not p or p["wallet"] != wallet or not (self.cfg().get("follow_sells", True)):
            return
        if p["status"] in ("buying", "waiting"):
            p["queued_sell"] = 1 - (1 - p["queued_sell"]) * (1 - frac)
            self._save()
            return
        asyncio.get_running_loop().create_task(self._sell(mint, frac, "copied sell"))

    def on_copy_sell_unknown(self, wallet, mint, tokens_sold):
        """Wallet sold a token we hold live, but we don't know its position size (e.g. after a restart):
        read what it has left on-chain and derive the share it sold."""
        p = self.positions.get(mint)
        if not p or p["wallet"] != wallet or not self.cfg().get("follow_sells", True):
            return

        async def go():
            try:
                r = await self.rpc("getTokenAccountsByOwner", [wallet, {"mint": mint}, {"encoding": "jsonParsed",
                                                                                     "commitment": "confirmed"}])
                left = sum(float(a["account"]["data"]["parsed"]["info"]["tokenAmount"].get("uiAmountString") or 0)
                           for a in (r or {}).get("value", []))
                frac = 1.0 if left <= 0 else tokens_sold / (tokens_sold + left)
                await self._sell(mint, min(1.0, frac), "copied sell")
            except Exception as e:
                self._event("error", f"Couldn't size copied sell of {p['symbol']}: {e}")
        asyncio.get_running_loop().create_task(go())

    # ------------------------------------------------------------------ execution
    async def _buy(self, mint):
        p = self.positions[mint]
        sig = None
        async with self._lock(mint):
            why_not = await self._pre_buy_check(p)
            if why_not:
                self.positions.pop(mint, None)
                self._save()
                return self._event("skip", f"{p['symbol']}: {why_not}", mint=mint)
            try:
                tx = await self._pumpportal("buy", mint, round(p["size"], 6), True, self.cfg().get("buy_slippage_pct", 20))
                ok, why, sig = await self._send_and_confirm(tx)
                p["sigs"].append(sig)
                if ok is None:                            # unknown: did the tokens arrive anyway?
                    ok = any(a["amount"] > 0 for a in await self._token_accounts(mint))
                if not ok:
                    raise RuntimeError(why)
                fill = await self._fill(sig)
                if fill and fill["txType"] == "buy":
                    p.update(status="open", sol_in=fill["solAmount"], tokens=fill["tokenAmount"],
                             tokens_bought=fill["tokenAmount"], entry_ts=time.time())
                    if p.get("wallet_px") and fill["tokenAmount"]:
                        p["entry_gap_pct"] = round((fill["solAmount"] / fill["tokenAmount"] / p["wallet_px"] - 1) * 100, 1)
                else:                                     # landed, fill not readable yet: use the balance
                    amt = sum(a["ui"] for a in await self._token_accounts(mint))
                    p.update(status="open", sol_in=p["size"], tokens=amt, tokens_bought=amt, entry_ts=time.time())
                self._event("buy", f"Bought {p['symbol']} for {p['sol_in']:.4f} SOL (copying {_short(p['wallet'])})",
                            sig=sig, mint=mint)
            except Exception as e:
                self.positions.pop(mint, None)
                self._save()
                self._event("error", f"Buy {p['symbol']} failed: {e or type(e).__name__}", mint=mint, sig=sig)
                if sig:                                   # in case it lands late, adopt it rather than lose track
                    asyncio.get_running_loop().create_task(self._late_check(mint, p, sig))
                return
            self._save()
        await self.refresh_balance(force=True)
        if p["queued_sell"] > 0:
            await self._sell(mint, p["queued_sell"], "copied sell (during our buy)")

    async def _current_px(self, mint, rpc=True):
        """SOL per token right now: on-chain curve/Jupiter read (if rpc), else the live feed."""
        px = None
        if rpc:
            try:
                px, _ = await spot_price(self.rpc, await self._session(), mint, self._usd())
            except Exception:
                px = None
        if not px:
            try:
                px = self.price_of(mint)
            except Exception:
                px = None
        return px

    async def _pre_buy_check(self, p):
        """Reason not to buy (the wallet already left, the price ran away and never came back...), or None.
        If the price is above the don't-chase limit, waits for a dip back to it (copy_trade.dip_wait_min)."""
        if p.get("queued_sell", 0) >= 0.99:
            return f"{_short(p['wallet'])} already sold out before our buy - skipped"
        cp = self._cfg().get("copy_trade") or {}
        lim = float(cp.get("max_entry_gap_pct") or 0)
        wpx = p.get("wallet_px")
        if not lim or not wpx:
            return None
        floor = float(cp.get("dip_floor_pct", 10) or 0)
        px = await self._current_px(p["mint"])
        if px and abs(px / wpx - 1) > 5:                  # >500% off: a bad price read, try the live feed
            feed = self.price_of(p["mint"])
            px = feed if feed and abs(feed / wpx - 1) <= 5 else None
            if not px:
                return "couldn't read a reliable price - skipped"
        if not px:
            self._event("info", f"{p['symbol']}: couldn't read the current price, buying without the chase check")
            return None
        gap = (px / wpx - 1) * 100
        p["quote_gap_pct"] = round(gap, 1)
        if floor and gap < -floor:
            return f"price already {gap:.0f}% below what {_short(p['wallet'])} paid (coin dropping) - skipped"
        if gap <= lim:
            return None
        wait = float(cp.get("dip_wait_min", 10) or 0)
        if not wait:
            return (f"price already +{gap:.0f}% above what {_short(p['wallet'])} paid "
                    f"(limit {lim:g}%) - not chasing")
        return await self._wait_for_dip(p, wpx, lim, floor, wait, gap)

    async def _wait_for_dip(self, p, wpx, lim, floor, wait_min, gap):
        mint, sym = p["mint"], p["symbol"]
        p["status"] = "waiting"
        self._save()
        self._event("info", f"{sym}: +{gap:.0f}% above what {_short(p['wallet'])} paid - waiting up to {wait_min:g} min "
                            f"for a dip back to +{lim:g}%", mint=mint)
        deadline, last_rpc = time.time() + wait_min * 60, time.time()
        while time.time() < deadline:
            await asyncio.sleep(self.dip_poll_s)
            if mint not in self.positions:
                return "cancelled"
            if p.get("queued_sell", 0) > 0:
                return f"{_short(p['wallet'])} started selling before the price dipped - cancelled"
            use_rpc = time.time() - last_rpc >= self.dip_rpc_every_s   # live feed every tick, chain every ~12s
            if use_rpc:
                last_rpc = time.time()
            px = await self._current_px(mint, rpc=use_rpc)
            if not px or abs(px / wpx - 1) > 5:
                continue
            gap = (px / wpx - 1) * 100
            p["quote_gap_pct"] = round(gap, 1)
            if floor and gap < -floor:
                return f"fell to {gap:.0f}% below what {_short(p['wallet'])} paid while waiting (coin dropping) - cancelled"
            if gap <= lim:
                p["status"] = "buying"
                self._event("info", f"{sym}: dipped back to {gap:+.0f}% - buying now", mint=mint)
                return None
        return f"never dipped back to +{lim:g}% within {wait_min:g} min - skipped"

    async def _late_check(self, mint, p, sig):
        for wait in self.late_waits:
            await asyncio.sleep(wait)
            if mint in self.positions:
                return
            try:
                amt = sum(a["ui"] for a in await self._token_accounts(mint))
            except Exception:
                continue
            if amt > 0:
                fill = await self._fill(sig)
                cost = fill["solAmount"] if fill and fill["txType"] == "buy" else p["size"]
                p.update(status="open", sol_in=cost, tokens=amt, tokens_bought=amt, entry_ts=time.time(), queued_sell=0.0)
                self.positions[mint] = p
                self._save()
                self._event("buy", f"{p['symbol']}: the earlier buy landed late - now tracking it ({cost:.4f} SOL)",
                            sig=sig, mint=mint)
                return

    async def _wallet_balances(self):
        """{mint: ui amount} for every token account the trading wallet holds (both token programs)."""
        out, errors = {}, []
        for prog in TOKEN_PROGRAMS:
            try:
                accts = await self._owner_accounts({"programId": prog})
            except Exception as e:
                errors.append(str(e))
                continue
            for a in accts:
                info = a["account"]["data"]["parsed"]["info"]
                out[info["mint"]] = out.get(info["mint"], 0.0) + float(info["tokenAmount"].get("uiAmountString") or 0)
        if len(errors) == len(TOKEN_PROGRAMS):
            raise RuntimeError(errors[0])
        if errors:
            raise RuntimeError("partial balance read: " + errors[0])   # never drop positions on half the data
        return out

    async def _external_sells(self, mint, since, known):
        """SOL received from sells of `mint` the bot didn't make (e.g. you sold in Phantom)."""
        sol, sigs = 0.0, []
        try:
            recent = await self.rpc("getSignaturesForAddress", [self.address, {"limit": 60, "commitment": "confirmed"}]) or []
            for r in recent:
                sig = r["signature"]
                if sig in known or sig in self.external_seen or r.get("err") or (r.get("blockTime") or 0) < since - 5:
                    continue
                fill = await self._fill(sig)
                self.external_seen.add(sig)
                if fill and fill["mint"] == mint and fill["txType"] == "sell":
                    sol += fill["solAmount"]
                    sigs.append(sig)
        except Exception as e:
            self._event("error", f"Couldn't look up the outside sell of {mint[:5]}: {e}")
        return sol, sigs

    async def sync_with_wallet(self):
        """Make the bot match the trading wallet: drop/shrink positions you sold yourself, adopt tokens it doesn't know."""
        if not self.kp or not self._hkey():
            return
        try:
            bal = await self._wallet_balances()
            self.sync_fails = 0
        except Exception as e:
            self.sync_fails = getattr(self, "sync_fails", 0) + 1       # positions are left untouched
            log.info("LIVE wallet sync skipped (%d in a row): %s", self.sync_fails, e)
            if self.sync_fails == 3:
                self._event("error", f"Wallet sync keeps failing (Helius busy?) - positions are unchanged, "
                                     f"will keep retrying: {str(e)[:90]}")
            return
        changed = False
        for mint, p in list(self.positions.items()):
            if p["status"] != "open" or (mint in self.locks and self.locks[mint].locked()):
                continue                                  # the bot is trading it right now
            have = bal.get(mint, 0.0)
            if p.get("orphan") and not p.get("sol_in"):
                continue                                  # handled with the untracked tokens below
            if have <= p["tokens_bought"] * 0.005 and have < p["tokens"] * 0.98:
                # looks fully gone. RPC reads can briefly miss an account, so confirm before closing
                first_seen = p.get("gone_since")
                if not first_seen:
                    p["gone_since"] = time.time()
                    changed = True
                    continue
                if time.time() - first_seen < self.gone_confirm_s:
                    continue
                try:
                    have = sum(a["ui"] for a in await self._token_accounts(mint))
                except Exception:
                    continue
                if have > p["tokens_bought"] * 0.005:
                    p.pop("gone_since", None)
                    p["tokens"] = have
                    changed = True
                    continue
            elif p.pop("gone_since", None):
                changed = True
            if have < p["tokens"] * 0.98:                 # tokens left the wallet outside the bot
                got, sigs = await self._external_sells(mint, p["opened"], set(p["sigs"]))
                p["sol_out"] += got
                p["sigs"] += sigs
                p["tokens"] = have
                changed = True
                if have <= p["tokens_bought"] * 0.005:
                    p["sol_out"] += await self._close_accounts(mint)
                    self._event("info", f"{p['symbol']} was sold outside the bot (received {got:.4f} SOL) - closing it here")
                    self._finish(mint, how="sold in wallet")
                else:
                    self._event("info", f"{p['symbol']}: part was sold outside the bot - now holding "
                                        f"{have / p['tokens_bought'] * 100:.0f}%")
            elif have > p["tokens"] * 1.02:               # more than we thought (e.g. you bought extra)
                p["tokens"] = have
                changed = True
        for mint, ui in bal.items():                      # tokens nobody is tracking
            if ui <= 0 or mint in self.ignored or (mint in self.locks and self.locks[mint].locked()):
                continue
            p = self.positions.get(mint)
            if p and not (p.get("orphan") and not p.get("sol_in")):
                continue
            if await self._adopt(mint, ui):
                changed = True
        if changed:
            self._save()
            await self.refresh_balance(force=True)

    async def _find_own_buy(self, mint, since=0.0):
        """Our own buy transaction of `mint` (SOL actually spent), or None."""
        try:
            recent = await self.rpc("getSignaturesForAddress", [self.address, {"limit": 100, "commitment": "confirmed"}]) or []
        except Exception:
            return None
        for r in recent:
            if r.get("err") or (r.get("blockTime") or 0) < since - 5:
                continue
            f = await self._fill(r["signature"])
            if f and f["mint"] == mint and f["txType"] == "buy":
                return f
        return None

    async def _adopt(self, mint, ui):
        """Tokens in the wallet the bot isn't tracking. Returns True if state changed."""
        # 1) a trade we closed too early (the balance read missed it): reopen that same record
        for i in range(len(self.closed) - 1, -1, -1):
            c = self.closed[i]
            if c["mint"] == mint and c.get("wallet") != "unknown" and time.time() - c["closed"] < 86400 \
                    and c.get("sol_out", 0) <= c.get("sol_in", 0) * 0.02:
                self.closed.pop(i)
                def drop_last(rows, mint=mint, wallet=c["wallet"]):
                    for j in range(len(rows) - 1, -1, -1):
                        if len(rows[j]) > 3 and rows[j][2] == mint and rows[j][3] == wallet:
                            return rows[:j] + rows[j + 1:]
                    return rows
                self._rewrite_csv(drop_last)
                self.positions[mint] = dict(mint=mint, symbol=c["symbol"], wallet=c["wallet"], status="open",
                                            size=c["sol_in"], sol_in=c["sol_in"], sol_out=c.get("sol_out", 0.0),
                                            tokens=ui, tokens_bought=max(ui, 1e-9), opened=c["opened"],
                                            queued_sell=0.0, sigs=list(c.get("sigs", [])), wallet_sol=0.0)
                self._event("info", f"{c['symbol']}: tokens are still in the wallet - reopened the trade (it was closed too early)")
                return True
        # 2) a buy of ours the bot lost track of: adopt it with its real cost
        f = await self._find_own_buy(mint)
        if f:
            self.positions[mint] = dict(mint=mint, symbol=mint[:5], wallet="unknown", status="open", size=f["solAmount"],
                                        sol_in=f["solAmount"], sol_out=0.0, tokens=ui, tokens_bought=ui, opened=time.time(),
                                        queued_sell=0.0, sigs=[f["signature"]], wallet_sol=0.0, orphan=True)
            self._event("info", f"Found untracked tokens ({mint[:5]}…) from one of our buys - added so you can sell them")
            return True
        # 3) arrived without us spending anything: airdrop / spam. Ignore it for good
        self.ignored.add(mint)
        if self.positions.get(mint, {}).get("orphan"):
            self.positions.pop(mint)
        self._event("info", f"Ignoring {mint[:5]}… - tokens that arrived without a buy (airdrop/spam)")
        return True

    async def sweep_orphans(self):
        await self.sync_with_wallet()

    async def sweep_loop(self):
        while True:
            await asyncio.sleep(30)
            if self.active:
                await self.sync_with_wallet()

    async def _sell(self, mint, frac, reason):
        async with self._lock(mint):
            await self._recheck_one(mint)                 # never sell on unverified state
            p = self.positions.get(mint)
            if not p or p["status"] != "open":
                return
            full = frac >= 0.99
            try:
                p["status"] = "selling"
                amount = "100%" if full else f"{max(frac * 100, 0.1):.2f}%"
                before = p["tokens"]
                for attempt in range(2):                  # one automatic retry if the network drops it
                    tx = await self._pumpportal("sell", mint, amount, False, self.cfg().get("sell_slippage_pct", 30))
                    ok, why, sig = await self._send_and_confirm(tx)
                    p["sigs"].append(sig)
                    if ok is None:                        # unknown: did our balance go down?
                        now_amt = sum(a["ui"] for a in await self._token_accounts(mint))
                        ok = now_amt < before * 0.98
                    if ok or ok is False and "failed on-chain" in why and attempt == 1:
                        break
                    if not ok:
                        self._event("error", f"Sell {p['symbol']} didn't land ({why}); retrying", mint=mint, sig=sig)
                if not ok:
                    raise RuntimeError(why)
                fill = await self._fill(sig)
                got = fill["solAmount"] if fill and fill["txType"] == "sell" else 0.0
                sold = fill["tokenAmount"] if fill and fill["txType"] == "sell" else p["tokens"] * frac
                p["sol_out"] += got
                p["tokens"] = max(0.0, p["tokens"] - sold)
                p["status"] = "open"
                self._event("sell", f"Sold {frac * 100:.0f}% of {p['symbol']} for {got:.4f} SOL ({reason})", sig=sig, mint=mint)
            except Exception as e:
                p["status"] = "open"
                self._save()
                return self._event("error", f"Sell {p['symbol']} ({reason}) failed: {e or type(e).__name__}", mint=mint)
            if full or p["tokens"] <= p["tokens_bought"] * 0.005:
                rent = await self._close_accounts(mint)
                p["sol_out"] += rent
                self._finish(mint)
            self._save()
        await self.refresh_balance(force=True)

    async def _close_accounts(self, mint):
        """Close empty token accounts for this mint; returns SOL reclaimed."""
        try:
            accts = [a for a in await self._token_accounts(mint) if a["amount"] == 0]
            if not accts:
                return 0.0
            owner = self.kp.pubkey()
            ixs = [Instruction(Pubkey.from_string(a["program"]), bytes([9]),
                               [AccountMeta(Pubkey.from_string(a["address"]), False, True),
                                AccountMeta(owner, False, True), AccountMeta(owner, True, False)]) for a in accts]
            bh = await self.rpc("getLatestBlockhash", [{"commitment": "confirmed"}])
            msg = MessageV0.try_compile(owner, ixs, [], Hash.from_string(bh["value"]["blockhash"]))
            sig = await self._send(VersionedTransaction(msg, [self.kp]))
            ok, _ = await self._confirm(sig, timeout=30)
            reclaimed = sum(a["lamports"] for a in accts) / LAMPORTS - 0.000005
            if ok:
                self._event("info", f"Reclaimed {reclaimed:.4f} SOL of token-account rent")
            return max(0.0, reclaimed) if ok else 0.0
        except Exception as e:
            self._event("error", f"Couldn't reclaim token-account rent: {e}")
            return 0.0

    def _finish(self, mint, how="bot"):
        p = self.positions.pop(mint)
        if any(c["mint"] == mint and abs(c["opened"] - p["opened"]) < 1 for c in self.closed[-200:]):
            self._save()                                  # already recorded (e.g. re-closed after a restart)
            return
        pnl = p["sol_out"] - p["sol_in"]
        px = self._usd() or 0
        rec = dict(mint=mint, symbol=p["symbol"], wallet=p["wallet"], opened=p["opened"], closed=time.time(),
                   sol_in=round(p["sol_in"], 6), sol_out=round(p["sol_out"], 6), pnl_sol=round(pnl, 6),
                   pnl_pct=round(pnl / p["sol_in"] * 100, 1) if p["sol_in"] else 0.0, pnl_usd=round(pnl * px, 2),
                   sigs=p["sigs"], how=how, entry_gap_pct=p.get("entry_gap_pct"))
        self.closed.append(rec)
        self._event("close", f"Closed {p['symbol']}: {pnl:+.4f} SOL ({rec['pnl_pct']:+.0f}%, ${rec['pnl_usd']:+.2f})",
                    mint=mint, pnl=pnl)
        try:
            with open(os.path.join(self.dir, "live_trades.csv"), "a", encoding="utf-8", errors="replace") as fh:
                if fh.tell() == 0:
                    fh.write("closed_utc,symbol,mint,wallet,sol_in,sol_out,pnl_sol,pnl_pct,pnl_usd,signatures\n")
                fh.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())},{rec['symbol']},{mint},{p['wallet']},"
                         f"{rec['sol_in']},{rec['sol_out']},{rec['pnl_sol']},{rec['pnl_pct']},{rec['pnl_usd']},"
                         f"{' '.join(p['sigs'])}\n")
        except OSError:
            pass
        self._save()                                      # write it now: a restart right after must not undo it

    # ------------------------------------------------------------------ pricing open positions
    def px(self, mint):
        """Current SOL price per token: our own recent on-chain read, else the live feed's."""
        own = self.prices.get(mint)
        if own and time.time() - own[1] < 45:
            return own[0]
        try:
            feed = self.price_of(mint)
        except Exception:
            feed = None
        return feed or (own[0] if own else None)

    async def refresh_prices(self):
        mints = [m for m, p in self.positions.items() if p["status"] not in ("buying", "waiting")]
        if not mints or not self._hkey():
            return
        grads = []
        for i in range(0, len(mints), 100):
            chunk = mints[i:i + 100]
            r = await self.rpc("getMultipleAccounts", [[curve_address(m) for m in chunk],
                                                       {"encoding": "base64", "commitment": "confirmed"}])
            for m, acc in zip(chunk, (r or {}).get("value") or []):
                px = parse_curve(base64.b64decode(acc["data"][0])) if acc else None
                if px:
                    self.prices[m] = (px, time.time())
                else:
                    grads.append(m)                      # graduated / not a pump.fun curve
        usd = self._usd()
        if grads and usd:
            s = await self._session()
            for i in range(0, len(grads), 50):
                chunk = grads[i:i + 50]
                try:
                    async with s.get(JUP_PRICE.format(mint=",".join(chunk)), timeout=aiohttp.ClientTimeout(total=8)) as rr:
                        j = await rr.json(content_type=None) if rr.status == 200 else {}
                    for m in chunk:
                        v = float(((j or {}).get(m) or {}).get("usdPrice") or 0)
                        if v > 0:
                            self.prices[m] = (v / usd, time.time())
                except Exception:
                    pass
        for m in list(self.prices):
            if m not in self.positions:
                self.prices.pop(m)

    async def price_loop(self, every=15):
        while True:
            try:
                await self.refresh_prices()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("live price refresh failed: %s", e)
            await asyncio.sleep(every)

    # ------------------------------------------------------------------ names
    def name_targets(self):
        return [m for m, p in self.positions.items() if is_placeholder(p.get("symbol", ""), m)] + \
               [c["mint"] for c in self.closed if is_placeholder(c.get("symbol", ""), c["mint"])]

    def rename(self, mint, symbol, name=""):
        olds = set()
        p = self.positions.get(mint)
        if p and is_placeholder(p.get("symbol", ""), mint):
            olds.add(p["symbol"])
            p["symbol"], p["name"] = symbol, name
        for c in self.closed:
            if c["mint"] == mint and is_placeholder(c.get("symbol", ""), mint):
                olds.add(c.get("symbol", ""))
                c["symbol"] = symbol
        if not olds:
            return
        olds.discard("")
        for e in self.events:
            if e.get("mint") == mint:
                for o in olds:
                    e["text"] = e["text"].replace(o, symbol)
        self._rewrite_csv(lambda rows: [r[:1] + [symbol] + r[2:] if len(r) > 2 and r[2] == mint and is_placeholder(r[1], mint)
                                        else r for r in rows])
        self._save()

    # ------------------------------------------------------------------ manual controls
    def fix_outside_sale(self, mint, opened, usd_back):
        """You sold a coin outside the bot (Phantom): record what you actually got back, in dollars."""
        px = self._usd()
        if not px:
            return "no SOL price yet"
        for c in reversed(self.closed):
            if c["mint"] == mint and abs(float(c["opened"]) - float(opened)) < 2:
                c["sol_out"] = round(float(usd_back) / px, 6)
                pnl = c["sol_out"] - c["sol_in"]
                c.update(pnl_sol=round(pnl, 6), pnl_pct=round(pnl / c["sol_in"] * 100, 1) if c["sol_in"] else 0.0,
                         pnl_usd=round(pnl * px, 2), how="sold in Phantom (amount entered by you)", fixed=True)
                self._save()
                try:
                    with open(os.path.join(self.dir, "corrections.csv"), "a", encoding="utf-8") as fh:
                        fh.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')},{c['symbol']},{mint},usd_back={usd_back},"
                                 f"pnl_usd={c['pnl_usd']}\n")
                except OSError:
                    pass
                self._event("info", f"{c['symbol']}: result corrected to {c['pnl_pct']:+.0f}% (${c['pnl_usd']:+.2f}) "
                                    f"- sold in Phantom for ${float(usd_back):.2f}")
                return None
        return "trade not found"

    async def sell_now(self, mint, frac=1.0):
        frac = min(max(float(frac), 0.01), 1.0)
        await self._sell(mint, frac, "manual" if frac >= 0.99 else f"manual - sold {frac * 100:.0f}%")

    async def sell_all(self):
        for m in list(self.positions):
            await self._sell(m, 1.0, "sell all")

    async def _recheck_one(self, m):
        p = self.positions.get(m)
        if not p or p["status"] != "check":
            return
        try:
            amt = sum(a["ui"] for a in await self._token_accounts(m))
            if amt <= 0:
                self._event("info", f"{p['symbol']}: no tokens held after restart; closing record")
                self._finish(m)
            else:
                p.update(status="open", tokens=amt, tokens_bought=max(p.get("tokens_bought") or 0, amt))
        except Exception as e:
            self._event("error", f"Recheck {p['symbol']} failed: {e}")

    async def recheck(self):
        """After a restart: positions that were mid-trade get their real token balance re-read on-chain."""
        for m in list(self.positions):
            await self._recheck_one(m)
        self._save()

    async def withdraw(self, dest):
        if self.positions:
            raise ValueError("Sell or close all live positions first")
        to = Pubkey.from_string(dest)
        bal = await self.refresh_balance(force=True)
        lamports = int((bal or 0) * LAMPORTS) - 5000
        if lamports <= 0:
            raise ValueError("Nothing to withdraw")
        bh = await self.rpc("getLatestBlockhash", [{"commitment": "confirmed"}])
        ix = transfer(TransferParams(from_pubkey=self.kp.pubkey(), to_pubkey=to, lamports=lamports))
        msg = MessageV0.try_compile(self.kp.pubkey(), [ix], [], Hash.from_string(bh["value"]["blockhash"]))
        sig = await self._send(VersionedTransaction(msg, [self.kp]))
        ok, why = await self._confirm(sig)
        if not ok:
            raise RuntimeError(why or "withdrawal not confirmed")
        self._event("info", f"Withdrew {lamports / LAMPORTS:.4f} SOL to {_short(dest)}", sig=sig)
        await self.refresh_balance(force=True)
        return sig

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # ------------------------------------------------------------------ dashboard
    def state(self, copy_wallets=()):
        c = self.cfg()
        px = self._usd()
        wallets = sorted(set(copy_wallets) | {p["wallet"] for p in self.positions.values()} | {x["wallet"] for x in self.closed})
        per = {}
        for w in wallets:
            op, cl, deployed, realized = self.per_wallet(w)
            priced = [p for p in op if p["status"] not in ("buying", "waiting") and self.px(p["mint"])]
            val = sum(p["tokens"] * self.px(p["mint"]) for p in priced) if priced else None
            per[_short(w)] = dict(open=len(op), closed=len(cl), wins=sum(1 for x in cl if x["pnl_sol"] > 0),
                                  deployed=round(deployed, 4), realized=round(realized, 5),
                                  realized_usd=round(realized * px, 2) if px else None,
                                  open_value=round(val, 5) if val is not None else None, budget_sol=round(self._sol(c.get("budget_usd_per_wallet", 10)) or 0, 4))
        return dict(
            has_wallet=self.kp is not None, address=self.address, balance=self.balance, sol_usd=px,
            enabled=bool(c.get("enabled")), ready=self.enabled(), active=self.active, paused=self.paused,
            has_helius=bool(self._hkey()), trade_usd=c.get("trade_usd", 2.5),
            budget_usd=c.get("budget_usd_per_wallet", 10), max_open=c.get("max_open_per_wallet", 4),
            per_wallet=per,
            positions=[dict(mint=p["mint"], symbol=p["symbol"], name=p.get("name", ""), wallet=_short(p["wallet"]), status=p["status"],
                            sol_in=round(p["sol_in"] or p["size"], 5), sol_out=round(p["sol_out"], 5),
                            left_pct=round(p["tokens"] / p["tokens_bought"] * 100) if p.get("tokens_bought") else None,
                            value=round(p["tokens"] * self.px(p["mint"]), 5)
                            if p["status"] not in ("buying", "waiting") and self.px(p["mint"]) else None,
                            # market cap we actually got in at (SOL spent incl. fees / tokens received), and now
                            entry_mcap=round(p["sol_in"] / p["tokens_bought"] * 1e9, 1)
                            if p.get("sol_in") and (p.get("tokens_bought") or 0) > 1 else None,
                            now_mcap=round(self.px(p["mint"]) * 1e9, 1) if self.px(p["mint"]) else None,
                            age_s=round(time.time() - p["opened"]), last_sig=(p["sigs"] or [None])[-1])
                       for p in self.positions.values()],
            closed=self.closed[-30:][::-1],
            events=self.events[-40:][::-1],
            total_realized=round(sum(x["pnl_sol"] for x in self.closed), 5),
            pnl=self.pnl_summary(),
        )

    def pnl_since(self):
        try:
            with open(os.path.join(self.dir, "pnl_since.txt"), encoding="utf-8") as fh:
                return float(fh.read().strip())
        except (OSError, ValueError):
            return 0.0

    def reset_pnl(self):
        ts = time.time()
        with open(os.path.join(self.dir, "pnl_since.txt"), "w", encoding="utf-8") as fh:
            fh.write(str(ts))
        return ts

    def pnl_summary(self):
        """Actual profit / loss of this wallet (closed trades, in SOL and $), plus what open coins are worth now."""
        px = self._usd() or 0
        now = time.time()
        day0 = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
        since = self.pnl_since()

        def tot(cl):
            sol = sum(x["pnl_sol"] for x in cl)
            return dict(sol=round(sol, 4), usd=round(sol * px, 2) if px else None, trades=len(cl),
                        wins=sum(1 for x in cl if x["pnl_sol"] > 0))
        cost = sum((p["sol_in"] or 0) - (p["sol_out"] or 0) for p in self.positions.values())
        val = sum(p["tokens"] * (self.px(p["mint"]) or 0) for p in self.positions.values()
                  if p["status"] not in ("buying", "waiting"))
        return dict(today=tot([x for x in self.closed if x["closed"] >= day0]),
                    week=tot([x for x in self.closed if x["closed"] >= now - 7 * 86400]),
                    since=tot([x for x in self.closed if x["closed"] >= since]), since_ts=since or None,
                    all=tot(self.closed), open_n=len(self.positions),
                    open_unreal_usd=round((val - cost) * px, 2) if px and self.positions else 0.0)
