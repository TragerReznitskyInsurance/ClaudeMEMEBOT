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

from memebot.chain import trade_from_tx

log = logging.getLogger("memebot")
TRADE_LOCAL = os.environ.get("MOMENTUM_PUMPPORTAL_TRADE", "https://pumpportal.fun/api/trade-local")
RPC = os.environ.get("MOMENTUM_HELIUS_RPC", "https://mainnet.helius-rpc.com/?api-key={key}")
LAMPORTS = 1_000_000_000
RESERVE_SOL = 0.01          # always keep this much for fees / rent


def _short(w):
    return w[:4] + "…" + w[-4:]


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
        self.price_of = lambda mint: None  # set by the app: current SOL price per token
        self.active = False                # true only while a live session is running
        self._load_state()

    # ------------------------------------------------------------------ wallet & state
    def _load_key(self):
        try:
            with open(self.key_path) as fh:
                return Keypair.from_base58_string(json.load(fh)["secret"])
        except (OSError, KeyError, ValueError, json.JSONDecodeError):
            return None

    def create_wallet(self):
        if self.kp is not None:
            raise ValueError("A trading wallet already exists")
        kp = Keypair()
        with open(self.key_path, "w") as fh:
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
            with open(self.state_path) as fh:
                s = json.load(fh)
            self.positions = s.get("positions", {})
            self.closed = s.get("closed", [])
            self.paused = s.get("paused", False)
            for p in self.positions.values():            # anything mid-flight when we stopped
                if p["status"] in ("buying", "selling"):
                    p["status"] = "check"
        except (OSError, json.JSONDecodeError):
            pass

    def _save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"positions": self.positions, "closed": self.closed[-500:], "paused": self.paused}, fh, indent=1)
        os.replace(tmp, self.state_path)

    def _event(self, kind, text, **extra):
        self.events.append(dict(ts=time.time(), kind=kind, text=text, **extra))
        self.events = self.events[-150:]
        (log.warning if kind == "error" else log.info)("LIVE %s", text)

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
        s = await self._session()
        async with s.post(RPC.format(key=self._hkey()), json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                          timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json(content_type=None)
        if "error" in j:
            raise RuntimeError(str(j["error"].get("message") if isinstance(j["error"], dict) else j["error"])[:160])
        return j.get("result")

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
                                                        "preflightCommitment": "confirmed", "maxRetries": 3}])

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
        for attempt in range(6):
            tx = await self.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                                                         "maxSupportedTransactionVersion": 0}])
            if tx:
                return trade_from_tx(tx, self.address, sig)
            await asyncio.sleep(1.5)
        return None

    async def _token_accounts(self, mint):
        r = await self.rpc("getTokenAccountsByOwner", [self.address, {"mint": mint}, {"encoding": "jsonParsed",
                                                                                   "commitment": "confirmed"}])
        out = []
        for a in (r or {}).get("value", []):
            info = a["account"]["data"]["parsed"]["info"]
            out.append(dict(address=a["pubkey"], program=a["account"]["owner"], lamports=a["account"].get("lamports", 0),
                            amount=int(info["tokenAmount"]["amount"]),
                            ui=float(info["tokenAmount"].get("uiAmountString") or 0)))
        return out

    async def _pumpportal(self, action, mint, amount, denom_sol, slippage):
        s = await self._session()
        body = {"publicKey": self.address, "action": action, "mint": mint, "amount": amount,
                "denominatedInSol": "true" if denom_sol else "false", "slippage": slippage,
                "priorityFee": self.cfg().get("priority_fee_sol", 0.0003), "pool": "auto"}
        async with s.post(TRADE_LOCAL, data=body, timeout=aiohttp.ClientTimeout(total=15)) as r:
            raw = await r.read()
            if r.status != 200:
                raise RuntimeError(f"PumpPortal HTTP {r.status}: {raw[:120].decode(errors='ignore')}")
        tx = VersionedTransaction.from_bytes(raw)
        return VersionedTransaction(tx.message, [self.kp])

    def _lock(self, mint):
        return self.locks.setdefault(mint, asyncio.Lock())

    # ------------------------------------------------------------------ copy hooks (called by the engine)
    def on_copy_buy(self, wallet, mint, wallet_sol, symbol=""):
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
                                    queued_sell=0.0, sigs=[], wallet_sol=wallet_sol)
        self._save()
        asyncio.get_running_loop().create_task(self._buy(mint))

    def on_copy_sell(self, wallet, mint, frac):
        p = self.positions.get(mint)
        if not p or p["wallet"] != wallet or not (self.cfg().get("follow_sells", True)):
            return
        if p["status"] == "buying":
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
        async with self._lock(mint):
            try:
                tx = await self._pumpportal("buy", mint, round(p["size"], 6), True, self.cfg().get("buy_slippage_pct", 20))
                sig = await self._send(tx)
                p["sigs"].append(sig)
                ok, why = await self._confirm(sig)
                if ok is None:                            # unknown: check if tokens actually arrived
                    accts = await self._token_accounts(mint)
                    ok = any(a["amount"] > 0 for a in accts)
                    why = why if not ok else ""
                if not ok:
                    raise RuntimeError(why)
                fill = await self._fill(sig)
                if not fill or fill["txType"] != "buy":
                    raise RuntimeError("bought, but couldn't read the fill back")
                p.update(status="open", sol_in=fill["solAmount"], tokens=fill["tokenAmount"],
                         tokens_bought=fill["tokenAmount"], entry_ts=time.time())
                self._event("buy", f"Bought {p['symbol']} for {fill['solAmount']:.4f} SOL (copying {_short(p['wallet'])})",
                            sig=sig, mint=mint)
            except Exception as e:
                self.positions.pop(mint, None)
                self._save()
                return self._event("error", f"Buy {p['symbol']} failed: {e}", mint=mint)
            self._save()
        await self.refresh_balance(force=True)
        if p["queued_sell"] > 0:
            await self._sell(mint, p["queued_sell"], "copied sell (during our buy)")

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
                tx = await self._pumpportal("sell", mint, amount, False, self.cfg().get("sell_slippage_pct", 30))
                sig = await self._send(tx)
                p["sigs"].append(sig)
                ok, why = await self._confirm(sig)
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
                return self._event("error", f"Sell {p['symbol']} ({reason}) failed: {e}", mint=mint)
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

    def _finish(self, mint):
        p = self.positions.pop(mint)
        pnl = p["sol_out"] - p["sol_in"]
        px = self._usd() or 0
        rec = dict(mint=mint, symbol=p["symbol"], wallet=p["wallet"], opened=p["opened"], closed=time.time(),
                   sol_in=round(p["sol_in"], 6), sol_out=round(p["sol_out"], 6), pnl_sol=round(pnl, 6),
                   pnl_pct=round(pnl / p["sol_in"] * 100, 1) if p["sol_in"] else 0.0, pnl_usd=round(pnl * px, 2),
                   sigs=p["sigs"])
        self.closed.append(rec)
        self._event("close", f"Closed {p['symbol']}: {pnl:+.4f} SOL ({rec['pnl_pct']:+.0f}%, ${rec['pnl_usd']:+.2f})",
                    mint=mint, pnl=pnl)
        try:
            with open(os.path.join(self.dir, "live_trades.csv"), "a") as fh:
                if fh.tell() == 0:
                    fh.write("closed_utc,symbol,mint,wallet,sol_in,sol_out,pnl_sol,pnl_pct,pnl_usd,signatures\n")
                fh.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())},{rec['symbol']},{mint},{p['wallet']},"
                         f"{rec['sol_in']},{rec['sol_out']},{rec['pnl_sol']},{rec['pnl_pct']},{rec['pnl_usd']},"
                         f"{' '.join(p['sigs'])}\n")
        except OSError:
            pass

    # ------------------------------------------------------------------ manual controls
    async def sell_now(self, mint):
        await self._sell(mint, 1.0, "manual")

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
            priced = [p for p in op if p["status"] != "buying" and self.price_of(p["mint"])]
            val = sum(p["tokens"] * self.price_of(p["mint"]) for p in priced) if priced else None
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
            positions=[dict(mint=p["mint"], symbol=p["symbol"], wallet=_short(p["wallet"]), status=p["status"],
                            sol_in=round(p["sol_in"] or p["size"], 5), sol_out=round(p["sol_out"], 5),
                            left_pct=round(p["tokens"] / p["tokens_bought"] * 100) if p.get("tokens_bought") else None,
                            value=round(p["tokens"] * self.price_of(p["mint"]), 5)
                            if p["status"] != "buying" and self.price_of(p["mint"]) else None,
                            age_s=round(time.time() - p["opened"]), last_sig=(p["sigs"] or [None])[-1])
                       for p in self.positions.values()],
            closed=self.closed[-30:][::-1],
            events=self.events[-40:][::-1],
            total_realized=round(sum(x["pnl_sol"] for x in self.closed), 5),
        )
