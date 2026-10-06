"""
Backup source for copy trading: reads the followed wallet's transactions straight
from the chain (Helius RPC, same free key as Wallet Lab).

- resolve(signature): exact token/SOL amounts for one trade, used when the live
  PumpPortal message arrives without a usable token amount (e.g. some PumpSwap sells)
- polling: every few seconds, checks the wallet's newest transactions and feeds any
  trade PumpPortal missed (reconnects, other venues) into the engine.

- fast feed: a Helius websocket tells us the moment a followed wallet signs anything
  (~1s), so trades the PumpPortal feed doesn't report are no longer 5-10s late.
- spot_price(): the token's current price read straight from its pump.fun bonding
  curve (Jupiter as a fallback after it graduates), used by the "don't chase" rule.

Cheap: ~1 credit per call on Helius's free plan. Read-only.
"""
import asyncio
import re
import base64
import logging
import struct
import time

import aiohttp
from solders.pubkey import Pubkey

log = logging.getLogger("memebot")
WSOL = "So11111111111111111111111111111111111111112"
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"
WS = "wss://mainnet.helius-rpc.com/?api-key={key}"
PUMP_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
JUP_PRICE = "https://lite-api.jup.ag/price/v3?ids={mint}"


def curve_address(mint: str) -> str:
    return str(Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))], PUMP_PROGRAM)[0])


def parse_curve(data: bytes):
    """SOL per token from a pump.fun bonding-curve account, or None if it has graduated."""
    if len(data) < 49:
        return None
    vtok, vsol = struct.unpack_from("<QQ", data, 8)
    if data[48] or not vtok:
        return None
    return (vsol / 1e9) / (vtok / 1e6)


async def spot_price(rpc, session, mint: str, sol_usd=None):
    """(SOL per token, source) right now, or (None, "")."""
    try:
        r = await rpc("getAccountInfo", [curve_address(mint), {"encoding": "base64", "commitment": "processed"}])
        v = (r or {}).get("value")
        if v:
            px = parse_curve(base64.b64decode(v["data"][0]))
            if px:
                return px, "curve"
    except Exception:
        pass
    if sol_usd and session is not None:
        try:
            async with session.get(JUP_PRICE.format(mint=mint), timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    usd = float(((await r.json(content_type=None)) or {}).get(mint, {}).get("usdPrice") or 0)
                    if usd > 0:
                        return usd / sol_usd, "jupiter"
        except Exception:
            pass
    return None, ""


def trade_from_tx(tx: dict, wallet: str, sig: str):
    """Turn a jsonParsed transaction into a PumpPortal-shaped trade event for `wallet`, or None."""
    if not tx or (tx.get("meta") or {}).get("err"):
        return None
    meta = tx["meta"]
    msg = (tx.get("transaction") or {}).get("message") or {}
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in msg.get("accountKeys") or msg.get("staticAccountKeys") or []]
    la = meta.get("loadedAddresses") or {}
    if la and len(keys) < len(meta.get("preBalances") or []):
        keys += list(la.get("writable") or []) + list(la.get("readonly") or [])
    sol = 0.0
    if wallet in keys:
        i = keys.index(wallet)
        sol = (meta["postBalances"][i] - meta["preBalances"][i]) / 1e9
    toks = {}
    for side, sign in (("preTokenBalances", -1), ("postTokenBalances", 1)):
        for b in meta.get(side) or []:
            if b.get("owner") != wallet:
                continue
            amt = float((b.get("uiTokenAmount") or {}).get("uiAmountString") or 0)
            toks[b["mint"]] = toks.get(b["mint"], 0.0) + sign * amt
    sol += toks.pop(WSOL, 0.0)
    toks = {m: v for m, v in toks.items() if abs(v) > 0}
    if len(toks) != 1:
        return None
    mint, amt = next(iter(toks.items()))
    if amt > 0 and sol < -0.00002:
        side = "buy"
    elif amt < 0 and sol > 0.00002:
        side = "sell"
    else:
        return None
    return {"signature": sig, "txType": side, "mint": mint, "traderPublicKey": wallet,
            "solAmount": abs(sol), "tokenAmount": abs(amt), "source": "chain"}


class ChainBackup:
    def __init__(self, engine_getter, key: str, poll_s: float = 5.0):
        self._engine = engine_getter
        self.url = RPC.format(key=key)
        self.ws_url = WS.format(key=key)
        self.poll_s = poll_s
        self.slow_poll_s = 15.0                     # when the fast feed is up, polling is only a safety net
        self.ws_connected = False
        self.pending: set[str] = set()
        self.session: aiohttp.ClientSession | None = None
        self.started = time.time()
        self.done: set[str] = set()
        self.stats = {"polls": 0, "recovered": 0, "resolved": 0, "errors": 0, "last_poll": 0.0,
                      "fast": 0, "fast_lag_s": None}
        self.last_error = ""

    async def _rpc(self, method, params):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        async with self.session.post(self.url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                     timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 429:
                raise RuntimeError("rate limited")
            if r.status in (401, 403):
                raise RuntimeError("Helius key rejected")
            j = await r.json(content_type=None)
            if "error" in j:
                raise RuntimeError(str(j["error"])[:120])
            return j.get("result")

    async def _get_tx(self, sig):
        """getTransaction that also accepts newer transaction versions: some wallets (trading bots/terminals) send
        version-1 transactions, which the RPC refuses unless we say we can read them (6 Oct: every BB1jeG trade
        failed with 'Transaction version (1) is not supported')."""
        ver = getattr(self, "tx_version", 0)
        for _ in range(3):
            try:
                return await self._rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                                                                 "maxSupportedTransactionVersion": ver}])
            except RuntimeError as e:
                m = re.search(r"[Tt]ransaction version \((\d+)\) is not supported", str(e))
                if not m or int(m.group(1)) <= ver:
                    raise
                ver = self.tx_version = int(m.group(1))
                log.info("wallet feed: reading version-%d transactions from now on", ver)
        return None

    async def fetch_trade(self, sig, wallet, want_time=False):
        for wait in (0.3, 0.6, 1.0, 1.5, 2.5, 4.0):  # a just-landed tx can take a moment to be queryable
            tx = await self._get_tx(sig)
            if tx:
                ev = trade_from_tx(tx, wallet, sig)
                return (ev, tx.get("blockTime")) if want_time else ev
            await asyncio.sleep(wait)
        return (None, None) if want_time else None

    # ---------------------------------------------------------------- fast feed (websocket)
    def _spawn(self, sig, wallet):
        if sig in self.done or sig in self.pending or self._engine().seen_signature(sig):
            return
        self.pending.add(sig)
        asyncio.get_running_loop().create_task(self._fast(sig, wallet))

    async def _fast(self, sig, wallet):
        try:
            ev, bt = await self.fetch_trade(sig, wallet, want_time=True)
            self.done.add(sig)
            eng = self._engine()
            if ev and not eng.seen_signature(sig):
                self.stats["fast"] += 1
                if bt:
                    self.stats["fast_lag_s"] = round(max(0.0, time.time() - bt), 1)
                ev["source"] = "chain-fast"
                eng.copy_from_chain(ev, time.time())
        except Exception as e:
            self.stats["errors"] += 1
            self.last_error = str(e) or type(e).__name__
        finally:
            self.pending.discard(sig)

    async def listen(self):
        """Helius websocket: one logsSubscribe per followed wallet; every new signature is read at once."""
        backoff = 1
        while True:
            try:
                wallets = sorted(self._engine().copy_wallets())
                if not wallets:
                    await asyncio.sleep(5)
                    continue
                if self.session is None or self.session.closed:
                    self.session = aiohttp.ClientSession()
                async with self.session.ws_connect(self.ws_url, heartbeat=25, timeout=15) as ws:
                    for i, w in enumerate(wallets):
                        await ws.send_json({"jsonrpc": "2.0", "id": i + 1, "method": "logsSubscribe",
                                            "params": [{"mentions": [w]}, {"commitment": "confirmed"}]})
                    subs = {}
                    async for msg in ws:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        j = msg.json()
                        if "error" in j:
                            raise RuntimeError(str(j["error"])[:120])
                        if "id" in j and "result" in j:
                            subs[j["result"]] = wallets[j["id"] - 1]
                            if len(subs) == len(wallets):
                                if not self.ws_connected:
                                    log.info("fast wallet feed connected (%d wallet(s))", len(wallets))
                                self.ws_connected, backoff = True, 1
                            continue
                        if j.get("method") == "logsNotification":
                            p = j["params"]
                            v = (p.get("result") or {}).get("value") or {}
                            w = subs.get(p.get("subscription"))
                            if w and v.get("signature") and not v.get("err"):
                                self._spawn(v["signature"], w)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats["errors"] += 1
                self.last_error = "fast feed: " + (str(e) or type(e).__name__)
            self.ws_connected = False
            await asyncio.sleep(backoff)
            backoff = min(30, backoff * 2)

    def resolve(self, sig: str, wallet: str):
        """Called by the engine when a live message lacks usable amounts."""
        asyncio.get_running_loop().create_task(self._resolve(sig, wallet))

    async def _resolve(self, sig, wallet):
        try:
            ev = await self.fetch_trade(sig, wallet)
            self.done.add(sig)
            if ev:
                self.stats["resolved"] += 1
                self._engine().copy_from_chain(ev, time.time())
        except Exception as e:
            self.stats["errors"] += 1
            self.last_error = str(e) or type(e).__name__

    async def run(self):
        while True:
            try:
                eng = self._engine()
                for w in eng.copy_wallets():
                    sigs = await self._rpc("getSignaturesForAddress", [w, {"limit": 40, "commitment": "confirmed"}]) or []
                    self.stats["polls"] += 1
                    self.stats["last_poll"] = time.time()
                    new = [s for s in reversed(sigs)
                           if not s.get("err") and (s.get("blockTime") or 0) >= self.started - 5
                           and s["signature"] not in self.done and s["signature"] not in self.pending
                           and not eng.seen_signature(s["signature"])]
                    for s in new:
                        ev = await self.fetch_trade(s["signature"], w)
                        self.done.add(s["signature"])
                        if ev and not eng.seen_signature(s["signature"]):
                            self.stats["recovered"] += 1
                            log.info("chain backup recovered a %s the live feed missed", ev["txType"])
                            eng.copy_from_chain(ev, time.time())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats["errors"] += 1
                self.last_error = str(e) or type(e).__name__
            await asyncio.sleep(self.slow_poll_s if self.ws_connected else self.poll_s)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
