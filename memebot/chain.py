"""
Backup source for copy trading: reads the followed wallet's transactions straight
from the chain (Helius RPC, same free key as Wallet Lab).

- resolve(signature): exact token/SOL amounts for one trade, used when the live
  PumpPortal message arrives without a usable token amount (e.g. some PumpSwap sells)
- polling: every few seconds, checks the wallet's newest transactions and feeds any
  trade PumpPortal missed (reconnects, other venues) into the engine.

Cheap: ~1 credit per call on Helius's free plan. Read-only.
"""
import asyncio
import logging
import time

import aiohttp

log = logging.getLogger("memebot")
WSOL = "So11111111111111111111111111111111111111112"
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"


def trade_from_tx(tx: dict, wallet: str, sig: str):
    """Turn a jsonParsed transaction into a PumpPortal-shaped trade event for `wallet`, or None."""
    if not tx or (tx.get("meta") or {}).get("err"):
        return None
    meta = tx["meta"]
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
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
    def __init__(self, engine_getter, key: str, poll_s: float = 10.0):
        self._engine = engine_getter
        self.url = RPC.format(key=key)
        self.poll_s = poll_s
        self.session: aiohttp.ClientSession | None = None
        self.started = time.time()
        self.done: set[str] = set()
        self.stats = {"polls": 0, "recovered": 0, "resolved": 0, "errors": 0}
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

    async def fetch_trade(self, sig, wallet):
        for attempt in range(4):                    # a just-landed tx can take a moment to be queryable
            tx = await self._rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                                                          "maxSupportedTransactionVersion": 0}])
            if tx:
                return trade_from_tx(tx, wallet, sig)
            await asyncio.sleep(1.5 * (attempt + 1))
        return None

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
                    new = [s for s in reversed(sigs)
                           if not s.get("err") and (s.get("blockTime") or 0) >= self.started - 5
                           and s["signature"] not in self.done and not eng.seen_signature(s["signature"])]
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
            await asyncio.sleep(self.poll_s)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
