"""
Free replacement for PumpPortal's paid per-token trade stream: every few seconds, read the pump.fun bonding-curve
accounts of all watched coins from Helius (100 coins per request) and turn every change into a trade event for the
engine (price, market cap, SOL in/out since the last read).

What it can't know: WHO traded (wallets, buyer counts) and individual trades between two reads - several trades in
one interval arrive as one combined event. Prices, market caps, peaks, pullbacks and bounces (all Reclaim needs) are
exact. New-coin and migration events still come from PumpPortal (free); wallet #1's trades too.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import struct
import time

import aiohttp

from memebot import wallet_tokens as WT
from memebot.chain import curve_address
from memebot.live import LiveFeed

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000


class PollingFeed(LiveFeed):
    """Same interface as LiveFeed for the engine, but watched coins are polled on-chain instead of paid streaming.
    Wallet (copy-trading) subscriptions still go to PumpPortal."""

    def __init__(self, has_key: bool, accounts=()):
        super().__init__(has_key, accounts)
        self.watch: set[str] = set()
        self.curves: dict[str, str] = {}
        self.stats = dict(polls=0, events=0, errors=0, watched=0)
        self.last_error = ""

    def subscribe(self, mint):
        if mint not in self.watch:
            try:
                self.curves[mint] = curve_address(mint)
            except Exception:
                return
            self.watch.add(mint)

    def unsubscribe(self, mint):
        self.watch.discard(mint)
        self.curves.pop(mint, None)


async def poll_loop(engine, feed: PollingFeed, key_getter, every=3.0):
    last: dict[str, tuple[int, int]] = {}
    session = aiohttp.ClientSession()
    try:
        while True:
            await asyncio.sleep(every)
            key = key_getter()
            mints = [m for m in list(feed.watch) if m in feed.curves]
            feed.stats["watched"] = len(mints)
            for gone in [m for m in last if m not in feed.watch]:
                last.pop(gone, None)
            if not key or not mints:
                continue
            for i in range(0, len(mints), 100):
                chunk = mints[i:i + 100]
                try:
                    async with session.post(WT.rpc_url(key), json={
                            "jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                            "params": [[feed.curves[m] for m in chunk], {"encoding": "base64", "commitment": "confirmed"}]},
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
                        j = await r.json(content_type=None)
                    vals = ((j or {}).get("result") or {}).get("value") or []
                except Exception as e:
                    feed.stats["errors"] += 1
                    feed.last_error = f"{type(e).__name__}"
                    continue
                feed.stats["polls"] += 1
                now = time.time()
                for m, acc in zip(chunk, vals):
                    if not acc:
                        continue
                    try:
                        d = base64.b64decode(acc["data"][0])
                    except Exception:
                        continue
                    if len(d) < 49 or d[48]:
                        continue                              # not a curve / graduated (migration comes from PumpPortal)
                    vt, vs = struct.unpack_from("<QQ", d, 8)
                    if not vt:
                        continue
                    prev = last.get(m)
                    last[m] = (vs, vt)
                    if prev is None or prev == (vs, vt):
                        continue
                    dsol = (vs - prev[0]) / 1e9
                    ev = dict(txType="buy" if dsol > 0 else "sell", mint=m, solAmount=abs(dsol),
                              tokenAmount=abs(vt - prev[1]) / 1e6, vSolInBondingCurve=vs / 1e9,
                              vTokensInBondingCurve=vt / 1e6, marketCapSol=(vs / 1e9) / (vt / 1e6) * SUPPLY,
                              traderPublicKey="", _poll=True)
                    try:
                        engine.on_event(ev, now)
                        feed.stats["events"] += 1
                    except Exception as e:
                        log.debug("poll event failed: %s", e)
    finally:
        await session.close()
