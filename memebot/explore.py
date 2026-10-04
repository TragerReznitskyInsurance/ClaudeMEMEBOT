"""
Research: saves what pump.fun's Explore page shows, every few minutes (4 Oct).

Reads pump.fun's public website data (the same lists the Explore page loads) - free: no SOL, no Helius, no
PumpPortal. pump.fun doesn't officially support this, so it can fail or change at any time; it backs off after
repeated failures. Nothing trading-related depends on it.

Each snapshot -> one line in data/explore.jsonl:
  {"type": "explore", "list": "market_cap" | "last_trade" | "new", "ts": ..., "coins": [
     {"mint", "sym", "rank", "mcap_usd", "mcap_sol", "age_min", "complete", "replies", "koth"} ...]}
Later we can check: do coins that show up on these lists (and how high) go on to run?
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import aiohttp

log = logging.getLogger("memebot")

LIST_URL = os.environ.get("MOMENTUM_PUMP_LIST_URL", "https://frontend-api-v3.pump.fun/coins")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/126.0 Safari/537.36",
           "Accept": "application/json", "Origin": "https://pump.fun", "Referer": "https://pump.fun/"}
LISTS = {"market_cap": "market_cap", "last_trade": "last_trade_timestamp", "new": "created_timestamp"}
MAX_BYTES = 30_000_000


class ExploreLog:
    def __init__(self, path, cfg_getter):
        self.path = path
        self._cfg = cfg_getter
        self.ok = 0
        self.fail = 0
        self.fails_in_row = 0
        self.last_error = ""
        self.last_ok_ts = 0.0
        self.last_count = 0

    def cfg(self):
        return self._cfg().get("explore") or {}

    def state(self):
        return dict(enabled=bool(self.cfg().get("enabled", True)), ok=self.ok, fail=self.fail,
                    last_ok_ts=self.last_ok_ts or None, last_count=self.last_count, error=self.last_error)

    def _write(self, rec):
        try:
            if os.path.exists(self.path) and os.path.getsize(self.path) > MAX_BYTES:
                os.replace(self.path, self.path + ".1")
            with open(self.path, "a", encoding="utf-8", errors="replace") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")
        except OSError as e:
            self.last_error = str(e)[:80]

    async def _one(self, session, name, sort, limit, sol_usd):
        params = {"offset": "0", "limit": str(limit), "sort": sort, "order": "DESC", "includeNsfw": "false"}
        async with session.get(LIST_URL, params=params, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                raise RuntimeError(f"{name}: HTTP {r.status}")
            j = await r.json(content_type=None)
        rows = j.get("coins") if isinstance(j, dict) else j
        if not isinstance(rows, list):
            raise RuntimeError(f"{name}: unexpected answer")
        now = time.time()
        coins = []
        for i, c in enumerate(rows):
            if not isinstance(c, dict) or not c.get("mint"):
                continue
            created = c.get("created_timestamp")
            created = created / 1000 if isinstance(created, (int, float)) and created > 1e11 else created
            usd = c.get("usd_market_cap")
            coins.append(dict(mint=c["mint"], sym=str(c.get("symbol") or "")[:20], rank=i + 1,
                              mcap_usd=round(usd) if isinstance(usd, (int, float)) else None,
                              mcap_sol=round(usd / sol_usd, 1) if isinstance(usd, (int, float)) and sol_usd else None,
                              age_min=round((now - created) / 60, 1) if isinstance(created, (int, float)) else None,
                              complete=bool(c.get("complete")), replies=c.get("reply_count"),
                              koth=bool(c.get("king_of_the_hill_timestamp"))))
        self._write(dict(type="explore", list=name, ts=round(now, 1), coins=coins))
        return len(coins)

    async def run(self, sol_usd_getter):
        async with aiohttp.ClientSession() as session:
            while True:
                c = self.cfg()
                every = float(c.get("every_s", 300))
                if not c.get("enabled", True):
                    await asyncio.sleep(60)
                    continue
                try:
                    n = 0
                    for name, sort in LISTS.items():
                        n += await self._one(session, name, sort, int(c.get("limit", 50)), sol_usd_getter())
                        await asyncio.sleep(2)
                    self.ok += 1
                    self.fails_in_row = 0
                    self.last_ok_ts, self.last_count, self.last_error = time.time(), n, ""
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.fail += 1
                    self.fails_in_row += 1
                    self.last_error = (str(e) or type(e).__name__)[:100]
                    log.info("explore snapshot failed: %s", self.last_error)
                    if self.fails_in_row >= 5:
                        every = 1800                       # back off for 30 min after repeated failures
                await asyncio.sleep(every)
