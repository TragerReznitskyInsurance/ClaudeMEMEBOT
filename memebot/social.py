"""
pump.fun "social" numbers for a coin: how many comments (replies) it has, when the last
one was posted, whether it's livestreaming, and whether it has hit King of the Hill.

Free - read from pump.fun's public website data (no SOL, no Helius, no PumpPortal credit).
pump.fun doesn't officially support this, so it can fail or change at any time: every
call is best effort, and after repeated failures it pauses itself for a while instead of
hammering the site. Nothing trading-related depends on it.
"""
from __future__ import annotations

import asyncio
import os
import time

import aiohttp

COIN_URL = os.environ.get("MOMENTUM_PUMP_COIN_URL", "https://frontend-api-v3.pump.fun/coins/{mint}")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/126.0 Safari/537.36",
           "Accept": "application/json", "Origin": "https://pump.fun", "Referer": "https://pump.fun/"}


class Social:
    def __init__(self):
        self.ok = 0
        self.fail = 0
        self.fails_in_row = 0
        self.paused_until = 0.0
        self.last_error = ""
        self.sem = asyncio.Semaphore(3)

    async def fetch(self, session: aiohttp.ClientSession, mint: str) -> dict:
        """{replies, last_reply_age_s, live, koth} or {} if pump.fun didn't answer."""
        if time.time() < self.paused_until:
            return {}
        async with self.sem:
            try:
                async with session.get(COIN_URL.format(mint=mint), headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        raise RuntimeError(f"HTTP {r.status}")
                    j = await r.json(content_type=None)
                if not isinstance(j, dict) or "reply_count" not in j:
                    raise RuntimeError("no reply_count in answer")
            except Exception as e:
                self.fail += 1
                self.fails_in_row += 1
                self.last_error = (str(e) or type(e).__name__)[:80]
                if self.fails_in_row >= 5:                 # back off: 10 min, then try again
                    self.paused_until = time.time() + 600
                    self.fails_in_row = 0
                return {}
        self.ok += 1
        self.fails_in_row = 0
        now = time.time()
        last = j.get("last_reply")
        last = last / 1000 if isinstance(last, (int, float)) and last > 1e11 else last
        koth = j.get("king_of_the_hill_timestamp")
        return dict(replies=int(j.get("reply_count") or 0),
                    last_reply_age_s=round(now - last) if isinstance(last, (int, float)) and last > 0 else None,
                    live=bool(j.get("is_currently_live")),
                    koth=bool(koth))

    def summary(self):
        return dict(ok=self.ok, fail=self.fail, paused=time.time() < self.paused_until, last_error=self.last_error)
