"""
"Why didn't it buy?" log - one line every time the bot passes on a coin, plus what that
coin did afterwards, so skipped coins that ran can be studied and the filters tuned.

data/coin_decisions.jsonl (rotates to .1 at ~30 MB):
  {"type":"why", "ts", "mint", "sym", "stage", "why"}
      stage: not watched | dropped | stopped watching | lookalike | real money | bought
  {"type":"after", "mint", "after_min", "mcap_sol", "graduated", "mayhem"}
      market cap 30 min and 2 h after the bot passed on it (one batched RPC call per 100 coins)

Free apart from those batched Helius calls (a few hundred a day).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import struct
import time
from collections import OrderedDict

import aiohttp

from memebot.chain import curve_address

log = logging.getLogger("memebot")
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"
CHECKS_MIN = (30, 120)
NO_FOLLOWUP = {"bought"}
MAX_PENDING = 30000
MAX_BYTES = 30_000_000


class WhyLog:
    def __init__(self, path: str, key_getter):
        self.path = path
        self._key = key_getter
        self.seen: OrderedDict = OrderedDict()          # (mint, stage, why-prefix) -> None, to avoid repeats
        self.pending: OrderedDict = OrderedDict()       # mint -> {ts, due}
        self.session: aiohttp.ClientSession | None = None
        self.count = 0
        self.checked = 0
        self.last_error = ""

    # ------------------------------------------------------------------ writing
    def _write(self, rec):
        try:
            if os.path.exists(self.path) and os.path.getsize(self.path) > MAX_BYTES:
                os.replace(self.path, self.path + ".1")
            with open(self.path, "a", encoding="utf-8", errors="replace") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")
        except OSError as e:
            self.last_error = str(e)

    def note(self, mint, symbol, stage, why, ts=None):
        """Record that the bot passed on (or bought) a coin, and why."""
        if not mint:
            return
        k = (mint, stage, why[:25])
        if k in self.seen:
            return
        self.seen[k] = None
        while len(self.seen) > 60000:
            self.seen.popitem(last=False)
        ts = ts or time.time()
        self._write(dict(type="why", ts=round(ts, 1), mint=mint, sym=(symbol or "")[:20], stage=stage, why=why))
        self.count += 1
        if stage in NO_FOLLOWUP:
            self.pending.pop(mint, None)                # we bought it: no "what did we miss" check
        elif mint not in self.pending:
            self.pending[mint] = dict(ts=ts, due=list(CHECKS_MIN))
            while len(self.pending) > MAX_PENDING:
                self.pending.popitem(last=False)

    # ------------------------------------------------------------------ follow-up checks
    async def _check(self):
        key = self._key()
        if not key:
            return
        now = time.time()
        due = [m for m, p in self.pending.items() if p["due"] and now - p["ts"] >= p["due"][0] * 60][:1000]
        if not due:
            return
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        addr = {}
        for m in due:
            try:
                addr[m] = curve_address(m)
            except Exception:
                self.pending.pop(m, None)                 # not a valid coin address - never check it
        due = list(addr)
        for i in range(0, len(due), 100):
            batch = due[i:i + 100]
            body = {"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                    "params": [[addr[m] for m in batch], {"encoding": "base64", "commitment": "confirmed"}]}
            async with self.session.post(RPC.format(key=key), json=body, timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
            if "error" in j:
                raise RuntimeError(str(j["error"])[:100])
            for m, acc in zip(batch, (j.get("result") or {}).get("value") or []):
                p = self.pending.get(m)
                if not p:
                    continue
                rec = dict(type="after", mint=m, after_min=p["due"].pop(0), mcap_sol=None, graduated=None, mayhem=None)
                if acc:
                    d = base64.b64decode(acc["data"][0])
                    if len(d) >= 49:
                        vtok, vsol = struct.unpack_from("<QQ", d, 8)
                        rec["graduated"] = bool(d[48])
                        rec["mayhem"] = bool(d[81]) if len(d) > 81 else None
                        if not rec["graduated"] and vtok:
                            rec["mcap_sol"] = round((vsol / 1e9) / (vtok / 1e6) * 1e9, 1)
                self._write(rec)
                self.checked += 1
                if not p["due"]:
                    self.pending.pop(m, None)
            await asyncio.sleep(0.3)

    async def run(self):
        while True:
            await asyncio.sleep(60)
            try:
                await self._check()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"follow-up: {e}"[:120]

    # ------------------------------------------------------------------ reading
    def find(self, mint: str, limit=60) -> list:
        out = []
        for p in (self.path + ".1", self.path):
            try:
                with open(p, encoding="utf-8", errors="replace") as fh:
                    for ln in fh:
                        if mint in ln:
                            try:
                                out.append(json.loads(ln))
                            except ValueError:
                                pass
            except OSError:
                pass
        return out[-limit:]

    def summary(self):
        return dict(logged=self.count, followups=self.checked, waiting=len(self.pending), last_error=self.last_error)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
