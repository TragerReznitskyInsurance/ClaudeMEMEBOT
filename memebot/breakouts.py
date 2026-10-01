"""
Breakout recorder - data to learn how wallet #1 picks its coins (and what makes a breakout win).

Records to data/breakouts.jsonl (rotates at ~40 MB):

  {"type":"cross"}   EVERY coin the bot sees rising through 44 SOL - young coins from the live feed,
                     older ones from Survivor's on-chain watch list - with what it looked like right then
                     (age, trading in the last minutes, how far below its high, ...)
  {"type":"holders"} a moment later: how concentrated the holders are (top 1 / top 10 / top 20 %,
                     the creator's share if known)
  {"type":"launch"}  for a sample of crossings (daily cap): the launch bundle / sniper check - how much of
                     the supply the creator's same-block wallets and first-3-second bots took, and how much
                     of it is still held
  {"type":"w1"}      every wallet #1 buy, with the wallets that bought the same coin in the ~minute before it
                     ("leaders" - if the same wallets keep buying just before it, we can follow them instead)
  {"type":"after"}   market cap 30 min, 2 h and 6 h after each crossing (graduated or not)

Costs: a couple of cheap RPC calls per crossing, one Enhanced-Transactions call per wallet #1 buy and per
sampled launch check (capped per day). No SOL.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import struct
import time
from collections import OrderedDict

import aiohttp

from memebot import wallet_tokens as WT
from memebot.chain import curve_address

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000
CHECKS_MIN = (30, 120, 360)
MAX_BYTES = 40_000_000


class BreakoutLog:
    def __init__(self, path, key_getter, cfg_getter):
        self.path = path
        self._key = key_getter
        self._cfg = cfg_getter
        self.last: dict[str, float] = {}                  # mint -> last crossing recorded (dedupe)
        self.first: dict[str, float] = {}                 # mint -> age (s) when it FIRST rose through 44 SOL
        self.pending: OrderedDict = OrderedDict()         # crossing id -> {mint, ts, due}
        self.q: asyncio.Queue | None = None
        self.session: aiohttp.ClientSession | None = None
        self.day, self.launch_checks = "", 0
        self.stats = dict(cross=0, holders=0, launch=0, w1=0, after=0, errors=0)
        self.last_error = ""

    def cfg(self):
        return self._cfg().get("breakouts") or {}

    def enabled(self):
        return bool(self.cfg().get("enabled", True))

    def _write(self, rec):
        try:
            if os.path.exists(self.path) and os.path.getsize(self.path) > MAX_BYTES:
                os.replace(self.path, self.path + ".1")
            with open(self.path, "a", encoding="utf-8", errors="replace") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")
        except OSError as e:
            self.last_error = str(e)

    def _queue(self):
        if self.q is None:
            self.q = asyncio.Queue(maxsize=5000)
        return self.q

    def _put(self, job):
        try:
            self._queue().put_nowait(job)
        except (asyncio.QueueFull, RuntimeError):
            pass

    # ------------------------------------------------------------------ hooks
    def on_cross(self, mint, symbol, ts, age_s, mcap, source, creator=None, feats=None):
        """A coin rose through the line (44 SOL)."""
        if not self.enabled() or not mint:
            return
        if age_s is not None and mint not in self.first:
            self.first[mint] = age_s
            if len(self.first) > 50000:
                for m in list(self.first)[:10000]:
                    self.first.pop(m, None)
        if ts - self.last.get(mint, 0) < float(self.cfg().get("dedupe_min", 30)) * 60:
            return
        self.last[mint] = ts
        if len(self.last) > 50000:
            for m in list(self.last)[:10000]:
                self.last.pop(m, None)
        cid = f"{mint}:{int(ts)}"
        rec = dict(feats or {})
        rec.update(type="cross", id=cid, mint=mint, sym=(symbol or "")[:20], ts=round(ts, 1),
                   age_s=round(age_s) if age_s is not None else None, mcap=round(mcap, 1), source=source,
                   creator=creator or rec.get("creator"))
        self._write(rec)
        self.stats["cross"] += 1
        self.pending[cid] = dict(mint=mint, ts=ts, due=list(CHECKS_MIN))
        while len(self.pending) > 20000:
            self.pending.popitem(last=False)
        self._put(("holders", cid, mint, creator))
        day = time.strftime("%Y-%m-%d")
        if day != self.day:
            self.day, self.launch_checks = day, 0
        c = self.cfg()
        if (self.launch_checks < int(c.get("launch_checks_per_day", 400))
                and random.random() < float(c.get("launch_check_share", 0.35))):
            self.launch_checks += 1
            self._put(("launch", cid, mint, None))

    def signal(self, rec, mint, ts):
        """A strategy's buy signal (with its features): record it and follow the coin for 30 min / 2 h / 6 h."""
        cid = f"sig:{mint}:{int(ts)}"
        rec = dict(rec, id=cid, mint=mint, ts=round(ts, 1))
        if mint in self.first:
            rec["first44_age_s"] = round(self.first[mint])
        self._write(rec)
        self.pending[cid] = dict(mint=mint, ts=ts, due=list(CHECKS_MIN))

    def on_w1_buy(self, wallet, mint, ts, sol, mcap, sig):
        if not self.enabled():
            return
        q = self.__dict__.setdefault("_w1_times", [])
        q[:] = [x for x in q if ts - x < 3600]
        if len(q) >= int(self.cfg().get("w1_per_hour", 150)):   # very busy followed wallets: keep Helius usage sane
            return
        q.append(ts)
        self._write(dict(type="w1", mint=mint, ts=round(ts, 1), wallet=wallet, sol=round(sol or 0, 4),
                         mcap=round(mcap, 1) if mcap else None, sig=sig))
        self.stats["w1"] += 1
        if sig:
            self._put(("leaders", sig, mint, wallet))
        self._put(("launch", f"w1:{mint}:{int(ts)}", mint, None))   # always profile the coins it buys

    # ------------------------------------------------------------------ workers
    async def _sess(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _rpc(self, method, params):
        s = await self._sess()
        async with s.post(WT.rpc_url(self._key()), json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                          timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json(content_type=None)
        if "error" in j:
            raise RuntimeError(str(j["error"])[:100])
        return j.get("result")

    async def _holders(self, cid, mint, creator):
        from memebot.snapshots import curve_token_accounts
        big = await self._rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        vals = (big or {}).get("value") or []
        curve = curve_token_accounts(mint)
        vals = [v for v in vals if v["address"] not in curve]
        amts = [float(v.get("uiAmountString") or v.get("uiAmount") or 0) for v in vals]
        rec = dict(type="holders", id=cid, mint=mint, n_top=len(amts),
                   top1_pct=round(amts[0] / SUPPLY * 100, 2) if amts else 0.0,
                   top10_pct=round(sum(amts[:10]) / SUPPLY * 100, 2), top20_pct=round(sum(amts[:20]) / SUPPLY * 100, 2))
        if creator and vals:
            accs = await self._rpc("getMultipleAccounts", [[v["address"] for v in vals[:20]], {"encoding": "jsonParsed"}])
            for v, a, amt in zip(vals, (accs or {}).get("value") or [], amts):
                owner = (((a or {}).get("data") or {}).get("parsed") or {}).get("info", {}).get("owner")
                if owner == creator:
                    rec["creator_pct"] = round(amt / SUPPLY * 100, 2)
                    break
            rec.setdefault("creator_pct", 0.0)                 # not among the top 20
        self._write(rec)
        self.stats["holders"] += 1

    async def _launch(self, cid, mint):
        from memebot import launchcheck
        r = await launchcheck.check(await self._sess(), self._key(), mint)
        if r.get("error"):
            return
        self._write(dict(type="launch", id=cid, mint=mint, verdict=r.get("verdict"),
                         mcap_after_block=r.get("mcap_after_block_sol"),
                         creator_buy=r.get("creator_buy"), bundle=r.get("bundle"), snipers=r.get("snipers"),
                         top10_pct=r.get("top10_pct"), top10_launch_pct=r.get("top10_launch_pct")))
        self.stats["launch"] += 1

    async def _leaders(self, sig, mint, wallet):
        from memebot.snapshots import fetch_before
        await asyncio.sleep(40)                                # let the indexer see the wallet's buy
        txs = await fetch_before(await self._sess(), self._key(), mint, sig)
        if not txs:
            return
        t0 = None
        out = []
        for tx in txs:                                         # newest first
            tr = WT._trade_of(tx, mint)
            ts = tx.get("timestamp")
            if not tr or not ts:
                continue
            t0 = t0 or ts
            if tr[1] != "buy" or tr[0] == wallet:
                continue
            out.append(dict(w=tr[0], s_before=round(t0 - ts), sol=round(tr[2], 3)))
            if len(out) >= 40:
                break
        self._write(dict(type="leaders", mint=mint, sig=sig, buyers=out))

    async def _worker(self):
        q = self._queue()
        while True:
            kind, a, b, c = await q.get()
            if not self._key():
                continue
            try:
                if kind == "holders":
                    await asyncio.sleep(2)
                    await self._holders(a, b, c)
                elif kind == "launch":
                    await self._launch(a, b)
                elif kind == "leaders":
                    await self._leaders(a, b, c)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats["errors"] += 1
                self.last_error = f"{kind}: {e}"[:120]
            await asyncio.sleep(0.2)

    async def _outcomes(self):
        now = time.time()
        due = [(cid, p) for cid, p in self.pending.items() if p["due"] and now - p["ts"] >= p["due"][0] * 60][:300]
        if not due:
            return
        mints = sorted({p["mint"] for _, p in due})
        state = {}
        for i in range(0, len(mints), 100):
            chunk = mints[i:i + 100]
            try:
                addrs = [curve_address(m) for m in chunk]
            except Exception:
                continue
            r = await self._rpc("getMultipleAccounts", [addrs, {"encoding": "base64", "commitment": "confirmed"}])
            for m, acc in zip(chunk, (r or {}).get("value") or []):
                d = base64.b64decode(acc["data"][0]) if acc else b""
                if len(d) >= 49:
                    vt, vs = struct.unpack_from("<QQ", d, 8)
                    grad = bool(d[48])
                    state[m] = dict(graduated=grad, mcap=None if grad or not vt else round((vs / 1e9) / (vt / 1e6) * SUPPLY, 1))
        for cid, p in due:
            after = p["due"].pop(0)
            st = state.get(p["mint"])
            if st is not None:
                self._write(dict(type="after", id=cid, mint=p["mint"], after_min=after, **st))
                self.stats["after"] += 1
            if not p["due"]:
                self.pending.pop(cid, None)

    async def run(self):
        workers = [asyncio.create_task(self._worker()) for _ in range(2)]
        try:
            while True:
                await asyncio.sleep(60)
                if not self._key():
                    continue
                try:
                    await self._outcomes()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.stats["errors"] += 1
                    self.last_error = f"outcomes: {e}"[:120]
        finally:
            for w in workers:
                w.cancel()

    def summary(self):
        return dict(**self.stats, waiting=len(self.pending), launch_checks_today=self.launch_checks,
                    last_error=self.last_error)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
