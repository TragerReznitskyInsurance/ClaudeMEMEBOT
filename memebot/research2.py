"""
Research data collectors (21 Oct, owner's request: "collect as much data as possible"). Nothing here trades.

1. SMART MONEY - for every coin that bonds (PumpPortal migration event) and a sample of coins that crossed 44 SOL
   and did NOT bond within 6 h, fetch the coin's first 100 transactions (Helius, same call the launch check uses)
   and record who bought early and when. Builds a running scoreboard of wallets that keep buying early into coins
   that go on to bond (data/smart_wallets.json). Once wallets qualify, every buy they make in a coin we're watching
   is logged as a signal (data/smart_signals.jsonl) so a "follow smart money" strategy can be paper-tested.
     data/smart_early.jsonl   {mint, label (1 bonded / 0 not), ts, buyers: [[wallet, seconds_after_launch, sol]]}

2. AFTER THE BOND - every coin that migrates is priced through Jupiter for 24 h (every 2 min for the first 2 h,
   then every 10 min), so "buy after migration" strategies can be tested.
     data/postbond.jsonl      {m, sym, t0 (migration time), dt (minutes since), usd (price), mc_usd}

3. ROCKETS - every coin that crosses 44 SOL gets its market cap recorded from the live trade stream every few
   seconds for 30 min (and the bot keeps watching it that long), so "catch the 45 -> 360 SOL rocket early and sell
   fast" strategies can be tested with real price paths.
     data/rocket_paths.jsonl  {m, sym, t0, age_s, mc0, path: [[seconds_since_cross, mcap_sol, buys, sells]]}

Costs: up to `smart_fetch_per_day` (300) Helius enhanced-transaction calls a day; Jupiter's free price API.
Settings: `research2` section in config.yaml.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from collections import OrderedDict

import aiohttp

from memebot import wallet_tokens as WT

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000
JUP = "https://lite-api.jup.ag/price/v3?ids={ids}"
MAX_FILE = 80_000_000


class Research2:
    def __init__(self, data_dir, key_getter, cfg_getter):
        self.dir = data_dir
        self._key = key_getter
        self._cfg = cfg_getter
        self.session = None
        self.q: asyncio.Queue | None = None
        self.crossed: OrderedDict = OrderedDict()      # mint -> cross ts (loser sampling after 6 h)
        self.migrated: dict[str, float] = {}           # mint -> migration ts
        self.post: dict[str, dict] = {}                # mint -> {sym, t0, next}
        self.rockets: dict[str, dict] = {}             # mint -> path being recorded
        self.fetched: set[str] = set()
        self.day, self.fetches = "", 0
        self.wallets: dict[str, list] = {}             # wallet -> [bond_hits, loss_hits, last_ts]
        self.stats = dict(smart_bond=0, smart_loss=0, smart_signals=0, post_coins=0, post_rows=0, rockets=0, errors=0)
        self.last_error = ""
        try:
            with open(os.path.join(data_dir, "smart_wallets.json"), encoding="utf-8") as fh:
                self.wallets = (json.load(fh) or {}).get("wallets") or {}
        except (OSError, ValueError):
            pass

    # ------------------------------------------------------------------ helpers
    def cfg(self):
        return self._cfg().get("research2") or {}

    def enabled(self):
        return bool(self.cfg().get("enabled", True))

    def _write(self, name, rec):
        p = os.path.join(self.dir, name)
        try:
            if os.path.exists(p) and os.path.getsize(p) > MAX_FILE:
                os.replace(p, p + ".1")
            with open(p, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        except OSError as e:
            self.last_error = f"write: {e}"[:120]

    def _save_wallets(self):
        try:
            p = os.path.join(self.dir, "smart_wallets.json")
            if len(self.wallets) > 60000:                  # keep the scoreboard bounded: drop one-off wallets
                self.wallets = {w: v for w, v in self.wallets.items() if v[0] + v[1] > 1}
            with open(p + ".tmp", "w", encoding="utf-8") as fh:
                json.dump(dict(updated=time.time(), wallets=self.wallets), fh, separators=(",", ":"))
            os.replace(p + ".tmp", p)
        except OSError:
            pass

    def _queue(self):
        if self.q is None:
            self.q = asyncio.Queue(maxsize=2000)
        return self.q

    async def _sess(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"User-Agent": "momentum-research"})
        return self.session

    def smart_set(self):
        """Wallets that qualify as smart money right now: >= min_bonds early buys in bonders and a bond share of
        at least min_share among their recorded early buys."""
        c = self.cfg()
        nb, share = int(c.get("smart_min_bonds", 3)), float(c.get("smart_min_share", 0.3))
        return {w for w, v in self.wallets.items() if v[0] >= nb and v[0] / max(1, v[0] + v[1]) >= share}

    # ------------------------------------------------------------------ engine hooks
    def on_migrate(self, mint, symbol, ts):
        # pump.fun coins only (their addresses end in "pump"): the migration feed also carries other launchpads,
        # whose supply/decimals make the market caps meaningless here
        if not self.enabled() or mint in self.migrated or not str(mint).endswith("pump"):
            return
        self.migrated[mint] = ts
        if len(self.migrated) > 20000:
            for m in list(self.migrated)[:5000]:
                self.migrated.pop(m, None)
        self.post[mint] = dict(sym=(symbol or "")[:20], t0=ts, next=ts + 30)
        self.stats["post_coins"] += 1
        self._fetch(mint, 1)

    def on_cross(self, mint, symbol, ts, age_s, mcap):
        if not self.enabled() or not str(mint).endswith("pump") or not mcap or mcap > 120:
            return                                      # pump.fun coins crossing 44 SOL normally (not launch-block jumps)
        if mint not in self.crossed:
            self.crossed[mint] = ts
            while len(self.crossed) > 30000:
                self.crossed.popitem(last=False)
        if mint not in self.rockets:
            self.rockets[mint] = dict(sym=(symbol or "")[:20], t0=ts, age_s=round(age_s or 0), mc0=round(mcap, 1),
                                      path=[[0, round(mcap, 1), 0, 0]], last=ts, buys=0, sells=0)

    def on_trade(self, mint, ts, mcap, trader, side, sol):
        if not self.enabled():
            return
        r = self.rockets.get(mint)
        if r is not None and mcap:
            if trader and side == "buy":                # (on-chain price polls carry no trader - not trades)
                r["buys"] += 1
            elif trader and side == "sell":
                r["sells"] += 1
            if ts - r["last"] >= float(self.cfg().get("rocket_sample_s", 3)):
                r["path"].append([round(ts - r["t0"]), round(mcap, 1), r["buys"], r["sells"]])
                r["last"] = ts
            if ts - r["t0"] > float(self.cfg().get("rocket_window_min", 30)) * 60 or mcap >= 400:
                self._finish_rocket(mint)
        if side == "buy" and trader and trader in self._smart_cache():
            self._write("smart_signals.jsonl", dict(ts=round(ts, 1), m=mint, w=trader, sol=round(sol or 0, 3),
                                                     mc=round(mcap or 0, 1), wallet_score=self.wallets.get(trader)))
            self.stats["smart_signals"] += 1

    def wants_watch(self, mint):
        r = self.rockets.get(mint)
        return r is not None and time.time() - r["t0"] < float(self.cfg().get("rocket_window_min", 30)) * 60

    def _smart_cache(self):
        now = time.time()
        if now - self.__dict__.get("_sc_ts", 0) > 300:
            self._sc, self._sc_ts = self.smart_set(), now
        return self._sc

    def _finish_rocket(self, mint):
        r = self.rockets.pop(mint, None)
        if not r:
            return
        self._write("rocket_paths.jsonl", dict(m=mint, sym=r["sym"], t0=round(r["t0"], 1), age_s=r["age_s"],
                                               mc0=r["mc0"], path=r["path"]))
        self.stats["rockets"] += 1

    # ------------------------------------------------------------------ smart money
    def _fetch(self, mint, label):
        if mint in self.fetched:
            return
        day = time.strftime("%Y-%m-%d")
        if day != self.day:
            self.day, self.fetches, self.fetch_split = day, 0, [0, 0]
        split = self.__dict__.setdefault("fetch_split", [0, 0])      # [not bonded, bonded] today
        cap = int(self.cfg().get("smart_fetch_per_day", 300))
        if self.fetches >= cap or split[label] >= (cap * (0.65 if label else 0.35)):
            return                                      # keep room for both kinds every day (bonders come first)
        split[label] += 1
        self.fetches += 1
        self.fetched.add(mint)
        if len(self.fetched) > 50000:
            self.fetched = set(list(self.fetched)[-20000:])
        try:
            self._queue().put_nowait((mint, label))
        except asyncio.QueueFull:
            pass

    async def _early(self, mint, label):
        key = self._key()
        if not key:
            return
        txs = await WT.fetch_early(await self._sess(), key, mint)
        if not txs:
            return
        t0 = txs[0].get("timestamp") or 0
        buyers, seen = [], set()
        for tx in txs:
            tr = WT._trade_of(tx, mint)
            if not tr or tr[1] != "buy" or tr[0] in seen:
                continue
            seen.add(tr[0])
            buyers.append([tr[0], round((tx.get("timestamp") or t0) - t0), round(tr[2], 3)])
        self._write("smart_early.jsonl", dict(mint=mint, label=label, ts=round(time.time()), t_launch=t0, buyers=buyers))
        win = int(self.cfg().get("smart_early_s", 300))
        for w, s, _ in buyers:
            if s > win:
                continue
            v = self.wallets.setdefault(w, [0, 0, 0])
            v[0 if label else 1] += 1
            v[2] = round(time.time())
        self.stats["smart_bond" if label else "smart_loss"] += 1
        if (self.stats["smart_bond"] + self.stats["smart_loss"]) % 10 == 0:
            self._save_wallets()

    async def _worker(self):
        q = self._queue()
        while True:
            mint, label = await q.get()
            try:
                await self._early(mint, label)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats["errors"] += 1
                self.last_error = f"smart: {e}"[:120]
            await asyncio.sleep(1.0)

    def _sample_losers(self, now):
        """Coins that crossed 44 SOL 6+ h ago and never migrated: a random share become 'did not bond' examples."""
        share = float(self.cfg().get("smart_loser_share", 0.12))
        while self.crossed:
            m, ts = next(iter(self.crossed.items()))
            if now - ts < 6 * 3600:
                break
            self.crossed.popitem(last=False)
            if m not in self.migrated and random.random() < share:
                self._fetch(m, 0)

    # ------------------------------------------------------------------ after the bond
    async def _post_prices(self, now):
        due = [m for m, p in self.post.items() if p["next"] <= now]
        for m in [m for m, p in self.post.items() if now - p["t0"] > 24 * 3600]:
            self.post.pop(m, None)
        s = await self._sess()
        for i in range(0, len(due), 50):
            chunk = due[i:i + 50]
            try:
                async with s.get(JUP.format(ids=",".join(chunk)), timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json(content_type=None) if r.status == 200 else {}
            except Exception as e:
                self.last_error = f"jupiter: {type(e).__name__}"
                continue
            for m in chunk:
                p = self.post.get(m)
                if not p:
                    continue
                dt = (now - p["t0"]) / 60
                p["next"] = now + (120 if dt < 120 else 600)
                px = ((data or {}).get(m) or {}).get("usdPrice")
                if px:
                    self._write("postbond.jsonl", dict(m=m, sym=p["sym"], t0=round(p["t0"]), dt=round(dt, 1),
                                                       usd=px, mc_usd=round(px * SUPPLY)))
                    self.stats["post_rows"] += 1
            await asyncio.sleep(0.5)

    # ------------------------------------------------------------------ loop
    async def run(self):
        workers = [asyncio.create_task(self._worker())]
        try:
            while True:
                await asyncio.sleep(20)
                if not self.enabled():
                    continue
                now = time.time()
                try:
                    self._sample_losers(now)
                    for m in [m for m, r in self.rockets.items()
                              if now - r["t0"] > float(self.cfg().get("rocket_window_min", 30)) * 60 + 60]:
                        self._finish_rocket(m)              # no trades lately: close the path anyway
                    await self._post_prices(now)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.stats["errors"] += 1
                    self.last_error = f"loop: {e}"[:120]
        finally:
            for w in workers:
                w.cancel()
            self._save_wallets()

    def summary(self):
        return dict(**self.stats, wallets=len(self.wallets), smart_wallets=len(self._smart_cache()),
                    following_post=len(self.post), recording_rockets=len(self.rockets),
                    fetches_today=self.fetches, last_error=self.last_error)
