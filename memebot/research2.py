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

4. BIG CAPS (22 Oct) - every bonded pump.fun coin (and any graduated coin pump.fun's lists show at $150K+) gets
   DexScreener's numbers every minute while it's worth $50K+ (for up to 3 days): market cap, liquidity, 5 min / 1 h /
   24 h volume, buys/sells, price change - to test "buy coins whose volume spikes at $150K+ and catch the runners".
     data/bigcap.jsonl        {m, ts, mc, liq, v5, v1h, v24, b5, s5, b1h, s1h, pc5, pc1h, dex}

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
DEXS = "https://api.dexscreener.com/tokens/v1/solana/{ids}"
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
        self.big: dict[str, dict] = {}                 # mint -> {t0, last_mc, low_n} (big-cap volume tracking)
        self.paper = None                              # BigCapPaper (paper test fed by the big-cap snapshots)
        self.fetched: set[str] = set()
        self.day, self.fetches = "", 0
        self.wallets: dict[str, list] = {}             # wallet -> [bond_hits, loss_hits, last_ts]  (first ~30 s)
        self.mid: dict[str, list] = {}                 # wallet -> [bond_hits, loss_hits, coins]  (after 44 SOL, 30 min)
        self.pending_buyers: dict[str, list] = {}      # mint -> wallets that bought it after 44 SOL (until resolved)
        self.stats = dict(smart_bond=0, smart_loss=0, smart_signals=0, post_coins=0, post_rows=0, rockets=0, errors=0)
        self.last_error = ""
        try:
            with open(os.path.join(data_dir, "smart_wallets.json"), encoding="utf-8") as fh:
                _w = json.load(fh) or {}
                self.wallets = _w.get("wallets") or {}
                self.mid = _w.get("mid") or {}
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
                if len(self.mid) > 80000:
                    self.mid = {w: v for w, v in self.mid.items() if v[0] + v[1] > 1}
                json.dump(dict(updated=time.time(), wallets=self.wallets, mid=self.mid), fh, separators=(",", ":"))
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
        # 22 Oct: scored on MID-stage buys (bought after the coin crossed 44 SOL, seen live) - the first-100-tx data
        # only covers a coin's first ~30 s (launch snipers/bots). Wallets in 15%+ of all coins are bots: excluded.
        n_coins = max(1, int(self.stats.get("mid_resolved", 0)) or sum(v[0] + v[1] for v in self.mid.values()) // 20)
        return {w for w, v in self.mid.items() if v[0] >= nb and v[0] / max(1, v[0] + v[1]) >= share
                and v[0] + v[1] < max(10, 0.15 * n_coins)}

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
        self.big.setdefault(mint, dict(t0=ts, mig=ts, last_mc=None, low_n=0))
        self._fetch(mint, 1)
        if (mint in self.crossed or mint in self.pending_buyers) and mint not in self.rockets:
            self._resolve(mint, 1)                      # (still recording: _finish_rocket resolves it with its buyers)

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
                b = r.setdefault("buyers", {})
                if trader not in b and (sol or 0) >= 0.05 and len(b) < 400:
                    b[trader] = [round(ts - r["t0"]), round(mcap, 1), round(sol, 3)]
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
        buyers = r.get("buyers") or {}
        self._write("rocket_paths.jsonl", dict(m=mint, sym=r["sym"], t0=round(r["t0"], 1), age_s=r["age_s"],
                                               mc0=r["mc0"], path=r["path"],
                                               buyers=[[w] + v for w, v in buyers.items()]))
        self.stats["rockets"] += 1
        if mint in self.migrated:
            self.pending_buyers[mint] = list(buyers)
            self._resolve(mint, 1)
        else:
            self.pending_buyers[mint] = list(buyers)

    def _resolve(self, mint, bonded):
        """Outcome of a 44-SOL crosser: bonded (migrated) or not within 6 h. Scores its mid-stage buyers."""
        if not self.__dict__.setdefault("_resolved", set()).isdisjoint({mint}):
            return
        self._resolved.add(mint)
        if len(self._resolved) > 50000:
            self._resolved = set(list(self._resolved)[-20000:])
        self._write("rocket_outcomes.jsonl", dict(m=mint, bonded=bonded, ts=round(time.time())))
        for w in self.pending_buyers.pop(mint, []) or []:
            v = self.mid.setdefault(w, [0, 0, 0])
            v[0 if bonded else 1] += 1
        self.stats["mid_resolved"] = self.stats.get("mid_resolved", 0) + 1
        self.stats["mid_bonded"] = self.stats.get("mid_bonded", 0) + bonded
        if self.stats["mid_resolved"] % 20 == 0:
            self._save_wallets()

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
            if m not in self.migrated:
                if m in self.pending_buyers:
                    self._resolve(m, 0)
                if random.random() < share:
                    self._fetch(m, 0)

    # ------------------------------------------------------------------ big caps (volume)
    def add_explore(self, coins, now):
        """Explore listener: graduated coins pump.fun lists at $150K+ join the big-cap tracking."""
        if not self.enabled():
            return
        for c in coins:
            m = c.get("mint")
            if m and c.get("complete") and (c.get("mcap_usd") or 0) >= 150000 and m not in self.big and len(self.big) < 600:
                self.big[m] = dict(t0=now, mig=None, last_mc=c.get("mcap_usd"), low_n=0)   # bonded before we saw it

    async def _bigcap(self, now):
        if now - self.__dict__.get("_big_ts", 0) < float(self.cfg().get("bigcap_every_s", 60)):
            return
        self._big_ts = now
        held = (lambda m: self.paper is not None and self.paper.holding(m))
        for m in [m for m, b in self.big.items() if (now - b["t0"] > 3 * 86400 or b["low_n"] >= 3) and not held(m)]:
            self.big.pop(m, None)
        if self.paper is not None:
            for m in list(self.paper.positions):
                self.big.setdefault(m, dict(t0=now, last_mc=None, low_n=0))
        # 23 Oct: every minute only while worth $100K+ (or held by the paper test), every 5 min below that - the file
        # was growing ~170 MB a day; dead coins (under $30K for 3 checks, after their first 10 min) drop off faster
        ids = [m for m, b in self.big.items() if now >= b.get("next", 0)]
        s = await self._sess()
        for i in range(0, len(ids), 30):
            chunk = ids[i:i + 30]
            try:
                async with s.get(DEXS.format(ids=",".join(chunk)), timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json(content_type=None) if r.status == 200 else []
            except Exception as e:
                self.last_error = f"dexscreener: {type(e).__name__}"
                continue
            best = {}
            for p in data if isinstance(data, list) else []:
                m = ((p.get("baseToken") or {}).get("address"))
                if m in self.big and ((p.get("liquidity") or {}).get("usd") or 0) >= ((best.get(m) or {}).get("liquidity") or {}).get("usd", -1):
                    best[m] = p
            for m in chunk:
                b, p = self.big.get(m), best.get(m)
                if not b:
                    continue
                if not p:
                    b["low_n"] += 1 if now - b["t0"] > 1800 else 0
                    continue
                mc = p.get("marketCap") or p.get("fdv") or 0
                b["last_mc"] = mc
                b["low_n"] = b["low_n"] + 1 if mc < 30000 and now - b["t0"] > 600 else 0
                b["next"] = now + (55 if mc >= 100000 or held(m) else 295)
                v, tx, pc = p.get("volume") or {}, p.get("txns") or {}, p.get("priceChange") or {}
                row = dict(m=m, ts=round(now), mc=round(mc), liq=round((p.get("liquidity") or {}).get("usd") or 0),
                           v5=round(v.get("m5") or 0), v1h=round(v.get("h1") or 0), v24=round(v.get("h24") or 0),
                           b5=(tx.get("m5") or {}).get("buys"), s5=(tx.get("m5") or {}).get("sells"),
                           b1h=(tx.get("h1") or {}).get("buys"), s1h=(tx.get("h1") or {}).get("sells"),
                           pc5=pc.get("m5"), pc1h=pc.get("h1"), dex=p.get("dexId"),
                           bond_h=round((now - b["mig"]) / 3600, 2) if b.get("mig") else None)
                try:
                    row["px"] = float(p.get("priceUsd") or 0) or None
                except (TypeError, ValueError):
                    row["px"] = None
                self._write("bigcap.jsonl", row)
                if self.paper is not None:
                    try:
                        self.paper.on_snapshot(m, row, row["px"], (p.get("baseToken") or {}).get("symbol"), now)
                    except Exception as e:
                        self.last_error = f"bigcap paper: {e}"[:120]
                self.stats["big_rows"] = self.stats.get("big_rows", 0) + 1
            await asyncio.sleep(0.3)

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
                    await self._bigcap(now)
                    if self.paper is not None:
                        self.paper.tick(now)
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
                    following_post=len(self.post), recording_rockets=len(self.rockets), bigcap_tracking=len(self.big),
                    fetches_today=self.fetches, last_error=self.last_error)
