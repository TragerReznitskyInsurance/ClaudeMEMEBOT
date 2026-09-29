"""
Narrative tracker: which memecoin themes are being launched, and which are actually taking off.

Inputs (all already arriving, no extra cost):
  - every pump.fun launch (name + ticker)            -> launches per theme
  - every graduation (PumpPortal migration event)    -> graduations per theme, for ALL coins
  - coins the bot watches reaching 44 / 80 SOL       -> "take-off" rate among watched coins

Themes = a few hand-made groups (cat, dog, frog, AI, politics...) plus any single word.
Counts are kept in hourly buckets (data/narratives.json, 72 h) so the tracker survives restarts.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
from collections import defaultdict

import aiohttp

from memebot.chain import curve_address, parse_curve

log = logging.getLogger("memebot")

GROUPS = {
    "cats": ["cat", "cats", "kitty", "kitten", "meow", "neko", "catcoin", "popcat", "mew"],
    "dogs": ["dog", "dogs", "doge", "inu", "shiba", "pup", "puppy", "wif", "bonk", "doggo"],
    "frogs / pepe": ["pepe", "frog", "frogs", "kek", "ribbit", "toad"],
    "AI / agents": ["ai", "agent", "agents", "gpt", "agi", "bot", "robot", "neural", "llm", "grok", "claude", "openai"],
    "politics": ["trump", "maga", "biden", "kamala", "vance", "president", "elect", "america", "usa", "patriot"],
    "Elon / X": ["elon", "musk", "tesla", "spacex", "doge", "xai"],
    "monkeys / apes": ["ape", "apes", "monkey", "chimp", "gorilla", "bape"],
    "anime / waifu": ["anime", "waifu", "chan", "kun", "sama", "senpai"],
    "money / pump": ["money", "cash", "rich", "million", "billion", "pump", "moon", "gains", "lambo"],
    "celebrities / meme people": ["drake", "kanye", "ye", "taylor", "swift", "mrbeast", "tate", "diddy"],
    "crypto meta": ["sol", "solana", "bitcoin", "btc", "eth", "coin", "token", "crypto", "memecoin", "degen"],
    "food": ["burger", "pizza", "taco", "burrito", "sushi", "cookie", "banana", "apple", "coffee"],
}
STOP = {"the", "and", "coin", "token", "of", "on", "in", "to", "is", "it", "my", "a", "an", "be", "we", "are", "not",
        "for", "you", "this", "that", "with", "fun", "pump", "official", "just", "new", "first", "real", "og", "by",
        "me", "i", "no", "yes", "all", "one", "big", "little", "super", "mr", "de", "la", "el", "le"}
WORD_TO_GROUP = {w: g for g, ws in GROUPS.items() for w in ws}
HOUR = 3600
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"
ENTRY_MCAP = 44.0                 # our strategies' entry level
X3_MCAP = ENTRY_MCAP * 3          # "3x" = 44 -> 132 SOL
X6_MCAP = ENTRY_MCAP * 6          # "6x" = 44 -> 264 SOL
TRACK_H = 6                       # after 44 SOL, keep reading the coin on-chain this long
METRICS = ("hit44", "hit80", "hit3x", "hit6x", "grad")


def words(name: str, symbol: str) -> set[str]:
    text = f"{name} {symbol}"
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)          # CatCoin -> Cat Coin
    out = set()
    for w in re.findall(r"[A-Za-z][A-Za-z0-9]*", text):
        w = w.lower()
        if (len(w) >= 3 or w in WORD_TO_GROUP) and w not in STOP and not w.isdigit():
            out.add(w)
    return out


def themes(name: str, symbol: str) -> tuple[set[str], set[str]]:
    """(groups, words) for one coin."""
    ws = words(name, symbol)
    return {WORD_TO_GROUP[w] for w in ws if w in WORD_TO_GROUP}, ws


class Narratives:
    def __init__(self, path: str, key_getter=lambda: ""):
        self.path = path
        self._key = key_getter
        self.session: aiohttp.ClientSession | None = None
        self.buckets: dict[int, dict] = {}      # hour -> {key: {launch, grad, watched, hit44, hit80, hit3x}}
        self.mints: dict[str, dict] = {}        # mint -> {ts, keys, name, symbol, hits: {field: ts}, peak} (48 h)
        self.last_error = ""
        self.dirty = False
        self.last_save = 0.0
        self._cache, self._cache_ts = None, 0.0
        self._load()

    # ------------------------------------------------------------------ storage
    def _load(self):
        try:
            with open(self.path, encoding="utf-8", errors="replace") as fh:
                s = json.load(fh)
            self.buckets = {int(k): v for k, v in s.get("buckets", {}).items()}
            for m, v in (s.get("mints") or {}).items():
                if isinstance(v, list):                    # older format: [ts, keys]
                    v = dict(ts=v[0], keys=v[1], name="", symbol="", hits={}, peak=0.0)
                self.mints[m] = v
        except (OSError, ValueError):
            pass

    def save(self, force=False):
        now = time.time()
        if not (self.dirty and (force or now - self.last_save > 120)):
            return
        cut = int(now // HOUR) - 72
        self.buckets = {h: b for h, b in self.buckets.items() if h >= cut}
        old = int(now // HOUR) - 24
        for h, b in self.buckets.items():                  # older than a day: one-off words only matter as "seen"
            if h < old:
                for k in [k for k, row in b.items() if k.startswith("w:") and row == {"launch": 1}]:
                    b.pop(k)
        self.mints = {m: v for m, v in self.mints.items() if now - v["ts"] < 48 * HOUR}
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"buckets": self.buckets, "mints": self.mints}, fh)
            os.replace(tmp, self.path)
            self.dirty, self.last_save = False, now
        except OSError as e:
            log.debug("narratives save failed: %s", e)

    def _bump(self, ts, keys, field):
        b = self.buckets.setdefault(int(ts // HOUR), {})
        for k in keys:
            row = b.setdefault(k, {})
            row[field] = row.get(field, 0) + 1
        b.setdefault("*", {})
        b["*"][field] = b["*"].get(field, 0) + 1
        self.dirty = True

    # ------------------------------------------------------------------ events (called by the engine)
    def on_launch(self, mint, name, symbol, ts):
        if not mint or mint in self.mints:
            return
        groups, ws = themes(str(name or ""), str(symbol or ""))
        keys = [f"g:{g}" for g in groups] + [f"w:{w}" for w in ws]
        self.mints[mint] = dict(ts=ts, keys=keys, name=str(name or "")[:40], symbol=str(symbol or "")[:14], hits={}, peak=0.0)
        self._bump(ts, keys, "launch")

    def _event(self, mint, ts, field):
        v = self.mints.get(mint)
        if not v or field in v["hits"]:
            return
        v["hits"][field] = round(ts)
        self._bump(ts, v["keys"], field)

    def on_graduate(self, mint, ts):
        v = self.mints.get(mint)
        if v:
            self._check_levels(mint, v, max(v.get("peak", 0.0), 410.0), ts)   # graduating means it passed 80 / 3x / 6x
        self._event(mint, ts, "grad")

    def on_watched(self, mint, ts):
        self._event(mint, ts, "watched")

    def on_mcap(self, mint, prev, now, ts):
        v = self.mints.get(mint)
        if not v or not now:
            return
        v["peak"] = max(v.get("peak", 0.0), now)
        if prev and prev < ENTRY_MCAP <= now:
            self._event(mint, ts, "hit44")
        self._check_levels(mint, v, now, ts)

    def _check_levels(self, mint, v, mcap, ts):
        if mcap >= 80 and "watched" in v["hits"]:          # a share of watched coins, so only count those
            self._event(mint, ts, "hit80")
        if mcap >= X3_MCAP and "hit44" in v["hits"]:
            self._event(mint, ts, "hit3x")
        if mcap >= X6_MCAP and "hit44" in v["hits"]:
            self._event(mint, ts, "hit6x")

    # ------------------------------------------------------------------ on-chain follow-up after 44 SOL
    async def track(self):
        """Coins that passed 44 SOL keep being read from their curve (batched) for TRACK_H hours,
        so runs that happen after the bot stopped watching still count."""
        key = self._key()
        now = time.time()
        todo = [m for m, v in self.mints.items() if "hit44" in v["hits"] and "grad" not in v["hits"]
                and "hit6x" not in v["hits"] and now - v["hits"]["hit44"] < TRACK_H * HOUR]
        if not key or not todo:
            return
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        for i in range(0, len(todo), 100):
            chunk = todo[i:i + 100]
            try:
                addrs = [curve_address(m) for m in chunk]
            except ValueError:
                continue
            async with self.session.post(RPC.format(key=key), json={"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                                                                   "params": [addrs, {"encoding": "base64", "commitment": "confirmed"}]},
                                         timeout=aiohttp.ClientTimeout(total=15)) as r:
                j = await r.json(content_type=None)
            for m, acc in zip(chunk, (j.get("result") or {}).get("value") or []):
                if not acc:
                    continue
                data = base64.b64decode(acc["data"][0])
                v = self.mints[m]
                if len(data) >= 49 and data[48]:
                    self._check_levels(m, v, max(v.get("peak", 0.0), 410.0), now)   # graduating passes 80 / 3x / 6x
                    self._event(m, now, "grad")            # curve completed: graduated
                    continue
                px = parse_curve(data)
                if px:
                    mc = px * 1e9
                    v["peak"] = max(v.get("peak", 0.0), mc)
                    self._check_levels(m, v, mc, now)

    async def run(self, every=30):
        while True:
            await asyncio.sleep(every)
            try:
                await self.track()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e) or type(e).__name__

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def coins(self, key=None, metric=None, limit=200):
        """Coins (last 48 h) that hit a milestone, optionally for one theme / one milestone. Newest first."""
        out = []
        for m, v in self.mints.items():
            if key and key not in v["keys"]:
                continue
            hits = {k: t for k, t in v["hits"].items() if k in METRICS}
            if metric and metric not in hits:
                continue
            if not hits and not (key and metric == "launch"):
                continue
            out.append(dict(mint=m, name=v.get("name", ""), symbol=v.get("symbol", ""), launched=round(v["ts"]),
                            hits=hits, peak_mcap=round(v.get("peak", 0.0), 1),
                            last=max(hits.values()) if hits else v["ts"],
                            themes=[k[2:] for k in v["keys"] if k.startswith("g:")]))
        out.sort(key=lambda x: -x["last"])
        return out[:limit]

    # ------------------------------------------------------------------ report
    def _sum(self, h_from, h_to):
        out = defaultdict(lambda: defaultdict(int))
        for h, b in self.buckets.items():
            if h_from <= h < h_to:
                for k, row in b.items():
                    for f, n in row.items():
                        out[k][f] += n
        return out

    def state(self, top=14):
        if self._cache is not None and time.time() - self._cache_ts < 30:
            return self._cache
        self._cache, self._cache_ts = self._compute(top), time.time()
        return self._cache

    def _compute(self, top):
        now_h = int(time.time() // HOUR)
        last1, last6, last24 = self._sum(now_h, now_h + 1), self._sum(now_h - 5, now_h + 1), self._sum(now_h - 23, now_h + 1)
        prev24 = self._sum(now_h - 47, now_h - 23)
        hours_seen = max(1, len([h for h in self.buckets if now_h - 23 <= h <= now_h]))
        prev_hours = len([h for h in self.buckets if now_h - 47 <= h < now_h - 23])   # "new" needs a yesterday to compare
        tot24 = last24.get("*", {})
        base_grad = tot24.get("grad", 0) / tot24["launch"] if tot24.get("launch") else None
        base_80 = tot24.get("hit80", 0) / tot24["watched"] if tot24.get("watched") else None

        def row(k):
            a, s6, h1, p = last24.get(k, {}), last6.get(k, {}), last1.get(k, {}), prev24.get(k, {})
            launches = a.get("launch", 0)
            per_h_24 = launches / hours_seen
            per_h_6 = s6.get("launch", 0) / min(6, hours_seen)
            grad_rate = a.get("grad", 0) / launches if launches else 0.0
            w = a.get("watched", 0)
            return dict(key=k, theme=k[2:], kind="group" if k.startswith("g:") else "word",
                        launches_24h=launches, launches_1h=h1.get("launch", 0), launches_6h=s6.get("launch", 0),
                        share_pct=round(launches / tot24["launch"] * 100, 1) if tot24.get("launch") else 0.0,
                        heat=round(per_h_6 / per_h_24, 2) if per_h_24 else None,
                        grads_24h=a.get("grad", 0), grad_rate_pct=round(grad_rate * 100, 2),
                        grad_vs_avg=round(grad_rate / base_grad, 2) if base_grad else None,
                        watched_24h=w, hit80_24h=a.get("hit80", 0),
                        hit80_rate_pct=round(a.get("hit80", 0) / w * 100, 1) if w else None,
                        hit44_24h=a.get("hit44", 0), hit3x_24h=a.get("hit3x", 0),
                        hit3x_rate_pct=round(a.get("hit3x", 0) / a["hit44"] * 100, 1) if a.get("hit44") else None,
                        hit6x_24h=a.get("hit6x", 0),
                        hit6x_rate_pct=round(a.get("hit6x", 0) / a["hit44"] * 100, 1) if a.get("hit44") else None,
                        new=prev_hours >= 6 and p.get("launch", 0) <= 1 and s6.get("launch", 0) >= 5)

        keys = [k for k in last24 if k != "*"]
        groups = sorted((row(k) for k in keys if k.startswith("g:")), key=lambda r: -r["launches_24h"])
        wrows = [row(k) for k in keys if k.startswith("w:") and last24[k].get("launch", 0) >= 8]
        # "taking off": enough launches to mean something, ranked by graduations vs the average coin
        hot = sorted([r for r in wrows + groups if r["launches_24h"] >= 15 and r["grads_24h"] >= 2],
                     key=lambda r: -(r["grad_vs_avg"] or 0))[:top]
        rising = sorted([r for r in wrows if r["launches_6h"] >= 5 and (r["heat"] or 0) >= 1.5],
                        key=lambda r: -(r["heat"] or 0))[:top]
        busiest = sorted(wrows, key=lambda r: -r["launches_24h"])[:top]
        return dict(
            computed_at=round(time.time()),
            hours=hours_seen, launches_24h=tot24.get("launch", 0), grads_24h=tot24.get("grad", 0),
            hit44_24h=tot24.get("hit44", 0), hit80_24h=tot24.get("hit80", 0), hit3x_24h=tot24.get("hit3x", 0),
            hit6x_24h=tot24.get("hit6x", 0),
            base_hit3x_rate_pct=round(tot24.get("hit3x", 0) / tot24["hit44"] * 100, 1) if tot24.get("hit44") else None,
            launches_1h=last1.get("*", {}).get("launch", 0),
            base_grad_rate_pct=round(base_grad * 100, 2) if base_grad is not None else None,
            base_hit80_rate_pct=round(base_80 * 100, 1) if base_80 is not None else None,
            groups=groups, hot=hot, rising=rising, busiest=busiest,
            new=[r for r in wrows if r["new"]][:top],
        )

    def themes_of(self, mint):
        v = self.mints.get(mint)
        return v["keys"] if v else []
