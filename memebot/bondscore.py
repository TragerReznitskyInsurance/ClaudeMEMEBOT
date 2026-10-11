"""
Chance a pump.fun coin still on the bonding curve bonds (reaches ~400 SOL / migrates) within the next 6 hours.

Built 11 Oct from 7 days of pump.fun Explore snapshots (2,302 observations of coins 15+ min old at 30-375 SOL;
label = seen complete or at 400+ SOL within 6 h). Market cap is by far the strongest factor, age second;
how close the coin is to its all-time high made little difference to the bond chance itself (it matters for
how much our trades make, not for whether the coin bonds). Holdout check (first half -> second half) was
within a few points per band. A rough guide, not a promise.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time

log = logging.getLogger("memebot")

# (mcap low SOL, mcap high SOL) -> chance for age 15 min-2 h, 2-24 h, 1 day+
TABLE = [
    ((30, 60), (0.012, 0.014, 0.009)),
    ((60, 90), (0.051, 0.021, 0.044)),
    ((90, 130), (0.108, 0.032, 0.029)),
    ((130, 180), (0.206, 0.138, 0.091)),
    ((180, 250), (0.354, 0.308, 0.281)),
    ((250, 375), (0.537, 0.500, 0.370)),
]
# what a loser costs us on average (hold tests: 6 h time limit -14%, -50% stops -58%)
AVG_LOSS = 0.32


PRIOR_N = 100          # the starting table counts as this many observations per cell; live results move it from there
TRACKER = None         # the running BondTracker (its learned table is used once it has data)


def cell(mcap_sol, age_min):
    """(row, col) of the table, or None outside 30-375 SOL."""
    if mcap_sol is None or age_min is None:
        return None
    col = 0 if age_min < 120 else 1 if age_min < 1440 else 2
    for i, ((lo, hi), _) in enumerate(TABLE):
        if lo <= mcap_sol < hi:
            return i, col
    return None


def bond_chance(mcap_sol, age_min):
    """0-1, or None when it doesn't apply (graduated / unknown). Uses the live-learned table when available."""
    if mcap_sol is None or age_min is None or mcap_sol >= 400:
        return None
    if mcap_sol >= 375:
        return 0.75
    c = cell(mcap_sol, age_min)
    if c is None:
        return 0.005                                        # under 30 SOL: launch level
    if TRACKER is not None:
        return TRACKER.learned(*c)
    return TABLE[c[0]][1][c[1]]


def breakeven_chance(mcap_sol, exit_sol=375.0):
    """Bond chance needed for a hold-to-bonding buy at this mcap to break even (payout vs average loser)."""
    if not mcap_sol or mcap_sol >= exit_sol:
        return None
    gain = exit_sol / mcap_sol - 1
    return AVG_LOSS / (gain + AVG_LOSS)


PRED_BINS = [(0, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.35), (0.35, 0.50), (0.50, 1.01)]


class BondTracker:
    """Keeps score on the bond chance (11 Oct): every ~5 min it takes the coins the hold tests follow (15+ min old,
    30-375 SOL, on the curve) and records the predicted chance - at most once an hour per coin. Six hours later it
    checks on-chain whether the coin bonded (curve complete or 400+ SOL). Each result updates its table cell
    (starting table = PRIOR_N observations, so a cell moves as real results pile up), plus a running tally:
    predicted vs actual per chance band and the Brier score (lower = better) vs always guessing the average.
    Files: data/bond_score.json (tally + pending checks), data/bond_predictions.jsonl (every prediction + result)."""
    HORIZON = 6 * 3600

    def __init__(self, data_dir, sources, rpc_owner):
        self.path = os.path.join(data_dir, "bond_score.json")
        self.log_path = os.path.join(data_dir, "bond_predictions.jsonl")
        self.sources = sources              # callable -> list of strategies with .cands
        self.rpc_owner = rpc_owner          # a strategy with ._rpc / ._key (Helius)
        self.pending, self.cells, self.bins, self.last_pred = [], {}, {}, {}
        self.resolved = self.hits = 0
        self.brier = self.brier_base = 0.0
        self.last_error = ""
        self._load()

    # ---------------------------------------------------------------- persistence
    def _load(self):
        try:
            with open(self.path, encoding="utf-8") as fh:
                d = json.load(fh)
            self.pending = d.get("pending") or []
            self.cells = {k: list(v) for k, v in (d.get("cells") or {}).items()}
            self.bins = {k: list(v) for k, v in (d.get("bins") or {}).items()}
            self.resolved, self.hits = int(d.get("resolved", 0)), int(d.get("hits", 0))
            self.brier, self.brier_base = float(d.get("brier", 0)), float(d.get("brier_base", 0))
            self.last_pred = {p["m"]: p["ts"] for p in self.pending}
        except (OSError, ValueError):
            pass

    def _save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(dict(pending=self.pending[-30000:], cells=self.cells, bins=self.bins, resolved=self.resolved,
                               hits=self.hits, brier=self.brier, brier_base=self.brier_base), fh)
            os.replace(tmp, self.path)
        except OSError as e:
            self.last_error = str(e)[:80]

    def _log(self, rec):
        try:
            if os.path.exists(self.log_path) and os.path.getsize(self.log_path) > 30_000_000:
                os.replace(self.log_path, self.log_path + ".1")
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except OSError:
            pass

    # ---------------------------------------------------------------- learned table
    def learned(self, i, j):
        n, h = self.cells.get(f"{i},{j}", (0, 0))
        prior = TABLE[i][1][j]
        return (PRIOR_N * prior + h) / (PRIOR_N + n)

    # ---------------------------------------------------------------- loop
    def sample(self, now=None):
        now = now or time.time()
        n = 0
        for src in self.sources():
            for m, cd in list(getattr(src, "cands", {}).items()):
                mc, created = cd.get("last_mc"), cd.get("created")
                if not mc or not created or cd.get("src") in ("jupiter", "mayhem") or not 30 <= mc < 375:
                    continue
                age = (now - created) / 60
                c = cell(mc, age)
                if age < 15 or c is None or now - self.last_pred.get(m, 0) < 3600:
                    continue
                p = self.learned(*c)
                self.last_pred[m] = now
                self.pending.append(dict(m=m, ts=round(now), mc=round(mc, 1), age=round(age), c=list(c), p=round(p, 4)))
                n += 1
        if len(self.last_pred) > 50000:
            self.last_pred = {m: t for m, t in self.last_pred.items() if now - t < 7200}
        return n

    async def _status(self, mints):
        """{mint: True (bonded) / False (still on the curve under 400 SOL)}; missing = couldn't tell."""
        from memebot.chain import curve_address, parse_curve
        out, curves = {}, {}
        for m in mints:
            try:
                curves[m] = curve_address(m)
            except ValueError:
                continue
        need = list(curves)
        for i in range(0, len(need), 100):
            chunk = need[i:i + 100]
            r = await self.rpc_owner._rpc("getMultipleAccounts", [[curves[m] for m in chunk],
                                                                 {"encoding": "base64", "commitment": "confirmed"}])
            for m, acc in zip(chunk, (r or {}).get("value") or []):
                data = base64.b64decode(acc["data"][0]) if acc else b""
                if len(data) < 49:
                    continue
                if data[48]:
                    out[m] = True                          # curve complete: bonded / migrated
                    continue
                px = parse_curve(data)
                if px:
                    out[m] = px * 1_000_000_000 >= 400
        return out

    def record(self, p, y):
        y = 1 if y else 0
        k = f"{p['c'][0]},{p['c'][1]}"
        n, h = self.cells.get(k, (0, 0))
        self.cells[k] = [n + 1, h + y]
        b = next(f"{lo:g}-{hi:g}" for lo, hi in PRED_BINS if lo <= p["p"] < hi)
        bn = self.bins.get(b, [0, 0.0, 0])
        self.bins[b] = [bn[0] + 1, bn[1] + p["p"], bn[2] + y]
        self.resolved += 1
        self.hits += y
        self.brier += (p["p"] - y) ** 2
        self._log(dict(p, y=y, resolved=round(time.time())))

    async def resolve(self, now=None):
        now = now or time.time()
        due = [p for p in self.pending if now - p["ts"] >= self.HORIZON]
        if not due:
            return 0
        st = await self._status(list({p["m"] for p in due}))
        keep, done = [], 0
        for p in self.pending:
            if now - p["ts"] < self.HORIZON:
                keep.append(p)
            elif p["m"] in st:
                self.record(p, st[p["m"]])
                done += 1
            elif p.get("tries", 0) < 3:
                p["tries"] = p.get("tries", 0) + 1
                keep.append(p)
        self.pending = keep
        if self.resolved:                                   # baseline: always guess the overall average
            base = self.hits / self.resolved
            self.brier_base = base * (1 - base) * self.resolved
        return done

    async def run(self, every=300):
        await asyncio.sleep(60)
        while True:
            try:
                self.sample()
                if self.rpc_owner._key():
                    await self.resolve()
                self.last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = (str(e) or type(e).__name__)[:100]
                log.info("bond score: %s", self.last_error)
            self._save()
            await asyncio.sleep(every)

    def state(self):
        rows = []
        for lo, hi in PRED_BINS:
            n, sp, h = self.bins.get(f"{lo:g}-{hi:g}", (0, 0.0, 0))
            if n:
                rows.append(dict(band=f"{lo * 100:g}–{min(hi, 1) * 100:g}%", n=n, predicted=round(sp / n * 100, 1),
                                 actual=round(h / n * 100, 1)))
        table = []
        for i, ((lo, hi), prior) in enumerate(TABLE):
            table.append(dict(mcap=f"{lo}–{hi}", cells=[dict(start=round(prior[j] * 100, 1), now=round(self.learned(i, j) * 100, 1),
                                                              n=self.cells.get(f"{i},{j}", (0, 0))[0]) for j in range(3)]))
        skill = (1 - self.brier / self.brier_base) * 100 if self.brier_base else None
        return dict(resolved=self.resolved, pending=len(self.pending), bonded=self.hits,
                    brier=round(self.brier / self.resolved, 4) if self.resolved else None,
                    skill_pct=round(skill, 1) if skill is not None else None, bands=rows, table=table,
                    error=self.last_error)
