"""
"Range breakout" PAPER test - a coin that traded sideways for a while, then breaks out of the top of the range.

Candidates: coins the bot watched that reached `run_mcap_sol` (50 SOL) - i.e. got some real interest - then
followed on-chain (pump.fun curve, or Jupiter once graduated) for up to `max_age_h`, sampling the market cap
every ~20 s.

Consolidation: over the last `consol_min` (20) minutes (not counting the last 90 s) the market cap stayed in a
range no wider than `max_range_pct` (35%) between its low and high, above `min_entry_mcap_sol`.
Breakout (buy, paper): the coin is now at least `break_pct` (5%) above the top of that range, but not more than
`max_chase_pct` (40%) above it (don't chase a candle that already ran), coin at least `min_age_min` old, and -
for coins still on the curve - at least `min_trades_2m` trades in the last 2 minutes (real buying, not one wallet).

Sell:
  - failed breakout: back below the middle of the range -> sell everything
  - hard stop -`stop_pct` (25%)
  - sell `tp_frac_pct` (1/3) at `tp_mult` (2x)
  - once up `trail_arm_mult` (1.5x): trailing stop `trail_pct` (30%) below the high
  - time limit `max_hold_h` (24h)
Never real money.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time

import aiohttp

from memebot.bondscore import bond_chance, breakeven_chance
from memebot.lookalike import curve_buy
from memebot.reclaim import Reclaim
from memebot.security import RUGCHECK_URL, judge

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000


class RangeBreak(Reclaim):
    NAME = "rangebreak"

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    # ------------------------------------------------------------------ entry
    def _range(self, cd, now, c):
        """(low, high) of the consolidation window, or None if there isn't enough history yet."""
        win = float(c.get("consol_min", 20)) * 60
        skip = float(c.get("recent_s", 90))
        pts = [mc for ts, mc in cd.get("hist", []) if now - skip - win <= ts <= now - skip]
        h = cd.get("hist") or []
        if len(pts) < 8 or not h or now - h[0][0] < win + skip - 30:
            return None
        return min(pts), max(pts)

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        usd = self._usd()
        if not usd:
            return
        self.traded.add(cd["mint"])
        if len(self.positions) >= int(c.get("max_open", 100)):
            return self._event("skip", f"{cd['symbol']}: max {c.get('max_open', 100)} paper positions open", mint=cd["mint"])
        lo, hi = cd["range"]
        size = float(c.get("size_usd", 25)) / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), px) * (1 - slip)
        mc = px * SUPPLY
        self.positions[cd["mint"]] = dict(
            mint=cd["mint"], symbol=cd["symbol"], name=cd.get("name", ""), opened=ts, entry_px=px, entry_mcap=round(mc, 1),
            age_at_entry_s=round(ts - cd["created"]), sol_in=size + prio, sol_out=0.0, tokens=tokens, tokens_bought=tokens,
            peak_mult=1.0, floor_mult=0.0, done=[], last_px=px, last_px_ts=ts, sells=[], verified=True,
            size_usd=round(size * usd, 2), range_lo=round(lo, 1), range_hi=round(hi, 1), trades_2m=trades_2m,
            graduated=cd.get("src") == "jupiter", prior_peak=round(max(cd.get("peak") or 0, mc), 1),
            pct_of_ath=round(mc / max(cd.get("peak") or mc, mc) * 100, 1), late=bool(cd.get("late")))
        rng = (hi / lo - 1) * 100
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), cd["mint"], cd["symbol"], "BUY",
                   f"breakout at {mc:.0f} SOL from a {lo:.0f}-{hi:.0f} SOL range ({rng:.0f}% wide), "
                   f"{mc / max(cd.get('peak') or mc, mc) * 100:.0f}% of ATH {max(cd.get('peak') or mc, mc):.0f}"
                   + (" · late riser" if cd.get("late") else ""), "1.00",
                   round(size + prio, 6), round(tokens, 2)])
        self._event("buy", f"Paper buy {cd['symbol']} at {mc:.0f} SOL mcap - broke out of a {lo:.0f}-{hi:.0f} SOL range "
                           f"({rng:.0f}% wide, {c.get('consol_min', 20):g} min)"
                           + (f", {trades_2m} trades in 2 min" if trades_2m is not None else ", graduated"), mint=cd["mint"])
        log.info("RANGEBREAK buy %s at mcap %.0f (range %.0f-%.0f)", cd["symbol"], mc, lo, hi)

    # ------------------------------------------------------------------ exits
    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        mc = px * SUPPLY
        if ts - p["opened"] > float(c.get("max_hold_h", 24)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 24):g}h", px, ts)
        mid = (p["range_lo"] + p["range_hi"]) / 2
        if p["peak_mult"] < float(c.get("trail_arm_mult", 1.5)) and mc < mid:
            return self._sell(p, 1.0, "failed breakout (back inside the range)", px, ts)
        if mult <= 1 - float(c.get("stop_pct", 25)) / 100:
            return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 25):g}%", px, ts)
        if p["peak_mult"] >= float(c.get("trail_arm_mult", 1.5)) and mult <= p["peak_mult"] * (1 - float(c.get("trail_pct", 30)) / 100):
            return self._sell(p, 1.0, f"trailing stop ({p['peak_mult']:.1f}x high)", px, ts)
        if mult >= float(c.get("tp_mult", 2)) and "2x" not in p["done"]:
            p["done"].append("2x")
            self._sell(p, float(c.get("tp_frac_pct", 33.3)) / 100, f"{c.get('tp_mult', 2):g}x - sold a third", px, ts)

    # ------------------------------------------------------------------ loop
    async def tick(self):
        if not self._key() or not (self.cands or self.positions):
            return
        c = self.cfg()
        now = time.time()
        px = await self.prices(list(self.cands) + list(self.positions))
        keep_s = (float(c.get("consol_min", 20)) + 3) * 60 + float(c.get("recent_s", 90))
        for m, cd in list(self.cands.items()):
            if m in self.positions or m in self.traded:
                self.cands.pop(m, None)
                continue
            age = now - cd["created"]
            price, src = px.get(m, (None, None))
            if age > float(c.get("max_age_h", 24)) * 3600 or src == "mayhem":
                self.cands.pop(m, None)
                continue
            if not price:
                cd["misses"] = cd.get("misses", 0) + 1
                if cd["misses"] > 60:
                    self.cands.pop(m, None)
                continue
            mc = price * SUPPLY
            last = cd.get("last_mc") or mc
            if not (1 / 3 <= mc / last <= 3):              # confirm big jumps before believing them
                if not (cd.get("suspect") and 0.8 <= mc / cd["suspect"] <= 1.25):
                    cd["suspect"] = mc
                    continue
            cd.pop("suspect", None)
            cd["last_mc"], cd["src"], cd["misses"] = mc, src, 0
            cd["peak"] = max(cd.get("peak") or 0, mc)       # all-time high seen (for the % of ATH at our buy)
            if mc < float(c.get("dead_mcap_sol", 20)):
                self.cands.pop(m, None)
                continue
            h = cd.setdefault("hist", [])
            if not h or now - h[-1][0] >= float(c.get("sample_s", 20)):
                h.append([round(now), round(mc, 1)])
                while h and now - h[0][0] > keep_s:
                    h.pop(0)
            rg = self._range(cd, now, c)
            if not rg:
                continue
            lo, hi = rg
            cd["range"] = [lo, hi]
            if (hi / lo - 1) * 100 > float(c.get("max_range_pct", 35)) or lo < float(c.get("min_entry_mcap_sol", 40)):
                continue
            if not (hi * (1 + float(c.get("break_pct", 5)) / 100) <= mc <= hi * (1 + float(c.get("max_chase_pct", 40)) / 100)):
                continue
            if age < float(c.get("min_age_min", 30)) * 60 or now - cd.get("checked_at", 0) < 20:
                continue
            cd["checked_at"] = now
            n = None
            if src != "jupiter":                            # still on the curve: confirm real buying
                try:
                    n = await self._activity(m)
                except Exception as e:
                    self.last_error = f"activity check: {e}"
                    continue
                cd["trades_2m"], cd["trades_at"] = n, now
                if n < int(c.get("min_trades_2m", 20)):
                    continue
            self._enter(cd, price, now, n)
            self.cands.pop(m, None)
        for m in list(self.positions):
            p = self.positions.get(m)
            price, src = px.get(m, (None, None))
            if p and price and price > 0 and self._accept(p, price, now):
                self.check(p, price, now)
        self._save()

    def watching(self, limit=200):
        """Coins this test follows and what it's waiting for. One reason per coin; coins it can never buy
        (graduated / off the bonding curve, stale prices) are left out."""
        c = self.cfg()
        now = time.time()
        usd = self._usd()
        cap_usd = float(c.get("max_entry_mcap_usd", 0) or 0)
        cap_sol = float(c.get("max_entry_mcap_sol", 0) or 0) if self.NAME != "rangebreak" else 0.0
        min_age = float(c.get("min_age_min", 30))
        out = []
        for m, cd in self.cands.items():
            if m in self.positions or m in self.traded or cd.get("src") == "jupiter":
                continue
            mc = cd.get("last_mc") or 0
            if not mc or mc >= 411:                        # past the bonding level: graduated, not a curve coin any more
                continue
            age = (now - cd["created"]) / 60
            rg = cd.get("range")
            w = (rg[1] / rg[0] - 1) * 100 if rg else None
            sideways = rg and w is not None and 0.5 <= w <= float(c.get("max_range_pct", 35)) \
                and rg[0] >= float(c.get("min_entry_mcap_sol", 40))
            over = ("above the $%gK cap" % (cap_usd / 1000)) if cap_usd and usd and mc * usd >= cap_usd else \
                   ("above the %g SOL limit" % cap_sol) if cap_sol and mc >= cap_sol else ""
            top = rg[1] * (1 + float(c.get("break_pct", 5)) / 100) if rg else 0
            chase = rg[1] * (1 + float(c.get("max_chase_pct", 40)) / 100) if rg else 0
            need = int(c.get("min_trades_2m", 20))
            n2 = cd.get("trades_2m") if now - cd.get("trades_at", 0) < 120 else None
            if sideways and not over and age >= min_age and mc > chase:
                why, rank = (f"Ran past the breakout zone ({mc / rg[1] * 100 - 100:.0f}% above the {rg[0]:.0f}–{rg[1]:.0f} range, "
                             f"limit {c.get('max_chase_pct', 40):g}%) · won't chase", 1)
            elif sideways and not over and age >= min_age and mc >= top:
                why, rank = ((f"Above the breakout level, but only {n2} trades in 2 min (needs {need}+ to buy)" if n2 is not None
                              and n2 < need else f"Breaking out now · checking for {need}+ trades in 2 min"), 0)
            elif sideways and not over and age >= min_age:
                why, rank = f"Sideways {rg[0]:.0f}–{rg[1]:.0f} SOL · buys above {top:.0f} SOL with {need}+ trades in 2 min", 0
            elif sideways and age < min_age:
                why, rank = f"Sideways {rg[0]:.0f}–{rg[1]:.0f} SOL, but too young ({age:.0f} min old, buys from {min_age:g} min)", 1
            elif sideways and over:
                why, rank = f"Sideways {rg[0]:.0f}–{rg[1]:.0f} SOL, but {over}", 1
            elif over:
                why, rank = over[0].upper() + over[1:], 3
            elif age < min_age:
                why, rank = f"Too young ({age:.0f} min old, buys from {min_age:g} min)", 3
            elif not rg:
                why, rank = "Collecting price history", 3
            else:
                why, rank = f"Too choppy ({w:.0f}% range, needs ≤{c.get('max_range_pct', 35):g}%)", 3
            ath = max(cd.get("peak") or 0, mc)
            bc, be = bond_chance(mc, age), breakeven_chance(mc)
            out.append(dict(mint=m, symbol=cd.get("symbol") or m[:5], age_min=round(age), mcap=round(mc, 1),
                            bond_pct=round(bc * 100, 1) if bc is not None else None,
                            breakeven_pct=round(be * 100, 1) if be is not None else None,
                            ath=round(ath, 1), pct_of_ath=round(mc / ath * 100) if ath else None,
                            why=why + (" · late riser" if cd.get("late") else ""), rank=rank, late=bool(cd.get("late"))))
        out.sort(key=lambda x: (x["rank"], -x["mcap"]))
        return out[:limit]

    def state(self):
        s = super().state()
        c = self.cfg()
        ranging = sum(1 for x in self.cands.values() if x.get("range") and
                      (x["range"][1] / x["range"][0] - 1) * 100 <= float(c.get("max_range_pct", 35)))
        s.update(name=self.NAME, pulled=ranging,
                 desc=(f"Coins (50+ SOL at some point) that traded sideways for {c.get('consol_min', 20):g} min in a range "
                       f"≤{c.get('max_range_pct', 35):g}% wide, then break {c.get('break_pct', 5):g}%+ above it with "
                       f"{c.get('min_trades_2m', 20)}+ trades/2 min · ${c.get('size_usd', 25)} each · no real money"))
        return s


class RangeHold(RangeBreak):
    """PAPER test (5 Oct): Range breakout's buys, but HOLD for the bonding level instead of selling on a failed
    breakout. Analysis of 120 on-curve Range breakout trades: 29% reached 375 SOL / migrated within 6 h (median 1.4 h
    after our buy) vs 7.7% of all coins - but 171 of 261 trades had been sold early by the "back inside the range" exit.
    Buys only coins still on the bonding curve and under `max_entry_mcap_sol`.
    Sells: everything at `bond_exit_mcap_sol` (375 SOL) or if it migrates; stop -`stop_pct` (50%); after `max_hold_h` (6h).
    Uses the `rangebreak` settings for buys plus the `rangehold` section; never real money."""
    NAME = "rangehold"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    async def prices(self, mints):
        px = await super().prices(mints)
        self._src = {m: v[1] for m, v in px.items()}
        return px

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        if cd.get("src") == "jupiter" or px * SUPPLY >= float(c.get("max_entry_mcap_sol", 250)):
            self.traded.add(cd["mint"])                    # already graduated / too close to the bonding level
            return
        return super()._enter(cd, px, ts, trades_2m)

    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        p["low_mult"] = min(p.get("low_mult") or mult, mult)  # lowest point while held (research)
        mc = px * SUPPLY
        if mc >= float(c.get("bond_exit_mcap_sol", 375)) or getattr(self, "_src", {}).get(p["mint"]) == "jupiter":
            return self._sell(p, 1.0, f"bonding level ({mc:.0f} SOL) - sold everything", px, ts)
        dead, usd = float(c.get("dead_mcap_usd", 0) or 0), self._usd()
        if dead and usd and mc * usd <= dead:               # back down at launch level: the coin is dead
            return self._sell(p, 1.0, f"fell under ${dead / 1000:g}K mcap (${mc * usd / 1000:.2f}K) - dead zone", px, ts)
        if mult <= 1 - float(c.get("stop_pct", 50)) / 100:
            return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 50):g}%", px, ts)
        if ts - p["opened"] > float(c.get("max_hold_h", 6)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 6):g}h", px, ts)

    @staticmethod
    def _dead_txt(c):
        d = float(c.get("dead_mcap_usd", 0) or 0)
        return f" or below ${d / 1000:g}K mcap" if d else ""

    def state(self):
        s = super().state()
        c = self.cfg()
        since = self.stats_from()
        s["bonded"] = sum(1 for x in self.closed if x["opened"] >= since and str(x.get("exit", "")).startswith("bonding level"))
        lt = [x for x in self.closed if x["opened"] >= since and x.get("late")]
        s["late"] = dict(closed=len(lt), bonded=sum(1 for x in lt if str(x.get("exit", "")).startswith("bonding level")),
                         pnl_usd=round(sum(x.get("pnl_usd") or 0 for x in lt), 2),
                         open=sum(1 for p in self.positions.values() if p.get("late")))
        s.update(name=self.NAME, desc=(f"Range breakout buys (on the curve, under {c.get('max_entry_mcap_sol', 250):g} SOL), "
                                       f"but HOLD: sell everything at {c.get('bond_exit_mcap_sol', 375):g} SOL / migration, "
                                       f"stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, {c.get('max_hold_h', 6):g}h limit · no real money"))
        return s


class RangeHoldSmall(RangeHold):
    """PAPER test (6 Oct): same as hold-to-bonding, but only buys coins under `max_entry_mcap_usd` ($12K, ~100 SOL),
    so the bonding level (375 SOL) is ~4x+ away. Uses rangebreak + rangehold settings plus the `rangehold_small`
    section; never real money."""
    NAME = "rangehold_small"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        usd = self._usd()
        if usd and px * SUPPLY * usd >= float(c.get("max_entry_mcap_usd", 12000)):
            return                                          # too big right now - may still dip back under later
        return super()._enter(cd, px, ts, trades_2m)

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Range breakout buys under ${float(c.get('max_entry_mcap_usd', 12000)) / 1000:g}K mcap, "
                     f"HOLD: sell everything at {c.get('bond_exit_mcap_sol', 375):g} SOL / migration, "
                     f"stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, {c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldV2(RangeHold):
    """PAPER test (6 Oct): hold-to-bonding, but only coins aged `min_age_min`-`max_age_h` at the breakout.
    First 39 hold-to-bonding trades: coins 85-190 min old -> 7 of 21 bonded (+39%/trade); younger or older ->
    1 of 18 bonded (-$176). Uses rangebreak + rangehold settings plus the `rangehold_v2` section; never real money."""
    NAME = "rangehold_v2"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Range breakout buys of coins {c.get('min_age_min', 80):g} min-{c.get('max_age_h', 3.2):g} h old "
                     f"(on the curve, under {c.get('max_entry_mcap_sol', 250):g} SOL), HOLD: sell everything at "
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, "
                     f"{c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldMix(RangeHoldSmall):
    """PAPER test (8 Oct): hybrid of the two hold-to-bonding variants - only coins aged `min_age_min`-`max_age_h`
    (v2's window) AND under `max_entry_mcap_usd` ($12K, the small test's cap). Uses rangebreak + rangehold settings
    plus the `rangehold_mix` section; never real money."""
    NAME = "rangehold_mix"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Range breakout buys of coins {c.get('min_age_min', 80):g} min-{c.get('max_age_h', 3.2):g} h old AND "
                     f"under ${float(c.get('max_entry_mcap_usd', 12000)) / 1000:g}K, HOLD: sell everything at "
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, "
                     f"{c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldSmallThin(RangeHoldSmall):
    """PAPER test (9 Oct): the under-$12K hold test, but it only needs `min_trades_2m` = 10 trades in the last
    2 minutes at the breakout (instead of 20) - small coins have fewer traders. Uses rangebreak + rangehold +
    rangehold_small settings plus the `rangehold_small10` section.
    REAL MONEY (12 Oct, owner's request, off by default): with `real_enabled` on, every paper buy is also bought for
    real in the real wallet - `real_size_usd` ($10) each, at most `real_max_open` (15) open, no new buys after
    `real_daily_loss_usd` ($40) lost today. Each real buy first gets a fresh RugCheck (late risers / coins RugCheck
    couldn't check never had one) and is skipped if it fails or RugCheck doesn't answer. Real sells follow the
    paper exits."""
    NAME = "rangehold_small10"
    REAL_KEYS = ("real_enabled", "real_size_usd", "real_max_open", "real_daily_loss_usd")

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.update(self._cfg().get("rangehold_small") or {})
        c.pop("stats_since", None)
        for k in self.REAL_KEYS:                            # real-money settings only ever come from this test's own section
            c.pop(k, None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = bool(c.get("real_enabled")) and self.NAME == "rangehold_small10"
        return c

    # ------------------------------------------------------------------ real money
    def _enter(self, cd, px, ts, trades_2m):
        had = cd["mint"] in self.positions
        super()._enter(cd, px, ts, trades_2m)
        c = self.cfg()
        if had or cd["mint"] not in self.positions or not c.get("real_enabled") or getattr(self, "live", None) is None:
            return
        try:
            asyncio.get_running_loop().create_task(self._real_buy(cd["mint"], cd["symbol"]))
        except RuntimeError:
            pass

    async def _rugcheck(self, mint):
        """(ok, reason) from a fresh RugCheck report; not ok if it can't be read."""
        sec = self._cfg().get("security") or {}
        why = "RugCheck didn't answer"
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(headers={"User-Agent": "momentum-paper-bot"}) as s:
                    async with s.get(RUGCHECK_URL.format(mint=mint), timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            return judge(await r.json(content_type=None) or {}, sec)
                        why = f"RugCheck HTTP {r.status}"
            except Exception as e:
                why = f"RugCheck {type(e).__name__}"
            await asyncio.sleep(3)
        return False, why

    async def _real_buy(self, mint, symbol):
        c = self.cfg()
        ok, why = await self._rugcheck(mint)
        p = self.positions.get(mint)
        if p is None:
            return                                          # paper already sold while we checked
        if not ok:
            p["real_skip"] = why
            log.info("SMALL10 no real buy %s: %s", symbol, why)
            return self._event("skip", f"{symbol}: no real buy - {why}", mint=mint)
        if p.get("last_px") and p["last_px"] / p["entry_px"] > 1.15:
            p["real_skip"] = "price ran 15%+ while checking"
            log.info("SMALL10 no real buy %s: price ran 15%%+ while checking", symbol)
            return self._event("skip", f"{symbol}: no real buy - price ran 15%+ while RugCheck was checking", mint=mint)
        why = self.live.open_strategy(self.NAME, mint, symbol, float(c.get("real_size_usd", 10)),
                                      int(c.get("real_max_open", 15)), float(c.get("real_daily_loss_usd", 40)))
        if why is None:
            p["real"] = True
            self._event("buy", f"{symbol}: REAL buy placed (${float(c.get('real_size_usd', 10)):g}) · RugCheck passed", mint=mint)
            log.info("SMALL10 REAL buy %s", symbol)
        else:
            p["real_skip"] = why
            log.info("SMALL10 no real buy %s: %s", symbol, why)
            self._event("skip", f"{symbol}: no real buy - {why}", mint=mint)
        self._save()

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Range breakout buys under ${float(c.get('max_entry_mcap_usd', 12000)) / 1000:g}K with only "
                     f"{c.get('min_trades_2m', 10)}+ trades in 2 min (others need 20), HOLD: sell everything at "
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, "
                     f"{c.get('max_hold_h', 6):g}h limit · "
                     + (f"REAL MONEY ON: ${float(c.get('real_size_usd', 10)):g} per buy, max {int(c.get('real_max_open', 15))} open, "
                        f"stops after -${float(c.get('real_daily_loss_usd', 40)):g} a day, RugCheck before every real buy"
                        if c.get("real_enabled") else "no real money"))
        s["real_enabled"] = bool(c.get("real_enabled"))
        s.update(real_size_usd=float(c.get("real_size_usd", 10)), real_max_open=int(c.get("real_max_open", 15)),
                 real_daily_loss_usd=float(c.get("real_daily_loss_usd", 40)),
                 real_open=sum(1 for p in self.positions.values() if p.get("real")),
                 real_today_usd=(round(self.live.realized_today_usd(self.NAME), 2)
                                 if getattr(self, "live", None) is not None else None))
        return s


class RangeHoldFloor(RangeHold):
    """PAPER test (10 Oct): the original hold-to-bonding test, plus a break-even floor - once a coin has reached
    `floor_arm_mult` (2x) it is sold if it falls back to `floor_mult` (1.0x, our buy price). 127 hold trades so far:
    13 coins doubled without bonding and 9 of them still ended at a loss. Uses rangebreak + rangehold settings plus
    the `rangehold_floor` section; never real money."""
    NAME = "rangehold_floor"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def check(self, p, px, ts):
        c = self.cfg()
        mult = px / p["entry_px"]
        peak = max(p["peak_mult"], mult)
        arm, floor = float(c.get("floor_arm_mult", 2.0)), float(c.get("floor_mult", 1.0))
        if peak >= arm and mult <= floor and px * SUPPLY < float(c.get("bond_exit_mcap_sol", 375)):
            p["last_px"], p["last_px_ts"], p["peak_mult"] = px, ts, peak
            return self._sell(p, 1.0, f"back to {floor:g}x after a {peak:.1f}x high (break-even floor)", px, ts)
        return super().check(p, px, ts)

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Same buys as hold-to-bonding (under {c.get('max_entry_mcap_sol', 250):g} SOL); once a coin hits "
                     f"{c.get('floor_arm_mult', 2.0):g}x it sells if it falls back to {c.get('floor_mult', 1.0):g}x · "
                     f"otherwise sell everything at {c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop "
                     f"−{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, {c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldNearATH(RangeHold):
    """PAPER test (11 Oct): the original hold-to-bonding test, but only buys when the breakout price is within
    `max_below_ath_pct` (20%) of the coin's all-time high so far. 63 coins with the ATH recorded at the buy:
    within 20% averaged +2%/trade, further below -22%/trade - better in all six hold tests. Uses rangebreak +
    rangehold settings plus the `rangehold_ath` section; never real money."""
    NAME = "rangehold_ath"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        mc = px * SUPPLY
        ath = max(cd.get("peak") or 0, mc)
        if mc < ath * (1 - float(c.get("max_below_ath_pct", 20)) / 100):
            return                                          # too far under its high right now - may get closer later
        return super()._enter(cd, px, ts, trades_2m)

    def watching(self, limit=200):
        out = super().watching(limit=10_000)
        lim = 100 - float(self.cfg().get("max_below_ath_pct", 20))
        for x in out:
            if x.get("pct_of_ath") is not None and x["pct_of_ath"] < lim and x["rank"] == 0:
                x["why"] += f" · but only {x['pct_of_ath']}% of ATH {x['ath']:.0f} (buys from {lim:g}%)"
                x["rank"] = 1
        out.sort(key=lambda x: (x["rank"], -x["mcap"]))
        return out[:limit]

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Same buys as hold-to-bonding, but only within {c.get('max_below_ath_pct', 20):g}% of the coin's "
                     f"all-time high · sell everything at {c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop "
                     f"−{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, {c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldSweet(RangeHold):
    """PAPER test (11 Oct): hold-to-bonding, but only the "sweet spot" from 164 hold coins - buy at
    `entry_min_sol`-`entry_max_sol` (100-200 SOL) and only when the breakout is `break_pct`-`max_chase_pct`
    (10-20%) above the top of the range. Past data: 21 coins, 7 bonded (33%), +$142 (+$95 without the best).
    Uses rangebreak + rangehold settings plus the `rangehold_sweet` section; never real money."""
    NAME = "rangehold_sweet"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        c.update(self._cfg().get("rangehold") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        mc = px * SUPPLY
        if not float(c.get("entry_min_sol", 100)) <= mc < float(c.get("entry_max_sol", 200)):
            return                                          # outside 100-200 SOL right now - may still fit later
        return super()._enter(cd, px, ts, trades_2m)

    def watching(self, limit=200):
        c = self.cfg()
        lo, hi = float(c.get("entry_min_sol", 100)), float(c.get("entry_max_sol", 200))
        out = super().watching(limit=10_000)
        for x in out:
            if x["rank"] == 0 and not lo <= x["mcap"] < hi:
                x["why"] += f" · but buys only at {lo:g}-{hi:g} SOL"
                x["rank"] = 1
        out.sort(key=lambda x: (x["rank"], -x["mcap"]))
        return out[:limit]

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Range breakout buys at {c.get('entry_min_sol', 100):g}-{c.get('entry_max_sol', 200):g} SOL, only when "
                     f"{c.get('break_pct', 10):g}-{c.get('max_chase_pct', 20):g}% above the range top · sell everything at "
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, "
                     f"{c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldSmallTuned(RangeHoldSmallThin):
    """PAPER test (11 Oct): the under-$12K / 10-trade test, tuned on its first 51 trades - buys only at
    `entry_min_sol` (60) SOL or more, only out of ranges at least `min_range_pct` (10%) wide, and once a coin has
    reached `floor_arm_mult` (2x) it is sold if it falls back to `floor_mult` (1.0x). Past data: 13 of 51 trades
    fit, 3 bonded, +$242 (+$121 without the best). Uses rangebreak + rangehold + rangehold_small + rangehold_small10
    settings plus the `rangehold_small10t` section; never real money."""
    NAME = "rangehold_small10t"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        for sec in ("rangehold", "rangehold_small", "rangehold_small10"):
            c.update(self._cfg().get(sec) or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        lo, hi = cd["range"]
        if px * SUPPLY < float(c.get("entry_min_sol", 60)) or (hi / lo - 1) * 100 < float(c.get("min_range_pct", 10)):
            return                                          # too small / too tight right now - may still fit later
        return super()._enter(cd, px, ts, trades_2m)

    def check(self, p, px, ts):
        c = self.cfg()
        mult = px / p["entry_px"]
        peak = max(p["peak_mult"], mult)
        arm, floor = float(c.get("floor_arm_mult", 2.0)), float(c.get("floor_mult", 1.0))
        if peak >= arm and mult <= floor and px * SUPPLY < float(c.get("bond_exit_mcap_sol", 375)):
            p["last_px"], p["last_px_ts"], p["peak_mult"] = px, ts, peak
            return self._sell(p, 1.0, f"back to {floor:g}x after a {peak:.1f}x high (break-even floor)", px, ts)
        return super().check(p, px, ts)

    def watching(self, limit=200):
        c = self.cfg()
        mn, rw = float(c.get("entry_min_sol", 60)), float(c.get("min_range_pct", 10))
        out = super().watching(limit=10_000)
        for x in out:
            if x["rank"] == 0 and x["mcap"] < mn:
                x["why"] += f" · but buys only from {mn:g} SOL"
                x["rank"] = 1
            elif x["rank"] == 0 and x["why"].startswith("Sideways"):
                g = re.match(r"Sideways (\d+)–(\d+)", x["why"])
                if g and float(g.group(1)) and (float(g.group(2)) / float(g.group(1)) - 1) * 100 < rw:
                    x["why"] += f" · but range under {rw:g}% wide"
                    x["rank"] = 1
        out.sort(key=lambda x: (x["rank"], -x["mcap"]))
        return out[:limit]

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Under ${float(c.get('max_entry_mcap_usd', 12000)) / 1000:g}K with {c.get('min_trades_2m', 10)}+ trades, "
                     f"but only from {c.get('entry_min_sol', 60):g} SOL and out of ranges {c.get('min_range_pct', 10):g}%+ wide · "
                     f"break-even floor after {c.get('floor_arm_mult', 2.0):g}x · sell everything at "
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, "
                     f"{c.get('max_hold_h', 6):g}h limit · no real money")
        return s


class RangeHoldV2Mid(RangeHoldV2):
    """PAPER test (12 Oct): hold-to-bonding v2 (coins `min_age_min`-`max_age_h` old), but only buys at
    `entry_min_sol`-`entry_max_sol` (85-130 SOL), plus a break-even floor once a coin has reached `floor_arm_mult`
    (2x). v2's first 61 trades: under 85 SOL 0 of 17 bonded (-$124); 85-130 SOL 5 of 24 bonded, +$146, positive in
    both halves; above 130 SOL bonds paid too little (-$113). Uses rangebreak + rangehold + rangehold_v2 settings
    plus the `rangehold_v2mid` section; never real money."""
    NAME = "rangehold_v2mid"

    def cfg(self):
        c = dict(self._cfg().get("rangebreak") or {})
        for sec in ("rangehold", "rangehold_v2"):
            c.update(self._cfg().get(sec) or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        if not float(c.get("entry_min_sol", 85)) <= px * SUPPLY < float(c.get("entry_max_sol", 130)):
            return                                          # outside 85-130 SOL right now - may still fit later
        return super()._enter(cd, px, ts, trades_2m)

    def check(self, p, px, ts):
        c = self.cfg()
        mult = px / p["entry_px"]
        peak = max(p["peak_mult"], mult)
        arm, floor = float(c.get("floor_arm_mult", 2.0)), float(c.get("floor_mult", 1.0))
        if peak >= arm and mult <= floor and px * SUPPLY < float(c.get("bond_exit_mcap_sol", 375)):
            p["last_px"], p["last_px_ts"], p["peak_mult"] = px, ts, peak
            return self._sell(p, 1.0, f"back to {floor:g}x after a {peak:.1f}x high (break-even floor)", px, ts)
        return super().check(p, px, ts)

    def watching(self, limit=200):
        c = self.cfg()
        lo, hi = float(c.get("entry_min_sol", 85)), float(c.get("entry_max_sol", 130))
        out = super().watching(limit=10_000)
        for x in out:
            if x["rank"] == 0 and not lo <= x["mcap"] < hi:
                x["why"] += f" · but buys only at {lo:g}-{hi:g} SOL"
                x["rank"] = 1
        out.sort(key=lambda x: (x["rank"], -x["mcap"]))
        return out[:limit]

    def state(self):
        s = super().state()
        c = self.cfg()
        s["desc"] = (f"Range breakout buys of coins {c.get('min_age_min', 80):g} min-{c.get('max_age_h', 3.2):g} h old, only at "
                     f"{c.get('entry_min_sol', 85):g}-{c.get('entry_max_sol', 130):g} SOL · break-even floor after "
                     f"{c.get('floor_arm_mult', 2.0):g}x · sell everything at {c.get('bond_exit_mcap_sol', 375):g} SOL / migration, "
                     f"stop −{c.get('stop_pct', 50):g}%{self._dead_txt(c)}, {c.get('max_hold_h', 6):g}h limit · no real money")
        return s
