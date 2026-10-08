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

import logging
import time

from memebot.lookalike import curve_buy
from memebot.reclaim import Reclaim

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
            graduated=cd.get("src") == "jupiter")
        rng = (hi / lo - 1) * 100
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), cd["mint"], cd["symbol"], "BUY",
                   f"breakout at {mc:.0f} SOL from a {lo:.0f}-{hi:.0f} SOL range ({rng:.0f}% wide)", "1.00",
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
        c = self.cfg()
        now = time.time()
        usd = self._usd()
        cap_usd = float(c.get("max_entry_mcap_usd", 0) or 0)
        out = []
        for m, cd in self.cands.items():
            if m in self.positions or m in self.traded:
                continue
            mc = cd.get("last_mc") or 0
            age = (now - cd["created"]) / 60
            rg = cd.get("range")
            w = (rg[1] / rg[0] - 1) * 100 if rg else None
            if age < float(c.get("min_age_min", 30)):
                why, rank = f"too young · can buy from {c.get('min_age_min', 30):g} min", 2
            elif not rg:
                why, rank = "collecting price history", 3
            elif w > float(c.get("max_range_pct", 35)):
                why, rank = f"too choppy · {w:.0f}% range (needs ≤{c.get('max_range_pct', 35):g}%)", 3
            elif rg[0] < float(c.get("min_entry_mcap_sol", 40)):
                why, rank = f"range too low ({rg[0]:.0f} SOL)", 3
            else:
                why, rank = (f"sideways {rg[0]:.0f}–{rg[1]:.0f} SOL ({w:.0f}%) · buys above "
                             f"{rg[1] * (1 + float(c.get('break_pct', 5)) / 100):.0f}"), 0
            if cap_usd and usd and mc * usd >= cap_usd:
                why, rank = f"above the ${cap_usd / 1000:g}K cap · " + why, max(rank, 1)
            if mc >= float(c.get("max_entry_mcap_sol", 1e9)):
                why, rank = "too close to bonding · " + why, max(rank, 1)
            out.append(dict(mint=m, symbol=cd.get("symbol") or m[:5], age_min=round(age), mcap=round(mc, 1),
                            why=why, rank=rank))
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
        mc = px * SUPPLY
        if mc >= float(c.get("bond_exit_mcap_sol", 375)) or getattr(self, "_src", {}).get(p["mint"]) == "jupiter":
            return self._sell(p, 1.0, f"bonding level ({mc:.0f} SOL) - sold everything", px, ts)
        if mult <= 1 - float(c.get("stop_pct", 50)) / 100:
            return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 50):g}%", px, ts)
        if ts - p["opened"] > float(c.get("max_hold_h", 6)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 6):g}h", px, ts)

    def state(self):
        s = super().state()
        c = self.cfg()
        since = self.stats_from()
        s["bonded"] = sum(1 for x in self.closed if x["opened"] >= since and str(x.get("exit", "")).startswith("bonding level"))
        s.update(name=self.NAME, desc=(f"Range breakout buys (on the curve, under {c.get('max_entry_mcap_sol', 250):g} SOL), "
                                       f"but HOLD: sell everything at {c.get('bond_exit_mcap_sol', 375):g} SOL / migration, "
                                       f"stop −{c.get('stop_pct', 50):g}%, {c.get('max_hold_h', 6):g}h limit · no real money"))
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
                     f"stop −{c.get('stop_pct', 50):g}%, {c.get('max_hold_h', 6):g}h limit · no real money")
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
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%, "
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
                     f"{c.get('bond_exit_mcap_sol', 375):g} SOL / migration, stop −{c.get('stop_pct', 50):g}%, "
                     f"{c.get('max_hold_h', 6):g}h limit · no real money")
        return s
