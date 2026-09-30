"""
"Lookalike · graduation exit" paper test - the SAME entries as the lookalike (a watched coin rising
through 44 SOL mcap, 5+ min old), with an exit plan built around pump.fun graduation (~410 SOL mcap),
modelled on how wallet #1 cashes out its big winners (mostly on the curve at 343-390 SOL, the rest after
graduation on PumpSwap).

Exit (multiples of the entry price; market caps in SOL):
  - before 3x: stop at -stop_pct (30%) -> sell everything; once it has reached breakeven_after_mult (2x)
    the stop moves up to breakeven_floor_mult (1.08x, covers fees) so a coin that doubled never ends a loss
  - 3x: sell tp_pct (1/3)
  - after 3x: trailing stop trail_pct (35%) below the highest price since the buy
  - graduation zone (mcap >= zone_mcap_sol, 350 ~ 8x): sell zone_sell_pct (2/3) of what is left, on the curve,
    before the migration dump - also done at the first reading after graduation if the zone was jumped
  - the rest (moonbag): trailing stop moon_trail_pct (25%) below its high; after graduation it is also sold
    if the price falls below the graduation price
  - time limit max_hold_h (48 h)
"""
from __future__ import annotations

import os
import time

from memebot.lookalike import Lookalike

SUPPLY = 1_000_000_000
GRAD_MCAP = 410.0


class LookalikeGrad(Lookalike):
    NAME = "lookalike_grad"

    def cfg(self):
        c = dict(super().cfg())
        if type(self) is LookalikeGrad:
            c["real_enabled"] = False      # paper now: the real wallet trades the Survivor strategy. Coins it still
        return c                           # holds for real keep being sold by this plan (see _sell)

    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        done = p["done"]
        graduated = p.get("src") == "jupiter"
        mcap = px * SUPPLY
        if ts - p["opened"] > float(c.get("max_hold_h", 48)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 48):g}h", px, ts)
        if "zone" in done:                                   # moonbag
            p["moon_peak"] = max(p.get("moon_peak", mult), mult)
            if mult <= p["moon_peak"] * (1 - float(c.get("moon_trail_pct", 25)) / 100):
                return self._sell(p, 1.0, f"moonbag trailing stop ({p['moon_peak']:.1f}x high)", px, ts)
            if graduated and mcap < float(c.get("grad_floor_mcap_sol", GRAD_MCAP)):
                return self._sell(p, 1.0, "moonbag fell below the graduation price", px, ts)
            return
        if "3x" not in done:
            be_arm = float(c.get("breakeven_after_mult", 2))
            if be_arm and p["peak_mult"] >= be_arm:          # it doubled: never let it become a loss
                if mult <= float(c.get("breakeven_floor_mult", 1.08)):
                    return self._sell(p, 1.0, f"break-even stop (was up to {p['peak_mult']:.1f}x)", px, ts)
            elif mult <= 1 - float(c.get("stop_pct", 30)) / 100:
                return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 30):g}%", px, ts)
        elif mult <= p["peak_mult"] * (1 - float(c.get("trail_pct", 35)) / 100):
            return self._sell(p, 1.0, f"trailing stop ({p['peak_mult']:.1f}x high)", px, ts)
        if mult >= float(c.get("tp_mult", 3)) and "3x" not in done:
            done.append("3x")
            self._sell(p, float(c.get("tp_pct", 33.3)) / 100, f"{c.get('tp_mult', 3):g}x - sold a third", px, ts)
            if p["mint"] not in self.positions:
                return
        if (mcap >= float(c.get("zone_mcap_sol", 350)) or graduated) and "zone" not in done:
            done.append("zone")
            p["moon_peak"] = mult
            where = "after graduation" if graduated else f"graduation zone {mcap:.0f} SOL"
            self._sell(p, float(c.get("zone_sell_pct", 66.7)) / 100, f"{where} - sold 2/3 of the rest", px, ts)

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME, desc=(f"Same entries as the lookalike ({c.get('entry_mcap_sol', 44)} SOL, ${c.get('size_usd', 2.5)}) · "
                                       f"exit: ⅓ at {c.get('tp_mult', 3):g}x, trail {c.get('trail_pct', 35):g}%, "
                                       f"⅔ of the rest at {c.get('zone_mcap_sol', 350):g} SOL (before graduation), moonbag rides"))
        for pos, p in zip(s["positions"], [self.positions.get(x["mint"]) for x in s["positions"]]):
            if p:
                pos["graduated"] = p.get("src") == "jupiter"
        return s


class LookalikeGrad3(LookalikeGrad):
    """PAPER test: exactly the graduation-exit lookalike (same settings, read live from `lookalike_grad`),
    except it may buy coins from `lookalike_grad3.min_age_s` (3 min) instead of 5 min. Never real money.
    Compared side by side with the 5-minute version over the same period."""
    NAME = "lookalike_grad3"

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.since_path = os.path.join(self.dir, f"{self.NAME}_since.txt")
        try:
            with open(self.since_path, encoding="utf-8") as fh:
                self.since = float(fh.read().strip())
        except (OSError, ValueError):
            self.since = time.time()
            try:
                with open(self.since_path, "w", encoding="utf-8") as fh:
                    fh.write(str(self.since))
            except OSError:
                pass

    def cfg(self):
        c = dict(self._cfg().get("lookalike_grad") or {})
        own = self._cfg().get(self.NAME) or {}
        c.update(enabled=own.get("enabled", True), min_age_s=own.get("min_age_s", 180), real_enabled=False,
                 max_prior_peak_mult=0, min_rise_2m_pct=0, min_rise_1m_pct=None, min_buys_2m=0)   # buys as before: no trend filters
        return c

    def state(self):
        s = super().state()
        s.update(name=self.NAME, real_enabled=False, since=self.since,
                 min_age_s=self.cfg().get("min_age_s", 180))
        return s


class LookalikeGradOld(LookalikeGrad3):
    """PAPER comparison: the graduation lookalike with the OLD entry rules - no up-trend and no
    "near its high" filters (it buys any coin rising through 44 SOL at 5+ min old) - so the new
    filters on the real wallet can be judged against what it would have done without them."""
    NAME = "lookalike_grad_old"

    def cfg(self):
        c = dict(self._cfg().get("lookalike_grad") or {})
        own = self._cfg().get(self.NAME) or {}
        c.update(enabled=own.get("enabled", True), real_enabled=False, min_age_s=c.get("min_age_s", 300),
                 max_prior_peak_mult=0, min_rise_2m_pct=0, min_rise_1m_pct=None, min_buys_2m=0)
        return c
