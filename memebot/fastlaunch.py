"""
"Fast launch" PAPER test - the strategy of wallet BB1jeGTH (analysed 4 Oct): brand-new pump.fun coins that are
taking off fast, small size, cut losers quickly, add to winners, take profit in steps.

Buy (paper): a pump.fun coin `min_age_s`-`max_age_s` old (1-5 min) whose market cap is between `min_mcap_sol` and
`max_mcap_sol` (50-120 SOL) and still rising - i.e. it went from 0 to 50+ SOL within minutes. Skips coins that had
already been above the range (falling back, not taking off). Once per coin.

Sell:
  - stop -`stop_pct` (25%) until it has been up 1.5x
  - no move: still below our price after `no_move_s` (4 min) and never up 10% -> sell
  - add the same size again once when up `add_pct` (20%) within the first `add_window_s` (10 min) ("pyramid")
  - sell 1/3 at `tp1_mult` (1.5x), half of the rest at `tp2_mult` (2x)
  - trailing stop `trail_pct` (30%) below the high once it has been up 1.5x
  - everything at the bonding level (`bond_exit_mcap_sol`, 375 SOL) or after `max_hold_min` (60 min)
Never real money.
"""
from __future__ import annotations

import logging
import time

from memebot.lookalike import Lookalike, curve_buy

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000


class FastLaunch(Lookalike):
    NAME = "fastlaunch"

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    # ------------------------------------------------------------------ entry (engine hook, every trade)
    def maybe_enter(self, t, prev_mcap, ts):
        if not (self.active and self.enabled()) or t.mint in self.traded or t.mint in self.positions:
            return False
        if not t.price or not t.mcap or not prev_mcap or not getattr(t, "created_ts", None):
            return False
        c = self.cfg()
        age = ts - t.created_ts
        if not (float(c.get("min_age_s", 60)) <= age <= float(c.get("max_age_s", 300))):
            return False
        lo, hi = float(c.get("min_mcap_sol", 50)), float(c.get("max_mcap_sol", 120))
        if not (lo <= t.mcap <= hi) or t.mcap <= prev_mcap:      # in the range and rising (this trade was a buy)
            return False
        prior = getattr(t, "prior_peak", 0) or 0
        if prior > hi:                                            # already ran higher: falling back, not taking off
            self.traded.add(t.mint)
            return False
        usd = self._usd()
        if not usd:
            return False
        self.traded.add(t.mint)
        if len(self.positions) >= int(c.get("max_open", 50)):
            self._event("skip", f"{t.symbol}: max {c.get('max_open', 50)} paper positions open", mint=t.mint)
            return False
        size = float(c.get("size_usd", 25)) / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), t.price) * (1 - slip)
        self.positions[t.mint] = dict(
            mint=t.mint, symbol=t.symbol, name=getattr(t, "name", "") or "", opened=ts, entry_px=t.price,
            entry_mcap=round(t.mcap, 1), age_at_entry_s=round(age), sol_in=size + prio, sol_out=0.0, tokens=tokens,
            tokens_bought=tokens, peak_mult=1.0, floor_mult=0.0, done=[], last_px=t.price, last_px_ts=ts, sells=[],
            verified=True, size_usd=round(size * usd, 2))
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), t.mint, t.symbol, "BUY",
                   f"fast launch: {t.mcap:.0f} SOL at {age:.0f}s old", "1.00", round(size + prio, 6), round(tokens, 2)])
        self._event("buy", f"Paper buy {t.symbol} at {t.mcap:.0f} SOL mcap, {age / 60:.1f} min after launch", mint=t.mint)
        log.info("FASTLAUNCH buy %s at mcap %.0f (%.0fs old)", t.symbol, t.mcap, age)
        return True

    # ------------------------------------------------------------------ exits
    def _add(self, p, px, ts, c):
        usd = self._usd()
        if not usd:
            return
        size = float(c.get("size_usd", 25)) / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), px) * (1 - slip)
        p["tokens"] += tokens
        p["tokens_bought"] += tokens
        p["sol_in"] += size + prio
        p["done"].append("add")
        mult = px / p["entry_px"]
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), p["mint"], p["symbol"], "BUY",
                   f"added at +{(mult - 1) * 100:.0f}%", f"{mult:.2f}", round(size + prio, 6), round(tokens, 2)])
        self._event("buy", f"{p['symbol']}: added ${size * usd:.0f} at {mult:.2f}x (pyramid)", mint=p["mint"])

    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        peak, held, done = p["peak_mult"], ts - p["opened"], p["done"]
        bond = float(c.get("bond_exit_mcap_sol", 375) or 0)
        if bond and (px * SUPPLY >= bond or p.get("src") == "jupiter"):
            return self._sell(p, 1.0, f"bonding level ({px * SUPPLY:.0f} SOL) - sold everything", px, ts)
        if held > float(c.get("max_hold_min", 60)) * 60:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_min', 60):g} min", px, ts)
        if peak < 1.5 and mult <= 1 - float(c.get("stop_pct", 25)) / 100:
            return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 25):g}%", px, ts)
        if held >= float(c.get("no_move_s", 240)) and peak < 1.1 and mult < 1.0:
            return self._sell(p, 1.0, f"no move after {float(c.get('no_move_s', 240)) / 60:g} min", px, ts)
        if peak >= 1.5 and mult <= peak * (1 - float(c.get("trail_pct", 30)) / 100):
            return self._sell(p, 1.0, f"trailing stop ({peak:.1f}x high)", px, ts)
        if ("add" not in done and held <= float(c.get("add_window_s", 600))
                and mult >= 1 + float(c.get("add_pct", 20)) / 100):
            self._add(p, px, ts, c)
        if mult >= float(c.get("tp1_mult", 1.5)) and "tp1" not in done:
            done.append("tp1")
            self._sell(p, 1 / 3, f"{c.get('tp1_mult', 1.5):g}x - sold a third", px, ts)
            if p["mint"] not in self.positions:
                return
        if mult >= float(c.get("tp2_mult", 2)) and "2x" not in done:
            done.append("2x")
            self._sell(p, 0.5, f"{c.get('tp2_mult', 2):g}x - sold half of the rest", px, ts)

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME,
                 desc=(f"pump.fun coins {float(c.get('min_age_s', 60)) / 60:g}-{float(c.get('max_age_s', 300)) / 60:g} min old "
                       f"already at {c.get('min_mcap_sol', 50):g}-{c.get('max_mcap_sol', 120):g} SOL and rising · "
                       f"stop −{c.get('stop_pct', 25):g}%, out if no move in {float(c.get('no_move_s', 240)) / 60:g} min, "
                       f"add once at +{c.get('add_pct', 20):g}%, ⅓ at {c.get('tp1_mult', 1.5):g}×, half the rest at "
                       f"{c.get('tp2_mult', 2):g}×, trail {c.get('trail_pct', 30):g}%, all out at "
                       f"{c.get('bond_exit_mcap_sol', 375):g} SOL / {c.get('max_hold_min', 60):g} min · "
                       f"${c.get('size_usd', 25)} paper (wallet BB1jeGTH's style)"))
        return s
