"""
"Reclaim" paper strategy - buy a second wave instead of racing the first one.

Candidates: coins the bot watched that made a real first run (peak >= run_mcap_sol, 80 SOL).
They are then tracked on-chain (bonding curve, batched) for up to max_age_h.

Buy (paper) when ALL of:
  - the coin is at least min_age_min old (30 min)
  - it pulled back >= pullback_pct (40%) from its peak
  - it bounced >= bounce_pct (25%) off the low since then
  - it is still <= max_of_peak_pct (80%) of the old peak (not already fully recovered)
  - market cap >= min_entry_mcap_sol (35) and still on the pump.fun curve
  - >= min_trades_2m (15) trades in the last 2 minutes (people are buying again)

Sell:
  - stop at -stop_pct (25%) until the coin has been up trail_arm_mult (1.5x)
  - after that, a trailing stop trail_pct (35%) below the highest point since the buy
  - sell tp_frac (1/3) at tp_mult (2x)
  - time limit: max_hold_h (24h)

Costs, price impact and the price sanity checks are the same as the lookalike test.
"""
from __future__ import annotations

import json
import logging
import os
import time

from memebot.chain import curve_address
from memebot.lookalike import Lookalike, curve_buy

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000


class Reclaim(Lookalike):
    NAME = "reclaim"

    def __init__(self, data_dir, cfg_getter, key_getter, sol_usd_getter):
        super().__init__(data_dir, cfg_getter, key_getter, sol_usd_getter)
        self.cands_path = os.path.join(data_dir, "reclaim_candidates.json")
        self.cands: dict[str, dict] = {}
        self.known: set[str] = set()
        try:
            with open(self.cands_path, encoding="utf-8", errors="replace") as fh:
                s = json.load(fh)
            self.cands, self.known = s.get("cands", {}), set(s.get("known", []))
        except (OSError, ValueError):
            pass

    def _save(self):
        super()._save()
        if not hasattr(self, "cands_path"):
            return                                         # called from the base constructor
        try:
            tmp = self.cands_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"cands": self.cands, "known": sorted(self.known)[-20000:]}, fh)
            os.replace(tmp, self.cands_path)
        except OSError:
            pass

    # ------------------------------------------------------------------ candidates (engine hook)
    def maybe_enter(self, t, prev_mcap, ts):
        """Called on every trade of a watched coin: remember coins that made a real first run."""
        if not (self.active and self.enabled()) or t.mint in self.known or not t.mcap or not t.creator:
            return False
        c = self.cfg()
        if t.mcap < float(c.get("run_mcap_sol", 80)):
            return False
        self.known.add(t.mint)
        self.cands[t.mint] = dict(mint=t.mint, symbol=t.symbol, name=t.name, created=t.created_ts, added=ts,
                                  peak=t.mcap, low=None, pulled=False, last_mc=t.mcap, misses=0)
        cap = int(c.get("max_candidates", 400))
        if len(self.cands) > cap:
            for m in sorted(self.cands, key=lambda m: self.cands[m]["added"])[:len(self.cands) - cap]:
                self.cands.pop(m)
        return False

    def wants_watch(self, mint):
        """Engine: keep watching a new coin until we know whether it made a first run."""
        return self.active and self.enabled() and mint not in self.known

    async def _activity(self, mint):
        sigs = await self._rpc("getSignaturesForAddress", [curve_address(mint), {"limit": 100, "commitment": "confirmed"}])
        now = time.time()
        return sum(1 for s in sigs or [] if not s.get("err") and s.get("blockTime") and now - s["blockTime"] <= 120)

    def _enter(self, cd, px, ts, trades_2m):
        c = self.cfg()
        usd = self._usd()
        if not usd:
            return
        self.traded.add(cd["mint"])
        if len(self.positions) >= int(c.get("max_open", 100)):
            return self._event("skip", f"{cd['symbol']}: max {c.get('max_open', 100)} paper positions open")
        size = float(c.get("size_usd", 25)) / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), px) * (1 - slip)
        mc = px * SUPPLY
        self.positions[cd["mint"]] = dict(
            mint=cd["mint"], symbol=cd["symbol"], name=cd.get("name", ""), opened=ts, entry_px=px, entry_mcap=round(mc, 1),
            age_at_entry_s=round(ts - cd["created"]), sol_in=size + prio, sol_out=0.0, tokens=tokens, tokens_bought=tokens,
            peak_mult=1.0, floor_mult=0.0, done=[], last_px=px, last_px_ts=ts, sells=[], verified=True,
            size_usd=round(size * usd, 2), first_peak=round(cd["peak"], 1), low=round((cd["low"] or mc), 1))
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), cd["mint"], cd["symbol"], "BUY",
                   f"reclaim at {mc:.0f} SOL (peak {cd['peak']:.0f} low {cd['low']:.0f} {trades_2m} trades/2m)",
                   "1.00", round(size + prio, 6), round(tokens, 2)])
        self._event("buy", f"Paper buy {cd['symbol']} at {mc:.0f} SOL mcap - bounced from {cd['low']:.0f} after a "
                           f"{cd['peak']:.0f} SOL peak ({trades_2m} trades in 2 min)", mint=cd["mint"])
        log.info("RECLAIM buy %s at mcap %.0f (peak %.0f, low %.0f, %d trades/2m)",
                 cd["symbol"], mc, cd["peak"], cd["low"], trades_2m)

    # ------------------------------------------------------------------ exits
    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        if ts - p["opened"] > float(c.get("max_hold_h", 24)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 24):g}h", px, ts)
        if p["peak_mult"] < float(c.get("trail_arm_mult", 1.5)):
            if mult <= 1 - float(c.get("stop_pct", 25)) / 100:
                return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 25):g}%", px, ts)
        elif mult <= p["peak_mult"] * (1 - float(c.get("trail_pct", 35)) / 100):
            return self._sell(p, 1.0, f"trailing stop ({p['peak_mult']:.1f}x peak)", px, ts)
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
        for m, cd in list(self.cands.items()):
            if m in self.positions or m in self.traded:
                self.cands.pop(m, None)
                continue
            age = now - cd["created"]
            price, src = px.get(m, (None, None))
            if age > float(c.get("max_age_h", 6)) * 3600 or src in ("jupiter", "mayhem"):
                self.cands.pop(m, None)                    # too old, or graduated off the curve
                continue
            if price is None:
                cd["misses"] = cd.get("misses", 0) + 1
                if cd["misses"] > 30:
                    self.cands.pop(m, None)
                continue
            mc = price * SUPPLY
            last = cd.get("last_mc") or mc
            if not (1 / 3 <= mc / last <= 3):              # same sanity gate as positions: confirm big jumps
                if not (cd.get("suspect") and 0.8 <= mc / cd["suspect"] <= 1.25):
                    cd["suspect"] = mc
                    continue
            cd.pop("suspect", None)
            cd["last_mc"] = mc
            if mc < float(c.get("dead_mcap_sol", 25)):
                self.cands.pop(m, None)
                continue
            if mc > cd["peak"]:
                cd.update(peak=mc, low=None, pulled=False)
            if mc <= cd["peak"] * (1 - float(c.get("pullback_pct", 40)) / 100):
                cd["pulled"] = True
            if cd["pulled"]:
                cd["low"] = min(cd["low"] or mc, mc)
            if (cd["pulled"] and age >= float(c.get("min_age_min", 30)) * 60
                    and mc >= cd["low"] * (1 + float(c.get("bounce_pct", 25)) / 100)
                    and mc <= cd["peak"] * float(c.get("max_of_peak_pct", 80)) / 100
                    and mc >= float(c.get("min_entry_mcap_sol", 35))
                    and now - cd.get("checked_at", 0) >= 30):
                cd["checked_at"] = now
                try:
                    n = await self._activity(m)
                except Exception as e:
                    self.last_error = f"activity check: {e}"
                    continue
                cd["last_trades_2m"] = n
                if n >= int(c.get("min_trades_2m", 15)):
                    self._enter(cd, price, now, n)
                    self.cands.pop(m, None)
        for m in list(self.positions):
            p = self.positions.get(m)
            price, src = px.get(m, (None, None))
            if p and price and price > 0 and self._accept(p, price, now):
                self.check(p, price, now)
        self._save()

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME, candidates=len(self.cands),
                 pulled=sum(1 for x in self.cands.values() if x.get("pulled")),
                 desc=(f"Coins that ran to {c.get('run_mcap_sol', 80)}+ SOL, pulled back {c.get('pullback_pct', 40)}%+, "
                       f"then bounce {c.get('bounce_pct', 25)}% with {c.get('min_trades_2m', 15)}+ trades/2 min "
                       f"(30+ min old) · ${c.get('size_usd', 25)} each · no real money"))
        return s
