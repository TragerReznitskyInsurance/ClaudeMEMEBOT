"""
"Survivor breakout" - trades the way wallet #1 actually trades (from its buys and round trips):

  It buys OLD coins (median ~1 h old, many 2 h+) that survived the launch chaos and are grinding up to a
  NEW HIGH around 44 SOL on a quiet tape - and it wins on exits: losers are cut within ~3 minutes at a
  small loss, winners are sold in pieces over 20-40 minutes.

Candidates: every coin the bot watched that showed real buying (reached `intake_mcap_sol`, 32) and passed
the safety checks. They are then followed ON-CHAIN (batched curve reads, cheap) for up to `max_age_h` -
coins near the line every few seconds, the rest about once a minute.

Buy when ALL of:
  - at least `min_age_min` (10) old and at most `max_age_h` (24) old, still on the pump.fun curve
    (wallet #1's winners on 30 Sep were 13 h and 38 h old coins; its 5-10 minute old buys lost)
  - market cap rises through `entry_mcap_sol` (44), and is not already above `max_entry_mult` x that
  - it is at a NEW HIGH: at or above the highest market cap it had since its launch settled (`settle_min`,
    15 min - launch-minute spikes by snipers/bundles don't count), within `near_high_pct`
  - grinding up: +`min_rise_2m_pct` (5%) over the last ~2 minutes with no pullback bigger than
    `max_pullback_pct` (12%) inside that window (no one dumping into it)

Sell (wallet #1 style):
  - scratch: if it hasn't reached +`scratch_mult` (10%) within `scratch_min` (3) minutes, sell everything
  - stop -`stop_pct` (20%) until it has doubled; after 2x the stop moves to break-even (`breakeven_floor_mult`)
  - scale out: 1/4 at 2x, 1/4 at 3x, 1/4 at 5x; the last 1/4 rides with a `trail_pct` (30%) trailing stop
  - time limit `max_hold_h` (24 h)

Real money: uses the lookalike wallet (its own separate wallet) and the real-money settings of
"Lookalike · graduation exit" (on/off, $ per coin, max open, daily loss limit).
"""
from __future__ import annotations

import json
import logging
import os
import time

from memebot.lookalike import Lookalike, curve_buy

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000
SCALE = [(2.0, "2x", 0.25), (3.0, "3x", 1 / 3), (5.0, "5x", 0.5)]      # fractions of what is LEFT = 1/4 of the original each


class Survivor(Lookalike):
    NAME = "survivor"

    def __init__(self, data_dir, cfg_getter, key_getter, sol_usd_getter):
        super().__init__(data_dir, cfg_getter, key_getter, sol_usd_getter)
        self.cands_path = os.path.join(data_dir, f"{self.NAME}_candidates.json")
        self.cands: dict[str, dict] = {}
        try:
            with open(self.cands_path, encoding="utf-8", errors="replace") as fh:
                self.cands = json.load(fh).get("cands", {})
        except (OSError, ValueError):
            pass
        for cd in self.cands.values():                          # older saves counted launch spikes as the high
            if not cd.get("settled_peak"):
                cd.update(peak=0.0, settled_peak=True)
        since_path = os.path.join(data_dir, f"{self.NAME}_since.txt")
        try:
            with open(since_path, encoding="utf-8") as fh:
                self.since = float(fh.read().strip())
        except (OSError, ValueError):
            self.since = time.time()
            try:
                with open(since_path, "w", encoding="utf-8") as fh:
                    fh.write(str(self.since))
            except OSError:
                pass

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        real = self._cfg().get("lookalike_grad") or {}            # the lookalike wallet's real-money settings
        for k in ("real_enabled", "real_size_usd", "real_max_open", "real_daily_loss_usd"):
            if k in real:
                c[k] = real[k]
        return c

    def _save(self):
        super()._save()
        if not hasattr(self, "cands_path"):
            return
        try:
            tmp = self.cands_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"cands": self.cands}, fh)
            os.replace(tmp, self.cands_path)
        except OSError:
            pass

    # ------------------------------------------------------------------ candidates (engine hook)
    def maybe_enter(self, t, prev_mcap, ts):
        """Every trade of a watched coin: remember coins that showed real buying and passed the safety checks."""
        if not (self.active and self.enabled()) or not t.mcap or not t.creator or not t.created_ts:
            return False
        if t.mint in self.cands or t.mint in self.traded or t.sec_state != "passed":
            return False
        c = self.cfg()
        if t.mcap < float(c.get("intake_mcap_sol", 32)):
            return False
        self.cands[t.mint] = dict(mint=t.mint, symbol=t.symbol, name=t.name, created=t.created_ts, added=ts,
                                  peak=0.0, last_mc=t.mcap, hist=[], next=0, misses=0,   # peak: after the launch settles
                                  settled_peak=True)
        cap = int(c.get("max_candidates", 8000))
        if len(self.cands) > cap:
            for m in sorted(self.cands, key=lambda m: self.cands[m]["added"])[:len(self.cands) - cap]:
                self.cands.pop(m)
        return False

    def seed(self, coins):
        """Add older coins the bot saw earlier (from the narratives log): [(mint, symbol, name, created_ts)]."""
        c = self.cfg()
        now = time.time()
        n = 0
        for mint, sym, name, created in coins:
            if mint in self.cands or mint in self.traded or now - created > float(c.get("max_age_h", 72)) * 3600:
                continue
            self.cands[mint] = dict(mint=mint, symbol=sym or mint[:5], name=name or "", created=created, added=now,
                                    peak=0.0, last_mc=None, hist=[], next=0, misses=0, settled_peak=True, seeded=True)
            n += 1
        return n

    # ------------------------------------------------------------------ entry
    def _signal(self, cd, mc, now):
        """None if it's a buy, else the reason it isn't (only asked when it crosses the line)."""
        c = self.cfg()
        line = float(c.get("entry_mcap_sol", 44))
        age = now - cd["created"]
        if age < float(c.get("min_age_min", 10)) * 60:
            return f"crossed {line:g} SOL at {age / 60:.0f} min old (needs {c.get('min_age_min', 10):g}+ min)"
        if mc > line * float(c.get("max_entry_mult", 1.35)):
            return f"jumped straight to {mc:.0f} SOL"
        if mc < cd["peak"] * (1 - float(c.get("near_high_pct", 3)) / 100):
            return f"not a new high (was up to {cd['peak']:.0f} SOL since its launch settled)"
        h = [x for x in cd["hist"] if now - x[0] <= 150]
        if not h or now - h[0][0] < 60:
            return "not enough price history yet"
        rise = (mc / h[0][1] - 1) * 100
        if rise < float(c.get("min_rise_2m_pct", 5)):
            return f"not grinding up ({rise:+.0f}% over ~2 min)"
        hi, pull = 0.0, 0.0
        for _, v in h + [(now, mc)]:
            hi = max(hi, v)
            pull = max(pull, (1 - v / hi) * 100)
        if pull > float(c.get("max_pullback_pct", 12)):
            return f"choppy - a {pull:.0f}% pullback in the last 2 min (someone dumping)"
        return None

    def _enter(self, cd, px, now):
        c = self.cfg()
        usd = self._usd()
        if not usd:
            return
        self.traded.add(cd["mint"])
        size = float(c.get("size_usd", 10)) / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), px) * (1 - slip)
        mc = px * SUPPLY
        p = dict(mint=cd["mint"], symbol=cd["symbol"], name=cd.get("name", ""), opened=now, entry_px=px,
                 entry_mcap=round(mc, 1), age_at_entry_s=round(now - cd["created"]), sol_in=size + prio, sol_out=0.0,
                 tokens=tokens, tokens_bought=tokens, peak_mult=1.0, floor_mult=0.0, done=[], last_px=px,
                 last_px_ts=now, sells=[], verified=True, size_usd=round(size * usd, 2), prior_peak=round(cd["peak"], 1))
        self.positions[cd["mint"]] = p
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)), cd["mint"], cd["symbol"], "BUY",
                   f"new high {mc:.0f} SOL at {(now - cd['created']) / 60:.0f} min old", "1.00", round(size + prio, 6),
                   round(tokens, 2)])
        self._event("buy", f"Paper buy {cd['symbol']} at {mc:.0f} SOL mcap - {(now - cd['created']) / 60:.0f} min old, "
                           f"breaking to a new high", mint=cd["mint"])
        log.info("SURVIVOR buy %s at mcap %.0f (%.0f min old)", cd["symbol"], mc, (now - cd["created"]) / 60)
        if c.get("real_enabled") and self.live is not None:
            why = self.live.open_strategy(self.NAME, cd["mint"], cd["symbol"], float(c.get("real_size_usd", 10)),
                                          int(c.get("real_max_open", 20)), float(c.get("real_daily_loss_usd", 25)))
            if why is None:
                p["real"] = True
                self._event("buy", f"{cd['symbol']}: REAL buy placed (${float(c.get('real_size_usd', 10)):.2f})",
                            mint=cd["mint"])
            else:
                self._event("skip", f"{cd['symbol']}: no real buy - {why}", mint=cd["mint"])

    # ------------------------------------------------------------------ exits
    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        done = p["done"]
        if ts - p["opened"] > float(c.get("max_hold_h", 24)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 24):g}h", px, ts)
        if "2x" not in done:
            sm = float(c.get("scratch_min", 0) or 0)                 # 0 = off
            if (sm and ts - p["opened"] >= sm * 60
                    and p["peak_mult"] < float(c.get("scratch_mult", 1.10))):
                return self._sell(p, 1.0, f"scratch - no follow-through in {sm:g} min", px, ts)
            if mult <= 1 - float(c.get("stop_pct", 30)) / 100:
                return self._sell(p, 1.0, f"stop -{c.get('stop_pct', 30):g}%", px, ts)
        else:
            if mult <= float(c.get("breakeven_floor_mult", 1.08)) and "3x" not in done:
                return self._sell(p, 1.0, f"break-even stop (was up to {p['peak_mult']:.1f}x)", px, ts)
            if mult <= p["peak_mult"] * (1 - float(c.get("trail_pct", 30)) / 100):
                return self._sell(p, 1.0, f"trailing stop ({p['peak_mult']:.1f}x high)", px, ts)
        for m, tag, frac in SCALE:
            if mult >= m and tag not in done:
                done.append(tag)
                self._sell(p, frac, f"{tag} - sold a quarter", px, ts)
                if p["mint"] not in self.positions:
                    return

    # ------------------------------------------------------------------ loop
    async def tick(self):
        if not self._key() or not (self.cands or self.positions):
            return
        c = self.cfg()
        now = time.time()
        line = float(c.get("entry_mcap_sol", 44))
        hot_from = line * float(c.get("hot_from_pct", 80)) / 100
        due = [m for m, cd in self.cands.items() if now >= cd.get("next", 0)]
        px = await self.prices(due + list(self.positions))
        for m in due:
            cd = self.cands.get(m)
            if not cd or m in self.positions or m in self.traded:
                self.cands.pop(m, None)
                continue
            age = now - cd["created"]
            price, src = px.get(m, (None, None))
            if age > float(c.get("max_age_h", 72)) * 3600 or src in ("jupiter", "mayhem"):
                self.cands.pop(m, None)
                continue
            if price is None:
                cd["misses"] = cd.get("misses", 0) + 1
                if cd["misses"] > 20:
                    self.cands.pop(m, None)
                continue
            cd["misses"] = 0
            mc = price * SUPPLY
            first = cd.get("last_mc") is None
            last = cd.get("last_mc") or mc
            if not (1 / 3 <= mc / last <= 3):                   # confirm big jumps with a second reading
                if not (cd.get("suspect") and 0.8 <= mc / cd["suspect"] <= 1.25):
                    cd["suspect"], cd["next"] = mc, now
                    continue
            cd.pop("suspect", None)
            hot = mc >= hot_from
            if hot:
                wait = 0
            elif mc < float(c.get("dead_mcap_sol", 29.5)) and age > 1800:
                wait = float(c.get("dead_every_s", 600))        # back at the launch price: check rarely
            elif age > float(c.get("old_after_h", 6)) * 3600:
                wait = float(c.get("old_every_s", 240))
            else:
                wait = float(c.get("cold_every_s", 90))
            cd["next"] = now + wait
            cd["hist"] = [x for x in cd["hist"] if now - x[0] <= 180] if hot else []
            if first and cd.get("seeded"):
                cd["peak"] = mc if age >= float(c.get("settle_min", 5)) * 60 else 0.0
            bl = getattr(self, "breakouts", None)
            if bl is not None and not first and last < line <= mc:
                h = [x for x in cd["hist"] if now - x[0] <= 150]
                hi, pull = 0.0, 0.0
                for _, v in h + [(now, mc)]:
                    hi = max(hi, v)
                    pull = max(pull, (1 - v / hi) * 100)
                bl.on_cross(m, cd["symbol"], now, age, mc, "survivor", None,
                            dict(rise_2m_pct=round((mc / h[0][1] - 1) * 100, 1) if h else None,
                                 pullback_2m_pct=round(pull, 1) if h else None, settled_peak=round(cd["peak"], 1),
                                 seeded=bool(cd.get("seeded"))))
            sk = getattr(self, "skimmer", None)
            if sk is not None and not first and last < line <= mc:
                sk.from_survivor(cd, price, now)
            if not first and last < line <= mc and m not in self.traded:
                why = self._signal(cd, mc, now)
                if why is None:
                    self._enter(cd, price, now)
                    self.cands.pop(m, None)
                    continue
                w = getattr(self, "why", None)
                if w is not None:
                    w.note(m, cd["symbol"], self.NAME, why, now)
            if hot:
                cd["hist"].append((now, mc))
            if age >= float(c.get("settle_min", 5)) * 60:       # launch-minute spikes don't count as its "high"
                cd["peak"] = max(cd["peak"], mc)
            cd["last_mc"] = mc
        for m in list(self.positions):
            p = self.positions.get(m)
            price, src = px.get(m, (None, None))
            if p and src:
                p["src"] = src
            if p and price and price > 0 and self._accept(p, price, now):
                self.check(p, price, now)
        self._save()

    def state(self):
        s = super().state()
        c = self.cfg()
        hot = sum(1 for x in self.cands.values() if (x.get("last_mc") or 0) >= float(c.get("entry_mcap_sol", 44)) * 0.8)
        s.update(name=self.NAME, candidates=len(self.cands), hot=hot, since=self.since,
                 min_age_min=c.get("min_age_min", 10), max_age_h=c.get("max_age_h", 24),
                 desc=(f"Coins {c.get('min_age_min', 10)} min to {c.get('max_age_h', 24)} h old breaking to a new high "
                       f"through {c.get('entry_mcap_sol', 44)} SOL on a quiet tape · wallet #1-style exits"))
        return s


class HotWord(Survivor):
    """PAPER test: Survivor's entry and exit rules, but only for coins whose name uses a HOT WORD right now
    (a word with 2+ real graduations in the last 6 h, from the narratives tracker), and from `min_age_min`
    (15) minutes old instead of 30 - these trends only last a few hours."""
    NAME = "hotword"
    narr = None                                                   # Narratives tracker, set by the app

    def cfg(self):
        c = dict(self._cfg().get("survivor") or {})
        c.update(self._cfg().get(self.NAME) or {})
        c.setdefault("min_age_min", 15)
        c["real_enabled"] = False
        return c

    def _hot(self, mint):
        c = self.cfg()
        if self.narr is None:
            return []
        return self.narr.hot_for(mint, float(c.get("window_h", 6)), int(c.get("min_grads", 2)))

    def maybe_enter(self, t, prev_mcap, ts):
        if not (self.active and self.enabled()) or t.mint in self.cands or not self._hot(t.mint):
            return False
        return super().maybe_enter(t, prev_mcap, ts)

    def _signal(self, cd, mc, now):
        words = self._hot(cd["mint"])
        if not words:
            return "its word isn't hot any more"
        cd["hot"] = words
        return super()._signal(cd, mc, now)

    def _enter(self, cd, px, now):
        super()._enter(cd, px, now)
        p = self.positions.get(cd["mint"])
        if p:
            p["hot"] = cd.get("hot", [])


class Skimmer(Lookalike):
    """PAPER test: lots of quick trades. Buys (almost) every coin rising through 44 SOL - young coins from the
    live feed (`min_age_s`+ old) and older coins from Survivor's watch list - and sells ALL of it at
    +`tp_pct` (20%), or right away if it drops `sl_pct` (5%) below the entry, or after `max_hold_min`.
    Paper fills include the pump.fun fee, slippage, priority fee and price impact, so the result shows what
    would really be left after costs."""
    NAME = "skimmer"

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        c.setdefault("entry_mcap_sol", 44)
        c.setdefault("min_age_s", 120)
        c.update(real_enabled=False, max_prior_peak_mult=0, min_rise_2m_pct=0, min_rise_1m_pct=None, min_buys_2m=0)
        return c

    def from_survivor(self, cd, px, now):
        """Survivor saw an older coin rise through the line: take it too (it's a skim, no extra rules)."""
        if not (self.active and self.enabled()) or cd["mint"] in self.traded or cd["mint"] in self.positions:
            return
        c = self.cfg()
        usd = self._usd()
        if not usd or len(self.positions) >= int(c.get("max_open", 300)):
            return
        self.traded.add(cd["mint"])
        size = float(c.get("size_usd", 10)) / usd
        fee, slip, prio = self._x()
        from memebot.lookalike import curve_buy
        tokens = curve_buy(size * (1 - fee), px) * (1 - slip)
        self.positions[cd["mint"]] = dict(
            mint=cd["mint"], symbol=cd["symbol"], name=cd.get("name", ""), opened=now, entry_px=px,
            entry_mcap=round(px * SUPPLY, 1), age_at_entry_s=round(now - cd["created"]), sol_in=size + prio, sol_out=0.0,
            tokens=tokens, tokens_bought=tokens, peak_mult=1.0, floor_mult=0.0, done=[], last_px=px, last_px_ts=now,
            sells=[], verified=True, size_usd=round(size * usd, 2))
        self._event("buy", f"Paper buy {cd['symbol']} at {px * SUPPLY:.0f} SOL mcap ({(now - cd['created']) / 60:.0f} min old)",
                    mint=cd["mint"])

    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        if mult >= 1 + float(c.get("tp_pct", 20)) / 100:
            return self._sell(p, 1.0, f"take profit +{c.get('tp_pct', 20):g}%", px, ts)
        if mult <= 1 - float(c.get("sl_pct", 5)) / 100:
            return self._sell(p, 1.0, f"down {c.get('sl_pct', 5):g}% - out", px, ts)
        if ts - p["opened"] > float(c.get("max_hold_min", 15)) * 60:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_min', 15):g} min", px, ts)

    def state(self):
        s = super().state()
        c = self.cfg()
        cl = self.closed
        wins = [x for x in cl if x["pnl_sol"] > 0]
        loss = [x for x in cl if x["pnl_sol"] <= 0]
        days = max((time.time() - min((x["opened"] for x in cl), default=time.time())) / 86400, 1 / 24)
        s.update(name=self.NAME, avg_win_pct=round(sum(x["pnl_pct"] for x in wins) / len(wins), 1) if wins else None,
                 avg_loss_pct=round(sum(x["pnl_pct"] for x in loss) / len(loss), 1) if loss else None,
                 per_day=round(len(cl) / days), size_usd=c.get("size_usd", 10),
                 desc=(f"Buys every coin rising through {c.get('entry_mcap_sol', 44)} SOL · sells at +{c.get('tp_pct', 20):g}% "
                       f"or at −{c.get('sl_pct', 5):g}% or after {c.get('max_hold_min', 15):g} min · ${c.get('size_usd', 10)} each · "
                       f"fees & slippage included · no real money"))
        return s
