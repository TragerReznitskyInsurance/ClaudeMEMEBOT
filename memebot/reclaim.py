"""
"Reclaim" paper strategy - buy a second wave instead of racing the first one.

Candidates: coins the bot watched that made a real first run (peak >= run_mcap_sol, 80 SOL).
They are then tracked on-chain (bonding curve, batched) for up to max_age_h.

Buy (paper) when ALL of:
  - the coin is at least min_age_min old (30 min)
  - it pulled back >= pullback_pct (40%) from its peak
  - it bounced >= bounce_pct (25%) off the low since then
  - it is still <= max_of_peak_pct (80%) of the old peak (not already fully recovered)
  - ...but back to >= min_of_peak_pct (55%) of it: coins that crashed far below their spike and only bounce
    a little lose (1 Oct analysis: entries under 55% of the peak won 31% and lost money; 55%+ won 47-55%)
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
        self.cands_path = os.path.join(data_dir, f"{self.NAME}_candidates.json")
        self.cands: dict[str, dict] = {}
        self.known: set[str] = set()
        self.track: dict[str, dict] = {}                   # research: price path from buy until 6 h after the sale
        try:
            with open(self.cands_path, encoding="utf-8", errors="replace") as fh:
                s = json.load(fh)
            self.cands, self.known = s.get("cands", {}), set(s.get("known", []))
            self.track = s.get("track", {})
        except (OSError, ValueError):
            pass

    def _save(self):
        super()._save()
        if not hasattr(self, "cands_path"):
            return                                         # called from the base constructor
        try:
            tmp = self.cands_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"cands": self.cands, "known": sorted(self.known)[-20000:], "track": self.track}, fh)
            os.replace(tmp, self.cands_path)
        except OSError:
            pass

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        real = self._cfg().get("lookalike_grad") or {}            # the real wallet's money settings (same wallet)
        for k in ("real_enabled", "real_size_usd", "real_max_open", "real_daily_loss_usd"):
            if k in real:
                c[k] = real[k]
        return c

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
        if self.NAME == "reclaim":
            self.track[cd["mint"]] = dict(entry_px=px, opened=ts, until=ts + 30 * 3600, last=0.0, sym=cd["symbol"],
                                          cid=f"path:{cd['mint']}:{int(ts)}")
        log.info("RECLAIM buy %s at mcap %.0f (peak %.0f, low %.0f, %d trades/2m)",
                 cd["symbol"], mc, cd["peak"], cd["low"], trades_2m)
        if c.get("real_enabled") and self.live is not None:          # the real wallet trades Reclaim
            why = self.live.open_strategy(self.NAME, cd["mint"], cd["symbol"], float(c.get("real_size_usd", 10)),
                                          int(c.get("real_max_open", 20)), float(c.get("real_daily_loss_usd", 25)))
            if why is None:
                self.positions[cd["mint"]]["real"] = True
                self._event("buy", f"{cd['symbol']}: REAL buy placed (${float(c.get('real_size_usd', 10)):.2f})",
                            mint=cd["mint"])
            else:
                self._event("skip", f"{cd['symbol']}: no real buy - {why}", mint=cd["mint"])

    async def _enter_checked(self, cd, px, ts, trades_2m):
        self._enter(cd, px, ts, trades_2m)

    def _log_signal(self, cd, mc, age, now, c):
        bl = getattr(self, "breakouts", None)
        if bl is None or not self._key():
            return
        import asyncio
        from memebot.volume import recent_volume
        passes = (mc >= cd["peak"] * float(c.get("min_of_peak_pct", 0) or 0) / 100
                  and mc >= float(c.get("min_entry_mcap_sol", 35)))

        async def go():
            try:
                v5 = await recent_volume(await self._sess(), self._key(), cd["mint"], 300)
            except Exception as e:
                v5 = {"error": str(e)[:60]}
            bl.signal(dict(type="reclaim_signal", sym=cd["symbol"], mcap=round(mc, 1), peak=round(cd["peak"], 1),
                           low=round(cd["low"] or mc, 1), of_peak_pct=round(mc / cd["peak"] * 100, 1),
                           age_min=round(age / 60, 1), passes_filters=passes, vol_5m=v5), cd["mint"], now)
        try:
            asyncio.get_running_loop().create_task(go())
        except RuntimeError:
            pass

    # ------------------------------------------------------------------ exits
    def _close(self, p, ts):
        super()._close(p, ts)
        tr = self.track.get(p["mint"])
        if tr:                                             # keep following it for 6 h after we sold
            tr.update(until=ts + 6 * 3600, exit_ts=ts, exit=p["sells"][-1]["reason"] if p.get("sells") else "")

    def _sample_paths(self, px, now):
        bl = getattr(self, "breakouts", None)
        for m, tr in list(self.track.items()):
            if now > tr["until"]:
                self.track.pop(m, None)
                continue
            if now - tr.get("last", 0) < 60:
                continue
            price, src = px.get(m, (None, None))
            if not price or not tr.get("entry_px"):
                continue
            tr["last"] = now
            if bl is not None:
                bl._write(dict(type="path", id=tr["cid"], mint=m, sym=tr.get("sym"), min=round((now - tr["opened"]) / 60, 1),
                               mult=round(price / tr["entry_px"], 4), held=m in self.positions,
                               after_exit_min=round((now - tr["exit_ts"]) / 60, 1) if tr.get("exit_ts") else None,
                               graduated=src == "jupiter"))

    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        be = float(c.get("be_after_pct", 0) or 0)            # break-even stop (real wallet since 1 Oct 17:45)
        if be and p["peak_mult"] >= 1 + be / 100 and mult <= float(c.get("be_floor_mult", 1.0)):
            return self._sell(p, 1.0, f"back to entry after +{be:g}%", px, ts)
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
        if not self._key() or not (self.cands or self.positions or self.track):
            return
        c = self.cfg()
        now = time.time()
        due = [m for m, tr in self.track.items() if now - tr.get("last", 0) >= 60 and m not in self.positions]
        px = await self.prices(list(self.cands) + list(self.positions) + due)
        self._sample_paths(px, now)
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
            signal = (cd["pulled"] and age >= float(c.get("min_age_min", 30)) * 60
                      and mc >= cd["low"] * (1 + float(c.get("bounce_pct", 25)) / 100)
                      and mc <= cd["peak"] * float(c.get("max_of_peak_pct", 80)) / 100)
            if signal and self.NAME == "reclaim" and now - cd.get("logged_at", 0) >= 600:
                cd["logged_at"] = now                       # research: volume & launch speed of every signal
                self._log_signal(cd, mc, age, now, c)
            if (signal
                    and mc >= cd["peak"] * float(c.get("min_of_peak_pct", 0) or 0) / 100   # held up, not a crash
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
                    await self._enter_checked(cd, price, now, n)
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
        s.update(name=self.NAME, candidates=len(self.cands), tp_mult=c.get("tp_mult", 2),
                 pulled=sum(1 for x in self.cands.values() if x.get("pulled")),
                 desc=(f"Coins that ran to {c.get('run_mcap_sol', 80)}+ SOL, pulled back {c.get('pullback_pct', 40)}%+, "
                       f"then bounce {c.get('bounce_pct', 25)}% with {c.get('min_trades_2m', 15)}+ trades/2 min "
                       f"(30+ min old) · ${c.get('size_usd', 25)} each · no real money"))
        return s



class ReclaimClean(Reclaim):
    """PAPER test: Reclaim, but it skips coins whose launch was bundled - where the creator plus the wallets
    that bought in the same block as the launch took more than `max_bundle_pct` (12%) of the supply.
    (Wallet #1's buys: median 6% bundled, 82% under 12%; other coins crossing 44 SOL: median 19%.)
    Uses the `reclaim` settings; never real money."""
    NAME = "reclaim_clean"

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.bundle_cache: dict[str, float] = {}

    def cfg(self):
        c = dict(self._cfg().get("reclaim") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    async def _bundled_pct(self, mint):
        if mint in self.bundle_cache:
            return self.bundle_cache[mint]
        from memebot import launchcheck
        r = await launchcheck.check(await self._sess(), self._key(), mint)
        if r.get("error"):
            return None
        pct = (r.get("bundle") or {}).get("supply_pct", 0) + (r.get("creator_buy") or {}).get("supply_pct", 0)
        self.bundle_cache[mint] = pct
        return pct

    async def _enter_checked(self, cd, px, ts, trades_2m):
        c = self.cfg()
        try:
            pct = await self._bundled_pct(cd["mint"])
        except Exception as e:
            self.last_error = f"bundle check: {e}"[:100]
            pct = None
        cap = float(c.get("max_bundle_pct", 12))
        if pct is None:
            self.traded.add(cd["mint"])
            return self._event("skip", f"{cd['symbol']}: couldn't read its launch - skipped", mint=cd["mint"])
        if pct > cap:
            self.traded.add(cd["mint"])
            w = getattr(self, "why", None)
            if w is not None:
                w.note(cd["mint"], cd["symbol"], self.NAME, f"launch was {pct:.0f}% bundled (max {cap:g}%)", ts)
            return self._event("skip", f"{cd['symbol']}: launch {pct:.0f}% bundled (max {cap:g}%) - skipped", mint=cd["mint"])
        self._enter(cd, px, ts, trades_2m)
        p = self.positions.get(cd["mint"])
        if p:
            p["bundle_pct"] = round(pct, 1)

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME, desc=(f"Reclaim, but only coins whose launch was at most {c.get('max_bundle_pct', 12):g}% bundled "
                                       f"(creator + same-block wallets) · ${c.get('size_usd', 25)} each · no real money"))
        return s


class ReclaimBig(Reclaim):
    """PAPER test: Reclaim, but only coins that are still big when it would buy - at least `min_entry_mcap_sol`
    (100 SOL). 1 Oct analysis of 78 Reclaim trades: bought at 130+ SOL won 50% and made +$555, under 80 SOL lost;
    55%+ of the peak AND 100+ SOL: 18 trades, 56% wins, +$547. Uses the `reclaim` settings; never real money."""
    NAME = "reclaim_big"

    def cfg(self):
        c = dict(self._cfg().get("reclaim") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME, desc=(f"Reclaim, but only coins still worth {c.get('min_entry_mcap_sol', 100):g}+ SOL when it buys "
                                       f"(back to {c.get('min_of_peak_pct', 55):g}%+ of the old high) · ${c.get('size_usd', 25)} each · no real money"))
        return s


class ReclaimBE(Reclaim):
    """PAPER test: Reclaim with the same buys, but once a coin has been up `be_after_pct` (20%), the stop moves to
    our buy price (`be_floor_mult` 1.0x): no more "was +20%, closed -30%". 1 Oct analysis: 64% of Reclaim buys went
    +20%, and 27 of those still ended around -27%. Uses the `reclaim` settings; never real money."""
    NAME = "reclaim_be"

    def cfg(self):
        c = dict(self._cfg().get("reclaim") or {})
        c.pop("stats_since", None)
        c.update(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    def check(self, p, px, ts):
        c = self.cfg()
        mult = px / p["entry_px"]
        peak = max(p["peak_mult"], mult)
        if peak >= 1 + float(c.get("be_after_pct", 20)) / 100 and mult <= float(c.get("be_floor_mult", 1.0)):
            p["last_px"], p["last_px_ts"], p["peak_mult"] = px, ts, peak
            return self._sell(p, 1.0, f"back to entry after +{c.get('be_after_pct', 20):g}%", px, ts)
        return super().check(p, px, ts)

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME, desc=(f"Same buys as Reclaim, but once a coin is up {c.get('be_after_pct', 20):g}% the stop "
                                       f"moves to our buy price · ${c.get('size_usd', 25)} each · no real money"))
        return s
