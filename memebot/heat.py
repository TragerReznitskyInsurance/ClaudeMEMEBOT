"""
Bond alerts (15 Oct): a phone push when bonds come back for the strategy real money trades.

Watches the 'Hold <$12K · 10 trades' paper test (it keeps running while real money is off):
  1. BONDS PICKING UP - >= `early_bonds_6h` (2) bonds in the last 6 h. Early heads-up.
  2. RESTART SIGNAL   - >= `restart_bonds_24h` (3) bonds in the last 24 h AND green over those 24 h (the rule agreed
     for switching real money back on).
Each is pushed at most once per `cooldown_h` (12 h).
  3. AUTO ON/OFF (16 Oct) - when real money is in auto mode, a push each time auto real buying switches on or off. (A market-wide bond count from pump.fun's lists was tried
first, but it barely moved between good and cold days - our own bond rate is the signal that matters.)
Nothing here trades. Settings: `heat` section in config.yaml.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

log = logging.getLogger("memebot")


class MarketHeat:
    def __init__(self, data_dir, cfg_getter, notifier, test_getter):
        self.path = os.path.join(data_dir, "market_heat.json")
        self._cfg = cfg_getter
        self.notifier = notifier
        self.test = test_getter                 # -> the rangehold_small10 strategy
        self.last_sent = {"early": 0.0, "restart": 0.0}
        self.auto_hot = None                    # last seen auto state (None = not known yet)
        try:
            with open(self.path, encoding="utf-8") as fh:
                d = json.load(fh) or {}
                self.last_sent.update(d.get("last_sent") or {})
                self.auto_hot = d.get("auto_hot")
        except (OSError, ValueError):
            pass

    def cfg(self):
        return self._cfg().get("heat") or {}

    def _save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(dict(last_sent=self.last_sent, auto_hot=self.auto_hot), fh)
            os.replace(tmp, self.path)
        except OSError:
            pass

    def window(self, hours, now=None):
        """(bonds, P&L $, trades) of the test's trades closed in the last `hours`."""
        now = now or time.time()
        cl = [x for x in getattr(self.test(), "closed", []) if 0 <= now - x.get("closed", 0) <= hours * 3600]
        bonds = sum(1 for x in cl if str(x.get("exit", "")).startswith("bonding"))
        return bonds, round(sum(x.get("pnl_usd") or 0 for x in cl), 2), len(cl)

    def check_auto(self, now=None):
        """Push when auto real buying flips on/off (only while real money is on in auto mode)."""
        t = self.test()
        if t is None or not hasattr(t, "auto_status"):
            return
        tc = t.cfg()
        if not (tc.get("real_enabled") and tc.get("real_auto")):
            self.auto_hot = None
            return
        hot, b24, p24 = t.auto_status(now, tc)
        if self.auto_hot is not None and hot != self.auto_hot and self.notifier is not None:
            need = int(tc.get("real_auto_bonds_24h", 2))
            if hot:
                self.notifier.send("Real money AUTO ON",
                                   f"Hold <$12K · 10 trades had {b24} bonds in the last 24 h "
                                   f"({'+' if p24 >= 0 else '-'}${abs(p24):.0f}). Real buys are back on: "
                                   f"${float(tc.get('real_size_usd', 10)):g} each.", tags="rocket", priority=4, force=True)
            else:
                self.notifier.send("Real money AUTO OFF",
                                   f"Market cold: only {b24} bond(s) in the last 24 h (needs {need}). No new real buys; "
                                   f"open real coins still sell normally.", tags="snowflake", priority=3, force=True)
            log.info("heat: real money auto %s (%d bonds 24h)", "ON" if hot else "OFF", b24)
        self.auto_hot = hot

    def check(self, now=None):
        now = now or time.time()
        c = self.cfg()
        try:
            self.check_auto(now)
        except Exception as e:
            log.info("heat auto check: %s", e)
        if not c.get("enabled", True) or self.notifier is None:
            return
        cool = float(c.get("cooldown_h", 12)) * 3600
        b6, p6, n6 = self.window(6, now)
        b24, p24, n24 = self.window(24, now)
        if b24 >= int(c.get("restart_bonds_24h", 3)) and p24 > 0 and now - self.last_sent["restart"] > cool:
            self.notifier.send("Restart signal: bonds are back",
                               f"Hold <$12K · 10 trades: {b24} bonds and +${p24:.0f} over the last 24 h ({n24} trades). "
                               f"(Real money in auto mode switches itself on at 2+ bonds.)",
                               tags="rocket", priority=4, force=True)
            self.last_sent["restart"] = self.last_sent["early"] = now
            log.info("heat: restart push (%d bonds, $%.0f)", b24, p24)
        elif b6 >= int(c.get("early_bonds_6h", 2)) and now - self.last_sent["early"] > cool:
            self.notifier.send("Bonds picking up",
                               f"Hold <$12K · 10 trades: {b6} bonds in the last 6 h ({'+' if p6 >= 0 else '-'}${abs(p6):.0f}). "
                               f"Last 24 h: {b24} bonds, {'+' if p24 >= 0 else '-'}${abs(p24):.0f}. Restart signal is "
                               f"{c.get('restart_bonds_24h', 3)}+ bonds and green over 24 h.",
                               tags="chart_with_upwards_trend", priority=3, force=True)
            self.last_sent["early"] = now
            log.info("heat: early push (%d bonds in 6h)", b6)

    def state(self):
        b6, p6, n6 = self.window(6)
        b24, p24, n24 = self.window(24)
        c = self.cfg()
        return dict(bonds_6h=b6, pnl_6h=p6, bonds_24h=b24, pnl_24h=p24, trades_24h=n24,
                    early=int(c.get("early_bonds_6h", 2)), restart=int(c.get("restart_bonds_24h", 3)),
                    restart_ready=b24 >= int(c.get("restart_bonds_24h", 3)) and p24 > 0, last_sent=self.last_sent)

    async def run(self, every=600):
        await asyncio.sleep(300)
        while True:
            try:
                self.check()
                self._save()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.info("heat check: %s", e)
            await asyncio.sleep(every)
