"""
Bond alerts (15 Oct): a phone push when bonds come back for the strategy real money trades.

Watches the 'Hold <$12K · 10 trades' paper test (it keeps running while real money is off):
  1. BONDS PICKING UP - >= `early_bonds_6h` (2) bonds in the last 6 h. Early heads-up.
  2. RESTART SIGNAL   - >= `restart_bonds_24h` (3) bonds in the last 24 h AND green over those 24 h (the rule agreed
     for switching real money back on).
Each is pushed at most once per `cooldown_h` (12 h). (A market-wide bond count from pump.fun's lists was tried
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
        try:
            with open(self.path, encoding="utf-8") as fh:
                self.last_sent.update((json.load(fh) or {}).get("last_sent") or {})
        except (OSError, ValueError):
            pass

    def cfg(self):
        return self._cfg().get("heat") or {}

    def _save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(dict(last_sent=self.last_sent), fh)
            os.replace(tmp, self.path)
        except OSError:
            pass

    def window(self, hours, now=None):
        """(bonds, P&L $, trades) of the test's trades closed in the last `hours`."""
        now = now or time.time()
        cl = [x for x in getattr(self.test(), "closed", []) if 0 <= now - x.get("closed", 0) <= hours * 3600]
        bonds = sum(1 for x in cl if str(x.get("exit", "")).startswith("bonding"))
        return bonds, round(sum(x.get("pnl_usd") or 0 for x in cl), 2), len(cl)

    def check(self, now=None):
        now = now or time.time()
        c = self.cfg()
        if not c.get("enabled", True) or self.notifier is None:
            return
        cool = float(c.get("cooldown_h", 12)) * 3600
        b6, p6, n6 = self.window(6, now)
        b24, p24, n24 = self.window(24, now)
        if b24 >= int(c.get("restart_bonds_24h", 3)) and p24 > 0 and now - self.last_sent["restart"] > cool:
            self.notifier.send("Restart signal: bonds are back",
                               f"Hold <$12K · 10 trades: {b24} bonds and +${p24:.0f} over the last 24 h ({n24} trades). "
                               f"Real money can go back on with the ▶ button on the real wallet card.",
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
