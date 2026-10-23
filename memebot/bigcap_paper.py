"""
PAPER test (22 Oct, owner's request): "Big caps · volume spike · ride 2 days".

Watches the big-cap feed (Research2 -> DexScreener every minute for bonded / graduated coins worth $50K+) and paper
buys a coin when, at $150K+ market cap, its volume spikes:
  * 5-minute volume >= `spike_mult` (2) x its average 5-minute volume over the last hour, and at least `min_v5_usd`
  * more buys than sells in those 5 minutes, price up over those 5 minutes
  * liquidity >= `min_liq_usd`, market cap between `min_mc_usd` and `max_mc_usd`
$`size_usd` (25) per coin, one buy per coin, no stop: held `hold_h` (48) hours, then sold at the market. `fee_pct`
(2%) is charged on the way in and out. Peak / low / time-to-peak are kept on every trade to design exits later.
Never real money. Settings: `bigcap_paper` section in config.yaml.
"""
from __future__ import annotations

import json
import logging
import os
import time

log = logging.getLogger("memebot")


class BigCapPaper:
    NAME = "bigcap_paper"
    VERSION = 2          # 23 Oct: v1 (any age) bought fresh migrations that rugged - archived, restarted with v2 rules

    def __init__(self, data_dir, cfg_getter):
        self.path = os.path.join(data_dir, "bigcap_paper_state.json")
        self.trades_path = os.path.join(data_dir, "bigcap_paper_trades.jsonl")
        self._cfg = cfg_getter
        self.positions: dict[str, dict] = {}
        self.closed: list[dict] = []
        self.traded: set[str] = set()
        try:
            with open(self.path, encoding="utf-8") as fh:
                s = json.load(fh) or {}
            if s and s.get("version", 1) < self.VERSION:   # rules changed: keep the old run for analysis, start fresh
                os.replace(self.path, self.path.replace(".json", f"_v{s.get('version', 1)}.json"))
                s = {}
            self.positions = s.get("positions") or {}
            self.closed = s.get("closed") or []
            self.traded = set(s.get("traded") or [])
        except (OSError, ValueError):
            pass

    def cfg(self):
        return self._cfg().get(self.NAME) or {}

    def enabled(self):
        return bool(self.cfg().get("enabled", True))

    def holding(self, mint):
        return mint in self.positions

    def _save(self):
        try:
            with open(self.path + ".tmp", "w", encoding="utf-8") as fh:
                json.dump(dict(version=self.VERSION, positions=self.positions, closed=self.closed[-2000:], traded=list(self.traded)[-20000:]), fh)
            os.replace(self.path + ".tmp", self.path)
        except OSError:
            pass

    # ------------------------------------------------------------------ feed
    def on_snapshot(self, m, row, price, symbol, now):
        """One DexScreener snapshot of a tracked coin (row = the bigcap.jsonl record)."""
        if price is None or price <= 0:
            return
        p = self.positions.get(m)
        if p is not None:
            mult = price / p["entry_px"]
            p["last_px"], p["last_mc"], p["last_ts"] = price, row.get("mc"), now
            if mult > p["peak_mult"]:
                p["peak_mult"], p["peak_min"] = round(mult, 3), round((now - p["opened"]) / 60)
            p["low_mult"] = round(min(p["low_mult"], mult), 3)
            if now - p["opened"] >= float(self.cfg().get("hold_h", 48)) * 3600:
                self._close(m, now, "held 48h")
            return
        if not self.enabled() or m in self.traded:
            return
        c = self.cfg()
        mc, v5, v1h = row.get("mc") or 0, row.get("v5") or 0, row.get("v1h") or 0
        b5, s5, pc5, liq = row.get("b5") or 0, row.get("s5") or 0, row.get("pc5") or 0, row.get("liq") or 0
        avg5 = v1h / 12 if v1h else 0
        if not (float(c.get("min_mc_usd", 150000)) <= mc <= float(c.get("max_mc_usd", 5000000))):
            return
        if liq < float(c.get("min_liq_usd", 15000)) or v5 < float(c.get("min_v5_usd", 3000)):
            return
        bh = row.get("bond_h")                          # hours since it bonded (None = bonded before we saw it)
        if bh is not None and bh < float(c.get("min_hours_since_bond", 0)):
            return                                      # 23 Oct: fresh migrations spike then rug - skip them
        if not (avg5 and v5 >= float(c.get("spike_mult", 2.0)) * avg5 and b5 > s5 and pc5 > 0):
            return
        self.traded.add(m)
        size = float(c.get("size_usd", 25))
        self.positions[m] = dict(mint=m, symbol=(symbol or m[:5])[:20], opened=now, entry_px=price, entry_mc=mc,
                                 size_usd=size, last_px=price, last_mc=mc, last_ts=now, peak_mult=1.0, peak_min=0,
                                 low_mult=1.0, entry=dict(v5=v5, v1h=v1h, spike=round(v5 / avg5, 1), b5=b5, s5=s5,
                                                          pc5=pc5, liq=liq))
        log.info("BIGCAP paper buy %s at $%.0fK (5m volume %.1fx its hourly pace)", symbol, mc / 1000, v5 / avg5)
        self._save()

    def tick(self, now):
        """Close anything past the hold time even if the coin stopped updating (dead / delisted)."""
        for m, p in list(self.positions.items()):
            if now - p["opened"] >= float(self.cfg().get("hold_h", 48)) * 3600:
                self._close(m, now, "held 48h" if now - p["last_ts"] < 3600 else "held 48h (no price lately)")

    def _close(self, m, now, why):
        p = self.positions.pop(m, None)
        if not p:
            return
        fee = float(self.cfg().get("fee_pct", 2)) / 100
        mult = p["last_px"] / p["entry_px"]
        out = p["size_usd"] * (1 - fee) * mult * (1 - fee)
        rec = dict(p, closed=now, exit=why, exit_mult=round(mult, 3), pnl_usd=round(out - p["size_usd"], 2))
        self.closed.append(rec)
        try:
            with open(self.trades_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except OSError:
            pass
        self._save()

    # ------------------------------------------------------------------ dashboard
    def state(self):
        fee = float(self.cfg().get("fee_pct", 2)) / 100
        now = time.time()
        opn = []
        for p in sorted(self.positions.values(), key=lambda p: -p["opened"]):
            mult = p["last_px"] / p["entry_px"]
            opn.append(dict(symbol=p["symbol"], mint=p["mint"], entry_mc=p["entry_mc"], now_mc=p["last_mc"],
                            mult=round(mult, 2), peak=p["peak_mult"], low=p["low_mult"],
                            pnl_usd=round(p["size_usd"] * ((1 - fee) ** 2 * mult - 1), 2),
                            age_h=round((now - p["opened"]) / 3600, 1), spike=(p.get("entry") or {}).get("spike")))
        real = round(sum(c["pnl_usd"] for c in self.closed), 2)
        unreal = round(sum(x["pnl_usd"] for x in opn), 2)
        c = self.cfg()
        cum, curve = 0.0, []
        for x in sorted(self.closed, key=lambda x: x["closed"]):
            cum += x["pnl_usd"]
            curve.append([round(x["closed"]), round(cum, 2)])
        wins = sum(1 for x in self.closed if x["pnl_usd"] > 0)
        return dict(name=self.NAME, enabled=self.enabled(), open=len(opn), closed=len(self.closed), bonded=None,
                    curve=curve[-500:], win_rate=round(wins / len(self.closed) * 100) if self.closed else None,
                    realized_usd=real, open_pnl_usd=unreal, positions=opn[:60],
                    recent=[dict(symbol=x["symbol"], entry_mc=x["entry_mc"], exit_mult=x["exit_mult"], peak=x["peak_mult"],
                                 pnl_usd=x["pnl_usd"]) for x in self.closed[-15:][::-1]],
                    wins=sum(1 for x in self.closed if x["pnl_usd"] > 0),
                    desc=(f"Bonded/graduated coins at ${float(c.get('min_mc_usd', 150000)) / 1000:g}K+, bonded {float(c.get('min_hours_since_bond', 0)):g}h+ ago, "
                          f"liquidity ${float(c.get('min_liq_usd', 15000)) / 1000:g}K+, whose 5-min volume jumps to "
                          f"{float(c.get('spike_mult', 2)):g}x+ their hourly pace (buys > sells, price up) · "
                          f"${float(c.get('size_usd', 25)):g} each · no stop, sold after {float(c.get('hold_h', 48)):g}h · paper only"))
