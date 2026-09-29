"""
"Lookalike" paper strategy - wallet #1's entry with Eugene's exit plan. No real money.

Entry: a coin the bot is watching rises through `entry_mcap_sol` (wallet #1 buys at ~44 SOL)
and is at least `min_age_s` old. One buy per coin, fixed USD size, realistic fees/slippage.

Exit (multiples of the entry price):
  - floor: sell everything if the price falls to (1 - stop_pct) x entry  (0.7x)
  - 2x:    sell enough to get the initial cost back; the rest rides for free
  - 6x:    sell tp6_sell_pct of what's left, and raise the floor to 1.0x (break-even)
  - 10x:   sell tp10_sell_pct of what's left
  - 100x:  sell everything

Open positions are priced every few seconds from the pump.fun bonding curve (Jupiter after
graduation) - or the live feed while the coin is still watched - so no feed subscriptions are
needed to hold hundreds of paper positions for days. State survives restarts
(data/lookalike_state.json); every sale goes to data/lookalike_fills.csv, finished trades to
data/lookalike_trades.csv.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time

import aiohttp

from memebot.chain import JUP_PRICE, curve_address, parse_curve

log = logging.getLogger("memebot")
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"
SUPPLY = 1_000_000_000
K = 30.0 * 1_073_000_000          # pump.fun bonding curve: virtual SOL x virtual tokens (constant product)
CURVE_MAX_MCAP = 450.0            # above this the coin has graduated to a pool: use a flat price instead


def curve_buy(sol_net, px):
    """Tokens received for `sol_net` SOL at spot price `px`, including the buy's own price impact."""
    if px * SUPPLY > CURVE_MAX_MCAP:
        return sol_net / px
    vs = (px * K) ** 0.5
    vt = K / vs
    return vt - K / (vs + sol_net)


def curve_sell(tokens, px):
    """SOL received for selling `tokens` at spot price `px`, including the sale's own price impact."""
    if px * SUPPLY > CURVE_MAX_MCAP:
        return tokens * px
    vs = (px * K) ** 0.5
    vt = K / vs
    return vs - K / (vt + tokens)


class Lookalike:
    def __init__(self, data_dir, cfg_getter, key_getter, sol_usd_getter):
        self.dir = data_dir
        self.state_path = os.path.join(data_dir, "lookalike_state.json")
        self._cfg = cfg_getter
        self._key = key_getter
        self._usd = sol_usd_getter
        self.positions: dict[str, dict] = {}
        self.closed: list[dict] = []
        self.traded: set[str] = set()
        self.events: list[dict] = []
        self.feed_price = lambda mint: None        # set by the app: (price, age_s) from the live feed, or None
        self.live = None                           # LiveTrader, set by the app: real-money mirror of this strategy
        self.active = False
        self.session: aiohttp.ClientSession | None = None
        self.last_error = ""
        self._load()
        self._repair()

    # ------------------------------------------------------------------ config / state
    def cfg(self):
        return self._cfg().get("lookalike") or {}

    def enabled(self):
        return bool(self.cfg().get("enabled", True))

    def _load(self):
        try:
            with open(self.state_path, encoding="utf-8", errors="replace") as fh:
                s = json.load(fh)
            self.positions = s.get("positions", {})
            self.closed = s.get("closed", [])
            self.traded = set(s.get("traded", []))
        except (OSError, ValueError):
            pass

    def _repair(self):
        """Remove trades created by bad price readings (before the sanity checks existed): any coin 'sold' above
        11x within 10 minutes of the buy - impossible on a pump.fun curve (44 -> ~450 SOL is the whole curve)."""
        path = os.path.join(self.dir, "lookalike_fills.csv")
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError:
            return
        if len(lines) < 2:
            return
        head, rows = lines[0], [ln.split(",") for ln in lines[1:] if ln.strip()]
        bought, bad = {}, set()
        for r in rows:
            if len(r) < 8:
                continue
            try:
                ts = time.mktime(time.strptime(r[0], "%Y-%m-%d %H:%M:%S"))
                mult = float(r[5])
            except ValueError:
                continue
            if r[3] == "BUY":
                bought[r[1]] = ts
            elif r[1] in bought and mult > 11 and ts - bought[r[1]] < 600:
                bad.add(r[1])
        if not bad:
            return
        self.closed = [c for c in self.closed if c["mint"] not in bad]
        for m in bad:
            self.positions.pop(m, None)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(head + "\n" + "".join(",".join(r) + "\n" for r in rows if len(r) < 2 or r[1] not in bad))
        os.replace(tmp, path)
        tpath = os.path.join(self.dir, "lookalike_trades.csv")
        try:
            with open(tpath, encoding="utf-8", errors="replace") as fh:
                tl = fh.read().splitlines()
            with open(tpath + ".tmp", "w", encoding="utf-8") as fh:
                keep = [ln for i, ln in enumerate(tl) if i == 0 or (ln.split(",") + ["", "", ""])[2] not in bad]
                fh.write("".join(ln + "\n" for ln in keep))
            os.replace(tpath + ".tmp", tpath)
        except (OSError, IndexError):
            pass
        self._save()
        log.info("LOOKALIKE removed %d trade(s) caused by bad price readings", len(bad))

    def _save(self):
        tmp = self.state_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"positions": self.positions, "closed": self.closed[-3000:],
                           "traded": sorted(self.traded)[-20000:]}, fh)
            os.replace(tmp, self.state_path)
        except OSError as e:
            self.last_error = str(e)

    def _csv(self, name, head, row):
        path = os.path.join(self.dir, name)
        try:
            new = not os.path.exists(path)
            with open(path, "a", encoding="utf-8", errors="replace") as fh:
                if new:
                    fh.write(head + "\n")
                fh.write(",".join(str(x).replace(",", " ") for x in row) + "\n")
        except OSError:
            pass

    def _event(self, kind, text, **extra):
        self.events.append(dict(ts=time.time(), kind=kind, text=text, **extra))
        self.events = self.events[-150:]

    # ------------------------------------------------------------------ costs (same model as the paper engine)
    def _x(self):
        x = self._cfg().get("execution") or {}
        return (x.get("platform_fee_pct", 1.25) / 100, x.get("extra_slippage_pct", 2.0) / 100,
                x.get("priority_fee_sol", 0.0005))

    def _sell_value(self, tokens, px):
        fee, slip, prio = self._x()
        return max(curve_sell(tokens, px) * (1 - slip) * (1 - fee) - prio, 0.0)

    # ------------------------------------------------------------------ entry (called by the engine)
    def maybe_enter(self, t, prev_mcap, ts):
        """Engine hook on every trade of a watched coin."""
        if not (self.active and self.enabled()) or t.mint in self.traded or not t.price or not t.mcap or not prev_mcap:
            return False
        c = self.cfg()
        line = float(c.get("entry_mcap_sol", 44))
        if not (prev_mcap < line <= t.mcap):
            return False
        if ts - t.created_ts < float(c.get("min_age_s", 300)):
            return False
        if t.sec_state == "failed" or t.dev_sold and c.get("skip_if_dev_sold", False):
            return False
        self.traded.add(t.mint)
        if len(self.positions) >= int(c.get("max_open", 300)):
            self._event("skip", f"{t.symbol}: max {c.get('max_open', 300)} paper positions open")
            return True
        usd = self._usd()
        if not usd:
            return True
        size = float(c.get("size_usd", 2.5)) / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), t.price) * (1 - slip)     # bigger buys pay more price impact
        self.positions[t.mint] = dict(
            mint=t.mint, symbol=t.symbol, name=t.name, opened=ts, entry_px=t.price, entry_mcap=round(t.mcap, 1),
            age_at_entry_s=round(ts - t.created_ts), sol_in=size + prio, sol_out=0.0, tokens=tokens,
            tokens_bought=tokens, peak_mult=1.0, floor_mult=1.0 - float(c.get("stop_pct", 30)) / 100,
            done=[], last_px=t.price, last_px_ts=ts, sells=[], verified=False, size_usd=round(size * usd, 2))
        self._save()
        try:
            asyncio.get_running_loop().create_task(self._quick_verify(t.mint))
        except RuntimeError:
            pass                                           # no running loop (tests): the price loop verifies it
        return True

    async def _quick_verify(self, mint):
        try:
            px = await self.prices([mint])
        except Exception as e:
            self.last_error = str(e) or type(e).__name__
            return
        p = self.positions.get(mint)
        if p and not p.get("verified", True):
            price, src = px.get(mint, (0.0, "none"))
            self._verify(p, price, src, time.time())
            self._save()

    def _verify(self, p, px, src, ts):
        """First on-chain look at a new paper buy: must be a live pump.fun curve coin priced like the feed said."""
        if src != "curve" or not (0.7 <= px / p["entry_px"] <= 1.3):
            self.positions.pop(p["mint"], None)
            why = "not a pump.fun bonding-curve coin" if src != "curve" else \
                f"on-chain price didn't match the feed ({px / p['entry_px']:.2f}x)"
            self._event("skip", f"{p['symbol']}: skipped - {why}", mint=p["mint"])
            log.info("LOOKALIKE skipped %s: %s", p["symbol"], why)
            return False
        p["verified"] = True
        self._csv("lookalike_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(p["opened"])), p["mint"], p["symbol"], "BUY",
                   f"entry at {p['entry_mcap']:.0f} SOL mcap", "1.00", round(p["sol_in"], 6), round(p["tokens"], 2)])
        self._event("buy", f"Paper buy {p['symbol']} at {p['entry_mcap']:.0f} SOL mcap (${p.get('size_usd', 0):.2f})",
                    mint=p["mint"])
        log.info("LOOKALIKE buy %s at mcap %.0f SOL", p["symbol"], p["entry_mcap"])
        c = self.cfg()
        if c.get("real_enabled") and self.live is not None:
            why = self.live.open_strategy("lookalike", p["mint"], p["symbol"], float(c.get("real_size_usd", 2.5)),
                                          int(c.get("real_max_open", 20)), float(c.get("real_daily_loss_usd", 25)))
            if why is None:
                p["real"] = True
                self._event("buy", f"{p['symbol']}: REAL buy placed (${float(c.get('real_size_usd', 2.5)):.2f})", mint=p["mint"])
            else:
                self._event("skip", f"{p['symbol']}: no real buy - {why}", mint=p["mint"])
        return True

    # ------------------------------------------------------------------ exits
    def _sell(self, p, frac, reason, px, ts):
        if p.get("real") and self.live is not None:
            self.live.strategy_sell("lookalike", p["mint"], 1.0 if frac >= 0.999 else frac, reason)
        qty = p["tokens"] if frac >= 0.999 else p["tokens"] * frac
        got = self._sell_value(qty, px)
        p["tokens"] -= qty
        p["sol_out"] += got
        mult = px / p["entry_px"]
        p["sells"].append(dict(ts=ts, reason=reason, mult=round(mult, 2), sol=round(got, 6)))
        self._csv("lookalike_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), p["mint"], p["symbol"], "SELL", reason,
                   f"{mult:.2f}", round(got, 6), round(qty, 2)])
        if p["tokens"] <= p["tokens_bought"] * 1e-6:
            self._close(p, ts)
        else:
            self._event("sell", f"{p['symbol']}: {reason} - sold {qty / p['tokens_bought'] * 100:.0f}% at "
                                f"{mult:.1f}x", mint=p["mint"])

    def _close(self, p, ts):
        self.positions.pop(p["mint"], None)
        pnl = p["sol_out"] - p["sol_in"]
        usd = self._usd() or 0
        rec = dict(mint=p["mint"], symbol=p["symbol"], opened=p["opened"], closed=ts, entry_mcap=p["entry_mcap"],
                   sol_in=round(p["sol_in"], 6), sol_out=round(p["sol_out"], 6), pnl_sol=round(pnl, 6),
                   pnl_pct=round(pnl / p["sol_in"] * 100, 1), pnl_usd=round(pnl * usd, 2),
                   peak_mult=round(p["peak_mult"], 2), stages=",".join(p["done"]),
                   exit=p["sells"][-1]["reason"] if p["sells"] else "")
        self.closed.append(rec)
        self._csv("lookalike_trades.csv",
                  "opened_utc,closed_utc,mint,symbol,entry_mcap_sol,hold_min,sol_in,sol_out,pnl_sol,pnl_pct,pnl_usd,peak_mult,stages,last_exit",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(p["opened"])),
                   time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), p["mint"], p["symbol"], p["entry_mcap"],
                   round((ts - p["opened"]) / 60, 1), rec["sol_in"], rec["sol_out"], rec["pnl_sol"], rec["pnl_pct"],
                   rec["pnl_usd"], rec["peak_mult"], rec["stages"], rec["exit"]])
        self._event("close", f"Closed {p['symbol']}: {rec['pnl_pct']:+.0f}% (${rec['pnl_usd']:+.2f}) - {rec['exit']}",
                    mint=p["mint"], pnl=pnl)
        self._save()

    def check(self, p, px, ts):
        """Apply the exit plan to one position at price `px`."""
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        done = p["done"]
        if mult <= p["floor_mult"]:
            why = "back to entry after 6x" if "6x" in done else f"stop {(p['floor_mult'] - 1) * 100:+.0f}%"
            return self._sell(p, 1.0, why, px, ts)
        if mult >= float(c.get("moon_mult", 100)):
            return self._sell(p, 1.0, f"{c.get('moon_mult', 100):g}x - full exit", px, ts)
        if mult >= float(c.get("tp_initial_mult", 2)) and "2x" not in done:
            done.append("2x")
            need = max(p["sol_in"] - p["sol_out"], 0.0)
            if self._sell_value(p["tokens"], px) <= need:
                frac = 1.0
            else:                                                       # smallest share that brings the initial back
                lo, hi = 0.0, 1.0
                for _ in range(40):
                    mid = (lo + hi) / 2
                    if self._sell_value(p["tokens"] * mid, px) >= need:
                        hi = mid
                    else:
                        lo = mid
                frac = hi
            self._sell(p, frac, "2x - initial back", px, ts)
            if p["mint"] not in self.positions:
                return
        if mult >= float(c.get("tp6_mult", 6)) and "6x" not in done:
            done.append("6x")
            p["floor_mult"] = max(p["floor_mult"], float(c.get("floor_after_6x_mult", 1.0)))
            self._sell(p, float(c.get("tp6_sell_pct", 25)) / 100, "6x - partial", px, ts)
            if p["mint"] not in self.positions:
                return
        if mult >= float(c.get("tp10_mult", 10)) and "10x" not in done:
            done.append("10x")
            self._sell(p, float(c.get("tp10_sell_pct", 25)) / 100, "10x - partial", px, ts)

    # ------------------------------------------------------------------ pricing loop
    async def _sess(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _rpc(self, method, params):
        s = await self._sess()
        async with s.post(RPC.format(key=self._key()), json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                          timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json(content_type=None)
        if "error" in j:
            raise RuntimeError(str(j["error"])[:120])
        return j.get("result")

    async def prices(self, mints):
        """{mint: (SOL per token, source)}. Sources: 'curve' (pump.fun bonding curve, on-chain), 'feed' (live feed,
        only for coins still on the curve), 'jupiter' (only after the curve says the coin graduated).
        Coins with no pump.fun curve get no price at all."""
        out, curves = {}, {}
        for m in mints:
            try:
                curves[m] = curve_address(m)
            except ValueError:
                continue                                   # not a valid address - skip, don't break the batch
        need, grads = list(curves), []
        for i in range(0, len(need), 100):
            chunk = need[i:i + 100]
            r = await self._rpc("getMultipleAccounts", [[curves[m] for m in chunk],
                                                        {"encoding": "base64", "commitment": "confirmed"}])
            for m, acc in zip(chunk, (r or {}).get("value") or []):
                data = base64.b64decode(acc["data"][0]) if acc else b""
                if len(data) < 49:
                    continue                               # no pump.fun curve: never trust another source
                if data[48]:
                    grads.append(m)                        # graduated: priced from its pool via Jupiter
                    continue
                px = parse_curve(data)
                if px:
                    fp = self.feed_price(m)
                    out[m] = (fp[0], "feed") if fp and fp[1] < 15 and 0.5 < fp[0] / px < 2 else (px, "curve")
        usd = self._usd()
        if grads and usd:
            s = await self._sess()
            for i in range(0, len(grads), 50):
                chunk = grads[i:i + 50]
                try:
                    async with s.get(JUP_PRICE.format(mint=",".join(chunk)), timeout=aiohttp.ClientTimeout(total=8)) as rr:
                        j = await rr.json(content_type=None) if rr.status == 200 else {}
                    for m in chunk:
                        v = float(((j or {}).get(m) or {}).get("usdPrice") or 0)
                        if v > 0:
                            out[m] = (v / usd, "jupiter")
                except Exception:
                    pass
        return out

    def _accept(self, p, px, ts):
        """Sanity gate: a reading more than 3x away from the last accepted price must be seen twice in a row
        (within 25%) before the exit plan acts on it - one bad number can't trigger a sale."""
        last = p.get("last_px") or p["entry_px"]
        if 1 / 3 <= px / last <= 3:
            p.pop("suspect_px", None)
            return True
        prev = p.get("suspect_px")
        if prev and 0.8 <= px / prev <= 1.25:
            p.pop("suspect_px", None)
            return True
        p["suspect_px"] = px
        return False

    async def tick(self):
        if not self.positions or not self._key():
            return
        now = time.time()
        px = await self.prices(list(self.positions))
        for m in list(self.positions):
            p = self.positions.get(m)
            price, src = px.get(m, (None, None))
            if not p.get("verified", True):
                if price is None:
                    price, src = 0.0, "none"
                if not self._verify(p, price, src, now):
                    continue
            if p and price and price > 0 and self._accept(p, price, now):
                self.check(p, price, now)
        self._save()

    async def run(self, every=8):
        while True:
            try:
                if self.active:
                    await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e) or type(e).__name__
                log.debug("lookalike tick failed: %s", e)
            await asyncio.sleep(every)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # ------------------------------------------------------------------ dashboard
    def rename(self, mint, symbol):
        from memebot.names import is_placeholder
        p = self.positions.get(mint)
        if p and is_placeholder(p["symbol"], mint):
            p["symbol"] = symbol
        for c in self.closed:
            if c["mint"] == mint and is_placeholder(c["symbol"], mint):
                c["symbol"] = symbol

    def state(self):
        usd = self._usd()
        c = self.cfg()
        cl = self.closed
        wins = [x for x in cl if x["pnl_sol"] > 0]
        realized = sum(x["pnl_sol"] for x in cl)
        live_pos = [p for p in self.positions.values() if p.get("verified", True)]
        open_val = sum(self._sell_value(p["tokens"], p["last_px"]) for p in live_pos)
        open_cost_left = sum(p["sol_in"] - p["sol_out"] for p in live_pos)
        ups = sum(1 for x in cl if "2x" in x["stages"])
        return dict(
            enabled=self.enabled(), active=self.active, size_usd=c.get("size_usd", 2.5),
            real_enabled=bool(c.get("real_enabled")), real_size_usd=c.get("real_size_usd", 2.5),
            real_max_open=c.get("real_max_open", 20), real_daily_loss_usd=c.get("real_daily_loss_usd", 25),
            real_today_usd=round(self.live.realized_today_usd("lookalike"), 2) if self.live is not None else None,
            entry_mcap_sol=c.get("entry_mcap_sol", 44), sol_usd=usd, last_error=self.last_error,
            open=len(live_pos), closed=len(cl), wins=len(wins),
            win_rate=round(len(wins) / len(cl) * 100, 1) if cl else None,
            hit_2x=ups, hit_6x=sum(1 for x in cl if "6x" in x["stages"]) + sum(1 for p in live_pos if "6x" in p["done"]),
            realized=round(realized, 5), realized_usd=round(realized * usd, 2) if usd else None,
            open_value=round(open_val, 5), open_pnl=round(open_val - open_cost_left, 5),
            open_pnl_usd=round((open_val - open_cost_left) * usd, 2) if usd else None,
            best=max(cl, key=lambda x: x["pnl_pct"], default=None),
            positions=sorted([dict(mint=p["mint"], symbol=p["symbol"], name=p.get("name", ""),
                                   mult=round(p["last_px"] / p["entry_px"], 2), peak=round(p["peak_mult"], 2),
                                   floor=round(p["floor_mult"], 2), stages=p["done"], entry_mcap=p["entry_mcap"],
                                   age_min=round((time.time() - p["opened"]) / 60),
                                   value=round(self._sell_value(p["tokens"], p["last_px"]), 6),
                                   cost_left=round(p["sol_in"] - p["sol_out"], 6))
                              for p in live_pos], key=lambda x: -x["mult"])[:60],
            recent=cl[-25:][::-1],
            events=self.events[-30:][::-1],
        )
