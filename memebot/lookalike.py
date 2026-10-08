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
    _curve_cache: dict = {}                        # mint -> (ts, (px, src) or None), shared by all strategies
    NAME = "lookalike"                             # config section, file prefix, real-money tag

    def __init__(self, data_dir, cfg_getter, key_getter, sol_usd_getter):
        self.dir = data_dir
        self.state_path = os.path.join(data_dir, f"{self.NAME}_state.json")
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
        return self._cfg().get(self.NAME) or {}

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
        path = os.path.join(self.dir, f"{self.NAME}_fills.csv")
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
        tpath = os.path.join(self.dir, f"{self.NAME}_trades.csv")
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
        log.info("%s removed %d trade(s) caused by bad price readings", self.NAME.upper(), len(bad))

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
        w, m = getattr(self, "why", None), extra.get("mint")
        if w is not None and m and kind in ("skip", "buy"):
            sym = text.split(":")[0].replace("Paper buy ", "").split(" at ")[0]
            if "REAL buy placed" in text:
                w.note(m, sym, "bought", f"{self.NAME}: real buy placed")
            elif "no real buy" in text:
                w.note(m, sym, "real money", f"{self.NAME}: {text.split('no real buy - ', 1)[-1]}")
            elif kind == "skip":
                w.note(m, sym, self.NAME, text.split(": ", 1)[-1])
            else:
                w.note(m, sym, self.NAME, "paper buy · " + text.split(" at ", 1)[-1])
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
        w = getattr(self, "why", None)
        if ts - t.created_ts < float(c.get("min_age_s", 300)):
            if w is not None:
                w.note(t.mint, t.symbol, self.NAME, f"crossed {line:g} SOL at {ts - t.created_ts:.0f}s old "
                                                    f"(needs {float(c.get('min_age_s', 300)) / 60:g}+ min)", ts)
            return False
        prior = getattr(t, "prior_peak", 0) or 0                    # highest mcap BEFORE this trade
        cap = float(c.get("max_prior_peak_mult", 0) or 0)
        if cap and prior > line * cap:
            if w is not None:
                w.note(t.mint, t.symbol, self.NAME, f"crossed {line:g} SOL but had already been up to {prior:.0f} SOL "
                                                    f"(dipped - only buys coins at a new high)", ts)
            return False
        r2, r1 = float(c.get("min_rise_2m_pct", 0) or 0), c.get("min_rise_1m_pct")
        if r2 or r1 is not None:                                     # must be on an up trajectory
            m2 = t.mcap_at(ts - 120) if hasattr(t, "mcap_at") else None
            m1 = t.mcap_at(ts - 60) if hasattr(t, "mcap_at") else None
            up2 = (t.mcap / m2 - 1) * 100 if m2 else None
            up1 = (t.mcap / m1 - 1) * 100 if m1 else None
            why = None
            if up2 is None:
                why = "no price history for the last 2 min"
            elif r2 and up2 < r2:
                why = f"not trending up: {up2:+.0f}% over 2 min (needs +{r2:g}%)"
            elif r1 is not None and up1 is not None and up1 < float(r1):
                why = f"falling in the last minute ({up1:+.0f}%)"
            if why:
                if w is not None:
                    w.note(t.mint, t.symbol, self.NAME, f"crossed {line:g} SOL but {why}", ts)
                return False
        mb = int(c.get("min_buys_2m", 0) or 0)
        buys2 = sum(1 for x in getattr(t, "recent", ()) if x[1] == "buy" and ts - x[0] <= 120)
        if mb and buys2 < mb:                                        # needs a crowd, not just a price tick
            if w is not None:
                w.note(t.mint, t.symbol, self.NAME, f"crossed {line:g} SOL but only {buys2} buys in the last 2 min "
                                                    f"(needs {mb}+)", ts)
            return False
        if t.sec_state == "failed" or t.dev_sold and c.get("skip_if_dev_sold", False):
            if w is not None:
                w.note(t.mint, t.symbol, self.NAME, "crossed the line but " +
                       ("security check failed" if t.sec_state == "failed" else "the dev had sold"), ts)
            return False
        self.traded.add(t.mint)
        if len(self.positions) >= int(c.get("max_open", 300)):
            self._event("skip", f"{t.symbol}: max {c.get('max_open', 300)} paper positions open", mint=t.mint)
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
            done=[], last_px=t.price, last_px_ts=ts, sells=[], verified=False, size_usd=round(size * usd, 2),
            prior_peak=round(prior, 1), buys_2m=buys2,
            rise_2m=round((t.mcap / t.mcap_at(ts - 120) - 1) * 100, 1) if hasattr(t, "mcap_at") and t.mcap_at(ts - 120) else None)
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
        on_curve = src in ("curve", "feed")                # 'feed' is only returned when the curve exists and agrees
        if not on_curve or not (0.7 <= px / p["entry_px"] <= 1.3):
            self.positions.pop(p["mint"], None)
            why = "Mayhem-mode coin (not traded)" if src == "mayhem" else "not a pump.fun bonding-curve coin" if not on_curve else \
                f"on-chain price didn't match the feed ({px / p['entry_px']:.2f}x)"
            self._event("skip", f"{p['symbol']}: skipped - {why}", mint=p["mint"])
            log.info("%s skipped %s: %s", self.NAME.upper(), p["symbol"], why)
            return False
        p["verified"] = True
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(p["opened"])), p["mint"], p["symbol"], "BUY",
                   f"entry at {p['entry_mcap']:.0f} SOL mcap", "1.00", round(p["sol_in"], 6), round(p["tokens"], 2)])
        self._event("buy", f"Paper buy {p['symbol']} at {p['entry_mcap']:.0f} SOL mcap (${p.get('size_usd', 0):.2f})",
                    mint=p["mint"])
        log.info("%s buy %s at mcap %.0f SOL", self.NAME.upper(), p["symbol"], p["entry_mcap"])
        c = self.cfg()
        if c.get("real_enabled") and self.live is not None:
            why = self.live.open_strategy(self.NAME, p["mint"], p["symbol"], float(c.get("real_size_usd", 2.5)),
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
            self.live.strategy_sell(self.NAME, p["mint"], 1.0 if frac >= 0.999 else frac, reason)
        qty = p["tokens"] if frac >= 0.999 else p["tokens"] * frac
        got = self._sell_value(qty, px)
        p["tokens"] -= qty
        p["sol_out"] += got
        mult = px / p["entry_px"]
        p["sells"].append(dict(ts=ts, reason=reason, mult=round(mult, 2), sol=round(got, 6)))
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
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
                   age_at_entry_s=p.get("age_at_entry_s"), prior_peak=p.get("prior_peak"),
                   sol_in=round(p["sol_in"], 6), sol_out=round(p["sol_out"], 6), pnl_sol=round(pnl, 6),
                   pnl_pct=round(pnl / p["sol_in"] * 100, 1), pnl_usd=round(pnl * usd, 2),
                   peak_mult=round(p["peak_mult"], 2), stages=",".join(p["done"]),
                   exit=p["sells"][-1]["reason"] if p["sells"] else "")
        self.closed.append(rec)
        self._csv(f"{self.NAME}_trades.csv",
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
        """Helius RPC with retries: when it's busy it answers HTTP 429 / an empty or non-JSON body."""
        s = await self._sess()
        last = None
        for attempt in range(3):
            try:
                async with s.post(RPC.format(key=self._key()), json={"jsonrpc": "2.0", "id": 1, "method": method,
                                                                    "params": params},
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                    status = r.status
                    try:
                        j = await r.json(content_type=None)
                    except ValueError:
                        j = None
                if not isinstance(j, dict):
                    last = f"Helius busy (HTTP {status})"
                    await asyncio.sleep(0.6 * (attempt + 1))
                    continue
                if "error" in j:
                    raise RuntimeError(str(j["error"])[:120])
                return j.get("result")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = f"Helius connection: {type(e).__name__}"
                await asyncio.sleep(0.6 * (attempt + 1))
        raise RuntimeError(last or "Helius not answering")

    async def prices(self, mints):
        """{mint: (SOL per token, source)}. Sources: 'curve' (pump.fun bonding curve, on-chain), 'feed' (live feed,
        only for coins still on the curve), 'jupiter' (only after the curve says the coin graduated).
        Coins with no pump.fun curve get no price at all."""
        out, curves = {}, {}
        now = time.time()
        cache = Lookalike._curve_cache
        if len(cache) > 20000:
            for k in [k for k, v in cache.items() if now - v[0] > 10]:
                cache.pop(k, None)
        fresh = [m for m in mints if m in cache and now - cache[m][0] < 1.5]
        for m in fresh:
            if cache[m][1] is not None:
                out[m] = cache[m][1]
        mints = [m for m in mints if m not in fresh]
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
                cache[m] = (now, None)
                if len(data) < 49:
                    continue                               # no pump.fun curve: never trust another source
                if len(data) > 81 and data[81] == 1:
                    out[m] = (0.0, "mayhem")               # mayhem-mode coin (different supply): not traded
                    cache[m] = (now, out[m])
                    continue
                if data[48]:
                    cache.pop(m, None)
                    grads.append(m)                        # graduated: priced from its pool via Jupiter
                    continue
                px = parse_curve(data)
                if px:
                    fp = self.feed_price(m)
                    out[m] = (fp[0], "feed") if fp and fp[1] < 15 and 0.5 < fp[0] / px < 2 else (px, "curve")
                    cache[m] = (now, out[m])
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
                            cache[m] = (now, out[m])
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
            if p and src:
                p["src"] = src                                 # 'jupiter' = graduated (priced from its PumpSwap pool)
            if p and price and price > 0 and self._accept(p, price, now):
                self.check(p, price, now)
        self._save()

    async def run(self, every=8):
        while True:
            try:
                if self.active:
                    await self.tick()
                    self.last_error = ""                   # a clean pass clears an old error message
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e) or type(e).__name__
                log.debug("%s tick failed: %s", self.NAME, e)
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

    def stats_since(self, since):
        """Paper results of the buys made since `since` (closed + still open), for side-by-side tests."""
        usd = self._usd() or 0
        cl = [x for x in self.closed if x["opened"] >= since]
        op = [p for p in self.positions.values() if p["opened"] >= since and p.get("verified", True)]
        real = sum(x["pnl_sol"] for x in cl)
        open_pnl = sum(self._sell_value(p["tokens"], p["last_px"]) - (p["sol_in"] - p["sol_out"]) for p in op)
        total = real + open_pnl
        return dict(buys=len(cl) + len(op), closed=len(cl), open=len(op), wins=sum(1 for x in cl if x["pnl_sol"] > 0),
                    hit3x=sum(1 for x in cl if "3x" in x["stages"]) + sum(1 for p in op if "3x" in p["done"]),
                    realized_usd=round(real * usd, 2), open_pnl_usd=round(open_pnl * usd, 2),
                    total_usd=round(total * usd, 2),
                    size_usd=self.cfg().get("size_usd", 2.5),
                    young=sum(1 for x in [*cl, *op] if (x.get("age_at_entry_s") or 1e9) < 300))

    def stats_from(self):
        """Results count from this time: the later of a 'Reset stats' click and the config's `stats_since` date."""
        ts = 0.0
        try:
            with open(os.path.join(self.dir, f"{self.NAME}_reset.txt"), encoding="utf-8") as fh:
                ts = float(fh.read().strip())
        except (OSError, ValueError):
            pass
        d = str(self.cfg().get("stats_since") or "").strip()
        if d:
            try:
                ts = max(ts, time.mktime(time.strptime(d, "%Y-%m-%d")))      # local midnight of that day
            except ValueError:
                pass
        return ts

    def reset_stats(self):
        ts = time.time()
        with open(os.path.join(self.dir, f"{self.NAME}_reset.txt"), "w", encoding="utf-8") as fh:
            fh.write(str(ts))
        return ts

    DIST_EDGES = (-50, -25, 0, 50, 100, 200)       # result buckets (%): <=-50 | -50..-25 | -25..0 | 0..50 | 50..100 | 100..200 | 200+

    def _charts(self, cl):
        """Cumulative realized P&L ($) over time (max ~120 points) + how many trades landed in each result bucket.
        Cached until a trade closes - the dashboard asks for this every second."""
        key = (len(cl), cl[-1]["closed"] if cl else 0)
        if getattr(self, "_chart_key", None) == key:
            return self._chart_cache
        pts, cum = [], 0.0
        for x in sorted(cl, key=lambda x: x["closed"]):
            cum += float(x.get("pnl_usd") or 0)
            pts.append([round(x["closed"]), round(cum, 2)])
        if len(pts) > 120:
            step = len(pts) / 120
            pts = [pts[int(i * step)] for i in range(120)] + [pts[-1]]
        dist = [0] * (len(self.DIST_EDGES) + 1)
        for x in cl:
            v = float(x.get("pnl_pct") or 0)
            dist[sum(1 for e in self.DIST_EDGES if v > e)] += 1
        self._chart_key, self._chart_cache = key, (pts, dist)
        return pts, dist

    def state(self):
        usd = self._usd()
        c = self.cfg()
        since = self.stats_from()
        cl = [x for x in self.closed if x["opened"] >= since]
        curve, dist = self._charts(cl)
        wins = [x for x in cl if x["pnl_sol"] > 0]
        realized = sum(x["pnl_sol"] for x in cl)
        live_pos = [p for p in self.positions.values() if p.get("verified", True) and p["opened"] >= since]
        open_val = sum(self._sell_value(p["tokens"], p["last_px"]) for p in live_pos)
        open_cost_left = sum(p["sol_in"] - p["sol_out"] for p in live_pos)
        ups = sum(1 for x in cl if "2x" in x["stages"])
        return dict(
            stats_since=since or None,
            name=self.NAME, desc=f"Buys coins rising through {c.get('entry_mcap_sol', 44)} SOL mcap · ${c.get('size_usd', 2.5)} each",
            enabled=self.enabled(), active=self.active, size_usd=c.get("size_usd", 2.5),
            real_enabled=bool(c.get("real_enabled")), real_size_usd=c.get("real_size_usd", 2.5),
            real_max_open=c.get("real_max_open", 20), real_daily_loss_usd=c.get("real_daily_loss_usd", 25),
            real_today_usd=round(self.live.realized_today_usd(self.NAME), 2) if self.live is not None else None,
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
                                   floor=round(p.get("floor_mult", 0.0), 2), stages=p["done"], entry_mcap=p["entry_mcap"],
                                   age_min=round((time.time() - p["opened"]) / 60),
                                   value=round(self._sell_value(p["tokens"], p["last_px"]), 6),
                                   cost_left=round(p["sol_in"] - p["sol_out"], 6))
                              for p in live_pos], key=lambda x: -x["mult"])[:60],
            recent=cl[-25:][::-1],
            events=self.events[-30:][::-1],
            curve=curve, dist=dist,
        )
