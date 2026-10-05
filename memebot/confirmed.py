"""
"Confirmed breakout V1" PAPER test (5 Oct, the owner's spec). Runs exactly as written - do not tune mid-run.

SETUP     market cap 80-250 SOL; a range of 3+ min, 5-20% wide (low to high); price breaks 8%+ above the range high.
CONFIRM   (checked once, at the breakout - one Helius trade read + holder reads)
          50+ trades in the last 2 min; last 60 s: buy SOL >= 1.75x sell SOL, buy SOL >= 3% of market cap,
          10+ different buyers, biggest buyer < 25% and top 3 buyers < 50% of buy volume;
          holders up 5%+ (or +10) since the snapshot taken as price neared the range high, and not falling;
          largest normal holder <= 8%, top 5 <= 25%, top 10 <= 40%, creator <= 5% (bonding curve and program-owned
          accounts excluded); mint + freeze authority revoked; creator sold < 10% of their tokens and < 0.5% of supply
          in the last 2 min; the coin's earliest buyers sold < 1% of supply in the last 2 min.
          (Creators who rugged before / serial launchers never reach this test: the bot's security gate drops them.)
WAIT      no buy on the breakout. Wait (up to `retest_wait_min`) for a pullback into the retest zone: within 0-8% above
          the range high, never more than 3% below it (below that = cancel). Never chase a coin that runs off.
ENTRY     after the retest, price +3% off the retest low AND the last 30 s show buy SOL >= 1.5x sell SOL, 5+ buyers,
          10+ trades -> paper buy `size_sol` (0.02 SOL), always the same size.
EXITS     stop 3% below the retest low, never worse than -15% from entry;
          first 90 s: out if not yet +5% and the last-60 s buy:sell < 1.0;
          first 5 min: out if sell SOL >= 2x buy SOL over the last 20 s (checked every 20 s);
          +50%: sell 25%, stop to entry; +100%: sell 25% more; last 50% rides a 20% trailing stop from 2x.

Every candidate that reaches the breakout (bought or rejected) is logged with all its numbers (type "cb" in
breakouts.jsonl) and followed for 60 min afterwards ("cb_after"), so the thresholds can be checked with evidence.
Never real money.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from memebot import wallet_tokens as WT
from memebot.lookalike import curve_buy
from memebot.reclaim import Reclaim
from memebot.snapshots import curve_token_accounts

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000
SYSTEM_PROGRAM = "11111111111111111111111111111111"


class ConfirmedBreakout(Reclaim):
    NAME = "confirmed"

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.hist: dict[str, list] = {}          # mint -> [[ts, mcap]] every tick (memory only)
        self.stage: dict[str, dict] = {}          # mint -> armed breakout waiting for the retest
        self.hsnap: dict[str, tuple] = {}         # mint -> (ts, holder count) taken as price nears the range high
        self.followed: dict[str, dict] = {}       # 60-min follow-up of every logged breakout
        self.busy: set[str] = set()
        self.cooldown: dict[str, float] = {}

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        c["real_enabled"] = False
        return c

    # ------------------------------------------------------------------ data helpers
    async def _flow(self, mint, creator=None, insiders=()):
        """Latest 100 trades of the coin -> flow numbers for the last 120 / 60 / 30 / 20 s."""
        s = await self._sess()
        async with s.get(WT.enhanced_url(mint), params={"api-key": self._key(), "limit": "100"},
                         timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                raise RuntimeError(f"trade read HTTP {r.status}")
            txs = await r.json(content_type=None)
        if not isinstance(txs, list):
            raise RuntimeError("trade read: bad answer")
        now = time.time()
        rows, oldest = [], now
        for tx in txs:
            ts = tx.get("timestamp") or 0
            if ts:
                oldest = min(oldest, ts)
            tr = WT._trade_of(tx, mint)
            if tr and ts:
                rows.append((now - ts, tr))
        out = dict(covered_s=round(now - oldest), full_page=len(txs) >= 100)
        for w in (120, 60, 30, 20):
            buy = sell = 0.0
            per, n, sellers = {}, 0, set()
            for age, (who, side, sol, tok) in rows:
                if age > w:
                    continue
                n += 1
                if side == "buy":
                    buy += sol
                    per[who] = per.get(who, 0.0) + sol
                else:
                    sell += sol
                    sellers.add(who)
            top = sorted(per.values(), reverse=True)
            out[f"w{w}"] = dict(trades=n, buy_sol=round(buy, 3), sell_sol=round(sell, 3),
                                ratio=round(buy / sell, 2) if sell > 0 else (99.0 if buy > 0 else 0.0),
                                buyers=len(per), sellers=len(sellers),
                                top1_pct=round(top[0] / buy * 100, 1) if buy > 0 else 0.0,
                                top3_pct=round(sum(top[:3]) / buy * 100, 1) if buy > 0 else 0.0)
        if out["full_page"] and out["covered_s"] < 120:      # 100 trades in < 2 min: more than 50 for sure
            out["w120"]["trades"] = max(out["w120"]["trades"], 100)
        dev_tok = sum(tr[3] for age, tr in rows if age <= 120 and tr[0] == creator and tr[1] == "sell")
        ins_tok = sum(tr[3] for age, tr in rows if age <= 120 and tr[0] in insiders and tr[1] == "sell")
        out["dev_sold_tok_2m"], out["insider_sold_pct_2m"] = dev_tok, round(ins_tok / SUPPLY * 100, 3)
        return out

    async def _holders(self, mint):
        """Number of wallets holding the coin (bonding curve excluded), via Helius DAS (max 3 pages of 1000)."""
        curve = curve_token_accounts(mint)
        n = 0
        for page in (1, 2, 3):
            res = await self._rpc("getTokenAccounts", {"mint": mint, "limit": 1000, "page": page})
            accs = (res or {}).get("token_accounts") or []
            n += sum(1 for a in accs if float(a.get("amount") or 0) > 0 and a.get("address") not in curve)
            if len(accs) < 1000:
                break
        return n

    async def _concentration(self, mint, creator):
        """Largest / top 5 / top 10 NORMAL holders (wallets owned by the System program) and the creator's %."""
        curve = curve_token_accounts(mint)
        big = await self._rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        vals = [v for v in ((big or {}).get("value") or []) if v["address"] not in curve]
        accs = await self._rpc("getMultipleAccounts", [[v["address"] for v in vals[:20]], {"encoding": "jsonParsed"}])
        owners = [((((a or {}).get("data") or {}).get("parsed") or {}).get("info") or {}).get("owner")
                  for a in (accs or {}).get("value") or []]
        uniq = list(dict.fromkeys(o for o in owners if o))
        prog = {}
        if uniq:
            info = await self._rpc("getMultipleAccounts", [uniq, {"encoding": "base64", "dataSlice": {"offset": 0, "length": 0}}])
            prog = {o: (x or {}).get("owner") for o, x in zip(uniq, (info or {}).get("value") or [])}
        normal, creator_amt = [], 0.0
        for v, o in zip(vals, owners):
            amt = float(v.get("uiAmountString") or v.get("uiAmount") or 0)
            if o == creator:
                creator_amt += amt
            if prog.get(o) == SYSTEM_PROGRAM:
                normal.append(amt)
        pct = lambda x: round(x / SUPPLY * 100, 2)
        return dict(top1=pct(normal[0]) if normal else 0.0, top5=pct(sum(normal[:5])), top10=pct(sum(normal[:10])),
                    dev_pct=pct(creator_amt), dev_amt=creator_amt)

    async def _authorities(self, mint):
        r = await self._rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
        info = ((((r or {}).get("value") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        return info.get("mintAuthority") is None, info.get("freezeAuthority") is None

    async def _insiders(self, mint):
        """The coin's earliest buyers: everyone who bought in the first 10 seconds (creator's block + snipers)."""
        early = await WT.fetch_early(await self._sess(), self._key(), mint)
        if not early:
            return set()
        t0 = early[0].get("timestamp") or 0
        out = set()
        for tx in early:
            if (tx.get("timestamp") or 0) - t0 > 10:
                break
            tr = WT._trade_of(tx, mint)
            if tr and tr[1] == "buy":
                out.add(tr[0])
        return out

    # ------------------------------------------------------------------ range / breakout
    def _range(self, h, now, c):
        """Longest stretch ending 10 s ago whose high/low stay within max_width: (lo, hi, seconds) or None."""
        pts = [(ts, mc) for ts, mc in h if ts <= now - 10]
        if len(pts) < 10:
            return None
        lo = hi = pts[-1][1]
        start = pts[-1][0]
        maxw = float(c.get("range_max_pct", 20)) / 100
        for ts, mc in reversed(pts):
            nlo, nhi = min(lo, mc), max(hi, mc)
            if nhi / nlo - 1 > maxw:
                break
            lo, hi, start = nlo, nhi, ts
        span = pts[-1][0] - start
        if span < float(c.get("range_min_s", 180)) or hi / lo - 1 < float(c.get("range_min_pct", 5)) / 100:
            return None
        return lo, hi, span

    def _log(self, rec):
        bl = getattr(self, "breakouts", None)
        if bl is not None:
            try:
                bl._write(rec)
            except Exception:
                pass

    async def _confirm(self, cd, lo, hi, span, mc, now):
        """Breakout found: run every check once, log the numbers, arm the retest if all pass."""
        c = self.cfg()
        m = cd["mint"]
        rec = dict(type="cb", id=f"cb:{m}:{int(now)}", mint=m, sym=cd.get("symbol"), ts=round(now, 1),
                   mcap=round(mc, 1), range_lo=round(lo, 1), range_hi=round(hi, 1), range_min=round(span / 60, 1),
                   range_width_pct=round((hi / lo - 1) * 100, 1), breakout_pct=round((mc / hi - 1) * 100, 1),
                   age_min=round((now - cd["created"]) / 60, 1))
        fails = []
        try:
            creator = cd.get("creator") or ""
            insiders = await self._insiders(m)
            insiders.discard(creator)
            f = await self._flow(m, creator, insiders)
            w1, w2 = f["w60"], f["w120"]
            rec.update(trades_2m=w2["trades"], buy_sol_60s=w1["buy_sol"], sell_sol_60s=w1["sell_sol"],
                       buy_sell_60s=w1["ratio"], buyers_60s=w1["buyers"], top_buyer_pct=w1["top1_pct"],
                       top3_buyers_pct=w1["top3_pct"], buy_vol_pct_mcap=round(w1["buy_sol"] / mc * 100, 2),
                       insiders=len(insiders), insider_sold_pct_2m=f["insider_sold_pct_2m"])
            if w2["trades"] < int(c.get("min_trades_2m", 50)):
                fails.append("trades")
            if w1["ratio"] < float(c.get("min_buy_sell", 1.75)):
                fails.append("buy:sell")
            if w1["buy_sol"] < mc * float(c.get("min_buy_vol_pct", 3)) / 100:
                fails.append("buy volume")
            if w1["buyers"] < int(c.get("min_buyers", 10)):
                fails.append("buyers")
            if w1["top1_pct"] >= float(c.get("max_top_buyer_pct", 25)) or w1["top3_pct"] >= float(c.get("max_top3_buyers_pct", 50)):
                fails.append("one whale")
            conc = await self._concentration(m, creator)
            rec.update(top1_holder=conc["top1"], top5_holders=conc["top5"], top10_holders=conc["top10"], dev_pct=conc["dev_pct"])
            if conc["top1"] > 8 or conc["top5"] > 25 or conc["top10"] > 40:
                fails.append("holder concentration")
            if conc["dev_pct"] > float(c.get("max_dev_pct", 5)):
                fails.append("dev holds too much")
            dev_sold = f["dev_sold_tok_2m"]
            rec["dev_sold_pct_supply_2m"] = round(dev_sold / SUPPLY * 100, 3)
            held = conc["dev_amt"] + dev_sold
            if dev_sold > 0 and (dev_sold > 0.10 * held or dev_sold / SUPPLY * 100 > 0.5):
                fails.append("dev selling")
            if f["insider_sold_pct_2m"] > 1.0:
                fails.append("insiders selling")
            mint_off, freeze_off = await self._authorities(m)
            rec.update(mint_auth_revoked=mint_off, freeze_auth_revoked=freeze_off)
            if not (mint_off and freeze_off):
                fails.append("mint/freeze authority")
            hn = await self._holders(m)
            prev = self.hsnap.get(m)
            rec["holders"] = hn
            if prev:
                rec["holders_before"], rec["holders_before_s_ago"] = prev[1], round(now - prev[0])
                grow = hn - prev[1]
                rec["holder_growth_pct"] = round(grow / prev[1] * 100, 1) if prev[1] else None
                if grow < 0 or not (grow >= 10 or (prev[1] and grow / prev[1] >= 0.05)):
                    fails.append("holders not growing")
            else:
                fails.append("no holder snapshot")
        except Exception as e:
            fails.append(f"check error: {str(e)[:60] or type(e).__name__}")
        rec["result"] = "armed (waiting for retest)" if not fails else "rejected: " + ", ".join(fails)
        self._log(rec)
        self.followed[m] = dict(id=rec["id"], t0=now, mc0=mc, hi=hi, max=mc, min=mc, last=0.0, armed=not fails)
        if fails:
            self.cooldown[m] = now + 600
            self._event("skip", f"{cd['symbol']}: breakout at {mc:.0f} SOL rejected - {', '.join(fails)}", mint=m)
        else:
            self.stage[m] = dict(hi=hi, lo=lo, t=now, low=None, touched=False, last_check=0.0, rec_id=rec["id"],
                                 peak=mc, rec=rec)
            self._event("info", f"{cd['symbol']}: confirmed breakout at {mc:.0f} SOL (range {lo:.0f}-{hi:.0f}) - "
                                f"waiting for a retest of {hi:.0f}", mint=m)

    async def _retest(self, cd, st, mc, px, now):
        c = self.cfg()
        m, hi = cd["mint"], st["hi"]
        st["peak"] = max(st["peak"], mc)
        if mc < hi * (1 - float(c.get("retest_floor_pct", 3)) / 100):
            self._log(dict(type="cb_cancel", id=st["rec_id"], mint=m, why="fell more than 3% below the breakout level",
                           mcap=round(mc, 1), ts=round(now, 1)))
            self.stage.pop(m, None)
            self.cooldown[m] = now + 600
            return self._event("skip", f"{cd['symbol']}: retest failed (below {hi * 0.97:.0f} SOL) - cancelled", mint=m)
        if now - st["t"] > float(c.get("retest_wait_min", 15)) * 60:
            self._log(dict(type="cb_cancel", id=st["rec_id"], mint=m, why="no retest in time (didn't chase)",
                           peak=round(st["peak"], 1), ts=round(now, 1)))
            self.stage.pop(m, None)
            return
        if mc <= hi * (1 + float(c.get("retest_zone_pct", 8)) / 100):
            st["touched"] = True
            st["low"] = mc if st["low"] is None else min(st["low"], mc)
        if not st["touched"] or mc < st["low"] * (1 + float(c.get("rebound_pct", 3)) / 100):
            return
        if now - st["last_check"] < 15 or m in self.busy:
            return
        st["last_check"] = now
        self.busy.add(m)
        try:
            f = await self._flow(m)
            w = f["w30"]
            ok = (w["ratio"] >= float(c.get("retest_buy_sell", 1.5)) and w["buyers"] >= int(c.get("retest_buyers", 5))
                  and w["trades"] >= int(c.get("retest_trades", 10)))
            if not ok:
                return
            rec = dict(st["rec"], retest_low=round(st["low"], 1), retest_depth_pct=round((st["low"] / hi - 1) * 100, 1),
                       retest_buy_sell_30s=w["ratio"], retest_buyers_30s=w["buyers"], retest_trades_30s=w["trades"])
            self.stage.pop(m, None)
            self._enter_cb(cd, px, now, st["low"], rec)
        except Exception as e:
            self.last_error = f"retest check: {e}"
        finally:
            self.busy.discard(m)

    def _enter_cb(self, cd, px, ts, retest_low, rec):
        c = self.cfg()
        size = float(c.get("size_sol", 0.02))
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), px) * (1 - slip)
        mc = px * SUPPLY
        stop = max(retest_low * 0.97 / SUPPLY, px * (1 - float(c.get("max_loss_pct", 15)) / 100))
        usd = self._usd() or 0
        self.traded.add(cd["mint"])
        self.positions[cd["mint"]] = dict(
            mint=cd["mint"], symbol=cd["symbol"], name=cd.get("name", ""), opened=ts, entry_px=px, entry_mcap=round(mc, 1),
            age_at_entry_s=round(ts - cd["created"]), sol_in=size + prio, sol_out=0.0, tokens=tokens, tokens_bought=tokens,
            peak_mult=1.0, floor_mult=0.0, done=[], last_px=px, last_px_ts=ts, sells=[], verified=True,
            size_usd=round(size * usd, 2), stop_px=stop, low_mult=1.0, rec_id=rec["id"], next_flow=ts + 20)
        rec.update(type="cb_entry", entry_mcap=round(mc, 1), stop_mcap=round(stop * SUPPLY, 1), ts=round(ts, 1))
        self._log(rec)
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), cd["mint"], cd["symbol"], "BUY",
                   f"retest of {rec['range_hi']:.0f} SOL held, rebound +3%", "1.00", round(size + prio, 6), round(tokens, 2)])
        self._event("buy", f"Paper buy {cd['symbol']} at {mc:.0f} SOL - breakout of {rec['range_hi']:.0f} retested "
                           f"(low {retest_low:.0f}) and buyers came back · stop {stop * SUPPLY:.0f} SOL", mint=cd["mint"])
        log.info("CONFIRMED buy %s at mcap %.0f", cd["symbol"], mc)

    # ------------------------------------------------------------------ exits
    def check(self, p, px, ts):
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        p["low_mult"] = min(p.get("low_mult", 1.0), mult)
        done, peak = p["done"], p["peak_mult"]
        if px <= p["stop_px"]:
            why = "stop at entry" if "50" in done else "stop (3% under the retest low / max -15%)"
            return self._sell(p, 1.0, why, px, ts)
        if mult >= 1.5 and "50" not in done:
            done.append("50")
            p["stop_px"] = max(p["stop_px"], p["entry_px"])
            self._sell(p, 0.25, "+50% - sold 25%, stop to entry", px, ts)
            if p["mint"] not in self.positions:
                return
        if mult >= 2.0 and "2x" not in done:
            done.append("2x")
            self._sell(p, 1 / 3, "+100% - sold another 25%", px, ts)     # 25% of the original = 1/3 of what's left
            if p["mint"] not in self.positions:
                return
        if peak >= 2.0 and mult <= peak * 0.8:
            return self._sell(p, 1.0, f"trailing stop 20% ({peak:.1f}x high)", px, ts)

    async def _flow_exits(self, p, now):
        """First 5 minutes: trade-flow exits (fast failure at 90 s, heavy selling over 20 s)."""
        held = now - p["opened"]
        if held > 300 or now < p.get("next_flow", 0) or p["mint"] in self.busy:
            return
        p["next_flow"] = now + 20
        self.busy.add(p["mint"])
        try:
            f = await self._flow(p["mint"])
            px = p["last_px"]
            if f["w20"]["sell_sol"] >= 2 * max(f["w20"]["buy_sol"], 1e-9) and f["w20"]["sell_sol"] > 0:
                self._sell(p, 1.0, "selling 2x buying over 20 s", px, now)
            elif held <= 100 and held >= 80 and p["peak_mult"] < 1.05 and f["w60"]["ratio"] < 1.0:
                self._sell(p, 1.0, "fast failure (not +5% in 90 s, sellers in control)", px, now)
        except Exception as e:
            self.last_error = f"flow exit: {e}"
        finally:
            self.busy.discard(p["mint"])

    def _close(self, p, ts):
        super()._close(p, ts)
        self._log(dict(type="cb_exit", id=p.get("rec_id"), mint=p["mint"], ts=round(ts, 1),
                       max_gain_pct=round((p["peak_mult"] - 1) * 100, 1), max_loss_pct=round((p.get("low_mult", 1) - 1) * 100, 1),
                       exit=p["sells"][-1]["reason"] if p.get("sells") else "",
                       pnl_pct=round((p["sol_out"] - p["sol_in"]) / p["sol_in"] * 100, 1) if p["sol_in"] else None))

    # ------------------------------------------------------------------ loop
    async def tick(self):
        if not self._key() or not (self.cands or self.positions or self.followed):
            return
        c = self.cfg()
        now = time.time()
        mints = list(dict.fromkeys(list(self.cands) + list(self.positions) + list(self.followed)))
        px = await self.prices(mints)
        lo_mc, hi_mc = float(c.get("min_mcap_sol", 80)), float(c.get("max_mcap_sol", 250))
        for m, cd in list(self.cands.items()):
            if m in self.positions or m in self.traded:
                self.cands.pop(m, None)
                continue
            price, src = px.get(m, (None, None))
            if now - cd["created"] > float(c.get("max_age_h", 6)) * 3600 or src in ("jupiter", "mayhem"):
                self.cands.pop(m, None)
                self.hist.pop(m, None)
                continue
            if not price:
                continue
            mc = price * SUPPLY
            if mc < float(c.get("dead_mcap_sol", 30)):
                self.cands.pop(m, None)
                self.hist.pop(m, None)
                continue
            h = self.hist.setdefault(m, [])
            h.append([now, mc])
            while h and now - h[0][0] > 900:
                h.pop(0)
            st = self.stage.get(m)
            if st:
                await self._retest(cd, st, mc, price, now)
                continue
            if self.cooldown.get(m, 0) > now or m in self.busy:
                continue
            rg = self._range(h, now, c)
            if not rg:
                continue
            lo, hi, span = rg
            if mc >= hi * 0.95 and (m not in self.hsnap or now - self.hsnap[m][0] > 180):
                self.busy.add(m)                   # nearing the top of its range: holder count for the growth check
                try:
                    self.hsnap[m] = (now, await self._holders(m))
                except Exception as e:
                    self.last_error = f"holder count: {e}"
                finally:
                    self.busy.discard(m)
            if lo_mc <= mc <= hi_mc and mc >= hi * (1 + float(c.get("breakout_pct", 8)) / 100):
                self.busy.add(m)
                try:
                    await self._confirm(cd, lo, hi, span, mc, now)
                finally:
                    self.busy.discard(m)
        for m in list(self.positions):
            p = self.positions.get(m)
            price, src = px.get(m, (None, None))
            if p and price and price > 0 and self._accept(p, price, now):
                self.check(p, price, now)
                if m in self.positions:
                    await self._flow_exits(p, now)
        for m, fw in list(self.followed.items()):         # 60-min follow-up of every breakout (bought or not)
            price, src = px.get(m, (None, None))
            if price:
                mc = price * SUPPLY
                fw["max"], fw["min"] = max(fw["max"], mc), min(fw["min"], mc)
            if now - fw["t0"] >= 3600:
                self._log(dict(type="cb_after", id=fw["id"], mint=m, ts=round(now, 1), mcap0=round(fw["mc0"], 1),
                               max_mult_60m=round(fw["max"] / fw["mc0"], 3), min_mult_60m=round(fw["min"] / fw["mc0"], 3),
                               end_mult_60m=round((price * SUPPLY) / fw["mc0"], 3) if price else None,
                               armed=fw["armed"], graduated=src == "jupiter"))
                self.followed.pop(m, None)
        for m in [m for m in self.hist if m not in self.cands]:
            self.hist.pop(m, None)
        self._save()

    def maybe_enter(self, t, prev_mcap, ts):
        added = super().maybe_enter(t, prev_mcap, ts)
        cd = self.cands.get(t.mint)
        if cd is not None and not cd.get("creator"):
            cd["creator"] = t.creator
        return added

    def state(self):
        s = super().state()
        c = self.cfg()
        s.update(name=self.NAME, candidates=len(self.cands), pulled=len(self.stage),
                 desc=(f"V1 as written: {c.get('min_mcap_sol', 80):g}-{c.get('max_mcap_sol', 250):g} SOL, 3+ min range "
                       f"5-20% wide, breakout +8%, volume/buyer/holder/safety checks, then WAIT for a retest of the "
                       f"breakout and buy the +3% rebound · stop 3% under the retest low (max −15%) · 25% at +50% "
                       f"(stop to entry), 25% at 2×, rest 20% trailing · {c.get('size_sol', 0.02):g} SOL paper"))
        return s
