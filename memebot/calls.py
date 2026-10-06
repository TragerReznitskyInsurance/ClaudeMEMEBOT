"""
"Discord calls" - buy the coins a call channel posts, for real, the moment the call arrives.

Where the calls come from: memebot/winnotify.py reads the Discord pop-ups Windows shows on this computer
(it never logs into Discord or touches the account) and hands every coin address it finds to `add_call`.
The dashboard's "Buy call" box does the same by hand.

Only notifications from the call channel (`channel`, matched in the pop-up's title) count. Each coin is bought
at most ONCE: the channel also posts updates on its coins, and any later message about a coin it has already
seen (bought or not) is ignored.
Buy: every new coin address, as soon as it arrives - no safety / bundle checks (the owner's choice).
     Only technical skips: not a pump.fun coin, a Mayhem-mode coin, already bought, limits reached.
     Real buy in the real (lookalike) wallet, `buy_usd` each; the same trade is tracked on paper too, so
     the panel always shows how the calls would have done even when real money is off or paused.
Limits: max `max_buys_per_day` real buys a day, `max_open` open at once, stops buying after
        -`daily_loss_usd` realized in a day.
Sell ("hold for runners"): stop -40% · sell 1/4 at 3x, 1/4 at 5x, 1/4 at 10x · after the first sale the rest
     also has a trailing stop `trail_pct` (50%) below its high · time limit `max_hold_h`.
"""
from __future__ import annotations

import logging
import re
import time

from memebot.lookalike import Lookalike, curve_buy

log = logging.getLogger("memebot")
SUPPLY = 1_000_000_000
ADDR = re.compile(r"(?<![1-9A-HJ-NP-Za-km-z])[1-9A-HJ-NP-Za-km-z]{32,44}(?![1-9A-HJ-NP-Za-km-z])")
NOT_COINS = {"So11111111111111111111111111111111111111112", "11111111111111111111111111111111",
             "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
             "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"}


def find_addresses(text):
    """Coin addresses in a message: pump.fun-style ones (ending in 'pump') first, then any other valid address."""
    from solders.pubkey import Pubkey
    out = []
    for a in ADDR.findall(text or ""):
        if a in NOT_COINS or a in out:
            continue
        try:
            Pubkey.from_string(a)
        except Exception:
            continue
        out.append(a)
    return sorted(out, key=lambda a: not a.endswith("pump"))


class CallBuyer(Lookalike):
    NAME = "calls"

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.inbox: list[dict] = []                 # last calls received (shown on the dashboard)

    def cfg(self):
        c = dict(self._cfg().get(self.NAME) or {})
        c.setdefault("enabled", True)
        c.setdefault("real_money", True)
        return c

    # ------------------------------------------------------------------ incoming calls
    def _real_today(self):
        day = time.strftime("%Y-%m-%d")
        n = sum(1 for p in [*self.positions.values(), *self.closed]
                if p.get("real") and time.strftime("%Y-%m-%d", time.localtime(p["opened"])) == day)
        n += sum(1 for p in self.positions.values() if p.get("real")
                 for t in p.get("add_ts", []) if time.strftime("%Y-%m-%d", time.localtime(t)) == day)
        return n

    async def add_call(self, text, source="Discord", title=""):
        """A message arrived (a Discord pop-up, or pasted by hand). Buys the coin(s) in it. Returns what happened."""
        addrs = find_addresses(text)
        if not addrs:
            return []
        res = []
        for mint in addrs[:2]:                     # a call names one coin; don't buy a whole list
            manual = source == "pasted"                  # your own Buy-now: allowed again and again
            if mint in self.positions and manual:
                why = await self._add_to(mint)             # already holding: buy more of it
            elif mint in self.positions:
                why = "already holding this coin"
            elif mint in self.traded and not manual:
                why = "already called before - update message, not bought again"
            else:
                self.traded.add(mint)              # once per coin, ever: later updates about it never buy
                self._save()
                why = await self._buy_call(mint, source, title)
            res.append(dict(mint=mint, result=why or "bought"))
            self.inbox.insert(0, dict(ts=time.time(), mint=mint, source=source, title=(title or "")[:80],
                                      text=(text or "")[:160], result=why or "bought"))
            if why is None:
                break                              # bought the first real coin in the message
        self.inbox = self.inbox[:30]
        return res

    async def _buy_call(self, mint, source, title):
        c = self.cfg()
        tag = f"{source}{(' · ' + title[:40]) if title else ''}"
        if not self.enabled():
            return "Discord calls are OFF in Settings"
        if not self.active:
            return "bot isn't running (Start live)"
        if not self._key():
            return "needs the Helius key"
        try:
            px = await self.prices([mint])
        except Exception as e:
            return f"couldn't read the price ({e})"
        price, src = px.get(mint, (None, None))
        if src == "mayhem":
            return "Mayhem-mode coin (the bot can't price these)"
        if not price:
            return "not a pump.fun coin (or not tradable yet)"
        usd = self._usd()
        if not usd:
            return "no SOL price yet"
        if len(self.positions) >= int(c.get("max_paper_open", 100)):
            return "too many open call positions"
        size_usd = float(c.get("buy_usd", 10))
        size = size_usd / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), price) * (1 - slip)
        now = time.time()
        mc = price * SUPPLY
        p = dict(mint=mint, symbol=mint[:5], name="", opened=now, entry_px=price, entry_mcap=round(mc, 1),
                 age_at_entry_s=None, sol_in=size + prio, sol_out=0.0, tokens=tokens, tokens_bought=tokens,
                 peak_mult=1.0, floor_mult=0.0, done=[], last_px=price, last_px_ts=now, sells=[], verified=True,
                 size_usd=round(size_usd, 2), source=tag, graduated=src == "jupiter")
        self.positions[mint] = p
        real_note = "paper only"
        tg_paper = source.lower().startswith("telegram") and not c.get("telegram_real_money", False)
        if tg_paper:
            real_note = "paper only - Telegram calls are paper-tracked (real money off for Telegram)"
        elif c.get("real_money") and self.live is not None:
            if self._real_today() >= int(c.get("max_buys_per_day", 10)):
                real_note = f"no real buy - {c.get('max_buys_per_day', 10)} real buys already today"
            else:
                why = self.live.open_strategy(self.NAME, mint, p["symbol"], size_usd, int(c.get("max_open", 10)),
                                              float(c.get("daily_loss_usd", 50)))
                if why is None:
                    p["real"] = True
                    real_note = f"REAL buy placed (${size_usd:g})"
                else:
                    real_note = f"no real buy - {why}"
        self._csv(f"{self.NAME}_fills.csv", "time_utc,mint,symbol,side,reason,mult,sol,tokens",
                  [time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)), mint, p["symbol"], "BUY",
                   f"call from {tag} at {mc:.0f} SOL ({real_note})", "1.00", round(size + prio, 6), round(tokens, 2)])
        self._event("buy", f"{mint[:5]}: call from {tag} at {mc:.0f} SOL mcap{' (graduated)' if src == 'jupiter' else ''}"
                           f" · {real_note}", mint=mint)
        log.info("CALLS %s from %s at mcap %.0f SOL - %s", mint, tag, mc, real_note)
        self._save()
        return None if p.get("real") else real_note

    def manual_sell(self, mint, frac):
        """Your Sell button (all / half / third / quarter) on a call coin: sells for real and updates the record."""
        p = self.positions.get(mint)
        if not p:
            return False
        frac = min(max(float(frac), 0.01), 1.0)
        self._sell(p, frac, "sold by you" if frac >= 0.99 else f"sold {frac * 100:.0f}% by you", p["last_px"], time.time())
        self._save()
        return True

    async def _add_to(self, mint):
        """Manual re-buy of a coin we still hold: buy `buy_usd` more (paper + real), averaging the entry price."""
        c = self.cfg()
        p = self.positions[mint]
        try:
            px = await self.prices([mint])
        except Exception as e:
            return f"couldn't read the price ({e})"
        price, src = px.get(mint, (None, None))
        usd = self._usd()
        if not price or not usd:
            return "couldn't read the price"
        size_usd = float(c.get("buy_usd", 10))
        if p.get("real") and self.live is not None:
            if self._real_today() >= int(c.get("max_buys_per_day", 10)):
                return f"no real buy - {c.get('max_buys_per_day', 10)} real buys already today"
            why = self.live.add_strategy(self.NAME, mint, size_usd, float(c.get("daily_loss_usd", 50)))
            if why:
                return f"no real buy - {why}"
        size = size_usd / usd
        fee, slip, prio = self._x()
        tokens = curve_buy(size * (1 - fee), price) * (1 - slip)
        old_tok, old_px = p["tokens"], p["entry_px"]
        new_entry = (old_tok * old_px + tokens * price) / (old_tok + tokens) if old_tok + tokens else price
        p["peak_mult"] = max(1.0, p["peak_mult"] * old_px / new_entry)
        p.update(entry_px=new_entry, tokens=old_tok + tokens, tokens_bought=p["tokens_bought"] + tokens,
                 sol_in=p["sol_in"] + size + prio, adds=p.get("adds", 0) + 1, last_px=price)
        p.setdefault("add_ts", []).append(time.time())
        self._event("buy", f"{p['symbol']}: bought ${size_usd:g} more at {price * SUPPLY:.0f} SOL mcap"
                           f"{' (REAL)' if p.get('real') else ''} - average entry now {new_entry * SUPPLY:.0f} SOL",
                    mint=mint)
        self._save()
        return None

    # ------------------------------------------------------------------ exits
    def _close(self, p, ts):
        real = bool(p.get("real"))
        super()._close(p, ts)
        if self.closed and self.closed[-1]["mint"] == p["mint"]:
            self.closed[-1].update(real=real, source=p.get("source"))
            self._save()

    def check(self, p, px, ts):
        c = self.cfg()
        p["last_px"], p["last_px_ts"] = px, ts
        mult = px / p["entry_px"]
        p["peak_mult"] = max(p["peak_mult"], mult)
        done = p["done"]
        if p.get("real") and self.live is not None and p["mint"] not in self.live.positions \
                and ts - p["opened"] > 120:                   # you sold it on the real wallet (Sell button)
            p["real"] = False
            return self._sell(p, 1.0, "sold by you", px, ts)
        if not c.get("auto_sell_on", True):
            return                                         # auto-sell OFF: you sell everything yourself
        stop = float(c.get("stop_pct", 40) or 0)
        if stop > 0 and mult <= 1 - stop / 100:
            return self._sell(p, 1.0, f"stop -{stop:g}%", px, ts)
        if not c.get("auto_take_profit", False):
            # optional "while I sleep" take-profits on crazy runners (toggles on the calls panel)
            if c.get("tp6_on") and mult >= 6 and "s6" not in done:
                done.append("s6")
                self._sell(p, 1 / 3, "6x - sold 1/3 (auto take-profit)", px, ts)
                if p["mint"] not in self.positions:
                    return
            if c.get("tp10_on") and mult >= 10 and "s10" not in done:
                done.append("s10")
                self._sell(p, 0.5, "10x - sold half (auto take-profit)", px, ts)
            return                                         # everything else is your call: sell with the Sell button
        if ts - p["opened"] > float(c.get("max_hold_h", 48)) * 3600:
            return self._sell(p, 1.0, f"time limit {c.get('max_hold_h', 48):g}h", px, ts)
        if done and mult <= p["peak_mult"] * (1 - float(c.get("trail_pct", 50)) / 100):
            return self._sell(p, 1.0, f"trailing stop ({p['peak_mult']:.1f}x high)", px, ts)
        q = float(c.get("sell_pct_each", 25)) / 100
        for lvl in (float(c.get("tp1_mult", 3)), float(c.get("tp2_mult", 5)), float(c.get("tp3_mult", 10))):
            key = f"{lvl:g}x"
            if mult >= lvl and key not in done:
                done.append(key)
                left = p["tokens"] / p["tokens_bought"]
                self._sell(p, min(1.0, q / left) if left > 0 else 1.0, f"{key} - sold a quarter", px, ts)
                if p["mint"] not in self.positions:
                    return

    def by_source(self):
        """Per call source (Discord channel / Telegram group): how its calls have done since the call, paper."""
        out = {}
        for q in [*self.positions.values(), *self.closed]:
            src = str(q.get("source") or "unknown")
            key = src.split(" · ", 1)[0] if src.startswith("pasted") else src[:60]
            o = out.setdefault(key, dict(calls=0, open=0, hit2=0, hit5=0, hit10=0, now=[], pnl_usd=0.0))
            o["calls"] += 1
            pk = float(q.get("peak_mult") or 1.0)
            o["hit2"] += pk >= 2
            o["hit5"] += pk >= 5
            o["hit10"] += pk >= 10
            if q["mint"] in self.positions:
                o["open"] += 1
                if q.get("entry_px") and q.get("last_px"):
                    o["now"].append(q["last_px"] / q["entry_px"])
            else:
                o["pnl_usd"] += float(q.get("pnl_usd") or 0)
        for o in out.values():
            n = o.pop("now")
            o["avg_now_mult"] = round(sum(n) / len(n), 2) if n else None
            o["pnl_usd"] = round(o["pnl_usd"], 2)
        return out

    def state(self):
        s = super().state()
        c = self.cfg()
        s["by_source"] = self.by_source()
        s["telegram_real_money"] = bool(c.get("telegram_real_money", False))
        s.update(name=self.NAME, buy_usd=c.get("buy_usd", 10), real_money=bool(c.get("real_money")),
                 real_enabled=bool(c.get("real_money")), real_size_usd=c.get("buy_usd", 10),
                 real_max_open=c.get("max_open", 10), real_daily_loss_usd=c.get("daily_loss_usd", 50),
                 max_buys_per_day=c.get("max_buys_per_day", 10), real_buys_today=self._real_today(),
                 inbox=self.inbox[:15], channel=str(c.get("channel") or ""),
                 telegram_channel=str(c.get("telegram_channel") or ""),
                 auto_take_profit=bool(c.get("auto_take_profit", False)),
                 auto_sell_on=bool(c.get("auto_sell_on", True)), stop_pct=c.get("stop_pct", 40),
                 tp6_on=bool(c.get("tp6_on")), tp10_on=bool(c.get("tp10_on")),
                 desc=(f"Buys every coin posted in the Discord / Telegram calls · ${c.get('buy_usd', 10):g} each · "
                       + ("AUTO-SELL OFF (no stop) · " if not c.get("auto_sell_on", True) else f"stop −{c.get('stop_pct', 40):g}% · ") + (
                           f"¼ at {c.get('tp1_mult', 3):g}×/{c.get('tp2_mult', 5):g}×/{c.get('tp3_mult', 10):g}× · "
                           f"{c.get('max_hold_h', 48):g}h limit" if c.get("auto_take_profit", False)
                           else "no automatic selling otherwise - you sell with the Sell button")))
        for q in s.get("positions", []):
            src = (self.positions.get(q["mint"]) or {}).get("source")
            q["source"] = src
        return s
