"""
Core engine: token tracking, entry filters, exit rules, and a paper broker.

The engine is fed plain event dicts (the same shape PumpPortal sends) plus a
timestamp, so the exact same code runs live (run_paper.py) and on recorded
data (backtest.py). It never signs or sends a transaction.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

log = logging.getLogger("memebot")

TOTAL_SUPPLY = 1_000_000_000  # pump.fun tokens all have 1B supply


# ─────────────────────────────────────────────────────────────── helpers
def _f(x, default=None):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _dev_buy_pct(ev: dict) -> float | None:
    """Dev's launch buy as % of total supply, from a create event."""
    tok = _f(ev.get("initialBuy"))
    if tok is None:
        sol = _f(ev.get("solAmount"))
        if sol is None:
            return None
        v_sol0, v_tok0 = 30.0, 1_073_000_000.0          # pump.fun's initial virtual reserves
        tok = v_tok0 - v_sol0 * v_tok0 / (v_sol0 + sol)
    return tok / TOTAL_SUPPLY * 100


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────────────────────────── state
@dataclass
class TokenState:
    mint: str
    symbol: str
    name: str
    creator: str
    created_ts: float
    dev_buy_sol: float = 0.0
    dev_buy_pct: float | None = None
    status: str = "new"               # new | watching | rejected | held | closed
    sec_state: str = ""               # pending (RugCheck running) | passed | failed
    screen_started: float = 0.0
    security_note: str = ""
    reason: str = ""
    last_fail: str = ""
    # market state
    price: float | None = None        # SOL per token
    v_sol: float | None = None        # bonding-curve / pool virtual reserves, when known
    v_tok: float | None = None
    mcap: float | None = None         # in SOL
    peak_mcap: float = 0.0
    prior_peak: float = 0.0            # peak before the latest trade
    mcap_hist: list = field(default_factory=list)   # [(ts, mcap)]
    last_trade_ts: float = 0.0
    # flow stats
    buys: int = 0
    sells: int = 0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    buyers: set = field(default_factory=set)
    buy_by_wallet: dict = field(default_factory=lambda: defaultdict(float))
    buyer_first: dict = field(default_factory=dict)      # wallet -> first buy ts (for buyer-surge trigger)
    recent: deque = field(default_factory=deque)         # (ts, side, sol) for the last ~2 minutes
    dev_sold: bool = False
    migrated: bool = False
    snapped: bool = False             # comparison snapshot already taken

    def update_market(self, ev: dict, ts: float):
        vs, vt = _f(ev.get("vSolInBondingCurve")), _f(ev.get("vTokensInBondingCurve"))
        mc = _f(ev.get("marketCapSol"))
        sol, tok = _f(ev.get("solAmount")), _f(ev.get("tokenAmount"))
        if vs and vt:
            self.v_sol, self.v_tok = vs, vt
            self.price = vs / vt
        elif mc:
            self.v_sol = self.v_tok = None
            self.price = mc / TOTAL_SUPPLY
        elif sol and tok:
            self.v_sol = self.v_tok = None
            self.price = sol / tok
        if self.price:
            self.mcap = mc if mc else self.price * TOTAL_SUPPLY
            self.peak_mcap = max(self.peak_mcap, self.mcap)
            self.mcap_hist.append((ts, self.mcap))

    def mcap_at(self, ts: float) -> float | None:
        best = None
        for t, m in self.mcap_hist:
            if t <= ts:
                best = m
            else:
                break
        return best if best is not None else (self.mcap_hist[0][1] if self.mcap_hist else None)


@dataclass
class Order:
    mint: str
    side: str            # buy | sell
    due_ts: float
    reason: str
    sol: float = 0.0     # buy size
    frac_of_initial: float | None = None   # sell: fraction of initial tokens; None = sell everything
    frac_of_left: float | None = None      # sell: fraction of tokens still held (copy mode)
    defer_sell: float = 0.0                # copy: share the wallet sold while our buy was still filling
    waited_s: float = 0.0                  # copy: how long we've waited for a first price for this coin


@dataclass
class Position:
    mint: str
    symbol: str
    open_ts: float
    entry_price: float           # effective SOL/token incl. fees
    tokens_initial: float
    tokens_left: float
    sol_in: float
    sol_out: float = 0.0
    peak_price: float = 0.0
    entry_mcap: float = 0.0
    entry_path: str = ""
    mode: str = "strategy"            # strategy | copy
    copy_wallet: str = ""
    wallet_entry_mcap: float = 0.0
    wallet_sell_mcap: float = 0.0     # mcap at the wallet's latest sell (for exit gap)
    exit_gaps: list = field(default_factory=list)
    tp_done: set = field(default_factory=set)
    pending_sell: Order | None = None
    exit_reasons: list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────── output
class Journal:
    def __init__(self, out_dir: str, tag: str):
        os.makedirs(out_dir, exist_ok=True)
        self.files = {}
        self.writers = {}
        specs = {
            "fills": ["time_utc", "mint", "symbol", "side", "reason", "sol", "tokens",
                      "market_price", "mcap_sol", "balance_after"],
            "positions": ["open_utc", "close_utc", "mint", "symbol", "hold_s", "sol_in",
                          "sol_out", "pnl_sol", "pnl_pct", "exit_reasons", "entry_path", "entry_gap_pct",
                          "exit_gap_pct"],
            "decisions": ["time_utc", "mint", "symbol", "outcome", "reason", "age_s",
                          "buyers", "buy_vol", "mcap_sol"],
        }
        for name, cols in specs.items():
            path = os.path.join(out_dir, f"{name}_{tag}.csv")
            new = not os.path.exists(path)
            fh = open(path, "a", newline="", encoding="utf-8", errors="replace")
            w = csv.writer(fh)
            if new:
                w.writerow(cols)
            self.files[name], self.writers[name] = fh, w

    def write(self, name, row):
        self.writers[name].writerow(row)
        self.files[name].flush()

    def close(self):
        for fh in self.files.values():
            fh.close()


class NullFeed:
    """Stand-in for the websocket when replaying."""
    def subscribe(self, mint): pass
    def unsubscribe(self, mint): pass


# ─────────────────────────────────────────────────────────────── engine
class Engine:
    def __init__(self, cfg: dict, feed=None, journal: Journal | None = None,
                 screener=None, blocklist_path: str | None = None):
        self.cfg = cfg
        self.feed = feed or NullFeed()
        self.screener = screener              # async RugCheck screener (live) / demo screener; None = skip
        self.blocklist_path = blocklist_path
        self.bad_creators: set[str] = set()
        if blocklist_path and os.path.exists(blocklist_path):
            with open(blocklist_path, encoding="utf-8", errors="replace") as fh:
                self.bad_creators = {ln.strip() for ln in fh if ln.strip()}
        self.creator_launches = defaultdict(deque)
        self.security_passed = 0
        self.security_failed = 0
        self._seen_sigs: deque = deque()
        self._seen_set: set = set()
        # copy trading
        self.copy_hold = defaultdict(float)        # (wallet, mint) -> tokens the copied wallet holds (seen by us)
        self.copy_bought = defaultdict(float)      # (wallet, mint) -> tokens it bought in the current position
        self.copy_seen = defaultdict(lambda: {"live": 0, "chain": 0, "last": 0.0})   # per-wallet feed diagnostics
        self.copy_stats = Counter()
        self.copy_wallet_mcap = {}                 # mint -> mcap the wallet bought at
        self.copy_wallet_px = {}                   # mint -> SOL/token the wallet paid on its first buy
        self.chain = None                          # ChainBackup (live mode with a Helius key)
        self.live = None                           # LiveTrader (real-money copies), live mode only
        self.snaps = None                          # SnapshotRecorder (lookalike research), live mode only
        self.last_feed_trade_ts = 0.0              # last trade message from the live feed (wall clock)
        self.narr = None                           # Narratives tracker, live mode only
        self.why = None                            # WhyLog: why each coin was passed on (live mode)
        self.lookalike = None                      # Lookalike paper strategy, live mode only
        self.reclaim = None                        # Reclaim paper strategy, live mode only
        self.reclaim_big = None                  # Reclaim, big coins only (paper)
        self.reclaim_strong = None                   # Reclaim, strong bounce only (paper)
        self.rangebreak = None                     # range breakout (paper)
        self.rangehold = None                      # range breakout buys, hold for the bonding level (paper)
        self.rangehold_small = None                # same, only coins under $12K (paper)
        self.rangehold_v2 = None                   # same, only coins 80 min - 3.2 h old (paper)
        self.confirmed = None                      # confirmed breakout V1 (owner's spec, paper)
        self.lookalike_grad = None                 # Lookalike with the graduation exit (paper), live mode only
        self.tests = []                            # paper test variants of the graduation lookalike
        self.survivor = None                       # Survivor breakout (old coins at a new high), live mode only
        self.hotword = None                        # hot-word paper test (Survivor rules, hot trends only)
        self.breakouts = None                      # BreakoutLog: every 44 SOL crossing + wallet #1 buys (research)
        self.skimmer = None                        # Skimmer paper test (quick +20% / -5% trades)
        self.copy_log = None                       # file handle: every followed-wallet trade we see
        self.journal = journal
        self.tokens: dict[str, TokenState] = {}
        self.positions: dict[str, Position] = {}
        self.pending_buys: dict[str, Order] = {}
        self.closed: list[dict] = []
        self.balance = cfg["risk"]["starting_balance_sol"]
        self.day = None
        self.day_pnl = 0.0
        self.day_trade_msgs = 0
        self.total_trade_msgs = 0
        self.outcomes = Counter()
        self.reject_reasons = Counter()
        self.now = 0.0
        self.activity = deque(maxlen=250)     # for the dashboard feed
        self.equity = []                      # [(ts, equity incl. open positions)]
        self._last_eq_ts = 0.0

    def _act(self, kind, t, text, **extra):
        self.activity.append(dict(ts=self.now, kind=kind, symbol=t.symbol if t else "",
                                  mint=t.mint if t else "", text=text, **extra))

    # ---------------------------------------------------------- event intake
    def on_event(self, ev: dict, ts: float):
        self.now = ts
        self._roll_day(ts)
        tx = ev.get("txType")
        mint = ev.get("mint")
        if not mint or not tx:
            return
        sig = ev.get("signature")
        if sig:
            if sig in self._seen_set:
                return                              # same trade from two streams (token + account)
            self._seen_set.add(sig)
            self._seen_sigs.append(sig)
            if len(self._seen_sigs) > 50000:
                self._seen_set.discard(self._seen_sigs.popleft())
        if tx in ("buy", "sell") and ev.get("traderPublicKey") in self.copy_wallets():
            self._on_copy(ev, ts)
        if self.narr is not None:
            if tx == "create":
                self.narr.on_launch(mint, ev.get("name"), ev.get("symbol"), ts)
            elif tx == "migrate":
                self.narr.on_graduate(mint, ts)
        if tx == "create":
            self._on_create(ev, ts)
        elif tx == "migrate":
            self._on_migrate(ev, ts)
        elif tx in ("buy", "sell"):
            if not ev.get("_poll"):                       # polled on-chain reads are free: not paid messages
                self.day_trade_msgs += 1
                self.total_trade_msgs += 1
            if not ev.get("_chain"):
                self.last_feed_trade_ts = time.time()     # wall clock: is PumpPortal still sending trades?
            self._on_trade(ev, ts)

    def on_tick(self, ts: float):
        self.now = ts
        self._roll_day(ts)
        sec = self.cfg["security"]
        for mint in list(self.tokens):
            t = self.tokens.get(mint)
            if t and t.status == "watching":
                self._evaluate_entry(t, ts)
            if t and t.sec_state == "pending" and ts - t.screen_started > sec.get("rugcheck_timeout_s", 6) + 90:
                # screener never answered - apply the configured error policy
                self.on_screen_result(mint, sec.get("rugcheck_on_error", "skip") == "allow",
                                      "rugcheck unavailable", ts)
        for mint in list(self.positions):
            self._evaluate_exit(self.tokens[mint], ts)
        self._fill_due(ts)
        self._mark_equity(ts)
        # prune finished tokens so memory stays flat over long runs
        for mint, t in list(self.tokens.items()):
            if (t.status in ("rejected", "closed") or (t.status == "copy" and mint not in self.pending_buys)) \
                    and ts - t.created_ts > 1800 and mint not in self.positions:
                del self.tokens[mint]

    # ---------------------------------------------------------- handlers
    def _roll_day(self, ts):
        d = _day(ts)
        if d != self.day:
            self.day, self.day_pnl, self.day_trade_msgs = d, 0.0, 0

    # ---------------------------------------------------------- security gate
    def _security_local(self, t: TokenState, ts: float) -> str | None:
        """Instant checks that need no network. Returns a failure reason or None."""
        sec = self.cfg["security"]
        name = f"{t.name} {t.symbol}".lower()
        if any(w.lower() in name for w in sec.get("name_blacklist", [])):
            return "blacklisted name"
        if t.dev_buy_pct is not None and t.dev_buy_pct > sec["max_dev_buy_pct"]:
            return "dev holds too much supply"
        if t.creator:
            if sec.get("block_repeat_dumpers", True) and t.creator in self.bad_creators:
                return "creator dumped a previous token"
            q = self.creator_launches[t.creator]
            while q and ts - q[0] > 86400:
                q.popleft()
            prior = len(q)
            q.append(ts)
            if prior >= sec["max_launches_per_creator_24h"]:
                return "serial launcher"
        return None

    def _gate(self, t: TokenState, ts: float):
        """Entry point for every new token. Instant checks decide whether it's watched at all.
        RugCheck runs in parallel (it needs ~5-25s to index a new mint); the token can't be
        bought until RugCheck passes, and is dropped the moment it fails."""
        fail = self._security_local(t, ts)
        if fail:
            self.security_failed += 1
            return self._reject(t, "security: " + fail, ts, subscribed=False)
        watching = sum(1 for x in self.tokens.values() if x.status == "watching")
        if watching >= self.cfg["max_concurrent_watch"]:
            return self._reject(t, "skipped: watch list full", ts, subscribed=False)
        if self.day_trade_msgs >= self.cfg["max_trade_messages_per_day"]:
            return self._reject(t, "skipped: daily data budget reached", ts, subscribed=False)
        t.status = "watching"
        if self.narr is not None:
            self.narr.on_watched(t.mint, ts)
        self.feed.subscribe(t.mint)
        if self.cfg["security"].get("rugcheck") and self.screener is not None:
            t.sec_state = "pending"
            t.screen_started = ts
            self.screener.request(t.mint)
        else:
            t.sec_state = "passed"
            self.security_passed += 1

    def on_screen_result(self, mint: str, ok: bool, reason: str = "", ts: float | None = None):
        """Called by the RugCheck screener (live) or demo screener when a check finishes."""
        t = self.tokens.get(mint)
        if not t or t.sec_state != "pending" or t.status in ("rejected", "closed"):
            return
        ts = ts or self.now
        if ok:
            t.sec_state = "passed"
            t.security_note = reason
            self.security_passed += 1
        else:
            t.sec_state = "failed"
            self.security_failed += 1
            self._reject(t, "security: " + reason, ts)

    def _mark_dumper(self, creator: str):
        if not creator or creator in self.bad_creators:
            return
        self.bad_creators.add(creator)
        if self.blocklist_path:
            try:
                os.makedirs(os.path.dirname(self.blocklist_path) or ".", exist_ok=True)
                with open(self.blocklist_path, "a", encoding="utf-8", errors="replace") as fh:
                    fh.write(creator + "\n")
            except OSError:
                pass

    def _momentum_on(self):
        return bool(self.cfg.get("strategies", {}).get("momentum", True))

    def _lk_on(self):
        return (self.lookalike is not None and self.lookalike.enabled()) or \
            (self.lookalike_grad is not None and self.lookalike_grad.enabled()) or \
            any(s.enabled() for s in self.tests)

    def _rc_on(self):
        return self.reclaim is not None and self.reclaim.enabled()

    def _research_only(self):
        """Momentum strategy off, but snapshots / lookalike on: watch new tokens without momentum buys."""
        return not self._momentum_on() and ((self.snaps is not None and self.snaps.enabled()) or self._lk_on()
                                            or self._rc_on())

    def _drop(self, t: TokenState, reason: str):
        """Stop watching quietly (research-only mode): no funnel counts, no feed entries."""
        t.status, t.reason = "rejected", reason
        if self.why is not None:
            age = (self.now or time.time()) - t.created_ts
            self.why.note(t.mint, t.symbol, "stopped watching",
                          f"{reason.replace('research: done', 'done')} · {age / 60:.0f} min old · now {t.mcap or 0:.0f} SOL"
                          f" · peak {t.peak_mcap or 0:.0f} SOL" + (" · dev sold" if t.dev_sold else ""))
        self.feed.unsubscribe(t.mint)
        t.mcap_hist.clear(); t.buyers.clear(); t.buy_by_wallet.clear(); t.buyer_first.clear(); t.recent.clear()

    def _on_create(self, ev, ts):
        if self.cfg["universe"] != "new_tokens" or not (self._momentum_on() or self._research_only()):
            return
        t = TokenState(
            mint=ev["mint"], symbol=str(ev.get("symbol", "?"))[:20], name=str(ev.get("name", ""))[:60],
            creator=ev.get("traderPublicKey", ""), created_ts=ts,
            dev_buy_sol=_f(ev.get("solAmount"), 0.0),
        )
        t.dev_buy_pct = _dev_buy_pct(ev)
        t.update_market(ev, ts)
        t.last_trade_ts = ts
        self.tokens[t.mint] = t
        self._gate(t, ts)

    def _on_migrate(self, ev, ts):
        mint = ev["mint"]
        if mint in self.tokens:
            self.tokens[mint].migrated = True
            return
        if self.cfg["universe"] != "migrated":
            return
        t = TokenState(mint=mint, symbol=str(ev.get("symbol", mint[:6])), name=str(ev.get("name", "")),
                       creator="", created_ts=ts, migrated=True, last_trade_ts=ts)
        self.tokens[mint] = t
        self._gate(t, ts)

    def _on_trade(self, ev, ts):
        t = self.tokens.get(ev["mint"])
        if not t or t.status in ("rejected", "closed"):
            return
        sol = _f(ev.get("solAmount"), 0.0)
        trader = ev.get("traderPublicKey", "")
        prev_mcap = t.mcap
        t.prior_peak = t.peak_mcap                  # highest mcap before this trade (lookalike: "has it dipped?")
        t.update_market(ev, ts)
        if self.narr is not None and t.creator:
            self.narr.on_mcap(t.mint, prev_mcap, t.mcap, ts)
        if self.breakouts is not None and t.creator and prev_mcap and t.mcap and prev_mcap < 44.0 <= t.mcap:
            try:
                from memebot.snapshots import stream_features
                f = stream_features(t, ts)
                f["prior_peak"] = round(t.prior_peak or 0, 1)
                self.breakouts.on_cross(t.mint, t.symbol, ts, ts - t.created_ts, t.mcap, "stream", t.creator, f)
            except Exception as e:
                log.debug("breakout record failed: %s", e)
        if self.snaps is not None and not t.snapped and prev_mcap and t.mcap and t.creator:
            sc = self.cfg.get("snapshots") or {}
            line = _f(sc.get("cross_mcap_sol"), 42.0)
            # skip launch-second jumps (creator/bundle buys) - wallet #1 buys coins minutes to hours old
            if prev_mcap < line <= t.mcap and ts - t.created_ts >= _f(sc.get("min_age_s"), 120.0):
                t.snapped = True
                self.snaps.crossed(t, ts)
        if self.lookalike is not None and t.creator and prev_mcap:
            self.lookalike.maybe_enter(t, prev_mcap, ts)
        if self.lookalike_grad is not None and t.creator and prev_mcap:
            self.lookalike_grad.maybe_enter(t, prev_mcap, ts)
        if t.creator and prev_mcap:
            for s in self.tests:
                s.maybe_enter(t, prev_mcap, ts)
        if self.reclaim is not None and t.creator:
            self.reclaim.maybe_enter(t, prev_mcap, ts)
        if self.reclaim_big is not None and t.creator:
            self.reclaim_big.maybe_enter(t, prev_mcap, ts)
        if self.reclaim_strong is not None and t.creator:
            self.reclaim_strong.maybe_enter(t, prev_mcap, ts)
        if self.rangebreak is not None and t.creator:
            self.rangebreak.maybe_enter(t, prev_mcap, ts)
        if self.rangehold is not None and t.creator:
            self.rangehold.maybe_enter(t, prev_mcap, ts)
        if self.rangehold_small is not None and t.creator:
            self.rangehold_small.maybe_enter(t, prev_mcap, ts)
        if self.rangehold_v2 is not None and t.creator:
            self.rangehold_v2.maybe_enter(t, prev_mcap, ts)
        if self.confirmed is not None and t.creator:
            self.confirmed.maybe_enter(t, prev_mcap, ts)
        if self.survivor is not None and t.creator:
            self.survivor.maybe_enter(t, prev_mcap, ts)
        if self.skimmer is not None and t.creator and prev_mcap:
            self.skimmer.maybe_enter(t, prev_mcap, ts)
        if self.hotword is not None and t.creator:
            self.hotword.maybe_enter(t, prev_mcap, ts)
        t.last_trade_ts = ts
        if ev["txType"] == "buy":
            t.buys += 1
            t.buy_vol += sol
            if trader and trader != t.creator:
                t.buyers.add(trader)
                t.buy_by_wallet[trader] += sol
                t.buyer_first.setdefault(trader, ts)
        else:
            t.sells += 1
            t.sell_vol += sol
            if t.creator and trader == t.creator:
                t.dev_sold = True
                if t.status in ("watching", "held"):
                    self._mark_dumper(t.creator)
        t.recent.append((ts, ev["txType"], sol))
        while t.recent and ts - t.recent[0][0] > 120:
            t.recent.popleft()
        if t.status == "watching":
            self._evaluate_entry(t, ts)
        elif t.status == "held":
            self._evaluate_exit(t, ts)
        self._fill_due(ts)

    # ---------------------------------------------------------- copy trading
    @staticmethod
    def _entry_gap(p):
        if p.mode != "copy" or not p.wallet_entry_mcap or not p.entry_mcap:
            return None
        return round((p.entry_mcap / p.wallet_entry_mcap - 1) * 100, 1)

    def _per_wallet(self):
        out = {}
        for w in sorted(self.copy_wallets()):
            short = w[:4] + "…" + w[-4:]
            tag = f"copy: {short}"
            cl = [c for c in self.closed if c.get("entry_path") == tag]
            op = [p for p in self.positions.values() if p.entry_path == tag]
            gaps = [c["entry_gap_pct"] for c in cl if c.get("entry_gap_pct") is not None] + \
                   [g for g in (self._entry_gap(p) for p in op) if g is not None]
            xg = [c["exit_gap_pct"] for c in cl if c.get("exit_gap_pct") is not None]
            wins = sum(1 for c in cl if c["pnl_sol"] > 0)
            upnl = sum(p.sol_out + self._position_value(p) - p.sol_in for p in op)
            seen = self.copy_seen.get(w) or {"live": 0, "chain": 0, "last": 0.0}
            out[short] = dict(wallet=w, open=len(op), closed=len(cl), wins=wins,
                              seen_live=seen["live"], seen_chain=seen["chain"],
                              last_seen_s=round(self.now - seen["last"]) if seen["last"] else None,
                              win_rate=round(wins / len(cl) * 100, 1) if cl else None,
                              pnl=round(sum(c["pnl_sol"] for c in cl), 4), open_pnl=round(upnl, 4),
                              sol_in=round(sum(c["sol_in"] for c in cl), 4),
                              avg_entry_gap=round(sum(gaps) / len(gaps), 1) if gaps else None,
                              avg_exit_gap=round(sum(xg) / len(xg), 1) if xg else None)
        return out

    def chase_gap(self, mint: str, price: float | None):
        """% our price is above the wallet's if that breaks the "don't chase" limit, else None."""
        lim = _f((self.cfg.get("copy_trade") or {}).get("max_entry_gap_pct"), 0.0)
        wpx = self.copy_wallet_px.get(mint)
        if not lim or not wpx or not price:
            return None
        gap = (price / wpx - 1) * 100
        return gap if gap > lim else None

    def rename(self, mint: str, symbol: str, name: str = ""):
        """Swap an address-placeholder symbol for the token's real ticker everywhere it's shown."""
        from memebot.names import is_placeholder
        t = self.tokens.get(mint)
        if t and is_placeholder(t.symbol, mint):
            t.symbol, t.name = symbol, t.name or name
        p = self.positions.get(mint)
        if p and is_placeholder(p.symbol, mint):
            p.symbol = symbol
        for c in self.closed:
            if c.get("mint") == mint and is_placeholder(c.get("symbol", ""), mint):
                c["symbol"] = symbol
        for a in self.activity:
            if a.get("mint") == mint and is_placeholder(a.get("symbol", ""), mint):
                a["symbol"] = symbol

    def name_targets(self) -> list:
        from memebot.names import is_placeholder
        out = [m for m, t in self.tokens.items() if t.status in ("copy", "held") and is_placeholder(t.symbol, m)]
        out += [c["mint"] for c in self.closed if is_placeholder(c.get("symbol", ""), c["mint"])]
        return out

    def seen_signature(self, sig: str) -> bool:
        return sig in self._seen_set

    def copy_from_chain(self, ev: dict, ts: float):
        """A followed-wallet trade read from the chain (backup source / amount lookup)."""
        sig = ev.get("signature")
        if sig and sig not in self._seen_set:
            self._seen_set.add(sig)
            self._seen_sigs.append(sig)
        self.now = ts
        ev["_chain"] = True
        self._on_copy(ev, ts)

    def _wallet_left_pct(self, p):
        if p.mode != "copy":
            return None
        for (w, m), bought in self.copy_bought.items():
            if m == p.mint and bought:
                return round(self.copy_hold.get((w, m), 0.0) / bought * 100)
        return None

    def copy_wallets(self) -> set:
        cp = self.cfg.get("copy_trade") or {}
        if not cp.get("enabled"):
            return set()
        return {w.strip() for w in str(cp.get("wallet", "")).split(",") if w.strip()}

    def _on_copy(self, ev: dict, ts: float):
        """The followed wallet traded. Mirror it on paper: same token, our size, after our latency."""
        cp = self.cfg["copy_trade"]
        w, mint = ev["traderPublicKey"], ev["mint"]
        tok = _f(ev.get("tokenAmount"), 0.0)
        wsol = _f(ev.get("solAmount"), 0.0)
        from_chain = bool(ev.get("_chain"))
        self.copy_seen[w]["chain" if from_chain else "live"] += 1
        self.copy_seen[w]["last"] = ts
        if self.copy_log:
            try:
                self.copy_log.write(json.dumps({"ts": ts, **{k: v for k, v in ev.items() if not k.startswith("_")},
                                                "via": "chain" if from_chain else "live"}) + "\n")
                self.copy_log.flush()
            except (OSError, TypeError, ValueError):
                pass
        if tok <= 0 and not from_chain:
            if self.chain is not None and ev.get("signature"):
                self.copy_stats["looked_up"] += 1         # no usable amount: read the exact trade on-chain
                self.chain.resolve(ev["signature"], w)
                return
            if ev["txType"] == "sell":
                tok = self.copy_hold.get((w, mint), 0.0)  # no backup: treat as a full exit
                self.copy_stats["assumed_full_sell"] += 1
            elif wsol and _f(ev.get("marketCapSol")):
                tok = wsol / (_f(ev.get("marketCapSol")) / TOTAL_SUPPLY)
        key = (w, mint)
        lat = self.cfg["execution"]["latency_s"]
        wshort = w[:4] + "…" + w[-4:]
        if ev["txType"] == "buy":
            self.copy_stats["wallet_buys"] += 1
            first = self.copy_hold[key] <= 0
            if first:
                self.copy_bought[key] = 0.0
            self.copy_hold[key] += tok
            self.copy_bought[key] += tok
            t = self.tokens.get(mint)
            if t is None or t.status in ("rejected", "closed"):
                t = TokenState(mint=mint, symbol=str(ev.get("symbol") or mint[:5]).upper()[:12], name="",
                               creator="", created_ts=ts, status="copy", last_trade_ts=ts)
                self.tokens[mint] = t
            if not from_chain:
                t.update_market(ev, ts)
                t.last_trade_ts = ts
            self.feed.subscribe(mint)                     # need live prices for fills and marking
            if first:
                wpx = wsol / tok if wsol > 0 and tok > 0 else None     # what the wallet actually paid per token
                self.copy_wallet_px[mint] = wpx
                self.copy_wallet_mcap[mint] = (wpx * TOTAL_SUPPLY if wpx else None) or \
                    _f(ev.get("marketCapSol")) or (t.mcap or 0.0)
                if self.live:
                    self.live.on_copy_buy(w, mint, wsol, t.symbol, wallet_px=wpx)
                if self.snaps is not None:
                    self.snaps.wallet_buy(w, mint, ts, wsol, wpx, self.copy_wallet_mcap[mint], t, ev.get("signature"))
                if self.breakouts is not None:
                    self.breakouts.on_w1_buy(w, mint, ts, wsol, self.copy_wallet_mcap[mint], ev.get("signature"))
            mode = cp.get("size_mode", "fixed")

            def sized(wallet_sol, fixed):
                if mode == "match" and wallet_sol > 0:
                    return wallet_sol
                if mode == "scale" and wallet_sol > 0:
                    return wallet_sol * cp.get("scale_pct", 10) / 100
                return fixed

            if mint in self.positions or mint in self.pending_buys:
                if not cp.get("follow_adds", True) or mint in self.pending_buys:
                    return
                first_sol = self.copy_stats.get(("first_sol", mint)) or wsol or 1.0
                size = sized(wsol, cp["size_sol"] * min(3.0, wsol / first_sol) if first_sol else cp["size_sol"])
            elif not first:
                return                                    # we skipped their first buy; don't start mid-position
            else:
                self.copy_stats[("first_sol", mint)] = wsol
                size = sized(wsol, cp["size_sol"])
                copy_open = sum(1 for p in self.positions.values() if p.mode == "copy")
                if copy_open >= cp.get("max_open", 100):
                    self.copy_stats["skipped: max open"] += 1
                    return self._act("reject", t, "Copy skipped · max copy positions reached")
            if self.balance < size + 2 * self.cfg["execution"]["priority_fee_sol"]:
                self.copy_stats["skipped: balance"] += 1
                return self._act("reject", t, "Copy skipped · paper balance too low")
            self.pending_buys[mint] = Order(mint, "buy", ts + lat, f"copy: {wshort}", sol=size)
            self.copy_stats["copied_buys"] += 1
            self._act("signal", t, f"Wallet {wshort} bought {wsol:.2f} SOL at mcap {t.mcap or 0:.0f} SOL · "
                                   f"copying with {size:.2f} SOL", path="copy")
            log.info("COPY    %-10s wallet %s bought %.2f SOL @ mcap %.0f", t.symbol, wshort, wsol, t.mcap or 0)
        else:
            self.copy_stats["wallet_sells"] += 1
            held = self.copy_hold.get(key, 0.0)
            t = self.tokens.get(mint)
            if t and not from_chain:
                t.update_market(ev, ts)
                t.last_trade_ts = ts
            if held <= 0:
                if self.live and mint in self.live.positions:
                    self.live.on_copy_sell_unknown(w, mint, tok)   # live position from before a restart
                self.copy_stats["older_sells"] += 1       # a position it opened before we started following
                return
            self.copy_stats["followed_sells"] += 1
            frac = min(1.0, tok / held) if held else 1.0
            self.copy_hold[key] = max(0.0, held - tok)
            if frac > 0.97 or self.copy_hold[key] <= held * 0.01:
                frac = 1.0
                self.copy_hold.pop(key, None)
            if self.live:
                self.live.on_copy_sell(w, mint, frac)
            if mint in self.pending_buys:
                o = self.pending_buys[mint]
                if frac >= 1.0 and mint not in self.positions:
                    del self.pending_buys[mint]           # they sold out before our buy even filled
                    self.copy_stats["cancelled_before_fill"] += 1
                    if t:
                        self._act("reject", t, "Copy cancelled · wallet sold before our buy filled")
                    return
                if mint not in self.positions:            # partial sell before our fill: apply right after it
                    o.defer_sell = 1 - (1 - o.defer_sell) * (1 - frac)
                    self.copy_stats["mirrored_sells"] += 1
                    return
            p = self.positions.get(mint)
            if not p or not cp.get("follow_sells", True):
                return
            if _f(ev.get("marketCapSol")):
                p.wallet_sell_mcap = _f(ev.get("marketCapSol"))
            if p.pending_sell is not None:
                prev = p.pending_sell.frac_of_left if p.pending_sell.frac_of_left is not None else 1.0
                frac = 1 - (1 - prev) * (1 - frac)
            p.pending_sell = Order(mint, "sell", ts + lat, f"copied sell ({frac * 100:.0f}%)",
                                   frac_of_left=None if frac >= 1.0 else frac)
            self.copy_stats["mirrored_sells"] += 1

    # ---------------------------------------------------------- entry logic
    def entry_checks(self, t: TokenState, ts: float) -> list[dict]:
        """Every entry filter with its current value.
        group: core     - safety filters, ALL required
               momentum - trigger A (steady buy pressure + price momentum)
               surge    - trigger B (rapid growth in new buyers)
        A token is bought when every core check passes AND either trigger passes."""
        e = self.cfg["entry"]
        mc = t.mcap or 0.0
        ratio = t.buy_vol / t.sell_vol if t.sell_vol > 0 else (99.0 if t.buy_vol else 0.0)
        top = (max(t.buy_by_wallet.values()) / max(t.buy_vol, 1e-9) * 100) if t.buy_by_wallet else 0.0
        past = t.mcap_at(ts - e["momentum_lookback_s"])
        mom = (mc / past - 1) * 100 if past and mc else 0.0
        dd = (1 - mc / t.peak_mcap) * 100 if t.peak_mcap and mc else 0.0
        checks = [
            dict(group="core", key="dev", label="Dev holding", val="sold" if t.dev_sold else "holding",
                 ok=not (e["reject_if_dev_sold"] and t.dev_sold), fail="HARD:dev sold"),
            dict(group="core", key="buyers", label="Unique buyers", val=len(t.buyers), need=f"≥{e['min_unique_buyers']}",
                 ok=len(t.buyers) >= e["min_unique_buyers"], fail="too few unique buyers"),
            dict(group="core", key="buys", label="Buys", val=t.buys, need=f"≥{e['min_buy_count']}",
                 ok=t.buys >= e["min_buy_count"], fail="too few buys"),
            dict(group="core", key="vol", label="Buy volume", val=round(t.buy_vol, 2), need=f"≥{e['min_buy_volume_sol']}",
                 ok=t.buy_vol >= e["min_buy_volume_sol"], fail="buy volume too low"),
            dict(group="core", key="mcap_min", label="Mcap floor", val=round(mc, 1), need=f"≥{e['min_mcap_sol']}",
                 ok=mc >= e["min_mcap_sol"], fail="mcap below min"),
            dict(group="core", key="mcap_max", label="Mcap ceiling", val=round(mc, 1), need=f"≤{e['max_mcap_sol']}",
                 ok=mc <= e["max_mcap_sol"], fail="HARD:mcap above max"),
            dict(group="core", key="whale", label="Top wallet", val=round(top, 1), need=f"≤{e['max_top_buyer_share_pct']}%",
                 ok=top <= e["max_top_buyer_share_pct"], fail="one wallet dominates buying"),
            dict(group="core", key="dd", label="Off peak", val=round(dd, 1), need=f"≤{e['max_drawdown_from_peak_pct']}%",
                 ok=dd <= e["max_drawdown_from_peak_pct"], fail="already pulling back from peak"),
            dict(group="momentum", key="ratio", label="Buy/sell (all time)", val=round(ratio, 2),
                 need=f"≥{e['min_buy_sell_ratio']}", ok=ratio >= e["min_buy_sell_ratio"], fail="sell pressure too high"),
            dict(group="momentum", key="mom", label=f"Price up ({e['momentum_lookback_s']:g}s)", val=round(mom, 1),
                 need=f"≥{e['min_momentum_pct']}%", ok=mom >= e["min_momentum_pct"] or not past, fail="momentum too weak"),
        ]
        sb = e.get("buyer_surge") or {}
        if sb.get("enabled"):
            win = sb["window_s"]
            new_b = sum(1 for f in t.buyer_first.values() if f >= ts - win)
            bv = sum(x[2] for x in t.recent if x[0] >= ts - win and x[1] == "buy")
            sv = sum(x[2] for x in t.recent if x[0] >= ts - win and x[1] == "sell")
            rr = bv / sv if sv > 0 else (99.0 if bv else 0.0)
            spast = t.mcap_at(ts - win)
            smom = (mc / spast - 1) * 100 if spast and mc else 0.0
            checks += [
                dict(group="surge", key="newb", label=f"New buyers ({win:g}s)", val=new_b,
                     need=f"≥{sb['min_new_buyers']}", ok=new_b >= sb["min_new_buyers"], fail="no buyer surge"),
                dict(group="surge", key="rratio", label=f"Buy/sell ({win:g}s)", val=round(rr, 2),
                     need=f"≥{sb['min_recent_buy_sell_ratio']}", ok=rr >= sb["min_recent_buy_sell_ratio"],
                     fail="surge: recent selling too heavy"),
                dict(group="surge", key="smom", label=f"Price up ({win:g}s)", val=round(smom, 1),
                     need=f"≥{sb['min_momentum_pct']}%", ok=smom >= sb["min_momentum_pct"],
                     fail="surge: price not rising"),
            ]
        return checks

    def entry_status(self, t: TokenState, ts: float, checks=None) -> tuple[str | None, str | None]:
        """(first failing reason or None, trigger that fired: 'momentum' | 'buyer surge' | None)."""
        if t.mcap is None:
            return "no price yet", None
        checks = checks if checks is not None else self.entry_checks(t, ts)
        for c in checks:
            if c["group"] == "core" and not c["ok"]:
                return c["fail"], None
        mom = [c for c in checks if c["group"] == "momentum"]
        surge = [c for c in checks if c["group"] == "surge"]
        if all(c["ok"] for c in mom):
            return None, "momentum"
        if surge and all(c["ok"] for c in surge):
            return None, "buyer surge"
        return next(c["fail"] for c in mom if not c["ok"]), None

    def _check_entry(self, t: TokenState, ts: float) -> str | None:
        return self.entry_status(t, ts)[0]

    def _evaluate_entry(self, t: TokenState, ts: float):
        w = self.cfg["watch"]
        age = ts - t.created_ts
        if t.mint in self.pending_buys:
            return
        if not self._momentum_on():                       # research-only: never buy, drop once recorded
            sc = self.cfg.get("snapshots") or {}
            snap_done = t.snapped or not (self.snaps is not None and self.snaps.enabled())
            lk_done = all(s is None or not s.enabled() or t.mint in s.traded for s in (self.lookalike, self.lookalike_grad, *self.tests, *([self.skimmer] if self.skimmer else [])))
            rc_done = not self._rc_on() or not self.reclaim.wants_watch(t.mint)
            if (snap_done and lk_done and rc_done) or age > _f(sc.get("watch_s"), 1800.0) or (age > 300 and t.mcap and t.mcap < 30):
                self._drop(t, "research: done")           # recorded, too old, or dead - free the slot
            return
        fail, path = self.entry_status(t, ts)
        if fail and fail.startswith("HARD:"):
            self._reject(t, fail[5:], ts)
            return
        if age > w["max_watch_s"]:
            self._reject(t, f"timed out ({t.last_fail or fail})", ts)
            return
        if age < w["min_age_s"]:
            return
        if fail:
            t.last_fail = fail
            return
        if t.sec_state != "passed":
            t.last_fail = "waiting for RugCheck"
            return
        # all filters passed -> risk checks
        r, x = self.cfg["risk"], self.cfg["execution"]
        strat_open = sum(1 for p in self.positions.values() if p.mode == "strategy") + \
            sum(1 for o in self.pending_buys.values() if not o.reason.startswith("copy"))
        if strat_open >= r["max_open_positions"]:
            t.last_fail = "max open positions"
            return
        if self.day_pnl <= -r["daily_loss_limit_sol"]:
            t.last_fail = "daily loss limit hit"
            return
        if self.balance < x["position_size_sol"] + 2 * x["priority_fee_sol"]:
            t.last_fail = "insufficient paper balance"
            return
        self.pending_buys[t.mint] = Order(t.mint, "buy", ts + x["latency_s"], path, sol=x["position_size_sol"])
        self._decision(t, "BUY SIGNAL", f"all filters passed ({path})", ts)
        log.info("SIGNAL  %-10s [%s] mcap=%.0f SOL buyers=%d buyvol=%.1f age=%.0fs",
                 t.symbol, path, t.mcap, len(t.buyers), t.buy_vol, age)
        if path == "buyer surge":
            win = self.cfg["entry"]["buyer_surge"]["window_s"]
            nb = sum(1 for f in t.buyer_first.values() if f >= ts - win)
            detail = f"Buyer surge · {nb} new buyers in {win:g}s"
        else:
            detail = "Momentum · steady buy pressure"
        self._act("signal", t, f"{detail} · {len(t.buyers)} buyers · mcap {t.mcap:.0f} SOL", path=path)

    def _reject(self, t: TokenState, reason: str, ts: float, subscribed=True):
        t.status, t.reason = "rejected", reason
        if self.why is not None:
            self.why.note(t.mint, t.symbol, "dropped" if subscribed else "not watched",
                          reason + (f" · {ts - t.created_ts:.0f}s old · {t.mcap or 0:.0f} SOL" if subscribed else ""), ts)
        if subscribed:
            self.feed.unsubscribe(t.mint)
        key = reason.split(" (")[0]
        self.reject_reasons[key if not key.startswith("timed out") else
                            "timed out: " + (t.last_fail or "no signal")] += 1
        self._decision(t, "REJECT", reason, ts)
        if subscribed:
            self._act("reject", t, reason)
        # drop heavy state; keep a stub so late messages are ignored
        t.mcap_hist.clear(); t.buyers.clear(); t.buy_by_wallet.clear(); t.buyer_first.clear(); t.recent.clear()
        return False

    def _decision(self, t, outcome, reason, ts):
        self.outcomes[outcome] += 1
        if self.journal:
            self.journal.write("decisions", [_iso(ts), t.mint, t.symbol, outcome, reason,
                                             round(ts - t.created_ts, 1), len(t.buyers),
                                             round(t.buy_vol, 3), round(t.mcap or 0, 1)])

    # ---------------------------------------------------------- exit logic
    def _evaluate_exit(self, t: TokenState, ts: float):
        p = self.positions.get(t.mint)
        if not p or not t.price:
            return
        x = self.cfg["exit"]
        p.peak_price = max(p.peak_price, t.price)
        if p.pending_sell and p.pending_sell.frac_of_initial is None and p.pending_sell.frac_of_left is None:
            return  # full exit already queued
        if p.mode == "copy":
            cp = self.cfg.get("copy_trade", {})
            lat = self.cfg["execution"]["latency_s"]
            if cp.get("safety_stop_pct") and (t.price / p.entry_price - 1) * 100 <= -cp["safety_stop_pct"]:
                p.pending_sell = Order(t.mint, "sell", ts + lat, "copy safety stop")
            elif cp.get("max_hold_h") and ts - p.open_ts >= cp["max_hold_h"] * 3600:
                p.pending_sell = Order(t.mint, "sell", ts + lat, "copy max hold")
            return

        def sell_all(reason):
            p.pending_sell = Order(t.mint, "sell", ts + self.cfg["execution"]["latency_s"], reason)

        gain = (t.price / p.entry_price - 1) * 100
        peak_gain = (p.peak_price / p.entry_price - 1) * 100
        if x["exit_on_dev_sell"] and t.dev_sold:
            return sell_all("dev sold")
        if gain <= -x["stop_loss_pct"]:
            return sell_all("stop loss")
        if peak_gain >= x["trailing_arms_after_gain_pct"] and \
                t.price <= p.peak_price * (1 - x["trailing_stop_pct"] / 100):
            return sell_all("trailing stop")
        if ts - p.open_ts >= x["max_hold_s"]:
            return sell_all("time stop")
        if ts - t.last_trade_ts >= x["stale_after_s"]:
            return sell_all("stale / no trades")
        if p.pending_sell is None:
            for i, lvl in enumerate(x["take_profit"]):
                if i not in p.tp_done and gain >= lvl["gain_pct"]:
                    p.tp_done.add(i)
                    p.pending_sell = Order(t.mint, "sell", ts + self.cfg["execution"]["latency_s"],
                                           f"take profit {i + 1} (+{lvl['gain_pct']}%)",
                                           frac_of_initial=lvl["sell_pct"] / 100)
                    break

    # ---------------------------------------------------------- paper broker
    def _sim_buy(self, t: TokenState, sol: float) -> float:
        x = self.cfg["execution"]
        sol_net = sol * (1 - x["platform_fee_pct"] / 100)
        if t.v_sol and t.v_tok:
            tokens = t.v_tok - (t.v_sol * t.v_tok) / (t.v_sol + sol_net)   # constant-product impact
        else:
            tokens = sol_net / t.price
        return tokens * (1 - x["extra_slippage_pct"] / 100)

    def _sim_sell(self, t: TokenState, tokens: float) -> float:
        x = self.cfg["execution"]
        if t.v_sol and t.v_tok:
            sol = t.v_sol - (t.v_sol * t.v_tok) / (t.v_tok + tokens)
        else:
            sol = tokens * t.price
        sol *= (1 - x["extra_slippage_pct"] / 100) * (1 - x["platform_fee_pct"] / 100)
        return max(sol - x["priority_fee_sol"], 0.0)

    def _fill_due(self, ts: float):
        x = self.cfg["execution"]
        for mint, o in list(self.pending_buys.items()):
            if ts < o.due_ts:
                continue
            t = self.tokens[mint]
            if not t.price and not t.dev_sold and o.reason.startswith("copy") and o.waited_s < 20:
                o.waited_s += 1.0                         # coin we weren't watching: its price arrives with the next
                o.due_ts = ts + 1.0                       # on-chain poll (every ~3 s) - wait for it instead of giving up
                continue
            del self.pending_buys[mint]
            if t.dev_sold or not t.price:
                self._reject(t, "aborted before fill (dev sold / no price)", ts)
                continue
            gap = self.chase_gap(mint, t.price) if o.reason.startswith("copy") and mint not in self.positions else None
            if gap is not None:
                self.copy_stats["skipped: chasing"] += 1
                t.status = "copy"
                self._act("reject", t, f"Copy skipped · price already +{gap:.0f}% above what the wallet paid "
                                       f"(limit {self.cfg['copy_trade'].get('max_entry_gap_pct', 0):g}%)")
                continue
            tokens = self._sim_buy(t, o.sol)
            cost = o.sol + x["priority_fee_sol"]
            self.balance -= cost
            t.status = "held"
            if mint in self.positions:                   # copy mode: wallet added to its position
                p = self.positions[mint]
                p.tokens_left += tokens
                p.tokens_initial += tokens
                p.sol_in += cost
                p.entry_price = p.sol_in / p.tokens_initial
                self._fill_row(ts, t, "BUY", "copy add", cost, tokens)
                self._act("buy", t, f"Copied an add · {cost:.3f} SOL at mcap {t.mcap:.0f} SOL", sol=cost)
                continue
            is_copy = o.reason.startswith("copy")
            self.positions[mint] = p_new = Position(mint, t.symbol, ts, cost / tokens, tokens, tokens, cost,
                                            peak_price=t.price, entry_mcap=t.mcap or 0.0, entry_path=o.reason,
                                            mode="copy" if is_copy else "strategy",
                                            copy_wallet=o.reason.split(":", 1)[1].strip() if is_copy and ":" in o.reason else "",
                                            wallet_entry_mcap=self.copy_wallet_mcap.get(mint, 0.0) if is_copy else 0.0)
            if o.defer_sell > 0:
                p_new.pending_sell = Order(mint, "sell", ts, f"copied sell ({o.defer_sell * 100:.0f}%)",
                                           frac_of_left=None if o.defer_sell >= 0.999 else o.defer_sell)
            self._fill_row(ts, t, "BUY", "entry: " + o.reason, cost, tokens)
            self._act("buy", t, f"Paper buy {cost:.3f} SOL at mcap {t.mcap:.0f} SOL", sol=cost)
            log.info("BUY     %-10s %.3f SOL @ mcap %.0f SOL  (balance %.3f)", t.symbol, cost, t.mcap, self.balance)
        for mint, p in list(self.positions.items()):
            o = p.pending_sell
            if not o or ts < o.due_ts:
                continue
            t = self.tokens[mint]
            if o.frac_of_left is not None:
                qty = p.tokens_left * min(1.0, o.frac_of_left)
            elif o.frac_of_initial is not None:
                qty = min(p.tokens_left, p.tokens_initial * o.frac_of_initial)
            else:
                qty = p.tokens_left
            proceeds = self._sim_sell(t, qty)
            p.tokens_left -= qty
            p.sol_out += proceeds
            p.exit_reasons.append(o.reason)
            p.pending_sell = None
            self.balance += proceeds
            self._fill_row(ts, t, "SELL", o.reason, proceeds, qty)
            if p.mode == "copy" and o.reason.startswith("copied sell") and p.wallet_sell_mcap and t.mcap:
                p.exit_gaps.append(round((t.mcap / p.wallet_sell_mcap - 1) * 100, 1))
            if p.tokens_left > p.tokens_initial * 1e-6:   # partial; full exits get a 'close' entry
                self._act("sell", t, f"{o.reason[:1].upper() + o.reason[1:]} · sold {qty / p.tokens_initial * 100:.0f}% "
                                     f"for {proceeds:.3f} SOL", sol=proceeds)
            log.info("SELL    %-10s %-24s +%.3f SOL @ mcap %.0f SOL", t.symbol, o.reason, proceeds, t.mcap or 0)
            if p.tokens_left <= p.tokens_initial * 1e-6:
                self._close(p, t, ts)

    def _close(self, p: Position, t: TokenState, ts: float):
        pnl = p.sol_out - p.sol_in
        self.day_pnl += pnl
        del self.positions[p.mint]
        t.status = "closed"
        self.feed.unsubscribe(p.mint)
        rec = dict(open_utc=_iso(p.open_ts), close_utc=_iso(ts), mint=p.mint, symbol=p.symbol,
                   hold_s=round(ts - p.open_ts), sol_in=round(p.sol_in, 5), sol_out=round(p.sol_out, 5),
                   pnl_sol=round(pnl, 5), pnl_pct=round(pnl / p.sol_in * 100, 1),
                   exit_reasons=" | ".join(p.exit_reasons), entry_path=p.entry_path,
                   entry_gap_pct=self._entry_gap(p), exit_gap_pct=round(sum(p.exit_gaps) / len(p.exit_gaps), 1)
                   if p.exit_gaps else None)
        self.closed.append(rec)
        if self.journal:
            self.journal.write("positions", list(rec.values()))
        log.info("CLOSED  %-10s pnl %+.3f SOL (%+.0f%%)  %s", p.symbol, pnl, rec["pnl_pct"], rec["exit_reasons"])
        self._act("close", t, f"Closed · {p.exit_reasons[-1]}", pnl=pnl, pnl_pct=rec["pnl_pct"])
        self._mark_equity(ts, force=True)

    def _fill_row(self, ts, t, side, reason, sol, tokens):
        if self.journal:
            self.journal.write("fills", [_iso(ts), t.mint, t.symbol, side, reason, round(sol, 5),
                                         round(tokens, 2), f"{t.price:.3e}", round(t.mcap or 0, 1),
                                         round(self.balance, 5)])

    # ---------------------------------------------------------- shutdown / summary
    def close_all(self, ts: float, reason="shutdown"):
        for mint, p in list(self.positions.items()):
            p.pending_sell = Order(mint, "sell", ts, reason)
        self._fill_due(ts)

    # ---------------------------------------------------------- dashboard support
    def _position_value(self, p: Position) -> float:
        t = self.tokens.get(p.mint)
        if not t or not t.price or p.tokens_left <= 0:
            return 0.0
        return self._sim_sell(t, p.tokens_left)

    def equity_now(self) -> float:
        # pending buys haven't been debited yet, so balance already includes them
        return self.balance + sum(self._position_value(p) for p in self.positions.values())

    def _mark_equity(self, ts, force=False):
        if force or ts - self._last_eq_ts >= 5:
            self._last_eq_ts = ts
            self.equity.append((ts, round(self.equity_now(), 6)))
            if len(self.equity) > 4000:
                self.equity = self.equity[::2]

    @staticmethod
    def _spark(hist, since=None, n=48):
        pts = [m for t, m in hist if since is None or t >= since - 1]
        if len(pts) > n:
            step = len(pts) / n
            pts = [pts[int(i * step)] for i in range(n)] + [pts[-1]]
        return [round(x, 2) for x in pts]

    def snapshot(self) -> dict:
        ts = self.now
        start = self.cfg["risk"]["starting_balance_sol"]
        realised = sum(c["pnl_sol"] for c in self.closed)
        wins = sum(1 for c in self.closed if c["pnl_sol"] > 0)
        x = self.cfg["exit"]

        positions = []
        for p in self.positions.values():
            t = self.tokens[p.mint]
            val = self._position_value(p)
            gain = (t.price / p.entry_price - 1) * 100 if t.price else 0.0
            positions.append(dict(
                mint=p.mint, symbol=p.symbol, name=t.name, held_s=round(ts - p.open_ts), entry_path=p.entry_path,
                mode=p.mode, copy_wallet=p.copy_wallet, wallet_entry_mcap=round(p.wallet_entry_mcap, 1),
                wallet_left_pct=self._wallet_left_pct(p), entry_gap_pct=self._entry_gap(p),
                sol_in=round(p.sol_in, 4), sol_out=round(p.sol_out, 4), value=round(val, 4),
                upnl=round(p.sol_out + val - p.sol_in, 4), gain_pct=round(gain, 1),
                peak_gain_pct=round((p.peak_price / p.entry_price - 1) * 100, 1),
                entry_mcap=round(p.entry_mcap, 1), mcap=round(t.mcap or 0, 1),
                left_pct=round(p.tokens_left / p.tokens_initial * 100),
                tp=[dict(gain=l["gain_pct"], sell=l["sell_pct"], done=i in p.tp_done)
                    for i, l in enumerate(x["take_profit"])],
                stop=-x["stop_loss_pct"], max_hold=x["max_hold_s"],
                exiting=p.pending_sell.reason if p.pending_sell else None,
                spark=self._spark(t.mcap_hist, since=p.open_ts - 60),
                dev_sold=t.dev_sold,
            ))

        watching = []
        for t in self.tokens.values():
            if t.status != "watching":
                continue
            checks = self.entry_checks(t, ts) if t.mcap else []
            core = [c for c in checks if c["group"] == "core"]
            paths = {g: [c for c in checks if c["group"] == g] for g in ("momentum", "surge")}
            paths = {g: v for g, v in paths.items() if v}
            best = max(paths, key=lambda g: sum(c["ok"] for c in paths[g]) / len(paths[g])) if paths else None
            fired = self.entry_status(t, ts, checks)[1] if checks else None
            watching.append(dict(
                mint=t.mint, symbol=t.symbol, name=t.name, age_s=round(ts - t.created_ts),
                dev_pct=round(t.dev_buy_pct, 1) if t.dev_buy_pct is not None else None,
                security=("RugCheck: checking…" if t.sec_state == "pending" else (t.security_note or "passed")),
                sec_state=t.sec_state,
                mcap=round(t.mcap or 0, 1), buyers=len(t.buyers), buys=t.buys, sells=t.sells,
                buy_vol=round(t.buy_vol, 2), sell_vol=round(t.sell_vol, 2),
                passed=sum(c["ok"] for c in core) + (sum(c["ok"] for c in paths[best]) if best else 0),
                total=len(core) + (len(paths[best]) if best else 0),
                ready=fired is not None and not any(not c["ok"] for c in core), fired=fired,
                checks=[{k: c[k] for k in ("group", "key", "label", "val", "ok")} | {"need": c.get("need", "")}
                        for c in checks],
                pending=t.mint in self.pending_buys,
                spark=self._spark(t.mcap_hist, n=32),
            ))
        watching.sort(key=lambda w: (w["pending"], w["passed"], w["buy_vol"]), reverse=True)

        eq = self.equity[-600:]
        screening = sum(1 for t in self.tokens.values() if t.sec_state == "pending" and t.status == "watching")
        return dict(
            now=ts,
            stats=dict(
                balance=round(self.balance, 4), start=start, equity=round(self.equity_now(), 4),
                realised=round(realised, 4), day_pnl=round(self.day_pnl, 4),
                closed=len(self.closed), wins=wins, losses=len(self.closed) - wins,
                win_rate=round(wins / len(self.closed) * 100, 1) if self.closed else None,
                open=sum(1 for p in self.positions.values() if p.mode == "strategy")
                + sum(1 for o in self.pending_buys.values() if not o.reason.startswith("copy")),
                max_open=self.cfg["risk"]["max_open_positions"],
                tokens_seen=self.security_passed + self.security_failed + screening,
                security_passed=self.security_passed, security_failed=self.security_failed,
                screening=screening, blocked_creators=len(self.bad_creators),
                rugcheck=bool(self.cfg["security"].get("rugcheck") and self.screener is not None),
                watching=len(watching), signals=self.outcomes["BUY SIGNAL"],
                msgs_today=self.day_trade_msgs, msg_budget=self.cfg["max_trade_messages_per_day"],
                data_cost=round(self.total_trade_msgs / 1e6, 5),
                loss_limit=self.cfg["risk"]["daily_loss_limit_sol"],
                loss_limit_hit=self.day_pnl <= -self.cfg["risk"]["daily_loss_limit_sol"],
            ),
            equity=[[round(a), b] for a, b in eq],
            positions=positions,
            watching=watching[:30],
            activity=list(self.activity)[-120:][::-1],
            closed=self.closed[-60:][::-1],
            rejects=[r for r in self.reject_reasons.most_common() if not r[0].startswith("security: ")][:6],
            copy=dict(wallets=sorted(self.copy_wallets()),
                      backup=(self.chain.stats | {"last_error": self.chain.last_error, "fast_on": self.chain.ws_connected})
                      if self.chain else None,
                      snaps=self.snaps.summary() if self.snaps is not None else None,
                      **{k: v for k, v in self.copy_stats.items() if isinstance(k, str)},
                      open=sum(1 for p in self.positions.values() if p.mode == "copy"),
                      closed=sum(1 for c in self.closed if str(c.get("entry_path", "")).startswith("copy")),
                      pnl=round(sum(c["pnl_sol"] for c in self.closed if str(c.get("entry_path", "")).startswith("copy")), 4),
                      per_wallet=self._per_wallet()),
            security_rejects=[(r[10:], c) for r, c in self.reject_reasons.most_common()
                              if r.startswith("security: ")][:6],
        )

    def summary(self) -> str:
        n = len(self.closed)
        wins = [c for c in self.closed if c["pnl_sol"] > 0]
        pnl = sum(c["pnl_sol"] for c in self.closed)
        start = self.cfg["risk"]["starting_balance_sol"]
        lines = [
            "─" * 56,
            f"Tokens seen: {self.security_passed + self.security_failed}   buy signals: {self.outcomes['BUY SIGNAL']}   "
            f"closed trades: {n}   open: {len(self.positions)}",
            f"Win rate: {len(wins) / n * 100:.0f}%" if n else "Win rate: n/a",
            f"Realised PnL: {pnl:+.4f} SOL   balance: {self.balance:.4f} SOL (start {start})",
            f"Security gate: {self.security_passed} passed, {self.security_failed} failed "
            f"({len(self.bad_creators)} creators blocklisted)",
            f"Trade messages received: {self.total_trade_msgs:,}  (~{self.total_trade_msgs / 1e6:.4f} SOL data cost)",
            "Top rejection reasons:",
        ]
        for r, c in self.reject_reasons.most_common(8):
            lines.append(f"  {c:>6}  {r}")
        lines.append("─" * 56)
        return "\n".join(lines)
