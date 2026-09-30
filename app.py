"""
Momentum - dashboard app for the memecoin paper-trading bot.

    python app.py              # opens http://localhost:8765 in your browser
    python app.py --port 9000 --no-browser

Paper trading only: there is no wallet, no private key, and no code path that
places a real order.
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
import webbrowser

from aiohttp import WSMsgType, web

from memebot import settings as S
from memebot.demo import generate
from memebot.engine import Engine, Journal
from memebot.live import LiveFeed, stream, tick_loop
from memebot import wallet as WL
from memebot.chain import ChainBackup
from memebot.live_trader import LiveTrader
from memebot.snapshots import SnapshotRecorder
from memebot.names import TokenNames
from memebot.lookalike import Lookalike
from memebot.reclaim import Reclaim
from memebot.lookalike_grad import LookalikeGrad, LookalikeGrad3, LookalikeGradOld
from memebot.survivor import Survivor, HotWord
from memebot.narratives import Narratives
from memebot.whylog import WhyLog
from memebot.updater import Updater
from memebot.prices import SolPrice
from memebot.security import DemoScreener, RugCheckScreener

log = logging.getLogger("memebot")
HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(HERE, "config.yaml")


class Runner:
    def __init__(self):
        self.cfg = S.load_config(CONFIG)
        self.engine: Engine | None = None
        self.mode: str | None = None       # "live" | "demo"
        self.running = False
        self.connected = False
        self.status_msg = "Stopped"
        self.started_at = None
        self.speed = 1
        self.tasks: list[asyncio.Task] = []
        self.journal = None
        self.recorder = None
        self.screener = None
        self.chain = None
        self.snaps = None
        self.copy_log = None
        self.live = LiveTrader(os.path.join(HERE, "data"),
                               lambda: self.engine.cfg if (self.engine and self.running) else self.cfg,
                               lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        self.lookalike = Lookalike(os.path.join(HERE, "data"),
                                   lambda: self.engine.cfg if (self.engine and self.running) else self.cfg,
                                   lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        self.lookalike.live = self.live
        self.lookalike_grad = LookalikeGrad(os.path.join(HERE, "data"),
                                            lambda: self.engine.cfg if (self.engine and self.running) else self.cfg,
                                            lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        # a SEPARATE real-money wallet just for the lookalike (graduation exit): own key, balance, positions, history
        cur = lambda: self.engine.cfg if (self.engine and self.running) else self.cfg
        self.live2 = LiveTrader(os.path.join(HERE, "data", "lookalike_wallet"),
                                lambda: {"live": dict(cur().get("live") or {},
                                                      enabled=bool((cur().get("lookalike_grad") or {}).get("real_enabled"))),
                                         "copy_trade": {}},
                                lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        self.lookalike_grad.live = self.live2
        self.lookalike_fresh = LookalikeGradOld(os.path.join(HERE, "data"), cur, lambda: S.helius_key(CONFIG),
                                                lambda: sol_price.usd)    # paper: old entry rules, for comparison
        self.tests = [self.lookalike_fresh]              # (the 3-minute test was removed on 30 Sep: it lost)
        self.survivor = Survivor(os.path.join(HERE, "data"), cur, lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        self.survivor.live = self.live2                     # the real lookalike wallet now trades this strategy
        self.hotword = HotWord(os.path.join(HERE, "data"), cur, lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        self.traders = {"copy": self.live, "lookalike": self.live2}
        self.reclaim = Reclaim(os.path.join(HERE, "data"),
                               lambda: self.engine.cfg if (self.engine and self.running) else self.cfg,
                               lambda: S.helius_key(CONFIG), lambda: sol_price.usd)
        self.narr = Narratives(os.path.join(HERE, "data", "narratives.json"), lambda: S.helius_key(CONFIG))
        self.hotword.narr = self.narr
        self.names = TokenNames(os.path.join(HERE, "data", "token_names.json"), lambda: S.helius_key(CONFIG))
        self.why = WhyLog(os.path.join(HERE, "data", "coin_decisions.jsonl"), lambda: S.helius_key(CONFIG))
        for strat in (self.lookalike, self.lookalike_grad, *self.tests, self.reclaim, self.survivor, self.hotword):
            strat.why = self.why
        self.updater = Updater(HERE, os.path.join(HERE, "data"),
                               lambda: self.engine.cfg if (self.engine and self.running) else self.cfg)

    def name_targets(self):
        from memebot.names import is_placeholder
        out = self.live.name_targets() + self.live2.name_targets()
        out += [m for m, p in self.lookalike.positions.items() if is_placeholder(p["symbol"], m)]
        out += [m for m, p in self.reclaim.positions.items() if is_placeholder(p["symbol"], m)]
        out += [m for m, p in self.lookalike_grad.positions.items() if is_placeholder(p["symbol"], m)]
        for tst in (*self.tests, self.survivor, self.hotword):
            out += [m for m, p in tst.positions.items() if is_placeholder(p["symbol"], m)]
        if self.engine is not None and self.mode == "live":
            out += self.engine.name_targets()
        return list(dict.fromkeys(out))

    def apply_name(self, mint, info):
        self.live.rename(mint, info["symbol"], info.get("name", ""))
        self.live2.rename(mint, info["symbol"], info.get("name", ""))
        self.lookalike.rename(mint, info["symbol"])
        self.reclaim.rename(mint, info["symbol"])
        self.lookalike_grad.rename(mint, info["symbol"])
        for tst in (*self.tests, self.survivor, self.hotword):
            tst.rename(mint, info["symbol"])
        if self.engine is not None and self.mode == "live":
            self.engine.rename(mint, info["symbol"], info.get("name", ""))

    # ------------------------------------------------------------------ helpers
    def api_key(self):
        return os.environ.get("PUMPPORTAL_API_KEY") or self.cfg["feed"].get("api_key") or ""

    def _status(self, connected, msg):
        self.connected = connected
        self.status_msg = msg

    def meta(self):
        key = self.api_key()
        return dict(sol_usd=sol_price.usd, sol_usd_source=sol_price.source,
                    mode=self.mode, running=self.running, connected=self.connected,
                    status=self.status_msg, started_at=self.started_at, speed=self.speed,
                    has_key=bool(key), key_from_env=bool(os.environ.get("PUMPPORTAL_API_KEY")),
                    key_hint=(key[:4] + "…" + key[-4:]) if len(key) > 10 else ("set" if key else ""),
                    universe=self.cfg["universe"],
                    rugcheck_on_error=self.cfg["security"].get("rugcheck_on_error", "skip"),
                    rugcheck_errors=getattr(self.screener, "stats", {}).get("error", 0),
                    rugcheck_checks=sum(getattr(self.screener, "stats", {}).values()),
                    rugcheck_last_error=getattr(self.screener, "last_error", ""),
                    trade_feed_silent_s=self.trade_feed_silent(), update=self.updater.state(), boot=BOOT_ID)

    def busy_trades(self):
        """Real buys/sells in flight right now (don't restart for an update in the middle of one)."""
        return sum(1 for tr in (self.live, self.live2) for p in tr.positions.values()
                   if p.get("status") in ("buying", "selling", "waiting"))

    async def shutdown_for_update(self):
        await self.stop()
        self.narr.save(force=True)
        for strat in (self.lookalike, self.lookalike_grad, *self.tests, self.reclaim, self.survivor, self.hotword):
            try:
                strat._save()
            except Exception:
                pass

    def trade_feed_silent(self):
        """Seconds the live feed has been connected and watching coins without sending one trade (None = fine)."""
        eng = self.engine
        if self.mode != "live" or not self.running or not self.connected or eng is None:
            return None
        watching = sum(1 for t in eng.tokens.values() if t.status == "watching")
        since = max(eng.last_feed_trade_ts, self.started_at or 0)
        silent = time.time() - since
        return round(silent) if watching >= 5 and silent > 300 else None

    # ------------------------------------------------------------------ lifecycle
    async def start(self, mode: str, speed: int = 3):
        await self.stop()
        self.cfg = S.load_config(CONFIG)
        self.mode, self.speed = mode, (speed if mode == "demo" else 1)
        tag = time.strftime("%Y%m%d_%H%M%S")
        out = self.cfg["output"]["dir"]
        self.journal = Journal(os.path.join(HERE, out, "demo") if mode == "demo" else os.path.join(HERE, out), tag)
        self.started_at = time.time()
        self.running = True

        if mode == "live":
            key = self.api_key()
            feed = LiveFeed(bool(key))
            self.screener = RugCheckScreener(lambda: self.engine, lambda: self.engine.cfg["security"])
            self.engine = Engine(self.cfg, feed, self.journal, screener=self.screener,
                                 blocklist_path=os.path.join(HERE, out, "creator_blocklist.txt"))
            feed.accounts = self.engine.copy_wallets()
            if feed.accounts:
                self.copy_log = open(os.path.join(HERE, out, f"copy_log_{tag}.jsonl"), "a", encoding="utf-8", errors="replace")
                self.engine.copy_log = self.copy_log
                hk = S.helius_key(CONFIG)
                if hk:
                    self.chain = ChainBackup(lambda: self.engine, hk)
                    self.engine.chain = self.chain
                    self.snaps = SnapshotRecorder(os.path.join(HERE, out, "snapshots.jsonl"), hk,
                                                  lambda: self.engine.cfg, lambda: sol_price.usd)
                    self.engine.snaps = self.snaps
            if self.cfg["output"]["record_raw_events"]:
                self.recorder = open(os.path.join(HERE, out, f"events_{tag}.jsonl"), "a", encoding="utf-8", errors="replace")
            url = self.cfg["feed"]["url"] + (f"?api-key={key}" if key else "")
            self._status(False, "Connecting to PumpPortal…")
            self.tasks = [asyncio.create_task(stream(self.engine, feed, url, self.recorder, self._status)),
                          asyncio.create_task(tick_loop(self.engine))]
            if self.chain:
                self.tasks.append(asyncio.create_task(self.chain.run()))
                self.tasks.append(asyncio.create_task(self.chain.listen()))
            if self.snaps:
                self.tasks.append(asyncio.create_task(self.snaps.run()))
            # real-money copies: only ever active inside a live session
            eng = self.engine
            self.live.price_of = lambda m: (eng.tokens[m].price if m in eng.tokens else None)
            self.live.active = True
            eng.live = self.live
            self.lookalike.active = True
            eng.lookalike = self.lookalike

            def feed_price(m):
                t = eng.tokens.get(m)
                if t and t.price and t.status == "watching":
                    return t.price, time.time() - t.last_trade_ts
                return None
            self.lookalike.feed_price = feed_price
            self.reclaim.active = True
            self.reclaim.feed_price = feed_price
            self.lookalike_grad.active = True
            self.lookalike_grad.feed_price = feed_price
            eng.lookalike_grad = self.lookalike_grad
            for tst in self.tests:
                tst.active = True
                tst.feed_price = feed_price
            eng.tests = self.tests
            self.survivor.active = True
            self.survivor.feed_price = feed_price
            eng.survivor = self.survivor
            self.hotword.active = True
            self.hotword.feed_price = feed_price
            eng.hotword = self.hotword
            try:                                             # also follow coins it saw in the last 3 days that got bought up
                seeded = self.survivor.seed([(m, v.get("symbol"), v.get("name"), v["ts"])
                                             for m, v in list(self.narr.mints.items())
                                             if "hit44" in (v.get("hits") or {}) and not v.get("excluded")])
                if seeded:
                    log.info("SURVIVOR following %d older coins from the last 3 days", seeded)
            except Exception as e:
                log.warning("survivor seed failed: %s", e)
            eng.reclaim = self.reclaim
            eng.narr = self.narr
            eng.why = self.why
            for m in list(self.live.positions):
                feed.subscribe(m)                        # keep pricing positions carried over from before
            self.tasks.append(asyncio.create_task(self.live.recheck()))
            self.tasks.append(asyncio.create_task(self.live.sweep_orphans()))
            self.tasks.append(asyncio.create_task(self.live.sweep_loop()))
            self.tasks.append(asyncio.create_task(self.live.refresh_balance(force=True)))
            self.live2.price_of = self.live.price_of
            self.live2.active = True
            for m in list(self.live2.positions):
                feed.subscribe(m)
            for job in (self.live2.recheck(), self.live2.sweep_orphans(), self.live2.sweep_loop(),
                        self.live2.refresh_balance(force=True)):
                self.tasks.append(asyncio.create_task(job))
            if self.live2.kp and (self.cfg.get("lookalike_grad") or {}).get("real_enabled"):
                self.live2._event("info", "Real-money lookalike (graduation exit) is ON for this session")
            if self.live.enabled():
                self.live._event("info", "Real-money copy trading is ON for this session")
        else:
            self.engine = Engine(self.cfg, None, self.journal)
            self.screener = DemoScreener(self.engine)
            self.engine.screener = self.screener
            self._status(True, f"Demo feed · {self.speed}× speed")
            self.tasks = [asyncio.create_task(self._demo_loop())]
        log.info("started %s mode", mode)

    async def stop(self):
        self.live.active = False
        self.live2.active = False
        self.lookalike.active = False
        self.reclaim.active = False
        self.lookalike_grad.active = False
        for tst in (*self.tests, self.survivor, self.hotword):
            tst.active = False
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self.tasks = []
        if isinstance(self.screener, RugCheckScreener):
            await self.screener.close()
        if self.chain:
            await self.chain.close()
            self.chain = None
        if self.snaps:
            await self.snaps.close()
            self.snaps = None
        if self.copy_log:
            self.copy_log.close()
            self.copy_log = None
        if self.engine and self.running:
            self.engine.close_all(self.engine.now or time.time(), "stopped")
            self.engine.on_tick(self.engine.now or time.time())
        if self.journal:
            self.journal.close()
            self.journal = None
        if self.recorder:
            self.recorder.close()
            self.recorder = None
        if self.running:
            log.info("stopped")
        self.running = False
        self.connected = False
        self.status_msg = "Stopped"

    async def _demo_loop(self):
        """Plays a synthetic feed on a virtual clock that runs `speed`× real time."""
        eng = self.engine
        import random as _r
        rng = _r.Random()
        wallets = sorted(eng.copy_wallets())
        held = {}   # mint -> (entry mcap, tokens, sold half?)

        def fake_wallet(ev):
            """Demo only: pretend the copied wallet buys ~40-48 SOL mcap, stops at -30%, sells into 3x/5x."""
            if not wallets or ev.get("txType") not in ("buy", "sell"):
                return []
            mint, mc = ev["mint"], ev.get("marketCapSol") or 0
            w = wallets[0]
            out = []
            base = dict(mint=mint, traderPublicKey=w, marketCapSol=mc,
                        vSolInBondingCurve=ev.get("vSolInBondingCurve"), vTokensInBondingCurve=ev.get("vTokensInBondingCurve"))
            if mint not in held and 40 <= mc <= 48 and rng.random() < 0.25:
                tok = 1.5 / (mc / 1e9)
                held[mint] = [mc, tok, False]
                out.append(base | dict(txType="buy", solAmount=1.5, tokenAmount=tok, signature=f"demo-{rng.random()}"))
            elif mint in held and held[mint][1] > 0:
                e0, tok, half = held[mint]
                if mc <= e0 * 0.7 or (half and mc >= e0 * 5):
                    out.append(base | dict(txType="sell", solAmount=tok * mc / 1e9, tokenAmount=tok, signature=f"demo-{rng.random()}"))
                    held[mint][1] = 0
                elif not half and mc >= e0 * 3:
                    out.append(base | dict(txType="sell", solAmount=tok / 2 * mc / 1e9, tokenAmount=tok / 2, signature=f"demo-{rng.random()}"))
                    held[mint] = [e0, tok / 2, True]
            return out
        vclock = time.time()
        events = generate(vclock + 2, n_tokens=80, seed=None)
        i = 0
        last_tick = vclock
        step = 0.2
        while True:
            await asyncio.sleep(step)
            vclock += step * self.speed
            self.screener.poll(vclock)
            if len(events) - i < 400:           # top up the synthetic stream
                events = events[i:] + generate(events[-1]["_ts"] + 5, n_tokens=60, seed=None)
                events.sort(key=lambda e: e["_ts"])
                i = 0
            while i < len(events) and events[i]["_ts"] <= vclock:
                ev = events[i]
                i += 1
                for extra in fake_wallet(ev):
                    eng.on_event(extra, ev["_ts"])
                while ev["_ts"] - last_tick >= 1.0:
                    last_tick += 1.0
                    eng.on_tick(last_tick)
                eng.on_event(ev, ev["_ts"])
            while vclock - last_tick >= 1.0:
                last_tick += 1.0
                eng.on_tick(last_tick)

    def survivor_state(self):
        sv = self.survivor
        s = sv.state()
        hw = self.hotword
        since = max(sv.since, hw.since)                       # same period for all three
        s["vs"] = dict(since=since, survivor=sv.stats_since(since), lookalike=self.lookalike_grad.stats_since(since),
                       hotword=hw.stats_since(since))
        s["hotword"] = dict(candidates=len(hw.cands), coins=self._age_test_coins(hw),
                            min_age_min=hw.cfg().get("min_age_min", 15))
        s["coins"] = self._age_test_coins(sv)
        return s

    def age_test(self):
        f = self._fresh_test()
        return dict(fresh=f) if f else None

    def _fresh_test(self):
        f = self.lookalike_fresh
        if not f.enabled():
            return None
        c = self.lookalike_grad.cfg()
        return dict(since=f.since, cap=c.get("max_prior_peak_mult", 0), rise=c.get("min_rise_2m_pct", 0),
                    buys=c.get("min_buys_2m", 0),
                    base=self.lookalike_grad.stats_since(f.since), fresh=f.stats_since(f.since),
                    coins=self._age_test_coins(f))

    @staticmethod
    def _age_test_coins(g3):
        """The 3-minute test's coins (newest first): open ones with their current multiple, closed ones with P/L."""
        usd = sol_price.usd or 0
        out = []
        for p in g3.positions.values():
            if p["opened"] >= g3.since and p.get("verified", True):
                val = g3._sell_value(p["tokens"], p["last_px"])
                pnl = val + p["sol_out"] - p["sol_in"]
                out.append(dict(mint=p["mint"], symbol=p["symbol"], opened=p["opened"], age_s=p.get("age_at_entry_s"),
                                entry_mcap=p["entry_mcap"], open=True, mult=round(p["last_px"] / p["entry_px"], 2),
                                pnl_usd=round(pnl * usd, 2), note=",".join(p["done"]), prior_peak=p.get("prior_peak"),
                                hot=p.get("hot")))
        for x in g3.closed:
            if x["opened"] >= g3.since:
                out.append(dict(mint=x["mint"], symbol=x["symbol"], opened=x["opened"], age_s=x.get("age_at_entry_s"),
                                entry_mcap=x["entry_mcap"], open=False, pnl_usd=x["pnl_usd"], pct=x["pnl_pct"],
                                note=x.get("exit", ""), prior_peak=x.get("prior_peak")))
        return sorted(out, key=lambda c: -c["opened"])[:40]

    def snapshot(self):
        snap = self.engine.snapshot() if self.engine else None
        cw = self.engine.copy_wallets() if self.engine else set()
        return {"type": "state", "meta": self.meta(), "data": snap, "live": self.live.state(cw),
                "live2": self.live2.state([]),
                "lookalike": self.lookalike.state(), "reclaim": self.reclaim.state(),
                "lookalike_grad": self.lookalike_grad.state(),
                "age_test": self.age_test(),
                "survivor": self.survivor_state(),
                "narratives": self.narr.state()}


# ---------------------------------------------------------------------- routes
BOOT_ID = str(time.time())                      # changes on every restart -> open dashboards reload themselves
runner = Runner()
sol_price = SolPrice()
clients: set[web.WebSocketResponse] = set()


async def index(request):
    return web.FileResponse(os.path.join(HERE, "web", "index.html"))


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    clients.add(ws)
    try:
        await ws.send_str(json.dumps(runner.snapshot()))
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        clients.discard(ws)
    return ws


async def broadcaster(app):
    async def loop():
        while True:
            await asyncio.sleep(1.0)
            runner.narr.save()                           # throttled internally (every ~2 min)
            if not clients:
                continue
            payload = json.dumps(runner.snapshot())
            for ws in list(clients):
                try:
                    await ws.send_str(payload)
                except Exception:
                    clients.discard(ws)
    task = asyncio.create_task(loop())
    price_task = asyncio.create_task(sol_price.run())
    names_task = asyncio.create_task(runner.names.run(runner.name_targets, runner.apply_name))
    live_px_task = asyncio.create_task(runner.live.price_loop())
    live2_px_task = asyncio.create_task(runner.live2.price_loop())
    lk_task = asyncio.create_task(runner.lookalike.run())
    rc_task = asyncio.create_task(runner.reclaim.run(every=10))
    lg_every = float((runner.cfg.get("lookalike_grad") or {}).get("check_every_s", 3))
    lg_task = asyncio.create_task(runner.lookalike_grad.run(every=lg_every))
    test_tasks = [asyncio.create_task(tst.run(every=lg_every)) for tst in runner.tests]
    test_tasks.append(asyncio.create_task(runner.survivor.run(every=5)))
    test_tasks.append(asyncio.create_task(runner.hotword.run(every=5)))
    nr_task = asyncio.create_task(runner.narr.run())
    why_task = asyncio.create_task(runner.why.run())
    upd_task = asyncio.create_task(runner.updater.run(runner))
    yield
    task.cancel()
    price_task.cancel()
    names_task.cancel()
    runner.narr.save(force=True)
    live_px_task.cancel()
    live2_px_task.cancel()
    await runner.live2.close()
    lk_task.cancel()
    rc_task.cancel()
    lg_task.cancel()
    for tt in test_tasks:
        tt.cancel()
    await runner.lookalike_grad.close()
    for tst in (*runner.tests, runner.survivor, runner.hotword):
        await tst.close()
    nr_task.cancel()
    why_task.cancel()
    upd_task.cancel()
    await runner.why.close()
    await runner.narr.close()
    await runner.lookalike.close()
    await runner.reclaim.close()
    await runner.names.close()
    await runner.live.close()
    await runner.stop()


async def api_start(request):
    body = await request.json()
    mode = body.get("mode", "demo")
    if mode not in ("live", "demo"):
        return web.json_response({"error": "bad mode"}, status=400)
    await runner.start(mode, int(body.get("speed", 3)))
    return web.json_response(runner.meta())


async def api_stop(request):
    await runner.stop()
    return web.json_response(runner.meta())


async def api_speed(request):
    body = await request.json()
    runner.speed = max(1, min(20, int(body.get("speed", 1))))
    if runner.mode == "demo" and runner.running:
        runner.status_msg = f"Demo feed · {runner.speed}× speed"
    return web.json_response(runner.meta())


async def api_get_settings(request):
    cfg = runner.engine.cfg if (runner.engine and runner.running) else S.load_config(CONFIG)
    return web.json_response({"schema": S.schema_with_values(cfg), "meta": runner.meta()})


async def api_save_settings(request):
    body = await request.json()
    if body.get("reset"):
        S.reset_overrides(CONFIG)
        runner.cfg = S.load_config(CONFIG)
        if runner.engine and runner.running:
            fresh = S.load_config(CONFIG)
            for g in S.SCHEMA:
                for it in g["items"]:
                    if not it.get("restart"):
                        S.set_path(runner.engine.cfg, it["path"], S.get_path(fresh, it["path"]))
        return web.json_response({"ok": True})
    items = {it["path"]: it for g in S.SCHEMA for it in g["items"]}
    values, errors = {}, []
    for path, raw in (body.get("values") or {}).items():
        it = items.get(path)
        if not it:
            continue
        try:
            v = S.coerce(it, raw)
            if it["type"] in ("int", "float") and v < 0:
                raise ValueError
            if path == "copy_trade.wallet":
                v = ",".join(w.strip() for w in v.split(",") if w.strip())
                if any(not WL.valid_address(w) for w in v.split(",") if w):
                    raise ValueError
            values[path] = v
        except (TypeError, ValueError):
            errors.append(it["label"])
    if errors:
        return web.json_response({"error": "Check these values: " + ", ".join(errors)}, status=400)
    key = body.get("api_key")
    hkey = body.get("helius_key")
    S.save_overrides(values, api_key=key if key is not None else None, config_path=CONFIG,
                     helius_key=hkey.strip() if isinstance(hkey, str) else None)
    runner.cfg = S.load_config(CONFIG)
    restart = False
    if runner.engine and runner.running:       # apply live where it's safe to
        for path, v in values.items():
            if items[path].get("restart"):
                restart = restart or S.get_path(runner.engine.cfg, path) != v
            else:
                S.set_path(runner.engine.cfg, path, v)
    return web.json_response({"ok": True, "restart_needed": restart, "meta": runner.meta()})


# ---------------------------------------------------------------------- wallet lab
WALLET_DIR = os.path.join(HERE, "data", "wallets")
lab = dict(running=False, address=None, phase="", done=0, total=0, message="", error="", task=None)


def _lab_state():
    return {k: v for k, v in lab.items() if k != "task"}


def _load_report(addr):
    try:
        with open(os.path.join(WALLET_DIR, addr, "summary.json"), encoding="utf-8", errors="replace") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


async def api_wallet_analyze(request):
    body = await request.json()
    addr = str(body.get("address", "")).strip()
    days = max(1, min(180, int(body.get("days", 30))))
    if not WL.valid_address(addr):
        return web.json_response({"error": "That doesn't look like a Solana wallet address"}, status=400)
    key = S.helius_key(CONFIG)
    if not key:
        return web.json_response({"error": "Add a free Helius API key first (Settings → Helius)"}, status=400)
    if lab["running"]:
        return web.json_response({"error": "An analysis is already running"}, status=409)

    def prog(phase, done, total, message):
        lab.update(phase=phase, done=done, total=total, message=message)

    async def run():
        try:
            await WL.analyze(addr, key, WALLET_DIR, days=days, progress_cb=prog)
            lab.update(phase="done", message="Done")
        except Exception as e:
            log.warning("wallet analysis failed: %s", e)
            lab.update(error=str(e) or type(e).__name__, phase="error")
        finally:
            lab["running"] = False

    lab.update(running=True, address=addr, phase="fetch", done=0, total=0, message="Starting…", error="")
    lab["task"] = asyncio.create_task(run())
    return web.json_response(_lab_state())


async def api_wallet_status(request):
    addr = request.query.get("address") or lab["address"]
    report = _load_report(addr) if addr and WL.valid_address(addr) and not lab["running"] else None
    return web.json_response({"lab": _lab_state(), "report": report, "has_key": bool(S.helius_key(CONFIG)),
                              "key_from_env": bool(os.environ.get("HELIUS_API_KEY"))})


async def api_wallet_list(request):
    out = []
    if os.path.isdir(WALLET_DIR):
        for a in os.listdir(WALLET_DIR):
            r = _load_report(a)
            if r:
                out.append(dict(address=a, generated=r.get("generated_utc"), trades=r["counts"]["trips"],
                                pnl=r["results"]["total_pnl_sol"], win_rate=r["results"]["win_rate_pct"]))
    out.sort(key=lambda x: x["generated"] or "", reverse=True)
    return web.json_response(out)


async def api_wallet_download(request):
    addr = request.query.get("address", "")
    if not WL.valid_address(addr):
        raise web.HTTPBadRequest()
    folder = os.path.join(WALLET_DIR, addr)
    zips = [f for f in os.listdir(folder) if f.endswith(".zip")] if os.path.isdir(folder) else []
    if not zips:
        raise web.HTTPNotFound()
    return web.FileResponse(os.path.join(folder, zips[0]),
                            headers={"Content-Disposition": f'attachment; filename="{zips[0]}"'})


# ---------------------------------------------------------------------- real money
def _trader(body):
    return runner.traders.get(str((body or {}).get("which") or "copy"), runner.live)


async def _body(request):
    try:
        return await request.json()
    except Exception:
        return {}


async def api_live_create(request):
    tr = _trader(await _body(request))
    try:
        addr = tr.create_wallet()
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response({"address": addr})


async def api_live_refresh(request):
    tr = _trader(await _body(request))
    await tr.refresh_balance(force=True)
    await tr.sync_with_wallet()
    return web.json_response({"balance": tr.balance})


async def api_live_pause(request):
    body = await _body(request)
    tr = _trader(body)
    tr.paused = bool(body.get("paused"))
    tr._save()
    what = "Real-money lookalike" if tr is runner.live2 else "Real-money copying"
    tr._event("info", f"{what} PAUSED (no new buys; sells still happen)" if tr.paused else f"{what} resumed")
    return web.json_response({"paused": tr.paused})


async def api_live_sell(request):
    body = await _body(request)
    tr = _trader(body)
    if not tr.kp:
        return web.json_response({"error": "No trading wallet"}, status=400)
    if body.get("all"):
        asyncio.create_task(tr.sell_all())
    elif body.get("mint") in tr.positions:
        asyncio.create_task(tr.sell_now(body["mint"]))
    else:
        return web.json_response({"error": "No such live position"}, status=400)
    return web.json_response({"ok": True})


async def api_live_withdraw(request):
    body = await _body(request)
    tr = _trader(body)
    addr = str(body.get("address", "")).strip()
    if not WL.valid_address(addr):
        return web.json_response({"error": "That doesn't look like a Solana address"}, status=400)
    try:
        sig = await tr.withdraw(addr)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response({"signature": sig})


# ---------------------------------------------------------------------- diagnostics
SAFE_EXCLUDE = {"trading_wallet.json"}             # never leaves the computer


async def api_narrative_coins(request):
    q = request.rel_url.query
    key, metric = q.get("key") or None, q.get("metric") or None
    if metric and metric not in ("hit44", "hit80", "hit3x", "hit6x", "grad", "launch", "excluded"):
        return web.json_response({"error": "bad metric"}, status=400)
    return web.json_response({"coins": runner.narr.coins(key, metric, limit=int(q.get("limit", 200)))})


async def api_launch(request):
    """Bundle / sniper check for one coin (read-only, uses the Helius key)."""
    import re
    import aiohttp
    from memebot import launchcheck
    m = re.search(r"[1-9A-HJ-NP-Za-km-z]{32,44}", request.rel_url.query.get("q") or "")
    if not m:
        return web.json_response({"error": "paste a pump.fun link or coin address"}, status=400)
    key = S.helius_key(CONFIG)
    if not key:
        return web.json_response({"error": "needs your Helius key (Settings)"}, status=400)
    try:
        async with aiohttp.ClientSession() as s:
            r = await launchcheck.check(s, key, m.group(0))
    except Exception as e:
        r = {"error": f"check failed: {e}"[:150]}
    return web.json_response(r)


async def api_why(request):
    """Why didn't the bot buy this coin? Accepts a mint or a pump.fun link."""
    import re
    q = (request.rel_url.query.get("q") or "").strip()
    m = re.search(r"[1-9A-HJ-NP-Za-km-z]{32,44}", q)
    if not m:
        return web.json_response({"error": "paste a pump.fun link or coin address"}, status=400)
    mint = m.group(0)
    nm = (runner.narr.mints.get(mint) or {}) if hasattr(runner.narr, "mints") else {}
    return web.json_response({"mint": mint, "entries": runner.why.find(mint),
                              "narr": {k: nm.get(k) for k in ("name", "symbol", "ts", "hits", "peak", "excluded")} if nm else None})


async def api_diagnostics(request):
    """One zip with everything needed to review a session - never the trading wallet key or API keys."""
    import glob
    import io
    import zipfile
    data_dir = os.path.join(HERE, "data")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("snapshot.json", json.dumps(runner.snapshot(), default=str, indent=1))
        for name in ("live_trades.csv", "live_state.json", "app.log", "creator_blocklist.txt", "snapshots.jsonl",
                     "lookalike_state.json", "lookalike_trades.csv", "lookalike_fills.csv",
                     "reclaim_state.json", "reclaim_trades.csv", "reclaim_fills.csv", "reclaim_candidates.json",
                     "narratives.json", "lookalike_grad_state.json", "lookalike_grad_trades.csv",
                     "lookalike_grad_fills.csv", "coin_decisions.jsonl", "lookalike_grad_old_state.json", "survivor_state.json", "survivor_trades.csv",
                     "survivor_fills.csv", "hotword_state.json", "hotword_trades.csv", "hotword_fills.csv",
                     "lookalike_grad_old_trades.csv", "lookalike_grad_old_fills.csv"):
            p = os.path.join(data_dir, name)
            if os.path.exists(p):
                z.write(p, name)
        for name in ("live_trades.csv", "live_state.json"):     # the lookalike wallet (never its key file)
            p = os.path.join(data_dir, "lookalike_wallet", name)
            if os.path.exists(p):
                z.write(p, "lookalike_wallet_" + name)
        for pattern in ("copy_log_*.jsonl", "positions_*.csv", "fills_*.csv"):
            for p in sorted(glob.glob(os.path.join(data_dir, pattern)))[-3:]:
                if os.path.basename(p) not in SAFE_EXCLUDE and os.path.getsize(p) < 40_000_000:
                    z.write(p, os.path.basename(p))
        ov = S.load_overrides(CONFIG)
        for k in ("api_key", "helius_key"):
            if ov.get(k):
                ov[k] = "(set, hidden)"
        z.writestr("settings_redacted.json", json.dumps(ov, indent=1))
    name = f"momentum_diagnostics_{time.strftime('%Y%m%d_%H%M')}.zip"
    return web.Response(body=buf.getvalue(), content_type="application/zip",
                        headers={"Content-Disposition": f'attachment; filename="{name}"'})


def make_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/api/start", api_start)
    app.router.add_post("/api/stop", api_stop)
    app.router.add_post("/api/speed", api_speed)
    app.router.add_get("/api/settings", api_get_settings)
    app.router.add_get("/api/narratives/coins", api_narrative_coins)
    app.router.add_post("/api/settings", api_save_settings)
    app.router.add_post("/api/wallet/analyze", api_wallet_analyze)
    app.router.add_get("/api/wallet/status", api_wallet_status)
    app.router.add_get("/api/wallet/list", api_wallet_list)
    app.router.add_get("/api/wallet/download", api_wallet_download)
    app.router.add_post("/api/live/create_wallet", api_live_create)
    app.router.add_post("/api/live/refresh", api_live_refresh)
    app.router.add_post("/api/live/pause", api_live_pause)
    app.router.add_post("/api/live/sell", api_live_sell)
    app.router.add_post("/api/live/withdraw", api_live_withdraw)
    app.router.add_get("/api/diagnostics", api_diagnostics)
    app.router.add_get("/api/why", api_why)
    app.router.add_get("/api/launch", api_launch)
    app.router.add_static("/static", os.path.join(HERE, "web"))
    app.cleanup_ctx.append(broadcaster)
    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--demo", action="store_true", help="start the demo feed immediately")
    a = ap.parse_args()
    os.makedirs(os.path.join(HERE, "data"), exist_ok=True)
    from logging.handlers import RotatingFileHandler
    for stream in (sys.stdout, sys.stderr):              # Windows consoles can't print every coin name
        try:
            stream.reconfigure(errors="replace")
        except Exception:
            pass
    fh = RotatingFileHandler(os.path.join(HERE, "data", "app.log"), maxBytes=5_000_000, backupCount=2, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S",
                        handlers=[logging.StreamHandler(), fh])
    app = make_app()
    url = f"http://localhost:{a.port}"

    async def on_start(_app):
        print(f"\n  Momentum dashboard running at {url}\n  Press Ctrl+C to quit.\n")
        resume = runner.updater.take_resume()
        if resume in ("live", "demo"):
            log.info("UPDATE installed - resuming %s mode", resume)
            await runner.start(resume, 3)
        elif a.demo:
            await runner.start("demo", 3)
        if not a.no_browser:
            asyncio.get_running_loop().call_later(0.8, webbrowser.open, url)
    app.on_startup.append(on_start)
    web.run_app(app, host=a.host, port=a.port, print=None)


if __name__ == "__main__":
    main()
