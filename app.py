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
import time
import webbrowser

from aiohttp import WSMsgType, web

from memebot import settings as S
from memebot.demo import generate
from memebot.engine import Engine, Journal
from memebot.live import LiveFeed, stream, tick_loop
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
                    rugcheck_last_error=getattr(self.screener, "last_error", ""))

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
            if self.cfg["output"]["record_raw_events"]:
                self.recorder = open(os.path.join(HERE, out, f"events_{tag}.jsonl"), "a")
            url = self.cfg["feed"]["url"] + (f"?api-key={key}" if key else "")
            self._status(False, "Connecting to PumpPortal…")
            self.tasks = [asyncio.create_task(stream(self.engine, feed, url, self.recorder, self._status)),
                          asyncio.create_task(tick_loop(self.engine))]
        else:
            self.engine = Engine(self.cfg, None, self.journal)
            self.screener = DemoScreener(self.engine)
            self.engine.screener = self.screener
            self._status(True, f"Demo feed · {self.speed}× speed")
            self.tasks = [asyncio.create_task(self._demo_loop())]
        log.info("started %s mode", mode)

    async def stop(self):
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
                while ev["_ts"] - last_tick >= 1.0:
                    last_tick += 1.0
                    eng.on_tick(last_tick)
                eng.on_event(ev, ev["_ts"])
            while vclock - last_tick >= 1.0:
                last_tick += 1.0
                eng.on_tick(last_tick)

    def snapshot(self):
        snap = self.engine.snapshot() if self.engine else None
        return {"type": "state", "meta": self.meta(), "data": snap}


# ---------------------------------------------------------------------- routes
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
    yield
    task.cancel()
    price_task.cancel()
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
            values[path] = v
        except (TypeError, ValueError):
            errors.append(it["label"])
    if errors:
        return web.json_response({"error": "Check these values: " + ", ".join(errors)}, status=400)
    key = body.get("api_key")
    S.save_overrides(values, api_key=key if key is not None else None, config_path=CONFIG)
    runner.cfg = S.load_config(CONFIG)
    restart = False
    if runner.engine and runner.running:       # apply live where it's safe to
        for path, v in values.items():
            if items[path].get("restart"):
                restart = restart or S.get_path(runner.engine.cfg, path) != v
            else:
                S.set_path(runner.engine.cfg, path, v)
    return web.json_response({"ok": True, "restart_needed": restart, "meta": runner.meta()})


def make_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/api/start", api_start)
    app.router.add_post("/api/stop", api_stop)
    app.router.add_post("/api/speed", api_speed)
    app.router.add_get("/api/settings", api_get_settings)
    app.router.add_post("/api/settings", api_save_settings)
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    os.makedirs(os.path.join(HERE, "data"), exist_ok=True)
    app = make_app()
    url = f"http://localhost:{a.port}"

    async def on_start(_app):
        print(f"\n  Momentum dashboard running at {url}\n  Press Ctrl+C to quit.\n")
        if a.demo:
            await runner.start("demo", 3)
        if not a.no_browser:
            asyncio.get_running_loop().call_later(0.8, webbrowser.open, url)
    app.on_startup.append(on_start)
    web.run_app(app, host=a.host, port=a.port, print=None)


if __name__ == "__main__":
    main()
