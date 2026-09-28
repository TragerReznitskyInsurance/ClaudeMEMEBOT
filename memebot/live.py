"""
Live PumpPortal feed loop, shared by the dashboard app and run_paper.py.
Data only - nothing here can sign or send a transaction.
"""
import asyncio
import json
import logging
import time

import websockets

log = logging.getLogger("memebot")


class LiveFeed:
    """Queues subscribe/unsubscribe requests; the socket task sends them in batches."""

    def __init__(self, has_key: bool, accounts=()):
        self.q: asyncio.Queue = asyncio.Queue()
        self.active: set[str] = set()
        self.has_key = has_key
        self.accounts = set(accounts)          # wallets to follow (copy trading)

    def subscribe(self, mint):
        if self.has_key and mint not in self.active:
            self.active.add(mint)
            self.q.put_nowait(("subscribeTokenTrade", mint))

    def unsubscribe(self, mint):
        if mint in self.active:
            self.active.discard(mint)
            self.q.put_nowait(("unsubscribeTokenTrade", mint))


async def _sender(ws, feed: LiveFeed):
    while True:
        method, mint = await feed.q.get()
        batch = {method: [mint]}
        await asyncio.sleep(0.2)
        while not feed.q.empty():
            m, k = feed.q.get_nowait()
            batch.setdefault(m, []).append(k)
        for m, keys in batch.items():
            await ws.send(json.dumps({"method": m, "keys": keys}))


async def tick_loop(engine, every=1.0, on_tick=None):
    while True:
        await asyncio.sleep(every)
        engine.on_tick(time.time())
        if on_tick:
            on_tick()


async def stream(engine, feed: LiveFeed, url: str, recorder=None, on_status=None):
    """Runs until cancelled. Reconnects with backoff. on_status(connected: bool, msg: str)."""
    status = on_status or (lambda c, m: None)
    backoff = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2 ** 22) as ws:
                log.info("connected to PumpPortal")
                status(True, "connected")
                backoff = 1
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeMigration"}))
                if feed.active:
                    await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": list(feed.active)}))
                if feed.accounts and feed.has_key:
                    await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": sorted(feed.accounts)}))
                    log.info("following %d wallet(s) for copy trading", len(feed.accounts))
                send_task = asyncio.create_task(_sender(ws, feed))
                try:
                    async for raw in ws:
                        try:
                            ev = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        if not isinstance(ev, dict) or "txType" not in ev:
                            if isinstance(ev, dict) and ev.get("message"):
                                log.debug("server: %s", ev["message"])
                                if "error" in str(ev.get("message", "")).lower():
                                    status(True, str(ev["message"])[:160])
                            continue
                        ts = time.time()
                        if recorder:
                            ev["_ts"] = ts
                            recorder.write(json.dumps(ev) + "\n")
                        engine.on_event(ev, ts)
                finally:
                    send_task.cancel()
            status(False, "disconnected - reconnecting")
        except asyncio.CancelledError:
            raise
        except Exception as e:   # network errors, handshake failures, proxy refusals
            log.warning("feed error: %s - reconnecting in %ss", e, backoff)
            status(False, f"feed error: {e}"[:160])
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)
