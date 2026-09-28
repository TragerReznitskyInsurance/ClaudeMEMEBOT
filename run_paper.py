"""
Headless live PAPER trading (no dashboard). For the visual app, run app.py.

    python run_paper.py                 # uses config.yaml (+ settings.json overrides)
Stop with Ctrl+C - open paper positions are marked to market and a summary prints.
"""
import argparse
import asyncio
import logging
import os
import time

from memebot.engine import Engine, Journal
from memebot.live import LiveFeed, stream, tick_loop
from memebot.security import RugCheckScreener
from memebot.settings import load_config

log = logging.getLogger("memebot")


async def run(cfg):
    out = cfg["output"]["dir"]
    tag = time.strftime("%Y%m%d_%H%M%S")
    journal = Journal(out, tag)
    key = os.environ.get("PUMPPORTAL_API_KEY") or cfg["feed"].get("api_key") or ""
    if not key:
        log.warning("No PumpPortal API key: the bot will see launches but not their trades, "
                    "so it can never get an entry signal. Set PUMPPORTAL_API_KEY.")
    feed = LiveFeed(bool(key))
    engine = None
    screener = RugCheckScreener(lambda: engine, lambda: engine.cfg["security"])
    engine = Engine(cfg, feed, journal, screener=screener,
                    blocklist_path=os.path.join(out, "creator_blocklist.txt"))
    rec = open(os.path.join(out, f"events_{tag}.jsonl"), "a") if cfg["output"]["record_raw_events"] else None
    url = cfg["feed"]["url"] + (f"?api-key={key}" if key else "")

    def status_line():
        if int(time.time()) % 60 == 0:
            log.info("status: open %d | closed %d | balance %.3f SOL | day pnl %+.3f | msgs today %d",
                     len(engine.positions), len(engine.closed), engine.balance, engine.day_pnl, engine.day_trade_msgs)

    tasks = [asyncio.create_task(stream(engine, feed, url, rec)),
             asyncio.create_task(tick_loop(engine, on_tick=status_line))]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await screener.close()
        engine.close_all(time.time())
        if rec:
            rec.close()
        journal.close()
        print(engine.summary())
        print(f"Logs written to {os.path.abspath(out)} (tag {tag})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        asyncio.run(run(load_config(a.config)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
