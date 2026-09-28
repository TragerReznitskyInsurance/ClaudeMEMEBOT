"""
Replay recorded feed data through the strategy with any config.

    python backtest.py data/events_*.jsonl
    python backtest.py -c tighter.yaml data/events_20260928_*.jsonl

Caveat: the live bot only records trades for tokens it chose to watch, and
stops recording a token once it rejects it. So replays are reliable for filters
that are the SAME or STRICTER than the ones used while recording. Loosening
filters (e.g. a longer watch window) can't be tested on data that was never captured.
"""
import argparse
import glob
import json
import logging
import os

import yaml

from memebot.engine import Engine, Journal


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("events", nargs="+", help="events_*.jsonl files recorded by run_paper.py")
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("-o", "--out", default="data/backtests")
    ap.add_argument("-q", "--quiet", action="store_true", help="only print the summary")
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING if a.quiet else logging.INFO,
                        format="%(message)s")
    with open(a.config) as fh:
        cfg = yaml.safe_load(fh)

    files = sorted(f for pat in a.events for f in glob.glob(pat))
    if not files:
        raise SystemExit("no event files matched")
    tag = "bt_" + os.path.splitext(os.path.basename(a.config))[0]
    journal = Journal(a.out, tag)
    engine = Engine(cfg, journal=journal)

    last_tick = None
    n = 0
    for path in files:
        with open(path) as fh:
            for line in fh:
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = ev.get("_ts")
                if ts is None:
                    continue
                if last_tick is None:
                    last_tick = ts
                while ts - last_tick >= 1.0:       # 1-second ticks, like live
                    last_tick += 1.0
                    engine.on_tick(last_tick)
                engine.on_event(ev, ts)
                n += 1
    if last_tick is not None:
        for _ in range(int(cfg["exit"]["max_hold_s"]) + 5):   # let open positions play out
            last_tick += 1.0
            engine.on_tick(last_tick)
            if not engine.positions and not engine.pending_buys:
                break
        engine.close_all(last_tick, "end of data")
    journal.close()
    print(f"Replayed {n:,} events from {len(files)} file(s) with {a.config}")
    print(engine.summary())
    print(f"Detailed CSVs in {os.path.abspath(a.out)} (tag {tag})")


if __name__ == "__main__":
    main()
