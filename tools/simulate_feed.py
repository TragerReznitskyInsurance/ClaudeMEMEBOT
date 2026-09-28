"""
Generates a FAKE feed (data/events_sim.jsonl) so you can check the bot runs end
to end without an API key. The numbers are synthetic - never judge a strategy on this.

    python tools/simulate_feed.py && python backtest.py data/events_sim.jsonl
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from memebot.demo import generate  # noqa: E402


def main():
    os.makedirs("data", exist_ok=True)
    events = generate(1_790_000_000.0, n_tokens=120, spacing_s=20, seed=7)
    with open("data/events_sim.jsonl", "w") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    print(f"wrote {len(events)} synthetic events -> data/events_sim.jsonl")


if __name__ == "__main__":
    main()
