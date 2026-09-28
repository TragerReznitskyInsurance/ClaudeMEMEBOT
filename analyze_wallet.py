"""
Analyze how a Solana wallet trades memecoins (command-line version of the dashboard's Wallet Lab).

    python analyze_wallet.py <wallet address> [--days 30]

Needs a free Helius API key: add it in the dashboard (Wallet Lab) or set HELIUS_API_KEY.
Results go to data/wallets/<address>/ - send the wallet_report_*.zip file back for review.
"""
import argparse
import asyncio
import os

from memebot import wallet as WL
from memebot.settings import helius_key

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("address")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--max-tx", type=int, default=4000)
    ap.add_argument("--raw", help="re-analyze a saved raw_transactions.jsonl instead of downloading")
    a = ap.parse_args()
    key = helius_key(os.path.join(HERE, "config.yaml"))
    s, out = asyncio.run(WL.analyze(a.address, key, os.path.join(HERE, "data", "wallets"), days=a.days,
                                    max_tx=a.max_tx, raw_path=a.raw,
                                    progress_cb=lambda **k: print("  " + k["message"], end="\r", flush=True)))
    print("\n" + WL.summary_text(s))
    print(f"\nFiles in {out}")


if __name__ == "__main__":
    main()
