"""
Combine results across every paper run (or backtest) into one scorecard.

    python report.py                      # all data/positions_*.csv
    python report.py "data/backtests/positions_*.csv"
"""
import csv
import glob
import sys
from collections import Counter, defaultdict

pattern = sys.argv[1] if len(sys.argv) > 1 else "data/positions_*.csv"
rows = []
for path in sorted(glob.glob(pattern)):
    with open(path) as fh:
        rows += list(csv.DictReader(fh))
if not rows:
    raise SystemExit(f"no closed positions found in {pattern}")

pnl = [float(r["pnl_sol"]) for r in rows]
pct = [float(r["pnl_pct"]) for r in rows]
wins = [p for p in pnl if p > 0]
losses = [p for p in pnl if p <= 0]
by_day = defaultdict(float)
for r in rows:
    by_day[r["close_utc"][:10]] += float(r["pnl_sol"])
last_exit = Counter(r["exit_reasons"].split(" | ")[-1] for r in rows)

# max drawdown on the cumulative PnL curve
peak = cum = mdd = 0.0
for p in pnl:
    cum += p
    peak = max(peak, cum)
    mdd = max(mdd, peak - cum)

print(f"Trades: {len(rows)}   win rate: {len(wins) / len(rows) * 100:.1f}%")
print(f"Total PnL: {sum(pnl):+.4f} SOL   avg/trade: {sum(pnl) / len(rows):+.4f} SOL")
print(f"Avg win: {sum(wins) / len(wins) if wins else 0:+.4f}   avg loss: {sum(losses) / len(losses) if losses else 0:+.4f}")
print(f"Best: {max(pct):+.0f}%   worst: {min(pct):+.0f}%   max drawdown: {mdd:.4f} SOL")
share = max(wins) / sum(wins) * 100 if wins else 0
print(f"Largest single win = {share:.0f}% of all winnings "
      f"{'(results hinge on one outlier - be careful)' if share > 40 else ''}")
print("\nPnL by day (UTC):")
for d in sorted(by_day):
    print(f"  {d}  {by_day[d]:+.4f} SOL")
if any(r.get("entry_path") for r in rows):
    print("\nBy entry trigger:")
    for path in sorted({r.get("entry_path") or "momentum" for r in rows}):
        sub = [float(r["pnl_sol"]) for r in rows if (r.get("entry_path") or "momentum") == path]
        w = sum(1 for p in sub if p > 0)
        gaps = [float(r["entry_gap_pct"]) for r in rows if (r.get("entry_path") or "momentum") == path and r.get("entry_gap_pct") not in (None, "", "None")]
        xg = [float(r["exit_gap_pct"]) for r in rows if (r.get("entry_path") or "momentum") == path and r.get("exit_gap_pct") not in (None, "", "None")]
        extra = (f"  entry gap {sum(gaps) / len(gaps):+.1f}%" if gaps else "") + (f"  exit gap {sum(xg) / len(xg):+.1f}%" if xg else "")
        print(f"  {path:<20} {len(sub):>4} trades  win {w / len(sub) * 100:>4.0f}%  pnl {sum(sub):+.4f} SOL{extra}")
print("\nFinal exit reason:")
for k, v in last_exit.most_common():
    print(f"  {v:>5}  {k}")
