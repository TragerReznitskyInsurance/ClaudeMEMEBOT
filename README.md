# Momentum: memecoin momentum bot (paper trading)

This bot watches pump.fun token launches on Solana in real time. It skips the
sniper window, then paper-buys only tokens with broad, organic-looking buying
momentum. Exits use a take-profit ladder, a stop loss, a trailing stop, a time
stop, and an instant exit if the dev sells.

**It is paper-only by design.** It has no wallet and no private key, and it
places no orders. It records what it *would* have done, using pessimistic
simulated fills, so you can find out whether the strategy has an edge before
any real money is involved.

## Quick start: the dashboard app

1. Install Python 3.10+ from https://www.python.org/downloads/. On Windows, tick "Add Python to PATH".
2. Double-click the launcher:
   - **Windows:** `start-windows.bat`
   - **Mac:** `start-mac.command`. If macOS blocks it the first time, right-click it and choose Open.

   The first launch installs everything automatically. After that, the dashboard opens at http://localhost:8765.
3. Click **Try the demo** to see it working on synthetic tokens. There's nothing to set up.
4. When you're ready for real launches, open **Settings** (the gear icon), paste your PumpPortal API key (see Setup below), and click **Start live**.

What's on the dashboard:
- **Top row:** equity, P&L, today's result against the daily loss limit, win rate, open position slots, and how much data you've used against the budget.
- **Equity curve:** hover over it to see any point.
- **Filter funnel:** tokens seen → watched → signals → trades, plus the reasons tokens were skipped.
- **Open positions:** each one shows a live chart and a track running from stop loss to TP1 and TP2, with links to pump.fun and DexScreener.
- **Watchlist:** every token being evaluated, with its 10 entry filters shown as green/red bars. Hover over the bars for exact values.
- **Activity feed and trade history.**
- **Settings drawer:** change filters, exits, sizing and risk. Changes apply to the running bot immediately, except the few marked "next start". They're saved in `settings.json`.

**Updating:** if you downloaded the bot with Git, the launcher pulls the latest version from GitHub every time you start it, so just restart. Your `settings.json` (API key and settings) and `data` folder are never uploaded to GitHub and are never overwritten.

Everything the app does is also logged to CSV files in `data/`. Demo runs go to `data/demo/`, so they never mix with real results.

Prefer the terminal? `python app.py` does the same as the launchers. `python run_paper.py` runs the bot with no dashboard.

## How it decides

1. **Security gate at launch.** A token must pass every check here before the bot even watches it. Failures cost nothing in paid data and never reach the watchlist.
   - **Dev bag:** the dev's launch buy must be ≤ 5% of supply. For scale, 1 SOL buys about 3.5%.
   - **Serial launchers:** skip creators who've launched more than 2 tokens in 24 hours. This counts launches the bot has seen since it started.
   - **Known dumpers:** any creator the bot has seen sell their own token is blocked for good. The list lives in `data/creator_blocklist.txt`.
   - **Name blacklist:** skip names containing words like "rug", "scam" or "test".
   - **RugCheck:** a free API check. It fails on freeze or mint authority still enabled, a creator history of rugged tokens, copycat tokens, and honeypots. RugCheck needs about 5–25 seconds to index a brand-new token, so the bot retries until it can. The token is watched during that wait, but it can't be bought until RugCheck passes, and it's dropped the moment RugCheck fails. RugCheck's overall score is ignored by default because it flags nearly every brand-new token. If RugCheck never answers, the default is to skip the token. You can change that in Settings.
2. **Watch window (30 s to 4 min).** Stream every trade on the token and track unique buyers, buy and sell volume, market cap, peak, whale concentration, and whether the dev has sold.
3. **Entry.** Every safety filter below must pass, and then **either** trigger fires:
   - **Trigger A (momentum):** buy/sell volume since launch ≥ 1.5×, and the price is up ≥ 15% over the last 30 s.
   - **Trigger B (buyer surge):** ≥ 10 wallets buying for the first time in the last 20 s, buy/sell ≥ 1.2× within those 20 s, and the price is up ≥ 5% over them. This catches runners where early snipers selling dragged the all-time ratio down while fresh buyers poured in.

   `report.py` and the trade history show which trigger each trade came from, so you can see which one actually makes money.

   Safety filters (all required):
   - ≥ 15 unique buyers
   - ≥ 20 buys and ≥ 5 SOL of buy volume
   - market cap between 40 and 300 SOL
   - no single wallet with more than 25% of the buying
   - no more than 20% below its peak
   - the dev hasn't sold
4. **Exit.** Whichever of these comes first:
   - −30% stop loss
   - sell 40% at +50% and 30% at +150%
   - 25% trailing stop, which arms once the position is up 30%
   - 15-minute time stop
   - 90 s with no trades
   - the dev sells

Every number above lives in `config.yaml`.

**Simulated execution is deliberately harsh.** Each fill:
- happens 1.5 s after the decision, at whatever the price is then
- includes bonding-curve price impact
- pays a 1.25% platform fee per side
- adds 2% extra slippage
- pays a priority fee on every transaction

## Setup

You need Python 3.10 or newer.

```bash
cd memebot
python -m venv .venv
# Mac/Linux:  source .venv/bin/activate
# Windows:    .venv\Scripts\activate
pip install -r requirements.txt
```

**Get a PumpPortal API key.** Launch events are free, but the per-token trade
stream the strategy needs is metered at 0.01 SOL per 10,000 messages.
1. Create a key at https://pumpportal.fun/trading-api/setup. This creates a linked wallet.
2. Send that wallet about **0.05–0.1 SOL** to pay for data. The minimum is 0.02 SOL.
3. The bot never uses that wallet for trading. It's only billed for data.

The easiest way to add the key is in the dashboard's Settings, which saves it to `settings.json` on your computer. Or set it as an environment variable, which takes priority:

```bash
# Mac/Linux
export PUMPPORTAL_API_KEY=your-key
# Windows PowerShell
$env:PUMPPORTAL_API_KEY="your-key"
```

`max_trade_messages_per_day` in `config.yaml` caps the data spend. The default
is 300k messages, about 0.3 SOL per day at most. Setting `universe: migrated`
watches only graduated tokens, which is far cheaper.

## Copy trading (paper)

In **Settings → Copy trading**, turn on **Copy a wallet**, paste the wallet address, click Save, then **Stop** and **Start live** again.

When that wallet buys, Momentum paper-buys the same token after your simulated delay. **Buy size** can be the same SOL amount as the wallet (default), a % of it, or a fixed amount. When it sells, Momentum sells the same share of the position, so with matching sizes the sell amounts match too. Matching a wallet that trades 1.5 SOL at a time needs a large paper balance: set **Starting paper balance** to about 300 SOL. Positions and trade history show a **Copy** badge, and a banner tracks what the wallet did and what was copied.

To test copying on its own, turn **Also run momentum strategy** off. Copied positions have their own limit (**Max copied positions**) and don't use up the momentum strategy's slots.

## Wallet Lab: learn from a wallet that trades well

Click **🔬 Wallet Lab** in the dashboard, paste any Solana wallet, and Momentum rebuilds every memecoin trade that wallet made. It shows:
- how it loses: loss sizes and how fast it cuts
- how it wins: hold times, first sell, selling in stages
- how it enters: size, market cap, seconds after launch, venue
- warning flags, such as sniper/insider timing or a few outlier wins carrying all the profit

Setup: a free API key from **helius.dev** (sign up and copy the key from the dashboard). Paste it into Wallet Lab once, and it's saved only on your computer.

Click **Download full report** and send the zip to Claude to turn the pattern into bot rules. The command-line version is `python analyze_wallet.py <address> --days 30`.

## Command-line tools

```bash
python app.py                                                               # dashboard (same as the launchers)
python run_paper.py                                                         # headless live paper trading (Ctrl+C to stop)
python report.py                                                            # scorecard across all runs
python tools/simulate_feed.py && python backtest.py data/events_sim.jsonl   # smoke test on fake data
```

Live runs, from either the app or `run_paper.py`, write to `data/`:
- `fills_*.csv`: every simulated buy and sell
- `positions_*.csv`: one row per closed trade, with PnL and exit reasons
- `decisions_*.csv`: every token and why it was bought or rejected. This is where you tune the filters.
- `events_*.jsonl`: the raw feed, for replaying

## Tuning without waiting for new data

RugCheck results can't be replayed, so backtests apply only the instant checks of the security gate (dev bag, serial launchers, dumpers, names).

Copy `config.yaml` to something like `tight.yaml` and change the numbers, then
replay what you've already recorded:

```bash
python backtest.py -q -c tight.yaml "data/events_*.jsonl"
python report.py "data/backtests/positions_*.csv"
```

The bot only records trades on tokens it's watching, and it stops recording a
token once it rejects it. So replays are trustworthy for settings that are the
same or **stricter**. They can't tell you what a looser filter would have caught.

## Reading the results honestly

- **Run it for at least 1–2 weeks** across different market moods before drawing conclusions. A single weekend proves nothing.
- **Check `report.py`'s "largest single win" line.** If one moonshot is carrying all the profit, the strategy doesn't have an edge. It got lucky once.
- **Paper fills are still optimistic in one way.** They assume your transaction lands. In reality some fail or land late during congestion, especially sells during a dump.
- **Ignore `tools/simulate_feed.py` results.** That feed is synthetic and exists only to prove the code runs. Never judge the strategy on it.

## Going live (later, and only if the paper results earn it)

Live execution isn't included. If weeks of paper results hold up, the next step
would be:
- a dedicated hot wallet holding only what you can afford to lose
- signing locally through PumpPortal's local transaction API or Jupiter
- a paid RPC
- a hard cap on total SOL at risk, with the same risk limits already in this config

This is not financial advice. Most memecoins go to zero, and most memecoin
bots lose money.
