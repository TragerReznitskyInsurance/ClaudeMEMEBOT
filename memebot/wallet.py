"""
Wallet Lab: reconstructs every memecoin trade a Solana wallet made and measures
HOW it trades - entries, sizing, exits, hold times, win/loss shape - so the
pattern can be turned into bot rules.

Data comes from Helius's Enhanced Transactions API (free key: helius.dev).
Launch times come from pump.fun's public API, with DexScreener as a fallback.
Read-only: this never signs or sends anything.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import statistics as st
import time
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone

import aiohttp

from memebot import wallet_tokens as WT

log = logging.getLogger("memebot")

WSOL = "So11111111111111111111111111111111111111112"
IGNORE_MINTS = {
    WSOL,
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
}
HELIUS_URL = os.environ.get("MOMENTUM_HELIUS_URL", "https://api.helius.xyz/v0/addresses/{addr}/transactions")
PUMP_COIN_URL = os.environ.get("MOMENTUM_PUMP_COIN_URL", "https://frontend-api-v3.pump.fun/coins/{mint}")
DEXSCREENER_URL = os.environ.get("MOMENTUM_DEXSCREENER_URL", "https://api.dexscreener.com/latest/dex/tokens/{mint}")
SUPPLY = 1_000_000_000          # pump.fun tokens; market caps assume this
DUST_SOL = 0.00002              # smaller SOL moves than this are just fees/rent noise
BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def valid_address(a: str) -> bool:
    return bool(BASE58.match(a or ""))


def _q(xs, q):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _med(xs):
    return _q(xs, 0.5)


def _iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if ts else ""


def _r(x, n=4):
    return round(x, n) if isinstance(x, (int, float)) else x


# ───────────────────────────────────────────────────────── fetching
class Progress:
    def __init__(self, cb=None):
        self.cb = cb or (lambda **k: None)

    def __call__(self, **k):
        try:
            self.cb(**k)
        except Exception:
            pass


async def fetch_history(session, wallet, key, since_ts, max_tx, progress: Progress):
    """All transactions touching the wallet since `since_ts`, newest first."""
    txs, before, use_param = [], None, "before-signature"
    seen = set()
    retries = 0
    while len(txs) < max_tx:
        params = {"api-key": key, "limit": "100", "token-accounts": "balanceChanged"}
        if before:
            params[use_param] = before
        try:
            async with session.get(HELIUS_URL.format(addr=wallet), params=params,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                body = await r.text()
                status = r.status
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retries += 1
            if retries > 5:
                raise RuntimeError(f"Helius unreachable: {type(e).__name__}")
            await asyncio.sleep(2 * retries)
            continue
        if status == 429:
            retries += 1
            await asyncio.sleep(min(2 * retries, 10))
            continue
        if status in (401, 403):
            raise RuntimeError("Helius rejected the API key - check it in Settings")
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            raise RuntimeError(f"Helius returned HTTP {status}: {body[:120]}")
        if isinstance(data, dict):
            # Helius asks you to continue from a signature when a window has no matches
            m = re.search(r"`?before(?:-signature)?`? parameter set to ([1-9A-HJ-NP-Za-km-z]{60,90})", str(data))
            if m:
                before = m.group(1)
                continue
            raise RuntimeError(f"Helius error: {str(data.get('error') or data)[:160]}")
        retries = 0
        if not data:
            break
        if before and data[0].get("signature") in seen and use_param == "before-signature":
            use_param = "before"          # older API name for the same parameter
            continue
        stop = False
        for tx in data:
            sig = tx.get("signature")
            if sig in seen:
                continue
            seen.add(sig)
            if (tx.get("timestamp") or 0) < since_ts:
                stop = True
                break
            txs.append(tx)
        progress(phase="fetch", done=len(txs), total=max_tx,
                 message=f"Downloaded {len(txs):,} transactions"
                         + (f" · back to {_iso(txs[-1]['timestamp'])[:10]}" if txs else ""))
        if stop or len(data) < 100:
            break
        before = data[-1].get("signature")
        await asyncio.sleep(0.12)
    return txs


async def fetch_launch_info(session, mints, progress: Progress, limit=500):
    """{mint: {created_ts, symbol, name, creator, graduated}} - best effort."""
    out = {}
    sem = asyncio.Semaphore(4)
    mints = list(mints)[:limit]
    done = 0

    async def one(m):
        nonlocal done
        async with sem:
            info = {}
            try:
                async with session.get(PUMP_COIN_URL.format(mint=m), timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        j = await r.json(content_type=None)
                        if isinstance(j, dict) and j.get("created_timestamp"):
                            info = dict(created_ts=j["created_timestamp"] / 1000, symbol=j.get("symbol"),
                                        name=j.get("name"), creator=j.get("creator"),
                                        graduated=bool(j.get("complete")), src="pump.fun")
            except Exception:
                pass
            if not info:
                try:
                    async with session.get(DEXSCREENER_URL.format(mint=m), timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            j = await r.json(content_type=None)
                            pairs = [p for p in (j.get("pairs") or []) if p.get("pairCreatedAt")]
                            if pairs:
                                p = min(pairs, key=lambda p: p["pairCreatedAt"])
                                info = dict(created_ts=p["pairCreatedAt"] / 1000,
                                            symbol=(p.get("baseToken") or {}).get("symbol"),
                                            name=(p.get("baseToken") or {}).get("name"),
                                            src="dexscreener (first pool)")
                except Exception:
                    pass
            out[m] = info
            done += 1
            if done % 10 == 0 or done == len(mints):
                progress(phase="launch", done=done, total=len(mints), message=f"Looked up {done}/{len(mints)} tokens")
            await asyncio.sleep(0.05)

    await asyncio.gather(*(one(m) for m in mints))
    return out


# ───────────────────────────────────────────────────────── reconstruction
def tx_deltas(tx, wallet):
    """(SOL change incl. fees & WSOL, {mint: token change}) for the wallet in one tx."""
    sol = 0.0
    toks = defaultdict(float)
    for ad in tx.get("accountData") or []:
        if ad.get("account") == wallet:
            sol += (ad.get("nativeBalanceChange") or 0) / 1e9
        for tb in ad.get("tokenBalanceChanges") or []:
            if tb.get("userAccount") != wallet:
                continue
            raw = tb.get("rawTokenAmount") or {}
            try:
                amt = int(raw.get("tokenAmount", 0)) / (10 ** int(raw.get("decimals", 0)))
            except (TypeError, ValueError):
                continue
            if tb.get("mint") == WSOL:
                sol += amt
            else:
                toks[tb.get("mint")] += amt
    return sol, {m: v for m, v in toks.items() if abs(v) > 0}


def build_fills(txs, wallet):
    fills, skipped = [], Counter()
    for tx in sorted(txs, key=lambda t: (t.get("timestamp") or 0, t.get("slot") or 0)):
        if tx.get("transactionError"):
            continue
        sol, toks = tx_deltas(tx, wallet)
        toks = {m: v for m, v in toks.items() if m not in IGNORE_MINTS}
        if not toks:
            continue
        if len(toks) > 1:
            skipped["multi-token transaction"] += 1
            continue
        mint, amt = next(iter(toks.items()))
        base = dict(ts=tx.get("timestamp"), slot=tx.get("slot"), sig=tx.get("signature"), mint=mint,
                    source=tx.get("source") or "", tokens=abs(amt))
        if amt > 0 and sol < -DUST_SOL:
            fills.append(base | dict(side="buy", sol=-sol, price=-sol / amt))
        elif amt < 0 and sol > DUST_SOL:
            fills.append(base | dict(side="sell", sol=sol, price=sol / -amt))
        elif amt > 0:
            fills.append(base | dict(side="in", sol=0.0, price=None))
        else:
            fills.append(base | dict(side="out", sol=0.0, price=None))
    return fills, skipped


def build_trips(fills, launch):
    """Group fills into round trips: flat -> position -> flat, per token."""
    by_mint = defaultdict(list)
    for f in fills:
        by_mint[f["mint"]].append(f)
    trips, orphan_sells = [], 0
    for mint, fs in by_mint.items():
        trip = None
        for f in fs:
            if f["side"] in ("buy", "in"):
                if trip is None:
                    trip = dict(mint=mint, fills=[], pos=0.0, max_pos=0.0, sol_in=0.0, sol_out=0.0,
                                bought_tokens=0.0, transferred_in=0.0, transferred_out=0.0)
                trip["fills"].append(f)
                trip["pos"] += f["tokens"]
                trip["max_pos"] = max(trip["max_pos"], trip["pos"])
                if f["side"] == "buy":
                    trip["sol_in"] += f["sol"]
                    trip["bought_tokens"] += f["tokens"]
                else:
                    trip["transferred_in"] += f["tokens"]
            else:
                if trip is None:
                    orphan_sells += 1          # bought before the analysis window
                    continue
                trip["fills"].append(f)
                trip["pos"] -= f["tokens"]
                if f["side"] == "sell":
                    trip["sol_out"] += f["sol"]
                else:
                    trip["transferred_out"] += f["tokens"]
                if trip["pos"] <= trip["max_pos"] * 0.01:
                    trip["status"] = "closed"
                    trips.append(trip)
                    trip = None
        if trip is not None:
            trip["status"] = "open"
            trips.append(trip)

    out = []
    for t in trips:
        fs = t["fills"]
        buys = [f for f in fs if f["side"] == "buy"]
        sells = [f for f in fs if f["side"] == "sell"]
        if not buys:
            continue                                  # airdrops / transfers only
        avg_entry = t["sol_in"] / t["bought_tokens"] if t["bought_tokens"] else None
        first_buy = buys[0]
        info = launch.get(t["mint"]) or {}
        pnl = t["sol_out"] - t["sol_in"]
        sell_steps = []
        remaining = t["max_pos"]
        for f in sells:
            sell_steps.append(dict(t=f["ts"] - first_buy["ts"], mult=f["price"] / avg_entry if avg_entry else None,
                                   frac=f["tokens"] / t["max_pos"] if t["max_pos"] else None))
        adds = buys[1:]
        add_style = ""
        if adds:
            higher = sum(1 for b in adds if b["price"] > first_buy["price"] * 1.02)
            lower = sum(1 for b in adds if b["price"] < first_buy["price"] * 0.98)
            add_style = "pyramid (added higher)" if higher > lower else "averaged down" if lower > higher else "mixed"
        created = info.get("created_ts")
        rec = dict(
            mint=t["mint"], symbol=info.get("symbol") or "", status=t["status"],
            first_buy_utc=_iso(first_buy["ts"]), first_buy_ts=first_buy["ts"], first_buy_sig=first_buy["sig"],
            last_fill_ts=fs[-1]["ts"], hold_s=(fs[-1]["ts"] - first_buy["ts"]) if t["status"] == "closed" else None,
            n_buys=len(buys), n_sells=len(sells), sol_in=t["sol_in"], sol_out=t["sol_out"],
            pnl_sol=pnl if t["status"] == "closed" else None,
            pnl_pct=(pnl / t["sol_in"] * 100) if t["status"] == "closed" and t["sol_in"] else None,
            first_buy_sol=first_buy["sol"],
            entry_mcap_sol=first_buy["price"] * SUPPLY if first_buy["price"] else None,
            entry_delay_s=(first_buy["ts"] - created) if created else None,
            launch_src=info.get("src", ""), graduated=info.get("graduated"),
            is_pump=t["mint"].endswith("pump"),
            buy_source=first_buy["source"],
            first_sell_s=sell_steps[0]["t"] if sell_steps else None,
            first_sell_mult=sell_steps[0]["mult"] if sell_steps else None,
            first_sell_frac=sell_steps[0]["frac"] if sell_steps else None,
            max_sell_mult=max((s["mult"] for s in sell_steps if s["mult"]), default=None),
            sell_style="ladder" if len(sells) > 1 else "single" if sells else "",
            add_style=add_style,
            transferred_out=t["transferred_out"] > 0,
            sell_steps=sell_steps,
        )
        out.append(rec)
    out.sort(key=lambda r: r["first_buy_ts"])
    return out, orphan_sells


# ───────────────────────────────────────────────────────── statistics
def summarize(wallet, trips, fills, skipped, orphan_sells, days, tx_count, max_tx=None):
    closed = [t for t in trips if t["status"] == "closed" and not t["transferred_out"]]
    wins = [t for t in closed if t["pnl_sol"] > 0]
    losses = [t for t in closed if t["pnl_sol"] <= 0]
    gross_win = sum(t["pnl_sol"] for t in wins)
    gross_loss = -sum(t["pnl_sol"] for t in losses)
    n = len(closed)
    first_ts = min((t["first_buy_ts"] for t in trips), default=None)
    last_ts = max((t["last_fill_ts"] for t in trips), default=None)
    span_days = max((last_ts - first_ts) / 86400, 1 / 24) if first_ts else None

    def dist(xs, edges, labels):
        c = Counter()
        for x in xs:
            for e, lab in zip(edges, labels):
                if x <= e:
                    c[lab] += 1
                    break
            else:
                c[labels[-1]] += 1
        tot = sum(c.values()) or 1
        return {lab: dict(count=c[lab], pct=round(c[lab] / tot * 100, 1)) for lab in labels}

    delays = [t["entry_delay_s"] for t in trips if t["entry_delay_s"] is not None]
    top5 = sorted(wins, key=lambda t: -t["pnl_sol"])[:5]
    hours = Counter(datetime.fromtimestamp(t["first_buy_ts"], tz=timezone.utc).hour for t in trips)
    s = dict(
        wallet=wallet, generated_utc=_iso(time.time()), window_days=days, transactions=tx_count,
        period=dict(first=_iso(first_ts), last=_iso(last_ts), days=_r(span_days, 2)),
        counts=dict(trips=len(trips), closed=n, open=len([t for t in trips if t["status"] == "open"]),
                    trips_per_day=_r(len(trips) / span_days, 1) if span_days else None,
                    fills=len(fills), orphan_sells=orphan_sells, skipped=dict(skipped),
                    transferred_out=len([t for t in trips if t["transferred_out"]])),
        results=dict(
            win_rate_pct=_r(len(wins) / n * 100, 1) if n else None,
            total_pnl_sol=_r(gross_win - gross_loss), gross_win_sol=_r(gross_win), gross_loss_sol=_r(gross_loss),
            profit_factor=_r(gross_win / gross_loss, 2) if gross_loss else None,
            expectancy_sol_per_trade=_r((gross_win - gross_loss) / n) if n else None,
            avg_win_sol=_r(gross_win / len(wins)) if wins else None,
            avg_loss_sol=_r(-gross_loss / len(losses)) if losses else None,
            payoff_ratio=_r((gross_win / len(wins)) / (gross_loss / len(losses)), 2) if wins and losses and gross_loss else None,
            median_win_pct=_r(_med([t["pnl_pct"] for t in wins]), 1),
            p90_win_pct=_r(_q([t["pnl_pct"] for t in wins], 0.9), 1),
            median_loss_pct=_r(_med([t["pnl_pct"] for t in losses]), 1),
            worst_loss_pct=_r(min((t["pnl_pct"] for t in losses), default=None), 1),
            top5_share_of_winnings_pct=_r(sum(t["pnl_sol"] for t in top5) / gross_win * 100, 1) if gross_win else None,
        ),
        losses=dict(
            size_buckets=dist([-t["pnl_pct"] for t in losses], [10, 25, 50, 80],
                              ["0-10%", "10-25%", "25-50%", "50-80%", "80-100%"]),
            median_hold_s=_r(_med([t["hold_s"] for t in losses]), 0),
            p75_hold_s=_r(_q([t["hold_s"] for t in losses], 0.75), 0),
            hold_buckets=dist([t["hold_s"] for t in losses], [30, 120, 600, 3600],
                              ["<30s", "30s-2m", "2-10m", "10-60m", ">1h"]),
            median_exit_mult=_r(_med([t["max_sell_mult"] for t in losses]), 3),
            single_sell_pct=_r(sum(1 for t in losses if t["sell_style"] == "single") / len(losses) * 100, 1) if losses else None,
        ),
        wins=dict(
            median_hold_s=_r(_med([t["hold_s"] for t in wins]), 0),
            p75_hold_s=_r(_q([t["hold_s"] for t in wins], 0.75), 0),
            hold_buckets=dist([t["hold_s"] for t in wins], [30, 120, 600, 3600],
                              ["<30s", "30s-2m", "2-10m", "10-60m", ">1h"]),
            median_first_sell_s=_r(_med([t["first_sell_s"] for t in wins]), 0),
            median_first_sell_mult=_r(_med([t["first_sell_mult"] for t in wins]), 2),
            median_first_sell_frac_pct=_r((_med([t["first_sell_frac"] for t in wins]) or 0) * 100, 0) if wins else None,
            median_max_sell_mult=_r(_med([t["max_sell_mult"] for t in wins]), 2),
            ladder_pct=_r(sum(1 for t in wins if t["sell_style"] == "ladder") / len(wins) * 100, 1) if wins else None,
        ),
        entries=dict(
            median_buy_sol=_r(_med([t["first_buy_sol"] for t in trips]), 3),
            p10_buy_sol=_r(_q([t["first_buy_sol"] for t in trips], 0.1), 3),
            p90_buy_sol=_r(_q([t["first_buy_sol"] for t in trips], 0.9), 3),
            avg_size_winners=_r(st.mean([t["first_buy_sol"] for t in wins]), 3) if wins else None,
            avg_size_losers=_r(st.mean([t["first_buy_sol"] for t in losses]), 3) if losses else None,
            pump_token_pct=_r(sum(1 for t in trips if t["is_pump"]) / len(trips) * 100, 1) if trips else None,
            entry_mcap_sol=dict(p25=_r(_q([t["entry_mcap_sol"] for t in trips if t["is_pump"]], 0.25), 1),
                                median=_r(_med([t["entry_mcap_sol"] for t in trips if t["is_pump"]]), 1),
                                p75=_r(_q([t["entry_mcap_sol"] for t in trips if t["is_pump"]], 0.75), 1)),
            entry_mcap_winners_median=_r(_med([t["entry_mcap_sol"] for t in wins if t["is_pump"]]), 1),
            entry_mcap_losers_median=_r(_med([t["entry_mcap_sol"] for t in losses if t["is_pump"]]), 1),
            launch_time_known=len(delays),
            delay_buckets=dist(delays, [3, 30, 300, 3600, 86400],
                               ["<3s", "3-30s", "30s-5m", "5-60m", "1-24h", ">1 day"]),
            median_delay_s=_r(_med(delays), 0),
            venues=dict(Counter(t["buy_source"] or "UNKNOWN" for t in trips).most_common(6)),
            scaled_in_pct=_r(sum(1 for t in trips if t["n_buys"] > 1) / len(trips) * 100, 1) if trips else None,
            add_styles=dict(Counter(t["add_style"] for t in trips if t["add_style"])),
            busiest_hours_utc=[h for h, _ in hours.most_common(5)],
        ),
        top_trades=[dict(symbol=t["symbol"], mint=t["mint"], pnl_sol=_r(t["pnl_sol"]), pnl_pct=_r(t["pnl_pct"], 0),
                         hold_s=t["hold_s"], entry_mcap_sol=_r(t["entry_mcap_sol"], 1), entry_delay_s=t["entry_delay_s"])
                    for t in top5],
    )
    s["max_tx"] = max_tx
    s["flags"] = flags(s, trips)
    return s


def flags(s, trips):
    out = []
    d = s["entries"]["delay_buckets"]
    known = s["entries"]["launch_time_known"]
    if known >= 10 and d["<3s"]["pct"] >= 30:
        out.append(("warn", "Sniper/insider pattern",
                    f"{d['<3s']['pct']}% of entries land within 3s of launch. That needs dedicated infrastructure "
                    "or inside knowledge - the bot can copy the picks, not the entry prices."))
    pump_entries = [t for t in trips if t["is_pump"] and t["entry_mcap_sol"]]
    at_launch = [t for t in pump_entries if t["entry_mcap_sol"] < 32]
    if len(pump_entries) >= 20 and len(at_launch) / len(pump_entries) >= 0.15:
        out.append(("warn", "Buys at launch price",
                    f"{len(at_launch) / len(pump_entries) * 100:.0f}% of its pump.fun entries are at the starting price "
                    "(before other buyers). If those win unusually often, its buys may be what attracts buyers "
                    "(copy-traders following it) - copying it could make you its exit liquidity."))
    if (s["results"]["top5_share_of_winnings_pct"] or 0) > 60 and s["counts"]["closed"] >= 20:
        out.append(("warn", "Outlier-driven",
                    f"The top 5 trades are {s['results']['top5_share_of_winnings_pct']}% of all winnings. "
                    "The edge may be a few lucky hits rather than a repeatable rule."))
    if s["counts"]["closed"] < 30:
        out.append(("info", "Small sample", f"Only {s['counts']['closed']} closed trades - treat patterns as tentative."))
    if s["counts"]["transferred_out"]:
        out.append(("info", "Tokens moved out",
                    f"{s['counts']['transferred_out']} positions were partly transferred to other wallets "
                    "(possibly sold elsewhere) and are excluded from win/loss stats."))
    if s["period"]["days"] and s["window_days"] and s["period"]["days"] < s["window_days"] * 0.8 \
            and s["transactions"] >= s.get("max_tx", 10 ** 9):
        out.append(("warn", "Window cut short",
                    f"Hit the {s['transactions']:,}-transaction limit, so only {s['period']['days']:.0f} of "
                    f"{s['window_days']} days were covered."))
    unsold = [t for t in trips if t["status"] == "open"]
    if unsold:
        old = [t for t in unsold if time.time() - t["first_buy_ts"] > 86400]
        out.append(("warn" if len(unsold) > 0.05 * max(1, len(trips)) else "info", "Never-sold positions",
                    f"{len(unsold)} positions ({sum(t['sol_in'] for t in unsold):.1f} SOL of buys) were never sold, "
                    f"{len(old)} of them over a day old. They are NOT in the PnL, so real results are lower "
                    "if those tokens died."))
    if s["counts"]["orphan_sells"]:
        out.append(("info", "Pre-window positions",
                    f"{s['counts']['orphan_sells']} sells were of tokens bought before the analysis window and are ignored."))
    ls = s["losses"]
    small = (ls["size_buckets"]["0-10%"]["pct"] + ls["size_buckets"]["10-25%"]["pct"]) if s["results"]["median_loss_pct"] is not None else 0
    if small >= 60:
        out.append(("good", "Cuts losers fast",
                    f"{small:.0f}% of losing trades were closed down less than 25% "
                    f"(median loss {s['results']['median_loss_pct']}%, median hold {fmt_s(ls['median_hold_s'])})."))
    pr = s["results"]["payoff_ratio"]
    if pr and pr >= 2:
        out.append(("good", "Asymmetric payoff", f"Average win is {pr}× the average loss."))
    return [dict(level=a, title=b, text=c) for a, b, c in out]


def fmt_s(x):
    if x is None:
        return "n/a"
    x = int(x)
    return f"{x}s" if x < 90 else f"{x // 60}m" if x < 5400 else f"{x / 3600:.1f}h"


def summary_text(s):
    r, w, l, e = s["results"], s["wins"], s["losses"], s["entries"]
    lines = [
        f"WALLET REPORT  {s['wallet']}",
        f"Generated {s['generated_utc']} UTC · window {s['window_days']} days · {s['transactions']:,} transactions",
        f"Period {s['period']['first']} → {s['period']['last']}",
        "",
        f"Trades: {s['counts']['trips']} ({s['counts']['closed']} closed, {s['counts']['open']} open) · "
        f"{s['counts']['trips_per_day']}/day",
        f"Win rate {r['win_rate_pct']}% · PnL {r['total_pnl_sol']} SOL · profit factor {r['profit_factor']} · "
        f"payoff ratio {r['payoff_ratio']}",
        f"Avg win {r['avg_win_sol']} SOL (median +{r['median_win_pct']}%, p90 +{r['p90_win_pct']}%) · "
        f"avg loss {r['avg_loss_sol']} SOL (median {r['median_loss_pct']}%, worst {r['worst_loss_pct']}%)",
        f"Top 5 trades = {r['top5_share_of_winnings_pct']}% of winnings",
        "",
        "HOW IT LOSES",
        f"  loss size: " + ", ".join(f"{k} {v['pct']}%" for k, v in l["size_buckets"].items()),
        f"  hold time: median {fmt_s(l['median_hold_s'])}, p75 {fmt_s(l['p75_hold_s'])} · "
        + ", ".join(f"{k} {v['pct']}%" for k, v in l["hold_buckets"].items()),
        f"  exits in one sell: {l['single_sell_pct']}%",
        "",
        "HOW IT WINS",
        f"  hold time: median {fmt_s(w['median_hold_s'])}, p75 {fmt_s(w['p75_hold_s'])} · "
        + ", ".join(f"{k} {v['pct']}%" for k, v in w["hold_buckets"].items()),
        f"  first sell: after {fmt_s(w['median_first_sell_s'])} at {w['median_first_sell_mult']}× "
        f"selling {w['median_first_sell_frac_pct']}% · best sell {w['median_max_sell_mult']}× (median) · "
        f"sells in stages {w['ladder_pct']}%",
        "",
        "HOW IT ENTERS",
        f"  size: median {e['median_buy_sol']} SOL (p10 {e['p10_buy_sol']}, p90 {e['p90_buy_sol']}) · "
        f"winners avg {e['avg_size_winners']} vs losers {e['avg_size_losers']}",
        f"  pump.fun tokens: {e['pump_token_pct']}% · venues {e['venues']}",
        f"  entry mcap (SOL): p25 {e['entry_mcap_sol']['p25']}, median {e['entry_mcap_sol']['median']}, "
        f"p75 {e['entry_mcap_sol']['p75']} · winners {e['entry_mcap_winners_median']} vs losers {e['entry_mcap_losers_median']}",
        f"  time after launch (known for {e['launch_time_known']}): median {fmt_s(e['median_delay_s'])} · "
        + ", ".join(f"{k} {v['pct']}%" for k, v in e["delay_buckets"].items()),
        f"  scaled in: {e['scaled_in_pct']}% {e['add_styles']} · busiest hours UTC {e['busiest_hours_utc']}",
        "",
        "FLAGS",
    ] + [f"  [{f['level']}] {f['title']}: {f['text']}" for f in s["flags"]]
    tk = s.get("tokens")
    if tk and tk.get("compared"):
        lines += ["", f"WHAT IT BUYS  ({tk['compared']} tokens profiled at the moment of its first buy)"]
        for key, title in (("age", "token age"), ("buyers_before", "buyers before it"), ("dev_buy", "creator's buy"),
                           ("dev_sold", "creator status"), ("links", "links"), ("creator_repeat", "creator")):
            lines.append(f"  {title}:")
            for r in tk.get(key) or []:
                lines.append(f"    {r['label']:<30} {r['share']:>5}% of buys · win {r['win_rate']:>5}% · avg {r['avg_pnl']:+.3f} SOL")
        lines.append(f"  common name words: {', '.join(tk.get('top_words', [])[:20])}")
        if tk.get("words_more_in_winners"):
            lines.append(f"  words much more common in winners: {', '.join(tk['words_more_in_winners'])}")
        if tk.get("wallet_is_creator"):
            lines.append(f"  !! the wallet itself created {tk['wallet_is_creator']} of these tokens")
    return "\n".join(lines)


# ───────────────────────────────────────────────────────── orchestration
async def analyze(wallet, helius_key, out_root, days=30, max_tx=20000, progress_cb=None, raw_path=None,
                  profile_tokens=True, profile_cap=600):
    """Full pipeline. Returns (summary dict, output dir). raw_path lets you re-run on saved data."""
    progress = Progress(progress_cb)
    if not valid_address(wallet):
        raise ValueError("That doesn't look like a Solana wallet address")
    out_dir = os.path.join(out_root, wallet)
    os.makedirs(out_dir, exist_ok=True)
    since = time.time() - days * 86400
    async with aiohttp.ClientSession(headers={"User-Agent": "momentum-wallet-lab"}) as session:
        if raw_path:
            with open(raw_path) as fh:
                txs = [json.loads(l) for l in fh if l.strip()]
        else:
            if not helius_key:
                raise ValueError("Add a Helius API key in Settings first (free at helius.dev)")
            progress(phase="fetch", done=0, total=max_tx, message="Downloading transactions from Helius…")
            txs = await fetch_history(session, wallet, helius_key, since, max_tx, progress)
            with open(os.path.join(out_dir, "raw_transactions.jsonl"), "w") as fh:
                for t in txs:
                    fh.write(json.dumps(t) + "\n")
        progress(phase="rebuild", done=0, total=1, message="Rebuilding trades…")
        fills, skipped = build_fills(txs, wallet)
        mints = {f["mint"] for f in fills if f["side"] == "buy"}
        launch = await fetch_launch_info(session, mints, progress) if mints else {}
        trips, orphans = build_trips(fills, launch)
        profiles = {}
        if helius_key and profile_tokens and trips:
            try:
                profiles = await WT.profile(session, helius_key, wallet, trips, progress, cap=profile_cap)
            except Exception as e:                       # profiling is a bonus; never fail the report on it
                log.warning("token profiling failed: %s", e)
    for t in trips:                                       # fill what the launch lookup missed
        pr = profiles.get(t["mint"]) or {}
        if not t["symbol"] and pr.get("symbol"):
            t["symbol"] = pr["symbol"]
        if t["entry_delay_s"] is None and pr.get("created_ts"):
            t["entry_delay_s"] = t["first_buy_ts"] - pr["created_ts"]
    s = summarize(wallet, trips, fills, skipped, orphans, days, len(txs), max_tx)
    if profiles:
        s["tokens"] = WT.analyze_profiles(trips, profiles)
        WT.write_profiles(out_dir, trips, profiles)
    write_outputs(out_dir, s, trips, fills, launch)
    progress(phase="done", done=1, total=1, message="Done")
    return s, out_dir


def write_outputs(out_dir, s, trips, fills, launch):
    with open(os.path.join(out_dir, "summary.json"), "w") as fh:
        json.dump(s, fh, indent=2)
    with open(os.path.join(out_dir, "summary.txt"), "w", encoding="utf-8") as fh:
        fh.write(summary_text(s))
    cols = ["first_buy_utc", "symbol", "mint", "status", "pnl_sol", "pnl_pct", "hold_s", "sol_in", "sol_out",
            "first_buy_sol", "n_buys", "n_sells", "entry_mcap_sol", "entry_delay_s", "buy_source", "is_pump",
            "graduated", "first_sell_s", "first_sell_mult", "first_sell_frac", "max_sell_mult", "sell_style",
            "add_style", "transferred_out", "sell_steps"]
    with open(os.path.join(out_dir, "trades.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for t in trips:
            row = []
            for c in cols:
                v = t.get(c)
                if c == "sell_steps":
                    v = " | ".join(f"{fmt_s(x['t'])}:{x['mult']:.2f}x:{x['frac'] * 100:.0f}%" for x in v
                                   if x["mult"] is not None and x["frac"] is not None)
                row.append(_r(v, 6) if isinstance(v, float) else v)
            w.writerow(row)
    with open(os.path.join(out_dir, "fills.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["utc", "side", "mint", "symbol", "sol", "tokens", "price_sol", "source", "signature"])
        for f in fills:
            w.writerow([_iso(f["ts"]), f["side"], f["mint"], (launch.get(f["mint"]) or {}).get("symbol", ""),
                        _r(f["sol"], 6), _r(f["tokens"], 2), f"{f['price']:.3e}" if f["price"] else "",
                        f["source"], f["sig"]])
    # one file to send back for review
    zpath = os.path.join(out_dir, f"wallet_report_{s['wallet'][:8]}.zip")
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        for name in ("summary.txt", "summary.json", "trades.csv", "fills.csv", "token_profiles.csv", "raw_transactions.jsonl"):
            p = os.path.join(out_dir, name)
            if os.path.exists(p) and os.path.getsize(p) < 60_000_000:
                z.write(p, name)
    return zpath
