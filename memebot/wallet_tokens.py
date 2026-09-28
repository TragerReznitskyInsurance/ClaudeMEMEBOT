"""
Token profiler for Wallet Lab: what did each token look like at the moment the
wallet bought it, and what do the tokens have in common?

Per token (Helius, same free key):
  * metadata: name, symbol, description, X / tweet / Telegram / website links
    (DAS getAssetBatch + the token's metadata JSON)
  * its first 100 trades (Enhanced Transactions, oldest first): creator, launch
    time, creator's buy, and everything that happened before the wallet's entry:
    buyers, volume, whether the creator had already sold, whether the wallet was first
Then every feature is compared across winners and losers.
"""
from __future__ import annotations

import asyncio
import csv
import os
import re
import statistics as st
from collections import Counter, defaultdict

import aiohttp

WSOL = "So11111111111111111111111111111111111111112"
V_SOL0, V_TOK0 = 30.0, 1_073_000_000.0
STOP = {"the", "and", "coin", "token", "sol", "solana", "for", "you", "this", "that", "with", "pump", "fun", "meme",
        "of", "on", "in", "to", "is", "it", "my", "a", "an", "be", "we", "are", "not", "just", "who", "what"}


def rpc_url(key):
    return os.environ.get("MOMENTUM_HELIUS_RPC", "https://mainnet.helius-rpc.com/?api-key={key}").format(key=key)


def enhanced_url(addr):
    return os.environ.get("MOMENTUM_HELIUS_URL", "https://api.helius.xyz/v0/addresses/{addr}/transactions").format(addr=addr)


def _dev_pct(sol):
    if not sol or sol <= 0:
        return 0.0
    tok = V_TOK0 - V_SOL0 * V_TOK0 / (V_SOL0 + sol)
    return tok / 1e9 * 100


# ───────────────────────────────────────────────────────── fetching
async def fetch_metadata(session, key, mints, progress):
    out = {m: {} for m in mints}
    mints = list(mints)
    for i in range(0, len(mints), 100):
        chunk = mints[i:i + 100]
        try:
            async with session.post(rpc_url(key), json={"jsonrpc": "2.0", "id": 1, "method": "getAssetBatch",
                                                         "params": {"ids": chunk}},
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                j = await r.json(content_type=None)
            for a in (j.get("result") or []):
                if not a:
                    continue
                c = a.get("content") or {}
                md = c.get("metadata") or {}
                out[a["id"]] = dict(name=md.get("name") or "", symbol=md.get("symbol") or "",
                                    description=md.get("description") or "", json_uri=c.get("json_uri") or "",
                                    external_url=(c.get("links") or {}).get("external_url") or "")
        except Exception:
            pass
        progress(phase="profile", done=min(i + 100, len(mints)), total=len(mints) * 3,
                 message=f"Token metadata {min(i + 100, len(mints))}/{len(mints)}")
    # socials live in the off-chain metadata JSON (pump.fun puts twitter/telegram/website there)
    sem = asyncio.Semaphore(8)
    done = 0

    async def one(m):
        nonlocal done
        uri = out[m].get("json_uri")
        if uri:
            async with sem:
                try:
                    async with session.get(uri, timeout=aiohttp.ClientTimeout(total=6)) as r:
                        if r.status == 200:
                            j = await r.json(content_type=None)
                            if isinstance(j, dict):
                                for k in ("twitter", "telegram", "website", "description"):
                                    v = j.get(k)
                                    if isinstance(v, str) and v.strip():
                                        out[m][k] = v.strip()
                except Exception:
                    pass
        done += 1
        if done % 25 == 0 or done == len(mints):
            progress(phase="profile", done=len(mints) + done, total=len(mints) * 3,
                     message=f"Token links {done}/{len(mints)}")

    await asyncio.gather(*(one(m) for m in mints))
    return out


async def fetch_early(session, key, mint):
    """First 100 transactions of a token, oldest first (None if unavailable)."""
    params = {"api-key": key, "limit": "100", "sort-order": "asc"}
    for attempt in range(4):
        try:
            async with session.get(enhanced_url(mint), params=params, timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                if r.status != 200:
                    return None
                data = await r.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(1)
            continue
        if not isinstance(data, list) or not data:
            return None
        ts = [t.get("timestamp") or 0 for t in data]
        if ts != sorted(ts):
            return None          # API ignored sort-order: can't trust this as "earliest"
        return data
    return None


def _trade_of(tx, mint):
    """(trader, side, sol, tokens) for the fee payer's trade of `mint` in this tx, or None."""
    who = tx.get("feePayer")
    if not who or tx.get("transactionError"):
        return None
    sol, tok = 0.0, 0.0
    for ad in tx.get("accountData") or []:
        if ad.get("account") == who:
            sol += (ad.get("nativeBalanceChange") or 0) / 1e9
        for tb in ad.get("tokenBalanceChanges") or []:
            if tb.get("userAccount") != who:
                continue
            raw = tb.get("rawTokenAmount") or {}
            try:
                amt = int(raw.get("tokenAmount", 0)) / 10 ** int(raw.get("decimals", 0))
            except (TypeError, ValueError):
                continue
            if tb.get("mint") == mint:
                tok += amt
            elif tb.get("mint") == WSOL:
                sol += amt
    if tok > 0:
        return who, "buy", -sol, tok
    if tok < 0:
        return who, "sell", sol, -tok
    return None


def snapshot(early, mint, wallet, entry_sig, entry_ts):
    """What had happened on this token before the wallet's first buy."""
    if not early:
        return {}
    first = early[0]
    creator = first.get("feePayer") or ""
    created = first.get("timestamp")
    trades = [(tx, _trade_of(tx, mint)) for tx in early]
    dev_buy = sum(t[2] for tx, t in trades[:3] if t and t[0] == creator and t[1] == "buy")
    idx = next((i for i, tx in enumerate(early) if tx.get("signature") == entry_sig), None)
    if idx is None:
        if len(early) >= 100 and (early[-1].get("timestamp") or 0) <= entry_ts:
            return dict(creator=creator, created_ts=created, dev_buy_sol=dev_buy, busy=True,
                        age_s=entry_ts - created if created else None, is_creator=creator == wallet)
        idx = sum(1 for tx in early if (tx.get("timestamp") or 0) < entry_ts)
    before = [t for tx, t in trades[:idx] if t]
    buyers = {t[0] for t in before if t[1] == "buy" and t[0] not in (creator, wallet)}
    return dict(
        creator=creator, created_ts=created, dev_buy_sol=dev_buy, busy=False,
        age_s=(entry_ts - created) if created else None,
        buyers_before=len(buyers),
        buy_sol_before=sum(t[2] for t in before if t[1] == "buy" and t[0] != creator),
        sell_sol_before=sum(t[2] for t in before if t[1] == "sell"),
        dev_sold_before=any(t[0] == creator and t[1] == "sell" for t in before),
        first_buyer=len(buyers) == 0,
        is_creator=creator == wallet,
        trades_before=len(before),
    )


async def profile(session, key, wallet, trips, progress, cap=600):
    firsts = {}
    for t in sorted(trips, key=lambda t: t["first_buy_ts"]):
        firsts.setdefault(t["mint"], t)
    mints = sorted(firsts, key=lambda m: -firsts[m]["first_buy_ts"])[:cap]
    meta = await fetch_metadata(session, key, mints, progress)
    snaps = {}
    sem = asyncio.Semaphore(5)
    done = 0

    async def one(m):
        nonlocal done
        async with sem:
            early = await fetch_early(session, key, m)
            await asyncio.sleep(0.05)
        t = firsts[m]
        snaps[m] = snapshot(early, m, wallet, t.get("first_buy_sig"), t["first_buy_ts"])
        done += 1
        if done % 10 == 0 or done == len(mints):
            progress(phase="profile", done=len(mints) * 2 + done, total=len(mints) * 3,
                     message=f"Token histories {done}/{len(mints)}")

    await asyncio.gather(*(one(m) for m in mints))
    return {m: dict(meta.get(m) or {}, **(snaps.get(m) or {})) for m in mints}


# ───────────────────────────────────────────────────────── analysis
def _links(p):
    tw = (p.get("twitter") or "") + " " + (p.get("website") or "") + " " + (p.get("external_url") or "")
    tweet = bool(re.search(r"(x|twitter)\.com/[^/\s]+/status/\d+", tw))
    has_x = bool(re.search(r"(x|twitter)\.com/", tw))
    return dict(tweet=tweet, x_account=has_x and not tweet, telegram=bool(p.get("telegram")),
                website=bool(p.get("website")) and not re.search(r"(x|twitter)\.com", p.get("website") or ""),
                no_links=not (has_x or p.get("telegram") or p.get("website")))


def _bucket(v, edges, labels):
    if v is None:
        return None
    for e, lab in zip(edges, labels):
        if v <= e:
            return lab
    return labels[-1]


def analyze_profiles(trips, profiles):
    """Joins profiles to each token's FIRST trade and compares winners vs losers per feature."""
    first_trip = {}
    for t in sorted(trips, key=lambda t: t["first_buy_ts"]):
        if t["mint"] in profiles and t["mint"] not in first_trip and t["status"] == "closed" and t["pnl_sol"] is not None:
            first_trip[t["mint"]] = t
    rows = []
    for m, t in first_trip.items():
        p = profiles[m]
        rows.append(dict(t=t, p=p, win=t["pnl_sol"] > 0, pnl=t["pnl_sol"], **_links(p)))
    n = len(rows)
    if not n:
        return {"profiled": len(profiles), "compared": 0}

    def table(key_fn, order):
        g = defaultdict(list)
        for r in rows:
            k = key_fn(r)
            if k is not None:
                g[k].append(r)
        tot = sum(len(v) for v in g.values()) or 1
        return [dict(label=k, n=len(g[k]), share=round(len(g[k]) / tot * 100, 1),
                     win_rate=round(sum(r["win"] for r in g[k]) / len(g[k]) * 100, 1),
                     avg_pnl=round(sum(r["pnl"] for r in g[k]) / len(g[k]), 3))
                for k in order if g.get(k)]

    have_hist = [r for r in rows if r["p"].get("created_ts")]
    creators = Counter(r["p"].get("creator") for r in rows if r["p"].get("creator"))
    repeat = {c for c, k in creators.items() if k >= 2}
    words = Counter()
    for r in rows:
        text = r['p'].get('name', '')
        words.update(w for w in re.findall(r"[a-z]{3,}", text.lower()) if w not in STOP)
    win_words, lose_words = Counter(), Counter()
    for r in rows:
        ws = set(w for w in re.findall(r"[a-z]{3,}", f"{r['p'].get('name', '')} {r['p'].get('description', '')}".lower())
                 if w not in STOP)
        (win_words if r["win"] else lose_words).update(ws)

    return dict(
        profiled=len(profiles), compared=n, with_history=len(have_hist),
        wallet_is_creator=sum(1 for r in rows if r["p"].get("is_creator")),
        age=table(lambda r: None if r["p"].get("age_s") is None else
                  _bucket(r["p"]["age_s"], [10, 60, 300, 1800, 3600 * 6], ["<10s", "10s-1m", "1-5m", "5-30m", "30m-6h", ">6h"]),
                  ["<10s", "10s-1m", "1-5m", "5-30m", "30m-6h", ">6h"]),
        buyers_before=table(lambda r: "100+ trades already" if r["p"].get("busy") else
                            None if "buyers_before" not in r["p"] else
                            _bucket(r["p"]["buyers_before"], [0, 5, 20, 60], ["first buyer", "1-5", "6-20", "21-60", "60+"]),
                            ["first buyer", "1-5", "6-20", "21-60", "60+", "100+ trades already"]),
        dev_buy=table(lambda r: None if not r["p"].get("created_ts") else
                      _bucket(_dev_pct(r["p"].get("dev_buy_sol")), [0.01, 2, 5, 10], ["none", "<2%", "2-5%", "5-10%", ">10%"]),
                      ["none", "<2%", "2-5%", "5-10%", ">10%"]),
        dev_sold=table(lambda r: None if "dev_sold_before" not in r["p"] else
                       ("creator already sold" if r["p"]["dev_sold_before"] else "creator still holding"),
                       ["creator still holding", "creator already sold"]),
        links=table(lambda r: "links a specific tweet" if r["tweet"] else "X account" if r["x_account"]
                    else "Telegram/website only" if (r["telegram"] or r["website"]) else "no links",
                    ["links a specific tweet", "X account", "Telegram/website only", "no links"]),
        creator_repeat=table(lambda r: None if not r["p"].get("creator") else
                             ("creator it bought from before" if r["p"]["creator"] in repeat else "one-off creator"),
                             ["creator it bought from before", "one-off creator"]),
        top_creators=[dict(creator=c, tokens=k,
                           win_rate=round(sum(r["win"] for r in rows if r["p"].get("creator") == c) / k * 100))
                      for c, k in creators.most_common(8) if k >= 2],
        top_words=[w for w, _ in words.most_common(25)],
        words_more_in_winners=[w for w, c in sorted(win_words.items(), key=lambda kv: -kv[1])
                               if c >= 5 and c / (lose_words.get(w, 0) + 1) >= 2.5][:12],
        median_buyers_before=st.median([r["p"]["buyers_before"] for r in rows if "buyers_before" in r["p"]] or [0]),
        median_age_s=st.median([r["p"]["age_s"] for r in rows if r["p"].get("age_s") is not None] or [0]),
    )


def write_profiles(out_dir, trips, profiles):
    firsts = {}
    for t in sorted(trips, key=lambda t: t["first_buy_ts"]):
        firsts.setdefault(t["mint"], t)
    cols = ["mint", "name", "symbol", "pnl_pct", "age_s", "buyers_before", "buy_sol_before", "sell_sol_before",
            "trades_before", "first_buyer", "busy", "dev_buy_sol", "dev_sold_before", "is_creator", "creator",
            "twitter", "telegram", "website", "description"]
    with open(os.path.join(out_dir, "token_profiles.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for m, p in profiles.items():
            t = firsts.get(m) or {}
            row = []
            for c in cols:
                v = m if c == "mint" else t.get("pnl_pct") if c == "pnl_pct" else p.get(c, "")
                if isinstance(v, float):
                    v = round(v, 4)
                if isinstance(v, str):
                    v = v.replace("\n", " ")[:300]
                row.append(v)
            w.writerow(row)
