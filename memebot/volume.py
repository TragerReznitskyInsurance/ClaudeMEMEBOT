"""
Recent trading volume of one coin: SOL bought vs sold, number of different buyers / sellers, and the biggest
buyer's share, over the last few minutes. One Helius Enhanced-Transactions call (the coin's latest 100 trades).
"""
from __future__ import annotations

import time

import aiohttp

from memebot import wallet_tokens as WT


async def recent_volume(session, key, mint, window_s=300):
    try:
        async with session.get(WT.enhanced_url(mint), params={"api-key": key, "limit": "100"},
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                return {"error": f"HTTP {r.status}"}
            txs = await r.json(content_type=None)
    except Exception as e:
        return {"error": type(e).__name__}
    if not isinstance(txs, list):
        return {"error": "bad answer"}
    now = time.time()
    buy_sol = sell_sol = 0.0
    buyers, sellers, per_buyer = set(), set(), {}
    oldest = now
    n = 0
    for tx in txs:
        ts = tx.get("timestamp") or 0
        oldest = min(oldest, ts or now)
        if not ts or now - ts > window_s:
            continue
        tr = WT._trade_of(tx, mint)
        if not tr:
            continue
        who, side, sol, _ = tr
        n += 1
        if side == "buy":
            buy_sol += sol
            buyers.add(who)
            per_buyer[who] = per_buyer.get(who, 0.0) + sol
        else:
            sell_sol += sol
            sellers.add(who)
    return dict(window_s=window_s, trades=n, buy_sol=round(buy_sol, 3), sell_sol=round(sell_sol, 3),
                net_sol=round(buy_sol - sell_sol, 3), buyers=len(buyers), sellers=len(sellers),
                top_buyer_pct=round(max(per_buyer.values()) / buy_sol * 100, 1) if buy_sol > 0 else None,
                covered_s=round(now - oldest), partial=len(txs) >= 100 and now - oldest < window_s)
