"""
Bundle / sniper check for one coin: who bought in its first seconds, how much of the supply they
got, whether they've sold, and whether they are still among the top holders.

  bundle  = buys that landed in the SAME block as the coin's creation (the creator's own helper
            wallets, submitted together - nobody else can see a coin before its creation block)
  snipers = buys in the next few seconds (bots watching for new launches)

Uses the bot's Helius key: one Enhanced Transactions call (the first 100 transactions) plus
two cheap RPC calls. Read-only.
"""
from __future__ import annotations

import aiohttp

from memebot import wallet_tokens as WT
from memebot.chain import curve_address
from memebot.snapshots import curve_token_accounts

SUPPLY = 1_000_000_000
SNIPE_S = 3


async def _rpc(session, key, method, params):
    async with session.post(WT.rpc_url(key), json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                            timeout=aiohttp.ClientTimeout(total=15)) as r:
        j = await r.json(content_type=None)
    if "error" in j:
        raise RuntimeError(str(j["error"])[:120])
    return j.get("result")


async def check(session, key, mint) -> dict:
    early = await WT.fetch_early(session, key, mint)
    if not early:
        return {"error": "couldn't read this coin's first transactions (too new for the indexer, or not a pump.fun coin)"}
    t0, slot0 = early[0].get("timestamp"), early[0].get("slot")
    creator = early[0].get("feePayer")
    w = {}                                  # wallet -> dict(role, bought, sol, sold)
    first_100_span = (early[-1].get("timestamp") or t0) - t0
    for tx in early:
        tr = WT._trade_of(tx, mint)
        if not tr:
            continue
        who, side, sol, tok = tr
        if who not in w:
            if who == creator:
                role = "creator"
            elif tx.get("slot") == slot0:
                role = "bundle"
            elif (tx.get("timestamp") or t0) - t0 <= SNIPE_S:
                role = "sniper"
            else:
                role = "other"
            w[who] = dict(role=role, bought=0.0, sol=0.0, sold=0.0, first_s=(tx.get("timestamp") or t0) - t0)
        if side == "buy":
            w[who]["bought"] += tok
            w[who]["sol"] += sol
        else:
            w[who]["sold"] += tok

    def group(role):
        g = [x for x in w.values() if x["role"] == role]
        bought = sum(x["bought"] for x in g)
        return dict(wallets=len(g), sol=round(sum(x["sol"] for x in g), 2),
                    supply_pct=round(bought / SUPPLY * 100, 1),
                    sold_pct=round(sum(min(x["sold"], x["bought"]) for x in g) / bought * 100) if bought else 0)

    out = dict(mint=mint, creator=creator, launch_ts=t0, txs_read=len(early), span_s=first_100_span,
               creator_buy=group("creator"), bundle=group("bundle"), snipers=group("sniper"))
    # launch-block market cap: supply sold in the creation block
    in_block = sum(x["bought"] for x in w.values() if x["role"] in ("creator", "bundle"))
    vtok = 1_073_000_000 - in_block
    out["mcap_after_block_sol"] = round((30 * 1_073_000_000 / vtok) / vtok * SUPPLY, 1) if vtok > 0 else None

    # who holds the most now - are the launch wallets still in there?
    try:
        big = await _rpc(session, key, "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        vals = (big or {}).get("value") or []
        curve = curve_token_accounts(mint)
        vals = [v for v in vals if v["address"] not in curve][:10]
        accs = await _rpc(session, key, "getMultipleAccounts", [[v["address"] for v in vals], {"encoding": "jsonParsed"}])
        top = []
        for v, a in zip(vals, (accs or {}).get("value") or []):
            owner = (((a or {}).get("data") or {}).get("parsed") or {}).get("info", {}).get("owner")
            amt = float(v.get("uiAmountString") or v.get("uiAmount") or 0)
            top.append(dict(owner=owner, pct=round(amt / SUPPLY * 100, 2),
                            role=(w.get(owner) or {}).get("role", "later buyer")))
        out["top10"] = top
        out["top10_pct"] = round(sum(x["pct"] for x in top), 1)
        out["top10_launch_pct"] = round(sum(x["pct"] for x in top if x["role"] in ("creator", "bundle", "sniper")), 1)
    except Exception as e:
        out["top_error"] = str(e)[:100]
    try:
        acc = await _rpc(session, key, "getAccountInfo", [curve_address(mint), {"encoding": "base64"}])
        import base64
        import struct
        d = base64.b64decode(((acc or {}).get("value") or {}).get("data", [""])[0] or b"")
        if len(d) >= 49:
            vt, vs = struct.unpack_from("<QQ", d, 8)
            out["graduated"] = bool(d[48])
            out["mayhem"] = bool(d[81]) if len(d) > 81 else False
            out["mcap_now_sol"] = None if d[48] or not vt else round((vs / 1e9) / (vt / 1e6) * SUPPLY, 1)
    except Exception:
        pass
    launch = out["bundle"]["supply_pct"] + out["snipers"]["supply_pct"] + out["creator_buy"]["supply_pct"]
    out["verdict"] = ("heavy bundle" if out["bundle"]["supply_pct"] >= 15 else
                      "sniped hard" if out["snipers"]["supply_pct"] >= 20 else
                      "moderate launch buying" if launch >= 15 else "fairly clean launch")
    return out
