"""
Token snapshots: what a token looked like at the moment a followed wallet bought it,
next to ordinary tokens at the same stage that it did NOT buy - so its picking rules
can be worked out and turned into a "lookalike" strategy.

Two kinds of records (data/snapshots.jsonl, kept across sessions):
  wallet_buy  a followed wallet's first buy of a token
  crossed     a comparison token: one the bot was watching, the moment its market cap
              first rose through `snapshots.cross_mcap_sol` (wallet #1 buys around 44 SOL)
Each gets the same features, then outcome checks 30 min and 2 h later
(still on the curve? graduated? market cap), so winners vs losers can be compared too.

Credits: comparison tokens use the free live feed plus one cheap RPC call. Token
metadata and outcomes are fetched in batches. Only wallet buys of tokens the bot
wasn't already watching use the Enhanced Transactions API (one call each).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import struct
import time
import base64
from collections import Counter, deque

import aiohttp
from solders.pubkey import Pubkey

from memebot.chain import curve_address
from memebot import wallet_tokens as WT
from memebot.social import Social

log = logging.getLogger("memebot")
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"
ATA_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
TOKEN_PROGRAMS = [Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
                  Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")]
JUP = "https://lite-api.jup.ag/price/v3?ids={ids}"
SUPPLY = 1_000_000_000
CHECKS_MIN = (30, 120)


def curve_token_accounts(mint: str) -> set[str]:
    """The bonding curve's own token account(s) - excluded from 'top holders'."""
    curve, m = Pubkey.from_string(curve_address(mint)), Pubkey.from_string(mint)
    return {str(Pubkey.find_program_address([bytes(curve), bytes(p), bytes(m)], ATA_PROGRAM)[0]) for p in TOKEN_PROGRAMS}


def stream_features(t, ts: float) -> dict:
    """Features from the bot's own live data (TokenState), same names as the history version."""
    non_dev = sum(t.buy_by_wallet.values())
    top = max(t.buy_by_wallet.values(), default=0.0)
    r60 = [x for x in t.recent if ts - x[0] <= 60]
    b60, s60 = sum(1 for x in r60 if x[1] == "buy"), sum(1 for x in r60 if x[1] == "sell")
    m30 = t.mcap_at(ts - 30)
    m120 = t.mcap_at(ts - 120)
    r120 = [x for x in t.recent if ts - x[0] <= 120]
    return dict(
        source="stream", age_s=round(ts - t.created_ts, 1) if t.created_ts else None,
        buyers=len(t.buyers), buys=t.buys, sells=t.sells, trades=t.buys + t.sells,
        buy_sol=round(non_dev, 3), sell_sol=round(t.sell_vol, 3),
        dev_buy_pct=round(t.dev_buy_pct, 2) if t.dev_buy_pct is not None else None, dev_sold=t.dev_sold,
        top_buyer_share_pct=round(top / non_dev * 100, 1) if non_dev else None,
        new_buyers_20s=sum(1 for v in t.buyer_first.values() if ts - v <= 20),
        buys_60s=b60, sells_60s=s60,
        **recent_block([(x[0], x[1], x[2]) for x in r120], ts,
                       (t.mcap / m120 - 1) * 100 if t.mcap and m120 and ts - t.created_ts >= 120 else None),
        momentum_30s_pct=round((t.mcap / m30 - 1) * 100, 1) if t.mcap and m30 else None,
        peak_mcap_sol=round(t.peak_mcap, 1),
        drawdown_pct=round((1 - t.mcap / t.peak_mcap) * 100, 1) if t.mcap and t.peak_mcap else None,
        security=t.sec_state or None, creator=t.creator or None,
    )


def recent_block(trades, ts, momentum_2m):
    """Activity in the 2 minutes before `ts` - same definition for live-feed and history data.
    trades: [(ts, side, sol)]"""
    w = [x for x in trades if 0 <= ts - x[0] <= 120]
    b = [x for x in w if x[1] == "buy"]
    s = [x for x in w if x[1] == "sell"]
    return dict(buys_2m=len(b), sells_2m=len(s), buy_sol_2m=round(sum(x[2] for x in b), 3),
                sell_sol_2m=round(sum(x[2] for x in s), 3),
                momentum_2m_pct=round(momentum_2m, 1) if momentum_2m is not None else None)


async def fetch_before(session, key, mint, before_sig):
    """The 100 trades of `mint` right before `before_sig` (newest first)."""
    params = {"api-key": key, "limit": "100", "before-signature": before_sig}
    for attempt in range(3):
        try:
            async with session.get(WT.enhanced_url(mint), params=params, timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                if r.status != 200:
                    return None
                data = await r.json(content_type=None)
                return data if isinstance(data, list) else None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(1)
    return None


def recent_from_history(txs, mint, ts):
    trades = []
    for tx in txs or []:
        t = WT._trade_of(tx, mint)
        if t and tx.get("timestamp"):
            trades.append((tx["timestamp"], t[1], t[2], t[2] / t[3] if t[3] else None))
    trades.sort()
    w = [x for x in trades if 0 <= ts - x[0] <= 120 and x[3]]
    mom = None
    if len(w) >= 2 and trades and ts - trades[0][0] >= 110:      # need history reaching back ~2 minutes
        mom = (w[-1][3] / w[0][3] - 1) * 100
    b60 = [x for x in trades if 0 <= ts - x[0] <= 60]
    return dict(buys_60s=sum(1 for x in b60 if x[1] == "buy"), sells_60s=sum(1 for x in b60 if x[1] == "sell"),
                **recent_block([x[:3] for x in trades], ts, mom))


def history_features(early, mint, wallet, sig, ts) -> dict:
    s = WT.snapshot(early, mint, wallet, sig, ts)
    if not s:
        return {"source": "history-missing"}
    return dict(source="history", age_s=round(s["age_s"], 1) if s.get("age_s") is not None else None,
                buyers=s.get("buyers_before"), trades=s.get("trades_before"),
                buy_sol=round(s["buy_sol_before"], 3) if s.get("buy_sol_before") is not None else None,
                sell_sol=round(s["sell_sol_before"], 3) if s.get("sell_sol_before") is not None else None,
                dev_buy_pct=round(WT._dev_pct(s.get("dev_buy_sol")), 2), dev_sold=s.get("dev_sold_before"),
                first_buyer=s.get("first_buyer"), busy=s.get("busy"), creator=s.get("creator") or None)


class SnapshotRecorder:
    def __init__(self, path: str, key: str, cfg_getter, sol_usd_getter):
        self.path = path
        self.url = RPC.format(key=key)
        self.key = key
        self._cfg = cfg_getter
        self._usd = sol_usd_getter
        self.session: aiohttp.ClientSession | None = None
        self.pending: dict[str, dict] = {}     # id -> {mint, ts, due: [minutes...]}
        self.meta_q: set[str] = set()
        self.meta_done: set[str] = set()
        self.control_times: deque = deque()
        self.wallet_times: deque = deque()          # wallet-buy snapshots in the last hour (Helius budget)
        self.stats = Counter()
        self.last_error = ""
        self.social = Social()                 # pump.fun comment counts (free, best effort)
        self.history_delay = 45                # s to wait before reading a token's history (indexer lag)
        self._load()

    # ------------------------------------------------------------------ file
    def _load(self):
        """Pick up outcome checks that were still due when the app last stopped."""
        if not os.path.exists(self.path):
            return
        snaps, done = {}, set()
        try:
            with open(self.path, encoding="utf-8", errors="replace") as fh:
                for ln in fh:
                    try:
                        r = json.loads(ln)
                    except ValueError:
                        continue
                    if r.get("type") == "snapshot":
                        snaps[r["id"]] = r
                        self.stats[r["kind"]] += 1
                    elif r.get("type") == "outcome":
                        done.add((r["id"], r["after_min"]))
                    elif r.get("type") == "meta":
                        self.meta_done.add(r["mint"])
        except OSError:
            return
        now = time.time()
        for sid, r in snaps.items():
            due = [m for m in CHECKS_MIN if (sid, m) not in done and now - r["ts"] < m * 60 + 3600]
            if due:
                self.pending[sid] = dict(mint=r["mint"], ts=r["ts"], due=due)

    def _write(self, rec: dict):
        try:
            with open(self.path, "a", encoding="utf-8", errors="replace") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")
        except OSError as e:
            self.last_error = str(e)

    def cfg(self):
        return self._cfg().get("snapshots") or {}

    def enabled(self):
        return bool(self.cfg().get("enabled", True))

    async def _sess(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _rpc(self, method, params):
        s = await self._sess()
        async with s.post(self.url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                          timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json(content_type=None)
        if "error" in j:
            raise RuntimeError(str(j["error"])[:120])
        return j.get("result")

    # ------------------------------------------------------------------ hooks (called by the engine)
    def wallet_buy(self, wallet, mint, ts, wallet_sol, wallet_px, mcap_sol, t, sig=None):
        if not self.enabled():
            return
        cap = int(self.cfg().get("wallet_per_hour", 150))   # a followed wallet that buys hundreds of coins an hour
        while self.wallet_times and ts - self.wallet_times[0] > 3600:   # must not use up the Helius allowance
            self.wallet_times.popleft()
        if cap and len(self.wallet_times) >= cap:
            self.stats["wallet_skipped_cap"] = self.stats.get("wallet_skipped_cap", 0) + 1
            return
        self.wallet_times.append(ts)
        feats = stream_features(t, ts) if (t is not None and t.creator and t.created_ts and t.buys + t.sells) else None
        base = dict(kind="wallet_buy", wallet=wallet, mint=mint, symbol=getattr(t, "symbol", "") or mint[:5], ts=ts,
                    mcap_sol=round(mcap_sol, 1) if mcap_sol else None, wallet_sol=round(wallet_sol or 0, 4),
                    wallet_px=wallet_px, watched=feats is not None, sig=sig)
        asyncio.get_running_loop().create_task(self._take(base, feats))

    def crossed(self, t, ts):
        if not self.enabled():
            return
        cap = int(self.cfg().get("control_per_hour", 60))
        while self.control_times and ts - self.control_times[0] > 3600:
            self.control_times.popleft()
        if cap and len(self.control_times) >= cap:
            self.stats["control_skipped_cap"] += 1
            return
        self.control_times.append(ts)
        base = dict(kind="crossed", wallet="", mint=t.mint, symbol=t.symbol, ts=ts,
                    mcap_sol=round(t.mcap, 1) if t.mcap else None, watched=True)
        asyncio.get_running_loop().create_task(self._take(base, stream_features(t, ts)))

    # ------------------------------------------------------------------ taking a snapshot
    async def _holders(self, mint):
        try:
            r = await self._rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        except Exception as e:
            self.last_error = f"holders: {e}"
            return {}
        curve = curve_token_accounts(mint)
        vals = (r or {}).get("value") or []
        in_curve = sum(float(v.get("uiAmountString") or v.get("uiAmount") or 0) for v in vals if v["address"] in curve)
        others = sorted((float(v.get("uiAmountString") or v.get("uiAmount") or 0) for v in vals if v["address"] not in curve),
                        reverse=True)
        return dict(top1_pct=round(others[0] / SUPPLY * 100, 2) if others else 0.0,
                    top10_pct=round(sum(others[:10]) / SUPPLY * 100, 2),
                    curve_supply_pct=round(in_curve / SUPPLY * 100, 1) if in_curve else None)

    async def _pf(self, mint):
        """pump.fun comment count etc., as pf_* fields (empty if pump.fun didn't answer)."""
        if not self.cfg().get("social", False):
            return {}
        return {"pf_" + k: v for k, v in (await self.social.fetch(await self._sess(), mint)).items()}

    async def _take(self, base, feats):
        try:
            holders = await self._holders(base["mint"])
            holders.update(await self._pf(base["mint"]))
            if feats is None:                                 # not watched by the bot: read its history instead
                await asyncio.sleep(self.history_delay)       # let the indexer catch up with the wallet's buy
                early = await WT.fetch_early(await self._sess(), self.key, base["mint"])
                feats = history_features(early, base["mint"], base["wallet"], base.get("sig"), base["ts"])
                if base.get("sig"):                            # busy/older coins: what was happening just before
                    before = await fetch_before(await self._sess(), self.key, base["mint"], base["sig"])
                    if before:
                        feats.update(recent_from_history(before, base["mint"], base["ts"]))
                self.stats["history_lookups"] += 1
            rec = dict(type="snapshot", id=f"{base['kind']}:{base['mint']}:{int(base['ts'])}", **base, **feats, **holders)
            self._write(rec)
            self.stats[base["kind"]] += 1
            self.pending[rec["id"]] = dict(mint=base["mint"], ts=base["ts"], due=list(CHECKS_MIN))
            if base["mint"] not in self.meta_done:
                self.meta_q.add(base["mint"])
        except Exception as e:
            self.stats["errors"] += 1
            self.last_error = str(e) or type(e).__name__

    # ------------------------------------------------------------------ batched follow-ups
    async def _do_meta(self):
        mints = sorted(self.meta_q)[:100]
        if not mints:
            return
        self.meta_q.difference_update(mints)
        noop = lambda **k: None
        meta = await WT.fetch_metadata(await self._sess(), self.key, mints, noop)
        for m in mints:
            md = meta.get(m) or {}
            links = WT._links(md)
            self._write(dict(type="meta", mint=m, name=md.get("name", ""), symbol=md.get("symbol", ""),
                             description=(md.get("description") or "")[:300], twitter_url=md.get("twitter", ""),
                             telegram_url=md.get("telegram", ""), website_url=md.get("website", ""),
                             **{"has_" + k if k in ("telegram", "website") else k: v for k, v in links.items()}))
            self.meta_done.add(m)

    async def _do_outcomes(self):
        now = time.time()
        due = [(sid, p, m) for sid, p in self.pending.items() for m in p["due"] if now - p["ts"] >= m * 60][:100]
        if not due:
            return
        mints = sorted({p["mint"] for _, p, _ in due})
        curves = [curve_address(m) for m in mints]
        state = {}
        r = await self._rpc("getMultipleAccounts", [curves, {"encoding": "base64", "commitment": "confirmed"}])
        for m, acc in zip(mints, (r or {}).get("value") or []):
            if not acc:
                state[m] = dict(on_curve=False)
                continue
            data = base64.b64decode(acc["data"][0])
            if len(data) < 49:
                state[m] = dict(on_curve=False)
                continue
            vtok, vsol = struct.unpack_from("<QQ", data, 8)
            done = bool(data[48])
            state[m] = dict(on_curve=not done, graduated=done,
                            mcap_sol=None if done or not vtok else round((vsol / 1e9) / (vtok / 1e6) * SUPPLY, 1))
        grads = [m for m in mints if state[m].get("graduated") or not state[m].get("on_curve")]
        px = self._usd()
        if grads and px:
            try:
                s = await self._sess()
                async with s.get(JUP.format(ids=",".join(grads[:50])), timeout=aiohttp.ClientTimeout(total=8)) as rr:
                    j = await rr.json(content_type=None) if rr.status == 200 else {}
                for m in grads:
                    usd = float(((j or {}).get(m) or {}).get("usdPrice") or 0)
                    if usd > 0:
                        state[m]["mcap_sol"] = round(usd / px * SUPPLY, 1)
            except Exception:
                pass
        pf = dict(zip(mints, await asyncio.gather(*(self._pf(m) for m in mints))))
        for sid, p, m in due:
            self._write(dict(type="outcome", id=sid, mint=p["mint"], after_min=m, ts=now, **state[p["mint"]],
                             **pf.get(p["mint"], {})))
            p["due"].remove(m)
            self.stats["outcomes"] += 1
            if not p["due"]:
                self.pending.pop(sid, None)

    async def run(self):
        while True:
            await asyncio.sleep(60)
            for step in (self._do_meta, self._do_outcomes):
                try:
                    await step()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.stats["errors"] += 1
                    self.last_error = f"{step.__name__[4:]}: {e}"

    def summary(self):
        return dict(wallet_buys=self.stats["wallet_buy"], compared=self.stats["crossed"], outcomes=self.stats["outcomes"],
                    waiting=len(self.pending), errors=self.stats["errors"], last_error=self.last_error,
                    cross_mcap_sol=self.cfg().get("cross_mcap_sol", 42), social=self.social.summary())

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
