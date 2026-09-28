"""
Token names for display. The wallet-trade feed and the chain only give a token's
address, so copied tokens used to show as the first letters of it ("2WN4X").
This looks the real ticker/name up (Helius DAS getAssetBatch, same free key, up to
100 tokens per call), caches them in data/token_names.json, and lets the app swap
the placeholder everywhere it appears.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time

import aiohttp

log = logging.getLogger("memebot")
RPC = "https://mainnet.helius-rpc.com/?api-key={key}"


def is_placeholder(symbol: str, mint: str) -> bool:
    """True when `symbol` is just the start of the address (what we show before the name is known)."""
    s = (symbol or "").strip()
    return not s or (len(s) <= 6 and mint.upper().startswith(s.upper()))


def clean(text: str, n: int) -> str:
    text = re.sub(r"[\x00-\x1f,\"]+", " ", str(text or "")).strip()   # commas would break the CSV
    return text[:n]


class TokenNames:
    def __init__(self, path: str, key_getter):
        self.path = path
        self._key = key_getter
        self.names: dict[str, dict] = {}          # mint -> {symbol, name}
        self.tries: dict[str, tuple[int, float]] = {}   # mint -> (attempts, next try ts)
        self.session: aiohttp.ClientSession | None = None
        try:
            with open(path) as fh:
                self.names = json.load(fh)
        except (OSError, ValueError):
            pass

    def get(self, mint):
        return self.names.get(mint)

    def _save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(self.names, fh)
            os.replace(tmp, self.path)
        except OSError:
            pass

    def _due(self, mint, now):
        n, nxt = self.tries.get(mint, (0, 0.0))
        return n < 12 and now >= nxt

    async def lookup(self, mints):
        """Fetch names for `mints` (those not cached). Returns {mint: {symbol, name}} of the new ones."""
        key = self._key()
        now = time.time()
        todo = [m for m in dict.fromkeys(mints) if m not in self.names and self._due(m, now)][:100]
        if not key or not todo:
            return {}
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        async with self.session.post(RPC.format(key=key), json={"jsonrpc": "2.0", "id": 1, "method": "getAssetBatch",
                                                                "params": {"ids": todo}},
                                     timeout=aiohttp.ClientTimeout(total=15)) as r:
            j = await r.json(content_type=None)
        found = {}
        for a in (j.get("result") or []):
            if not a:
                continue
            md = ((a.get("content") or {}).get("metadata")) or {}
            sym, name = clean(md.get("symbol"), 14), clean(md.get("name"), 40)
            if sym or name:
                found[a["id"]] = dict(symbol=sym or name[:14], name=name)
        for m in todo:
            if m in found:
                self.tries.pop(m, None)
            else:                                   # brand-new tokens can take a moment to be indexed
                n = self.tries.get(m, (0, 0.0))[0] + 1
                self.tries[m] = (n, now + min(300, 10 * 2 ** n))
        if found:
            self.names.update(found)
            if len(self.names) > 20000:
                for k in list(self.names)[:5000]:
                    self.names.pop(k)
            self._save()
        return found

    async def run(self, wanted, apply, every=2.0):
        """Loop: `wanted()` -> mints showing placeholders; `apply(mint, info)` swaps the name in."""
        while True:
            try:
                mints = wanted()
                for m in [m for m in mints if m in self.names]:
                    apply(m, self.names[m])         # known already (e.g. from an earlier session)
                missing = [m for m in mints if m not in self.names]
                if missing:
                    for m, info in (await self.lookup(missing)).items():
                        apply(m, info)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("token name lookup failed: %s", e)
            await asyncio.sleep(every)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
