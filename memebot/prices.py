"""Live SOL/USD price for showing market caps in dollars (display only - never used for trading)."""
import asyncio
import logging
import time

import aiohttp

log = logging.getLogger("memebot")
SOL_MINT = "So11111111111111111111111111111111111111112"
SOURCES = [
    ("Jupiter", f"https://lite-api.jup.ag/price/v3?ids={SOL_MINT}",
     lambda j: j[SOL_MINT]["usdPrice"]),
    ("CoinGecko", "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
     lambda j: j["solana"]["usd"]),
]


class SolPrice:
    def __init__(self):
        self.usd: float | None = None
        self.updated: float = 0.0
        self.source = ""

    async def run(self, every=60):
        async with aiohttp.ClientSession(headers={"User-Agent": "momentum-paper-bot"}) as session:
            while True:
                for name, url, pick in SOURCES:
                    try:
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                            if r.status == 200:
                                price = float(pick(await r.json(content_type=None)))
                                if price > 0:
                                    self.usd, self.updated, self.source = price, time.time(), name
                                    break
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.debug("SOL price from %s failed: %s", name, e)
                await asyncio.sleep(every if self.usd else 15)
