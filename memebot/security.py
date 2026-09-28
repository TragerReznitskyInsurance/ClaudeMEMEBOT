"""
Pre-watch security screening.

RugCheckScreener  - live: asks RugCheck's free API about each new token before
                    the bot spends paid data watching it.
DemoScreener      - demo mode: simulated results on a virtual clock.
"""
import asyncio
import logging
import random

import aiohttp

log = logging.getLogger("memebot")
RUGCHECK_URL = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"


def judge(summary: dict, sec: dict) -> tuple[bool, str]:
    """Apply the config's rules to a RugCheck summary. Returns (ok, reason)."""
    block = [b.lower() for b in sec.get("rugcheck_block_risks", [])]
    for r in summary.get("risks") or []:
        name = str(r.get("name", ""))
        text = (name + " " + str(r.get("description", ""))).lower()
        hit = next((b for b in block if b in text), None)
        if hit:
            return False, f"rugcheck: {name.lower()}"
    max_score = sec.get("rugcheck_max_score", 0) or 0
    score = summary.get("score_normalised")
    if max_score and score is not None and score > max_score:
        return False, f"rugcheck: risk score {score} > {max_score}"
    return True, "RugCheck clean" if not summary.get("risks") else \
        f"RugCheck ok ({len(summary['risks'])} minor flag{'s' if len(summary['risks']) != 1 else ''})"


class RugCheckScreener:
    def __init__(self, engine_getter, sec_getter, concurrency=4, max_queue=40):
        self._engine = engine_getter        # callables so settings changes apply live
        self._sec = sec_getter
        self.sem = asyncio.Semaphore(concurrency)
        self.max_queue = max_queue
        self.inflight = 0
        self.session: aiohttp.ClientSession | None = None
        self.stats = {"ok": 0, "fail": 0, "error": 0}
        self.last_error = ""

    def request(self, mint: str):
        if self.inflight >= self.max_queue:
            self._error(mint, "rugcheck queue full")
            return
        self.inflight += 1
        asyncio.get_running_loop().create_task(self._check(mint))

    def _error(self, mint, why):
        self.stats["error"] += 1
        self.last_error = why
        allow = self._sec().get("rugcheck_on_error", "skip") == "allow"
        self._engine().on_screen_result(mint, allow, "rugcheck unavailable" if allow else f"rugcheck unavailable ({why})")

    # RugCheck answers HTTP 400/404 until it has indexed a brand-new mint, which
    # takes a few seconds after launch. Retry on this schedule (seconds after the
    # first try) - all well inside the 30s before the bot is allowed to buy.
    RETRY_DELAYS = [3, 4, 5, 6, 7]

    async def _check(self, mint):
        why = "no response"
        try:
            await asyncio.sleep(2)                     # give RugCheck a head start on indexing
            for delay in [0] + self.RETRY_DELAYS:
                if delay:
                    await asyncio.sleep(delay)         # wait outside the semaphore
                eng = self._engine()
                t = eng.tokens.get(mint)
                if not t or t.status in ("rejected", "closed"):
                    return                              # token already dropped for other reasons
                async with self.sem:
                    if self.session is None or self.session.closed:
                        self.session = aiohttp.ClientSession(headers={"User-Agent": "momentum-paper-bot"})
                    timeout = aiohttp.ClientTimeout(total=self._sec().get("rugcheck_timeout_s", 6))
                    try:
                        async with self.session.get(RUGCHECK_URL.format(mint=mint), timeout=timeout) as r:
                            if r.status == 200:
                                data = await r.json(content_type=None)
                                ok, reason = judge(data or {}, self._sec())
                                self.stats["ok" if ok else "fail"] += 1
                                self._engine().on_screen_result(mint, ok, reason)
                                return
                            why = {400: "not indexed in time", 404: "not indexed in time",
                                   429: "rate limited"}.get(r.status, f"HTTP {r.status}")
                    except asyncio.TimeoutError:
                        why = "timed out"
                    except aiohttp.ClientError as e:
                        why = type(e).__name__
            self._error(mint, why)
        except asyncio.CancelledError:
            raise
        except Exception as e:     # bad JSON etc.
            self._error(mint, type(e).__name__)
        finally:
            self.inflight -= 1

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


class DemoScreener:
    """Pretend RugCheck: answers after a short virtual delay, flags ~10% of tokens."""
    FLAGS = ["Freeze Authority still enabled", "Creator history of rugged tokens", "Copycat token"]

    def __init__(self, engine):
        self.engine = engine
        self.pending = []
        self.rng = random.Random()

    def request(self, mint):
        self.pending.append((self.engine.now + self.rng.uniform(0.4, 2.0), mint))

    def poll(self, now):
        due = [p for p in self.pending if p[0] <= now]
        self.pending = [p for p in self.pending if p[0] > now]
        sec = self.engine.cfg["security"]
        for _, mint in due:
            if self.rng.random() < 0.10:
                ok, reason = judge({"risks": [{"name": self.rng.choice(self.FLAGS)}]}, sec)
            else:
                ok, reason = judge({"risks": []}, sec)
            self.engine.on_screen_result(mint, ok, reason)
