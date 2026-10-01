"""
Phone alerts for real trades through ntfy (https://ntfy.sh): free, no account, no phone number.
Install the ntfy app, subscribe to this bot's topic (shown on the dashboard), and every real buy / sell / close
arrives as a push notification. The topic is a long random name created once and kept in data/notify_topic.txt.
"""
from __future__ import annotations

import asyncio
import logging
import os
import secrets

import aiohttp

log = logging.getLogger("memebot")


class Notifier:
    def __init__(self, data_dir, cfg_getter):
        self._cfg = cfg_getter
        self.path = os.path.join(data_dir, "notify_topic.txt")
        self.topic = ""
        try:
            with open(self.path, encoding="utf-8") as fh:
                self.topic = fh.read().strip()
        except OSError:
            pass
        if not self.topic:
            self.topic = "momentum-" + secrets.token_hex(8)
            try:
                os.makedirs(data_dir, exist_ok=True)
                with open(self.path, "w", encoding="utf-8") as fh:
                    fh.write(self.topic)
            except OSError:
                pass
        self.sent = 0
        self.last_error = ""

    def cfg(self):
        return self._cfg().get("notify") or {}

    def enabled(self):
        return bool(self.cfg().get("enabled", True)) and bool(self.topic)

    def state(self):
        return dict(enabled=self.enabled(), topic=self.topic, server=self.cfg().get("server", "https://ntfy.sh"),
                    sent=self.sent, error=self.last_error)

    async def _post(self, title, msg, tags, priority):
        url = f"{self.cfg().get('server', 'https://ntfy.sh').rstrip('/')}/{self.topic}"
        headers = {"Title": title.encode("ascii", "ignore").decode(), "Tags": tags, "Priority": str(priority)}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, data=msg.encode("utf-8"), headers=headers,
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status >= 300:
                        self.last_error = f"ntfy HTTP {r.status}"
                        return False
            self.sent += 1
            self.last_error = ""
            return True
        except Exception as e:
            self.last_error = f"ntfy: {type(e).__name__}"
            return False

    def send(self, title, msg, tags="", priority=3, force=False):
        if not (force or self.enabled()):
            return
        try:
            asyncio.get_running_loop().create_task(self._post(title, msg, tags, priority))
        except RuntimeError:
            pass

    async def test(self):
        return await self._post("Momentum test alert", "Phone alerts are working. You'll get one for every real buy and sell.",
                                "white_check_mark", 3)
