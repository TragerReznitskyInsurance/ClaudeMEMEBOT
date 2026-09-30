"""
Auto-update: every few minutes, ask GitHub whether a newer version exists. When one does and
no real trade is mid-flight, save everything and exit with code 3; the start-windows.bat /
start-mac.command loop then pulls the update and starts the bot again, which picks up where
it left off (live mode is resumed automatically, open positions are kept on disk as always).

Only restarts when launched from those scripts (they set MOMENTUM_LAUNCHER=1) - if you run
`python app.py` by hand, the dashboard just shows that an update is available.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time

log = logging.getLogger("memebot")
EXIT_UPDATE = 3


async def _git(cwd, *args, timeout=60):
    p = await asyncio.create_subprocess_exec("git", *args, cwd=cwd, stdout=asyncio.subprocess.PIPE,
                                             stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(p.communicate(), timeout)
    except asyncio.TimeoutError:
        p.kill()
        raise RuntimeError("git timed out")
    if p.returncode != 0:
        raise RuntimeError((err or out).decode(errors="replace").strip()[:120] or f"git {args[0]} failed")
    return out.decode(errors="replace").strip()


class Updater:
    def __init__(self, repo_dir, data_dir, cfg_getter):
        self.repo = repo_dir
        self.state_path = os.path.join(data_dir, "update_state.json")
        self.resume_path = os.path.join(data_dir, "resume.json")
        self._cfg = cfg_getter
        self.launcher = os.environ.get("MOMENTUM_LAUNCHER") == "1"
        self.available = False
        self.behind = 0
        self.remote = ""
        self.waiting_since = None
        self.failed = False                 # restarted for this version already but it didn't install
        self.last_check = 0.0
        self.last_error = ""

    def cfg(self):
        return self._cfg().get("auto_update") or {}

    def usable(self):
        return shutil.which("git") is not None and os.path.isdir(os.path.join(self.repo, ".git"))

    def _tried(self):
        try:
            with open(self.state_path, encoding="utf-8") as fh:
                return json.load(fh).get("tried", "")
        except (OSError, ValueError):
            return ""

    async def check(self):
        self.last_check = time.time()
        await _git(self.repo, "fetch", "--quiet")
        self.remote = await _git(self.repo, "rev-parse", "@{u}")
        self.behind = int(await _git(self.repo, "rev-list", "--count", "HEAD..@{u}") or 0)
        self.available = self.behind > 0
        self.failed = self.available and self._tried() == self.remote
        self.last_error = ""

    # ------------------------------------------------------------------ resume after restart
    def take_resume(self):
        """Mode to resume after an update restart (and forget it), or None."""
        try:
            with open(self.resume_path, encoding="utf-8") as fh:
                r = json.load(fh)
            os.remove(self.resume_path)
            if time.time() - float(r.get("ts", 0)) < 900:
                return r.get("mode")
        except (OSError, ValueError):
            pass
        return None

    # ------------------------------------------------------------------ main loop
    async def run(self, runner):
        if not self.usable():
            self.last_error = "git not found - auto-update off"
            return
        await asyncio.sleep(30)
        while True:
            c = self.cfg()
            try:
                if c.get("enabled", True):
                    await self.check()
                    if self.available and self.launcher and not self.failed:
                        busy = runner.busy_trades()
                        self.waiting_since = self.waiting_since or time.time()
                        if not busy or time.time() - self.waiting_since > 1800:
                            await self._restart(runner)
                            return
                    else:
                        self.waiting_since = None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e)[:120]
            await asyncio.sleep(60 if self.waiting_since else float(c.get("check_min", 5)) * 60)

    async def _restart(self, runner):
        log.info("UPDATE found (%d new change(s)) - saving and restarting to install it", self.behind)
        try:
            with open(self.state_path, "w", encoding="utf-8") as fh:
                json.dump({"tried": self.remote, "ts": time.time()}, fh)
            if runner.running:
                with open(self.resume_path, "w", encoding="utf-8") as fh:
                    json.dump({"mode": runner.mode, "ts": time.time()}, fh)
        except OSError:
            pass
        try:
            await runner.shutdown_for_update()
        except Exception as e:
            log.warning("update: shutdown problem (%s) - restarting anyway", e)
        for h in logging.getLogger().handlers:
            try:
                h.flush()
            except Exception:
                pass
        os._exit(EXIT_UPDATE)

    def state(self):
        return dict(available=self.available, behind=self.behind, auto=self.launcher and self.cfg().get("enabled", True),
                    failed=self.failed, waiting=bool(self.waiting_since), error=self.last_error)
