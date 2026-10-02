"""
Reads the Discord pop-ups (notifications) Windows shows on this computer and passes each new one to a callback.

It never logs into Discord, never uses the Discord account or its token, and sends nothing anywhere: it only
reads what Windows already displays to you. Windows only (uses the built-in notification listener API).

Needs:
  - the Python packages winrt-runtime + winrt-Windows.UI.Notifications(.Management) etc. (installed
    automatically on first use if missing)
  - Windows Settings -> Privacy & security -> Notifications -> "Let apps access your notifications" = On
    (Windows asks once)
  - Discord desktop notifications on for the channel (or a ping role you have), and Windows not in Do Not
    Disturb / Focus (Focus hides the pop-ups but they usually still reach the notification list).
"""
from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import time

log = logging.getLogger("memebot")

PKGS = ["winrt-runtime", "winrt-Windows.Foundation", "winrt-Windows.Foundation.Collections",
        "winrt-Windows.ApplicationModel", "winrt-Windows.UI.Notifications",
        "winrt-Windows.UI.Notifications.Management"]


def _import():
    """(UserNotificationListener, status enum, NotificationKinds, KnownNotificationBindings) or raises ImportError."""
    try:
        import winrt.windows.applicationmodel  # noqa: F401  (types used by the notification objects)
        import winrt.windows.foundation  # noqa: F401
        import winrt.windows.foundation.collections  # noqa: F401
        from winrt.windows.ui.notifications import KnownNotificationBindings, NotificationKinds
        from winrt.windows.ui.notifications.management import (UserNotificationListener,
                                                               UserNotificationListenerAccessStatus)
    except ImportError:
        from winsdk.windows.ui.notifications import KnownNotificationBindings, NotificationKinds  # older package
        from winsdk.windows.ui.notifications.management import (UserNotificationListener,
                                                                UserNotificationListenerAccessStatus)
    return UserNotificationListener, UserNotificationListenerAccessStatus, NotificationKinds, KnownNotificationBindings


def _static(cls, name):
    v = getattr(cls, name)
    return v() if callable(v) else v


class NotificationWatcher:
    def __init__(self, cfg_getter, on_message):
        self._cfg = cfg_getter
        self.on_message = on_message              # async (text, source, title)
        self.status = "starting"
        self.ok = False
        self.seen: set[int] = set()
        self.recent: list[dict] = []              # last Discord pop-ups read (for the dashboard)
        self.count = 0
        self.last_error = ""

    def cfg(self):
        return self._cfg().get("calls") or {}

    def state(self):
        return dict(status=self.status, ok=self.ok, channel=str(self.cfg().get("channel") or ""), windows=sys.platform == "win32", read=self.count,
                    recent=self.recent[:8], error=self.last_error)

    async def _install(self):
        self.status = "installing the Windows notification reader (one time)…"
        loop = asyncio.get_running_loop()
        code = await loop.run_in_executor(None, lambda: subprocess.call(
            [sys.executable, "-m", "pip", "install", "-q", *PKGS]))
        return code == 0

    def _texts(self, n, KB):
        binding = None
        for attr in ("toast_generic", "get_toast_generic"):
            try:
                binding = n.notification.visual.get_binding(_static(KB, attr))
                break
            except Exception:
                continue
        if binding is None:
            return []
        return [t.text for t in binding.get_text_elements() if t.text]

    def _app(self, n):
        name, aumid = "", ""
        try:
            aumid = n.app_info.app_user_model_id or ""
        except Exception:
            pass
        try:
            name = n.app_info.display_info.display_name or ""
        except Exception:
            pass
        return name, aumid

    async def run(self):
        if sys.platform != "win32":
            self.status = "only works on Windows"
            return
        try:
            L, ST, NK, KB = _import()
        except ImportError:
            if not await self._install():
                self.status = "couldn't install the notification reader (pip failed)"
                return
            try:
                L, ST, NK, KB = _import()
            except ImportError as e:
                self.status = f"notification reader not available ({e})"
                return
        listener = _static(L, "current")
        try:
            access = await listener.request_access_async()
        except Exception as e:
            self.status = f"Windows refused notification access ({e})"
            return
        if int(access) != 1:                       # 1 = Allowed
            self.status = ("Windows is blocking notification access: turn on Settings → Privacy & security → "
                           "Notifications → \"Let apps access your notifications\", then restart the bot")
            return
        first = True
        while True:
            c = self.cfg()
            try:
                if not c.get("enabled", True) or not c.get("read_notifications", True):
                    self.ok, self.status = False, "reading Discord notifications is OFF in Settings"
                    await asyncio.sleep(5)
                    continue
                notes = await listener.get_notifications_async(NK.TOAST)
                apps = [a.lower() for a in (c.get("apps") or ["discord"])]
                only = str(c.get("channel") or c.get("only_titles_with") or "").lower().strip().lstrip("#").strip()
                tg_only = str(c.get("telegram_channel") or "").lower().strip().lstrip("@").strip()
                if tg_only and "telegram" not in apps:
                    apps.append("telegram")
                for n in notes:
                    nid = n.id
                    if nid in self.seen:
                        continue
                    self.seen.add(nid)
                    if first:
                        continue                   # already there when the bot started: never buy old calls
                    name, aumid = self._app(n)
                    ident = f"{name} {aumid}".lower()
                    if not any(a in ident for a in apps):
                        continue
                    tg = "telegram" in ident
                    texts = self._texts(n, KB)
                    title, body = (texts[0] if texts else ""), "\n".join(texts[1:])
                    if tg:                         # Telegram: the pop-up title is the group/channel name
                        match = bool(tg_only) and tg_only in title.lower()
                    else:                          # Discord: title (+ attribution line), not the message
                        where = " ".join([title, *texts[2:]]).lower()
                        match = bool(only) and only in where
                    self.count += 1
                    self.recent.insert(0, dict(ts=time.time(), app="Telegram" if tg else "Discord",
                                               title=title[:80], text=body[:140], match=match))
                    self.recent = self.recent[:20]
                    if not match:
                        continue                   # not a call channel/group (or none picked yet)
                    try:
                        await self.on_message(f"{title}\n{body}", "Telegram" if tg else (name or "Discord"), title)
                    except Exception as e:
                        self.last_error = f"buy: {e}"[:120]
                if len(self.seen) > 5000:
                    keep = {n.id for n in notes}
                    self.seen = keep
                first = False
                self.ok = True
                srcs = [x for x, on in (("Discord", only), ("Telegram", tg_only)) if on]
                self.status = (f"listening for {' + '.join(srcs)} call notifications" if srcs else
                               "reading pop-ups, but NOT buying until you set a call channel / group")
                self.last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e)[:120]
            await asyncio.sleep(float(c.get("poll_s", 1.0)))
