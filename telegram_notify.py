"""
telegram_notify.py — shared Telegram notification helper for TWT pipelines.

Usage:
    from telegram_notify import notify, PipelineStatus

    # At the start of your pipeline:
    status = PipelineStatus("pipeline_name")

    # Track counts/errors as you go:
    status.add("stocks_processed", 1574)
    status.add("errors", 0)

    # On success (call at the very end):
    status.success("Optional extra line")

    # On failure (in except block — re-raises after notifying):
    status.failure(e)
"""

import os
import time
import traceback
import requests

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

PIPELINE_EMOJIS = {
    "pipeline":                   "📊",
    "pipeline_master":            "🗂️",
    "pipeline_nse":               "🏦",
    "pipeline_insider":           "🕵️",
    "pipeline_feeds":             "📰",
    "pipeline_finedge":           "💹",
    "pipeline_corporate_actions": "🏢",
    "pipeline_fii_dii":           "🌐",
}


# ──────────────────────────────────────────────
# Low-level send
# ──────────────────────────────────────────────

def send_message(text: str, silent: bool = False, chat_id: str = "") -> bool:
    """
    Send a plain-text Telegram message. Returns True on success.

    chat_id: optional override — sends to this chat instead of the default
    TELEGRAM_CHAT_ID (e.g. a separate channel for a specific message type,
    like financial-results alerts, so they don't mix with pipeline status
    notifications in the main channel). Falls back to TELEGRAM_CHAT_ID if
    not given.
    """
    target_chat_id = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target_chat_id:
        print("[telegram_notify] Token or chat_id missing — skipping notification.")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id":                  target_chat_id,
                "text":                     text,
                "parse_mode":               "HTML",
                "disable_notification":     silent,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        print(f"[telegram_notify] Failed to send message: {exc}")
        return False


# ──────────────────────────────────────────────
# Convenience shortcut
# ──────────────────────────────────────────────

def notify(pipeline_name: str, status: str, details: str = "", silent: bool = False) -> bool:
    """
    Quick one-shot notify.

    status : "success" | "failure" | "warning" | "info"
    """
    icons = {"success": "✅", "failure": "❌", "warning": "⚠️", "info": "ℹ️"}
    icon  = icons.get(status, "🔔")
    emoji = PIPELINE_EMOJIS.get(pipeline_name, "⚙️")
    ist_time = _ist_now()

    lines = [
        f"{icon} <b>{emoji} {pipeline_name}</b>",
        f"Status : <code>{status.upper()}</code>",
        f"Time   : {ist_time}",
    ]
    if details:
        lines.append(details)

    return send_message("\n".join(lines), silent=silent)


# ──────────────────────────────────────────────
# Stateful helper class (recommended)
# ──────────────────────────────────────────────

class PipelineStatus:
    """
    Tracks pipeline run stats and sends a summary notification on finish.

    Example
    -------
    status = PipelineStatus("pipeline_nse")
    status.add("symbols",  1574)
    status.add("uploaded", 8)
    status.success()          # sends ✅ message
    # or
    status.failure(exc)       # sends ❌ message then re-raises exc
    """

    def __init__(self, pipeline_name: str):
        self.name       = pipeline_name
        self.emoji      = PIPELINE_EMOJIS.get(pipeline_name, "⚙️")
        self._stats: dict[str, int | str] = {}
        self._start     = time.time()
        self._warnings: list[str] = []

    # ── stat helpers ──

    def add(self, key: str, value: int = 1):
        """Increment an integer counter (or set string value)."""
        if isinstance(value, int):
            self._stats[key] = self._stats.get(key, 0) + value
        else:
            self._stats[key] = value

    def set(self, key: str, value):
        """Set a stat value directly."""
        self._stats[key] = value

    def warn(self, message: str):
        """Record a non-fatal warning; included in final summary."""
        self._warnings.append(message)
        print(f"[WARN] {message}")

    # ── finish methods ──

    def success(self, extra: str = "", silent: bool = False):
        """Send ✅ success notification."""
        elapsed = _fmt_elapsed(time.time() - self._start)
        lines   = [
            f"✅ <b>{self.emoji} {self.name}</b>",
            f"Status  : <code>SUCCESS</code>",
            f"Time    : {_ist_now()}",
            f"Elapsed : {elapsed}",
        ]
        if self._stats:
            lines.append("")
            for k, v in self._stats.items():
                lines.append(f"• {k}: <code>{v}</code>")
        if self._warnings:
            lines.append("")
            lines.append(f"⚠️ <b>{len(self._warnings)} warning(s):</b>")
            for w in self._warnings[:5]:          # cap at 5 to avoid huge message
                lines.append(f"  └ {w}")
        if extra:
            lines.append(f"\n{extra}")
        send_message("\n".join(lines), silent=silent)

    def failure(self, exc: Exception, reraise: bool = True, silent: bool = False):
        """
        Send ❌ failure notification.
        Re-raises exc by default so GitHub Actions marks the job as failed.
        """
        elapsed = _fmt_elapsed(time.time() - self._start)
        tb_tail = _tail_traceback(exc, lines=8)

        lines = [
            f"❌ <b>{self.emoji} {self.name}</b>",
            f"Status  : <code>FAILURE</code>",
            f"Time    : {_ist_now()}",
            f"Elapsed : {elapsed}",
            f"Error   : <code>{_escape(str(exc)[:200])}</code>",
        ]
        if self._stats:
            lines.append("")
            for k, v in self._stats.items():
                lines.append(f"• {k}: <code>{v}</code>")
        if tb_tail:
            lines.append(f"\n<pre>{_escape(tb_tail)}</pre>")

        send_message("\n".join(lines), silent=silent)

        if reraise:
            raise exc


# ──────────────────────────────────────────────
# Internal utilities
# ──────────────────────────────────────────────

def _ist_now() -> str:
    """Current time as IST string (UTC+5:30)."""
    ist = time.gmtime(time.time() + 5 * 3600 + 30 * 60)
    return time.strftime("%d %b %Y, %I:%M %p IST", ist)


def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m {s % 60}s"


def _tail_traceback(exc: Exception, lines: int = 8) -> str:
    tb = traceback.format_exc()
    tail = "\n".join(tb.strip().splitlines()[-lines:])
    return tail[:800]          # Telegram message size guard


def _escape(text: str) -> str:
    """Minimal HTML escaping for Telegram HTML parse mode."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
