"""
handle/common.py

Small helpers shared by stats.py, ban.py, unban.py and broadcast.py.
"""

import asyncio
import importlib
import logging
import re

from pyrogram import filters
from pyrogram.errors import FloodWait

from database.user_info import user_db

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# "ᴛɪɴʏ ᴛᴇxᴛ" style (only letters change, digits/symbols stay the same)
# ----------------------------------------------------------------------
_TINY = str.maketrans(
    "abcdefghijklmnopqrstuvwxyz",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ",
)


def tiny(text) -> str:
    """Convert plain text to small caps. Never pass HTML tags through this."""
    return str(text).lower().translate(_TINY)


# ----------------------------------------------------------------------
# Owner-only filter (works for messages and callback queries)
# ----------------------------------------------------------------------
async def _owner_check(_, __, update):
    user = getattr(update, "from_user", None)
    return bool(user and user_db.is_owner(user.id))


owner_only = filters.create(_owner_check)


# ----------------------------------------------------------------------
# Safe Telegram calls
# ----------------------------------------------------------------------
async def flood_safe(func, *args, retries=3, **kwargs):
    """Await func(...) and wait/retry when Telegram sends a FloodWait."""
    for attempt in range(retries + 1):
        try:
            return await func(*args, **kwargs)
        except FloodWait as exc:
            if attempt == retries:
                raise
            wait = getattr(exc, "value", None) or getattr(exc, "x", 5)
            await asyncio.sleep(min(int(wait) + 1, 120))


async def safe_reply(message, text, **kwargs):
    try:
        return await flood_safe(message.reply_text, text, **kwargs)
    except Exception:
        logger.debug("safe_reply failed", exc_info=True)
        return None


async def safe_answer(query, text=None, alert=False):
    try:
        await query.answer(text, show_alert=alert)
    except Exception:
        pass


# ----------------------------------------------------------------------
# Input parsing
# ----------------------------------------------------------------------
_ID_RE = re.compile(r"^\d{1,15}$")


def parse_user_id(raw):
    """'12345' -> 12345, anything else (letters, negatives, huge) -> None."""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not _ID_RE.match(raw):
        return None
    value = int(raw)
    return value if value > 0 else None


# ----------------------------------------------------------------------
# Access to choice.py (lazy import, so there is no circular import)
# ----------------------------------------------------------------------
def get_choice_module():
    for name in ("choice", "handle.choice"):
        try:
            return importlib.import_module(name)
        except ImportError:
            continue
        except Exception:
            logger.exception("Could not import %s", name)
    return None


def live_task_count() -> int:
    mod = get_choice_module()
    try:
        return len(mod.active_jobs) if mod else 0
    except Exception:
        return 0


def open_session_count() -> int:
    mod = get_choice_module()
    try:
        return len(mod.user_sessions) if mod else 0
    except Exception:
        return 0


async def stop_user_activity(user_id: int) -> None:
    """Cancel a user's running task / open menu (used when banning). Never raises."""
    try:
        mod = get_choice_module()
        if mod is None:
            return
        tracker = mod.get_active_tracker(user_id)
        task = mod.user_job_tasks.get(user_id)

        if tracker is not None and not tracker.is_cancelled:
            await tracker.cancel()
        elif task is not None and not task.done():
            task.cancel()
        elif user_id in mod.user_sessions:
            await mod.clear_session(user_id, delete_interface=True)
    except Exception:
        logger.exception("stop_user_activity failed for %s", user_id)
