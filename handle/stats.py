"""
handle/stats.py

/stats  - anyone can use it. Shows an image (or plain text if no image is
configured) with live bot numbers and Refresh / Close buttons.

Image:  set STATS_IMAGE in config.py (or as an environment variable).
        Can be a direct image URL, a Telegram file_id or a local file path.
"""

import logging
import os
import time

from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from database.user_info import STARTED_AT, user_db
from handle.common import (
    flood_safe,
    live_task_count,
    open_session_count,
    safe_answer,
    safe_reply,
    tiny,
)

try:
    from config import STATS_IMAGE  # type: ignore
except Exception:
    STATS_IMAGE = os.environ.get("STATS_IMAGE", "")

logger = logging.getLogger(__name__)

COMMAND_COOLDOWN = 5      # seconds between /stats from the same user
REFRESH_COOLDOWN = 3      # seconds between refresh taps from the same user
_last_command: dict = {}
_last_refresh: dict = {}

# (database key, label shown in the card)
FEATURES = (
    ("zip", "zip"),
    ("trim", "trim"),
    ("clips", "multi-clip"),
    ("merge", "merge"),
    ("audio", "video → audio"),
    ("edit", "video + audio"),
    ("other", "other"),
)


# ----------------------------------------------------------------------
# Formatting
# ----------------------------------------------------------------------
def _num(value) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def _uptime() -> str:
    seconds = max(0, int(time.time() - STARTED_AT))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h {minutes}m"
    return f"{hours}h {minutes}m"


def build_text(stats: dict) -> str:
    t = tiny

    def row(branch, label, value):
        return f"{branch} {t(label)} : <code>{value}</code>"

    lines = [
        f"📊 <b>{t('bot statistics')}</b>",
        "",
        f"👥 <b>{t('users')}</b>",
        row("├", "total users", _num(stats["total"])),
        row("├", "active · 24h", _num(stats["active_24h"])),
        row("├", "new · 24h", _num(stats["new_24h"])),
        row("└", "banned", _num(stats["banned"])),
        "",
        f"⚙️ <b>{t('tasks')}</b>",
        row("├", "live now", _num(live_task_count())),
        row("├", "open menus", _num(open_session_count())),
        row("├", "finished", _num(stats["finished"])),
        row("├", "failed", _num(stats["failed"])),
        row("└", "cancelled", _num(stats["cancelled"])),
    ]

    kinds = stats.get("kinds") or {}
    feature_rows = []
    for key, label in FEATURES:
        data = kinds.get(key)
        done = data.get("finished", 0) if isinstance(data, dict) else 0
        if done:
            feature_rows.append((label, done))
    if feature_rows:
        lines += ["", f"🧩 <b>{t('finished by feature')}</b>"]
        for i, (label, done) in enumerate(feature_rows):
            branch = "└" if i == len(feature_rows) - 1 else "├"
            lines.append(row(branch, label, _num(done)))

    lines += ["", f"⏱ {t('uptime')} : <code>{_uptime()}</code>"]
    if not stats.get("ok", True):
        lines += ["", f"⚠️ <i>{t('database is not reachable')}</i>"]
    return "\n".join(lines)


def build_keyboard(owner_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 ʀᴇꜰʀᴇꜱʜ", callback_data=f"stats:refresh:{owner_id}"),
        InlineKeyboardButton("✖️ ᴄʟᴏꜱᴇ", callback_data=f"stats:close:{owner_id}"),
    ]])


def _prune(store: dict) -> None:
    if len(store) > 5000:
        store.clear()


# ----------------------------------------------------------------------
# /stats
# ----------------------------------------------------------------------
async def _stats_command(client, message):
    user = message.from_user
    if not user:
        return

    now = time.monotonic()
    if now - _last_command.get(user.id, 0) < COMMAND_COOLDOWN:
        return                      # silently ignore spam
    _prune(_last_command)
    _last_command[user.id] = now

    text = build_text(await user_db.get_stats())
    keyboard = build_keyboard(user.id)

    if STATS_IMAGE:
        try:
            await flood_safe(
                message.reply_photo,
                STATS_IMAGE,
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            return
        except Exception:
            logger.exception("Could not send the stats image; falling back to text.")

    await flood_safe(
        message.reply_text,
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def stats_command(client, message):
    try:
        await _stats_command(client, message)
    except Exception:
        logger.exception("stats_command failed")
        await safe_reply(message, "⚠️ Could not load the statistics. Try again later.")


# ----------------------------------------------------------------------
# Buttons
# ----------------------------------------------------------------------
async def _stats_callback(client, query):
    if not query.from_user or not query.message:
        return

    _, action, owner = query.data.split(":")
    allowed = query.from_user.id == int(owner) or user_db.is_owner(query.from_user.id)
    if not allowed:
        await safe_answer(query, "Only the person who opened this can use it.", True)
        return

    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            logger.debug("Could not delete the stats message", exc_info=True)
        await safe_answer(query)
        return

    # refresh
    now = time.monotonic()
    if now - _last_refresh.get(query.from_user.id, 0) < REFRESH_COOLDOWN:
        await safe_answer(query, "Please wait a moment.")
        return
    _prune(_last_refresh)
    _last_refresh[query.from_user.id] = now

    text = build_text(await user_db.get_stats())
    keyboard = build_keyboard(int(owner))
    try:
        if query.message.photo:
            await flood_safe(
                query.message.edit_caption,
                text, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            )
        else:
            await flood_safe(
                query.message.edit_text,
                text, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            )
        await safe_answer(query, "Updated ✅")
    except MessageNotModified:
        await safe_answer(query, "Already up to date.")


async def stats_callback(client, query):
    try:
        await _stats_callback(client, query)
    except Exception:
        logger.exception("stats_callback failed")
        await safe_answer(query, "⚠️ Something went wrong.", True)


# ----------------------------------------------------------------------
def register_stats(app):
    app.add_handler(MessageHandler(stats_command, filters.command("stats")))
    app.add_handler(
        CallbackQueryHandler(
            stats_callback,
            filters.regex(r"^stats:(refresh|close):\d{1,15}$"),
        )
    )
