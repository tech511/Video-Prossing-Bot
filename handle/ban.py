"""
handle/ban.py

/ban <user_id>   (owner only)

Also contains the GLOBAL GUARD (`ban_guard`). It runs before every other
handler (group -2) and:
    1. blocks banned users everywhere (messages AND button presses),
       including choice.py, /start, /stats ...
    2. registers every new user the first time they send anything, so
       /stats counts people even if they never pressed /start.

Register it once with `register_ban(app)`.
"""

import logging
import time

from pyrogram import StopPropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import CallbackQuery

from database.user_info import user_db
from handle.common import (
    owner_only,
    parse_user_id,
    safe_answer,
    safe_reply,
    stop_user_activity,
)

logger = logging.getLogger(__name__)

NOTICE_COOLDOWN = 60          # tell a banned user at most once per minute
_notice_at: dict = {}


# ----------------------------------------------------------------------
# Global guard
# ----------------------------------------------------------------------
async def _notify_banned(update) -> None:
    user = update.from_user
    now = time.monotonic()
    if now - _notice_at.get(user.id, 0) < NOTICE_COOLDOWN:
        return
    if len(_notice_at) > 5000:
        _notice_at.clear()
    _notice_at[user.id] = now

    text = "🚫 You are banned from using this bot."
    if isinstance(update, CallbackQuery):
        await safe_answer(update, text, True)
    else:
        await safe_reply(update, text)


async def ban_guard(client, update):
    user = getattr(update, "from_user", None)
    if user is None or getattr(user, "is_bot", False):
        return

    if user_db.is_owner(user.id):
        return

    try:
        banned = await user_db.is_banned(user.id)
    except Exception:
        logger.exception("ban check failed")
        banned = False

    if banned:
        try:
            await _notify_banned(update)
        except Exception:
            logger.debug("banned notice failed", exc_info=True)
        raise StopPropagation          # nothing else will run for this update

    # Not banned: make sure the user is stored (throttled, never raises).
    await user_db.register_user(client, user)


# ----------------------------------------------------------------------
# /ban
# ----------------------------------------------------------------------
async def _ban_command(client, message):
    owner = message.from_user
    args = message.command[1:]

    if len(args) != 1:
        await safe_reply(message, "ℹ️ Usage: <code>/ban user_id</code>", parse_mode=ParseMode.HTML)
        return

    target = parse_user_id(args[0])
    if target is None:
        await safe_reply(message, "⚠️ Invalid user ID. Send numbers only, e.g. <code>/ban 123456789</code>",
                         parse_mode=ParseMode.HTML)
        return

    if user_db.is_owner(target):
        await safe_reply(message, "⚠️ You can't ban an owner.")
        return

    result = await user_db.ban_user(target, by=owner.id)

    if result == "ok":
        await stop_user_activity(target)      # stop their running task / menu
        await safe_reply(message, f"✅ User <code>{target}</code> has been banned.",
                         parse_mode=ParseMode.HTML)
    elif result == "already":
        await safe_reply(message, f"ℹ️ User <code>{target}</code> is already banned.",
                         parse_mode=ParseMode.HTML)
    elif result == "not_found":
        await safe_reply(message, f"❌ User <code>{target}</code> was not found in the database.",
                         parse_mode=ParseMode.HTML)
    elif result == "owner":
        await safe_reply(message, "⚠️ You can't ban an owner.")
    else:
        await safe_reply(message, "❌ Database error. The user was NOT banned, please try again.")


async def ban_command(client, message):
    try:
        await _ban_command(client, message)
    except Exception:
        logger.exception("ban_command failed")
        await safe_reply(message, "❌ Something went wrong. Please try again.")


# ----------------------------------------------------------------------
def register_ban(app):
    # Guard: group -2 runs before everything else (even /cancel at -1).
    app.add_handler(MessageHandler(ban_guard, filters.all), group=-2)
    app.add_handler(CallbackQueryHandler(ban_guard), group=-2)

    # Command: owner only (non-owners get no reply, the command stays hidden).
    app.add_handler(MessageHandler(ban_command, filters.command("ban") & owner_only))
