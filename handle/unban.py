"""
handle/unban.py

/unban <user_id>   (owner only)

Checks that the user exists in the database AND is currently banned,
then unbans instantly. The ban guard (handle/ban.py) reads the same
in-memory list, so the user can use the bot again immediately.
"""

import logging

from pyrogram.enums import ParseMode
from pyrogram.handlers import MessageHandler
from pyrogram import filters

from database.user_info import user_db
from handle.common import owner_only, parse_user_id, safe_reply

logger = logging.getLogger(__name__)


async def _unban_command(client, message):
    args = message.command[1:]

    if len(args) != 1:
        await safe_reply(message, "ℹ️ Usage: <code>/unban user_id</code>", parse_mode=ParseMode.HTML)
        return

    target = parse_user_id(args[0])
    if target is None:
        await safe_reply(
            message,
            "⚠️ Invalid user ID. Send numbers only, e.g. <code>/unban 123456789</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    result = await user_db.unban_user(target)

    if result == "ok":
        await safe_reply(message, f"✅ User <code>{target}</code> has been unbanned.",
                         parse_mode=ParseMode.HTML)
    elif result == "not_banned":
        await safe_reply(message, f"ℹ️ User <code>{target}</code> is not banned.",
                         parse_mode=ParseMode.HTML)
    elif result == "not_found":
        await safe_reply(message, f"❌ User <code>{target}</code> was not found in the database.",
                         parse_mode=ParseMode.HTML)
    else:
        await safe_reply(message, "❌ Database error. The user was NOT unbanned, please try again.")


async def unban_command(client, message):
    try:
        await _unban_command(client, message)
    except Exception:
        logger.exception("unban_command failed")
        await safe_reply(message, "❌ Something went wrong. Please try again.")


def register_unban(app):
    app.add_handler(MessageHandler(unban_command, filters.command("unban") & owner_only))
