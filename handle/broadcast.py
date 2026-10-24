"""
handle/broadcast.py

/broadcast   (owner only) - reply to any post with this command.

- The post is COPIED to every user (no "Forwarded from" header). Inline
  URL buttons of the post are kept. Albums are supported.
- A live progress card refreshes every 2 seconds, with a Cancel button.
- At the end the card becomes a report: how many users received the post
  and how many did not (blocked / deleted account / other failure).
- Banned users are skipped. Only one broadcast can run at a time.
- Runs as a background task, so the bot stays responsive meanwhile.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

from pyrogram import errors as E
from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from database.user_info import user_db
from handle.common import owner_only, safe_answer, safe_reply

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------
BATCH_SIZE = 20             # messages sent in parallel
BATCH_MIN_SECONDS = 1.0     # a batch never finishes faster than this (Telegram ~30 msg/s)
EDIT_INTERVAL = 2.0         # progress card refresh
MAX_FLOOD_RETRIES = 3
MAX_FLOOD_SLEEP = 120
BAR_WIDTH = 12


def _errors(*names):
    """Exception classes that exist in the installed Pyrogram version."""
    return tuple(getattr(E, n) for n in names if hasattr(E, n))


BLOCKED = _errors("UserIsBlocked")
DEACTIVATED = _errors("InputUserDeactivated", "UserDeactivated", "UserDeactivatedBan")


@dataclass
class BroadcastState:
    owner_id: int
    total: int
    started: float = field(default_factory=time.monotonic)
    done: int = 0
    success: int = 0
    blocked: int = 0
    deactivated: int = 0
    failed: int = 0
    cancelled: bool = False
    finished: bool = False
    db_error: bool = False

    @property
    def not_received(self) -> int:
        return self.blocked + self.deactivated + self.failed


_running = False
_state = None
_tasks: set = set()


# ----------------------------------------------------------------------
# Formatting
# ----------------------------------------------------------------------
def _clock(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"


def _bar(percent: float) -> str:
    filled = int(BAR_WIDTH * percent / 100)
    filled = max(0, min(BAR_WIDTH, filled))
    return "█" * filled + "░" * (BAR_WIDTH - filled)


def _lines(state: BroadcastState) -> str:
    return (
        f"👥 Total users   : {state.total:,}\n"
        f"✅ Received      : {state.success:,}\n"
        f"🚫 Blocked bot   : {state.blocked:,}\n"
        f"👻 Deleted acc.  : {state.deactivated:,}\n"
        f"⚠️ Other failed  : {state.failed:,}"
    )


def progress_text(state: BroadcastState) -> str:
    elapsed = time.monotonic() - state.started
    percent = min(100.0, state.done * 100 / state.total) if state.total else 100.0
    speed = state.done / elapsed if elapsed > 0 else 0
    remaining = (state.total - state.done) / speed if speed > 0 else 0
    return (
        "📣 <b>Broadcast in progress...</b>\n\n"
        f"<code>[{_bar(percent)}] {percent:.1f}%</code>\n\n"
        f"<code>{_lines(state)}\n"
        f"⚡ Speed         : {speed:.1f}/s\n"
        f"⏱ Elapsed       : {_clock(elapsed)}\n"
        f"⏳ Time left     : {_clock(remaining)}</code>"
    )


def final_text(state: BroadcastState) -> str:
    elapsed = time.monotonic() - state.started
    if state.cancelled:
        title = "🛑 <b>Broadcast cancelled</b>"
    elif state.db_error:
        title = "⚠️ <b>Broadcast stopped (database error)</b>"
    else:
        title = "✅ <b>Broadcast completed</b>"

    percent = min(100.0, state.done * 100 / state.total) if state.total else 100.0
    return (
        f"{title}\n\n"
        f"<code>[{_bar(percent)}] {percent:.1f}%</code>\n\n"
        f"<code>{_lines(state)}\n"
        f"⏱ Time taken    : {_clock(elapsed)}</code>\n\n"
        f"📬 <b>{state.success:,}</b> user(s) have this post\n"
        f"📭 <b>{state.not_received:,}</b> user(s) do not have it (blocked / deleted / failed)"
    )


def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel broadcast", callback_data="bc:cancel")]])


# ----------------------------------------------------------------------
# Sending
# ----------------------------------------------------------------------
async def _deliver(client, source, target: int) -> str:
    """Send the post to one user. Returns ok | blocked | deactivated | failed."""
    markup = source.reply_markup
    if not isinstance(markup, InlineKeyboardMarkup):
        markup = None                        # keep inline buttons only

    for attempt in range(MAX_FLOOD_RETRIES + 1):
        try:
            if source.media_group_id:
                await client.copy_media_group(target, source.chat.id, source.id)
            else:
                await source.copy(target, reply_markup=markup)   # copy = no forward tag
            return "ok"
        except FloodWait as exc:
            if attempt == MAX_FLOOD_RETRIES:
                return "failed"
            wait = getattr(exc, "value", None) or getattr(exc, "x", 5)
            await asyncio.sleep(min(int(wait) + 1, MAX_FLOOD_SLEEP))
        except BLOCKED:
            return "blocked"
        except DEACTIVATED:
            return "deactivated"
        except Exception as exc:
            logger.debug("Broadcast to %s failed: %s", target, exc)
            return "failed"
    return "failed"


async def _send_batch(client, state: BroadcastState, source, batch: list) -> None:
    started = time.monotonic()
    results = await asyncio.gather(
        *(_deliver(client, source, uid) for uid in batch),
        return_exceptions=True,
    )
    for result in results:
        if result == "ok":
            state.success += 1
        elif result == "blocked":
            state.blocked += 1
        elif result == "deactivated":
            state.deactivated += 1
        else:
            state.failed += 1
    state.done += len(batch)

    spare = BATCH_MIN_SECONDS - (time.monotonic() - started)
    if spare > 0:
        await asyncio.sleep(spare)


async def _refresher(state: BroadcastState, status) -> None:
    """Edit the progress card every EDIT_INTERVAL seconds."""
    while not state.finished:
        await asyncio.sleep(EDIT_INTERVAL)
        if state.finished:
            break
        try:
            await status.edit_text(
                progress_text(state),
                parse_mode=ParseMode.HTML,
                reply_markup=cancel_keyboard(),
            )
        except MessageNotModified:
            pass
        except FloodWait as exc:
            await asyncio.sleep(min(int(getattr(exc, "value", 5)) + 1, 30))
        except Exception:
            logger.debug("Progress edit failed", exc_info=True)


async def _run_broadcast(client, state: BroadcastState, source, status) -> None:
    global _running, _state
    refresher = asyncio.create_task(_refresher(state, status))
    try:
        batch = []
        async for uid in user_db.iter_user_ids():
            if state.cancelled:
                break
            batch.append(uid)
            if len(batch) >= BATCH_SIZE:
                await _send_batch(client, state, source, batch)
                batch = []
        if batch and not state.cancelled:
            await _send_batch(client, state, source, batch)
    except asyncio.CancelledError:
        state.cancelled = True
        raise
    except Exception:
        logger.exception("Broadcast stopped by an error")
        state.db_error = True
    finally:
        state.finished = True
        refresher.cancel()
        try:
            await status.edit_text(final_text(state), parse_mode=ParseMode.HTML)
        except Exception:
            # Editing failed (message deleted?) -> send the report as a new message.
            try:
                await client.send_message(
                    state.owner_id, final_text(state), parse_mode=ParseMode.HTML
                )
            except Exception:
                logger.exception("Could not deliver the broadcast report")
        _running = False
        _state = None


# ----------------------------------------------------------------------
# /broadcast
# ----------------------------------------------------------------------
async def _broadcast_command(client, message):
    global _running, _state

    source = message.reply_to_message
    if source is None:
        await safe_reply(message, "ℹ️ Reply to the post you want to broadcast with <code>/broadcast</code>",
                         parse_mode=ParseMode.HTML)
        return
    if getattr(source, "empty", False) or getattr(source, "service", None):
        await safe_reply(message, "⚠️ That message can't be broadcast. Reply to a normal post.")
        return

    # check + set with no await in between = no race
    if _running:
        await safe_reply(message, "⏳ A broadcast is already running. Wait for it or cancel it first.")
        return
    _running = True

    try:
        total = await user_db.count_broadcast_users()
        if total == 0:
            await safe_reply(message, "ℹ️ There are no users to broadcast to.")
            _running = False
            return

        status = await message.reply_text(
            "📣 Starting broadcast...", reply_markup=cancel_keyboard()
        )
        state = BroadcastState(owner_id=message.from_user.id, total=total)
        _state = state

        task = asyncio.create_task(_run_broadcast(client, state, source, status))
        _tasks.add(task)
        task.add_done_callback(_tasks.discard)
    except Exception:
        _running = False
        _state = None
        raise


async def broadcast_command(client, message):
    try:
        await _broadcast_command(client, message)
    except Exception:
        logger.exception("broadcast_command failed")
        await safe_reply(message, "❌ Could not start the broadcast. Please try again.")


async def broadcast_cancel_callback(client, query):
    try:
        state = _state
        if state is None or state.finished:
            await safe_answer(query, "No broadcast is running.", True)
            return
        state.cancelled = True
        await safe_answer(query, "Cancelling... finishing the current batch.")
    except Exception:
        logger.exception("broadcast_cancel_callback failed")
        await safe_answer(query, "⚠️ Something went wrong.", True)


def register_broadcast(app):
    app.add_handler(
        MessageHandler(broadcast_command, filters.command("broadcast") & owner_only)
    )
    app.add_handler(
        CallbackQueryHandler(
            broadcast_cancel_callback,
            filters.regex(r"^bc:cancel$") & owner_only,
        )
    )
