"""
file name - choice.py

File-action menu for the all-in-one bot.

Features: ZIP (multi-file), TRIM, MULTI-CLIP, MERGE, VIDEO -> AUDIO.

Rules enforced here:
- One session per user (keyed by user id), many users at the same time.
- A session (and its choice message) is deleted after SESSION_TIMEOUT
  seconds without activity.
- A user can only run ONE task at a time.
- After a task finishes successfully, the same user must wait
  COOLDOWN_SECONDS before starting another one.
- Jobs run as background tasks so one user's long job never blocks
  other users or the Cancel buttons.
"""
import asyncio
import functools
import logging
import os
import shutil
import tempfile
import time

from pyrogram import filters
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from logic.trim import (
    parse_time,
    probe_media,
    validate_range,
    trim_media_in_temp,
    cleanup_temp,
    TrimError,
    TrimCancelled,
)
from logic.zip import (
    create_zip_in_temp,
    ZipError,
    ZipCancelled,
)
from logic.audio_extractor import (
    extract_audio_in_temp,
    AudioExtractionError,
    AudioExtractionCancelled,
)
from logic.clips import (
    cut_clips_in_temp,
    parse_clip_ranges,
    format_time,
    ClipError,
    ClipCancelled,
    MAX_CLIPS,
)
from logic.merge import (
    merge_files_in_temp,
    MergeError,
    MergeCancelled,
)
from progress import ProgressTracker


logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------
SESSION_TIMEOUT = 180            # 3 minutes of inactivity
COOLDOWN_SECONDS = 300           # 5 minutes after a finished task
MAX_CONCURRENT_JOBS = 3          # heavy jobs running at once (others queue)
MAX_ZIP_FILES = 10
MAX_MERGE_FILES = 10
MAX_COLLECT_TOTAL_BYTES = int(1.5 * 1024 ** 3)
FLOOD_RETRIES = 3

SUPPORTED_EXTENSIONS = {
    ".mp3", ".wav", ".flac", ".aac", ".m4a", ".ogg", ".opus", ".wma",
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".mpeg", ".mpg", ".m4v",
    ".wmv", ".3gp", ".ts", ".flv",
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff",
    ".pdf", ".txt", ".csv", ".json", ".xml",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2",
}

TRIMMABLE_EXTENSIONS = {
    ".mp3", ".wav", ".flac", ".aac", ".m4a", ".ogg", ".opus", ".wma",
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".mpeg", ".mpg", ".m4v",
    ".wmv", ".3gp", ".ts", ".flv",
}

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".mpeg", ".mpg", ".m4v",
    ".wmv", ".3gp", ".ts", ".flv",
}

# Sessions that are collecting several files before the job starts.
COLLECT_MODES = {
    "zip_collect": {
        "title": "📦 **ZIP Mode**",
        "max_files": MAX_ZIP_FILES,
        "done_label": "✅ DONE",
        "done_data": "choice:zipdone",
        "hint": "Send more files to add them, then press **DONE**.",
    },
    "merge_collect": {
        "title": "🔗 **Merge Mode**",
        "max_files": MAX_MERGE_FILES,
        "done_label": "✅ MERGE",
        "done_data": "choice:mergedone",
        "hint": (
            "Files are joined in the order shown. Send more audio or video "
            "files, then press **MERGE** (audio with audio, video with video)."
        ),
    },
}


# ----------------------------------------------------------------------
# Shared state
# ----------------------------------------------------------------------
user_sessions = {}      # user_id -> session dict
active_jobs = set()     # user ids that currently have a running task
user_cooldowns = {}     # user_id -> time.monotonic() when last task finished
_background_tasks = set()
_job_semaphore = None


class JobError(Exception):
    """A friendly, expected error raised by choice.py itself."""


class JobCancelled(JobError):
    """Raised when the user cancels a running job."""


CANCELLED_ERRORS = (
    JobCancelled, TrimCancelled, ZipCancelled,
    AudioExtractionCancelled, ClipCancelled, MergeCancelled,
)
KNOWN_ERRORS = (
    JobError, TrimError, ZipError,
    AudioExtractionError, ClipError, MergeError,
)


# ----------------------------------------------------------------------
# Small safe helpers (never raise)
# ----------------------------------------------------------------------
def _get_semaphore():
    # Created lazily so it binds to the running event loop.
    global _job_semaphore
    if _job_semaphore is None:
        _job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    return _job_semaphore


def spawn(coro):
    """Run a coroutine in the background and log any crash."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)

    def _done(t):
        _background_tasks.discard(t)
        if not t.cancelled() and t.exception():
            logger.error("Background task crashed", exc_info=t.exception())

    task.add_done_callback(_done)
    return task


async def with_flood_retry(func, *args, **kwargs):
    """Await func(*args, **kwargs), sleeping and retrying on FloodWait."""
    for attempt in range(FLOOD_RETRIES + 1):
        try:
            return await func(*args, **kwargs)
        except FloodWait as exc:
            if attempt == FLOOD_RETRIES:
                raise
            wait = getattr(exc, "value", None) or getattr(exc, "x", 5)
            await asyncio.sleep(int(wait) + 1)


async def safe_answer(query, text=None, alert=False):
    try:
        await query.answer(text, show_alert=alert)
    except Exception:
        pass


async def safe_edit(message, text, markup=None):
    try:
        await with_flood_retry(message.edit_text, text, reply_markup=markup)
    except MessageNotModified:
        pass
    except Exception:
        logger.debug("safe_edit failed", exc_info=True)


async def safe_reply(message, text):
    try:
        return await with_flood_retry(message.reply_text, text)
    except Exception:
        logger.debug("safe_reply failed", exc_info=True)
        return None


def safe_name(name):
    return str(name).replace("`", "'")[:45]


def fmt_size(num):
    num = float(num or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if num < 1024 or unit == "GB":
            return f"{num:.1f} {unit}" if unit != "B" else f"{int(num)} B"
        num /= 1024


def fmt_wait(seconds):
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}:{secs:02d}"


# ----------------------------------------------------------------------
# File detection
# ----------------------------------------------------------------------
def _size(obj):
    return getattr(obj, "file_size", 0) or 0


def get_file_info(message):
    if message.photo:
        photo = message.photo
        if isinstance(photo, (list, tuple)):
            photo = photo[-1]
        return {"file_id": photo.file_id, "file_name": "photo.jpg", "file_type": "photo", "file_size": _size(photo)}
    if message.video:
        return {"file_id": message.video.file_id, "file_name": message.video.file_name or "video.mp4", "file_type": "video", "file_size": _size(message.video)}
    if message.audio:
        return {"file_id": message.audio.file_id, "file_name": message.audio.file_name or "audio.mp3", "file_type": "audio", "file_size": _size(message.audio)}
    if message.animation:
        return {"file_id": message.animation.file_id, "file_name": message.animation.file_name or "animation.mp4", "file_type": "animation", "file_size": _size(message.animation)}
    if message.document:
        name = message.document.file_name or ""
        if os.path.splitext(name)[1].lower() not in SUPPORTED_EXTENSIONS:
            return None
        return {"file_id": message.document.file_id, "file_name": name, "file_type": "document", "file_size": _size(message.document)}
    return None


def supported_file_filter(_, client, message):
    try:
        return bool(message.from_user and get_file_info(message) is not None)
    except Exception:
        return False


supported_files = filters.create(supported_file_filter)


def is_media_info(info):
    """True for audio/video files (works with a file info or a session)."""
    extension = os.path.splitext(info["file_name"])[1].lower()
    return (
        info["file_type"] in {"audio", "video", "animation"}
        or extension in TRIMMABLE_EXTENSIONS
    )


# ----------------------------------------------------------------------
# Buttons / texts
# ----------------------------------------------------------------------
def choice_buttons():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📦 ZIP", callback_data="choice:zip"),
            InlineKeyboardButton("✂️ TRIM", callback_data="choice:trim"),
        ],
        [
            InlineKeyboardButton("🎞 MULTI-CLIP", callback_data="choice:clips"),
            InlineKeyboardButton("🔗 MERGE", callback_data="choice:merge"),
        ],
        [InlineKeyboardButton("🎵 VIDEO → AUDIO", callback_data="choice:audio")],
        [InlineKeyboardButton("✖️ CLOSE", callback_data="choice:close")],
    ])


def collect_buttons(session):
    mode = COLLECT_MODES[session["stage"]]
    count = len(session["files"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{mode['done_label']} ({count} file{'s' if count != 1 else ''})",
            callback_data=mode["done_data"],
        )],
        [InlineKeyboardButton("✖️ CLOSE", callback_data="choice:close")],
    ])


def collect_text(session):
    mode = COLLECT_MODES[session["stage"]]
    files = session["files"]
    lines = "\n".join(
        f"{i}. `{safe_name(f['file_name'])}`" for i, f in enumerate(files, 1)
    )
    total = sum(f["file_size"] for f in files)
    return (
        f"{mode['title']}\n\n"
        f"{lines}\n\n"
        f"📊 Files: {len(files)}/{mode['max_files']} • Size: {fmt_size(total)}\n\n"
        f"{mode['hint']}"
    )


def clips_prompt_text():
    return (
        "🎞 **Multi-Clip Cutter**\n\n"
        f"Send your time ranges in **one message**, one per line "
        f"(up to {MAX_CLIPS}):\n\n"
        "`00:10 to 00:20`\n"
        "`00:20 to 00:30`\n\n"
        "Each range becomes its own file.\n"
        "Send `/cancel` to stop."
    )


# ----------------------------------------------------------------------
# Session / cooldown management
# ----------------------------------------------------------------------
def cooldown_remaining(user_id):
    finished = user_cooldowns.get(user_id)
    if finished is None:
        return 0
    remaining = COOLDOWN_SECONDS - (time.monotonic() - finished)
    if remaining <= 0:
        user_cooldowns.pop(user_id, None)
        return 0
    return int(remaining) + 1


def prune_cooldowns():
    now = time.monotonic()
    for uid in list(user_cooldowns):
        if now - user_cooldowns[uid] >= COOLDOWN_SECONDS:
            user_cooldowns.pop(uid, None)


async def clear_session(key, delete_interface=False):
    session = user_sessions.pop(key, None)
    if not session:
        return
    task = session.get("timeout_task")
    if task and task is not asyncio.current_task():
        task.cancel()
    session["timeout_task"] = None
    if delete_interface:
        try:
            await session["choice_message"].delete()
        except Exception:
            pass


def reset_timeout(client, key, session):
    old_task = session.get("timeout_task")
    if old_task and old_task is not asyncio.current_task():
        old_task.cancel()
    session["timeout_task"] = asyncio.create_task(expire_session(client, key, session))


async def expire_session(client, key, session):
    try:
        await asyncio.sleep(SESSION_TIMEOUT)
        if (
            user_sessions.get(key) is session
            and session.get("stage") != "processing"
            and key not in active_jobs
        ):
            await clear_session(key, delete_interface=True)
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("expire_session failed")


def stop_timeout(session):
    task = session.get("timeout_task")
    if task and task is not asyncio.current_task():
        task.cancel()
    session["timeout_task"] = None


# ----------------------------------------------------------------------
# Generic job runner
# ----------------------------------------------------------------------
def start_job(client, user_id, session, reply_to, title, worker):
    """Lock the user and run `worker` in the background.

    Returns False when the user already has a running task.
    The check + lock has no `await` in between, so it is race-free.
    """
    if user_id in active_jobs:
        return False
    active_jobs.add(user_id)
    session["stage"] = "processing"
    stop_timeout(session)
    spawn(execute_job(client, user_id, session, reply_to, title, worker))
    return True


async def execute_job(client, user_id, session, reply_to, title, worker):
    tracker = None
    success = False
    try:
        await safe_edit(session["choice_message"], "⏳ Your task is being processed...")

        progress_message = await with_flood_retry(reply_to.reply_text, "⏳ Starting...")
        tracker = ProgressTracker(client, progress_message, user_id, title)
        session["progress"] = tracker

        async def on_cancel():
            await clear_session(user_id, delete_interface=True)

        tracker.set_cancel_callback(on_cancel)

        semaphore = _get_semaphore()
        if semaphore.locked():
            tracker.set_progress(0, "Waiting in queue...")
        await tracker.start()

        async with semaphore:
            if tracker.is_cancelled:
                raise JobCancelled("Cancelled.")
            tracker.set_progress(1, "Starting...")
            final_text = await worker(client, session, tracker)

        await tracker.finish(final_text)
        success = True

    except CANCELLED_ERRORS:
        if tracker:
            await tracker.finish("❌ **Cancelled.** Temporary files cleared.")
    except KNOWN_ERRORS as exc:
        if tracker:
            await tracker.finish(f"⚠️ **Task failed:**\n{exc}")
        else:
            await safe_reply(reply_to, f"⚠️ Task failed: {exc}")
    except asyncio.CancelledError:
        raise
    except FloodWait:
        logger.warning("FloodWait during job for user %s", user_id)
        if tracker:
            await tracker.finish("⚠️ Telegram is rate-limiting the bot. Please try again later.")
    except Exception:
        logger.exception("Job crashed for user %s", user_id)
        if tracker:
            await tracker.finish("❌ Something went wrong. Please try again.")
        else:
            await safe_reply(reply_to, "❌ Something went wrong. Please try again.")
    finally:
        try:
            if tracker:
                try:
                    await tracker.finish()
                except Exception:
                    pass
                for path in list(tracker.temp_paths):
                    shutil.rmtree(path, ignore_errors=True)
            if success:
                user_cooldowns[user_id] = time.monotonic()
            if user_sessions.get(user_id) is session:
                await clear_session(user_id, delete_interface=True)
        except Exception:
            logger.exception("Job cleanup failed for user %s", user_id)
        finally:
            active_jobs.discard(user_id)


async def download_file(client, message, directory, tracker, low, high, label):
    os.makedirs(directory, exist_ok=True)
    path = await client.download_media(
        message,
        file_name=os.path.join(directory, ""),
        progress=tracker.make_transfer_callback(low, high, label),
    )
    if tracker.is_cancelled:
        raise JobCancelled("Cancelled.")
    if not path or not isinstance(path, str) or not os.path.isfile(path):
        raise JobError("Could not download the original file.")
    return path


async def fetch_messages(client, session, message_ids):
    messages = await client.get_messages(
        chat_id=session["chat_id"],
        message_ids=message_ids,
    )
    if not isinstance(messages, list):
        messages = [messages]
    return messages


async def download_collected(client, session, tracker, prefix):
    """Download every collected file (uses 1-30% of the progress bar)."""
    files = session["files"]
    count = len(files)

    download_dir = tempfile.mkdtemp(prefix=prefix)
    tracker.add_temp_path(download_dir)

    messages = await fetch_messages(client, session, [f["message_id"] for f in files])

    input_paths = []
    for index, entry in enumerate(files):
        if tracker.is_cancelled:
            raise JobCancelled("Cancelled.")
        message = messages[index] if index < len(messages) else None
        if not message or getattr(message, "empty", False) or not get_file_info(message):
            raise JobError(f"Original file not found: {entry['file_name']}")

        low = 1 + 29 * index / count
        high = 1 + 29 * (index + 1) / count
        path = await download_file(
            client,
            message,
            os.path.join(download_dir, str(index)),  # own folder = no name clashes
            tracker,
            low,
            high,
            f"Downloading {index + 1}/{count}",
        )
        input_paths.append(path)

    return input_paths


async def upload_result(
    tracker, sender, percent=95, show_transfer=True,
    low=95, high=99, label="Uploading", **kwargs
):
    tracker.set_progress(percent, f"{label}...")
    await tracker.render()
    if show_transfer:
        kwargs["progress"] = tracker.make_transfer_callback(low, high, label)
    await with_flood_retry(sender, **kwargs)
    if tracker.is_cancelled:
        raise JobCancelled("Cancelled.")


# ----------------------------------------------------------------------
# Workers (each returns the final success text)
# ----------------------------------------------------------------------
async def zip_worker(client, session, tracker):
    files = session["files"]
    count = len(files)

    input_paths = await download_collected(client, session, tracker, "zip_input_")

    tracker.set_progress(30, "Creating ZIP archive...")
    zip_path, output_dir = await create_zip_in_temp(
        input_paths,
        progress_callback=tracker.make_callback(30, 90),
        cancel_check=tracker.cancel_check,
    )
    tracker.add_temp_path(output_dir)

    if tracker.is_cancelled:
        raise JobCancelled("Cancelled.")

    if count == 1:
        stem = os.path.splitext(files[0]["file_name"])[0] or "archive"
        zip_name = f"{stem}.zip"
    else:
        zip_name = "archive.zip"

    await upload_result(
        tracker,
        client.send_document,
        chat_id=session["chat_id"],
        document=zip_path,
        file_name=zip_name,
        caption=(
            "✅ **ZIP created!**\n\n"
            f"📄 Files: `{count}`\n"
            f"📦 Size: `{fmt_size(os.path.getsize(zip_path))}`"
        ),
        reply_to_message_id=files[0]["message_id"],
    )
    return "✅ **ZIP created!**"


async def merge_worker(client, session, tracker):
    files = session["files"]
    count = len(files)

    input_paths = await download_collected(client, session, tracker, "merge_input_")

    tracker.set_progress(30, "Merging files...")
    merged_path, output_dir = await merge_files_in_temp(
        input_paths,
        progress_callback=tracker.make_callback(30, 90),
        cancel_check=tracker.cancel_check,
        process_callback=tracker.attach_process,
    )
    tracker.add_temp_path(output_dir)

    if tracker.is_cancelled:
        raise JobCancelled("Cancelled.")

    extension = os.path.splitext(merged_path)[1].lower()
    caption = (
        "✅ **Merge complete!**\n\n"
        f"📄 Files joined: `{count}`\n"
        f"📦 Size: `{fmt_size(os.path.getsize(merged_path))}`"
    )

    if extension == ".mp4":
        await upload_result(
            tracker,
            client.send_document,
            chat_id=session["chat_id"],
            document=merged_path,
            file_name="merged.mp4",
            caption=caption,
            reply_to_message_id=files[0]["message_id"],
        )
    else:
        await upload_result(
            tracker,
            client.send_audio,
            chat_id=session["chat_id"],
            audio=merged_path,
            file_name=f"merged{extension}",
            caption=caption,
            reply_to_message_id=files[0]["message_id"],
        )
    return "✅ **Merge complete!**"


async def clips_worker(client, session, tracker, ranges):
    download_dir = tempfile.mkdtemp(prefix="clips_input_")
    tracker.add_temp_path(download_dir)

    messages = await fetch_messages(client, session, session["source_message_id"])
    message = messages[0] if messages else None
    if not message or getattr(message, "empty", False) or not get_file_info(message):
        raise JobError("The original file could not be found.")

    input_path = await download_file(
        client, message, download_dir, tracker, 1, 30, "Downloading"
    )

    tracker.set_progress(30, "Cutting clips...")
    outputs, output_dir = await cut_clips_in_temp(
        input_path,
        ranges,
        progress_callback=tracker.make_callback(30, 90),
        cancel_check=tracker.cancel_check,
        process_callback=tracker.attach_process,
    )
    tracker.add_temp_path(output_dir)

    if tracker.is_cancelled:
        raise JobCancelled("Cancelled.")

    count = len(outputs)
    for index, path in enumerate(outputs):
        start, end = ranges[index]
        low = 90 + 9 * index / count
        high = 90 + 9 * (index + 1) / count
        extension = os.path.splitext(path)[1].lower()
        caption = (
            f"✂️ **Clip {index + 1}/{count}**\n\n"
            f"⏱ `{format_time(start)}` → `{format_time(end)}`\n"
            f"⌛ Length: `{format_time(end - start)}`"
        )
        common = dict(
            percent=int(low),
            low=low,
            high=high,
            label=f"Uploading clip {index + 1}/{count}",
            chat_id=session["chat_id"],
            caption=caption,
            reply_to_message_id=session["source_message_id"],
        )

        if extension == ".mp4":
            await upload_result(
                tracker,
                client.send_document,
                document=path,
                file_name=f"clip_{index + 1}.mp4",
                **common,
            )
        else:
            await upload_result(
                tracker,
                client.send_audio,
                audio=path,
                file_name=f"clip_{index + 1}{extension}",
                **common,
            )

    return f"✅ **{count} clip{'s' if count != 1 else ''} ready!**"


async def audio_worker(client, session, tracker):
    download_dir = tempfile.mkdtemp(prefix="audio_input_")
    tracker.add_temp_path(download_dir)

    messages = await fetch_messages(client, session, session["source_message_id"])
    message = messages[0] if messages else None
    if not message or getattr(message, "empty", False) or not get_file_info(message):
        raise JobError("The original file could not be found.")

    input_path = await download_file(
        client, message, download_dir, tracker, 1, 30, "Downloading"
    )

    tracker.set_progress(30, "Extracting audio...")
    audio_path, output_dir = await extract_audio_in_temp(
        input_path,
        progress_callback=tracker.make_callback(30, 90),
        cancel_check=tracker.cancel_check,
    )
    tracker.add_temp_path(output_dir)

    if tracker.is_cancelled:
        raise JobCancelled("Cancelled.")

    stem = os.path.splitext(session["file_name"])[0] or "audio"
    await upload_result(
        tracker,
        client.send_audio,
        chat_id=session["chat_id"],
        audio=audio_path,
        file_name=f"{stem}.mp3",
        caption=(
            "✅ **Audio extracted!**\n\n"
            f"📄 Source: `{safe_name(session['file_name'])}`"
        ),
        reply_to_message_id=session["source_message_id"],
    )
    return "✅ **Audio extracted!**"


async def trim_worker(client, session, tracker, start, end):
    download_dir = tempfile.mkdtemp(prefix="trim_input_")
    tracker.add_temp_path(download_dir)
    output_dir = None
    try:
        messages = await fetch_messages(client, session, session["source_message_id"])
        message = messages[0] if messages else None
        if not message or getattr(message, "empty", False) or not get_file_info(message):
            raise TrimError("The original file could not be found.")

        tracker.set_progress(1, "Downloading original file...")
        input_path = await client.download_media(
            message,
            file_name=os.path.join(download_dir, ""),
        )
        if tracker.is_cancelled:
            raise TrimCancelled("Trimming was cancelled.")
        if not input_path or not os.path.isfile(input_path):
            raise TrimError("Could not download the original file.")

        duration, _ = await probe_media(input_path)
        validate_range(start, end, duration)

        tracker.set_progress(2, "Trimming media...")
        output_path, output_dir = await trim_media_in_temp(
            input_path, start, end, tracker=tracker
        )
        session["output_dir"] = output_dir
        tracker.add_temp_path(output_dir)

        if tracker.is_cancelled:
            raise TrimCancelled("Trimming was cancelled.")

        await upload_result(
            tracker,
            client.send_document,
            percent=100,
            show_transfer=False,
            chat_id=session["chat_id"],
            document=output_path,
            file_name="trimmed.mp4",
            caption=(
                "✅ **Trimming complete!**\n\n"
                f"⏱ Start: `{start:.2f}s`\n"
                f"⏱ End: `{end:.2f}s`\n"
                f"⌛ Clip length: `{end - start:.2f}s`"
            ),
            reply_to_message_id=session["source_message_id"],
        )
        return "✅ **Trimming complete!**"
    finally:
        if output_dir:
            cleanup_temp(output_dir)


# ----------------------------------------------------------------------
# Incoming files
# ----------------------------------------------------------------------
async def add_collect_file(client, message, info, session):
    """Add a file to a ZIP or MERGE session."""
    stage = session["stage"]
    mode = COLLECT_MODES[stage]
    files = session["files"]

    if stage == "merge_collect" and not is_media_info(info):
        await safe_reply(message, "⚠️ Merge supports audio and video files only.")
        return

    if len(files) >= mode["max_files"]:
        await safe_reply(
            message,
            f"⚠️ You can add up to {mode['max_files']} files. Press the button to continue.",
        )
        return

    total = sum(f["file_size"] for f in files) + info["file_size"]
    if total > MAX_COLLECT_TOTAL_BYTES:
        await safe_reply(
            message,
            f"⚠️ Total size limit reached ({fmt_size(MAX_COLLECT_TOTAL_BYTES)}). "
            "Press the button to continue.",
        )
        return

    # No await between the checks above and this append -> race-free.
    files.append({
        "message_id": message.id,
        "file_name": info["file_name"],
        "file_size": info["file_size"],
    })
    reset_timeout(client, message.from_user.id, session)
    await safe_edit(
        session["choice_message"],
        collect_text(session),
        collect_buttons(session),
    )


async def _detect_file(client, message):
    if not message.from_user:
        return
    info = get_file_info(message)
    if not info:
        return

    uid = message.from_user.id

    if uid in active_jobs:
        await safe_reply(
            message,
            "⏳ Your previous task is still running. Please wait until it finishes.",
        )
        return

    session = user_sessions.get(uid)
    if (
        session
        and session.get("stage") in COLLECT_MODES
        and session["chat_id"] == message.chat.id
    ):
        await add_collect_file(client, message, info, session)
        return

    prune_cooldowns()
    remaining = cooldown_remaining(uid)
    if remaining > 0:
        await safe_reply(
            message,
            f"⏳ Please wait **{fmt_wait(remaining)}** before starting another task.",
        )
        return

    await clear_session(uid, delete_interface=True)

    choice_message = await with_flood_retry(
        message.reply_text,
        f"**File detected!**\n\n📄 Name: `{safe_name(info['file_name'])}`\n"
        f"🗂 Type: `{info['file_type']}`\n\nChoose an action below:",
        reply_markup=choice_buttons(),
    )
    session = {
        **info,
        "chat_id": message.chat.id,
        "user_id": uid,
        "source_message_id": message.id,
        "choice_message": choice_message,
        "stage": "choice",
        "files": [],
        "timeout_task": None,
        "progress": None,
    }
    user_sessions[uid] = session
    reset_timeout(client, uid, session)


async def detect_file(client, message):
    try:
        await _detect_file(client, message)
    except Exception:
        logger.exception("detect_file failed")
        await safe_reply(message, "⚠️ Something went wrong. Please send your file again.")


# ----------------------------------------------------------------------
# Button presses
# ----------------------------------------------------------------------
async def _choice_callback(client, query):
    if not query.message or not query.from_user:
        return
    uid = query.from_user.id
    session = user_sessions.get(uid)

    if (
        not session
        or session["choice_message"].id != query.message.id
        or session["chat_id"] != query.message.chat.id
    ):
        await safe_answer(query, "This session has expired. Send your file again.", True)
        return

    action = query.data.split(":", 1)[1]

    if uid in active_jobs or session.get("stage") == "processing":
        await safe_answer(query, "Please wait, your task is already running.", True)
        return

    if action == "close":
        await clear_session(uid, delete_interface=True)
        await safe_answer(query, "Session closed.")
        return

    stage = session.get("stage")
    extension = os.path.splitext(session["file_name"])[1].lower()
    first_file = {
        "message_id": session["source_message_id"],
        "file_name": session["file_name"],
        "file_size": session["file_size"],
    }

    if action == "zip":
        if stage != "choice":
            await safe_answer(query)
            return
        session["files"] = [first_file]
        session["stage"] = "zip_collect"
        reset_timeout(client, uid, session)
        await safe_answer(query)
        await safe_edit(session["choice_message"], collect_text(session), collect_buttons(session))
        return

    if action == "merge":
        if stage != "choice":
            await safe_answer(query)
            return
        if not is_media_info(session):
            await safe_answer(query, "MERGE supports audio and video files only.", True)
            return
        session["files"] = [first_file]
        session["stage"] = "merge_collect"
        reset_timeout(client, uid, session)
        await safe_answer(query)
        await safe_edit(session["choice_message"], collect_text(session), collect_buttons(session))
        return

    if action == "zipdone":
        if stage != "zip_collect" or not session["files"]:
            await safe_answer(query, "Nothing to zip.", True)
            return
        if not start_job(client, uid, session, session["choice_message"], "Creating ZIP", zip_worker):
            await safe_answer(query, "Your previous task is still running.", True)
            return
        await safe_answer(query)
        return

    if action == "mergedone":
        if stage != "merge_collect":
            await safe_answer(query)
            return
        if len(session["files"]) < 2:
            await safe_answer(query, "Send at least 2 files to merge.", True)
            return
        if not start_job(client, uid, session, session["choice_message"], "Merging files", merge_worker):
            await safe_answer(query, "Your previous task is still running.", True)
            return
        await safe_answer(query)
        return

    if action == "clips":
        if stage != "choice":
            await safe_answer(query)
            return
        if not is_media_info(session):
            await safe_answer(query, "MULTI-CLIP supports audio and video files only.", True)
            return
        session["stage"] = "clips_input"
        reset_timeout(client, uid, session)
        await safe_answer(query)
        await safe_edit(session["choice_message"], clips_prompt_text())
        return

    if action == "audio":
        if stage != "choice":
            await safe_answer(query)
            return
        if session["file_type"] not in {"video", "animation"} and extension not in VIDEO_EXTENSIONS:
            await safe_answer(query, "VIDEO → AUDIO supports video files only.", True)
            return
        if not start_job(client, uid, session, session["choice_message"], "Extracting audio", audio_worker):
            await safe_answer(query, "Your previous task is still running.", True)
            return
        await safe_answer(query)
        return

    if action == "trim":
        if stage != "choice":
            await safe_answer(query)
            return
        if not is_media_info(session):
            await safe_answer(query, "TRIM supports audio and video files only.", True)
            return
        session["stage"] = "start_time"
        reset_timeout(client, uid, session)
        await safe_answer(query)
        await safe_edit(
            session["choice_message"],
            "✂️ **Media Trimmer**\n\nSend the **start time**.\n\n"
            "Examples: `00:01:30`, `01:30`, or `90` seconds.\n\n"
            "Send `/cancel` to stop.",
        )
        return

    await safe_answer(query)


async def choice_callback(client, query):
    try:
        await _choice_callback(client, query)
    except Exception:
        logger.exception("choice_callback failed")
        await safe_answer(query, "⚠️ Something went wrong. Please send your file again.", True)
        try:
            uid = query.from_user.id
            if uid in user_sessions and uid not in active_jobs:
                await clear_session(uid, delete_interface=True)
        except Exception:
            pass


# ----------------------------------------------------------------------
# Text input: trim start/end time and multi-clip ranges
# ----------------------------------------------------------------------
async def _trim_time_handler(client, message):
    if not message.from_user or not message.text:
        return
    uid = message.from_user.id
    session = user_sessions.get(uid)
    if not session or session["chat_id"] != message.chat.id:
        return

    stage = session.get("stage")
    if stage not in {"start_time", "end_time", "clips_input"}:
        return

    if message.text.strip().lower().split("@")[0] == "/cancel":
        await clear_session(uid, delete_interface=True)
        await safe_reply(message, "✖️ Cancelled.")
        return

    # ---- Multi-clip: all ranges arrive in one message ----
    if stage == "clips_input":
        try:
            ranges = parse_clip_ranges(message.text)
        except ClipError as exc:
            await safe_reply(message, f"⚠️ {exc}\nPlease send your ranges again.")
            reset_timeout(client, uid, session)
            return

        worker = functools.partial(clips_worker, ranges=ranges)
        if not start_job(client, uid, session, message, "Cutting clips", worker):
            await safe_reply(message, "⏳ Your previous task is still running.")
        return

    # ---- Trim: start time, then end time ----
    try:
        timestamp = parse_time(message.text)
    except TrimError as exc:
        await safe_reply(message, f"⚠️ {exc}\nPlease enter the time again.")
        reset_timeout(client, uid, session)
        return

    if stage == "start_time":
        session["start_time"] = timestamp
        session["stage"] = "end_time"
        reset_timeout(client, uid, session)
        await safe_reply(message, "✅ Start time saved.\n\nNow send the **end time**.")
        return

    start, end = session["start_time"], timestamp
    if end <= start:
        await safe_reply(message, "⚠️ End time must be after start time. Try again.")
        reset_timeout(client, uid, session)
        return

    worker = functools.partial(trim_worker, start=start, end=end)
    if not start_job(client, uid, session, message, "Trimming media", worker):
        await safe_reply(message, "⏳ Your previous task is still running.")


async def trim_time_handler(client, message):
    try:
        await _trim_time_handler(client, message)
    except Exception:
        logger.exception("trim_time_handler failed")
        try:
            uid = message.from_user.id
            if uid in user_sessions and uid not in active_jobs:
                await clear_session(uid, delete_interface=True)
        except Exception:
            pass
        await safe_reply(message, "⚠️ Something went wrong. Please send your file again.")


# ----------------------------------------------------------------------
def register_choice(app):
    app.add_handler(MessageHandler(detect_file, supported_files))
    app.add_handler(
        MessageHandler(trim_time_handler, filters.text & ~filters.command("start")),
        group=1,
    )
    app.add_handler(
        CallbackQueryHandler(
            choice_callback,
            filters.regex(
                r"^choice:(zip|zipdone|trim|clips|merge|mergedone|audio|close)$"
            ),
        )
    )
