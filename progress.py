"""
file name - progress.py
Reusable progress UI and cancellation manager for Pyrogram handlers.

Other features can create a ProgressTracker, update its percentage, attach a
subprocess, and cancel it from a /cancel command (see choice.py).

There is no Cancel button any more. Cancelling is done by tracker.cancel(),
which:
- sets the cancel flag (logic modules poll it through cancel_check),
- stops the attached FFmpeg process,
- HARD-cancels the running job task (tracker.job_task), so the job stops
  even if it is stuck in a step that never checks the flag,
- removes temporary files and shows the final message.
"""
import asyncio
import logging
import os
import re
import shutil
import time
import uuid

from pyrogram.errors import FloodWait, MessageNotModified

logger = logging.getLogger(__name__)

active_progress = {}

REFRESH_INTERVAL = 3   # seconds between automatic card updates
BAR_WIDTH = 14
CANCELLED_TEXT = "🛑 **ᴛᴀꜱᴋ ᴄᴀɴᴄᴇʟᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ**."

_TITLE_ICONS = (
    ("zip", "📦"),
    ("trim", "✂️"),
    ("clip", "🎞"),
    ("merg", "🔗"),
    ("audio", "🎵"),
    ("watermark", "💧"),
    ("rename", "✏️"),
    ("download", "📥"),
    ("upload", "📤"),
)

_MD_TRANSLATE = str.maketrans({
    "`": "'", "*": "∗", "~": "∼", "|": "¦", "[": "(", "]": ")",
})


# ----------------------------------------------------------------------
# Formatting helpers
# ----------------------------------------------------------------------
def _safe_text(text):
    """Make text safe to place inside a Markdown message."""
    text = str(text).translate(_MD_TRANSLATE)
    return re.sub(r"_{2,}", "_", text)


def _pick_icon(title):
    lowered = str(title).lower()
    for key, icon in _TITLE_ICONS:
        if key in lowered:
            return icon
    return "⏳"


def format_duration(seconds):
    seconds = max(0, int(seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def fmt_size(num):
    num = float(num or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if num < 1024 or unit == "GB":
            return f"{int(num)} B" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024


def progress_bar(percent, width=BAR_WIDTH):
    """Return a bar such as '▰▰▰▰▱▱▱▱ 50%'."""
    percent = max(0, min(100, int(percent)))
    filled = round(width * percent / 100)
    return f"{'▰' * filled}{'▱' * (width - filled)} {percent}%"


def get_active_tracker(owner_id):
    """Return the running tracker that belongs to this user, if any."""
    for tracker in list(active_progress.values()):
        if tracker.owner_id == owner_id:
            return tracker
    return None


class ProgressTracker:
    def __init__(self, client, message, owner_id, title="Processing", icon=None):
        self.client = client
        self.message = message
        self.owner_id = owner_id
        self.title = title
        self.icon = icon or _pick_icon(title)
        self.task_id = uuid.uuid4().hex[:10]
        self.percent = 0
        self.detail = "Preparing..."
        self.cancelled = asyncio.Event()
        self.process = None
        self.job_task = None      # the asyncio task running the job
        self.temp_paths = set()
        self.started_at = time.monotonic()
        self._refresh_task = None
        self._finished = False
        self._cancel_callback = None
        self._edit_lock = asyncio.Lock()
        self._last_text = None
        self._blocked_until = 0.0
        active_progress[self.task_id] = self

    # ------------------------------------------------------------------
    # Card (message) rendering
    # ------------------------------------------------------------------
    def elapsed(self):
        return time.monotonic() - self.started_at

    def _build_text(self):
        percent = self.percent
        elapsed = self.elapsed()

        footer = f"⏱ `{format_duration(elapsed)}`"
        if 10 <= percent < 100 and elapsed >= 5:
            eta = elapsed * (100 - percent) / percent
            footer += f"   ⏳ ~`{format_duration(eta)}` left"

        filled = round(BAR_WIDTH * percent / 100)
        bar = f"{'▰' * filled}{'▱' * (BAR_WIDTH - filled)}"

        return (
            f"{self.icon} **{_safe_text(self.title)}**\n\n"
            f"{bar}  **{percent}%**\n\n"
            f"📍 {_safe_text(self.detail)}\n"
            f"{footer}\n\n"
            f"🛑 Send /cancel to stop"
        )

    async def _edit(self, text, final=False, retry=None):
        """The only place that edits the card.

        Edits are serialised with a lock. Normal (non-final) edits are
        skipped once the tracker is finished/cancelled, when the text is
        unchanged, or while Telegram is rate-limiting us.
        """
        if retry is None:
            retry = final

        async with self._edit_lock:
            for attempt in range(2 if retry else 1):
                if not final:
                    if self._finished or self.cancelled.is_set():
                        return
                    if text == self._last_text:
                        return
                    if time.monotonic() < self._blocked_until:
                        return

                try:
                    await self.message.edit_text(text)
                    self._last_text = text
                    return
                except MessageNotModified:
                    self._last_text = text
                    return
                except FloodWait as exc:
                    wait = int(getattr(exc, "value", None) or getattr(exc, "x", 5)) + 1
                    self._blocked_until = time.monotonic() + wait
                    if attempt == 1 or not retry:
                        return
                    await asyncio.sleep(min(wait, 10))
                except Exception:
                    # The message may already be deleted.
                    return

    async def start(self):
        await self.render()
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        return self

    async def render(self):
        if self._finished or self.cancelled.is_set():
            return
        await self._edit(self._build_text())

    async def _refresh_loop(self):
        try:
            while not self._finished and not self.cancelled.is_set():
                await asyncio.sleep(REFRESH_INTERVAL)
                if not self._finished and not self.cancelled.is_set():
                    await self.render()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Progress refresh loop crashed")

    # ------------------------------------------------------------------
    # Progress state
    # ------------------------------------------------------------------
    def set_progress(self, percent=None, detail=None):
        """Update the bar. The percentage never moves backwards."""
        if percent is not None:
            value = max(0, min(100, int(percent)))
            self.percent = max(self.percent, value)
        if detail is not None:
            self.detail = str(detail)

    def reset_progress(self, percent=0, detail=None):
        """Explicitly move the bar backwards (e.g. for a new phase)."""
        self.percent = max(0, min(100, int(percent)))
        if detail is not None:
            self.detail = str(detail)

    # ------------------------------------------------------------------
    # Adapters for logic modules (zip.py, audio_extractor.py, ...)
    # ------------------------------------------------------------------
    def make_callback(self, start=0, end=100):
        """Return a sync callback(percent, detail) for logic modules.

        The module's 0-100% is mapped into the start-end range of the
        overall progress bar. Safe to call from worker threads, because it
        only sets plain attributes; the refresh loop does the rendering.
        """
        span = end - start

        def callback(percent, detail=None):
            percent = max(0, min(100, percent or 0))
            self.set_progress(start + span * percent / 100, detail)

        return callback

    def cancel_check(self):
        """Pass as cancel_check= to logic modules."""
        return self.is_cancelled

    def make_transfer_callback(self, start=0, end=100, label="Downloading"):
        """Return an async (current, total) callback for Pyrogram's
        download_media / send_* progress= argument.

        Shows size and speed, and stops the transfer when cancelled.
        """
        span = end - start
        state = {"t0": None, "c0": 0}

        async def callback(current, total):
            if self.is_cancelled:
                # This RAISES StopTransmission, which Pyrogram uses to abort
                # the running download/upload. It must never be caught here.
                stop = getattr(self.client, "stop_transmission", None)
                if stop is not None:
                    stop()
                return

            if not total:
                return

            now = time.monotonic()
            if state["t0"] is None:
                state["t0"], state["c0"] = now, current

            elapsed = now - state["t0"]
            speed = (current - state["c0"]) / elapsed if elapsed > 1 else 0

            text = f"{label}... {fmt_size(current)} / {fmt_size(total)}"
            if speed > 0:
                text += f" • {fmt_size(speed)}/s"

            self.set_progress(start + span * current / total, text)

        return callback

    def add_temp_path(self, path):
        if path:
            self.temp_paths.add(str(path))

    def attach_process(self, process):
        self.process = process

    def set_cancel_callback(self, callback):
        self._cancel_callback = callback

    # ------------------------------------------------------------------
    # Cancel / finish
    # ------------------------------------------------------------------
    async def cancel(self):
        if self._finished or self.cancelled.is_set():
            return
        self.cancelled.set()
        self.detail = "Cancelling..."

        # Show the "cancelling" state immediately. Don't wait on rate limits.
        await self._edit(
            f"🛑 **Cancelling...**\n\n"
            f"Stopping **{_safe_text(self.title)}** and clearing temporary files.",
            final=True,
            retry=False,
        )

        # Stop FFmpeg (if one is attached).
        process = self.process
        if process and process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=2)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    process.kill()
                    await process.wait()
                except ProcessLookupError:
                    pass
            except Exception:
                pass

        # HARD cancel: interrupt the job wherever it is waiting (download,
        # upload, FFmpeg, ...), instead of hoping it checks the flag soon.
        job = self.job_task
        if job is not None and job is not asyncio.current_task() and not job.done():
            job.cancel()

        # Remove registered temp paths immediately; workflow finally blocks
        # also clean them safely (ignore_errors=True).
        for path in list(self.temp_paths):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                elif os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass

        if self._cancel_callback:
            try:
                result = self._cancel_callback()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Cancel callback failed")

        await self.finish(CANCELLED_TEXT, show_time=False)

    async def finish(self, final_text=None, show_time=None):
        """Show the final message. Safe to call more than once."""
        if self._finished:
            return
        self._finished = True
        if self._refresh_task and self._refresh_task is not asyncio.current_task():
            self._refresh_task.cancel()
        active_progress.pop(self.task_id, None)

        if final_text:
            if show_time is None:
                show_time = final_text.lstrip().startswith("✅")
            if show_time:
                final_text += f"\n\n⏱ Completed in `{format_duration(self.elapsed())}`"
            await self._edit(final_text, final=True)

    @property
    def is_cancelled(self):
        return self.cancelled.is_set()


def register_progress(app):
    """Kept so existing app.py code still works.

    The Cancel button was removed, so there is nothing to register here.
    The /cancel command is registered by register_choice(app).
    """
    return None
