"""Multi-clip cutting engine.
file name - logic/clips.py

Responsibilities:
- Parse time ranges such as "00:10 to 00:20" (one per line, max 7).
- Cut several clips out of one audio or video file.
- Report progress through an optional callback.
- Support cancellation through an optional callback.
- Create temporary output files and clean them up.

This module does not handle Telegram messages or buttons.
"""

import asyncio
import json
import math
import os
import re
import shutil
import tempfile
from pathlib import Path


FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

MAX_CLIPS = 7
PROCESS_TIMEOUT = 900  # 15 minutes per clip

# Audio containers where the stream can be copied without re-encoding.
AUDIO_COPY_EXTENSIONS = {
    ".mp3", ".wav", ".flac", ".aac", ".m4a", ".ogg", ".opus", ".wma",
}

_TIME_RE = re.compile(r"^\d+(?::\d{1,2}){0,2}(?:\.\d+)?$")
_SEPARATOR_RE = re.compile(r"\s*(?:\bto\b|->|→|—|–|-)\s*", re.IGNORECASE)


class ClipError(Exception):
    """Raised when clip parsing or cutting fails."""


class ClipCancelled(ClipError):
    """Raised when clip cutting is cancelled."""


def _check_cancelled(cancel_check):
    """Stop the operation when the caller requests cancellation."""
    if cancel_check and cancel_check():
        raise ClipCancelled("Clip cutting was cancelled.")


# ----------------------------------------------------------------------
# Time parsing
# ----------------------------------------------------------------------
def format_time(seconds):
    """Format seconds as HH:MM:SS (with decimals only when needed)."""
    centi = int(round(max(0.0, float(seconds)) * 100))
    whole, cs = divmod(centi, 100)
    hours, rest = divmod(whole, 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{hours:02d}:{minutes:02d}:{secs:02d}"
    if cs:
        text += f".{cs:02d}".rstrip("0")
    return text


def parse_time(value):
    """Convert 'SS', 'MM:SS' or 'HH:MM:SS' (decimals allowed) to seconds."""
    text = str(value).strip()

    if not _TIME_RE.match(text):
        raise ClipError(f"Invalid time `{text}`. Use 00:10, 01:30 or 90.")

    parts = text.split(":")

    if len(parts) > 1 and any(float(part) >= 60 for part in parts[1:]):
        raise ClipError(f"Invalid time `{text}`. Minutes and seconds must be below 60.")

    seconds = 0.0

    for part in parts:
        seconds = seconds * 60 + float(part)

    return seconds


def parse_clip_ranges(text, max_clips=MAX_CLIPS):
    """Parse lines like '00:10 to 00:20' into [(start, end), ...].

    Ranges may be separated by new lines or semicolons. The start and end
    can be separated by 'to', '-', '->' or an arrow.
    """

    lines = [
        line.strip()
        for line in re.split(r"[\n;]+", text or "")
        if line.strip()
    ]

    if not lines:
        raise ClipError("Send at least one range, like `00:10 to 00:20`.")

    if len(lines) > max_clips:
        raise ClipError(f"You can send up to {max_clips} ranges at once.")

    ranges = []

    for number, line in enumerate(lines, start=1):
        parts = [part for part in _SEPARATOR_RE.split(line) if part.strip()]

        if len(parts) != 2:
            raise ClipError(
                f"Line {number}: use the format `00:10 to 00:20`."
            )

        try:
            start = parse_time(parts[0])
            end = parse_time(parts[1])
        except ClipError as exc:
            raise ClipError(f"Line {number}: {exc}") from exc

        if end <= start:
            raise ClipError(
                f"Line {number}: end time must be after start time."
            )

        ranges.append((start, end))

    return ranges


def validate_ranges(ranges, duration):
    """Check every range against the media duration."""

    valid = []

    for number, (start, end) in enumerate(ranges, start=1):
        if start >= duration:
            raise ClipError(
                f"Clip {number}: start `{format_time(start)}` is beyond "
                f"the file length (`{format_time(duration)}`)."
            )

        if end > duration:
            if end - duration > 1:
                raise ClipError(
                    f"Clip {number}: end `{format_time(end)}` is beyond "
                    f"the file length (`{format_time(duration)}`)."
                )

            end = duration

        valid.append((start, end))

    return valid


# ----------------------------------------------------------------------
# FFmpeg helpers
# ----------------------------------------------------------------------
async def probe_source(input_path):
    """Return (duration, has_video, has_audio) for a media file."""

    command = [
        FFPROBE,
        "-v", "error",
        "-show_entries",
        "format=duration:stream=codec_type:stream_disposition=attached_pic",
        "-of", "json",
        str(input_path),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise ClipError(
            "FFprobe could not start. Check your FFmpeg installation."
        ) from exc

    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=30)

    except asyncio.TimeoutError as exc:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise ClipError("Media inspection timed out.") from exc

    if process.returncode != 0:
        raise ClipError("This file could not be read as audio or video.")

    try:
        data = json.loads(stdout.decode(errors="replace") or "{}")
        duration = float(data.get("format", {}).get("duration", 0))
    except (ValueError, TypeError, AttributeError):
        data = {}
        duration = 0.0

    if not math.isfinite(duration) or duration <= 0:
        raise ClipError("Could not read the media duration.")

    streams = data.get("streams", []) or []

    has_video = any(
        stream.get("codec_type") == "video"
        and not (stream.get("disposition") or {}).get("attached_pic")
        for stream in streams
    )
    has_audio = any(stream.get("codec_type") == "audio" for stream in streams)

    if not has_video and not has_audio:
        raise ClipError("This file has no audio or video stream.")

    return duration, has_video, has_audio


async def _stop_process(process):
    """Stop FFmpeg safely when an operation is cancelled or fails."""

    if process is None or process.returncode is not None:
        return

    try:
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=2)

    except asyncio.TimeoutError:
        process.kill()
        await process.wait()

    except ProcessLookupError:
        pass


async def _run_ffmpeg(
    command,
    on_seconds=None,
    process_callback=None,
    cancel_check=None,
):
    """Run one FFmpeg command and report elapsed output seconds."""

    process = None

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        if process_callback:
            process_callback(process)

        async def read_progress():
            while True:
                line = await process.stdout.readline()

                if not line:
                    break

                _check_cancelled(cancel_check)

                text = line.decode(errors="replace").strip()

                if text.startswith("out_time_ms="):
                    try:
                        elapsed = int(text.split("=", 1)[1]) / 1_000_000
                    except ValueError:
                        continue

                    if on_seconds:
                        on_seconds(elapsed)

        async def read_errors():
            # Drain stderr concurrently to avoid blocking FFmpeg.
            return await process.stderr.read()

        await asyncio.wait_for(
            asyncio.gather(read_progress(), read_errors()),
            timeout=PROCESS_TIMEOUT,
        )

        return_code = await process.wait()

        _check_cancelled(cancel_check)

        if return_code != 0:
            raise ClipError("FFmpeg could not cut this clip.")

    except (ClipCancelled, asyncio.CancelledError):
        await _stop_process(process)
        raise

    except asyncio.TimeoutError as exc:
        await _stop_process(process)
        raise ClipError("Clip cutting timed out.") from exc

    except ClipError:
        await _stop_process(process)
        raise

    except OSError as exc:
        await _stop_process(process)
        raise ClipError("Could not run FFmpeg.") from exc


async def _cut_one(
    input_path,
    output_path,
    start,
    end,
    has_video,
    on_seconds=None,
    process_callback=None,
    cancel_check=None,
):
    """Cut one clip. Video is re-encoded for accuracy, audio is copied."""

    length = end - start
    suffix = Path(output_path).suffix.lower()

    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel", "error",
        "-nostdin",
        "-y",
        "-ss", f"{start:.3f}",
        "-i", str(input_path),
        "-t", f"{length:.3f}",
    ]

    if has_video:
        command += [
            "-map", "0:v:0",
            "-map", "0:a:0?",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "23",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-movflags", "+faststart",
        ]
    elif suffix in AUDIO_COPY_EXTENSIONS:
        command += ["-map", "0:a:0", "-vn", "-c:a", "copy"]
    else:
        command += [
            "-map", "0:a:0",
            "-vn",
            "-c:a", "libmp3lame",
            "-b:a", "192k",
        ]

    command += [
        "-sn",
        "-dn",
        "-progress", "pipe:1",
        "-nostats",
        str(output_path),
    ]

    await _run_ffmpeg(
        command,
        on_seconds=on_seconds,
        process_callback=process_callback,
        cancel_check=cancel_check,
    )

    if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
        raise ClipError("A cut clip came out empty.")


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
async def cut_clips_in_temp(
    input_path,
    ranges,
    progress_callback=None,
    cancel_check=None,
    process_callback=None,
):
    """Cut every (start, end) range into its own file.

    Args:
        input_path: Path to the source audio/video file.
        ranges: List of (start_seconds, end_seconds), max MAX_CLIPS.
        progress_callback: Optional callback(percent, detail).
        cancel_check: Optional callback returning True if cancelled.
        process_callback: Optional callback(process) for each FFmpeg run,
            so the caller can attach it to a cancel button.

    Returns:
        (list_of_clip_paths, temporary_directory)

    The caller must clean up the temporary directory after sending.
    """

    input_path = Path(input_path)

    if not input_path.is_file():
        raise ClipError("Input file does not exist.")

    if not ranges:
        raise ClipError("No time ranges were provided.")

    if len(ranges) > MAX_CLIPS:
        raise ClipError(f"You can cut up to {MAX_CLIPS} clips at once.")

    duration, has_video, _ = await probe_source(input_path)
    _check_cancelled(cancel_check)

    ranges = validate_ranges(ranges, duration)

    if has_video:
        extension = ".mp4"
    elif input_path.suffix.lower() in AUDIO_COPY_EXTENSIONS:
        extension = input_path.suffix.lower()
    else:
        extension = ".mp3"

    total_length = sum(end - start for start, end in ranges)
    count = len(ranges)
    temp_dir = tempfile.mkdtemp(prefix="clips_")

    try:
        outputs = []
        done_length = 0.0

        if progress_callback:
            progress_callback(0, f"Cutting clip 1/{count}...")

        for index, (start, end) in enumerate(ranges, start=1):
            _check_cancelled(cancel_check)

            length = end - start
            output_path = os.path.join(temp_dir, f"clip_{index}{extension}")

            def on_seconds(elapsed, base=done_length, length=length, index=index):
                if not progress_callback or total_length <= 0:
                    return

                percent = int((base + min(elapsed, length)) / total_length * 100)
                progress_callback(
                    min(99, percent),
                    f"Cutting clip {index}/{count}...",
                )

            await _cut_one(
                input_path,
                output_path,
                start,
                end,
                has_video,
                on_seconds=on_seconds,
                process_callback=process_callback,
                cancel_check=cancel_check,
            )

            outputs.append(output_path)
            done_length += length

        if progress_callback:
            progress_callback(100, "Clips ready.")

        return outputs, temp_dir

    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def cleanup_temp(temp_dir):
    """Remove temporary clip files."""
    shutil.rmtree(temp_dir, ignore_errors=True)
