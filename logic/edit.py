"""Video + audio editor engine.
file name - logic/edit.py

Responsibilities:
- Put an audio track into a video file (the video may be the file the
  user sent first, or the one sent second - choice.py decides the order).
- Two modes:
    "replace" -> the new audio replaces the original sound.
    "mix"     -> the new audio plays together with the original sound.
- NEVER trim anything. The result is as long as the LONGER of the two:
    * audio longer than video -> the last video frame is held (frozen)
      until the audio ends.
    * video longer than audio -> the audio is followed by silence
      (in "mix" mode the original sound simply continues).
- Copy the video stream without re-encoding when it is safe (H.264 and
  no extension needed), so most jobs finish very fast.
- Report progress through an optional callback.
- Support cancellation through an optional callback / process callback.
- Create temporary output files and clean them up on every failure.

This module does not handle Telegram messages or buttons.
Connected to: choice.py (edit_worker) and progress.py (callbacks).
"""

import asyncio
import json
import logging
import math
import os
import shutil
import tempfile
from pathlib import Path


logger = logging.getLogger(__name__)

FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

MODE_REPLACE = "replace"
MODE_MIX = "mix"
MODES = (MODE_REPLACE, MODE_MIX)

PROBE_TIMEOUT = 30
PROCESS_TIMEOUT = 3600                # 1 hour for the whole FFmpeg run
LENGTH_TOLERANCE = 0.05               # seconds; smaller gaps count as "same"
MAX_OUTPUT_SECONDS = 3 * 3600         # refuse absurdly long results
MAX_OUTPUT_BYTES = 2000 * 1024 ** 2   # Telegram bot upload limit (~2 GB)

# Length relation between the two inputs (returned in the result).
SAME_LENGTH = "same"
AUDIO_LONGER = "audio_longer"
VIDEO_LONGER = "video_longer"


class EditError(Exception):
    """Raised when adding audio to a video fails."""


class EditCancelled(EditError):
    """Raised when the operation is cancelled."""


def _check_cancelled(cancel_check):
    """Stop the operation when the caller requests cancellation."""
    if cancel_check and cancel_check():
        raise EditCancelled("Editing was cancelled.")


def format_length(seconds):
    """Return 12.5 -> '00:12', 3725 -> '1:02:05' (for captions)."""
    try:
        seconds = max(0, int(round(float(seconds))))
    except (TypeError, ValueError, OverflowError):
        seconds = 0
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


# ----------------------------------------------------------------------
# Probing
# ----------------------------------------------------------------------
def _parse_duration(value):
    """Return a positive finite float, or 0.0 when the value is unusable."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number) or number <= 0:
        return 0.0
    return number


async def _stop_process(process):
    """Stop FFmpeg/FFprobe safely when an operation is cancelled or fails."""
    if process is None or process.returncode is not None:
        return

    try:
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=2)

    except asyncio.TimeoutError:
        try:
            process.kill()
            await process.wait()
        except ProcessLookupError:
            pass

    except ProcessLookupError:
        pass


async def _probe(path, label):
    """Inspect one input file.

    Returns a dict with: duration, video_index, video_codec, audio_index.
    video_index / audio_index are None when that stream type is missing.
    Cover art (attached pictures) is NOT counted as a video stream.
    """
    command = [
        FFPROBE,
        "-v", "error",
        "-show_entries",
        "format=duration:stream=index,codec_type,codec_name,duration"
        ":stream_disposition=attached_pic",
        "-of", "json",
        str(path),
    ]

    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(
            process.communicate(), timeout=PROBE_TIMEOUT
        )

    except asyncio.TimeoutError as exc:
        await _stop_process(process)
        raise EditError(f"Inspecting the {label} timed out.") from exc

    except asyncio.CancelledError:
        await _stop_process(process)
        raise

    except OSError as exc:
        raise EditError(
            "FFprobe could not start. Check your FFmpeg installation."
        ) from exc

    if process.returncode != 0:
        raise EditError(f"The {label} could not be read. It may be damaged.")

    try:
        data = json.loads(stdout.decode(errors="replace") or "{}")
    except (ValueError, TypeError):
        data = {}

    if not isinstance(data, dict):
        data = {}

    streams = data.get("streams") or []
    video_index = video_codec = audio_index = None
    stream_duration = 0.0

    for stream in streams:
        if not isinstance(stream, dict):
            continue

        kind = stream.get("codec_type")
        index = stream.get("index")

        if not isinstance(index, int):
            continue

        if kind == "video":
            if (stream.get("disposition") or {}).get("attached_pic"):
                continue  # cover art, not a real video
            if video_index is None:
                video_index = index
                video_codec = stream.get("codec_name")

        elif kind == "audio":
            if audio_index is None:
                audio_index = index

        stream_duration = max(
            stream_duration, _parse_duration(stream.get("duration"))
        )

    format_info = data.get("format")
    duration = _parse_duration(
        format_info.get("duration") if isinstance(format_info, dict) else None
    )
    if duration <= 0:
        duration = stream_duration   # some containers only store it per stream

    if duration <= 0:
        raise EditError(f"Could not read the length of the {label}.")

    return {
        "duration": duration,
        "video_index": video_index,
        "video_codec": video_codec,
        "audio_index": audio_index,
    }


# ----------------------------------------------------------------------
# Command building
# ----------------------------------------------------------------------
_AUDIO_FORMAT = (
    "aresample=44100,"
    "aformat=sample_fmts=fltp:channel_layouts=stereo,"
    "asetpts=PTS-STARTPTS"
)


def _build_command(
    video_path, audio_path, output_path,
    video_info, audio_info, mode, output_duration, extra_video,
):
    """Build the FFmpeg command.

    extra_video: seconds the last video frame must be held (0 = none).
    Returns (command, copy_video).
    """
    audio_index = audio_info["audio_index"]
    video_index = video_info["video_index"]

    # The video stream is copied (fast, lossless) only when nothing has to
    # be added to it and it is H.264, which every MP4 player understands.
    copy_video = extra_video <= 0 and video_info["video_codec"] == "h264"

    # Every audio branch is padded with silence up to the final length, so
    # the audio never ends before the video and nothing is ever cut off.
    pad = f"apad=whole_dur={output_duration:.3f}"
    parts = []

    if mode == MODE_MIX:
        parts.append(f"[1:{audio_index}]{_AUDIO_FORMAT},{pad}[anew]")
        parts.append(
            f"[0:{video_info['audio_index']}]{_AUDIO_FORMAT},{pad}[aold]"
        )
        # Both inputs stay active for the full length, so amix always
        # halves them; volume=2 restores the level and the limiter
        # prevents clipping.
        parts.append(
            "[aold][anew]amix=inputs=2:duration=longest:"
            "dropout_transition=0,volume=2,alimiter=limit=0.95[a]"
        )
    else:
        parts.append(f"[1:{audio_index}]{_AUDIO_FORMAT},{pad}[a]")

    if not copy_video:
        chain = f"[0:{video_index}]setpts=PTS-STARTPTS,"
        if extra_video > 0:
            chain += f"tpad=stop_mode=clone:stop_duration={extra_video:.3f},"
        # libx264 + yuv420p need even width and height.
        chain += "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p[v]"
        parts.append(chain)

    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel", "error",
        "-nostdin",
        "-y",
        "-i", str(video_path),
        "-i", str(audio_path),
        "-filter_complex", ";".join(parts),
    ]

    if copy_video:
        command += ["-map", f"0:{video_index}", "-map", "[a]", "-c:v", "copy"]
    else:
        command += [
            "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "23",
            "-pix_fmt", "yuv420p",
        ]

    command += [
        "-c:a", "aac",
        "-b:a", "192k",
        "-movflags", "+faststart",
        # Safety net only (half a second above the real length): it can
        # never cut real content, but guarantees FFmpeg always stops.
        "-t", f"{output_duration + 0.5:.3f}",
        "-progress", "pipe:1",
        "-nostats",
        str(output_path),
    ]

    return command, copy_video


# ----------------------------------------------------------------------
# FFmpeg runner
# ----------------------------------------------------------------------
def _friendly_ffmpeg_error(stderr_text):
    lowered = (stderr_text or "").lower()
    if "no space left" in lowered:
        return "The server ran out of disk space. Please try a smaller file."
    if "invalid data found" in lowered or "moov atom not found" in lowered:
        return "One of the files is damaged or incomplete."
    return (
        "FFmpeg could not combine these files. "
        "They may be damaged or in an unsupported format."
    )


async def _run_ffmpeg(
    command,
    total_duration,
    progress_callback=None,
    process_callback=None,
    cancel_check=None,
):
    """Run FFmpeg, report progress (0-99) and enforce timeout/cancel."""
    process = None
    errors = {"text": ""}

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

                if text.startswith(("out_time_us=", "out_time_ms=")):
                    try:
                        elapsed = int(text.split("=", 1)[1]) / 1_000_000
                    except ValueError:
                        continue   # "N/A" at the start of a run

                    if progress_callback and total_duration > 0:
                        percent = min(99, max(0, int(elapsed / total_duration * 100)))
                        progress_callback(percent, "Adding audio to video...")

        async def read_errors():
            # Drain stderr concurrently so FFmpeg can never block on it.
            data = await process.stderr.read()
            errors["text"] = data.decode(errors="replace")[-1500:]

        await asyncio.wait_for(
            asyncio.gather(read_progress(), read_errors()),
            timeout=PROCESS_TIMEOUT,
        )

        return_code = await process.wait()

        _check_cancelled(cancel_check)

        if return_code != 0:
            logger.warning("FFmpeg failed (code %s): %s", return_code, errors["text"])
            raise EditError(_friendly_ffmpeg_error(errors["text"]))

    except (EditCancelled, asyncio.CancelledError):
        await _stop_process(process)
        raise

    except asyncio.TimeoutError as exc:
        await _stop_process(process)
        raise EditError("Editing took too long and was stopped.") from exc

    except EditError:
        await _stop_process(process)
        raise

    except OSError as exc:
        await _stop_process(process)
        raise EditError("Could not run FFmpeg.") from exc


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
async def add_audio_to_video(
    video_path,
    audio_path,
    output_path,
    mode=MODE_REPLACE,
    progress_callback=None,
    cancel_check=None,
    process_callback=None,
):
    """Add the audio of `audio_path` to the video of `video_path`.

    Args:
        video_path: File that supplies the picture.
        audio_path: File that supplies the new sound (an audio file, or a
            video file whose first audio track is used).
        output_path: Destination path ending in .mp4.
        mode: "replace" or "mix".
        progress_callback: Optional callback(percent, detail).
        cancel_check: Optional callback returning True if cancelled.
        process_callback: Optional callback(process) so the caller can
            terminate FFmpeg from outside.

    Returns a dict:
        output_path, video_duration, audio_duration, output_duration,
        length_relation (SAME_LENGTH / AUDIO_LONGER / VIDEO_LONGER),
        mode_used ("replace" / "mix"), video_copied (bool).
    """
    video_path = Path(video_path)
    audio_path = Path(audio_path)
    output_path = Path(output_path)

    if mode not in MODES:
        raise EditError("Unknown mode. Choose Replace or Mix.")

    if not video_path.is_file():
        raise EditError("The video file does not exist.")

    if not audio_path.is_file():
        raise EditError("The audio file does not exist.")

    resolved_out = output_path.resolve()
    if resolved_out in (video_path.resolve(), audio_path.resolve()):
        raise EditError("Input and output paths must be different.")

    if output_path.suffix.lower() != ".mp4":
        raise EditError("Output filename must end in .mp4.")

    _check_cancelled(cancel_check)
    video_info = await _probe(video_path, "video file")
    _check_cancelled(cancel_check)
    audio_info = await _probe(audio_path, "audio file")
    _check_cancelled(cancel_check)

    if video_info["video_index"] is None:
        raise EditError("The video file has no picture (video stream).")

    if audio_info["audio_index"] is None:
        raise EditError("The audio file has no sound (audio stream).")

    video_duration = video_info["duration"]
    audio_duration = audio_info["duration"]
    output_duration = max(video_duration, audio_duration)

    if output_duration > MAX_OUTPUT_SECONDS:
        raise EditError(
            "The result would be longer than "
            f"{MAX_OUTPUT_SECONDS // 3600} hours, which is not supported."
        )

    # How do the lengths compare? Nothing is ever trimmed.
    if audio_duration > video_duration + LENGTH_TOLERANCE:
        relation = AUDIO_LONGER
        extra_video = audio_duration - video_duration
    elif video_duration > audio_duration + LENGTH_TOLERANCE:
        relation = VIDEO_LONGER
        extra_video = 0.0
    else:
        relation = SAME_LENGTH
        extra_video = 0.0

    # "Mix" needs original sound; without it, mixing equals replacing.
    mode_used = mode
    if mode == MODE_MIX and video_info["audio_index"] is None:
        mode_used = MODE_REPLACE

    command, copy_video = _build_command(
        video_path, audio_path, output_path,
        video_info, audio_info, mode_used, output_duration, extra_video,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if progress_callback:
        progress_callback(0, "Adding audio to video...")

    try:
        await _run_ffmpeg(
            command,
            output_duration,
            progress_callback=progress_callback,
            process_callback=process_callback,
            cancel_check=cancel_check,
        )

        if not output_path.is_file() or output_path.stat().st_size == 0:
            raise EditError("The edited file is empty.")

        # Final check: the result must really contain picture AND sound.
        checked = await _probe(output_path, "edited file")
        if checked["video_index"] is None or checked["audio_index"] is None:
            raise EditError("The edited file is incomplete.")

    except BaseException:
        try:
            output_path.unlink()
        except OSError:
            pass
        raise

    if progress_callback:
        progress_callback(100, "Audio added.")

    return {
        "output_path": str(output_path),
        "video_duration": video_duration,
        "audio_duration": audio_duration,
        "output_duration": output_duration,
        "length_relation": relation,
        "mode_used": mode_used,
        "video_copied": copy_video,
    }


async def add_audio_in_temp(
    video_path,
    audio_path,
    mode=MODE_REPLACE,
    progress_callback=None,
    cancel_check=None,
    process_callback=None,
):
    """Run add_audio_to_video() inside a temporary directory.

    Returns:
        (result_dict, temporary_directory)

    result_dict["output_path"] is the finished .mp4. The caller must clean
    up the temporary directory after sending the file. On ANY failure
    (including cancellation) the directory is removed here.
    """
    temp_dir = tempfile.mkdtemp(prefix="edit_")
    output_path = os.path.join(temp_dir, "edited.mp4")

    try:
        result = await add_audio_to_video(
            video_path,
            audio_path,
            output_path,
            mode=mode,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            process_callback=process_callback,
        )
        return result, temp_dir

    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def cleanup_temp(temp_dir):
    """Remove temporary edit files."""
    shutil.rmtree(temp_dir, ignore_errors=True)
