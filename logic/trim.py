"""Accurate audio/video trimming with cancellable asynchronous FFmpeg."""
import asyncio
import json
import math
import os
import shutil
import tempfile
from pathlib import Path

FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"
PROCESS_TIMEOUT = 900


class TrimError(Exception):
    """Raised when media trimming fails."""


class TrimCancelled(TrimError):
    """Raised when a trim process was cancelled."""


def parse_time(value):
    value = str(value).strip()
    try:
        if not value:
            raise ValueError
        parts = value.split(":")
        if len(parts) == 1:
            seconds = float(parts[0])
        elif len(parts) == 2:
            minutes = int(parts[0])
            sec = float(parts[1])
            if minutes < 0 or not 0 <= sec < 60:
                raise ValueError
            seconds = minutes * 60 + sec
        elif len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            sec = float(parts[2])
            if hours < 0 or not 0 <= minutes < 60 or not 0 <= sec < 60:
                raise ValueError
            seconds = hours * 3600 + minutes * 60 + sec
        else:
            raise ValueError
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError
        return seconds
    except (ValueError, OverflowError):
        raise TrimError("Invalid time. Use HH:MM:SS, MM:SS, or seconds.")


async def _run_capture(command, timeout=30):
    try:
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError as exc:
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass
        raise TrimError("Media inspection timed out.") from exc
    except OSError as exc:
        raise TrimError("FFmpeg/FFprobe could not be started.") from exc
    if proc.returncode != 0:
        raise TrimError("Could not read this media file.")
    return stdout.decode(errors="replace"), stderr.decode(errors="replace")


async def probe_media(input_path):
    command = [
        FFPROBE, "-v", "error", "-show_entries",
        "format=duration:stream=codec_type", "-of", "json", str(input_path),
    ]
    stdout, _ = await _run_capture(command)
    try:
        data = json.loads(stdout)
        duration = float(data["format"]["duration"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise TrimError("The media duration is unavailable.")
    if not math.isfinite(duration) or duration <= 0:
        raise TrimError("The media duration is invalid.")
    stream_types = {s.get("codec_type") for s in data.get("streams", [])}
    if not stream_types.intersection({"audio", "video"}):
        raise TrimError("This file has no audio or video stream.")
    return duration, stream_types


def validate_range(start, end, duration):
    if start < 0:
        raise TrimError("Start time cannot be negative.")
    if end <= start:
        raise TrimError("End time must be after start time.")
    if end > duration:
        raise TrimError(f"End time exceeds media duration ({duration:.2f}s).")


async def trim_media(input_path, output_path, start, end, tracker=None):
    """Trim accurately; tracker is optional and supports cancellation."""
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.is_file():
        raise TrimError("Input file does not exist.")
    if input_path.resolve() == output_path.resolve():
        raise TrimError("Input and output paths must be different.")

    duration, stream_types = await probe_media(input_path)
    validate_range(float(start), float(end), duration)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() != ".mp4":
        raise TrimError("Output filename must end in .mp4.")

    command = [
        FFMPEG, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
        "-i", str(input_path),
        "-ss", str(float(start)),
        "-t", str(float(end) - float(start)),
        "-map", "0:v?", "-map", "0:a?", "-sn", "-dn",
    ]
    if "video" in stream_types:
        command.extend([
            "-c:v", "libx264", "-preset", "medium", "-crf", "18",
            "-pix_fmt", "yuv420p",
        ])
    if "audio" in stream_types:
        command.extend(["-c:a", "aac", "-b:a", "192k"])
    command.extend(["-movflags", "+faststart", "-progress", "pipe:1", "-nostats", str(output_path)])

    try:
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise TrimError("FFmpeg could not be started. Check your deployment.") from exc

    if tracker:
        tracker.attach_process(proc)
        tracker.set_progress(1, "Trimming media...")

    async def read_progress():
        total_duration = max(0.001, float(end) - float(start))
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            decoded = line.decode(errors="replace").strip()
            if decoded.startswith("out_time_ms="):
                try:
                    # FFmpeg's progress out_time_ms is in microseconds.
                    elapsed = int(decoded.split("=", 1)[1]) / 1_000_000
                    pct = min(99, int(elapsed / total_duration * 100))
                    if tracker:
                        tracker.set_progress(pct, "Trimming media...")
                except ValueError:
                    pass
            elif decoded == "progress=end":
                if tracker:
                    tracker.set_progress(99, "Finalizing output...")

    try:
        await asyncio.wait_for(
            asyncio.gather(read_progress(), proc.stderr.read()),
            timeout=PROCESS_TIMEOUT,
        )
        return_code = await proc.wait()
    except asyncio.TimeoutError as exc:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        output_path.unlink(missing_ok=True)
        raise TrimError("Trimming timed out.") from exc
    except asyncio.CancelledError:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        output_path.unlink(missing_ok=True)
        raise TrimCancelled("Trimming was cancelled.")
    finally:
        if tracker and tracker.process is proc:
            tracker.attach_process(None)

    if tracker and tracker.is_cancelled:
        output_path.unlink(missing_ok=True)
        raise TrimCancelled("Trimming was cancelled.")
    if return_code != 0:
        output_path.unlink(missing_ok=True)
        raise TrimError("FFmpeg could not trim this file.")
    if not output_path.is_file() or output_path.stat().st_size == 0:
        output_path.unlink(missing_ok=True)
        raise TrimError("The output file was not created correctly.")

    if tracker:
        tracker.set_progress(100, "Trimming complete.")
    return str(output_path)


async def trim_media_in_temp(input_path, start, end, tracker=None):
    temp_dir = tempfile.mkdtemp(prefix="trim_")
    if tracker:
        tracker.add_temp_path(temp_dir)
    output_path = os.path.join(temp_dir, "trimmed.mp4")
    try:
        result = await trim_media(input_path, output_path, start, end, tracker=tracker)
        return result, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def cleanup_temp(temp_dir):
    shutil.rmtree(temp_dir, ignore_errors=True)
