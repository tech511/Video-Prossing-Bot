"""Media merge engine.
file name - logic/merge.py

Responsibilities:
- Join multiple audio files into one MP3, or multiple video files into
  one MP4, in the order they were given.
- Normalise different codecs, sample rates, sizes and frame rates so
  files from different sources can be joined.
- Report progress through an optional callback.
- Support cancellation through an optional callback.
- Create temporary output files and clean them up.

This module does not handle Telegram messages or buttons.
"""

import asyncio
import json
import math
import os
import shutil
import tempfile
from pathlib import Path


FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

MIN_FILES = 2
MAX_SIDE = 1920           # longest video side of the merged output
VIDEO_FPS = 30
PROCESS_TIMEOUT = 3600    # 1 hour


class MergeError(Exception):
    """Raised when merging fails."""


class MergeCancelled(MergeError):
    """Raised when merging is cancelled."""


def _check_cancelled(cancel_check):
    """Stop the operation when the caller requests cancellation."""
    if cancel_check and cancel_check():
        raise MergeCancelled("Merging was cancelled.")


# ----------------------------------------------------------------------
# Probing
# ----------------------------------------------------------------------
def _rotation(stream):
    """Read rotation metadata from a video stream (0 when absent)."""

    try:
        rotate = (stream.get("tags") or {}).get("rotate")

        if rotate is not None:
            return int(float(rotate))

        for side_data in stream.get("side_data_list") or []:
            if "rotation" in side_data:
                return int(float(side_data["rotation"]))

    except (ValueError, TypeError, AttributeError):
        pass

    return 0


async def _probe(path):
    """Return info about one input file."""

    name = Path(path).name

    command = [
        FFPROBE,
        "-v", "error",
        "-show_entries",
        "format=duration:stream=codec_type,width,height"
        ":stream_tags=rotate:stream_side_data=rotation"
        ":stream_disposition=attached_pic",
        "-of", "json",
        str(path),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise MergeError(
            "FFprobe could not start. Check your FFmpeg installation."
        ) from exc

    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=30)

    except asyncio.TimeoutError as exc:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise MergeError(f"Inspecting `{name}` timed out.") from exc

    if process.returncode != 0:
        raise MergeError(f"`{name}` could not be read as audio or video.")

    try:
        data = json.loads(stdout.decode(errors="replace") or "{}")
        duration = float(data.get("format", {}).get("duration", 0))
    except (ValueError, TypeError, AttributeError):
        data = {}
        duration = 0.0

    if not math.isfinite(duration) or duration <= 0:
        raise MergeError(f"Could not read the duration of `{name}`.")

    streams = data.get("streams", []) or []

    video_stream = None

    for stream in streams:
        if (
            stream.get("codec_type") == "video"
            and not (stream.get("disposition") or {}).get("attached_pic")
        ):
            video_stream = stream
            break

    has_audio = any(stream.get("codec_type") == "audio" for stream in streams)

    width = height = 0

    if video_stream:
        try:
            width = int(video_stream.get("width") or 0)
            height = int(video_stream.get("height") or 0)
        except (ValueError, TypeError):
            width = height = 0

        if abs(_rotation(video_stream)) % 180 == 90:
            width, height = height, width

    if not video_stream and not has_audio:
        raise MergeError(f"`{name}` has no audio or video stream.")

    return {
        "name": name,
        "duration": duration,
        "has_video": video_stream is not None,
        "has_audio": has_audio,
        "width": width,
        "height": height,
    }


def _target_size(infos):
    """Pick the output size from the first video that has a size."""

    width, height = 1280, 720

    for info in infos:
        if info["has_video"] and info["width"] > 0 and info["height"] > 0:
            width, height = info["width"], info["height"]
            break

    scale = min(1.0, MAX_SIDE / max(width, height))
    width = max(2, int(width * scale) // 2 * 2)
    height = max(2, int(height * scale) // 2 * 2)

    return width, height


# ----------------------------------------------------------------------
# FFmpeg helpers
# ----------------------------------------------------------------------
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
    total_duration,
    progress_callback=None,
    process_callback=None,
    cancel_check=None,
):
    """Run the merge command and report progress (30-99% style 0-99)."""

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

                    if progress_callback and total_duration > 0:
                        percent = min(99, int(elapsed / total_duration * 100))
                        progress_callback(percent, "Merging files...")

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
            raise MergeError("FFmpeg could not merge these files.")

    except (MergeCancelled, asyncio.CancelledError):
        await _stop_process(process)
        raise

    except asyncio.TimeoutError as exc:
        await _stop_process(process)
        raise MergeError("Merging timed out.") from exc

    except MergeError:
        await _stop_process(process)
        raise

    except OSError as exc:
        await _stop_process(process)
        raise MergeError("Could not run FFmpeg.") from exc


def _build_audio_command(input_paths, infos, output_path):
    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel", "error",
        "-nostdin",
        "-y",
    ]

    for path in input_paths:
        command += ["-i", str(path)]

    count = len(input_paths)
    parts = []

    for index in range(count):
        parts.append(
            f"[{index}:a:0]aresample=44100,"
            "aformat=sample_fmts=fltp:channel_layouts=stereo,"
            f"asetpts=PTS-STARTPTS[a{index}]"
        )

    joined = "".join(f"[a{index}]" for index in range(count))
    parts.append(f"{joined}concat=n={count}:v=0:a=1[a]")

    command += [
        "-filter_complex", ";".join(parts),
        "-map", "[a]",
        "-codec:a", "libmp3lame",
        "-b:a", "192k",
        "-progress", "pipe:1",
        "-nostats",
        str(output_path),
    ]

    return command


def _build_video_command(input_paths, infos, output_path):
    width, height = _target_size(infos)

    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel", "error",
        "-nostdin",
        "-y",
    ]

    for path in input_paths:
        command += ["-i", str(path)]

    count = len(input_paths)
    parts = []

    for index, info in enumerate(infos):
        parts.append(
            f"[{index}:v:0]scale={width}:{height}:"
            "force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,"
            f"setsar=1,fps={VIDEO_FPS},format=yuv420p,"
            f"setpts=PTS-STARTPTS[v{index}]"
        )

        if info["has_audio"]:
            parts.append(
                f"[{index}:a:0]aresample=44100,"
                "aformat=sample_fmts=fltp:channel_layouts=stereo,"
                f"asetpts=PTS-STARTPTS[a{index}]"
            )
        else:
            # Silent audio so every part has both streams.
            parts.append(
                "anullsrc=r=44100:cl=stereo,"
                f"atrim=0:{info['duration']:.3f},"
                f"asetpts=PTS-STARTPTS[a{index}]"
            )

    joined = "".join(f"[v{index}][a{index}]" for index in range(count))
    parts.append(f"{joined}concat=n={count}:v=1:a=1[v][a]")

    command += [
        "-filter_complex", ";".join(parts),
        "-map", "[v]",
        "-map", "[a]",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "192k",
        "-movflags", "+faststart",
        "-progress", "pipe:1",
        "-nostats",
        str(output_path),
    ]

    return command


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
async def merge_files(
    input_paths,
    output_path_base,
    progress_callback=None,
    cancel_check=None,
    process_callback=None,
):
    """Merge files into output_path_base + '.mp3' or '.mp4'.

    Returns the final output path.
    """

    paths = [Path(item) for item in input_paths]

    if len(paths) < MIN_FILES:
        raise MergeError(f"Send at least {MIN_FILES} files to merge.")

    for path in paths:
        if not path.is_file():
            raise MergeError(f"Input file does not exist: {path.name}")

    infos = []

    for path in paths:
        _check_cancelled(cancel_check)
        infos.append(await _probe(path))

    video_flags = {info["has_video"] for info in infos}

    if len(video_flags) > 1:
        raise MergeError(
            "You cannot merge audio and video files together. "
            "Send only audio files or only video files."
        )

    is_video = video_flags == {True}

    if not is_video:
        for info in infos:
            if not info["has_audio"]:
                raise MergeError(f"`{info['name']}` has no audio stream.")

    total_duration = sum(info["duration"] for info in infos)

    if is_video:
        output_path = f"{output_path_base}.mp4"
        command = _build_video_command(paths, infos, output_path)
    else:
        output_path = f"{output_path_base}.mp3"
        command = _build_audio_command(paths, infos, output_path)

    if progress_callback:
        progress_callback(0, "Merging files...")

    try:
        await _run_ffmpeg(
            command,
            total_duration,
            progress_callback=progress_callback,
            process_callback=process_callback,
            cancel_check=cancel_check,
        )

        if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
            raise MergeError("The merged file is empty.")

    except BaseException:
        try:
            os.remove(output_path)
        except OSError:
            pass
        raise

    if progress_callback:
        progress_callback(100, "Merge complete.")

    return output_path


async def merge_files_in_temp(
    input_paths,
    progress_callback=None,
    cancel_check=None,
    process_callback=None,
):
    """Merge files into a temporary directory.

    Returns:
        (merged_path, temporary_directory)

    The merged file ends in .mp4 (video) or .mp3 (audio). The caller must
    clean up the temporary directory after sending the file.
    """

    temp_dir = tempfile.mkdtemp(prefix="merge_")
    output_base = os.path.join(temp_dir, "merged")

    try:
        result = await merge_files(
            input_paths,
            output_base,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            process_callback=process_callback,
        )

        return result, temp_dir

    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def cleanup_temp(temp_dir):
    """Remove temporary merge files."""
    shutil.rmtree(temp_dir, ignore_errors=True)
