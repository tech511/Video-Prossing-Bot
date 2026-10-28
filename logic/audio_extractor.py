
"""Audio extraction engine.
file name - logic/audio_extractor.py
Responsibilities:
- Extract audio from video/media files.
- Save the extracted audio as MP3.
- Support asynchronous execution and cancellation.
- Report progress through an optional callback.
- Create temporary output files and clean them up.
This module does not handle Telegram messages or buttons.
"""

import asyncio
import math
import os
import shutil
import tempfile
from pathlib import Path


FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

PROCESS_TIMEOUT = 900  # 15 minutes


class AudioExtractionError(Exception):
    """Raised when audio extraction fails."""


class AudioExtractionCancelled(AudioExtractionError):
    """Raised when extraction is cancelled."""


def _check_cancelled(cancel_check):
    """Stop the operation when the caller requests cancellation."""
    if cancel_check and cancel_check():
        raise AudioExtractionCancelled("Audio extraction was cancelled.")


async def _probe_audio(input_path):
    """Check that the input contains an audio stream."""

    command = [
        FFPROBE,
        "-v", "error",
        "-select_streams", "a:0",
        "-show_entries", "stream=codec_type",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(input_path),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, _ = await asyncio.wait_for(
            process.communicate(),
            timeout=30,
        )

    except asyncio.TimeoutError as exc:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise AudioExtractionError("Audio inspection timed out.") from exc

    except OSError as exc:
        raise AudioExtractionError(
            "FFprobe could not start. Check your FFmpeg installation."
        ) from exc

    if process.returncode != 0 or not stdout.strip():
        raise AudioExtractionError(
            "This file does not contain an extractable audio stream."
        )


async def extract_audio(
    input_path,
    output_path,
    progress_callback=None,
    cancel_check=None,
):
    """Extract the first audio stream and encode it as MP3.

    Args:
        input_path: Path to the source media file.
        output_path: Destination path ending in .mp3.
        progress_callback: Optional callback(percent, detail).
        cancel_check: Optional callback returning True if cancelled.

    Returns:
        The output MP3 path.
    """

    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.is_file():
        raise AudioExtractionError("Input file does not exist.")

    if input_path.resolve() == output_path.resolve():
        raise AudioExtractionError(
            "Input and output paths must be different."
        )

    if output_path.suffix.lower() != ".mp3":
        raise AudioExtractionError(
            "Output filename must end in .mp3."
        )

    await _probe_audio(input_path)
    _check_cancelled(cancel_check)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel", "error",
        "-nostdin",
        "-y",
        "-i", str(input_path),
        "-map", "0:a:0",
        "-vn",
        "-sn",
        "-dn",
        "-codec:a", "libmp3lame",
        "-b:a", "192k",
        "-progress", "pipe:1",
        "-nostats",
        str(output_path),
    ]

    process = None

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        total_duration = await _get_duration(input_path)

        if progress_callback:
            progress_callback(0, "Extracting audio...")

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

                        if total_duration > 0:
                            percent = min(
                                99,
                                int(elapsed / total_duration * 100),
                            )
                        else:
                            percent = 0

                        if progress_callback:
                            progress_callback(
                                percent,
                                "Extracting audio...",
                            )

                    except ValueError:
                        pass

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
            raise AudioExtractionError(
                "FFmpeg could not extract the audio."
            )

        if not output_path.is_file() or output_path.stat().st_size == 0:
            raise AudioExtractionError(
                "The extracted audio file is empty."
            )

        if progress_callback:
            progress_callback(100, "Audio extraction complete.")

        return str(output_path)

    except AudioExtractionCancelled:
        await _stop_process(process)
        output_path.unlink(missing_ok=True)
        raise

    except asyncio.CancelledError:
        await _stop_process(process)
        output_path.unlink(missing_ok=True)
        raise

    except asyncio.TimeoutError as exc:
        await _stop_process(process)
        output_path.unlink(missing_ok=True)
        raise AudioExtractionError(
            "Audio extraction timed out."
        ) from exc

    except AudioExtractionError:
        await _stop_process(process)
        output_path.unlink(missing_ok=True)
        raise

    except OSError as exc:
        await _stop_process(process)
        output_path.unlink(missing_ok=True)
        raise AudioExtractionError(
            "Could not run FFmpeg or write the output file."
        ) from exc


async def _get_duration(input_path):
    """Read media duration in seconds."""

    command = [
        FFPROBE,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(input_path),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, _ = await asyncio.wait_for(
            process.communicate(),
            timeout=30,
        )

        duration = float(stdout.decode().strip())

        if process.returncode != 0 or not math.isfinite(duration):
            return 0.0

        return max(0.0, duration)

    except (OSError, ValueError, asyncio.TimeoutError):
        return 0.0


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


async def extract_audio_in_temp(
    input_path,
    progress_callback=None,
    cancel_check=None,
):
    """Extract audio into a temporary directory.

    Returns:
        (output_mp3_path, temporary_directory)

    The caller must clean up the temporary directory after using the MP3.
    """

    temp_dir = tempfile.mkdtemp(prefix="audio_extract_")
    output_path = os.path.join(temp_dir, "extracted_audio.mp3")

    try:
        result = await extract_audio(
            input_path,
            output_path,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
        )

        return result, temp_dir

    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def cleanup_temp(temp_dir):
    """Remove temporary extraction files."""
    shutil.rmtree(temp_dir, ignore_errors=True)
