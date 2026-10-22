
"""ZIP creation engine.
file name - logic/zip.py

Responsibilities:
- Create a ZIP archive from one or multiple files.
- Use balanced compression.
- Report progress through an optional callback.
- Support cancellation through an optional callback.
- Manage temporary output and cleanup.

This module does not handle Telegram messages or buttons.
"""

import asyncio
import os
import shutil
import tempfile
import zipfile

from pathlib import Path


CHUNK_SIZE = 1024 * 1024  # Read 1 MB at a time
COMPRESSION_LEVEL = 6     # Balanced compression


class ZipError(Exception):
    """Raised when ZIP creation fails."""


class ZipCancelled(ZipError):
    """Raised when ZIP creation is cancelled."""


def _check_cancelled(cancel_check):
    """Raise ZipCancelled when the caller requests cancellation."""

    if cancel_check and cancel_check():
        raise ZipCancelled("ZIP creation was cancelled.")


def _create_zip(
    input_paths,
    output_path,
    progress_callback=None,
    cancel_check=None,
):
    """Create one ZIP archive from multiple input files.

    Args:
        input_paths: A list of local file paths.
        output_path: Destination path ending in .zip.
        progress_callback: Optional callback(percent, detail).
        cancel_check: Optional callback returning True when cancelled.

    Returns:
        The output ZIP file path.
    """

    if isinstance(input_paths, (str, os.PathLike)):
        input_paths = [input_paths]

    if not input_paths:
        raise ZipError("No input files were provided.")

    files = []

    for item in input_paths:
        path = Path(item)

        if not path.is_file():
            raise ZipError(f"Input file does not exist: {path.name}")

        files.append(path)

    output_path = Path(output_path)

    if output_path.suffix.lower() != ".zip":
        raise ZipError("Output filename must end in .zip.")

    # Do not allow the output archive to be one of its own inputs.
    output_resolved = output_path.resolve()

    for path in files:
        if path.resolve() == output_resolved:
            raise ZipError("The output archive cannot be an input file.")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_bytes = sum(path.stat().st_size for path in files)
    processed_bytes = 0
    used_names = set()

    try:
        with zipfile.ZipFile(
            output_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=COMPRESSION_LEVEL,
            allowZip64=True,
        ) as archive:

            for index, path in enumerate(files, start=1):
                _check_cancelled(cancel_check)

                # Keep only the filename inside the archive.
                archive_name = path.name

                # Avoid overwriting entries with identical filenames.
                if archive_name in used_names:
                    stem = path.stem
                    suffix = path.suffix
                    number = 2

                    while f"{stem}_{number}{suffix}" in used_names:
                        number += 1

                    archive_name = f"{stem}_{number}{suffix}"

                used_names.add(archive_name)

                file_size = path.stat().st_size
                file_processed = 0

                # Stream each file in chunks instead of loading it all
                # into RAM at once.
                with path.open("rb") as source:
                    with archive.open(
                        archive_name,
                        mode="w",
                        force_zip64=True,
                    ) as destination:

                        while True:
                            _check_cancelled(cancel_check)

                            chunk = source.read(CHUNK_SIZE)

                            if not chunk:
                                break

                            destination.write(chunk)

                            chunk_size = len(chunk)
                            file_processed += chunk_size
                            processed_bytes += chunk_size

                            if progress_callback:
                                if total_bytes > 0:
                                    percent = int(
                                        processed_bytes / total_bytes * 100
                                    )
                                else:
                                    percent = int(
                                        index / len(files) * 100
                                    )

                                progress_callback(
                                    min(percent, 99),
                                    f"Adding {path.name} "
                                    f"({file_processed}/{file_size} bytes)",
                                )

                if progress_callback:
                    progress_callback(
                        min(99, int(index / len(files) * 100)),
                        f"Added {path.name} ({index}/{len(files)} files)",
                    )

        _check_cancelled(cancel_check)

        if not output_path.is_file() or output_path.stat().st_size == 0:
            raise ZipError("The ZIP archive was not created correctly.")

        # Verify that the resulting archive can be read.
        with zipfile.ZipFile(output_path, "r") as archive:
            bad_file = archive.testzip()

            if bad_file:
                raise ZipError(
                    f"ZIP verification failed for: {bad_file}"
                )

        if progress_callback:
            progress_callback(100, "ZIP archive created successfully.")

        return str(output_path)

    except ZipCancelled:
        output_path.unlink(missing_ok=True)
        raise

    except (OSError, zipfile.BadZipFile, RuntimeError, ValueError) as exc:
        output_path.unlink(missing_ok=True)
        raise ZipError(f"Could not create ZIP archive: {exc}") from exc


async def create_zip(
    input_paths,
    output_path,
    progress_callback=None,
    cancel_check=None,
):
    """Async wrapper for ZIP creation.

    Use this from Pyrogram handlers without blocking the event loop.
    """

    return await asyncio.to_thread(
        _create_zip,
        input_paths,
        output_path,
        progress_callback,
        cancel_check,
    )


async def create_zip_in_temp(
    input_paths,
    progress_callback=None,
    cancel_check=None,
):
    """Create a ZIP archive in a temporary directory.

    Returns:
        (zip_path, temp_directory)

    The caller is responsible for calling cleanup_temp(temp_directory)
    after sending or otherwise using the archive.
    """

    temp_dir = tempfile.mkdtemp(prefix="zip_")
    output_path = os.path.join(temp_dir, "archive.zip")

    try:
        result = await create_zip(
            input_paths,
            output_path,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
        )

        return result, temp_dir

    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def cleanup_temp(temp_dir):
    """Delete a ZIP operation's temporary directory."""

    shutil.rmtree(temp_dir, ignore_errors=True)
