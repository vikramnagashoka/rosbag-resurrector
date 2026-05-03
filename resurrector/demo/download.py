"""Real-data sample bag downloader for `resurrector demo --download`.

Why this exists: the synthetic sample bag (``sample_bag.py``) is fast to
generate and good for smoke tests, but every camera frame is colored noise.
That makes the dashboard's CLIP-powered semantic frame search return
visually meaningless results — bad demo. This module fetches a real
public-domain robotics dataset so demos and the search GIF actually
look impressive.

Default sample: HKU FAST-LIVO ``hku2`` (Hong Kong University campus walking
sequence with LiDAR + camera + IMU). 844 MB MCAP. License is
CC-BY-NC-4.0 — fine for non-commercial demos with attribution.

Source: https://huggingface.co/datasets/DapengFeng/MCAP
"""

from __future__ import annotations

import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

# Public, CC-BY-NC-4.0 sample. ~844 MB, real urban campus footage with
# people, buildings, and indoor/outdoor scenes — CLIP-friendly content.
DEFAULT_SAMPLE_URL = (
    "https://huggingface.co/datasets/DapengFeng/MCAP/resolve/main/"
    "FAST-LIVO/hku2/hku2_0.mcap?download=true"
)
DEFAULT_SAMPLE_FILENAME = "hku2.mcap"
# Sanity bound — refuse anything outrageously off (corrupted download,
# wrong URL pointing at a redirect HTML page, etc.). Real file is ~844 MB.
MIN_EXPECTED_SIZE_BYTES = 500_000_000   # 500 MB
MAX_EXPECTED_SIZE_BYTES = 2_000_000_000  # 2 GB

LICENSE_NOTE = (
    "Sample data: HKU FAST-LIVO dataset, CC-BY-NC-4.0\n"
    "  https://huggingface.co/datasets/DapengFeng/MCAP\n"
    "  Non-commercial use only. Attribute as 'HKU FAST-LIVO dataset' in any\n"
    "  public demo, screenshot, or screencast."
)


@dataclass
class DownloadResult:
    """Outcome of a download_sample() call."""
    path: Path
    bytes_downloaded: int
    skipped: bool  # True if file already existed at the target path


def download_sample(
    target: Path | None = None,
    url: str = DEFAULT_SAMPLE_URL,
    progress_callback=None,
    force: bool = False,
) -> DownloadResult:
    """Fetch a real-data sample MCAP bag for demos.

    Idempotent: if ``target`` already exists and is the right size,
    returns immediately with ``skipped=True``. No re-download.

    Args:
        target: Destination path. Defaults to
            ``~/.resurrector/samples/<DEFAULT_SAMPLE_FILENAME>``.
        url: Source URL. Defaults to the HKU FAST-LIVO hku2 sample.
        progress_callback: Optional callable invoked as ``cb(bytes_so_far,
            total_bytes)`` after each chunk. Use this to wire a Rich progress
            bar. ``total_bytes`` may be 0 if the server doesn't send a
            Content-Length header.
        force: Re-download even if the file already exists at ``target``.

    Returns:
        ``DownloadResult`` with the final path, byte count, and skipped flag.

    Raises:
        urllib.error.URLError: Network / DNS / TLS failure.
        urllib.error.HTTPError: Non-2xx HTTP response.
        ValueError: Downloaded file size is outside the sanity bounds —
            indicates a redirect to an HTML page, partial download, or wrong URL.
    """
    if target is None:
        target = Path.home() / ".resurrector" / "samples" / DEFAULT_SAMPLE_FILENAME

    target = Path(target).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        existing_size = target.stat().st_size
        # Treat any plausibly-sized existing file as "already downloaded";
        # users who hit a corrupted state can pass force=True or rm it.
        if MIN_EXPECTED_SIZE_BYTES <= existing_size <= MAX_EXPECTED_SIZE_BYTES:
            return DownloadResult(
                path=target,
                bytes_downloaded=existing_size,
                skipped=True,
            )

    # Stream to a temp path next to the target, atomic-rename on success
    # so an interrupted download never leaves a half-file at the final path.
    tmp_path = target.with_suffix(target.suffix + ".partial")
    bytes_so_far = 0
    chunk_size = 1024 * 1024  # 1 MB chunks

    try:
        with urllib.request.urlopen(url) as resp:
            # Server may or may not send a Content-Length header
            total_str = resp.headers.get("Content-Length")
            total = int(total_str) if total_str and total_str.isdigit() else 0

            with open(tmp_path, "wb") as out:
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    out.write(chunk)
                    bytes_so_far += len(chunk)
                    if progress_callback is not None:
                        progress_callback(bytes_so_far, total)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        # Clean up partial file on any failure so the next run starts fresh
        tmp_path.unlink(missing_ok=True)
        raise

    # Sanity check before atomic rename — refuse implausibly small / large
    if not (MIN_EXPECTED_SIZE_BYTES <= bytes_so_far <= MAX_EXPECTED_SIZE_BYTES):
        tmp_path.unlink(missing_ok=True)
        raise ValueError(
            f"Downloaded file size {bytes_so_far:,} bytes is outside the "
            f"expected range [{MIN_EXPECTED_SIZE_BYTES:,}, "
            f"{MAX_EXPECTED_SIZE_BYTES:,}]. Likely a redirect to an HTML "
            f"page or wrong URL. URL: {url}"
        )

    tmp_path.replace(target)

    return DownloadResult(
        path=target,
        bytes_downloaded=bytes_so_far,
        skipped=False,
    )
