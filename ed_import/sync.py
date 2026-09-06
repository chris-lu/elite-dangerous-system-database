"""Keep the database up to date from https://downloads.spansh.co.uk/.

Spansh rebuilds every dump once a day (~05:43 UTC). galaxy_1day holds the
systems updated during the 24 h before the build, galaxy_7days / _1month the
same for longer windows, galaxy_populated every populated system and galaxy
everything. `sync` looks at the build time of the last successful run and the
build time of the current files and picks the smallest dump whose window
covers the gap (with a margin: consecutive daily builds are 24 h +/- minutes
apart, so a strict 24 h rule would flip to the 7-day file every other day).
All comparisons are build time to build time; the wall clock is never used.
"""
from __future__ import annotations

import datetime as dt
import email.utils
import logging
import os
import urllib.request
from pathlib import Path

log = logging.getLogger("ed_import.sync")

BASE = "https://downloads.spansh.co.uk/"
# (dump name, hours of history the file covers, incl. margin for build drift)
WINDOWS = [
    ("galaxy_1day", 36),
    ("galaxy_7days", 7 * 24 + 12),
    ("galaxy_1month", 28 * 24),
]
FULL = "galaxy"
POPULATED = "galaxy_populated"
# files larger than this are downloaded (resumable) instead of streamed:
# a dropped connection cannot be resumed inside a gzip stream
STREAM_MAX_BYTES = 2 << 30


def url_of(name: str) -> str:
    return f"{BASE}{name}.json.gz"


def head(url: str) -> dict:
    """Last-Modified (aware UTC), ETag and Content-Length of a dump."""
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "ed_import"})
    with urllib.request.urlopen(req, timeout=30) as r:
        lm = r.headers.get("Last-Modified")
        return {
            "url": url,
            "last_modified": email.utils.parsedate_to_datetime(lm) if lm else None,
            "etag": r.headers.get("ETag"),
            "content_length": int(r.headers.get("Content-Length") or 0) or None,
        }


def choose_dump(prev_built: dt.datetime | None, cur_built: dt.datetime, *, full: bool = False) -> str | None:
    """Name of the smallest dump covering the gap, None when the current build
    is already imported. Both datetimes must be timezone-aware."""
    if cur_built.tzinfo is None or (prev_built is not None and prev_built.tzinfo is None):
        raise ValueError("naive datetime: build times must be timezone-aware (UTC)")
    if prev_built is None:
        return FULL if full else POPULATED
    if prev_built >= cur_built:
        return None
    gap_h = (cur_built - prev_built).total_seconds() / 3600
    for name, hours in WINDOWS:
        if gap_h <= hours:
            return name
    return FULL if full else POPULATED


def download(url: str, dest: Path, info: dict) -> Path:
    """Download with HTTP Range resume; verifies the final size."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    have = part.stat().st_size if part.exists() else 0
    total = info["content_length"]
    if dest.exists() and total and dest.stat().st_size == total:
        log.info("already downloaded: %s", dest)
        return dest
    while True:
        headers = {"User-Agent": "ed_import"}
        if have:
            headers["Range"] = f"bytes={have}-"
            if info.get("etag"):
                headers["If-Range"] = info["etag"]
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as r, open(part, "ab" if r.status == 206 else "wb") as f:
                if r.status != 206:
                    have = 0
                t = last = 0.0
                while True:
                    chunk = r.read(8 << 20)
                    if not chunk:
                        break
                    f.write(chunk)
                    have += len(chunk)
                    if total and have - last > 200 << 20:
                        last = have
                        log.info("downloading %s: %.1f%%", dest.name, 100 * have / total)
            break
        except (OSError, urllib.error.URLError) as e:
            have = part.stat().st_size if part.exists() else 0
            log.warning("download interrupted at %d bytes (%s), resuming", have, e)
    if total and have != total:
        raise RuntimeError(f"downloaded {have} bytes, expected {total}")
    os.replace(part, dest)
    return dest


def pg_ts(d: dt.datetime) -> str:
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
