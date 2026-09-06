"""Stream a Spansh galaxy dump and cut it into blobs of complete JSON lines.

The dumps are a JSON array pretty-printed with exactly one system per line:

    [
    \\t{"id64":...},
    \\t{"id64":...}
    ]

so no streaming JSON parser is needed: strip the tab / trailing comma and each
line is a complete document. Lines range from ~700 bytes to ~9 MB, so batches
are cut by a byte budget, never inside a line.

`smart_open` opens local paths and http(s):// URLs alike; we ask it for the
raw (still compressed) stream so the compressed bytes can be counted for
progress / verification, and decompress with ISA-L's igzip when it is
installed (2-3x faster than zlib), else the standard library.
"""
from __future__ import annotations

import bz2
import gzip
import os
import queue
import threading
from typing import Iterator

from smart_open import open as smart_open_open

try:  # optional accelerator (python-isal); Linux wheels exist for every Python
    from isal import igzip as _igzip
except ImportError:  # pragma: no cover
    _igzip = None

HTTP_TRANSPORT = {"buffer_size": 8 << 20, "timeout": (15, 120)}
READ_CHUNK = 8 << 20


class CountingReader:
    """Counts the compressed bytes pulled from the underlying stream."""

    def __init__(self, raw):
        self.raw = raw
        self.bytes_read = 0

    def read(self, n=-1):
        b = self.raw.read(n)
        self.bytes_read += len(b)
        return b

    def readinto(self, buf):  # used by BufferedReader-style consumers
        b = self.raw.read(len(buf))
        buf[:len(b)] = b
        self.bytes_read += len(b)
        return len(b)

    def readable(self):
        return True

    def close(self):
        self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def open_dump(source: str):
    """Return (decompressed file object, CountingReader of the raw stream)."""
    kw = {"compression": "disable"}
    if source.startswith(("http://", "https://")):
        kw["transport_params"] = HTTP_TRANSPORT
    raw = CountingReader(smart_open_open(source, "rb", **kw))
    if source.endswith(".gz"):
        f = _igzip.IGzipFile(fileobj=raw) if _igzip else gzip.GzipFile(fileobj=raw)
    elif source.endswith(".bz2"):
        f = bz2.BZ2File(raw)
    else:
        f = raw
    return f, raw


class DumpReader:
    """Iterates blobs of whole lines (~batch_bytes each) from a dump.

    Decompression runs in a background thread (zlib / isal release the GIL)
    and hands blobs over a small queue, so the consumer's own work (pickling
    the blob onto a multiprocessing queue) overlaps with it.

    Attributes updated while reading: bytes_read (compressed), lines,
    raw_bytes (decompressed), last_line, ended_ok (stream ended with ']').
    """

    def __init__(self, source: str, batch_bytes: int = 32 << 20, prefetch: int = 4):
        self.source = source
        self.batch_bytes = batch_bytes
        self.prefetch = prefetch
        self.bytes_read = 0
        self.raw_bytes = 0
        self.lines = 0
        self.last_line = b""
        self.ended_ok = False
        self.error: BaseException | None = None
        self.stopped = False

    def _pump(self, q: queue.Queue):
        try:
            f, raw = open_dump(self.source)
            with f:
                tail = b""
                blob = b""
                while not self.stopped:
                    chunk = f.read(self.batch_bytes)
                    self.bytes_read = raw.bytes_read
                    if not chunk:
                        break
                    self.raw_bytes += len(chunk)
                    cut = chunk.rfind(b"\n")
                    if cut < 0:  # a single line longer than the budget: keep accumulating
                        tail += chunk
                        continue
                    blob = tail + chunk[:cut + 1]
                    tail = chunk[cut + 1:]
                    q.put(blob)
                if tail and not self.stopped:
                    q.put(tail)
                if not self.stopped:
                    # everything after the last newline, or the last line
                    last = tail.strip() if tail.strip() else b"]" if blob.rstrip().endswith(b"]") else b""
                    self.last_line = last
                    self.ended_ok = last == b"]"
                self.bytes_read = raw.bytes_read
        except BaseException as e:  # surfaced to the consumer
            self.error = e
        finally:
            q.put(None)

    def __iter__(self) -> Iterator[bytes]:
        q: queue.Queue = queue.Queue(maxsize=self.prefetch)
        t = threading.Thread(target=self._pump, args=(q,), name="dump-reader", daemon=True)
        t.start()
        while True:
            blob = q.get()
            if blob is None:
                break
            yield blob
        t.join()
        if self.error:
            raise self.error

    def stop(self):
        self.stopped = True


def iter_lines(blob: bytes) -> Iterator[bytes]:
    """JSON documents inside a blob: strip the tab / trailing comma, skip the
    array brackets."""
    for line in blob.split(b"\n"):
        line = line.strip()
        if not line or line == b"[" or line == b"]":
            continue
        if line[-1] == 0x2C:  # ','
            line = line[:-1]
        yield line


def source_size(source: str) -> int | None:
    """Compressed size, for progress reporting."""
    try:
        if source.startswith(("http://", "https://")):
            from .sync import head
            return head(source)["content_length"]
        return os.path.getsize(source)
    except Exception:
        return None
