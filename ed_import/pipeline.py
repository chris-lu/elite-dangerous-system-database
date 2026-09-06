"""Import orchestration: reader process -> multiprocessing queue -> worker
processes (parse + flatten + COPY) -> finalisation.

Processes, not threads: orjson and the flattening loop hold the GIL. The
decompression also gets a process of its own: measured on a 16-core host,
inflating in a thread of the coordinating process fell from ~450 MB/s to
~280 MB/s once the workers saturated the CPUs, because it shared the GIL
with the queue feeder. Each worker owns two connections: one for the batch
transactions and an autocommit side connection for DDL (enum labels) and
lookup rows.
"""
from __future__ import annotations

import dataclasses
import gzip
import logging
import multiprocessing as mp
import queue as queue_mod
import random
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path

import psycopg

from . import __version__, db
from .flatten import Batch, EnumTracker
from .reader import DumpReader, iter_lines

log = logging.getLogger("ed_import")


@dataclasses.dataclass
class Config:
    dsn: str
    mode: str                      # "fresh" | "update"
    source: str
    built_at: str                  # timestamptz literal for the dump build time
    etag: str | None = None
    content_length: int | None = None
    workers: int = 8
    batch_mb: int = 32
    logged: bool = True            # SET LOGGED at the end of the load
    index_parallel: int = 4
    dead_letter_dir: str = "failed"
    progress_every: float = 5.0


# ---------------------------------------------------------------------------
# reader process
# ---------------------------------------------------------------------------
def reader_main(cfg: Config, q, rq, nworkers: int) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    reader = DumpReader(cfg.source, batch_bytes=cfg.batch_mb << 20)
    n = 0
    info = {}
    try:
        for blob in reader:
            q.put((n, blob))
            n += 1
            rq.put({"reader": (reader.bytes_read, reader.raw_bytes)})
        info = {"ended_ok": reader.ended_ok, "last_line": reader.last_line[:60].decode("utf-8", "replace")}
    except BaseException as e:  # truncated / corrupt stream, network error
        info = {"ended_ok": False, "error": repr(e)}
    finally:
        info.update(bytes_read=reader.bytes_read, raw_bytes=reader.raw_bytes, batches=n)
        for _ in range(nworkers):
            q.put(None)
        rq.put({"reader_done": info})


# ---------------------------------------------------------------------------
# worker process
# ---------------------------------------------------------------------------
def worker_main(wid: int, q, rq, cfg: Config, run_id: int, market: str) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent drives shutdown
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s w{wid} %(levelname)s %(message)s", stream=sys.stderr)
    apply = db.apply_fresh if cfg.mode == "fresh" else db.apply_update
    app = f"ed_import:{run_id}"

    def fresh_conn():
        c = db.connect(cfg.dsn, app=app)
        if cfg.mode == "update":
            db.create_temp_tables(c)
        return c

    conn = fresh_conn()
    side = db.connect(cfg.dsn, autocommit=True, app=app)
    tracker = EnumTracker(db.enum_labels(side))
    known_lookups = db.known_lookup_ids(side)

    while True:
        item = q.get()
        if item is None:
            break
        batch_no, blob = item
        t0 = time.perf_counter()
        batch = Batch(tracker, run_id, cfg.built_at, known_lookups)
        bad = []
        for line in iter_lines(blob):
            try:
                batch.add_line(line)
            except Exception as e:  # one broken record must not kill the batch
                bad.append((line[:120].decode("utf-8", "replace"), repr(e)))
        payloads = batch.payloads()
        lookups = batch.lookup_payloads()
        new_labels = tracker.take_new()
        t1 = time.perf_counter()

        msg = {"batch": batch_no, "wid": wid, "bytes": len(blob), "flatten_s": t1 - t0,
               "stats": dict(batch.stats), "bad": bad, "failed": None, "counts": {}}
        try:
            if new_labels:
                db.add_enum_labels(side, new_labels)
            if lookups:
                db.upsert_lookups(side, lookups)
            for attempt in range(1, 8):
                try:
                    msg["counts"] = apply(conn, payloads, market)
                    break
                except (psycopg.errors.DeadlockDetected, psycopg.errors.SerializationFailure) as e:
                    conn.rollback()
                    delay = min(30.0, 0.2 * 2 ** attempt) * (0.5 + random.random())
                    log.warning("batch %d: %s, retry %d in %.1fs", batch_no, e.__class__.__name__, attempt, delay)
                    time.sleep(delay)
                except psycopg.OperationalError as e:
                    log.warning("batch %d: connection problem (%s), reconnecting", batch_no, str(e).strip()[:120])
                    try:
                        conn.close()
                    except Exception:
                        pass
                    time.sleep(2.0 * attempt)
                    conn = fresh_conn()
            else:
                raise RuntimeError("batch failed after repeated retries")
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            msg["failed"] = _dead_letter(cfg, run_id, batch_no, blob, e)
        msg["db_s"] = time.perf_counter() - t1
        rq.put(msg)
    conn.close()
    side.close()
    rq.put({"done": wid})


def _dead_letter(cfg: Config, run_id: int, batch_no: int, blob: bytes, exc: Exception) -> str:
    d = Path(cfg.dead_letter_dir)
    d.mkdir(parents=True, exist_ok=True)
    base = d / f"run{run_id}-batch{batch_no:05d}"
    with gzip.open(str(base) + ".jsonl.gz", "wb", compresslevel=1) as f:
        f.write(blob)
    diag = getattr(exc, "diag", None)
    detail = f"{exc.__class__.__name__}: {exc}"
    if diag is not None and diag.context:
        detail += "\nCONTEXT: " + diag.context
    (Path(str(base) + ".error.txt")).write_text(detail, encoding="utf-8")
    log.error("batch %d failed, dead-lettered to %s: %s", batch_no, base, detail.splitlines()[0][:200])
    return str(base)


# ---------------------------------------------------------------------------
# main process
# ---------------------------------------------------------------------------
class Progress:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.t0 = time.perf_counter()
        self.last = 0.0
        self.bytes_read = 0
        self.raw_bytes = 0
        self.systems = 0
        self.rows = defaultdict(int)
        self.stats = defaultdict(int)
        self.batches = 0
        self.failed = []
        self.bad = []
        self.flatten_s = 0.0
        self.db_s = 0.0
        self.systems_applied = 0

    def absorb(self, msg: dict) -> None:
        self.batches += 1
        st = msg["stats"]
        self.systems += st.get("systems", 0)
        for k, v in st.items():
            if k.startswith("rows_"):
                self.rows[k[5:]] += v
            else:
                self.stats[k] += v
        self.flatten_s += msg["flatten_s"]
        self.db_s += msg.get("db_s", 0.0)
        if msg["failed"]:
            self.failed.append(msg["failed"])
        else:
            self.systems_applied += msg["counts"].get("systems_applied", st.get("systems", 0))
        self.bad.extend(msg["bad"])

    def line(self, queued: int, final: bool = False) -> str:
        el = time.perf_counter() - self.t0
        br = self.bytes_read
        pct = f"{100 * br / self.cfg.content_length:5.1f}%" if self.cfg.content_length else "     "
        eta = ""
        if self.cfg.content_length and br and not final:
            eta = f"  ETA {(self.cfg.content_length - br) / (br / el):4.0f}s"
        return (f"[{pct}] {self.systems:>9,} systems  {self.raw_bytes / 1e9:5.2f} GB json  "
                f"{self.systems / el:6,.0f} sys/s  {self.raw_bytes / el / 1e6:4.0f} MB/s  "
                f"batches {self.batches} (queued {queued})  {el:5.0f}s{eta}")

    def maybe_print(self, queued: int) -> None:
        now = time.perf_counter()
        if now - self.last >= self.cfg.progress_every:
            self.last = now
            print(self.line(queued), flush=True)


def _qsize(q) -> int:
    try:
        return q.qsize()
    except NotImplementedError:  # macOS
        return -1


def run(cfg: Config) -> dict:
    """Execute one import run; returns the summary written to import_runs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    main = db.connect(cfg.dsn, autocommit=True)
    if cfg.mode == "fresh":
        db.create_schema(main, unlogged=True)
    elif not db.schema_exists(main):
        raise SystemExit("no schema: run `fresh <dump>` first")
    run_id = db.start_run(main, mode=cfg.mode, source=cfg.source, built_at=cfg.built_at,
                          etag=cfg.etag, content_length=cfg.content_length, version=__version__)
    market = db.create_market_table(main, run_id)
    log.info("run %d (%s) source=%s built_at=%s workers=%d batch=%dMB",
             run_id, cfg.mode, cfg.source, cfg.built_at, cfg.workers, cfg.batch_mb)

    ctx = mp.get_context("spawn")
    q = ctx.Queue(maxsize=cfg.workers * 2)
    rq = ctx.Queue()
    procs = [ctx.Process(target=worker_main, args=(i, q, rq, cfg, run_id, market), name=f"w{i}")
             for i in range(cfg.workers)]
    for p in procs:
        p.start()
    reader = ctx.Process(target=reader_main, args=(cfg, q, rq, cfg.workers), name="reader")
    reader.start()

    prog = Progress(cfg)
    status = "failed"
    timings: dict = {}
    reader_info: dict | None = None
    t_load = time.perf_counter()
    try:
        done = 0
        while done < len(procs):
            try:
                msg = rq.get(timeout=5)
            except queue_mod.Empty:
                if reader_info is None and not reader.is_alive():
                    raise RuntimeError("reader process died")
                if not any(p.is_alive() for p in procs):
                    raise RuntimeError("all workers died")
                prog.maybe_print(_qsize(q))
                continue
            if "reader" in msg:
                prog.bytes_read, prog.raw_bytes = msg["reader"]
            elif "reader_done" in msg:
                reader_info = msg["reader_done"]
                prog.bytes_read, prog.raw_bytes = reader_info["bytes_read"], reader_info["raw_bytes"]
            elif "done" in msg:
                done += 1
            else:
                prog.absorb(msg)
            prog.maybe_print(_qsize(q))
        for p in procs:
            p.join()
        reader.join()
        timings["load"] = round(time.perf_counter() - t_load, 1)
        print(prog.line(0, final=True), flush=True)

        if reader_info is None or reader_info.get("error"):
            raise RuntimeError(f"reading the dump failed: {reader_info and reader_info.get('error')}")
        if not reader_info["ended_ok"]:
            raise RuntimeError(f"dump did not end with ']' (last line {reader_info['last_line']!r}): truncated stream?")
        if cfg.content_length and reader_info["bytes_read"] != cfg.content_length:
            raise RuntimeError(f"read {reader_info['bytes_read']} compressed bytes, expected {cfg.content_length}")

        t = time.perf_counter()
        if cfg.mode == "fresh":
            timings.update(db.finalize_fresh(cfg.dsn, main, market, logged=cfg.logged, index_parallel=cfg.index_parallel))
        else:
            timings.update(db.finalize_update(cfg.dsn, main, market, run_id, logged=cfg.logged))
        timings["finalize"] = round(time.perf_counter() - t, 1)
        db.apply_views(main)
        status = "partial" if prog.failed else "ok"
    except KeyboardInterrupt:
        log.warning("interrupted: stopping workers")
        status = "aborted"
        reader.terminate()
        for _ in procs:
            try:
                q.put_nowait(None)
            except Exception:
                pass
        for p in procs:
            p.join(timeout=30)
            if p.is_alive():
                p.terminate()
        q.cancel_join_thread()
        rq.cancel_join_thread()
    except Exception:
        log.exception("run %d failed", run_id)
        for p in [reader, *procs]:
            if p.is_alive():
                p.terminate()
    finally:
        rows = dict(prog.rows)
        summary = dict(status=status, systems_seen=prog.systems, systems_applied=prog.systems_applied,
                       batches=prog.batches, batches_failed=len(prog.failed), rows_by_table=psycopg.types.json.Jsonb(rows),
                       timings=psycopg.types.json.Jsonb({**timings, "flatten_cpu_s": round(prog.flatten_s, 1),
                                                         "db_cpu_s": round(prog.db_s, 1), **{k: v for k, v in prog.stats.items()}}),
                       bytes_read=prog.bytes_read,
                       notes=("; ".join(prog.failed[:20]) + (f"; {len(prog.bad)} unparsable records" if prog.bad else "")) or None)
        try:
            db.finish_run(main, run_id, **summary)
        except Exception:
            log.exception("could not write the final status of run %d", run_id)
        main.close()
    summary["run_id"] = run_id
    summary["rows_by_table"] = rows
    summary["timings"] = timings
    if prog.bad:
        log.warning("%d records could not be flattened, first: %s", len(prog.bad), prog.bad[0])
    return summary
