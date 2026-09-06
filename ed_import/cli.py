"""Command line.

  ed_import fresh  <dump>      drop everything and load a dump from scratch
  ed_import update <dump>      merge a dump into the existing database
  ed_import sync               fetch what is needed from spansh and merge it
  ed_import schema             (re)apply views and helper functions
  ed_import stats              row counts, sizes, last runs

<dump> is a local path or an http(s):// URL of a .json.gz (or .json).
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import struct
import sys
from pathlib import Path

from . import __version__, db, sync
from .pipeline import Config, run


def _dsn(args) -> str:
    dsn = args.dsn or os.environ.get("PG_DSN")
    if not dsn:
        sys.exit("set PG_DSN (or --dsn), e.g. postgresql://elite_user:elite_password@localhost:5432/elite_dangerous")
    return dsn


def _built_at_local(path: str) -> dt.datetime:
    """Build time of a local dump: --built-at, else the gzip header mtime when
    set, else the file mtime."""
    try:
        with open(path, "rb") as f:
            h = f.read(10)
        if h[:2] == b"\x1f\x8b":
            mt = struct.unpack("<I", h[4:8])[0]
            if mt:
                return dt.datetime.fromtimestamp(mt, dt.timezone.utc)
    except OSError:
        pass
    return dt.datetime.fromtimestamp(os.path.getmtime(path), dt.timezone.utc)


def _source_info(args) -> tuple[str, str | None, int | None]:
    """(built_at literal, etag, content_length) of args.source."""
    if args.source.startswith(("http://", "https://")):
        h = sync.head(args.source)
        built = args.built_at or (h["last_modified"] and sync.pg_ts(h["last_modified"]))
        return built, h["etag"], h["content_length"]
    built = args.built_at or sync.pg_ts(_built_at_local(args.source))
    return built, None, os.path.getsize(args.source)


def _config(args, mode: str, source: str, built_at: str, etag, content_length) -> Config:
    return Config(dsn=_dsn(args), mode=mode, source=source, built_at=built_at, etag=etag,
                  content_length=content_length, workers=args.workers, batch_mb=args.batch_mb,
                  logged=not args.unlogged, index_parallel=args.index_parallel,
                  dead_letter_dir=args.dead_letter_dir)


def _report(summary: dict) -> None:
    print(f"\nrun {summary['run_id']}: {summary['status']}  systems seen {summary['systems_seen']:,} "
          f"applied {summary['systems_applied']:,}  batches {summary['batches']} (failed {summary['batches_failed']})")
    rows = summary["rows_by_table"]
    if rows:
        print("rows:", ", ".join(f"{k} {v:,}" for k, v in sorted(rows.items(), key=lambda x: -x[1])))
    print("timings (s):", ", ".join(f"{k} {v}" for k, v in summary["timings"].items()))


def cmd_import(args, mode: str) -> None:
    built_at, etag, size = _source_info(args)
    summary = run(_config(args, mode, args.source, built_at, etag, size))
    _report(summary)
    if summary["status"] not in ("ok", "partial"):
        sys.exit(1)


def cmd_sync(args) -> None:
    dsn = _dsn(args)
    with db.connect(dsn, autocommit=True) as c:
        if not db.schema_exists(c):
            prev = None
        else:
            row = db.last_ok_run(c)
            prev = row[2] if row else None
    cur = sync.head(sync.url_of("galaxy_1day"))
    cur_built = cur["last_modified"]
    name = args.dump or sync.choose_dump(prev, cur_built, full=args.full)
    if name is None and not args.force:
        print(f"up to date: build {cur_built:%Y-%m-%d %H:%M} UTC already imported (last run built_at {prev:%Y-%m-%d %H:%M})")
        return
    name = name or "galaxy_1day"
    info = sync.head(sync.url_of(name))
    mode = "update" if prev is not None else "fresh"
    print(f"last import: {prev or 'none'}; current build: {cur_built:%Y-%m-%d %H:%M} UTC -> {name} ({info['content_length'] / 1e9:.2f} GB, {mode})")
    if args.dry_run:
        return
    built_at = sync.pg_ts(info["last_modified"])
    if info["content_length"] and info["content_length"] > sync.STREAM_MAX_BYTES:
        dest = sync.download(info["url"], Path(args.dump_dir) / f"{name}-{info['last_modified']:%Y%m%d}.json.gz", info)
        source = str(dest)
    else:
        source = info["url"]
    summary = run(_config(args, mode, source, built_at, info["etag"], info["content_length"]))
    _report(summary)
    if summary["status"] not in ("ok", "partial"):
        sys.exit(1)
    if mode == "fresh" and name == sync.POPULATED:
        # populated is not a superset of the daily file (freshly scanned
        # unpopulated systems only appear in the incrementals)
        d = sync.head(sync.url_of("galaxy_1day"))
        summary = run(_config(args, "update", d["url"], sync.pg_ts(d["last_modified"]), d["etag"], d["content_length"]))
        _report(summary)


def cmd_schema(args) -> None:
    with db.connect(_dsn(args), autocommit=True) as c:
        if not db.schema_exists(c):
            sys.exit("no schema yet: run `fresh <dump>`")
        db.apply_views(c)
    print("views and functions applied")


def cmd_stats(args) -> None:
    with db.connect(_dsn(args), autocommit=True) as c:
        if not db.schema_exists(c):
            sys.exit("no schema yet")
        print("last runs:")
        for r in c.execute("""SELECT id, mode, status, built_at, systems_seen, systems_applied, batches_failed,
                                     round(extract(epoch FROM finished - started))::int, source
                              FROM import_runs ORDER BY id DESC LIMIT 8"""):
            print("  run %-4s %-6s %-8s built %s  systems %s/%s  failed batches %s  %ss  %s" % tuple(x if x is not None else "-" for x in r))
        print("tables:")
        for r in c.execute("SELECT table_name, table_size, indexes_size, total_size, est_rows FROM v_table_sizes"):
            print("  %-24s table %-9s indexes %-9s total %-9s ~%s rows" % r)
        print("database:", c.execute("SELECT pg_size_pretty(pg_database_size(current_database()))").fetchone()[0])


def main(argv=None) -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(prog="ed_import", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--version", action="version", version=__version__)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dsn", help="PostgreSQL DSN (default: $PG_DSN)")
    load = argparse.ArgumentParser(add_help=False)
    load.add_argument("--workers", type=int, default=int(os.environ.get("ED_WORKERS", min(8, max(2, (os.cpu_count() or 4) - 2)))),
                      help="parser/loader processes (default 8)")
    load.add_argument("--batch-mb", type=int, default=32, help="JSON bytes per batch (default 32)")
    load.add_argument("--unlogged", action="store_true", help="leave tables UNLOGGED (faster; emptied by a crash)")
    load.add_argument("--index-parallel", type=int, default=4, help="concurrent index builds (default 4)")
    load.add_argument("--built-at", help="dump build time, e.g. '2026-09-05 05:43:52+00' (default: HTTP Last-Modified or file time)")
    load.add_argument("--dead-letter-dir", default=os.environ.get("ED_DUMP_DIR", ".") + "/failed")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("fresh", parents=[common, load], help="drop and reload from a dump")
    s.add_argument("source")
    s = sub.add_parser("update", parents=[common, load], help="merge a dump into the database")
    s.add_argument("source")
    s = sub.add_parser("sync", parents=[common, load], help="fetch and merge the newest spansh dump needed")
    s.add_argument("--dump", choices=["galaxy_1day", "galaxy_7days", "galaxy_1month", "galaxy_populated", "galaxy"],
                   help="force a specific dump")
    s.add_argument("--full", action="store_true", help="use the full galaxy dump instead of galaxy_populated for a first load")
    s.add_argument("--force", action="store_true", help="import even if the build was already imported")
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--dump-dir", default=os.environ.get("ED_DUMP_DIR", "dumps"), help="where big dumps are downloaded")
    sub.add_parser("schema", parents=[common], help="re-apply views and functions")
    sub.add_parser("stats", parents=[common], help="row counts, sizes and recent runs")
    args = p.parse_args(argv)
    if args.cmd in ("fresh", "update"):
        cmd_import(args, args.cmd)
    elif args.cmd == "sync":
        cmd_sync(args)
    elif args.cmd == "schema":
        cmd_schema(args)
    else:
        cmd_stats(args)
