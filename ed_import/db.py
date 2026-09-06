"""Database side of the importer: connections, schema, per-batch apply
(fresh COPY or replace-by-system merge), and the end-of-run finalisation
(index builds, market partition swap, vacuum).
"""
from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from psycopg import sql

from . import tables as T

log = logging.getLogger("ed_import.db")

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
ADVISORY_RUN = "ed_import_run"
ADVISORY_DDL = "ed_import_ddl"

# tables written by the workers, in COPY order (parents before children is
# irrelevant without FKs, but keeps the log readable)
LOAD_TABLES = tuple(T.TABLES)
LOOKUP_TABLES = ("commodities", "modules", "ships")
# in-place tables that get VACUUM (ANALYZE) after an update run
INPLACE_TABLES = tuple(t for t in LOAD_TABLES if t != "station_commodities")


def connect(dsn: str, *, autocommit: bool = False, app: str = "ed_import") -> psycopg.Connection:
    # connect_timeout: with the compose port bound to 127.0.0.1 only, a DSN
    # saying "localhost" first tries ::1, which Docker Desktop's proxy accepts
    # and never answers; the timeout makes libpq fall through to 127.0.0.1.
    return psycopg.connect(dsn, autocommit=autocommit, application_name=app, connect_timeout=10,
                           options="-c client_encoding=UTF8 -c lock_timeout=0")


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def statements(text: str) -> list[str]:
    """One statement per non-comment line (indexes.sql / market_indexes.sql)."""
    return [l.strip().rstrip(";") for l in text.splitlines() if l.strip() and not l.lstrip().startswith("--")]


def cols(table: str) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(c) for c in T.TABLES[table])


def copy_sql(table: str, target: str | None = None) -> sql.Composed:
    return sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(target or table), cols(table))


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------
def schema_exists(conn) -> bool:
    return conn.execute("SELECT to_regclass('public.systems') IS NOT NULL").fetchone()[0]


def create_schema(conn, *, unlogged: bool) -> None:
    """Drop and recreate everything. `conn` must be autocommit."""
    log.info("creating schema (drop public)")
    conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    conn.execute(read_sql("enums.sql"))
    conn.execute(read_sql("schema.sql"))
    # lookup tables need their keys from the start (ON CONFLICT DO NOTHING)
    for t in LOOKUP_TABLES:
        conn.execute(sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (id)").format(sql.Identifier(t)))
    if unlogged:
        for t in LOAD_TABLES:
            if t != "station_commodities":
                conn.execute(sql.SQL("ALTER TABLE {} SET UNLOGGED").format(sql.Identifier(t)))
    conn.execute(read_sql("views.sql"))


def apply_views(conn) -> None:
    conn.execute(read_sql("views.sql"))


def enum_labels(conn) -> dict[str, set[str]]:
    known: dict[str, set[str]] = {t: set() for t in T.ENUM_TYPES}
    for typ, lab in conn.execute(
            "SELECT t.typname, e.enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid"):
        known.setdefault(typ, set()).add(lab)
    return known


def add_enum_labels(side, new: dict[str, set[str]]) -> None:
    """ALTER TYPE ... ADD VALUE on an autocommit connection, serialised across
    workers. Must be committed before the batch transaction starts (a value
    added by an uncommitted transaction cannot be used)."""
    side.execute("SELECT pg_advisory_lock(hashtext(%s))", (ADVISORY_DDL,))
    try:
        for typ, labels in new.items():
            for label in sorted(labels):
                if len(label.encode()) > 63:
                    raise ValueError(f"enum label too long for {typ}: {label!r}")
                side.execute(sql.SQL("ALTER TYPE {} ADD VALUE IF NOT EXISTS {}").format(
                    sql.Identifier(typ), sql.Literal(label)))
                log.info("enum %s: added label %r", typ, label)
    finally:
        side.execute("SELECT pg_advisory_unlock(hashtext(%s))", (ADVISORY_DDL,))


def known_lookup_ids(conn) -> dict[str, set[int]]:
    return {t: {r[0] for r in conn.execute(sql.SQL("SELECT id FROM {}").format(sql.Identifier(t)))}
            for t in LOOKUP_TABLES}


def upsert_lookups(side, payloads: dict[str, bytes]) -> None:
    """Insert new commodities / modules / ships (rows sorted by id) outside the
    batch transaction so concurrent workers cannot deadlock on them."""
    for t, data in payloads.items():
        tmp = f"lk_{t}"
        side.execute(sql.SQL("CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {}); TRUNCATE {}").format(
            sql.Identifier(tmp), sql.Identifier(t), sql.Identifier(tmp)))
        with side.cursor().copy(sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(tmp), sql.SQL(", ").join(sql.Identifier(c) for c in getattr(T, t.upper())))) as cp:
            cp.write(data)
        side.execute(sql.SQL("INSERT INTO {} SELECT * FROM {} ORDER BY id ON CONFLICT (id) DO NOTHING").format(
            sql.Identifier(t), sql.Identifier(tmp)))


# ---------------------------------------------------------------------------
# import_runs bookkeeping
# ---------------------------------------------------------------------------
def start_run(conn, *, mode: str, source: str, built_at, etag, content_length, version) -> int:
    """`conn` autocommit. Marks stale 'running' rows aborted, refuses to update
    on top of an unfinished fresh load, takes the run-level advisory lock."""
    conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (ADVISORY_RUN,))
    conn.execute("""
        UPDATE import_runs r SET status = 'aborted', finished = now(), notes = coalesce(notes || '; ', '') || 'no importer process found'
        WHERE status = 'running'
          AND NOT EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.application_name = 'ed_import:' || r.id)""")
    if mode == "update":
        keyed = conn.execute("""SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
                                WHERE i.indisprimary AND c.relname IN ('systems', 'stations', 'bodies')""").fetchone()[0]
        if keyed < 3:
            raise RuntimeError("the tables have no primary keys (an unfinished fresh load?): run `fresh` again")
    run_id = conn.execute("""
        INSERT INTO import_runs (mode, source, built_at, etag, content_length, status, importer_version)
        VALUES (%s, %s, %s, %s, %s, 'running', %s) RETURNING id""",
        (mode, source, built_at, etag, content_length, version)).fetchone()[0]
    conn.execute(sql.SQL("SET application_name = {}").format(sql.Literal(f"ed_import:{run_id}")))
    return run_id


def finish_run(conn, run_id: int, *, status: str, **fields) -> None:
    """Final status write, made durable even under synchronous_commit=off."""
    sets = sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()) for k in fields)
    with conn.transaction():
        conn.execute("SET LOCAL synchronous_commit = on")
        conn.execute(sql.SQL("UPDATE import_runs SET status = %s, finished = now(){} WHERE id = %s").format(
            sql.SQL(", ") + sets if fields else sql.SQL("")), (status, *fields.values(), run_id))
    conn.execute("SELECT pg_advisory_unlock_all()")


def last_ok_run(conn):
    return conn.execute("""SELECT id, source, built_at, etag, content_length, finished
                           FROM import_runs WHERE status IN ('ok', 'partial') AND built_at IS NOT NULL
                           ORDER BY built_at DESC, id DESC LIMIT 1""").fetchone()


# ---------------------------------------------------------------------------
# per-batch apply
# ---------------------------------------------------------------------------
def market_table(run_id: int) -> str:
    return f"station_commodities_r{run_id}"


def create_market_table(conn, run_id: int) -> str:
    name = market_table(run_id)
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}; CREATE UNLOGGED TABLE {} (LIKE station_commodities)").format(
        sql.Identifier(name), sql.Identifier(name)))
    return name


def create_temp_tables(conn) -> None:
    """Per-session staging tables for update mode (created once; emptied at
    every commit). Generated columns of the source become plain columns."""
    for t in LOAD_TABLES:
        conn.execute(sql.SQL("CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {}) ON COMMIT DELETE ROWS").format(
            sql.Identifier("tmp_" + t), sql.Identifier(t)))
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS w_sys (id64 bigint) ON COMMIT DELETE ROWS")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS w_st (id bigint) ON COMMIT DELETE ROWS")
    conn.commit()


def apply_fresh(conn, payloads: dict[str, bytes], market: str) -> dict[str, int]:
    """Fresh load: COPY straight into the bare final tables."""
    counts = {}
    with conn.transaction():
        cur = conn.cursor()
        for t, data in payloads.items():
            with cur.copy(copy_sql(t, market if t == "station_commodities" else None)) as cp:
                cp.write(data)
            counts[t] = cur.rowcount
    return counts


def _upsert(table: str, key: str, guard: str) -> sql.Composed:
    c = T.TABLES[table]
    return sql.SQL(
        "WITH up AS (INSERT INTO {tbl} AS s ({cols}) SELECT {sel} FROM {tmp} t {join} ORDER BY t.{key} "
        "ON CONFLICT ({key}) DO UPDATE SET ({cols}) = ({excl}) WHERE {guard} RETURNING s.{key}) "
        "INSERT INTO {w} SELECT {key} FROM up"
    ).format(
        tbl=sql.Identifier(table), tmp=sql.Identifier("tmp_" + table), cols=cols(table),
        sel=sql.SQL(", ").join(sql.SQL("t.") + sql.Identifier(x) for x in c),
        excl=sql.SQL(", ").join(sql.SQL("EXCLUDED.") + sql.Identifier(x) for x in c),
        key=sql.Identifier(key), guard=sql.SQL(guard),
        join=sql.SQL("JOIN w_sys w ON w.id64 = t.system_id64") if table == "stations" else sql.SQL(""),
        w=sql.Identifier("w_st" if table == "stations" else "w_sys"),
    )


UPSERT_SYSTEMS = _upsert("systems", "id64", "EXCLUDED.built_at >= s.built_at")
UPSERT_STATIONS = _upsert("stations", "id", "EXCLUDED.version_ts > s.version_ts")

APPLY_UPDATE = [
    # staging statistics: temp tables are never auto-analysed
    "ANALYZE tmp_systems, tmp_stations, tmp_bodies",
    # one global lock order for every worker: systems, then stations
    "SELECT 1 FROM systems s JOIN tmp_systems t USING (id64) ORDER BY s.id64 FOR UPDATE OF s",
    "SELECT 1 FROM stations s JOIN tmp_stations t ON t.id = s.id ORDER BY s.id FOR UPDATE OF s",
    # systems: replace unless the stored row came from a newer dump build
    UPSERT_SYSTEMS,
    # old children of the replaced systems, grandchildren via RETURNING
    "DELETE FROM system_factions f USING w_sys w WHERE f.system_id64 = w.id64",
    """WITH d AS (DELETE FROM bodies b USING w_sys w WHERE b.system_id64 = w.id64 RETURNING b.id64),
            m AS (DELETE FROM body_materials x USING d WHERE x.body_id64 = d.id64),
            g AS (DELETE FROM body_signals x USING d WHERE x.body_id64 = d.id64),
            r AS (DELETE FROM body_rings x USING d WHERE x.body_id64 = d.id64),
            q AS (DELETE FROM ring_signals x USING d WHERE x.body_id64 = d.id64)
       SELECT 1""",
    """WITH d AS (DELETE FROM stations s USING w_sys w WHERE s.system_id64 = w.id64 RETURNING s.id),
            o AS (DELETE FROM station_outfitting x USING d WHERE x.station_id = d.id),
            y AS (DELETE FROM station_shipyard x USING d WHERE x.station_id = d.id)
       SELECT 1""",
    # new children of the replaced systems
    "INSERT INTO system_factions SELECT t.* FROM tmp_system_factions t JOIN w_sys w ON w.id64 = t.system_id64",
    "INSERT INTO bodies ({c}) SELECT {tc} FROM tmp_bodies t JOIN w_sys w ON w.id64 = t.system_id64".format(
        c=", ".join(T.BODIES), tc=", ".join("t." + x for x in T.BODIES)),
    "INSERT INTO body_materials SELECT m.* FROM tmp_body_materials m JOIN tmp_bodies b ON b.id64 = m.body_id64 JOIN w_sys w ON w.id64 = b.system_id64",
    "INSERT INTO body_signals SELECT m.* FROM tmp_body_signals m JOIN tmp_bodies b ON b.id64 = m.body_id64 JOIN w_sys w ON w.id64 = b.system_id64",
    "INSERT INTO body_rings SELECT m.* FROM tmp_body_rings m JOIN tmp_bodies b ON b.id64 = m.body_id64 JOIN w_sys w ON w.id64 = b.system_id64",
    "INSERT INTO ring_signals SELECT m.* FROM tmp_ring_signals m JOIN tmp_bodies b ON b.id64 = m.body_id64 JOIN w_sys w ON w.id64 = b.system_id64",
    # stations: last sighting wins (a moving carrier may currently live under
    # another system with a newer timestamp); w_st = rows actually written
    UPSERT_STATIONS,
    "DELETE FROM station_outfitting x USING w_st w WHERE x.station_id = w.id",
    "DELETE FROM station_shipyard x USING w_st w WHERE x.station_id = w.id",
    "INSERT INTO station_outfitting SELECT t.* FROM tmp_station_outfitting t JOIN w_st w ON w.id = t.station_id",
    "INSERT INTO station_shipyard SELECT t.* FROM tmp_station_shipyard t JOIN w_st w ON w.id = t.station_id",
    # market rows go to this run's rebuild table (see finalize_update)
    "INSERT INTO {market} SELECT t.* FROM tmp_station_commodities t JOIN w_st w ON w.id = t.station_id",
    "SELECT (SELECT count(*) FROM w_sys), (SELECT count(*) FROM w_st)",
]


def apply_update(conn, payloads: dict[str, bytes], market: str) -> dict[str, int]:
    """Replace-by-system merge of one batch, in one transaction."""
    counts = {}
    with conn.transaction():
        cur = conn.cursor()
        for t, data in payloads.items():
            with cur.copy(copy_sql(t, "tmp_" + t)) as cp:
                cp.write(data)
        for stmt in APPLY_UPDATE:
            if isinstance(stmt, str):
                stmt = stmt.replace("{market}", market)
            cur.execute(stmt)
        applied, written = cur.fetchone()
        counts["systems_applied"] = applied
        counts["stations_written"] = written
    return counts


# ---------------------------------------------------------------------------
# finalisation
# ---------------------------------------------------------------------------
class Pool:
    """Run SQL statements concurrently, one connection per thread."""

    def __init__(self, dsn: str, size: int, session_sql: str = ""):
        self.dsn, self.size, self.session_sql = dsn, size, session_sql
        self.local = threading.local()

    def _conn(self):
        c = getattr(self.local, "conn", None)
        if c is None or c.closed:
            c = self.local.conn = connect(self.dsn, autocommit=True, app="ed_import:finalize")
            if self.session_sql:
                c.execute(self.session_sql)
        return c

    def _one(self, stmt):
        t = time.perf_counter()
        self._conn().execute(stmt)
        dt = time.perf_counter() - t
        log.info("%6.1fs  %s", dt, re.sub(r"\s+", " ", str(stmt))[:110])
        return dt

    def run(self, stmts: list) -> float:
        with ThreadPoolExecutor(max_workers=self.size) as ex:
            return sum(ex.map(self._one, stmts))


def build_indexes(dsn: str, stmts: list[str], *, parallel: int = 4, order_by_size: bool = True) -> float:
    """Btree builds (parallelisable in PG17) on `parallel` connections with a
    private 2 GB maintenance_work_mem; GIN/GiST builds are single-threaded
    and run on their own connections alongside. Biggest tables first so the
    phase does not end with one long tail."""
    serial = [s for s in stmts if re.search(r"USING\s+(gin|gist)", s, re.I)]
    btree = [s for s in stmts if s not in serial]
    if order_by_size:
        with connect(dsn, autocommit=True) as c:
            sizes = dict(c.execute("SELECT relname, pg_table_size(oid) FROM pg_class WHERE relkind IN ('r','p')"))

        def size_of(s):
            m = re.search(r"(?:ON|ALTER TABLE)\s+(\w+)", s)
            return -sizes.get(m.group(1), 0) if m else 0
        btree.sort(key=size_of)
        serial.sort(key=size_of)
    session = "SET maintenance_work_mem = '2GB'; SET max_parallel_maintenance_workers = 4"
    t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(Pool(dsn, parallel, session).run, btree)
        f2 = ex.submit(Pool(dsn, 2, session).run, serial)
        f1.result(), f2.result()
    return time.perf_counter() - t


def dedupe_fresh(conn, market: str) -> dict[str, int]:
    """A station listed under two systems of the dump (never seen in practice,
    but nothing in the format forbids it) would break the PK build: keep the
    most recent sighting. Same for a system appearing twice."""
    out = {}
    n = conn.execute("SELECT count(*) - count(DISTINCT id64) FROM systems").fetchone()[0]
    if n:
        conn.execute("""DELETE FROM systems a USING (SELECT ctid, row_number() OVER (PARTITION BY id64 ORDER BY built_at DESC, ctid) rn FROM systems) b
                        WHERE a.ctid = b.ctid AND b.rn > 1""")
        out["dup_systems_removed"] = n
    n = conn.execute("SELECT count(*) - count(DISTINCT id) FROM stations").fetchone()[0]
    if n:
        conn.execute("""CREATE TEMP TABLE dup_st AS SELECT id FROM stations GROUP BY id HAVING count(*) > 1;
            DELETE FROM stations a USING (SELECT s.ctid, row_number() OVER (PARTITION BY s.id ORDER BY s.version_ts DESC, s.ctid) rn
                                          FROM stations s JOIN dup_st d ON d.id = s.id) b WHERE a.ctid = b.ctid AND b.rn > 1""")
        for t, key in (("station_outfitting", "station_id"), ("station_shipyard", "station_id"), (market, "station_id, commodity_id")):
            conn.execute(sql.SQL("""DELETE FROM {t} a USING (SELECT s.ctid, row_number() OVER (PARTITION BY {k} ORDER BY s.ctid) rn
                                    FROM {t} s JOIN dup_st d ON d.id = s.station_id) b WHERE a.ctid = b.ctid AND b.rn > 1""").format(
                t=sql.Identifier(t), k=sql.SQL(key)))
        conn.execute("DROP TABLE dup_st")
        out["dup_stations_removed"] = n
    return out


def swap_market(conn, market: str) -> None:
    """Attach the freshly built market partition in place of the current one."""
    old = [r[0] for r in conn.execute(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'station_commodities'::regclass")]
    for attempt in range(20):
        try:
            with conn.transaction():
                conn.execute("SET LOCAL lock_timeout = '5s'")
                for o in old:
                    conn.execute(sql.SQL("ALTER TABLE station_commodities DETACH PARTITION {}").format(sql.Identifier(o)))
                conn.execute(sql.SQL("ALTER TABLE station_commodities ATTACH PARTITION {} FOR VALUES FROM (MINVALUE) TO (MAXVALUE)").format(
                    sql.Identifier(market)))
                for o in old:
                    conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(o)))
            return
        except psycopg.errors.LockNotAvailable:
            log.warning("market swap: waiting for a lock on station_commodities (attempt %d)", attempt + 1)
            time.sleep(3)
    raise RuntimeError("could not lock station_commodities to swap the market partition")


def finalize_market(dsn: str, conn, market: str, *, logged: bool, timings: dict) -> None:
    """Index, vacuum, (log) and attach the run's market table."""
    stmts = statements(read_sql("market_indexes.sql").replace("{T}", market))
    if logged:
        t = time.perf_counter()
        conn.execute(sql.SQL("ALTER TABLE {} SET LOGGED").format(sql.Identifier(market)))
        timings["market_set_logged"] = round(time.perf_counter() - t, 1)
    timings["market_indexes"] = round(build_indexes(dsn, stmts, parallel=3, order_by_size=False), 1)
    t = time.perf_counter()
    conn.execute(sql.SQL("VACUUM (FREEZE, ANALYZE) {}").format(sql.Identifier(market)))
    timings["market_vacuum"] = round(time.perf_counter() - t, 1)
    swap_market(conn, market)


def finalize_fresh(dsn: str, conn, market: str, *, logged: bool, index_parallel: int) -> dict:
    """After all workers are done: de-duplicate, SET LOGGED (before the
    indexes so they are built once), build every index, vacuum-freeze."""
    timings = {}
    t = time.perf_counter()
    timings.update(dedupe_fresh(conn, market))
    timings["dedupe"] = round(time.perf_counter() - t, 1)
    if logged:
        t = time.perf_counter()
        Pool(dsn, index_parallel).run([sql.SQL("ALTER TABLE {} SET LOGGED").format(sql.Identifier(x))
                                      for x in INPLACE_TABLES + (market,)])
        timings["set_logged"] = round(time.perf_counter() - t, 1)
    stmts = statements(read_sql("indexes.sql")) + statements(read_sql("market_indexes.sql").replace("{T}", market))
    stmts = [s for s in stmts if not re.match(r"ALTER TABLE (commodities|modules|ships) ", s)]
    timings["indexes"] = round(build_indexes(dsn, stmts, parallel=index_parallel), 1)
    t = time.perf_counter()
    Pool(dsn, index_parallel).run([sql.SQL("VACUUM (FREEZE, ANALYZE) {}").format(sql.Identifier(x))
                                  for x in INPLACE_TABLES + LOOKUP_TABLES + (market,)])
    timings["vacuum"] = round(time.perf_counter() - t, 1)
    swap_market(conn, market)
    return timings


def finalize_update(dsn: str, conn, market: str, run_id: int, *, logged: bool) -> dict:
    """Carry the markets of untouched stations into the rebuild table, index
    and swap it, vacuum the in-place tables, audit orphans."""
    timings = {}
    t = time.perf_counter()
    conn.execute(sql.SQL("""INSERT INTO {} SELECT o.* FROM station_commodities o
                            JOIN stations s ON s.id = o.station_id WHERE s.run_id IS DISTINCT FROM %s""").format(
        sql.Identifier(market)), (run_id,))
    timings["market_carry_forward"] = round(time.perf_counter() - t, 1)
    finalize_market(dsn, conn, market, logged=logged, timings=timings)
    t = time.perf_counter()
    Pool(dsn, 4).run([sql.SQL("VACUUM (ANALYZE) {}").format(sql.Identifier(x)) for x in INPLACE_TABLES])
    timings["vacuum"] = round(time.perf_counter() - t, 1)
    timings["orphans"] = orphan_audit(conn)
    return timings


def orphan_audit(conn) -> dict[str, int]:
    checks = {
        "bodies": "SELECT count(*) FROM bodies b LEFT JOIN systems s ON s.id64 = b.system_id64 WHERE s.id64 IS NULL",
        "stations": "SELECT count(*) FROM stations b LEFT JOIN systems s ON s.id64 = b.system_id64 WHERE s.id64 IS NULL",
        "system_factions": "SELECT count(*) FROM system_factions b LEFT JOIN systems s ON s.id64 = b.system_id64 WHERE s.id64 IS NULL",
        "body_materials": "SELECT count(*) FROM body_materials m LEFT JOIN bodies b ON b.id64 = m.body_id64 WHERE b.id64 IS NULL",
        "body_signals": "SELECT count(*) FROM body_signals m LEFT JOIN bodies b ON b.id64 = m.body_id64 WHERE b.id64 IS NULL",
        "body_rings": "SELECT count(*) FROM body_rings m LEFT JOIN bodies b ON b.id64 = m.body_id64 WHERE b.id64 IS NULL",
        "ring_signals": "SELECT count(*) FROM ring_signals m LEFT JOIN bodies b ON b.id64 = m.body_id64 WHERE b.id64 IS NULL",
        "station_outfitting": "SELECT count(*) FROM station_outfitting m LEFT JOIN stations s ON s.id = m.station_id WHERE s.id IS NULL",
        "station_shipyard": "SELECT count(*) FROM station_shipyard m LEFT JOIN stations s ON s.id = m.station_id WHERE s.id IS NULL",
        "station_commodities": "SELECT count(*) FROM (SELECT DISTINCT station_id FROM station_commodities) m LEFT JOIN stations s ON s.id = m.station_id WHERE s.id IS NULL",
    }
    out = {k: conn.execute(q).fetchone()[0] for k, q in checks.items()}
    bad = {k: v for k, v in out.items() if v}
    if bad:
        log.warning("orphan rows found: %s", bad)
    return out


def row_counts(conn) -> dict[str, int]:
    return {t: conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(t))).fetchone()[0]
            for t in LOAD_TABLES + LOOKUP_TABLES}
