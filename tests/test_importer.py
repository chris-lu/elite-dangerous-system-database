"""Importer tests. The COPY-format tests need a PostgreSQL reachable through
$PG_DSN (the compose database is fine; they only touch a scratch schema).

    pip install pytest
    PG_DSN=postgresql://elite_user:elite_password@localhost:5432/elite_dangerous pytest -q
"""
from __future__ import annotations

import datetime as dt
import os

import orjson
import pytest

from ed_import import flatten as F
from ed_import import tables as T
from ed_import.sync import choose_dump

UTC = dt.timezone.utc
NASTY = ["Krüger", "Pilgrim’s Ruin", "¿PORQUE TE VAS?", "Gonçalves", " ORTEM  /(Oo)(=)(OO)\\", "KOBEP-BEPTO/\\ET",
         " ", "tab\there", "new\nline", "cr\rhere", "back\\slash", "\\N", "", "quote\"d", "{brace,comma}"]


# ---------------------------------------------------------------------------
def test_i64_wrap():
    assert F.i64(5) == 5
    assert F.i64(2**63 - 1) == 2**63 - 1
    assert F.i64(2**63) == -2**63
    assert F.i64(2**64 - 1) == -1


def test_ts_key_orders_both_spellings():
    a, b = "2026-09-04 01:31:47+00", "2026-09-04T01:31:38Z"
    assert F.ts_key(a) > F.ts_key(b)
    assert F.ts_key("2022-05-11 01:37:51.977+00") > F.ts_key("2022-05-11 01:37:51+00")


@pytest.mark.parametrize("prev,cur,expect", [
    (None, dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), "galaxy_populated"),
    (dt.datetime(2026, 9, 5, 5, 43, tzinfo=UTC), dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), "galaxy_1day"),
    (dt.datetime(2026, 9, 5, 5, 43, tzinfo=UTC), dt.datetime(2026, 9, 6, 17, 30, tzinfo=UTC), "galaxy_1day"),   # 35.8 h
    (dt.datetime(2026, 9, 4, 5, 43, tzinfo=UTC), dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), "galaxy_7days"),
    (dt.datetime(2026, 8, 29, 5, 43, tzinfo=UTC), dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), "galaxy_1month"),
    (dt.datetime(2026, 7, 1, 5, 43, tzinfo=UTC), dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), "galaxy_populated"),
    (dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), None),
    (dt.datetime(2026, 9, 7, 5, 34, tzinfo=UTC), dt.datetime(2026, 9, 6, 5, 34, tzinfo=UTC), None),
])
def test_choose_dump(prev, cur, expect):
    assert choose_dump(prev, cur) == expect


def test_choose_dump_rejects_naive():
    with pytest.raises(ValueError):
        choose_dump(None, dt.datetime(2026, 9, 6, 5, 34))


# ---------------------------------------------------------------------------
def make_system(**over):
    s = {
        "id64": 10477373803, "name": "Sol", "coords": {"x": 0, "y": 0, "z": 0}, "date": "2026-09-04 01:31:47+00",
        "allegiance": "Federation", "population": 22780919531, "security": "High",
        "controllingFaction": {"name": "Mother Gaia", "state": "Boom"},
        "factions": [{"name": "Mother Gaia", "influence": 0.5, "state": "Boom", "activeStates": [{"state": "Boom"}]},
                     {"name": "Mother Gaia", "influence": 0.1}],   # duplicate name
        "powers": ["Aisling Duval", "Zemina Torval"],
        "bodies": [
            {"id64": 2**63 + 5, "bodyId": 256, "name": "Sol", "type": "Star", "updateTime": "2026-09-04 01:31:47+00",
             "subType": "G (White-Yellow) Star", "mainStar": True, "solidComposition": None,
             "belts": [{"name": "Main Belt", "type": "Rocky", "mass": 1.5, "innerRadius": 1, "outerRadius": 2}],
             "stations": [{"id": 3, "name": "Surface Port", "updateTime": "2026-09-04 02:00:00+00",
                           "market": {"updateTime": "2026-09-04 03:00:00+00",
                                      "commodities": [{"commodityId": 1, "name": "Gold", "symbol": "gold", "category": "Metals",
                                                       "demand": 2147483647, "supply": 0, "buyPrice": 0, "sellPrice": 50000},
                                                      {"commodityId": 1, "name": "Gold", "symbol": "gold", "category": "Metals",
                                                       "demand": 1, "supply": 1, "buyPrice": 1, "sellPrice": 1}]}}]},
            {"id64": 36028797029285259, "bodyId": 1, "name": "Earth", "type": "Planet", "updateTime": "2026-09-04 01:31:47+00",
             "subType": "Earth-like world", "solidComposition": {"Ice": 0.0, "Metal": 32.3, "Rock": 67.7},
             "materials": {"Iron": 20.5, "Polonium": 0.4}, "parents": [{"Star": 0}],
             "signals": {"signals": {"$SAA_SignalType_Biological;": 3}, "genuses": ["$Codex_Ent_Bacterial_Genus_Name;"],
                         "updateTime": "2026-09-04 04:00:00+00"},
             "rings": [{"name": "Earth A Ring", "type": "Icy", "mass": 1, "innerRadius": 1, "outerRadius": 2, "id64": 2**64 - 1,
                        "signals": {"signals": {"Platinum": 2}, "updateTime": "2026-09-04 01:00:00+00"}}]},
        ],
        "stations": [
            {"id": 1, "name": "K4Z-8QY", "carrierName": "Nasty " + NASTY[4], "updateTime": "2022-03-09 21:25:54+00",
             "type": "Drake-Class Carrier", "services": NASTY, "economies": {"Private Enterprise": 100.0},
             "landingPads": {"large": 8, "medium": 4, "small": 4},
             "outfitting": {"updateTime": "2026-09-04 05:00:00+00",
                            "modules": [{"moduleId": 20, "name": "m", "symbol": "m", "class": 1, "rating": "A", "category": "internal"},
                                        {"moduleId": 10, "name": "n", "symbol": "n", "class": 2, "rating": "B", "category": "mercgear"},
                                        {"moduleId": 20, "name": "m", "symbol": "m", "class": 1, "rating": "A", "category": "internal"}]},
             "shipyard": {"updateTime": "2026-09-04 05:00:00+00", "ships": [{"shipId": 128049249, "name": "Sidewinder", "symbol": "sidewinder"}]}},
            {"id": 2, "name": NASTY[7], "updateTime": "2026-09-04 01:31:47+00", "type": "Outpost"},
        ],
    }
    s.update(over)
    return s


def known_enums():
    from ed_import import db
    with open(os.path.join(os.path.dirname(__file__), "..", "sql", "enums.sql"), encoding="utf-8") as f:
        import re
        known = {}
        for m in re.finditer(r"CREATE TYPE (\w+) AS ENUM \((.*?)\);", f.read()):
            known[m.group(1)] = {x.strip()[1:-1].replace("''", "'") for x in m.group(2).split(", ")}
    return known


def flatten_one(s):
    tracker = F.EnumTracker(known_enums())
    b = F.Batch(tracker, 42, "2026-09-05 05:43:52+00", {t: set() for t in ("commodities", "modules", "ships")})
    b.add_system(s)
    return b, tracker, b.payloads(), b.lookup_payloads()


def test_flatten_dedup_and_shapes():
    b, tracker, pay, lk = flatten_one(make_system())
    assert b.stats["dup_factions_dropped"] == 1
    assert b.stats["dup_commodities_dropped"] == 1
    assert pay["system_factions"].count(b"\n") == 1
    assert pay["station_commodities"] == b"3\t1\t2147483647\t0\t0\t50000\n"
    rows = {r.split(b"\t")[0]: r for r in pay["station_outfitting"].splitlines()}
    assert rows[b"1"].endswith(b"\t{10,20}")
    assert tracker.take_new() == {"module_category_t": {"mercgear"}}
    # body id64 with bit 63 set is wrapped and reused for the children
    bodies = pay["bodies"].decode().splitlines()
    assert bodies[0].split("\t")[0] == str(2**63 + 5 - 2**64)
    assert pay["body_rings"].decode().splitlines()[0].startswith(str(2**63 + 5 - 2**64) + "\t\\N")
    assert pay["body_rings"].decode().splitlines()[1].split("\t")[1] == "-1"   # ring id64 2**64-1
    # version_ts folds the newest nested timestamp (outfitting 05:00)
    sysrow = pay["systems"].decode().split("\t")
    assert sysrow[T.SYSTEMS.index("version_ts")] == "2026-09-04 05:00:00+00"
    strow = {r.split("\t")[0]: r.split("\t") for r in pay["stations"].decode().splitlines()}
    assert strow["1"][T.STATIONS.index("version_ts")] == "2026-09-04 05:00:00+00"
    assert strow["3"][T.STATIONS.index("version_ts")] == "2026-09-04 03:00:00+00"
    assert strow["3"][T.STATIONS.index("body_id64")] == str(2**63 + 5 - 2**64)
    assert strow["1"][T.STATIONS.index("run_id")] == "42"
    assert set(lk) == {"commodities", "modules", "ships"}


def test_station_dedup_across_systems_keeps_newest():
    tracker = F.EnumTracker(known_enums())
    b = F.Batch(tracker, 1, "2026-09-05 05:43:52+00", {t: set() for t in ("commodities", "modules", "ships")})
    old = make_system(id64=1, name="A", stations=[{"id": 9, "name": "C", "updateTime": "2022-01-01 00:00:00+00"}], bodies=[])
    new = make_system(id64=2, name="B", stations=[{"id": 9, "name": "C", "updateTime": "2026-01-01 00:00:00+00"}], bodies=[])
    b.add_system(new)
    b.add_system(old)
    pay = b.payloads()
    rows = pay["stations"].decode().splitlines()
    assert len(rows) == 1 and rows[0].split("\t")[1] == "2"
    assert b.stats["dup_stations_dropped"] == 1


# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def pg():
    import psycopg
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        pytest.skip("PG_DSN not set")
    conn = psycopg.connect(dsn, autocommit=True, options="-c client_encoding=UTF8")
    conn.execute("DROP SCHEMA IF EXISTS ed_test CASCADE; CREATE SCHEMA ed_test; SET search_path = ed_test")
    yield conn
    conn.execute("DROP SCHEMA ed_test CASCADE")
    conn.close()


def test_copy_escaping_roundtrip(pg):
    pg.execute("CREATE TABLE t (i int, s text, a text[], j jsonb, b boolean, n real)")
    rows = []
    for i, s in enumerate(NASTY):
        rows.append(f"{i}\t{F.esc(s)}\t{F.text_array([s, 'x', ''])}\t{F.jsonb({'k': s, 'n': [1, s]})}\t{F.boolean(i % 2 == 0)}\t{F.num(i / 3)}\n")
    rows.append(f"99\t{F.opt(None)}\t{F.text_array(None)}\t{F.jsonb(None)}\t{F.boolean(None)}\t{F.num(None)}\n")
    rows.append(f"98\t{F.opt('')}\t{F.text_array([])}\t{F.jsonb({})}\t{F.boolean(False)}\t{F.num(0)}\n")
    with pg.cursor().copy("COPY t (i, s, a, j, b, n) FROM STDIN") as cp:
        cp.write("".join(rows).encode())
    got = {r[0]: r[1:] for r in pg.execute("SELECT * FROM t")}
    for i, s in enumerate(NASTY):
        assert got[i][0] == s, (i, s)
        assert got[i][1] == [s, "x", ""]
        assert got[i][2] == {"k": s, "n": [1, s]}
        assert got[i][3] is (i % 2 == 0)
        assert abs(got[i][4] - i / 3) < 1e-5
    assert got[99] == (None, None, None, None, None)
    assert got[98] == ("", [], {}, False, 0.0)


def test_timestamps_parse(pg):
    r = pg.execute("SELECT %s::timestamptz, %s::timestamptz, %s::timestamptz",
                   ("2026-09-04 01:31:47+00", "2022-05-11 01:37:51.977+00", "2026-09-04T01:31:38Z")).fetchone()
    assert r[0] == dt.datetime(2026, 9, 4, 1, 31, 47, tzinfo=UTC)
    assert r[1].microsecond == 977000
    assert r[2] == dt.datetime(2026, 9, 4, 1, 31, 38, tzinfo=UTC)


def test_real_record_roundtrip(pg):
    """Flatten a real system and COPY every table into scratch copies of the
    real schema (types included), then read a few values back."""
    sample = os.path.join(os.path.dirname(__file__), "fixtures", "herschel36.json")
    if not os.path.exists(sample):
        pytest.skip("fixture missing")
    s = orjson.loads(open(sample, "rb").read())
    b, tracker, pay, lk = flatten_one(s)
    for t in list(T.TABLES):
        pg.execute(f"CREATE TABLE {t} (LIKE public.{t})")
    for t, data in pay.items():
        with pg.cursor().copy(f"COPY {t} ({','.join(T.TABLES[t])}) FROM STDIN") as cp:
            cp.write(data)
    assert pg.execute("SELECT name FROM systems").fetchone()[0] == s["name"]
    assert pg.execute("SELECT count(*) FROM stations").fetchone()[0] == b.stats["rows_stations"]
    assert pg.execute("SELECT count(*) FROM station_commodities").fetchone()[0] == b.stats["rows_station_commodities"]
