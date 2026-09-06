"""Turn Spansh system records into COPY TEXT rows for every table.

This is the CPU hot path of the importer (≈ 50 M child rows per daily dump),
so it is written as straight-line code: one format expression per row, no
per-row helper calls for the big tables, and escaping only where a value can
actually contain a special character.

COPY TEXT rules used here (https://www.postgresql.org/docs/current/sql-copy.html):
  * TAB between columns, LF between rows, \\N for NULL
  * in text values, only backslash / TAB / LF / CR need escaping
  * array literals ({"a","b"}) escape \\ and " inside elements and are then
    COPY-escaped as a whole
  * orjson never emits raw control characters, so jsonb only needs its
    backslashes doubled

Per-batch de-duplication is done here (a station listed under two systems of
the same batch, a commodity repeated in one market, ...) so the database side
can rely on unique natural keys.
"""
from __future__ import annotations

from collections import defaultdict

import orjson

from . import tables as T

NULL = "\\N"
_WRAP = 1 << 64
_HALF = 1 << 63


def i64(v: int) -> int:
    """uint64 -> bigint (two's complement). Body/ring id64 can set bit 63."""
    return v - _WRAP if v >= _HALF else v


def esc(s: str) -> str:
    """COPY-escape a text value. Chained replace: each call returns the very
    same object when nothing matches, which is the common case."""
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def opt(s) -> str:
    return NULL if s is None else esc(s)


def num(v) -> str:
    return NULL if v is None else str(v)


def boolean(v) -> str:
    return NULL if v is None else ("t" if v else "f")


def jsonb(v) -> str:
    return NULL if v is None else orjson.dumps(v).decode().replace("\\", "\\\\")


def text_array(items) -> str:
    if items is None:
        return NULL
    if not items:
        return "{}"
    return esc("{" + ",".join('"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"' for s in items) + "}")


def int_array(ids) -> str:
    return "{" + ",".join(map(str, ids)) + "}"


def ts_key(t: str) -> str:
    """Comparable form of the two timestamp spellings found in the dumps,
    'YYYY-MM-DD HH:MM:SS+00' and 'YYYY-MM-DDTHH:MM:SSZ'."""
    return t.replace("T", " ", 1)


class EnumTracker:
    """Maps enum labels to their escaped COPY form and records labels the
    database does not know yet (the worker adds them with ALTER TYPE before
    the batch is written)."""

    __slots__ = ("known", "new", "cache")

    def __init__(self, known: dict[str, set[str]]):
        self.known = known
        self.new: dict[str, set[str]] = defaultdict(set)
        self.cache: dict[str, str] = {}

    def __call__(self, typ: str, v) -> str:
        if v is None:
            return NULL
        if v not in self.known[typ]:
            self.known[typ].add(v)
            self.new[typ].add(v)
        e = self.cache.get(v)
        if e is None:
            e = self.cache[v] = esc(v)
        return e

    def take_new(self) -> dict[str, set[str]]:
        new, self.new = self.new, defaultdict(set)
        return new


class Batch:
    """Accumulates the rows of one batch of systems.

    rows[table]  list of str rows for the system-level tables
    stations     station id -> (version, station row, outfitting row, shipyard row, [commodity rows])
                 kept as a dict so a station listed under several systems in
                 the batch is written once, with its most recent sighting
    lookups      table -> {id: row} for commodities/modules/ships not yet in the DB
    """

    __slots__ = ("enum", "run_id", "built_at", "known_lookups", "rows", "stations", "lookups", "seen_systems", "stats")

    def __init__(self, enum: EnumTracker, run_id: int, built_at: str, known_lookups: dict[str, set[int]]):
        self.enum = enum
        self.run_id = run_id
        self.built_at = built_at
        self.known_lookups = known_lookups
        self.rows: dict[str, list[str]] = {t: [] for t in T.TABLES}
        self.stations: dict[int, tuple] = {}
        self.lookups: dict[str, dict[int, str]] = {"commodities": {}, "modules": {}, "ships": {}}
        self.seen_systems: set[int] = set()
        self.stats = defaultdict(int)

    # ------------------------------------------------------------------
    def add_line(self, line: bytes) -> None:
        self.add_system(orjson.loads(line))

    def add_system(self, s: dict) -> None:
        sid = i64(s["id64"])
        if sid in self.seen_systems:
            self.stats["dup_systems_dropped"] += 1
            return
        E = self.enum
        rows_factions: list[str] = []
        rows_bodies: list[str] = []
        rows_materials: list[str] = []
        rows_signals: list[str] = []
        rows_rings: list[str] = []
        rows_ring_signals: list[str] = []
        stations: dict[int, tuple] = {}

        vmax = s["date"]
        vkey = ts_key(vmax)

        # ---- factions ---------------------------------------------------
        seen_f: set[str] = set()
        for f in s.get("factions") or ():
            fname = f["name"]
            if fname in seen_f:
                self.stats["dup_factions_dropped"] += 1
                continue
            seen_f.add(fname)
            rows_factions.append(
                f"{sid}\t{num(f.get('influence'))}\t{E('allegiance_t', f.get('allegiance'))}\t"
                f"{E('government_t', f.get('government'))}\t{E('faction_state_t', f.get('state'))}\t"
                f"{esc(fname)}\t{jsonb(f.get('activeStates'))}\t{jsonb(f.get('pendingStates'))}\t"
                f"{jsonb(f.get('recoveringStates'))}\n"
            )

        # ---- bodies -----------------------------------------------------
        for b in s.get("bodies") or ():
            bid = i64(b["id64"])
            but = b.get("updateTime")
            if but is not None:
                k = ts_key(but)
                if k > vkey:
                    vkey, vmax = k, but
            sc = b.get("solidComposition")
            if sc:
                comp = f"{num(sc.get('Ice'))}\t{num(sc.get('Metal'))}\t{num(sc.get('Rock'))}"
            else:
                comp = "\\N\t\\N\t\\N"
            sig = b.get("signals")
            genuses = None
            if sig:
                genuses = sig.get("genuses")
                sut = sig.get("updateTime")
                if sut is not None:
                    k = ts_key(sut)
                    if k > vkey:
                        vkey, vmax = k, sut
                for name, count in (sig.get("signals") or {}).items():
                    rows_signals.append(f"{bid}\t{count}\t{E('signal_t', name)}\n")
            rows_bodies.append(
                f"{bid}\t{sid}\t{opt(but)}\t"
                f"{num(b.get('distanceToArrival'))}\t{num(b.get('age'))}\t{num(b.get('absoluteMagnitude'))}\t"
                f"{num(b.get('solarMasses'))}\t{num(b.get('solarRadius'))}\t{num(b.get('surfaceTemperature'))}\t"
                f"{num(b.get('rotationalPeriod'))}\t{num(b.get('axialTilt'))}\t{num(b.get('gravity'))}\t"
                f"{num(b.get('earthMasses'))}\t{num(b.get('radius'))}\t{num(b.get('surfacePressure'))}\t"
                f"{num(b.get('orbitalPeriod'))}\t{num(b.get('semiMajorAxis'))}\t{num(b.get('orbitalEccentricity'))}\t"
                f"{num(b.get('orbitalInclination'))}\t{num(b.get('argOfPeriapsis'))}\t{num(b.get('meanAnomaly'))}\t"
                f"{num(b.get('ascendingNode'))}\t{comp}\t"
                f"{E('body_type_t', b['type'])}\t{E('body_sub_type_t', b.get('subType'))}\t"
                f"{E('luminosity_t', b.get('luminosity'))}\t{E('volcanism_t', b.get('volcanismType'))}\t"
                f"{E('atmosphere_t', b.get('atmosphereType'))}\t{E('terraforming_t', b.get('terraformingState'))}\t"
                f"{E('reserve_level_t', b.get('reserveLevel'))}\t"
                f"{b['bodyId']}\t{boolean(b.get('mainStar'))}\t{boolean(b.get('rotationalPeriodTidallyLocked'))}\t"
                f"{boolean(b.get('isLandable'))}\t"
                f"{esc(b['name'])}\t{opt(b.get('spectralClass'))}\t{jsonb(b.get('parents'))}\t"
                f"{jsonb(b.get('atmosphereComposition'))}\t{text_array(genuses)}\n"
            )
            mats = b.get("materials")
            if mats:
                for name, pct in mats.items():
                    rows_materials.append(f"{bid}\t{pct}\t{E('material_t', name)}\n")
            for is_belt, key in (("f", "rings"), ("t", "belts")):
                rl = b.get(key)
                if not rl:
                    continue
                seen_r: set[str] = set()
                for r in rl:
                    rname = r["name"]
                    if rname in seen_r:
                        self.stats["dup_rings_dropped"] += 1
                        continue
                    seen_r.add(rname)
                    rid = r.get("id64")
                    rows_rings.append(
                        f"{bid}\t{NULL if rid is None else i64(rid)}\t{num(r.get('mass'))}\t"
                        f"{num(r.get('innerRadius'))}\t{num(r.get('outerRadius'))}\t{E('ring_type_t', r['type'])}\t"
                        f"{is_belt}\t{esc(rname)}\n"
                    )
                    rs = r.get("signals")
                    if rs:
                        rut = rs.get("updateTime")
                        if rut is not None:
                            k = ts_key(rut)
                            if k > vkey:
                                vkey, vmax = k, rut
                        ername = esc(rname)
                        for name, count in (rs.get("signals") or {}).items():
                            rows_ring_signals.append(f"{bid}\t{opt(rut)}\t{count}\t{E('signal_t', name)}\t{ername}\n")
            for st in b.get("stations") or ():
                k = self._station(st, sid, bid, stations)
                if k > vkey:
                    vkey = k

        # ---- orbital stations -------------------------------------------
        for st in s.get("stations") or ():
            k = self._station(st, sid, None, stations)
            if k > vkey:
                vkey = k
        if vkey != ts_key(vmax):
            vmax = vkey  # a station timestamp won; the normalised form is valid timestamptz input too

        # ---- system row ---------------------------------------------------
        c = s["coords"]
        ts = s.get("timestamps") or {}
        cf = s.get("controllingFaction")
        row = (
            f"{sid}\t{num(s.get('population'))}\t{s['date']}\t{self.built_at}\t{vmax}\t"
            f"{opt(ts.get('controllingPower'))}\t{opt(ts.get('factions'))}\t{opt(ts.get('powerState'))}\t{opt(ts.get('powers'))}\t"
            f"{c['x']}\t{c['y']}\t{c['z']}\t"
            f"{num(s.get('powerStateControlProgress'))}\t{num(s.get('powerStateReinforcement'))}\t{num(s.get('powerStateUndermining'))}\t"
            f"{E('allegiance_t', s.get('allegiance'))}\t{E('government_t', s.get('government'))}\t"
            f"{E('economy_t', s.get('primaryEconomy'))}\t{E('economy_t', s.get('secondaryEconomy'))}\t"
            f"{E('security_t', s.get('security'))}\t{E('power_state_t', s.get('powerState'))}\t"
            f"{E('power_t', s.get('controllingPower'))}\t{E('faction_state_t', cf.get('state') if cf else None)}\t"
            f"{num(s.get('bodyCount'))}\t{esc(s['name'])}\t{opt(cf['name'] if cf else None)}\t"
            f"{text_array(s.get('powers'))}\t{jsonb(s.get('powerConflictProgress'))}\t{jsonb(s.get('thargoidWar'))}\n"
        )

        # ---- commit the system into the batch buffers ---------------------
        self.seen_systems.add(sid)
        R = self.rows
        R["systems"].append(row)
        R["system_factions"].extend(rows_factions)
        R["bodies"].extend(rows_bodies)
        R["body_materials"].extend(rows_materials)
        R["body_signals"].extend(rows_signals)
        R["body_rings"].extend(rows_rings)
        R["ring_signals"].extend(rows_ring_signals)
        S = self.stations
        for stid, entry in stations.items():
            old = S.get(stid)
            if old is not None:
                self.stats["dup_stations_dropped"] += 1
                if old[0] >= entry[0]:
                    continue
            S[stid] = entry
        self.stats["systems"] += 1

    # ------------------------------------------------------------------
    def _station(self, st: dict, sid: int, bid, stations: dict) -> str:
        """Flatten one station; returns the comparable version key."""
        E = self.enum
        stid = st["id"]
        ut = st["updateTime"]
        vkey = ts_key(ut)
        vmax = ut
        mk = st.get("market")
        sy = st.get("shipyard")
        of = st.get("outfitting")
        mk_ut = sy_ut = of_ut = None
        if mk:
            mk_ut = mk.get("updateTime")
            if mk_ut is not None:
                k = ts_key(mk_ut)
                if k > vkey:
                    vkey, vmax = k, mk_ut
        if sy:
            sy_ut = sy.get("updateTime")
            if sy_ut is not None:
                k = ts_key(sy_ut)
                if k > vkey:
                    vkey, vmax = k, sy_ut
        if of:
            of_ut = of.get("updateTime")
            if of_ut is not None:
                k = ts_key(of_ut)
                if k > vkey:
                    vkey, vmax = k, of_ut
        pads = st.get("landingPads")
        if pads:
            pads_s = f"{num(pads.get('large'))}\t{num(pads.get('medium'))}\t{num(pads.get('small'))}"
        else:
            pads_s = "\\N\t\\N\t\\N"
        row = (
            f"{stid}\t{sid}\t{NULL if bid is None else bid}\t{ut}\t{vmax}\t"
            f"{opt(mk_ut)}\t{opt(sy_ut)}\t{opt(of_ut)}\t"
            f"{num(st.get('distanceToArrival'))}\t{num(st.get('latitude'))}\t{num(st.get('longitude'))}\t"
            f"{E('station_type_t', st.get('type'))}\t{E('station_state_t', st.get('state'))}\t"
            f"{E('allegiance_t', st.get('allegiance'))}\t{E('government_t', st.get('government'))}\t"
            f"{E('economy_t', st.get('primaryEconomy'))}\t{E('economy_t', st.get('secondaryEconomy'))}\t"
            f"{E('faction_state_t', st.get('controllingFactionState'))}\t{self.run_id}\t{pads_s}\t"
            f"{'t' if mk else 'f'}\t{'t' if sy else 'f'}\t{'t' if of else 'f'}\t"
            f"{esc(st['name'])}\t{opt(st.get('realName'))}\t{opt(st.get('carrierName'))}\t"
            f"{opt(st.get('controllingFaction'))}\t{opt(st.get('carrierDockingAccess'))}\t"
            f"{text_array(st.get('services'))}\t{text_array(mk.get('prohibitedCommodities') if mk else None)}\t"
            f"{jsonb(st.get('economies'))}\n"
        )

        comm_rows: list[str] = []
        if mk:
            cl = mk.get("commodities")
            if cl:
                known = self.known_lookups["commodities"]
                ids = [co["commodityId"] for co in cl]
                if len(set(ids)) == len(ids) and known.issuperset(ids):
                    # common case (31M rows/day): no duplicate, nothing new -> one comprehension
                    comm_rows = ["%d\t%d\t%d\t%d\t%d\t%d\n" % (stid, co["commodityId"], co["demand"], co["supply"], co["buyPrice"], co["sellPrice"])
                                 for co in cl]
                else:
                    new = self.lookups["commodities"]
                    seen: set[int] = set()
                    for co in cl:
                        cid = co["commodityId"]
                        if cid in seen:
                            self.stats["dup_commodities_dropped"] += 1
                            continue
                        seen.add(cid)
                        if cid not in known:
                            known.add(cid)
                            new[cid] = f"{cid}\t{E('commodity_category_t', co.get('category'))}\t{esc(co['name'])}\t{esc(co['symbol'])}\n"
                        comm_rows.append("%d\t%d\t%d\t%d\t%d\t%d\n" % (stid, cid, co["demand"], co["supply"], co["buyPrice"], co["sellPrice"]))

        of_row = None
        if of:
            ml = of.get("modules")
            if ml:
                known = self.known_lookups["modules"]
                ids = {m["moduleId"] for m in ml}
                if not known.issuperset(ids):
                    new = self.lookups["modules"]
                    for m in ml:
                        mid = m["moduleId"]
                        if mid not in known:
                            known.add(mid)
                            new[mid] = (
                                f"{mid}\t{num(m.get('class'))}\t{E('module_rating_t', m.get('rating'))}\t"
                                f"{E('module_category_t', m.get('category'))}\t{esc(m['name'])}\t{esc(m['symbol'])}\t{opt(m.get('ship'))}\n"
                            )
                of_row = f"{stid}\t{opt(of_ut)}\t{int_array(sorted(ids))}\n"

        sy_row = None
        if sy:
            sl = sy.get("ships")
            if sl:
                known = self.known_lookups["ships"]
                new = self.lookups["ships"]
                ids = set()
                for sh in sl:
                    shid = sh["shipId"]
                    ids.add(shid)
                    if shid not in known:
                        known.add(shid)
                        new[shid] = f"{shid}\t{esc(sh['name'])}\t{esc(sh['symbol'])}\n"
                sy_row = f"{stid}\t{opt(sy_ut)}\t{int_array(sorted(ids))}\n"

        old = stations.get(stid)
        if old is not None:
            self.stats["dup_stations_dropped"] += 1
            if old[0] >= vkey:
                return vkey
        stations[stid] = (vkey, row, of_row, sy_row, comm_rows)
        return vkey

    # ------------------------------------------------------------------
    def payloads(self) -> dict[str, bytes]:
        """Finish the batch: emit station rows and encode every table."""
        R = self.rows
        st_rows = R["stations"]
        of_rows = R["station_outfitting"]
        sy_rows = R["station_shipyard"]
        cm_rows = R["station_commodities"]
        for _, row, of_row, sy_row, comm in self.stations.values():
            st_rows.append(row)
            if of_row is not None:
                of_rows.append(of_row)
            if sy_row is not None:
                sy_rows.append(sy_row)
            cm_rows.extend(comm)
        out = {}
        for t, rows in R.items():
            if rows:
                out[t] = "".join(rows).encode()
                self.stats["rows_" + t] += len(rows)
        return out

    def lookup_payloads(self) -> dict[str, bytes]:
        return {t: "".join(v for _, v in sorted(rows.items())).encode() for t, rows in self.lookups.items() if rows}
