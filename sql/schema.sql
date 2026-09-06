-- Elite Dangerous galaxy database (Spansh dump) - tables.
--
-- Applied by `ed_import fresh` after `DROP SCHEMA public CASCADE`. Tables are
-- created without primary keys / indexes (see indexes.sql) so that the bulk
-- COPY runs against bare heaps; the importer adds them afterwards.
--
-- Conventions
--   * ids are the source ids: system/body id64 (uint64, stored two's-complement
--     wrapped in bigint, see id64_u()/id64_s()), station id (= market id,
--     global: fleet carriers keep their id when they move system).
--   * columns are declared in alignment order (8-byte, 4-byte, 2-byte, bool,
--     varlena) which saves 10-15 B/row of padding on the wide tables.
--   * low-cardinality strings are native ENUMs (enums.sql): 4 bytes, compare
--     directly to literals (allegiance = 'Federation'); cast to text for
--     ILIKE / concatenation (allegiance::text ILIKE 'fed%').
--   * `real` (float4) for physical measurements: Elite coordinates are
--     multiples of 1/32 ly with |coord| < 65,631, exactly representable.

CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;   -- needs superuser (POSTGRES_USER is one)

-- uint64 <-> bigint helpers. Source id64 values can set bit 63 (body id64 =
-- system id64 | bodyId << 55), stored wrapped so keys stay 8 bytes.
CREATE FUNCTION id64_u(bigint) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
$$ SELECT CASE WHEN $1 < 0 THEN $1::numeric + 18446744073709551616 ELSE $1::numeric END $$;
CREATE FUNCTION id64_s(numeric) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
$$ SELECT CASE WHEN $1 >= 9223372036854775808 THEN ($1 - 18446744073709551616)::bigint ELSE $1::bigint END $$;
COMMENT ON FUNCTION id64_u(bigint) IS 'stored (wrapped) id64 -> unsigned value as shown by Spansh/EDSM/Inara';
COMMENT ON FUNCTION id64_s(numeric) IS 'unsigned id64 (as pasted from Spansh/journal SystemAddress) -> stored bigint; IMMUTABLE so WHERE id64 = id64_s(...) uses the PK';

---------------------------------------------------------------------------
CREATE TABLE systems (
    id64                         bigint      NOT NULL,
    population                   bigint,
    date                         timestamptz,            -- source "date": last update of the system record
    built_at                     timestamptz NOT NULL,   -- build time of the dump this row came from (replace guard)
    version_ts                   timestamptz,            -- newest timestamp anywhere inside the record (bodies, stations, markets...)
    ts_controlling_power         timestamptz,            -- source timestamps.*
    ts_factions                  timestamptz,
    ts_power_state               timestamptz,
    ts_powers                    timestamptz,
    x                            real        NOT NULL,
    y                            real        NOT NULL,
    z                            real        NOT NULL,
    power_state_control_progress real,
    power_state_reinforcement    real,
    power_state_undermining      real,
    allegiance                   allegiance_t,
    government                   government_t,
    primary_economy              economy_t,
    secondary_economy            economy_t,
    security                     security_t,
    power_state                  power_state_t,
    controlling_power            power_t,
    controlling_faction_state    faction_state_t,
    body_count                   smallint,
    name                         text        NOT NULL,
    controlling_faction          text,
    powers                       text[],
    power_conflict_progress      jsonb,                  -- [{"power":..,"progress":..}]
    thargoid_war                 jsonb,
    -- 3-D point for the GiST index: coords <@ cube_enlarge(p, r, 3), coords <-> p
    coords    cube    GENERATED ALWAYS AS (cube(ARRAY[x, y, z]::float8[])) STORED,
    -- false for procedurally generated names ("Praea Euq XX-A b12-3"), which
    -- are >90% of systems and useless to search by substring
    is_named  boolean GENERATED ALWAYS AS (name !~ ' [A-Z][A-Z]-[A-Z] [a-h][0-9]') STORED
);
COMMENT ON TABLE systems IS 'One row per star system (Spansh galaxy dump)';
COMMENT ON COLUMN systems.id64 IS 'uint64 SystemAddress stored two''s-complement wrapped; use id64_u()/id64_s(); never ORDER BY it';
COMMENT ON COLUMN systems.built_at IS 'Last-Modified of the dump that wrote this row. A system is only replaced by a dump built at the same time or later.';

CREATE TABLE system_factions (
    system_id64       bigint NOT NULL,
    influence         real,
    allegiance        allegiance_t,
    government        government_t,
    state             faction_state_t,
    name              text   NOT NULL,
    active_states     jsonb,      -- [{"state":..,"trend":..}]
    pending_states    jsonb,
    recovering_states jsonb
);

---------------------------------------------------------------------------
CREATE TABLE bodies (
    id64                 bigint NOT NULL,
    system_id64          bigint NOT NULL,
    update_time          timestamptz,
    distance_to_arrival  real,          -- light seconds (real: 0.5 ls resolution at the 6.4M ls maximum)
    age                  int,           -- million years (stars)
    absolute_magnitude   real,
    solar_masses         real,
    solar_radius         real,
    surface_temperature  real,          -- K
    rotational_period    real,          -- days
    axial_tilt           real,          -- degrees
    gravity              real,          -- g
    earth_masses         real,
    radius               real,          -- km
    surface_pressure     real,          -- atm
    orbital_period       real,          -- days
    semi_major_axis      real,          -- km
    orbital_eccentricity real,
    orbital_inclination  real,
    arg_of_periapsis     real,
    mean_anomaly         real,
    ascending_node       real,
    comp_ice             real,          -- source solidComposition {Ice, Metal, Rock} in %
    comp_metal           real,
    comp_rock            real,
    type                 body_type_t NOT NULL,
    sub_type             body_sub_type_t,
    luminosity           luminosity_t,
    volcanism_type       volcanism_t,
    atmosphere_type      atmosphere_t,
    terraforming_state   terraforming_t,
    reserve_level        reserve_level_t,
    body_id              smallint NOT NULL,   -- id within the system (0..511)
    main_star            boolean,
    tidally_locked       boolean,
    is_landable          boolean,
    name                 text NOT NULL,
    spectral_class       text,
    parents              jsonb,         -- [{"Star":0}, {"Null":3}, ...] as in the journal
    atmosphere_composition jsonb,       -- {"Nitrogen": 91.2, ...}
    genuses              text[]         -- biological genuses detected ($Codex_Ent_..._Name;)
);
COMMENT ON TABLE bodies IS 'Stars, planets and barycentres. id64 = system id64 | body_id << 55 (wrapped, see id64_u()).';

CREATE TABLE body_materials (
    body_id64  bigint NOT NULL,
    percentage real   NOT NULL,
    material   material_t NOT NULL
);
COMMENT ON TABLE body_materials IS 'Raw materials on landable planets, % abundance';

CREATE TABLE body_signals (
    body_id64 bigint   NOT NULL,
    count     int      NOT NULL,
    signal    signal_t NOT NULL
);
COMMENT ON TABLE body_signals IS 'Surface signal counts (biological, geological, guardian, ...)';

CREATE TABLE body_rings (
    body_id64    bigint NOT NULL,
    id64         bigint,                 -- wrapped uint64, NULL for ~70% of rings
    mass         double precision,       -- megatons
    inner_radius double precision,       -- km
    outer_radius double precision,
    type         ring_type_t NOT NULL,
    is_belt      boolean NOT NULL,       -- asteroid belt (star) vs planetary ring
    name         text NOT NULL
);

CREATE TABLE ring_signals (
    body_id64   bigint NOT NULL,
    update_time timestamptz,
    count       int NOT NULL,
    signal      signal_t NOT NULL,       -- mining hotspot: Platinum, Painite, LowTemperatureDiamond...
    ring_name   text NOT NULL
);

---------------------------------------------------------------------------
CREATE TABLE stations (
    id                        bigint NOT NULL,          -- market id, global (carriers keep it across systems)
    system_id64               bigint NOT NULL,
    body_id64                 bigint,                   -- set for surface stations / settlements
    update_time               timestamptz NOT NULL,
    version_ts                timestamptz NOT NULL,     -- max(update_time, market/outfitting/shipyard update times): replace guard for moving stations
    market_update_time        timestamptz,
    shipyard_update_time      timestamptz,
    outfitting_update_time    timestamptz,
    distance_to_arrival       real,
    latitude                  real,
    longitude                 real,
    type                      station_type_t,
    state                     station_state_t,
    allegiance                allegiance_t,
    government                government_t,
    primary_economy           economy_t,
    secondary_economy         economy_t,
    controlling_faction_state faction_state_t,
    run_id                    int,                      -- import run that last wrote the row (drives the market rebuild)
    pads_large                smallint,
    pads_medium               smallint,
    pads_small                smallint,
    has_market                boolean NOT NULL,
    has_shipyard              boolean NOT NULL,
    has_outfitting            boolean NOT NULL,
    name                      text NOT NULL,            -- carriers: the callsign ("K4Z-8QY")
    real_name                 text,
    carrier_name              text,                     -- player-given carrier name
    controlling_faction       text,
    carrier_docking_access    text,
    services                  text[],
    prohibited_commodities    text[],
    economies                 jsonb,                    -- {"Industrial": 60, "Refinery": 40}
    display_name text GENERATED ALWAYS AS (coalesce(carrier_name, real_name, name)) STORED
);
COMMENT ON TABLE stations IS 'Orbital and surface stations, settlements, fleet carriers, mega ships';

CREATE TABLE station_outfitting (
    station_id  bigint NOT NULL,
    update_time timestamptz,
    module_ids  int[] NOT NULL      -- sorted, see modules(id); WHERE module_ids @> ARRAY[128672278]
);
CREATE TABLE station_shipyard (
    station_id  bigint NOT NULL,
    update_time timestamptz,
    ship_ids    int[] NOT NULL      -- see ships(id)
);

-- Market: 31M rows/day are replaced. The table is a partitioned parent with
-- exactly one partition; every import builds a fresh, packed, fully indexed
-- partition and swaps it in (DETACH old / ATTACH new), so the table never
-- carries dead tuples and the parent OID (views, functions) never changes.
CREATE TABLE station_commodities (
    station_id   bigint NOT NULL,
    commodity_id int    NOT NULL,   -- see commodities(id)
    demand       int    NOT NULL,
    supply       int    NOT NULL,
    buy_price    int    NOT NULL,   -- 0 when the station does not sell
    sell_price   int    NOT NULL
) PARTITION BY RANGE (station_id);
CREATE TABLE station_commodities_p0 PARTITION OF station_commodities FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
COMMENT ON TABLE station_commodities IS 'Station market. Rebuilt and swapped as a whole every import (see importer). Prices in credits.';

---------------------------------------------------------------------------
CREATE TABLE commodities (
    id       int  NOT NULL,
    category commodity_category_t,
    name     text NOT NULL,
    symbol   text NOT NULL
);
CREATE TABLE modules (
    id       int  NOT NULL,
    class    smallint,
    rating   module_rating_t,
    category module_category_t,
    name     text NOT NULL,
    symbol   text NOT NULL,
    ship     text                     -- ship-specific modules (e.g. Guardian / ship kit)
);
CREATE TABLE ships (
    id     int  NOT NULL,
    name   text NOT NULL,
    symbol text NOT NULL
);

---------------------------------------------------------------------------
CREATE TABLE import_runs (
    id               serial PRIMARY KEY,
    mode             text NOT NULL,            -- fresh | update
    source           text NOT NULL,            -- path or URL
    built_at         timestamptz,              -- dump build time (Last-Modified / --built-at / file mtime)
    etag             text,
    content_length   bigint,
    bytes_read       bigint,
    started          timestamptz NOT NULL DEFAULT now(),
    finished         timestamptz,
    status           text NOT NULL,            -- running | ok | partial | failed | aborted
    systems_seen     bigint,
    systems_applied  bigint,
    batches          int,
    batches_failed   int,
    rows_by_table    jsonb,
    timings          jsonb,
    importer_version text,
    notes            text
);
