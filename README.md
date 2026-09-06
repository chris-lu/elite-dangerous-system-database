# Elite Dangerous galaxy database (Spansh dump → PostgreSQL 17)

A PostgreSQL 17 database of the Elite Dangerous galaxy built from the
[Spansh dumps](https://spansh.co.uk/dumps), and a fast Python importer that
loads a dump from scratch or merges the daily / weekly / monthly incremental
dumps into it.

* **Import speed** (this machine, Ryzen 9 7950X / 64 GB, Docker Desktop):
  the 1-day dump — 1.26 GB gzip, 8.16 GB of JSON, 117 k systems, 717 k
  bodies, 272 k stations, **31.3 M market rows** — loads from scratch in
  about a minute including all indexes (see [Benchmarks](#benchmarks)).
* **Incremental updates**: `ed_import sync` looks at what was imported last,
  picks the smallest Spansh dump that covers the gap and streams it straight
  from `downloads.spansh.co.uk` with `smart_open`. Re-importing a file, or
  importing files out of order, never regresses data.
* **Query-oriented schema**: 3-D proximity with the `cube` GiST index,
  trigram / prefix name search, normalized market and materials tables with
  price-ordered indexes, `int[]` outfitting with GIN, native enums.

---

## 1. Setup

```bash
cp .env.example .env            # database name / user / password / port
docker compose up -d postgres   # PostgreSQL 17 on 127.0.0.1:5432
docker compose build importer   # the importer image (python:3.12-slim)
```

The Postgres service is tuned for bulk loading on a 64 GB host
(`shared_buffers=8GB`, `wal_level=minimal`, `synchronous_commit=off`,
parallel index builds…) — see the comments in [docker-compose.yml](docker-compose.yml).
The database is initialised with the PG17 builtin `C.UTF-8` collation:
code-point ordering (fast sorts, `LIKE 'prefix%'` on plain btrees) with
Unicode-aware `lower()` / `ILIKE`.

Docker Desktop (WSL2) sees half of the host RAM by default. Give it more with
`%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=48GB
swap=0
```

> **Disk.** The 1-day dump makes a ~4 GB database. The full `galaxy.json.gz`
> (117 GB compressed) needs several hundred GB; move Docker Desktop's disk
> image (Settings → Resources → Advanced → *Disk image location*) to a drive
> with space **before** importing it, and download the dump to that drive too.

## 2. Importing

Dumps: `https://downloads.spansh.co.uk/{galaxy,galaxy_1day,galaxy_7days,galaxy_1month,galaxy_populated,galaxy_stations}.json.gz`
(rebuilt daily around 05:30 UTC). Put local copies in `./data` (mounted read-only at `/data` in the importer).

```bash
# From scratch (drops everything), from a local file or a URL:
docker compose run --rm importer fresh /data/galaxy_1day.json.gz
docker compose run --rm importer fresh https://downloads.spansh.co.uk/galaxy_populated.json.gz

# Merge a dump into the existing database (replace-by-system):
docker compose run --rm importer update /data/galaxy_1day.json.gz

# Keep up to date: picks 1day / 7days / 1month / populated depending on the
# gap since the last successful run, streams it from spansh, merges it.
docker compose run --rm importer sync
docker compose run --rm importer sync --dry-run     # only say what it would do

docker compose run --rm importer stats              # row counts, sizes, last runs
docker compose run --rm importer --help
```

The importer can also run on the host (`pip install -r requirements.txt`,
`python -m ed_import ... --dsn postgresql://elite_user:elite_password@127.0.0.1:5432/elite_dangerous`
or `PG_DSN` in `.env`), but the container is faster: COPY through Docker
Desktop's localhost port-forward is ~2.7× slower than inside the compose network.

Options: `--workers N` (parser/loader processes, default 8), `--batch-mb`
(JSON per batch, default 32), `--unlogged` (leave tables UNLOGGED — a bit
faster, but a crash empties them), `--built-at '2026-09-06 05:34:22+00'`
(build time of a local file when its mtime is not the download time).

Schedule `sync` twice a day (e.g. 06:30 and 12:30 UTC — Spansh's build finishes
~05:45 UTC; the second run is a no-op when nothing new was published):

```
30 6,12 * * *  cd /path/to/system-search-v2 && docker compose run --rm importer sync >> sync.log 2>&1
```

### How the importer works

* The dump is a JSON array pretty-printed **one system per line**, so it is
  read line by line (no streaming JSON parser). A reader process streams it
  through `smart_open` (local file or URL), inflates with ISA-L (`isal`,
  2-3× faster than zlib; the standard library is the fallback), cuts it into
  ~32 MB batches on line boundaries and hands them to worker processes over
  a queue.
* Each worker parses with `orjson`, flattens a system into COPY text rows for
  the 11 tables (hand-escaped, one string format per row, per-batch
  de-duplication of stations / commodities / factions / rings), and writes
  them with `COPY ... FROM STDIN` on its own connection.
* **fresh**: tables are created UNLOGGED without keys, workers COPY straight
  into them; at the end the tables are switched to LOGGED (a WAL-free rewrite
  under `wal_level=minimal`), the keys and indexes are built on 4 parallel
  connections (GIN/GiST builds, which PG17 cannot parallelise, run alongside),
  everything is `VACUUM (FREEZE, ANALYZE)`d.
* **update**: one transaction per batch: COPY into per-session temp tables,
  lock the affected systems and stations in a global order, then
  `INSERT ... ON CONFLICT DO UPDATE` systems (guard: the dump's build time
  must be ≥ the stored one), delete the old children of the replaced systems
  (grandchildren through `RETURNING`), insert the new ones, upsert stations
  (guard: newest sighting wins, so a fleet carrier that moved keeps its newer
  row), replace their outfitting / shipyard.
* The 31 M-row **market table is never updated in place**: every run COPYs
  the incoming market rows into a fresh unlogged table, carries over the
  rows of stations the dump did not touch, builds its 3 indexes, vacuums it,
  and swaps it in as the single partition of `station_commodities` (the
  parent OID, and therefore views and functions, never change). No dead
  tuples, no daily vacuum debt, no index bloat.
* Unknown enum labels (new game content) are added with `ALTER TYPE ... ADD
  VALUE` on a side connection before the batch is written; new commodities /
  modules / ships go to the lookup tables the same way.
* Failures: a batch that fails after retries (deadlock, connection loss) is
  written to `dumps/failed/run<id>-batch<n>.jsonl.gz` with the Postgres error
  context, the run ends `partial`, and the file can simply be re-imported. The
  final status is written with a synchronous commit; `import_runs` keeps the
  history (`stats` shows it).

## 3. Schema

| table | rows / day | keyed by | notes |
|---|---|---|---|
| `systems` | 117 k | `id64` | `coords cube` (generated) + GiST, `x y z real`, powers, thargoid war, `is_named` (not procedural) |
| `system_factions` | 91 k | `(system_id64, name)` | influence, states as jsonb |
| `bodies` | 717 k | `id64` | 40 physical columns, `parents` / `atmosphere_composition` jsonb, `genuses text[]` |
| `body_materials` | 3.6 M | `(body_id64, material)` | index `(material, percentage DESC)` |
| `body_signals` | 301 k | `(body_id64, signal)` | biological / geological / … counts |
| `body_rings` | 104 k | `(body_id64, name)` | rings and belts (`is_belt`) |
| `ring_signals` | 76 k | `(body_id64, ring_name, signal)` | mining hotspots |
| `stations` | 272 k | `id` (global market id) | `display_name` (carrier name / real name / callsign), `services text[]` + GIN, pads, economies |
| `station_commodities` | **31.3 M** | `(station_id, commodity_id)` | partitioned parent, swapped every import; partial indexes `(commodity_id, buy_price) WHERE supply > 0`, `(commodity_id, sell_price DESC) WHERE demand > 0` |
| `station_outfitting` | 49 k | `station_id` | `module_ids int[]` + GIN (16 M module entries/day as arrays) |
| `station_shipyard` | 34 k | `station_id` | `ship_ids int[]` + GIN |
| `commodities`, `modules`, `ships` | 411 / 1.2 k / 48 | `id` | lookups (Frontier ids) |
| `import_runs` | | | one row per import: source, build time, status, row counts, timings |

Views: `v_station_market` (market with names), `v_station_modules`,
`v_station_ships`, `v_system_stations`, `v_table_sizes`.
Functions: `systems_within(x,y,z,r)`, `systems_near(name, r)`,
`systems_nearest(x,y,z,n)`, `systems_along_route(from, to, width)`,
`plot_route(from, to, jump_range)`, `dist_point_segment(...)`,
`id64_u()` / `id64_s()`.

Conventions worth knowing:

* `id64` values are uint64 in the source; they are stored two's-complement
  wrapped in `bigint` (body ids can set bit 63). `id64_u(id64)` shows the
  unsigned value, `WHERE id64 = id64_s(9300000000000000000)` looks one up
  (still uses the primary key).
* Low-cardinality strings are native enums: compare to literals directly
  (`allegiance = 'Federation'`), cast to text for `ILIKE` or concatenation
  (`sub_type::text ILIKE '%gas giant%'`). Labels come from
  [galaxy.schema.json](data/galaxy.schema.json) (`tools/gen_enums.py`).
* `systems.built_at` is the build time of the dump the row came from and
  `version_ts` the newest timestamp anywhere inside the record — use it for
  "recently updated" queries. Fleet carriers keep their station id across
  systems; `stations.version_ts` is the latest sighting.
* Prices are in credits; `demand` is clamped to 2 147 483 647 by the game.

## 4. Queries

### Text search

```sql
-- prefix, case-insensitive (write it exactly like this: the index is on lower(name))
SELECT id64, name FROM systems WHERE lower(name) LIKE lower('hip 5') || '%' ORDER BY name LIMIT 50;

-- prefix, case-sensitive (plain btree, C.UTF-8 collation)
SELECT id64, name FROM systems WHERE name LIKE 'HIP 5%' ORDER BY name LIMIT 50;

-- contains / fuzzy on human-named systems (trigram GIN; procedural names are excluded on purpose)
SELECT name FROM systems WHERE name ILIKE '%olonia%' AND is_named;
SELECT name, similarity(name, 'Hershel 36') AS sim FROM systems
 WHERE is_named AND name % 'Hershel 36' ORDER BY name <-> 'Hershel 36' LIMIT 10;

-- station name search (finds fleet carriers by their player-given name too)
SELECT display_name, type, s.name AS system FROM stations st JOIN systems s ON s.id64 = st.system_id64
 WHERE st.display_name ILIKE '%jameson%';

-- faction name search
SELECT name, count(*) AS systems, round(avg(influence)::numeric, 3) AS avg_influence
  FROM system_factions WHERE lower(name) LIKE 'dukes%' GROUP BY name ORDER BY 2 DESC;
```

### Proximity

```sql
-- systems within 50 ly of Sol (0,0,0), nearest first
SELECT * FROM systems_within(0, 0, 0, 50);
SELECT * FROM systems_near('Sol', 50);

-- the same query written out (GiST bounding box, then exact distance)
SELECT id64, name, cube_distance(coords, ly_point(0, 0, 0)) AS ly FROM systems
 WHERE coords <@ cube_enlarge(ly_point(0, 0, 0), 50, 3)
   AND cube_distance(coords, ly_point(0, 0, 0)) <= 50 ORDER BY ly;

-- 10 nearest systems to a point (exact KNN through the GiST index)
SELECT * FROM systems_nearest(-92.2, 4474.6, -1.5, 10);
```

### Inhabited systems with stations

```sql
-- inhabited systems with the most stations (fleet carriers excluded)
SELECT s.name, s.population, count(*) AS stations
  FROM systems s JOIN stations st ON st.system_id64 = s.id64
 WHERE s.population > 0 AND st.type <> 'Drake-Class Carrier'
 GROUP BY s.id64, s.name, s.population ORDER BY stations DESC LIMIT 25;

SELECT * FROM v_system_stations WHERE population > 0 ORDER BY stations DESC LIMIT 25;
```

### Stations with a service within N ly

```sql
-- stations with a shipyard within 100 ly of Sol, closest first
WITH near AS (SELECT * FROM systems_within(0, 0, 0, 100))
SELECT n.name AS system, round(n.distance_ly::numeric, 1) AS ly,
       st.display_name AS station, st.type, st.distance_to_arrival AS ls, st.pads_large
  FROM near n JOIN stations st ON st.system_id64 = n.id64
 WHERE st.services @> ARRAY['Shipyard']
 ORDER BY n.distance_ly, st.distance_to_arrival;

-- rare service anywhere in the galaxy (GIN on services)
SELECT s.name, st.display_name FROM stations st JOIN systems s ON s.id64 = st.system_id64
 WHERE st.services @> ARRAY['Material Trader'] LIMIT 50;

-- stations selling a given module (GIN on module_ids)
SELECT s.name, st.display_name FROM station_outfitting o
  JOIN stations st ON st.id = o.station_id JOIN systems s ON s.id64 = st.system_id64
 WHERE o.module_ids @> ARRAY[(SELECT id FROM modules WHERE name = 'Frame Shift Drive' AND class = 5 AND rating = 'A' LIMIT 1)];
```

### Materials

```sql
-- bodies richest in a rare material
SELECT s.name AS system, b.name AS body, m.percentage
  FROM body_materials m JOIN bodies b ON b.id64 = m.body_id64 JOIN systems s ON s.id64 = b.system_id64
 WHERE m.material = 'Polonium' ORDER BY m.percentage DESC LIMIT 50;

-- landable bodies with two materials, near a point
WITH near AS (SELECT * FROM systems_within(0, 0, 0, 150))
SELECT n.name, b.name, m1.percentage AS polonium, m2.percentage AS yttrium
  FROM near n JOIN bodies b ON b.system_id64 = n.id64
  JOIN body_materials m1 ON m1.body_id64 = b.id64 AND m1.material = 'Polonium'
  JOIN body_materials m2 ON m2.body_id64 = b.id64 AND m2.material = 'Yttrium'
 ORDER BY m1.percentage DESC LIMIT 20;

-- ring mining hotspots
SELECT s.name, r.ring_name, r.count FROM ring_signals r
  JOIN bodies b ON b.id64 = r.body_id64 JOIN systems s ON s.id64 = b.system_id64
 WHERE r.signal = 'Platinum' ORDER BY r.count DESC LIMIT 20;
```

### Markets

```sql
-- best places to sell Gold (galaxy-wide, ordered partial index: sub-millisecond)
-- give the commodity id as a constant / scalar subquery so the planner can walk
-- the (commodity_id, sell_price DESC) index and stop after N rows
SELECT s.name AS system, st.display_name AS station, st.type, c.sell_price, c.demand
  FROM station_commodities c JOIN stations st ON st.id = c.station_id JOIN systems s ON s.id64 = st.system_id64
 WHERE c.commodity_id = (SELECT id FROM commodities WHERE name = 'Gold')
   AND c.demand > 0 AND st.type <> 'Drake-Class Carrier'
 ORDER BY c.sell_price DESC LIMIT 20;

-- cheapest Tritium within 100 ly of a system
WITH near AS (SELECT * FROM systems_near('Sol', 100))
SELECT n.name, st.display_name, c.buy_price, c.supply, round(n.distance_ly::numeric, 1) AS ly
  FROM near n JOIN stations st ON st.system_id64 = n.id64
  JOIN station_commodities c ON c.station_id = st.id
 WHERE c.commodity_id = (SELECT id FROM commodities WHERE name = 'Tritium') AND c.supply > 0
 ORDER BY c.buy_price LIMIT 20;

-- a station's market with names
SELECT * FROM v_station_market WHERE station = 'Jameson Memorial' ORDER BY category, commodity;
```

### Factions and powers

```sql
-- where a faction is present, by influence
SELECT s.name, f.influence, f.state, s.controlling_faction = f.name AS controls
  FROM system_factions f JOIN systems s ON s.id64 = f.system_id64
 WHERE f.name = 'Mother Gaia' ORDER BY f.influence DESC;

-- factions of one system
SELECT name, influence, state, allegiance, government FROM system_factions
 WHERE system_id64 = (SELECT id64 FROM systems WHERE name = 'Sol') ORDER BY influence DESC;

-- powerplay: systems per controlling power
SELECT controlling_power, power_state, count(*) FROM systems
 WHERE controlling_power IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2;
```

### Galaxy statistics

```sql
SELECT count(*) AS systems, count(*) FILTER (WHERE population > 0) AS inhabited,
       sum(population) AS population,
       (SELECT count(*) FROM bodies) AS bodies, (SELECT count(*) FROM stations) AS stations,
       (SELECT count(*) FROM station_commodities) AS market_rows
  FROM systems;

SELECT allegiance, count(*) AS systems, sum(population) AS population
  FROM systems WHERE allegiance IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;

-- last import runs (counts per table are in import_runs.rows_by_table)
SELECT id, mode, status, built_at, systems_seen, timings FROM import_runs ORDER BY id DESC LIMIT 5;
```

### Navigation

```sql
-- systems in a 20 ly wide corridor between two systems, in route order (e.g. inhabited stops)
SELECT * FROM systems_along_route('Sol', 'Achenar', 20) WHERE population > 0;

-- greedy route with a 60 ly jump range (intermediate systems, hop by hop)
SELECT * FROM plot_route('Sol', 'Colonia', 60);
```

### Density

```sql
-- densest 100 ly cubes (grid on the real x/y/z columns)
SELECT floor(x/100)::int AS gx, floor(y/100)::int AS gy, floor(z/100)::int AS gz, count(*) AS n
  FROM systems GROUP BY 1, 2, 3 ORDER BY n DESC LIMIT 20;

-- inhabited systems with the most neighbours within 10 ly
SELECT s.name, (SELECT count(*) - 1 FROM systems t
                 WHERE t.coords <@ cube_enlarge(s.coords, 10, 3) AND cube_distance(t.coords, s.coords) <= 10) AS neighbours
  FROM systems s WHERE s.population > 0 ORDER BY neighbours DESC LIMIT 20;
```

### Monitoring

```sql
SELECT * FROM v_table_sizes;

-- hottest queries (pg_stat_statements is preloaded)
SELECT calls, round(mean_exec_time::numeric, 2) AS ms, left(query, 100) AS query
  FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 20;

-- dead tuples / last vacuum per table
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, last_vacuum FROM pg_stat_user_tables ORDER BY n_dead_tup DESC;
```

## 5. Maintenance

* Every run ends with `VACUUM (ANALYZE)` of the in-place tables (`fresh`:
  `VACUUM (FREEZE, ANALYZE)`); the market partition is vacuumed once when it
  is built and never modified afterwards.
* Autovacuum is configured aggressively (`autovacuum_vacuum_cost_delay=0`,
  6 workers). Transaction-id wraparound is a non-issue: one transaction per
  batch, a few hundred per import.
* GiST / GIN indexes do not shrink under churn; after weeks of daily updates
  check them with `pgstattuple` (`CREATE EXTENSION pgstattuple;
  SELECT * FROM pgstatindex('systems_coords_gist')`) and rebuild the ones
  with `avg_leaf_density < 60`:
  `REINDEX INDEX CONCURRENTLY systems_coords_gist;` (not during an import).
* `wal_level=minimal` means no streaming replication / `pg_basebackup`; back
  up with `pg_dump` or by re-importing the dumps.

## 6. Benchmarks

Measured on a Ryzen 9 7950X (16 cores), 64 GB, Docker Desktop / WSL2,
`galaxy_1day.json.gz` of 2026-09-05 (1.26 GB gzip → 8.16 GB JSON, 117,158
systems, 53.2 M rows in total):

| run | workers | load (inflate + parse + COPY) | finalize (SET LOGGED, indexes, vacuum) | total |
|---|---|---|---|---|
| `fresh`, container, `isal` | 8 | 23 s | 16 s | **39 s** |
| `fresh`, container, `isal` | 12 | 22 s | 19 s | 41 s |
| `fresh`, container, zlib (before `isal`) | 6–12 | 30 s | 15 s | 45 s |
| `fresh`, host (Windows, port-forward), zlib | 12 | 33 s | 19 s | 52 s |
| `update` of the same file (every system replaced) | 8 | 33 s | 20 s | 53 s |
| `update` of an older build (every system skipped) | 8 | 30 s | 36 s | 66 s |
| `sync`: next day's `galaxy_1day` streamed from spansh (1.31 GB, 143 k systems) | 8 | 100 s (download-bound, ~13 MB/s) | 27 s | 127 s |

Resulting database: 3.8 GB (market 2.8 GB of which 1.0 GB indexes).
`--unlogged` removes the SET LOGGED step (6–9 s).

Where the time goes: inflating the gzip is a single thread on the critical
path (zlib ≈ 22 s for the 8.16 GB inside the WSL2 VM, ISA-L ≈ 9 s; a
multi-threaded inflater — `rapidgzip` — was measured at the same 8 s and
cannot read non-seekable streams, so it is not used); parsing + flattening
is ~70 s of CPU spread over the workers; Postgres spends ~17 s parsing the
COPY text; moving 8 GB of batches through the queue costs ~4 s. The worker
count barely matters above 6 because the reader is the bottleneck.

## 7. Tests

```bash
pip install pytest
PG_DSN=postgresql://elite_user:elite_password@127.0.0.1:5432/elite_dangerous python -m pytest -q tests
```

Covers COPY escaping round trips (names with backslashes, tabs, `\N`, NBSP,
quotes in arrays), id64 wrapping, per-batch de-duplication, the dump
selection rule of `sync`, and a real system record through the whole
flatten → COPY path.

## 8. Layout

```
docker-compose.yml       postgres:17 (tuned) + importer service
Dockerfile.importer      python:3.12-slim + requirements.txt
sql/enums.sql            enum types generated from data/galaxy.schema.json
sql/schema.sql           tables (alignment-ordered columns), id64 helpers
sql/indexes.sql          primary keys and indexes (built after the bulk load)
sql/market_indexes.sql   indexes of a market partition
sql/views.sql            views and spatial / routing functions
ed_import/reader.py      smart_open + (isal|gzip) streaming, byte-budget batches
ed_import/flatten.py     JSON -> COPY rows, de-duplication, enum tracking
ed_import/db.py          schema, per-batch apply (fresh / update), finalisation
ed_import/pipeline.py    worker processes, progress, dead-lettering
ed_import/sync.py        dump selection, HEAD / Range download
ed_import/cli.py         fresh / update / sync / schema / stats
tools/gen_enums.py       regenerate sql/enums.sql from a new galaxy.schema.json
tests/                   pytest suite
```
