-- Views and query helpers. Applied after the tables; re-applied (CREATE OR
-- REPLACE) on every `ed_import schema` run.

---------------------------------------------------------------------------
-- Spatial helpers (cube). All distances in light years.
---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ly_point(px real, py real, pz real) RETURNS cube
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
$$ SELECT cube(ARRAY[px, py, pz]::float8[]) $$;

-- Systems within `radius` ly of a point, nearest first.
CREATE OR REPLACE FUNCTION systems_within(px real, py real, pz real, radius real)
RETURNS TABLE (id64 bigint, name text, distance_ly double precision)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT s.id64, s.name, cube_distance(s.coords, ly_point(px, py, pz))
    FROM systems s
    WHERE s.coords <@ cube_enlarge(ly_point(px, py, pz), radius, 3)      -- GiST bounding box
      AND cube_distance(s.coords, ly_point(px, py, pz)) <= radius        -- exact sphere
    ORDER BY 3
$$;

-- Same, around a named system (the system itself is excluded).
CREATE OR REPLACE FUNCTION systems_near(system_name text, radius real)
RETURNS TABLE (id64 bigint, name text, distance_ly double precision)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT w.id64, w.name, w.distance_ly
    FROM systems o, LATERAL systems_within(o.x, o.y, o.z, radius) w
    WHERE o.name = system_name AND w.id64 <> o.id64
$$;

-- The n nearest systems to a point (exact KNN through the GiST index).
CREATE OR REPLACE FUNCTION systems_nearest(px real, py real, pz real, n int DEFAULT 10)
RETURNS TABLE (id64 bigint, name text, distance_ly double precision)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT s.id64, s.name, s.coords <-> ly_point(px, py, pz)
    FROM systems s
    ORDER BY s.coords <-> ly_point(px, py, pz)
    LIMIT n
$$;

-- Distance from point P to the segment AB (plain arithmetic on x/y/z).
CREATE OR REPLACE FUNCTION dist_point_segment(
    px real, py real, pz real, ax real, ay real, az real, bx real, by real, bz real)
RETURNS double precision LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    WITH v AS (SELECT (bx-ax)::float8 vx, (by-ay)::float8 vy, (bz-az)::float8 vz),
         t AS (SELECT CASE WHEN vx*vx+vy*vy+vz*vz = 0 THEN 0
                           ELSE greatest(0, least(1, ((px-ax)*vx + (py-ay)*vy + (pz-az)*vz) / (vx*vx+vy*vy+vz*vz))) END t
               FROM v)
    SELECT sqrt(power(px - (ax + t*vx), 2) + power(py - (ay + t*vy), 2) + power(pz - (az + t*vz), 2))
    FROM v, t
$$;

-- Systems inside a corridor of `width` ly around the straight line between
-- two named systems, ordered along the route. Useful to pick intermediate
-- stops (e.g. inhabited systems along the way: add WHERE population > 0).
CREATE OR REPLACE FUNCTION systems_along_route(from_name text, to_name text, width real)
RETURNS TABLE (id64 bigint, name text, population bigint, progress_ly double precision, off_route_ly double precision)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    WITH a AS (SELECT * FROM systems WHERE name = from_name LIMIT 1),
         b AS (SELECT * FROM systems WHERE name = to_name LIMIT 1),
         len AS (SELECT cube_distance(a.coords, b.coords) l FROM a, b)
    SELECT s.id64, s.name, s.population,
           -- projection of s on AB, in ly from A
           ((s.x-a.x)*(b.x-a.x) + (s.y-a.y)*(b.y-a.y) + (s.z-a.z)*(b.z-a.z)) / nullif(len.l, 0),
           dist_point_segment(s.x, s.y, s.z, a.x, a.y, a.z, b.x, b.y, b.z)
    FROM a, b, len, systems s
    WHERE s.coords <@ cube_enlarge(cube_union(a.coords, b.coords), width, 3)
      AND dist_point_segment(s.x, s.y, s.z, a.x, a.y, a.z, b.x, b.y, b.z) <= width
    ORDER BY 4
$$;

-- Greedy route plotter: from each hop, jump to the reachable system (within
-- jump_range ly) closest to the destination. Not optimal, but fast and good
-- enough to list intermediate systems; NULL rows mean no progress possible.
CREATE OR REPLACE FUNCTION plot_route(from_name text, to_name text, jump_range real, max_hops int DEFAULT 200)
RETURNS TABLE (hop int, id64 bigint, name text, jump_ly double precision, remaining_ly double precision)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    cur systems%ROWTYPE;
    dst systems%ROWTYPE;
    nxt RECORD;
    i int := 0;
BEGIN
    SELECT * INTO cur FROM systems WHERE systems.name = from_name LIMIT 1;
    SELECT * INTO dst FROM systems WHERE systems.name = to_name LIMIT 1;
    IF cur.id64 IS NULL OR dst.id64 IS NULL THEN
        RAISE EXCEPTION 'unknown system';
    END IF;
    hop := 0; id64 := cur.id64; name := cur.name; jump_ly := 0;
    remaining_ly := cube_distance(cur.coords, dst.coords);
    RETURN NEXT;
    WHILE cur.id64 <> dst.id64 AND i < max_hops LOOP
        i := i + 1;
        SELECT s.*, cube_distance(s.coords, cur.coords) AS d INTO nxt
        FROM systems s
        WHERE s.coords <@ cube_enlarge(cur.coords, jump_range, 3)
          AND cube_distance(s.coords, cur.coords) <= jump_range
          AND s.id64 <> cur.id64
        ORDER BY cube_distance(s.coords, dst.coords)
        LIMIT 1;
        IF nxt.id64 IS NULL OR cube_distance(nxt.coords, dst.coords) >= cube_distance(cur.coords, dst.coords) THEN
            RETURN;  -- stuck: no reachable system gets closer
        END IF;
        SELECT * INTO cur FROM systems WHERE systems.id64 = nxt.id64;
        hop := i; id64 := cur.id64; name := cur.name; jump_ly := nxt.d;
        remaining_ly := cube_distance(cur.coords, dst.coords);
        RETURN NEXT;
    END LOOP;
END $$;

---------------------------------------------------------------------------
-- Views
---------------------------------------------------------------------------
-- Market with names; the price columns are in credits.
CREATE OR REPLACE VIEW v_station_market AS
SELECT c.station_id, st.display_name AS station, st.system_id64, sy.name AS system,
       c.commodity_id, co.name AS commodity, co.category,
       c.demand, c.supply, c.buy_price, c.sell_price, st.market_update_time
FROM station_commodities c
JOIN commodities co ON co.id = c.commodity_id
JOIN stations st ON st.id = c.station_id
JOIN systems sy ON sy.id64 = st.system_id64;

-- One row per (station, module) for relational use of the int[] column.
CREATE OR REPLACE VIEW v_station_modules AS
SELECT o.station_id, m.id AS module_id, m.name, m.class, m.rating, m.category, m.ship, o.update_time
FROM station_outfitting o
CROSS JOIN LATERAL unnest(o.module_ids) AS u(module_id)
JOIN modules m ON m.id = u.module_id;

CREATE OR REPLACE VIEW v_station_ships AS
SELECT y.station_id, s.id AS ship_id, s.name, s.symbol, y.update_time
FROM station_shipyard y
CROSS JOIN LATERAL unnest(y.ship_ids) AS u(ship_id)
JOIN ships s ON s.id = u.ship_id;

-- Systems with their station counts (fleet carriers excluded).
CREATE OR REPLACE VIEW v_system_stations AS
SELECT s.id64, s.name, s.population, s.allegiance, s.government, s.primary_economy,
       count(st.id) AS stations,
       count(st.id) FILTER (WHERE st.services @> ARRAY['Shipyard']) AS shipyards,
       count(st.id) FILTER (WHERE st.pads_large > 0) AS large_pads
FROM systems s
LEFT JOIN stations st ON st.system_id64 = s.id64 AND st.type IS DISTINCT FROM 'Drake-Class Carrier'
GROUP BY s.id64, s.name, s.population, s.allegiance, s.government, s.primary_economy;

-- Table and index sizes.
CREATE OR REPLACE VIEW v_table_sizes AS
SELECT c.relname AS table_name,
       pg_size_pretty(pg_table_size(c.oid)) AS table_size,
       pg_size_pretty(pg_indexes_size(c.oid)) AS indexes_size,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_total_relation_size(c.oid) AS total_bytes,
       c.reltuples::bigint AS est_rows
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
ORDER BY pg_total_relation_size(c.oid) DESC;
