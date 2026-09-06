-- Primary keys and indexes. Applied by the importer AFTER the bulk load
-- (fresh mode) and present from the start in update mode. One statement per
-- line; the importer runs the btree builds on up to 4 connections in parallel
-- (each with maintenance_work_mem = 2GB, 4 parallel workers) and the GIN /
-- GiST builds, which PG17 cannot parallelise, serially alongside them.

-- systems
ALTER TABLE systems ADD PRIMARY KEY (id64);
CREATE INDEX systems_coords_gist ON systems USING gist (coords);
CREATE INDEX systems_name_idx ON systems (name);
CREATE INDEX systems_lower_name_idx ON systems (lower(name) text_pattern_ops);
CREATE INDEX systems_name_trgm ON systems USING gin (name gin_trgm_ops) WHERE is_named;
CREATE INDEX systems_population_idx ON systems (population DESC) WHERE population > 0;
CREATE INDEX systems_controlling_faction_idx ON systems (controlling_faction) WHERE controlling_faction IS NOT NULL;
CREATE INDEX systems_controlling_power_idx ON systems (controlling_power) WHERE controlling_power IS NOT NULL;

-- system_factions
ALTER TABLE system_factions ADD PRIMARY KEY (system_id64, name);
CREATE INDEX system_factions_name_idx ON system_factions (name, influence DESC);
CREATE INDEX system_factions_lower_name_idx ON system_factions (lower(name) text_pattern_ops);
CREATE INDEX system_factions_name_trgm ON system_factions USING gin (name gin_trgm_ops);

-- bodies
ALTER TABLE bodies ADD PRIMARY KEY (id64);
CREATE INDEX bodies_system_idx ON bodies (system_id64);
ALTER TABLE body_materials ADD PRIMARY KEY (body_id64, material);
CREATE INDEX body_materials_material_idx ON body_materials (material, percentage DESC);
ALTER TABLE body_signals ADD PRIMARY KEY (body_id64, signal);
CREATE INDEX body_signals_signal_idx ON body_signals (signal, count DESC);
ALTER TABLE body_rings ADD PRIMARY KEY (body_id64, name);
ALTER TABLE ring_signals ADD PRIMARY KEY (body_id64, ring_name, signal);
CREATE INDEX ring_signals_signal_idx ON ring_signals (signal, count DESC);

-- stations
ALTER TABLE stations ADD PRIMARY KEY (id);
CREATE INDEX stations_system_idx ON stations (system_id64);
CREATE INDEX stations_body_idx ON stations (body_id64) WHERE body_id64 IS NOT NULL;
CREATE INDEX stations_type_idx ON stations (type);
CREATE INDEX stations_services_gin ON stations USING gin (services);
CREATE INDEX stations_display_name_trgm ON stations USING gin (display_name gin_trgm_ops);
CREATE INDEX stations_lower_display_name_idx ON stations (lower(display_name) text_pattern_ops);
CREATE INDEX stations_controlling_faction_idx ON stations (controlling_faction) WHERE controlling_faction IS NOT NULL;
ALTER TABLE station_outfitting ADD PRIMARY KEY (station_id);
CREATE INDEX station_outfitting_modules_gin ON station_outfitting USING gin (module_ids);
ALTER TABLE station_shipyard ADD PRIMARY KEY (station_id);
CREATE INDEX station_shipyard_ships_gin ON station_shipyard USING gin (ship_ids);

-- station_commodities: indexes live on the partitioned parent; every new
-- partition built by the importer gets identical indexes before it is
-- attached (market_indexes.sql), so ATTACH is instantaneous.
ALTER TABLE station_commodities ADD PRIMARY KEY (station_id, commodity_id);
CREATE INDEX station_commodities_buy_idx ON station_commodities (commodity_id, buy_price) WHERE supply > 0;
CREATE INDEX station_commodities_sell_idx ON station_commodities (commodity_id, sell_price DESC) WHERE demand > 0;

-- lookups
ALTER TABLE commodities ADD PRIMARY KEY (id);
ALTER TABLE modules ADD PRIMARY KEY (id);
ALTER TABLE ships ADD PRIMARY KEY (id);
