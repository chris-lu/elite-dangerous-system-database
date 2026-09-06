-- Indexes of a freshly built market partition. {T} is replaced by the table
-- name (station_commodities_r<run>). They must match indexes.sql exactly so
-- that ATTACH PARTITION reuses them instead of building new ones.
ALTER TABLE {T} ADD PRIMARY KEY (station_id, commodity_id);
CREATE INDEX {T}_buy_idx ON {T} (commodity_id, buy_price) WHERE supply > 0;
CREATE INDEX {T}_sell_idx ON {T} (commodity_id, sell_price DESC) WHERE demand > 0;
