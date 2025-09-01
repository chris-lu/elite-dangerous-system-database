-- ==================================================
-- Elite Dangerous Database Initialization Script
-- ==================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Create custom functions for 3D distance calculations
CREATE OR REPLACE FUNCTION distance_3d(x1 FLOAT, y1 FLOAT, z1 FLOAT, x2 FLOAT, y2 FLOAT, z2 FLOAT)
RETURNS FLOAT AS $$
BEGIN
    RETURN sqrt(power(x1 - x2, 2) + power(y1 - y2, 2) + power(z1 - z2, 2));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Main systems table
CREATE TABLE systems (
    id BIGINT PRIMARY KEY,
    id64 BIGINT UNIQUE,
    name VARCHAR(255) NOT NULL,
    
    -- 3D Coordinates
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    z DOUBLE PRECISION NOT NULL,
    coords POINT GENERATED ALWAYS AS (point(x, y)) STORED,
    
    -- Political and economic data
    allegiance VARCHAR(50),
    government VARCHAR(50),
    state VARCHAR(50),
    economy VARCHAR(50),
    security VARCHAR(50),
    population BIGINT DEFAULT 0,
    
    -- Controlling faction
    controlling_faction_id INTEGER,
    controlling_faction_name VARCHAR(255),
    controlling_faction_allegiance VARCHAR(50),
    controlling_faction_government VARCHAR(50),
    controlling_faction_is_player BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    date_discovered TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw JSON data for complex nested structures
    raw_data JSONB
);

-- Factions table
CREATE TABLE factions (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    allegiance VARCHAR(50),
    government VARCHAR(50),
    is_player BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- System factions relationship (many-to-many)
CREATE TABLE system_factions (
    system_id BIGINT REFERENCES systems(id) ON DELETE CASCADE,
    faction_id INTEGER REFERENCES factions(id) ON DELETE CASCADE,
    influence DOUBLE PRECISION CHECK (influence >= 0 AND influence <= 1),
    state VARCHAR(50),
    happiness VARCHAR(50),
    last_update BIGINT,
    active_states JSONB DEFAULT '[]',
    recovering_states JSONB DEFAULT '[]',
    pending_states JSONB DEFAULT '[]',
    PRIMARY KEY (system_id, faction_id)
);

-- Stations table
CREATE TABLE stations (
    id INTEGER PRIMARY KEY,
    system_id BIGINT REFERENCES systems(id) ON DELETE CASCADE,
    market_id BIGINT UNIQUE,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(100),
    
    -- Location data
    distance_to_arrival DOUBLE PRECISION,
    body_id INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    
    -- Political data
    allegiance VARCHAR(50),
    government VARCHAR(50),
    economy VARCHAR(50),
    second_economy VARCHAR(50),
    
    -- Services
    have_market BOOLEAN DEFAULT FALSE,
    have_shipyard BOOLEAN DEFAULT FALSE,
    have_outfitting BOOLEAN DEFAULT FALSE,
    other_services JSONB DEFAULT '[]',
    
    -- Controlling faction
    controlling_faction_id INTEGER REFERENCES factions(id),
    
    -- Update times
    information_update TIMESTAMP,
    market_update TIMESTAMP,
    shipyard_update TIMESTAMP,
    outfitting_update TIMESTAMP,
    
    -- Raw data
    raw_data JSONB
);

-- Bodies table (planets, stars, etc.)
CREATE TABLE bodies (
    id INTEGER PRIMARY KEY,
    id64 BIGINT UNIQUE,
    system_id BIGINT REFERENCES systems(id) ON DELETE CASCADE,
    body_id INTEGER,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50),
    sub_type VARCHAR(100),
    
    -- Physical properties
    distance_to_arrival DOUBLE PRECISION,
    is_main_star BOOLEAN DEFAULT FALSE,
    is_landable BOOLEAN DEFAULT FALSE,
    is_scoopable BOOLEAN DEFAULT FALSE,
    
    -- Orbital mechanics
    orbital_period DOUBLE PRECISION,
    semi_major_axis DOUBLE PRECISION,
    orbital_eccentricity DOUBLE PRECISION,
    orbital_inclination DOUBLE PRECISION,
    arg_of_periapsis DOUBLE PRECISION,
    rotational_period DOUBLE PRECISION,
    rotational_period_tidally_locked BOOLEAN DEFAULT FALSE,
    axial_tilt DOUBLE PRECISION,
    
    -- Physical characteristics
    age INTEGER,
    solar_masses DOUBLE PRECISION,
    solar_radius DOUBLE PRECISION,
    earth_masses DOUBLE PRECISION,
    radius DOUBLE PRECISION,
    gravity DOUBLE PRECISION,
    surface_temperature DOUBLE PRECISION,
    surface_pressure DOUBLE PRECISION,
    
    -- Stellar data
    spectral_class VARCHAR(10),
    luminosity VARCHAR(10),
    absolute_magnitude DOUBLE PRECISION,
    
    -- Atmospheric data
    volcanism_type VARCHAR(100),
    atmosphere_type VARCHAR(100),
    atmosphere_composition JSONB,
    terraforming_state VARCHAR(50),
    
    -- Composition
    solid_composition JSONB,
    materials JSONB,
    
    -- Other data
    reserve_level VARCHAR(50),
    rings JSONB DEFAULT '[]',
    belts JSONB DEFAULT '[]',
    parents JSONB DEFAULT '[]',
    
    -- Metadata
    last_updated TIMESTAMP,
    raw_data JSONB
);

-- Materials table for body materials
CREATE TABLE body_materials (
    body_id INTEGER REFERENCES bodies(id) ON DELETE CASCADE,
    material_name VARCHAR(50),
    percentage DOUBLE PRECISION,
    PRIMARY KEY (body_id, material_name)
);

-- Inhabited systems view
CREATE VIEW inhabited_systems AS
SELECT s.*, 
       COUNT(st.id) as station_count,
       MAX(st.distance_to_arrival) as max_station_distance,
       MIN(st.distance_to_arrival) as min_station_distance
FROM systems s
LEFT JOIN stations st ON s.id = st.system_id
WHERE s.population > 0
GROUP BY s.id;

-- Performance indexes
CREATE INDEX idx_systems_coords ON systems USING gist(x, y, z);
CREATE INDEX idx_systems_name ON systems USING gin(to_tsvector('english', name));
CREATE INDEX idx_systems_allegiance ON systems(allegiance);
CREATE INDEX idx_systems_population ON systems(population DESC) WHERE population > 0;
CREATE INDEX idx_systems_economy ON systems(economy);
CREATE INDEX idx_systems_security ON systems(security);

-- Spatial index for 3D coordinates
CREATE INDEX idx_systems_3d_coords ON systems(x, y, z);

-- Stations indexes
CREATE INDEX idx_stations_system_id ON stations(system_id);
CREATE INDEX idx_stations_name ON stations USING gin(to_tsvector('english', name));
CREATE INDEX idx_stations_services ON stations USING gin(other_services);
CREATE INDEX idx_stations_market ON stations(have_market) WHERE have_market = true;
CREATE INDEX idx_stations_shipyard ON stations(have_shipyard) WHERE have_shipyard = true;

-- Bodies indexes
CREATE INDEX idx_bodies_system_id ON bodies(system_id);
CREATE INDEX idx_bodies_type ON bodies(type, sub_type);
CREATE INDEX idx_bodies_landable ON bodies(is_landable) WHERE is_landable = true;
CREATE INDEX idx_bodies_materials ON body_materials(material_name, percentage DESC);

-- Factions indexes
CREATE INDEX idx_factions_name ON factions USING gin(to_tsvector('english', name));
CREATE INDEX idx_system_factions_influence ON system_factions(influence DESC);

-- JSON indexes for complex queries
CREATE INDEX idx_systems_raw_data ON systems USING gin(raw_data);
CREATE INDEX idx_stations_raw_data ON stations USING gin(raw_data);
CREATE INDEX idx_bodies_raw_data ON bodies USING gin(raw_data);

-- Query specific indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_systems_coords_populated ON systems(x, y, z) WHERE population > 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stations_services_combo ON stations(have_market, have_shipyard, have_outfitting);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bodies_landable_materials ON bodies(is_landable, type) WHERE is_landable = true;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_materials_percentage ON body_materials(percentage DESC) WHERE percentage > 1.0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_system_factions_influence_desc ON system_factions(influence DESC) WHERE influence > 0.1;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_systems_security_economy ON systems(security, economy) WHERE population > 0;

-- Partitioning preparation (optional for very large datasets)
-- CREATE TABLE systems_partitioned (LIKE systems INCLUDING ALL) PARTITION BY HASH(id);

-- Create 8 partitions for better performance on large datasets
-- CREATE TABLE systems_p0 PARTITION OF systems_partitioned FOR VALUES WITH (modulus 8, remainder 0);
-- CREATE TABLE systems_p1 PARTITION OF systems_partitioned FOR VALUES WITH (modulus 8, remainder 1);
-- ... (continue for all 8 partitions)

-- Useful functions for distance queries
CREATE OR REPLACE FUNCTION systems_within_range(
    center_x DOUBLE PRECISION,
    center_y DOUBLE PRECISION, 
    center_z DOUBLE PRECISION,
    range_ly DOUBLE PRECISION
)
RETURNS TABLE(
    system_id BIGINT,
    system_name VARCHAR(255),
    distance DOUBLE PRECISION,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    z DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT s.id, s.name, 
           distance_3d(s.x, s.y, s.z, center_x, center_y, center_z) as dist,
           s.x, s.y, s.z
    FROM systems s
    WHERE s.x BETWEEN center_x - range_ly AND center_x + range_ly
       AND s.y BETWEEN center_y - range_ly AND center_y + range_ly
       AND s.z BETWEEN center_z - range_ly AND center_z + range_ly
       AND distance_3d(s.x, s.y, s.z, center_x, center_y, center_z) <= range_ly;
END;
$$ LANGUAGE plpgsql;