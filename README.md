# Elite Dangerous Database Setup

PostgreSQL database configuration optimized for Elite Dangerous spatial data.

## Installation and Configuration

### 1. Starting Services

```bash
# Create necessary folders
mkdir -p  ./data

# Start services
docker-compose up -d

# Check that services are running
docker-compose ps
```

### 2. Service Access

- **PostgreSQL**: `localhost:5432`
  - Database: `elite_dangerous`
  - User: `elite_user`
  - Password: `elite_password`

## Data Import

Data can be downloaded at https://www.edsm.net/en/nightly-dumps


### Data File Preparation

```bash
# Place your JSON file in the data folder
cp your_systems_file.json ./data/

# Install Python dependencies
pip install psycopg2-binary ijson smart_open

# Start import
python import_elite_data.py ./data/your_systems_file.json --batch-size 500 --workers 6

# Start import using distant file
python import_elite_data.py https://example.com/data.json.bz2
```

### Import Options

- `--batch-size`: Number of systems processed per batch (default: 1000)
- `--workers`: Number of parallel processes (default: 4)
- `--verbose`: Detailed logging mode

## Useful Queries

### 1. Text Search Examples

```sql
-- Full text search (uses GIN index for complex queries)
SELECT name, population, allegiance 
FROM systems 
WHERE to_tsvector('english', name) @@ to_tsquery('english', 'sol | earth');

-- Simple pattern matching (uses text_pattern_ops index for ILIKE)
SELECT name, population, allegiance 
FROM systems 
WHERE name ILIKE '%sol%' 
ORDER BY population DESC;

-- Case-sensitive pattern search
SELECT name, population 
FROM systems 
WHERE name LIKE 'Sol%'
ORDER BY name;

-- Station name search
SELECT s.name as system_name, st.name as station_name, st.type
FROM systems s
JOIN stations st ON s.id = st.system_id
WHERE st.name ILIKE '%starport%'
ORDER BY s.name;

-- Faction name search
SELECT name, allegiance, government
FROM factions 
WHERE name ILIKE '%federal%'
ORDER BY name;
```

### 2. Proximity Search

```sql
-- Systems within 50 ly radius around Sol (0,0,0)
SELECT name, x, y, z, population,
       distance_3d(x, y, z, 0, 0, 0) as distance_ly
FROM systems 
  WHERE x BETWEEN -50 AND 50
    AND y BETWEEN -50 AND 50
    AND z BETWEEN -50 AND 50
    AND distance_3d(x, y, z, 0, 0, 0) <= 50
ORDER BY distance_ly;

-- Using optimized function
SELECT * FROM systems_within_range(0, 0, 0, 50);
```

### 2. Inhabited Systems with Stations

```sql
-- Inhabited systems with most stations
SELECT s.name, s.population, s.allegiance,
       COUNT(st.id) as station_count,
       MIN(st.distance_to_arrival) as closest_station
FROM systems s
JOIN stations st ON s.id = st.system_id
WHERE s.population > 0
GROUP BY s.id, s.name, s.population, s.allegiance
ORDER BY station_count DESC, s.population DESC
LIMIT 20;
```

### 3. Search for Stations with Specific Services

```sql
-- Stations with shipyard within 100 ly
SELECT s.name as system_name, st.name as station_name,
       st.distance_to_arrival,
       distance_3d(s.x, s.y, s.z, 0, 0, 0) as distance_from_sol
FROM systems s
JOIN stations st ON s.id = st.system_id
WHERE st.have_shipyard = true
  AND distance_3d(s.x, s.y, s.z, 0, 0, 0) <= 100
ORDER BY distance_from_sol, st.distance_to_arrival;
```

### 4. Materials Analysis

```sql
-- Celestial bodies with rare materials
SELECT s.name as system_name, b.name as body_name, 
       bm.material_name, bm.percentage
FROM systems s
JOIN bodies b ON s.id = b.system_id
JOIN body_materials bm ON b.id = bm.body_id
WHERE bm.material_name IN ('Polonium', 'Technetium', 'Antimony')
  AND bm.percentage > 1.0
  AND b.is_landable = true
ORDER BY bm.percentage DESC;
```

### 5. Systems by Faction

```sql
-- Faction influence by system
SELECT s.name as system_name, f.name as faction_name,
       sf.influence * 100 as influence_percent,
       sf.state, sf.happiness
FROM systems s
JOIN system_factions sf ON s.id = sf.system_id
JOIN factions f ON sf.faction_id = f.id
WHERE f.is_player = true
ORDER BY sf.influence DESC;
```

### 6. Global Statistics

```sql
-- Galaxy overview
SELECT 
    COUNT(*) as total_systems,
    COUNT(*) FILTER (WHERE population > 0) as inhabited_systems,
    COUNT(DISTINCT allegiance) as allegiances,
    AVG(population) FILTER (WHERE population > 0) as avg_population,
    MAX(population) as max_population
FROM systems;

-- Distribution by allegiance
SELECT allegiance, COUNT(*) as system_count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM systems), 2) as percentage
FROM systems
WHERE allegiance IS NOT NULL
GROUP BY allegiance
ORDER BY system_count DESC;
```

## Performance Optimizations

### Custom Indexes

```sql
-- Indexes for frequent searches
CREATE INDEX CONCURRENTLY idx_systems_coords_populated 
ON systems(x, y, z) WHERE population > 0;

-- Index for material searches
CREATE INDEX CONCURRENTLY idx_materials_rare 
ON body_materials(material_name, percentage DESC) 
WHERE percentage > 1.0;
```

### PostgreSQL Configuration

Adjust in `postgresql.conf` for better performance:

```
shared_buffers = 25% of RAM
effective_cache_size = 75% of RAM
work_mem = 256MB
maintenance_work_mem = 1GB
```

### Maintenance

```sql
-- Table statistics
VACUUM ANALYZE systems;
VACUUM ANALYZE stations;
VACUUM ANALYZE bodies;

-- Periodic reindexing
REINDEX INDEX CONCURRENTLY idx_systems_3d_coords;
```

## Advanced Navigation Queries

### Route Between Two Systems

```sql
-- Function to find intermediate systems
CREATE OR REPLACE FUNCTION find_intermediate_systems(
    start_x FLOAT, start_y FLOAT, start_z FLOAT,
    end_x FLOAT, end_y FLOAT, end_z FLOAT,
    max_jump_range FLOAT DEFAULT 50
)
RETURNS TABLE(
    system_name VARCHAR(255),
    x FLOAT, y FLOAT, z FLOAT,
    distance_from_start FLOAT,
    distance_from_end FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT s.name, s.x, s.y, s.z,
           distance_3d(s.x, s.y, s.z, start_x, start_y, start_z) as dist_start,
           distance_3d(s.x, s.y, s.z, end_x, end_y, end_z) as dist_end
    FROM systems s
    WHERE distance_3d(s.x, s.y, s.z, start_x, start_y, start_z) <= max_jump_range
       OR distance_3d(s.x, s.y, s.z, end_x, end_y, end_z) <= max_jump_range
    ORDER BY (dist_start + dist_end);
END;
$$ LANGUAGE plpgsql;
```

### System Density Analysis

```sql
-- Dense system regions
SELECT 
    FLOOR(x/50)*50 as sector_x,
    FLOOR(y/50)*50 as sector_y,
    FLOOR(z/50)*50 as sector_z,
    COUNT(*) as system_count,
    COUNT(*) FILTER (WHERE population > 0) as inhabited_count
FROM systems
GROUP BY FLOOR(x/50), FLOOR(y/50), FLOOR(z/50)
HAVING COUNT(*) > 10
ORDER BY system_count DESC;
```

## Monitoring and Surveillance

```sql
-- Table sizes
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(tablename::regclass)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(tablename::regclass) DESC;

-- Query performance
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
ORDER BY total_time DESC
LIMIT 10;
```