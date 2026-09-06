"""Column lists shared by the flattener (row layout) and the database layer
(COPY column lists, temp tables, upsert statements). Order matters: it is the
order of the values in every COPY row produced by flatten.py, and it matches
the declaration order in sql/schema.sql.
"""

SYSTEMS = (
    "id64", "population", "date", "built_at", "version_ts",
    "ts_controlling_power", "ts_factions", "ts_power_state", "ts_powers",
    "x", "y", "z",
    "power_state_control_progress", "power_state_reinforcement", "power_state_undermining",
    "allegiance", "government", "primary_economy", "secondary_economy", "security",
    "power_state", "controlling_power", "controlling_faction_state",
    "body_count", "name", "controlling_faction", "powers",
    "power_conflict_progress", "thargoid_war",
)

SYSTEM_FACTIONS = (
    "system_id64", "influence", "allegiance", "government", "state", "name",
    "active_states", "pending_states", "recovering_states",
)

BODIES = (
    "id64", "system_id64", "update_time",
    "distance_to_arrival", "age", "absolute_magnitude", "solar_masses", "solar_radius",
    "surface_temperature", "rotational_period", "axial_tilt", "gravity", "earth_masses",
    "radius", "surface_pressure", "orbital_period", "semi_major_axis", "orbital_eccentricity",
    "orbital_inclination", "arg_of_periapsis", "mean_anomaly", "ascending_node",
    "comp_ice", "comp_metal", "comp_rock",
    "type", "sub_type", "luminosity", "volcanism_type", "atmosphere_type",
    "terraforming_state", "reserve_level",
    "body_id", "main_star", "tidally_locked", "is_landable",
    "name", "spectral_class", "parents", "atmosphere_composition", "genuses",
)

BODY_MATERIALS = ("body_id64", "percentage", "material")
BODY_SIGNALS = ("body_id64", "count", "signal")
BODY_RINGS = ("body_id64", "id64", "mass", "inner_radius", "outer_radius", "type", "is_belt", "name")
RING_SIGNALS = ("body_id64", "update_time", "count", "signal", "ring_name")

STATIONS = (
    "id", "system_id64", "body_id64", "update_time", "version_ts",
    "market_update_time", "shipyard_update_time", "outfitting_update_time",
    "distance_to_arrival", "latitude", "longitude",
    "type", "state", "allegiance", "government", "primary_economy", "secondary_economy",
    "controlling_faction_state", "run_id",
    "pads_large", "pads_medium", "pads_small",
    "has_market", "has_shipyard", "has_outfitting",
    "name", "real_name", "carrier_name", "controlling_faction", "carrier_docking_access",
    "services", "prohibited_commodities", "economies",
)

STATION_OUTFITTING = ("station_id", "update_time", "module_ids")
STATION_SHIPYARD = ("station_id", "update_time", "ship_ids")
STATION_COMMODITIES = ("station_id", "commodity_id", "demand", "supply", "buy_price", "sell_price")

COMMODITIES = ("id", "category", "name", "symbol")
MODULES = ("id", "class", "rating", "category", "name", "symbol", "ship")
SHIPS = ("id", "name", "symbol")

# table name -> columns, in the order the flattener fills its buffers
TABLES = {
    "systems": SYSTEMS,
    "system_factions": SYSTEM_FACTIONS,
    "bodies": BODIES,
    "body_materials": BODY_MATERIALS,
    "body_signals": BODY_SIGNALS,
    "body_rings": BODY_RINGS,
    "ring_signals": RING_SIGNALS,
    "stations": STATIONS,
    "station_outfitting": STATION_OUTFITTING,
    "station_shipyard": STATION_SHIPYARD,
    "station_commodities": STATION_COMMODITIES,
}

# Tables whose rows belong to a system (carry system_id64) and are replaced
# with it, versus tables keyed by station id (replaced with the station).
SYSTEM_CHILD_TABLES = ("system_factions", "bodies", "body_materials", "body_signals", "body_rings", "ring_signals")
STATION_CHILD_TABLES = ("station_outfitting", "station_shipyard", "station_commodities")

# column -> enum type, per table, for the runtime label tracker
ENUM_COLUMNS = {
    "systems": {
        "allegiance": "allegiance_t", "government": "government_t",
        "primary_economy": "economy_t", "secondary_economy": "economy_t",
        "security": "security_t", "power_state": "power_state_t",
        "controlling_power": "power_t", "controlling_faction_state": "faction_state_t",
    },
    "system_factions": {"allegiance": "allegiance_t", "government": "government_t", "state": "faction_state_t"},
    "bodies": {
        "type": "body_type_t", "sub_type": "body_sub_type_t", "luminosity": "luminosity_t",
        "volcanism_type": "volcanism_t", "atmosphere_type": "atmosphere_t",
        "terraforming_state": "terraforming_t", "reserve_level": "reserve_level_t",
    },
    "body_materials": {"material": "material_t"},
    "body_signals": {"signal": "signal_t"},
    "body_rings": {"type": "ring_type_t"},
    "ring_signals": {"signal": "signal_t"},
    "stations": {
        "type": "station_type_t", "state": "station_state_t", "allegiance": "allegiance_t",
        "government": "government_t", "primary_economy": "economy_t",
        "secondary_economy": "economy_t", "controlling_faction_state": "faction_state_t",
    },
    "commodities": {"category": "commodity_category_t"},
    "modules": {"rating": "module_rating_t", "category": "module_category_t"},
}
ENUM_TYPES = sorted({t for cols in ENUM_COLUMNS.values() for t in cols.values()})
