#!/usr/bin/env python3
"""
Elite Dangerous data import script to PostgreSQL - Streaming version with ijson
Usage: python import_elite_data.py <json_file> [--batch-size 100] [--workers 4]

Dependencies: pip install ijson psycopg2-binary
"""

import os
import sys
import psycopg2
import psycopg2.extras
import argparse
import logging
import time
import queue
import json
import ijson

from typing import Dict, List, Any, Optional, Iterator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "elite_dangerous"
    user: str = "elite_user"
    password: str = "elite_password"

class StreamingJSONReader:
    """JSON streaming reader for line-by-line processing"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_size = os.path.getsize(file_path)
        
    def read_systems(self) -> Iterator[tuple[Dict[str, Any], int, float]]:
        """Generator that reads systems one by one with ijson"""
        logger.info(f"Starting streaming read of file: {self.file_path}")
        logger.info(f"File size: {self.file_size / (1024**3):.2f} GB")
        
        systems_count = 0
        
        try:
            # Use ijson.items directly which is more robust
            with open(self.file_path, 'rb') as file:
                systems = ijson.items(file, 'item')
                for system in systems:
                    systems_count += 1
                    bytes_read = file.tell() if hasattr(file, 'tell') else systems_count * 1000
                    progress = (bytes_read / self.file_size) * 100 if self.file_size > 0 else 0
                    yield system, systems_count, progress
                    
        except Exception as e:
            logger.error(f"Error reading file with ijson: {e}")
            raise


class EliteDataImporter:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.conn_string = f"host={db_config.host} port={db_config.port} dbname={db_config.database} user={db_config.user} password={db_config.password}"
    
    def get_connection(self):
        """Create a new database connection"""
        return psycopg2.connect(self.conn_string)
    
    def parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse a date string to datetime"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return None
    
    def insert_faction(self, conn, faction_data: Dict[str, Any]) -> int:
        """Insert or retrieve a faction"""
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM factions WHERE id = %s", (faction_data['id'],))
            result = cur.fetchone()
            if result:
                return result[0]
            
            cur.execute("""
                INSERT INTO factions (id, name, allegiance, government, is_player)
                VALUES (%(id)s, %(name)s, %(allegiance)s, %(government)s, %(isPlayer)s)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            """, faction_data)
            
            result = cur.fetchone()
            return result[0] if result else faction_data['id']
    
    def insert_system_batch(self, systems_batch: List[Dict[str, Any]]) -> tuple[int, int]:
        """Insérer un batch de systèmes avec gestion d'erreurs optimisée"""
        processed = 0
        errors = 0
        
        conn = None
        try:
            conn = self.get_connection()
            conn.autocommit = False
            
            for system_data in systems_batch:
                try:
                    # Quick validation of essential data
                    if not self._validate_system_data(system_data):
                        errors += 1
                        continue
                    
                    self.insert_system(conn, system_data)
                    conn.commit()  # Commit after each system to avoid aborted transactions
                    processed += 1
                    
                except Exception as e:
                    errors += 1
                    logger.debug(f"Error - {system_data.get('name', 'Unknown')}: {str(e)[:100]}")
                    try:
                        conn.rollback()
                    except:
                        # If rollback fails, recreate connection
                        try:
                            conn.close()
                        except:
                            pass
                        conn = self.get_connection()
                        conn.autocommit = False
            
        except Exception as e:
            logger.error(f"Fatal batch error: {e}")
            errors += len(systems_batch) - processed
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
        return processed, errors
    
    def _validate_system_data(self, system_data: Dict[str, Any]) -> bool:
        """Quick validation of essential system data"""
        required_fields = ['id', 'name', 'coords']
        for field in required_fields:
            if field not in system_data:
                return False
        
        coords = system_data.get('coords', {})
        if not all(coord in coords for coord in ['x', 'y', 'z']):
            return False
            
        return True
    
    def insert_system(self, conn, system_data: Dict[str, Any]):
        """Insert a stellar system with optimizations"""
        with conn.cursor() as cur:
            # Process controlling faction
            controlling_faction_id = None
            if 'controllingFaction' in system_data and system_data['controllingFaction']:
                try:
                    controlling_faction_id = self.insert_faction(conn, system_data['controllingFaction'])
                except:
                    pass  # Continue without controlling faction if error
            
            # Prepare system data (optimized version)
            coords = system_data['coords']
            controlling_faction = system_data.get('controllingFaction', {})
            
            system_insert_data = {
                'id': system_data['id'],
                'id64': system_data.get('id64'),
                'name': system_data['name'],
                'x': coords['x'],
                'y': coords['y'],
                'z': coords['z'],
                'allegiance': system_data.get('allegiance'),
                'government': system_data.get('government'),
                'state': system_data.get('state'),
                'economy': system_data.get('economy'),
                'security': system_data.get('security'),
                'population': system_data.get('population', 0),
                'controlling_faction_id': controlling_faction_id,
                'controlling_faction_name': controlling_faction.get('name'),
                'controlling_faction_allegiance': controlling_faction.get('allegiance'),
                'controlling_faction_government': controlling_faction.get('government'),
                'controlling_faction_is_player': controlling_faction.get('isPlayer', False),
                'date_discovered': self.parse_datetime(system_data.get('date')),
                'raw_data': json.dumps(system_data, cls=DecimalEncoder, separators=(',', ':'))  # Compact JSON
            }
            
            # Insert the system (optimized query)
            cur.execute("""
                INSERT INTO systems (
                    id, id64, name, x, y, z, allegiance, government, state, economy, 
                    security, population, controlling_faction_id, controlling_faction_name,
                    controlling_faction_allegiance, controlling_faction_government,
                    controlling_faction_is_player, date_discovered, raw_data
                ) VALUES (
                    %(id)s, %(id64)s, %(name)s, %(x)s, %(y)s, %(z)s, %(allegiance)s, 
                    %(government)s, %(state)s, %(economy)s, %(security)s, %(population)s,
                    %(controlling_faction_id)s, %(controlling_faction_name)s,
                    %(controlling_faction_allegiance)s, %(controlling_faction_government)s,
                    %(controlling_faction_is_player)s, %(date_discovered)s, %(raw_data)s
                ) ON CONFLICT (id) DO UPDATE SET
                    allegiance = EXCLUDED.allegiance,
                    government = EXCLUDED.government,
                    state = EXCLUDED.state,
                    population = EXCLUDED.population,
                    last_updated = CURRENT_TIMESTAMP
            """, system_insert_data)
            
            # Process factions (optimized with batch insert)
            if 'factions' in system_data and system_data['factions']:
                self._insert_system_factions_batch(cur, system_data['id'], system_data['factions'])
            
            # Process stations and celestial bodies
            if 'stations' in system_data:
                for station in system_data['stations']:
                    try:
                        self.insert_station(conn, system_data['id'], station)
                    except:
                        pass  # Continue even if station fails
            
            if 'bodies' in system_data:
                for body in system_data['bodies']:
                    try:
                        self.insert_body(conn, system_data['id'], body)
                    except:
                        pass  # Continue even if body fails
    
    def _insert_system_factions_batch(self, cur, system_id: int, factions: List[Dict[str, Any]]):
        """Insert system factions in batch"""
        faction_data = []
        
        for faction in factions:
            try:
                faction_id = self.insert_faction(cur.connection, faction)
                faction_data.append((
                    system_id,
                    faction_id,
                    faction.get('influence'),
                    faction.get('state'),
                    faction.get('happiness'),
                    faction.get('lastUpdate'),
                    json.dumps(faction.get('activeStates', []), cls=DecimalEncoder, separators=(',', ':')),
                    json.dumps(faction.get('recoveringStates', []), cls=DecimalEncoder, separators=(',', ':')),
                    json.dumps(faction.get('pendingStates', []), cls=DecimalEncoder, separators=(',', ':'))
                ))
            except:
                continue
        
        if faction_data:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO system_factions (
                    system_id, faction_id, influence, state, happiness,
                    last_update, active_states, recovering_states, pending_states
                ) VALUES %s
                ON CONFLICT (system_id, faction_id) DO UPDATE SET
                    influence = EXCLUDED.influence,
                    state = EXCLUDED.state,
                    happiness = EXCLUDED.happiness
                """,
                faction_data,
                template=None,
                page_size=100
            )
    
    def insert_station(self, conn, system_id: int, station_data: Dict[str, Any]):
        """Simplified version for station insertion"""
        with conn.cursor() as cur:
            controlling_faction_id = None
            if 'controllingFaction' in station_data and station_data['controllingFaction']:
                try:
                    controlling_faction_id = self.insert_faction(conn, station_data['controllingFaction'])
                except:
                    pass
            
            body_data = station_data.get('body', {})
            update_time = station_data.get('updateTime', {})
            
            cur.execute("""
                INSERT INTO stations (
                    id, system_id, market_id, name, type, distance_to_arrival,
                    body_id, latitude, longitude, allegiance, government, economy,
                    second_economy, have_market, have_shipyard, have_outfitting,
                    other_services, controlling_faction_id, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (id) DO UPDATE SET
                    have_market = EXCLUDED.have_market,
                    have_shipyard = EXCLUDED.have_shipyard,
                    have_outfitting = EXCLUDED.have_outfitting
            """, (
                station_data['id'],
                system_id,
                station_data.get('marketId'),
                station_data['name'],
                station_data.get('type'),
                station_data.get('distanceToArrival'),
                body_data.get('id'),
                body_data.get('latitude'),
                body_data.get('longitude'),
                station_data.get('allegiance'),
                station_data.get('government'),
                station_data.get('economy'),
                station_data.get('secondEconomy'),
                station_data.get('haveMarket', False),
                station_data.get('haveShipyard', False),
                station_data.get('haveOutfitting', False),
                json.dumps(station_data.get('otherServices', []), cls=DecimalEncoder, separators=(',', ':')),
                controlling_faction_id,
                json.dumps(station_data, cls=DecimalEncoder, separators=(',', ':'))
            ))
    
    def insert_body(self, conn, system_id: int, body_data: Dict[str, Any]):
        """Simplified version for celestial body insertion"""
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bodies (
                    id, id64, system_id, body_id, name, type, sub_type, distance_to_arrival,
                    is_main_star, is_landable, is_scoopable, gravity, surface_temperature,
                    earth_masses, radius, spectral_class, atmosphere_type, terraforming_state,
                    raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (id) DO UPDATE SET
                    is_landable = EXCLUDED.is_landable,
                    terraforming_state = EXCLUDED.terraforming_state
            """, (
                body_data['id'],
                body_data.get('id64'),
                system_id,
                body_data.get('bodyId'),
                body_data['name'],
                body_data.get('type'),
                body_data.get('subType'),
                body_data.get('distanceToArrival'),
                body_data.get('isMainStar', False),
                body_data.get('isLandable', False),
                body_data.get('isScoopable', False),
                body_data.get('gravity'),
                body_data.get('surfaceTemperature'),
                body_data.get('earthMasses'),
                body_data.get('radius'),
                body_data.get('spectralClass'),
                body_data.get('atmosphereType'),
                body_data.get('terraformingState'),
                json.dumps(body_data, cls=DecimalEncoder, separators=(',', ':'))
            ))
            
            # Insert materials if body is exploitable
            if body_data.get('isLandable') and 'materials' in body_data:
                materials_data = [
                    (body_data['id'], mat_name, percentage)
                    for mat_name, percentage in body_data['materials'].items()
                    if percentage > 0.1  # Only significant materials
                ]
                
                if materials_data:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO body_materials (body_id, material_name, percentage) VALUES %s ON CONFLICT (body_id, material_name) DO UPDATE SET percentage = EXCLUDED.percentage",
                        materials_data,
                        page_size=50
                    )

def import_data_streaming(file_path: str, batch_size: int = 100, max_workers: int = 4):
    """Main streaming import function with ijson"""
    db_config = DatabaseConfig()
    
    # Check database connection
    try:
        importer = EliteDataImporter(db_config)
        conn = importer.get_connection()
        conn.close()
        logger.info("Database connection verified")
    except Exception as e:
        logger.error(f"Unable to connect to database: {e}")
        return False
    
    # Initialize streaming reader
    reader = StreamingJSONReader(file_path)
    
    # Tracking variables
    total_processed = 0
    total_errors = 0
    start_time = time.time()
    last_report_time = start_time
    systems_batch = []
    
    logger.info("=" * 70)
    logger.info("STARTING STREAMING IMPORT WITH IJSON")
    logger.info(f"File: {file_path}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Workers: {max_workers}")
    logger.info("=" * 70)
    
    # Queue for batches
    batch_queue = queue.Queue(maxsize=max_workers * 2)
    
    def worker():
        """Worker thread to process batches"""
        nonlocal total_processed, total_errors
        importer = EliteDataImporter(db_config)
        
        while True:
            try:
                batch = batch_queue.get(timeout=30)
                if batch is None:  # Stop signal
                    break
                
                processed, errors = importer.insert_system_batch(batch)
                total_processed += processed
                total_errors += errors
                
                batch_queue.task_done()
                
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                batch_queue.task_done()
    
    # Start workers
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        workers = [executor.submit(worker) for _ in range(max_workers)]
        
        try:
            # Read and process systems with ijson
            for system_data, systems_count, file_progress in reader.read_systems():
                systems_batch.append(system_data)
                
                # If batch is full, send to workers
                if len(systems_batch) >= batch_size:
                    batch_queue.put(systems_batch.copy())
                    systems_batch.clear()
                
                # Progress report every 15 seconds
                current_time = time.time()
                if current_time - last_report_time >= 15:
                    elapsed = current_time - start_time
                    rate = total_processed / elapsed if elapsed > 0 else 0
                    
                    logger.info(f"Progress: {file_progress:.1f}% file | "
                              f"Processed: {total_processed:,} | "
                              f"Speed: {rate:.1f} sys/s | "
                              f"Errors: {total_errors} | "
                              f"Queue: {batch_queue.qsize()}")
                    
                    last_report_time = current_time
            
            # Process last batch
            if systems_batch:
                batch_queue.put(systems_batch)
            
            # Wait for all batches to be processed
            batch_queue.join()
            
            # Stop workers
            for _ in range(max_workers):
                batch_queue.put(None)
            
        except KeyboardInterrupt:
            logger.info("User interruption detected")
        except Exception as e:
            logger.error(f"Error during import: {e}")
            return False
    
    # Final statistics
    total_time = time.time() - start_time
    final_rate = total_processed / total_time if total_time > 0 else 0
    success_rate = (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0
    
    logger.info("=" * 70)
    logger.info("IMPORT COMPLETED")
    logger.info("=" * 70)
    logger.info(f"Successfully imported systems: {total_processed:,}")
    logger.info(f"Total errors: {total_errors:,}")
    logger.info(f"Success rate: {success_rate:.1f}%")
    logger.info(f"Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    logger.info(f"Final average speed: {final_rate:.1f} systems/second")
    logger.info("=" * 70)
    
    return success_rate > 85

def main():
    parser = argparse.ArgumentParser(description='Import Elite Dangerous data')
    parser.add_argument('file', help='JSON file to import')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel threads (default: 4)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose mode')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check that file exists
    if not os.path.exists(args.file):
        logger.error(f"File not found: {args.file}")
        sys.exit(1)
    
    # Start import
    success = import_data_streaming(args.file, args.batch_size, args.workers)
    
    if not success:
        sys.exit(1)
    
    logger.info("Import successful!")

if __name__ == "__main__":
    main()