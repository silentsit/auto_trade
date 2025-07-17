import os
import json
import logging
import asyncio
import subprocess
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import asyncpg

from config import config
from utils import logger


def db_retry(max_retries=3, retry_delay=2):
    """
    Decorator to retry async DB operations on connection errors.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except asyncpg.exceptions.PostgresConnectionError as e:
                    retries += 1
                    logger.warning(
                        f"Database connection error in {func.__name__}, retry {retries}/{max_retries}: {str(e)}"
                    )
                    if retries >= max_retries:
                        logger.error(f"Max database retries reached for {func.__name__}")
                        raise
                    wait_time = retry_delay * (2 ** (retries - 1))  # exponential backoff
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Database error in {func.__name__}: {str(e)}", exc_info=True)
                    raise
        return wrapper
    return decorator

# PostgreSQL Database Manager - Focused implementation

class DatabaseManager:
    def __init__(
        self,
        db_url: str = config.database.url,
        min_connections: int = config.database.pool_size,
        max_connections: int = config.database.max_overflow,
    ):
        """Initialize PostgreSQL database manager"""
        self.db_url = db_url
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.pool = None
        self.logger = logging.getLogger("postgres_manager")

    async def initialize(self):
        """Initialize connection pool"""
        try:
            # Check for common placeholder issues in database URL
            if not self.db_url or self.db_url == "":
                raise Exception("Database URL is empty")
            
            if "<port>" in self.db_url or "<host>" in self.db_url or "<password>" in self.db_url:
                raise Exception(f"Database URL contains placeholder values: {self.db_url}")
            
            self.pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=60.0,
                timeout=10.0,
            )
            
            if self.pool:
                await self._create_tables()
                self.logger.info("PostgreSQL connection pool initialized")
            else:
                self.logger.error("Failed to create PostgreSQL connection pool")
                raise Exception("Failed to create PostgreSQL connection pool")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL database: {str(e)}")
            raise

    async def backup_database(self, backup_path: str) -> bool:
        """Create a backup of the database using pg_dump."""
        try:
            parsed_url = urlparse(self.db_url)
            db_params = {
                'username': parsed_url.username,
                'password': parsed_url.password,
                'host': parsed_url.hostname,
                'port': str(parsed_url.port or 5432),
                'dbname': parsed_url.path.lstrip('/')
            }
            if not all([db_params['username'], db_params['password'], db_params['host'], db_params['dbname']]):
                self.logger.error("Database URL is missing required components.")
                return False

            cmd = [
                'pg_dump',
                f"--host={db_params['host']}",
                f"--port={db_params['port']}",
                f"--username={db_params['username']}",
                f"--dbname={db_params['dbname']}",
                '--format=custom',
                f"--file={backup_path}",
            ]

            env = os.environ.copy()
            env['PGPASSWORD'] = db_params['password']

            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                self.logger.info(f"[DATABASE BACKUP] Success. Backup saved at: {backup_path}")
                return True
            else:
                self.logger.error(f"[DATABASE BACKUP] pg_dump failed: {result.stderr.strip()}")
                return False

        except Exception as e:
            self.logger.error(f"[DATABASE BACKUP] Error during backup: {str(e)}")
            return False

    async def restore_from_backup(self, backup_path: str) -> bool:
        """Restore database from a PostgreSQL backup file."""
        try:
            parsed_url = urlparse(self.db_url)
            db_params = {
                'username': parsed_url.username,
                'password': parsed_url.password,
                'host': parsed_url.hostname,
                'port': str(parsed_url.port or 5432),
                'dbname': parsed_url.path.lstrip('/')
            }

            if '?' in db_params['dbname']:
                db_params['dbname'] = db_params['dbname'].split('?')[0]

            cmd = [
                'pg_restore',
                f"--host={db_params['host']}",
                f"--port={db_params['port']}",
                f"--username={db_params['username']}",
                f"--dbname={db_params['dbname']}",
                '--clean',
                '--no-owner',
                backup_path,
            ]

            env = os.environ.copy()
            if db_params['password']:
                env['PGPASSWORD'] = db_params['password']

            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                self.logger.info(f"Database restored from {backup_path}")
                return True
            else:
                self.logger.error(f"pg_restore failed: {result.stderr}")
                return False

        except Exception as e:
            self.logger.error(f"Error restoring database from backup: {str(e)}")
            return False

    async def close(self):
        """Close the connection pool"""
        if self.pool:
            await self.pool.close()
            self.logger.info("PostgreSQL connection pool closed")

    async def _create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            async with self.pool.acquire() as conn:
                # Create positions table
                await conn.execute(
                    '''
                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    size DOUBLE PRECISION NOT NULL,
                    stop_loss DOUBLE PRECISION,
                    take_profit DOUBLE PRECISION,
                    open_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    close_time TIMESTAMP WITH TIME ZONE,
                    exit_price DOUBLE PRECISION,
                    current_price DOUBLE PRECISION NOT NULL,
                    pnl DOUBLE PRECISION NOT NULL,
                    pnl_percentage DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL,
                    last_update TIMESTAMP WITH TIME ZONE NOT NULL,
                    metadata JSONB,
                    exit_reason TEXT
                )
                '''
                )

                # Create indexes for common query patterns
                await conn.execute(
                    'CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)'
                )
                await conn.execute(
                    'CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)'
                )

                self.logger.info(
                    "PostgreSQL database tables created or verified"
                )
        except Exception as e:
            self.logger.error(f"Error creating database tables: {str(e)}")
            raise

    @db_retry()
    async def save_position(self, position_data: Dict[str, Any]) -> bool:
        """Save position to database"""
        try:
            # Process metadata to ensure it's in the right format for PostgreSQL
            position_data = (
                position_data.copy()
            )  # Create a copy to avoid modifying the original

            # Convert metadata to JSON if it exists and is a dict
            if "metadata" in position_data and isinstance(
                position_data["metadata"], dict
            ):
                position_data["metadata"] = json.dumps(
                    position_data["metadata"]
                )

            # Convert datetime strings to datetime objects if needed
            for field in ["open_time", "close_time", "last_update"]:
                if field in position_data and isinstance(
                    position_data[field], str
                ):
                    try:
                        position_data[field] = datetime.fromisoformat(
                            position_data[field].replace('Z', '+00:00')
                        )
                    except ValueError:
                        # Keep as string if datetime parsing fails
                        pass
            
            # Use an UPSERT operation for efficiency
            async with self.pool.acquire() as conn:
                columns = list(position_data.keys())
                values = list(position_data.values())
                
                insert_query = f"""
                    INSERT INTO positions ({", ".join(columns)}) 
                    VALUES ({", ".join([f'${i+1}' for i in range(len(columns))])})
                    ON CONFLICT (position_id) DO UPDATE SET
                        {", ".join([f'{col} = EXCLUDED.{col}' for col in columns if col != 'position_id'])}
                """
                
                await conn.execute(insert_query, *values)
                return True

        except Exception as e:
            self.logger.error(f"Error saving position to database: {str(e)}")
            return False

    @db_retry()
    async def update_position(
        self, position_id: str, updates: Dict[str, Any]
    ) -> bool:
        """Update position in database"""
        try:
            updates = updates.copy()

            if "metadata" in updates and isinstance(updates["metadata"], dict):
                updates["metadata"] = json.dumps(updates["metadata"])

            for field in ["open_time", "close_time", "last_update"]:
                if field in updates and isinstance(updates[field], str):
                    try:
                        updates[field] = datetime.fromisoformat(
                            updates[field].replace('Z', '+00:00')
                        )
                    except ValueError:
                        pass
            
            async with self.pool.acquire() as conn:
                set_items = [
                    f"{key} = ${i+1}" for i, key in enumerate(updates.keys())
                ]
                values = list(updates.values())
                values.append(position_id)
                
                query = f"UPDATE positions SET {', '.join(set_items)} WHERE position_id = ${len(values)}"
                
                await conn.execute(query, *values)
                return True

        except Exception as e:
            self.logger.error(f"Error updating position in database: {str(e)}")
            return False
        
    @db_retry()
    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM positions WHERE position_id = $1", position_id
                )
                
                if not row:
                    return None
                    
                position_data = dict(row)
                
                # Parse metadata if it's a string
                if "metadata" in position_data and isinstance(
                    position_data["metadata"], str
                ):
                    try:
                        position_data["metadata"] = json.loads(
                            position_data["metadata"]
                        )
                    except json.JSONDecodeError:
                        self.logger.warning(f"Failed to decode metadata for position {position_id}")
                        
                # Format datetimes as ISO strings for consistency
                for field in ["open_time", "close_time", "last_update"]:
                    if position_data.get(field) and isinstance(
                        position_data[field], datetime
                    ):
                        position_data[field] = position_data[field].isoformat()
                        
                return position_data
                
        except Exception as e:
            self.logger.error(f"Error getting position from database: {str(e)}")
            return None

    async def get_all_positions_by_status(
        self, status: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Helper to get positions by status"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM positions WHERE status = $1 ORDER BY open_time DESC LIMIT $2",
                    status,
                    limit,
                )
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Error getting positions by status from DB: {e}")
            return []

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        return await self.get_all_positions_by_status("open")

    async def get_closed_positions(
        self, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get the most recent closed positions"""
        return await self.get_all_positions_by_status("closed", limit=limit)

    @db_retry()
    async def delete_position(self, position_id: str) -> bool:
        """Delete a position from the database"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("DELETE FROM positions WHERE position_id = $1", position_id)
                return True
        except Exception as e:
            self.logger.error(f"Error deleting position from database: {e}")
            return False

    async def get_positions_by_symbol(
        self, symbol: str, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get positions by symbol, with optional status filter"""
        try:
            async with self.pool.acquire() as conn:
                if status:
                    rows = await conn.fetch(
                        "SELECT * FROM positions WHERE symbol = $1 AND status = $2 ORDER BY open_time DESC",
                        symbol,
                        status,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM positions WHERE symbol = $1 ORDER BY open_time DESC",
                        symbol,
                    )
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(
                f"Error getting positions by symbol from database: {e}"
            )
            return []

    async def get_all_positions(self) -> List[Dict[str, Any]]:
        """Get all positions from the database"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM positions ORDER BY open_time DESC")
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Error getting all positions from database: {e}")
            return []

    async def ensure_connection(self):
        """Ensure the database connection is active."""
        if not self.pool or self.pool.is_closing():
            await self.initialize()
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            self.logger.info("Database connection confirmed.")
        except Exception as e:
            self.logger.error(f"Database connection check failed: {e}")
            await self.initialize()
