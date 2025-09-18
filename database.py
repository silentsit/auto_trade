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
import sqlite3
import aiosqlite

from config import config
from utils import logger

# Emergency fallback for missing DATABASE_URL
if not os.getenv("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "sqlite:///trading_bot.db"

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
                except (asyncpg.exceptions.PostgresConnectionError, sqlite3.OperationalError) as e:
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

# Flexible Database Manager - Supports PostgreSQL and SQLite

class DatabaseManager:
    def __init__(
        self,
        db_url: str = None,
        min_connections: int = None,
        max_connections: int = None,
    ):
        """Initialize database manager with support for PostgreSQL and SQLite"""
        # Handle None values and provide fallbacks
        self.db_url = db_url or getattr(config, 'database_url', 'sqlite:///trading_bot.db')
        self.min_connections = min_connections or getattr(config, 'db_min_connections', 5)
        self.max_connections = max_connections or getattr(config, 'db_max_connections', 10)
        
        self.pool = None
        self.db_type = "sqlite" if "sqlite" in self.db_url.lower() else "postgresql"
        self.logger = logging.getLogger("database_manager")

    async def initialize(self):
        """Initialize connection pool with robust fallback"""
        try:
            if self.db_type == "postgresql":
                await self._initialize_postgresql()
            else:
                await self._initialize_sqlite()
        except Exception as e:
            logger.error(f"PostgreSQL failed, falling back to SQLite: {str(e)}")
            self.db_type = "sqlite"
            await self._initialize_sqlite()

    async def _initialize_postgresql(self):
        """Initialize PostgreSQL connection pool"""
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

    async def _initialize_sqlite(self):
        """Initialize SQLite database"""
        # Extract database path from URL
        db_path = self.db_url.replace("sqlite:///", "").replace("sqlite://", "")
        if db_path.startswith("./"):
            db_path = db_path[2:]  # Remove ./ prefix
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        
        # Store the database path for SQLite operations
        self.db_path = db_path
        # Set pool to True to indicate SQLite is initialized (SQLite doesn't use connection pooling)
        self.pool = True
        
        # Create tables
        await self._create_tables()
        self.logger.info(f"SQLite database initialized at {db_path}")

    async def _create_tables(self):
        """Create database tables"""
        if self.db_type == "postgresql":
            await self._create_postgresql_tables()
        else:
            await self._create_sqlite_tables()

    async def _create_postgresql_tables(self):
        """Create PostgreSQL tables"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id SERIAL PRIMARY KEY,
                    position_id VARCHAR(255) UNIQUE NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    action VARCHAR(10) NOT NULL,
                    units DECIMAL(15,2) NOT NULL,
                    size DECIMAL(15,2),
                    entry_price DECIMAL(15,5) NOT NULL,
                    stop_loss DECIMAL(15,5),
                    take_profit DECIMAL(15,5),
                    status VARCHAR(20) DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP,
                    exit_price DECIMAL(15,5),
                    pnl DECIMAL(15,2),
                    metadata JSONB
                )
            """)

    async def _create_sqlite_tables(self):
        """Create SQLite tables"""
        db_path = self.db_url.replace("sqlite:///", "").replace("sqlite://", "")
        if db_path.startswith("./"):
            db_path = db_path[2:]
        
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT UNIQUE NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    units REAL NOT NULL,
                    size REAL,
                    entry_price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    status TEXT DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP,
                    exit_price REAL,
                    pnl REAL,
                    metadata TEXT,
                    timeframe TEXT,
                    open_time TIMESTAMP,
                    close_time TIMESTAMP,
                    last_update TIMESTAMP
                )
            """)
            
            # Add missing columns to existing tables if they don't exist
            await self._migrate_sqlite_schema(db)
            await db.commit()
    
    async def _migrate_sqlite_schema(self, db):
        """Add missing columns to existing SQLite tables"""
        try:
            # Get current table schema
            cursor = await db.execute("PRAGMA table_info(positions)")
            columns = await cursor.fetchall()
            existing_columns = {col[1] for col in columns}  # col[1] is column name
            
            # List of required columns and their definitions
            required_columns = {
                'timeframe': 'TEXT',
                'open_time': 'TIMESTAMP',
                'close_time': 'TIMESTAMP', 
                'last_update': 'TIMESTAMP',
                'size': 'REAL'
            }
            
            # Add missing columns
            for column_name, column_type in required_columns.items():
                if column_name not in existing_columns:
                    await db.execute(f"ALTER TABLE positions ADD COLUMN {column_name} {column_type}")
                    self.logger.info(f"✅ Added missing column: {column_name}")
                    
        except Exception as e:
            self.logger.warning(f"Schema migration warning: {e}")

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
        if self.pool and self.db_type == "postgresql":
            await self.pool.close()
            self.logger.info("PostgreSQL connection pool closed")
        elif self.db_type == "sqlite":
            self.logger.info("SQLite database connection closed")

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
            if self.db_type == "postgresql":
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
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    columns = list(position_data.keys())
                    values = list(position_data.values())
                    
                    # SQLite UPSERT using INSERT OR REPLACE
                    placeholders = ", ".join(["?" for _ in values])
                    insert_query = f"""
                        INSERT OR REPLACE INTO positions ({", ".join(columns)}) 
                        VALUES ({placeholders})
                    """
                    
                    await conn.execute(insert_query, values)
                    await conn.commit()
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
            
            if self.db_type == "postgresql":
                async with self.pool.acquire() as conn:
                    set_items = [
                        f"{key} = ${i+1}" for i, key in enumerate(updates.keys())
                    ]
                    values = list(updates.values())
                    values.append(position_id)
                    
                    query = f"UPDATE positions SET {', '.join(set_items)} WHERE position_id = ${len(values)}"
                    
                    await conn.execute(query, *values)
                    return True
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    set_items = [
                        f"{key} = ?" for key in updates.keys()
                    ]
                    values = list(updates.values())
                    values.append(position_id)
                    
                    query = f"UPDATE positions SET {', '.join(set_items)} WHERE position_id = ?"
                    
                    await conn.execute(query, *values)
                    await conn.commit()
                    return True

        except Exception as e:
            self.logger.error(f"Error updating position in database: {str(e)}")
            return False
        
    @db_retry()
    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID"""
        try:
            # Check if pool is available
            if not self.pool:
                self.logger.warning("Database pool not available, returning None for position lookup")
                return None
                
            if self.db_type == "postgresql":
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
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute(
                        "SELECT * FROM positions WHERE position_id = ?", (position_id,)
                    )
                    row = await cursor.fetchone()
                    
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
                    
                    return position_data
                
        except Exception as e:
            self.logger.error(f"Error getting position from database: {str(e)}")
            return None

    async def get_all_positions_by_status(
        self, status: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Helper to get positions by status"""
        try:
            # Check if pool is available
            if not self.pool:
                self.logger.warning("Database pool not available, returning empty positions list")
                return []
                
            if self.db_type == "postgresql":
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT * FROM positions WHERE status = $1 ORDER BY open_time DESC LIMIT $2",
                        status,
                        limit,
                    )
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute(
                        "SELECT * FROM positions WHERE status = ? ORDER BY open_time DESC LIMIT ?",
                        (status, limit)
                    )
                    rows = await cursor.fetchall()
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
            if self.db_type == "postgresql":
                async with self.pool.acquire() as conn:
                    await conn.execute("DELETE FROM positions WHERE position_id = $1", position_id)
                    return True
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute("DELETE FROM positions WHERE position_id = ?", (position_id,))
                    await conn.commit()
                    return True
        except Exception as e:
            self.logger.error(f"Error deleting position from database: {e}")
            return False

    async def get_positions_by_symbol(
        self, symbol: str, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get positions by symbol, with optional status filter"""
        try:
            # Check if pool is available
            if not self.pool:
                self.logger.warning("Database pool not available, returning empty positions list")
                return []
                
            if self.db_type == "postgresql":
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
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    conn.row_factory = aiosqlite.Row
                    if status:
                        cursor = await conn.execute(
                            "SELECT * FROM positions WHERE symbol = ? AND status = ? ORDER BY open_time DESC",
                            (symbol, status)
                        )
                    else:
                        cursor = await conn.execute(
                            "SELECT * FROM positions WHERE symbol = ? ORDER BY open_time DESC",
                            (symbol,)
                        )
                    rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(
                f"Error getting positions by symbol from database: {e}"
            )
            return []

    async def get_all_positions(self) -> List[Dict[str, Any]]:
        """Get all positions from the database"""
        try:
            # Check if pool is available
            if not self.pool:
                self.logger.warning("Database pool not available, returning empty positions list")
                return []
                
            if self.db_type == "postgresql":
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("SELECT * FROM positions ORDER BY open_time DESC")
                    return [dict(row) for row in rows]
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute("SELECT * FROM positions ORDER BY open_time DESC")
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Error getting all positions from database: {e}")
            return []

    async def get_connection(self):
        """Get database connection - handles both PostgreSQL and SQLite"""
        if self.db_type == "postgresql":
            return self.pool.acquire()
        else:  # SQLite
            return aiosqlite.connect(self.db_path)
    
    async def ensure_connection(self):
        """Ensure the database connection is active."""
        if not self.pool:
            await self.initialize()
        try:
            if self.db_type == "postgresql":
                if self.pool.is_closing():
                    await self.initialize()
                async with self.pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
            else:  # SQLite
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute("SELECT 1")
            self.logger.info("Database connection confirmed.")
        except Exception as e:
            self.logger.error(f"Database connection check failed: {e}")
            await self.initialize()
