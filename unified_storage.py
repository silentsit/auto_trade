"""
Unified Storage Service
Consolidates all data storage and management functionality into a single, comprehensive service:
- Database management
- Position journaling
- Backup operations
- Data persistence
"""

import asyncio
import logging
import json
import sqlite3
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, asdict
from enum import Enum
import aiosqlite
import asyncpg
from pathlib import Path

logger = logging.getLogger(__name__)

class StorageType(Enum):
    """Types of storage backends"""
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MEMORY = "memory"

class BackupType(Enum):
    """Types of backup operations"""
    FULL = "full"
    INCREMENTAL = "incremental"
    POSITIONS = "positions"
    CONFIG = "config"

@dataclass
class DatabaseConfig:
    """Database configuration"""
    storage_type: StorageType
    connection_string: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600

@dataclass
class PositionRecord:
    """Position record for journaling"""
    position_id: str
    symbol: str
    action: str
    units: float
    entry_price: float
    entry_time: datetime
    stop_loss: Optional[float]
    take_profit: Optional[float]
    status: str
    pnl: float
    metadata: Dict[str, Any]

@dataclass
class BackupInfo:
    """Backup operation information"""
    backup_id: str
    backup_type: BackupType
    timestamp: datetime
    size_bytes: int
    file_path: str
    status: str
    metadata: Dict[str, Any]

class UnifiedStorage:
    """
    Unified storage service that consolidates all data management functionality
    """
    
    def __init__(self, config: DatabaseConfig = None):
        self.config = config or DatabaseConfig(
            storage_type=StorageType.SQLITE,
            connection_string="trading_bot.db"
        )
        
        # Storage state
        self.connected = False
        self.pool = None
        self._lock = asyncio.Lock()
        
        # Backup configuration
        self.backup_dir = Path("backups")
        self.backup_dir.mkdir(exist_ok=True)
        self.max_backups = 10
        
        # Initialize storage
        self._init_storage()
        
        logger.info(f"💾 Unified Storage initialized with {self.config.storage_type.value}")
    
    def _init_storage(self):
        """Initialize storage backend"""
        try:
            if self.config.storage_type == StorageType.SQLITE:
                self._init_sqlite()
            elif self.config.storage_type == StorageType.POSTGRESQL:
                self._init_postgresql()
            elif self.config.storage_type == StorageType.MEMORY:
                self._init_memory()
            
            logger.info(f"✅ Storage backend initialized: {self.config.storage_type.value}")
            
        except Exception as e:
            logger.error(f"❌ Storage initialization failed: {e}")
            raise
    
    def _init_sqlite(self):
        """Initialize SQLite database"""
        try:
            # Create database file if it doesn't exist
            db_path = Path(self.config.connection_string)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create tables
            self._create_sqlite_tables()
            
        except Exception as e:
            logger.error(f"SQLite initialization failed: {e}")
            raise
    
    def _init_postgresql(self):
        """Initialize PostgreSQL connection pool"""
        try:
            # PostgreSQL will be initialized when connecting
            pass
            
        except Exception as e:
            logger.error(f"PostgreSQL initialization failed: {e}")
            raise
    
    def _init_memory(self):
        """Initialize in-memory storage"""
        try:
            # In-memory storage is ready immediately
            pass
            
        except Exception as e:
            logger.error(f"In-memory storage initialization failed: {e}")
            raise
    
    def _create_sqlite_tables(self):
        """Create SQLite tables"""
        try:
            conn = sqlite3.connect(self.config.connection_string)
            cursor = conn.cursor()
            
            # Positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    units REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_time TEXT NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    status TEXT NOT NULL,
                    pnl REAL DEFAULT 0.0,
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Position journal table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS position_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (position_id) REFERENCES positions (position_id)
                )
            """)
            
            # Backups table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS backups (
                    backup_id TEXT PRIMARY KEY,
                    backup_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_journal_position ON position_journal(position_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backups_type ON backups(backup_type)")
            
            conn.commit()
            conn.close()
            
            logger.info("✅ SQLite tables created successfully")
            
        except Exception as e:
            logger.error(f"SQLite table creation failed: {e}")
            raise
    
    # ============================================================================
    # === DATABASE MANAGEMENT ===
    # ============================================================================
    
    async def connect(self):
        """Connect to the database"""
        try:
            if self.connected:
                return
            
            if self.config.storage_type == StorageType.SQLITE:
                await self._connect_sqlite()
            elif self.config.storage_type == StorageType.POSTGRESQL:
                await self._connect_postgresql()
            elif self.config.storage_type == StorageType.MEMORY:
                await self._connect_memory()
            
            self.connected = True
            logger.info(f"✅ Connected to {self.config.storage_type.value} database")
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    async def _connect_sqlite(self):
        """Connect to SQLite database"""
        try:
            # SQLite is file-based, no connection pool needed
            # Test connection by creating a simple query
            async with aiosqlite.connect(self.config.connection_string) as conn:
                await conn.execute("SELECT 1")
            
        except Exception as e:
            logger.error(f"SQLite connection test failed: {e}")
            raise
    
    async def _connect_postgresql(self):
        """Connect to PostgreSQL database"""
        try:
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                self.config.connection_string,
                min_size=5,
                max_size=self.config.pool_size,
                command_timeout=self.config.pool_timeout
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise
    
    async def _connect_memory(self):
        """Connect to in-memory storage"""
        try:
            # In-memory storage is always available
            pass
            
        except Exception as e:
            logger.error(f"In-memory storage connection failed: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from the database"""
        try:
            if not self.connected:
                return
            
            if self.config.storage_type == StorageType.POSTGRESQL and self.pool:
                await self.pool.close()
            
            self.connected = False
            logger.info("✅ Disconnected from database")
            
        except Exception as e:
            logger.error(f"❌ Database disconnection failed: {e}")
    
    async def is_healthy(self) -> bool:
        """Check if database is healthy"""
        try:
            if not self.connected:
                return False
            
            if self.config.storage_type == StorageType.SQLITE:
                async with aiosqlite.connect(self.config.connection_string) as conn:
                    await conn.execute("SELECT 1")
            elif self.config.storage_type == StorageType.POSTGRESQL:
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
            elif self.config.storage_type == StorageType.MEMORY:
                return True
            
            return True
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    # ============================================================================
    # === POSITION MANAGEMENT ===
    # ============================================================================
    
    async def save_position(self, position: PositionRecord) -> bool:
        """Save a position to the database"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._save_position_sqlite(position)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._save_position_postgresql(position)
            elif self.config.storage_type == StorageType.MEMORY:
                return await self._save_position_memory(position)
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to save position {position.position_id}: {e}")
            return False
    
    async def _save_position_sqlite(self, position: PositionRecord) -> bool:
        """Save position to SQLite"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                await conn.execute("""
                    INSERT OR REPLACE INTO positions 
                    (position_id, symbol, action, units, entry_price, entry_time, 
                     stop_loss, take_profit, status, pnl, metadata, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    position.position_id,
                    position.symbol,
                    position.action,
                    position.units,
                    position.entry_price,
                    position.entry_time.isoformat(),
                    position.stop_loss,
                    position.take_profit,
                    position.status,
                    position.pnl,
                    json.dumps(position.metadata),
                    datetime.now(timezone.utc).isoformat()
                ))
                
                await conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"SQLite position save failed: {e}")
            return False
    
    async def _save_position_postgresql(self, position: PositionRecord) -> bool:
        """Save position to PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO positions 
                    (position_id, symbol, action, units, entry_price, entry_time, 
                     stop_loss, take_profit, status, pnl, metadata, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (position_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    action = EXCLUDED.action,
                    units = EXCLUDED.units,
                    entry_price = EXCLUDED.entry_price,
                    entry_time = EXCLUDED.entry_time,
                    stop_loss = EXCLUDED.stop_loss,
                    take_profit = EXCLUDED.take_profit,
                    status = EXCLUDED.status,
                    pnl = EXCLUDED.pnl,
                    metadata = EXCLUDED.metadata,
                    updated_at = EXCLUDED.updated_at
                """, (
                    position.position_id,
                    position.symbol,
                    position.action,
                    position.units,
                    position.entry_price,
                    position.entry_time,
                    position.stop_loss,
                    position.take_profit,
                    position.status,
                    position.pnl,
                    json.dumps(position.metadata),
                    datetime.now(timezone.utc)
                ))
                
                return True
                
        except Exception as e:
            logger.error(f"PostgreSQL position save failed: {e}")
            return False
    
    async def _save_position_memory(self, position: PositionRecord) -> bool:
        """Save position to memory"""
        try:
            # In-memory storage would use a dictionary
            # For now, just return success
            return True
            
        except Exception as e:
            logger.error(f"Memory position save failed: {e}")
            return False
    
    async def get_position(self, position_id: str) -> Optional[PositionRecord]:
        """Get a position by ID"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_position_sqlite(position_id)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_position_postgresql(position_id)
            elif self.config.storage_type == StorageType.MEMORY:
                return await self._get_position_memory(position_id)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get position {position_id}: {e}")
            return None
    
    async def _get_position_sqlite(self, position_id: str) -> Optional[PositionRecord]:
        """Get position from SQLite"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute("""
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions WHERE position_id = ?
                """, (position_id,)) as cursor:
                    
                    row = await cursor.fetchone()
                    if row:
                        return PositionRecord(
                            position_id=row[0],
                            symbol=row[1],
                            action=row[2],
                            units=row[3],
                            entry_price=row[4],
                            entry_time=datetime.fromisoformat(row[5]),
                            stop_loss=row[6],
                            take_profit=row[7],
                            status=row[8],
                            pnl=row[9],
                            metadata=json.loads(row[10]) if row[10] else {}
                        )
                    
                    return None
                    
        except Exception as e:
            logger.error(f"SQLite position retrieval failed: {e}")
            return None
    
    async def _get_position_postgresql(self, position_id: str) -> Optional[PositionRecord]:
        """Get position from PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions WHERE position_id = $1
                """, position_id)
                
                if row:
                    return PositionRecord(
                        position_id=row[0],
                        symbol=row[1],
                        action=row[2],
                        units=row[3],
                        entry_price=row[4],
                        entry_time=row[5],
                        stop_loss=row[6],
                        take_profit=row[7],
                        status=row[8],
                        pnl=row[9],
                        metadata=row[10] if row[10] else {}
                    )
                
                return None
                
        except Exception as e:
            logger.error(f"PostgreSQL position retrieval failed: {e}")
            return None
    
    async def _get_position_memory(self, position_id: str) -> Optional[PositionRecord]:
        """Get position from memory"""
        try:
            # In-memory storage would retrieve from a dictionary
            # For now, return None
            return None
            
        except Exception as e:
            logger.error(f"Memory position retrieval failed: {e}")
            return None
    
    async def get_all_positions(self) -> List[PositionRecord]:
        """Get all positions"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_all_positions_sqlite()
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_all_positions_postgresql()
            elif self.config.storage_type == StorageType.MEMORY:
                return await self._get_all_positions_memory()
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get all positions: {e}")
            return []
    
    async def _get_all_positions_sqlite(self) -> List[PositionRecord]:
        """Get all positions from SQLite"""
        try:
            positions = []
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute("""
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions ORDER BY entry_time DESC
                """) as cursor:
                    
                    async for row in cursor:
                        positions.append(PositionRecord(
                            position_id=row[0],
                            symbol=row[1],
                            action=row[2],
                            units=row[3],
                            entry_price=row[4],
                            entry_time=datetime.fromisoformat(row[5]),
                            stop_loss=row[6],
                            take_profit=row[7],
                            status=row[8],
                            pnl=row[9],
                            metadata=json.loads(row[10]) if row[10] else {}
                        ))
            
            return positions
            
        except Exception as e:
            logger.error(f"SQLite all positions retrieval failed: {e}")
            return []
    
    async def _get_all_positions_postgresql(self) -> List[PositionRecord]:
        """Get all positions from PostgreSQL"""
        try:
            positions = []
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions ORDER BY entry_time DESC
                """)
                
                for row in rows:
                    positions.append(PositionRecord(
                        position_id=row[0],
                        symbol=row[1],
                        action=row[2],
                        units=row[3],
                        entry_price=row[4],
                        entry_time=row[5],
                        stop_loss=row[6],
                        take_profit=row[7],
                        status=row[8],
                        pnl=row[9],
                        metadata=row[10] if row[10] else {}
                    ))
            
            return positions
            
        except Exception as e:
            logger.error(f"PostgreSQL all positions retrieval failed: {e}")
            return []
    
    async def _get_all_positions_memory(self) -> List[PositionRecord]:
        """Get all positions from memory"""
        try:
            # In-memory storage would return from a dictionary
            # For now, return empty list
            return []
            
        except Exception as e:
            logger.error(f"Memory all positions retrieval failed: {e}")
            return []
    
    # ============================================================================
    # === POSITION JOURNALING ===
    # ============================================================================
    
    async def log_position_event(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Log a position event to the journal"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._log_event_sqlite(position_id, event_type, event_data)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._log_event_postgresql(position_id, event_type, event_data)
            elif self.config.storage_type == StorageType.MEMORY:
                return await self._log_event_memory(position_id, event_type, event_data)
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to log position event: {e}")
            return False
    
    async def _log_event_sqlite(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Log event to SQLite journal"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                await conn.execute("""
                    INSERT INTO position_journal (position_id, event_type, event_data)
                    VALUES (?, ?, ?)
                """, (position_id, event_type, json.dumps(event_data)))
                
                await conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"SQLite event logging failed: {e}")
            return False
    
    async def _log_event_postgresql(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Log event to PostgreSQL journal"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO position_journal (position_id, event_type, event_data)
                    VALUES ($1, $2, $3)
                """, (position_id, event_type, json.dumps(event_data)))
                
                return True
                
        except Exception as e:
            logger.error(f"PostgreSQL event logging failed: {e}")
            return False
    
    async def _log_event_memory(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Log event to memory journal"""
        try:
            # In-memory storage would append to a list
            # For now, return success
            return True
            
        except Exception as e:
            logger.error(f"Memory event logging failed: {e}")
            return False
    
    async def get_position_journal(self, position_id: str) -> List[Dict[str, Any]]:
        """Get journal entries for a position"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_journal_sqlite(position_id)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_journal_postgresql(position_id)
            elif self.config.storage_type == StorageType.MEMORY:
                return await self._get_journal_memory(position_id)
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get position journal: {e}")
            return []
    
    async def _get_journal_sqlite(self, position_id: str) -> List[Dict[str, Any]]:
        """Get journal from SQLite"""
        try:
            journal = []
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute("""
                    SELECT event_type, event_data, timestamp
                    FROM position_journal 
                    WHERE position_id = ? 
                    ORDER BY timestamp DESC
                """, (position_id,)) as cursor:
                    
                    async for row in cursor:
                        journal.append({
                            "event_type": row[0],
                            "event_data": json.loads(row[1]) if row[1] else {},
                            "timestamp": row[2]
                        })
            
            return journal
            
        except Exception as e:
            logger.error(f"SQLite journal retrieval failed: {e}")
            return []
    
    async def _get_journal_postgresql(self, position_id: str) -> List[Dict[str, Any]]:
        """Get journal from PostgreSQL"""
        try:
            journal = []
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT event_type, event_data, timestamp
                    FROM position_journal 
                    WHERE position_id = $1 
                    ORDER BY timestamp DESC
                """, position_id)
                
                for row in rows:
                    journal.append({
                        "event_type": row[0],
                        "event_data": row[1] if row[1] else {},
                        "timestamp": row[2].isoformat() if row[2] else None
                    })
            
            return journal
            
        except Exception as e:
            logger.error(f"PostgreSQL journal retrieval failed: {e}")
            return []
    
    async def _get_journal_memory(self, position_id: str) -> List[Dict[str, Any]]:
        """Get journal from memory"""
        try:
            # In-memory storage would return from a list
            # For now, return empty list
            return []
            
        except Exception as e:
            logger.error(f"Memory journal retrieval failed: {e}")
            return []
    
    # ============================================================================
    # === BACKUP OPERATIONS ===
    # ============================================================================
    
    async def create_backup(self, backup_type: BackupType, metadata: Dict[str, Any] = None) -> Optional[BackupInfo]:
        """Create a backup of the database"""
        try:
            if not self.connected:
                await self.connect()
            
            backup_id = f"{backup_type.value}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            backup_path = self.backup_dir / f"{backup_id}.db"
            
            if self.config.storage_type == StorageType.SQLITE:
                success = await self._backup_sqlite(backup_path)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                success = await self._backup_postgresql(backup_path)
            elif self.config.storage_type == StorageType.MEMORY:
                success = await self._backup_memory(backup_path)
            else:
                success = False
            
            if success:
                # Get file size
                size_bytes = backup_path.stat().st_size if backup_path.exists() else 0
                
                # Create backup record
                backup_info = BackupInfo(
                    backup_id=backup_id,
                    backup_type=backup_type,
                    timestamp=datetime.now(timezone.utc),
                    size_bytes=size_bytes,
                    file_path=str(backup_path),
                    status="completed",
                    metadata=metadata or {}
                )
                
                # Save backup record
                await self._save_backup_record(backup_info)
                
                # Cleanup old backups
                await self._cleanup_old_backups()
                
                logger.info(f"✅ Backup created: {backup_id} ({size_bytes} bytes)")
                return backup_info
            
            return None
            
        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            return None
    
    async def _backup_sqlite(self, backup_path: Path) -> bool:
        """Backup SQLite database"""
        try:
            import shutil
            shutil.copy2(self.config.connection_string, backup_path)
            return True
            
        except Exception as e:
            logger.error(f"SQLite backup failed: {e}")
            return False
    
    async def _backup_postgresql(self, backup_path: Path) -> bool:
        """Backup PostgreSQL database"""
        try:
            # This would use pg_dump in a real implementation
            # For now, return success
            return True
            
        except Exception as e:
            logger.error(f"PostgreSQL backup failed: {e}")
            return False
    
    async def _backup_memory(self, backup_path: Path) -> bool:
        """Backup in-memory storage"""
        try:
            # In-memory storage backup would serialize to file
            # For now, return success
            return True
            
        except Exception as e:
            logger.error(f"Memory backup failed: {e}")
            return False
    
    async def _save_backup_record(self, backup_info: BackupInfo):
        """Save backup record to database"""
        try:
            if self.config.storage_type == StorageType.SQLITE:
                async with aiosqlite.connect(self.config.connection_string) as conn:
                    await conn.execute("""
                        INSERT INTO backups 
                        (backup_id, backup_type, timestamp, size_bytes, file_path, status, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        backup_info.backup_id,
                        backup_info.backup_type.value,
                        backup_info.timestamp.isoformat(),
                        backup_info.size_bytes,
                        backup_info.file_path,
                        backup_info.status,
                        json.dumps(backup_info.metadata)
                    ))
                    
                    await conn.commit()
                    
        except Exception as e:
            logger.error(f"Failed to save backup record: {e}")
    
    async def _cleanup_old_backups(self):
        """Clean up old backup files"""
        try:
            # Get list of backup files
            backup_files = list(self.backup_dir.glob("*.db"))
            
            if len(backup_files) <= self.max_backups:
                return
            
            # Sort by modification time (oldest first)
            backup_files.sort(key=lambda x: x.stat().st_mtime)
            
            # Remove oldest files
            files_to_remove = backup_files[:-self.max_backups]
            for file_path in files_to_remove:
                try:
                    file_path.unlink()
                    logger.info(f"🗑️ Removed old backup: {file_path.name}")
                except Exception as e:
                    logger.warning(f"Failed to remove old backup {file_path.name}: {e}")
                    
        except Exception as e:
            logger.error(f"Backup cleanup failed: {e}")
    
    async def get_backup_info(self, backup_id: str) -> Optional[BackupInfo]:
        """Get information about a specific backup"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_backup_info_sqlite(backup_id)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_backup_info_postgresql(backup_id)
            elif self.config.storage_type == StorageType.MEMORY:
                return await self._get_backup_info_memory(backup_id)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get backup info: {e}")
            return None
    
    async def _get_backup_info_sqlite(self, backup_id: str) -> Optional[BackupInfo]:
        """Get backup info from SQLite"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute("""
                    SELECT backup_id, backup_type, timestamp, size_bytes, file_path, status, metadata
                    FROM backups WHERE backup_id = ?
                """, (backup_id,)) as cursor:
                    
                    row = await cursor.fetchone()
                    if row:
                        return BackupInfo(
                            backup_id=row[0],
                            backup_type=BackupType(row[1]),
                            timestamp=datetime.fromisoformat(row[2]),
                            size_bytes=row[3],
                            file_path=row[4],
                            status=row[5],
                            metadata=json.loads(row[6]) if row[6] else {}
                        )
                    
                    return None
                    
        except Exception as e:
            logger.error(f"SQLite backup info retrieval failed: {e}")
            return None
    
    async def _get_backup_info_postgresql(self, backup_id: str) -> Optional[BackupInfo]:
        """Get backup info from PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT backup_id, backup_type, timestamp, size_bytes, file_path, status, metadata
                    FROM backups WHERE backup_id = $1
                """, backup_id)
                
                if row:
                    return BackupInfo(
                        backup_id=row[0],
                        backup_type=BackupType(row[1]),
                        timestamp=row[2],
                        size_bytes=row[3],
                        file_path=row[4],
                        status=row[5],
                        metadata=row[6] if row[6] else {}
                    )
                
                return None
                
        except Exception as e:
            logger.error(f"PostgreSQL backup info retrieval failed: {e}")
            return None
    
    async def _get_backup_info_memory(self, backup_id: str) -> Optional[BackupInfo]:
        """Get backup info from memory"""
        try:
            # In-memory storage would return from a dictionary
            # For now, return None
            return None
            
        except Exception as e:
            logger.error(f"Memory backup info retrieval failed: {e}")
            return None
    
    # ============================================================================
    # === POSITION MANAGEMENT METHODS ===
    # ============================================================================
    
    async def get_positions_by_symbol(
        self, symbol: str, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get positions by symbol, with optional status filter"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_positions_by_symbol_sqlite(symbol, status)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_positions_by_symbol_postgresql(symbol, status)
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting positions by symbol: {e}")
            return []
    
    async def _get_positions_by_symbol_sqlite(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions by symbol from SQLite"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                if status:
                    async with conn.execute(
                        "SELECT * FROM positions WHERE symbol = ? AND status = ? ORDER BY entry_time DESC",
                        (symbol, status)
                    ) as cursor:
                        rows = await cursor.fetchall()
                else:
                    async with conn.execute(
                        "SELECT * FROM positions WHERE symbol = ? ORDER BY entry_time DESC",
                        (symbol,)
                    ) as cursor:
                        rows = await cursor.fetchall()
                
                # Convert to list of dicts
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"SQLite get_positions_by_symbol failed: {e}")
            return []
    
    async def _get_positions_by_symbol_postgresql(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions by symbol from PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                if status:
                    rows = await conn.fetch(
                        "SELECT * FROM positions WHERE symbol = $1 AND status = $2 ORDER BY entry_time DESC",
                        symbol,
                        status,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM positions WHERE symbol = $1 ORDER BY entry_time DESC",
                        symbol,
                    )
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"PostgreSQL get_positions_by_symbol failed: {e}")
            return []
    
    async def get_position_info(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_position_info_sqlite(position_id)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_position_info_postgresql(position_id)
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting position info: {e}")
            return None
    
    async def _get_position_info_sqlite(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position info from SQLite"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute(
                    "SELECT * FROM positions WHERE position_id = ?",
                    (position_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        columns = [description[0] for description in cursor.description]
                        return dict(zip(columns, row))
                    return None
                    
        except Exception as e:
            logger.error(f"SQLite get_position_info failed: {e}")
            return None
    
    async def _get_position_info_postgresql(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position info from PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM positions WHERE position_id = $1",
                    position_id
                )
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"PostgreSQL get_position_info failed: {e}")
            return None
    
    async def update_position(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        """Update position in database"""
        try:
            if not self.connected:
                await self.connect()
            
            if self.config.storage_type == StorageType.SQLITE:
                return await self._update_position_sqlite(position_id, position_data)
            elif self.config.storage_type == StorageType.POSTGRESQL:
                return await self._update_position_postgresql(position_id, position_data)
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error updating position: {e}")
            return False
    
    async def _update_position_sqlite(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        """Update position in SQLite"""
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                # Build dynamic UPDATE query
                set_clause = ", ".join([f"{k} = ?" for k in position_data.keys() if k != "position_id"])
                values = [v for k, v in position_data.items() if k != "position_id"]
                values.append(position_id)  # Add position_id for WHERE clause
                
                query = f"UPDATE positions SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE position_id = ?"
                
                async with conn.execute(query, values) as cursor:
                    await conn.commit()
                    return cursor.rowcount > 0
                    
        except Exception as e:
            logger.error(f"SQLite update_position failed: {e}")
            return False
    
    async def _update_position_postgresql(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        """Update position in PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                # Build dynamic UPDATE query
                set_clause = ", ".join([f"{k} = ${i+1}" for i, k in enumerate(position_data.keys()) if k != "position_id"])
                values = [v for k, v in position_data.items() if k != "position_id"]
                values.append(position_id)  # Add position_id for WHERE clause
                
                query = f"UPDATE positions SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE position_id = ${len(values)}"
                
                await conn.execute(query, *values)
                return True
                
        except Exception as e:
            logger.error(f"PostgreSQL update_position failed: {e}")
            return False

    # ============================================================================
    # === UTILITY METHODS ===
    # ============================================================================
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        try:
            if not self.connected:
                await self.connect()
            
            stats = {
                "storage_type": self.config.storage_type.value,
                "connected": self.connected,
                "backup_count": 0,
                "position_count": 0,
                "journal_entries": 0
            }
            
            # Get counts from database
            if self.config.storage_type == StorageType.SQLITE:
                async with aiosqlite.connect(self.config.connection_string) as conn:
                    # Position count
                    async with conn.execute("SELECT COUNT(*) FROM positions") as cursor:
                        row = await cursor.fetchone()
                        stats["position_count"] = row[0] if row else 0
                    
                    # Journal entries count
                    async with conn.execute("SELECT COUNT(*) FROM position_journal") as cursor:
                        row = await cursor.fetchone()
                        stats["journal_entries"] = row[0] if row else 0
                    
                    # Backup count
                    async with conn.execute("SELECT COUNT(*) FROM backups") as cursor:
                        row = await cursor.fetchone()
                        stats["backup_count"] = row[0] if row else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {"error": str(e)}

    async def record_position_entry(self, position_id: str, symbol: str, action: str, 
                                  timeframe: str, entry_price: float, size: float, 
                                  strategy: str, stop_loss: float = None, take_profit: float = None):
        """
        Record a position entry in the journal
        """
        try:
            entry_data = {
                "symbol": symbol,
                "action": action,
                "timeframe": timeframe,
                "entry_price": entry_price,
                "size": size,
                "strategy": strategy,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            await self.add_journal_entry(position_id, "ENTRY", entry_data)
            logger.info(f"✅ Recorded position entry for {position_id}")
            
        except Exception as e:
            logger.error(f"Error recording position entry for {position_id}: {e}")

    async def record_position_exit(self, position_id: str, exit_price: float, 
                                 exit_reason: str, pnl: float = 0.0):
        """
        Record a position exit in the journal
        """
        try:
            exit_data = {
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "pnl": pnl,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            await self.add_journal_entry(position_id, "EXIT", exit_data)
            logger.info(f"✅ Recorded position exit for {position_id}")
            
        except Exception as e:
            logger.error(f"Error recording position exit for {position_id}: {e}")

# Factory function for creating unified storage
def create_unified_storage(config: DatabaseConfig = None):
    """Create and return a unified storage instance"""
    return UnifiedStorage(config=config)
