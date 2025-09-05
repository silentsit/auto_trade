"""
Unified Storage Service
Consolidates all data storage and management functionality into a single, comprehensive service:
- Database management
- Position journaling
- Backup operations
- Data persistence
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import aiosqlite
import asyncpg

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Enums & Models
# -----------------------------------------------------------------------------
class StorageType(str, Enum):
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MEMORY = "memory"  # keep since callers reference it


class BackupType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    MANUAL = "manual"


@dataclass
class BackupInfo:
    backup_id: str
    backup_type: BackupType
    timestamp: datetime
    size_bytes: int
    file_path: str
    status: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PositionRecord:
    position_id: str
    symbol: str
    action: str
    units: float
    entry_price: float
    # Accept both str (ISO) and datetime for flexibility with existing callers
    entry_time: Union[str, datetime]
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    status: str = "open"
    pnl: float = 0.0
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DatabaseConfig:
    """
    Unified configuration for both SQLite and PostgreSQL.

    Backwards compatible with callers that:
      • call DatabaseConfig() with no args, OR
      • pass pool_min_size / pool_max_size / command_timeout / ssl / app_name,
        OR
      • only set storage_type + connection_string.
    """
    storage_type: StorageType = StorageType.SQLITE
    # For sqlite, this is the file path; for PG, a DSN.
    connection_string: str = ""

    # Optional (PG-focused) tuning
    sqlite_path: Optional[str] = None
    pool_min_size: int = 1
    pool_max_size: int = 5
    command_timeout: int = 60
    ssl: Optional[str] = None        # e.g. "require" / "disable" / None
    app_name: Optional[str] = "auto-trade-bot"

    @classmethod
    def for_sqlite(cls, path: str) -> "DatabaseConfig":
        return cls(
            storage_type=StorageType.SQLITE,
            connection_string=path,
            sqlite_path=path,
        )

    @classmethod
    def for_postgres(
        cls,
        dsn: str,
        *,
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        command_timeout: int = 60,
        ssl: Optional[str] = None,
        app_name: Optional[str] = "auto-trade-bot",
    ) -> "DatabaseConfig":
        return cls(
            storage_type=StorageType.POSTGRESQL,
            connection_string=dsn,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
            command_timeout=command_timeout,
            ssl=ssl,
            app_name=app_name,
        )


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_iso(ts: Union[str, datetime]) -> str:
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    return ts


def _to_dt(ts: Union[str, datetime]) -> datetime:
    if isinstance(ts, datetime):
        return ts
    return datetime.fromisoformat(ts)


def _pg_ssl_param(ssl_value: Optional[str]):
    """Translate string SSL settings to asyncpg-compatible values."""
    if ssl_value is None:
        return None
    s = str(ssl_value).strip().lower()
    if s in {"require", "enabled", "enable", "true", "1", "on", "yes"}:
        return True
    if s in {"disable", "disabled", "false", "0", "off", "no"}:
        return False
    # If user passes something else (like a path), just let asyncpg decide
    return None


# -----------------------------------------------------------------------------
# Unified Storage
# -----------------------------------------------------------------------------
class UnifiedStorage:
    """
    Unified storage service that consolidates all data management functionality
    """

    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config or DatabaseConfig(
            storage_type=StorageType.SQLITE,
            connection_string="trading_bot.db",
        )

        # Storage state
        self.connected: bool = False
        self.pool: Optional[asyncpg.Pool] = None
        self._lock = asyncio.Lock()

        # Backup configuration
        self.backup_dir = Path("backups")
        self.backup_dir.mkdir(exist_ok=True)
        self.max_backups = 10

        # Initialize storage structures/tables (SQLite immediate, PG on connect)
        self._init_storage()

        logger.info(f"💾 Unified Storage initialized with {self.config.storage_type.value}")

    # -- init backends ---------------------------------------------------------
    def _init_storage(self):
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
        """Initialize SQLite database (create file + tables)."""
        try:
            db_path = Path(self.config.connection_string or "trading_bot.db")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._create_sqlite_tables()
        except Exception as e:
            logger.error(f"SQLite initialization failed: {e}")
            raise

    def _init_postgresql(self):
        """PG structures are created on first connect."""
        pass

    def _init_memory(self):
        """In-memory storage placeholder."""
        pass

    # -- DDL -------------------------------------------------------------------
    def _create_sqlite_tables(self):
        try:
            conn = sqlite3.connect(self.config.connection_string)
            cursor = conn.cursor()

            cursor.execute(
                """
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
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS position_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (position_id) REFERENCES positions (position_id)
                )
                """
            )

            cursor.execute(
                """
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
                """
            )

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

    async def _create_postgresql_tables(self):
        try:
            if not self.connected:
                await self.connect()

            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS positions (
                        position_id VARCHAR PRIMARY KEY,
                        symbol VARCHAR NOT NULL,
                        action VARCHAR NOT NULL,
                        units DECIMAL NOT NULL,
                        entry_price DECIMAL NOT NULL,
                        entry_time VARCHAR NOT NULL,
                        stop_loss DECIMAL,
                        take_profit DECIMAL,
                        status VARCHAR NOT NULL,
                        pnl DECIMAL DEFAULT 0.0,
                        metadata TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS position_journal (
                        id SERIAL PRIMARY KEY,
                        position_id VARCHAR NOT NULL,
                        event_type VARCHAR NOT NULL,
                        event_data TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (position_id) REFERENCES positions (position_id)
                    )
                    """
                )

                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS backups (
                        backup_id VARCHAR PRIMARY KEY,
                        backup_type VARCHAR NOT NULL,
                        timestamp VARCHAR NOT NULL,
                        size_bytes INTEGER NOT NULL,
                        file_path VARCHAR NOT NULL,
                        status VARCHAR NOT NULL,
                        metadata TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                await conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_journal_position ON position_journal(position_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_backups_type ON backups(backup_type)")

            logger.info("✅ PostgreSQL tables created successfully")
        except Exception as e:
            logger.error(f"❌ PostgreSQL table creation failed: {e}")
            raise

    # -----------------------------------------------------------------------------
    # Connection Management
    # -----------------------------------------------------------------------------
    async def connect(self):
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
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                await conn.execute("SELECT 1")
        except Exception as e:
            logger.error(f"SQLite connection test failed: {e}")
            raise

    async def _connect_postgresql(self):
        try:
            ssl_param = _pg_ssl_param(self.config.ssl)
            server_settings = {}
            if self.config.app_name:
                server_settings["application_name"] = self.config.app_name

            self.pool = await asyncpg.create_pool(
                dsn=self.config.connection_string,
                min_size=self.config.pool_min_size,
                max_size=self.config.pool_max_size,
                command_timeout=self.config.command_timeout,
                ssl=ssl_param,
                server_settings=server_settings or None,
            )
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            await self._create_postgresql_tables()
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise

    async def _connect_memory(self):
        # Nothing to do for the stub
        return

    async def disconnect(self):
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

    # -----------------------------------------------------------------------------
    # Positions
    # -----------------------------------------------------------------------------
    async def save_position(self, position: PositionRecord) -> bool:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._save_position_sqlite(position)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._save_position_postgresql(position)
            if self.config.storage_type == StorageType.MEMORY:
                return await self._save_position_memory(position)
            return False
        except Exception as e:
            logger.error(f"Failed to save position {position.position_id}: {e}")
            return False

    async def _save_position_sqlite(self, position: PositionRecord) -> bool:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO positions
                    (position_id, symbol, action, units, entry_price, entry_time,
                     stop_loss, take_profit, status, pnl, metadata, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        position.position_id,
                        position.symbol,
                        position.action,
                        position.units,
                        position.entry_price,
                        _to_iso(position.entry_time),
                        position.stop_loss,
                        position.take_profit,
                        position.status,
                        position.pnl,
                        json.dumps(position.metadata or {}),
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"SQLite position save failed: {e}")
            return False

    async def _save_position_postgresql(self, position: PositionRecord) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
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
                    """,
                    (
                        position.position_id,
                        position.symbol,
                        position.action,
                        position.units,
                        position.entry_price,
                        _to_iso(position.entry_time),
                        position.stop_loss,
                        position.take_profit,
                        position.status,
                        position.pnl,
                        json.dumps(position.metadata or {}),
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                return True
        except Exception as e:
            logger.error(f"PostgreSQL position save failed: {e}")
            return False

    async def _save_position_memory(self, position: PositionRecord) -> bool:
        # Stubbed memory backend: pretend success
        return True

    async def get_position(self, position_id: str) -> Optional[PositionRecord]:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_position_sqlite(position_id)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_position_postgresql(position_id)
            if self.config.storage_type == StorageType.MEMORY:
                return await self._get_position_memory(position_id)
            return None
        except Exception as e:
            logger.error(f"Failed to get position {position_id}: {e}")
            return None

    async def _get_position_sqlite(self, position_id: str) -> Optional[PositionRecord]:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute(
                    """
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions WHERE position_id = ?
                    """,
                    (position_id,),
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return PositionRecord(
                            position_id=row[0],
                            symbol=row[1],
                            action=row[2],
                            units=row[3],
                            entry_price=row[4],
                            entry_time=_to_dt(row[5]),
                            stop_loss=row[6],
                            take_profit=row[7],
                            status=row[8],
                            pnl=row[9],
                            metadata=json.loads(row[10]) if row[10] else {},
                        )
                    return None
        except Exception as e:
            logger.error(f"SQLite position retrieval failed: {e}")
            return None

    async def _get_position_postgresql(self, position_id: str) -> Optional[PositionRecord]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions WHERE position_id = $1
                    """,
                    position_id,
                )
                if row:
                    # entry_time is stored as VARCHAR; convert if present
                    et = row[5]
                    et = _to_dt(et) if isinstance(et, str) else et
                    meta = row[10]
                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except Exception:
                            meta = {}
                    return PositionRecord(
                        position_id=row[0],
                        symbol=row[1],
                        action=row[2],
                        units=float(row[3]),
                        entry_price=float(row[4]),
                        entry_time=et,
                        stop_loss=float(row[6]) if row[6] is not None else None,
                        take_profit=float(row[7]) if row[7] is not None else None,
                        status=row[8],
                        pnl=float(row[9]) if row[9] is not None else 0.0,
                        metadata=meta or {},
                    )
                return None
        except Exception as e:
            logger.error(f"PostgreSQL position retrieval failed: {e}")
            return None

    async def _get_position_memory(self, position_id: str) -> Optional[PositionRecord]:
        return None

    async def get_all_positions(self) -> List[PositionRecord]:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_all_positions_sqlite()
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_all_positions_postgresql()
            if self.config.storage_type == StorageType.MEMORY:
                return await self._get_all_positions_memory()
            return []
        except Exception as e:
            logger.error(f"Failed to get all positions: {e}")
            return []

    async def _get_all_positions_sqlite(self) -> List[PositionRecord]:
        try:
            positions: List[PositionRecord] = []
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute(
                    """
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions ORDER BY entry_time DESC
                    """
                ) as cursor:
                    async for row in cursor:
                        positions.append(
                            PositionRecord(
                                position_id=row[0],
                                symbol=row[1],
                                action=row[2],
                                units=row[3],
                                entry_price=row[4],
                                entry_time=_to_dt(row[5]),
                                stop_loss=row[6],
                                take_profit=row[7],
                                status=row[8],
                                pnl=row[9],
                                metadata=json.loads(row[10]) if row[10] else {},
                            )
                        )
            return positions
        except Exception as e:
            logger.error(f"SQLite all positions retrieval failed: {e}")
            return []

    async def _get_all_positions_postgresql(self) -> List[PositionRecord]:
        try:
            positions: List[PositionRecord] = []
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT position_id, symbol, action, units, entry_price, entry_time,
                           stop_loss, take_profit, status, pnl, metadata
                    FROM positions ORDER BY entry_time DESC
                    """
                )
                for row in rows:
                    et = row[5]
                    et = _to_dt(et) if isinstance(et, str) else et
                    meta = row[10]
                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except Exception:
                            meta = {}
                    positions.append(
                        PositionRecord(
                            position_id=row[0],
                            symbol=row[1],
                            action=row[2],
                            units=float(row[3]),
                            entry_price=float(row[4]),
                            entry_time=et,
                            stop_loss=float(row[6]) if row[6] is not None else None,
                            take_profit=float(row[7]) if row[7] is not None else None,
                            status=row[8],
                            pnl=float(row[9]) if row[9] is not None else 0.0,
                            metadata=meta or {},
                        )
                    )
            return positions
        except Exception as e:
            logger.error(f"PostgreSQL all positions retrieval failed: {e}")
            return []

    async def _get_all_positions_memory(self) -> List[PositionRecord]:
        return []

    # -----------------------------------------------------------------------------
    # Journaling
    # -----------------------------------------------------------------------------
    async def log_position_event(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._log_event_sqlite(position_id, event_type, event_data)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._log_event_postgresql(position_id, event_type, event_data)
            if self.config.storage_type == StorageType.MEMORY:
                return await self._log_event_memory(position_id, event_type, event_data)
            return False
        except Exception as e:
            logger.error(f"Failed to log position event: {e}")
            return False

    async def _log_event_sqlite(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                await conn.execute(
                    """
                    INSERT INTO position_journal (position_id, event_type, event_data)
                    VALUES (?, ?, ?)
                    """,
                    (position_id, event_type, json.dumps(event_data)),
                )
                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"SQLite event logging failed: {e}")
            return False

    async def _log_event_postgresql(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO position_journal (position_id, event_type, event_data)
                    VALUES ($1, $2, $3)
                    """,
                    position_id,
                    event_type,
                    json.dumps(event_data),
                )
                return True
        except Exception as e:
            logger.error(f"PostgreSQL event logging failed: {e}")
            return False

    async def _log_event_memory(self, position_id: str, event_type: str, event_data: Dict[str, Any]) -> bool:
        return True

    async def get_position_journal(self, position_id: str) -> List[Dict[str, Any]]:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_journal_sqlite(position_id)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_journal_postgresql(position_id)
            if self.config.storage_type == StorageType.MEMORY:
                return await self._get_journal_memory(position_id)
            return []
        except Exception as e:
            logger.error(f"Failed to get position journal: {e}")
            return []

    async def _get_journal_sqlite(self, position_id: str) -> List[Dict[str, Any]]:
        try:
            journal: List[Dict[str, Any]] = []
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute(
                    """
                    SELECT event_type, event_data, timestamp
                    FROM position_journal
                    WHERE position_id = ?
                    ORDER BY timestamp DESC
                    """,
                    (position_id,),
                ) as cursor:
                    async for row in cursor:
                        journal.append(
                            {
                                "event_type": row[0],
                                "event_data": json.loads(row[1]) if row[1] else {},
                                "timestamp": row[2],
                            }
                        )
            return journal
        except Exception as e:
            logger.error(f"SQLite journal retrieval failed: {e}")
            return []

    async def _get_journal_postgresql(self, position_id: str) -> List[Dict[str, Any]]:
        try:
            journal: List[Dict[str, Any]] = []
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT event_type, event_data, timestamp
                    FROM position_journal
                    WHERE position_id = $1
                    ORDER BY timestamp DESC
                    """,
                    position_id,
                )
                for row in rows:
                    journal.append(
                        {
                            "event_type": row[0],
                            "event_data": row[1] if row[1] else {},
                            "timestamp": row[2].isoformat() if row[2] else None,
                        }
                    )
            return journal
        except Exception as e:
            logger.error(f"PostgreSQL journal retrieval failed: {e}")
            return []

    async def _get_journal_memory(self, position_id: str) -> List[Dict[str, Any]]:
        return []

    # -----------------------------------------------------------------------------
    # Backups
    # -----------------------------------------------------------------------------
    async def create_backup(self, backup_type: BackupType, metadata: Dict[str, Any] | None = None) -> Optional[BackupInfo]:
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
                size_bytes = backup_path.stat().st_size if backup_path.exists() else 0
                backup_info = BackupInfo(
                    backup_id=backup_id,
                    backup_type=backup_type,
                    timestamp=datetime.now(timezone.utc),
                    size_bytes=size_bytes,
                    file_path=str(backup_path),
                    status="completed",
                    metadata=metadata or {},
                )
                await self._save_backup_record(backup_info)
                await self._cleanup_old_backups()
                logger.info(f"✅ Backup created: {backup_id} ({size_bytes} bytes)")
                return backup_info

            return None
        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            return None

    async def _backup_sqlite(self, backup_path: Path) -> bool:
        try:
            import shutil

            shutil.copy2(self.config.connection_string, backup_path)
            return True
        except Exception as e:
            logger.error(f"SQLite backup failed: {e}")
            return False

    async def _backup_postgresql(self, backup_path: Path) -> bool:
        # Placeholder: real implementation would call pg_dump
        return True

    async def _backup_memory(self, backup_path: Path) -> bool:
        return True

    async def _save_backup_record(self, backup_info: BackupInfo):
        try:
            if self.config.storage_type == StorageType.SQLITE:
                async with aiosqlite.connect(self.config.connection_string) as conn:
                    await conn.execute(
                        """
                        INSERT INTO backups
                        (backup_id, backup_type, timestamp, size_bytes, file_path, status, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            backup_info.backup_id,
                            backup_info.backup_type.value,
                            backup_info.timestamp.isoformat(),
                            backup_info.size_bytes,
                            backup_info.file_path,
                            backup_info.status,
                            json.dumps(backup_info.metadata),
                        ),
                    )
                    await conn.commit()
        except Exception as e:
            logger.error(f"Failed to save backup record: {e}")

    async def _cleanup_old_backups(self):
        try:
            backup_files = list(self.backup_dir.glob("*.db"))
            if len(backup_files) <= self.max_backups:
                return
            backup_files.sort(key=lambda x: x.stat().st_mtime)
            for file_path in backup_files[:-self.max_backups]:
                try:
                    file_path.unlink()
                    logger.info(f"🗑️ Removed old backup: {file_path.name}")
                except Exception as e:
                    logger.warning(f"Failed to remove old backup {file_path.name}: {e}")
        except Exception as e:
            logger.error(f"Backup cleanup failed: {e}")

    async def get_backup_info(self, backup_id: str) -> Optional[BackupInfo]:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_backup_info_sqlite(backup_id)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_backup_info_postgresql(backup_id)
            if self.config.storage_type == StorageType.MEMORY:
                return await self._get_backup_info_memory(backup_id)
            return None
        except Exception as e:
            logger.error(f"Failed to get backup info: {e}")
            return None

    async def _get_backup_info_sqlite(self, backup_id: str) -> Optional[BackupInfo]:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                async with conn.execute(
                    """
                    SELECT backup_id, backup_type, timestamp, size_bytes, file_path, status, metadata
                    FROM backups WHERE backup_id = ?
                    """,
                    (backup_id,),
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return BackupInfo(
                            backup_id=row[0],
                            backup_type=BackupType(row[1]),
                            timestamp=datetime.fromisoformat(row[2]),
                            size_bytes=row[3],
                            file_path=row[4],
                            status=row[5],
                            metadata=json.loads(row[6]) if row[6] else {},
                        )
                    return None
        except Exception as e:
            logger.error(f"SQLite backup info retrieval failed: {e}")
            return None

    async def _get_backup_info_postgresql(self, backup_id: str) -> Optional[BackupInfo]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT backup_id, backup_type, timestamp, size_bytes, file_path, status, metadata
                    FROM backups WHERE backup_id = $1
                    """,
                    backup_id,
                )
                if row:
                    ts = row[2]
                    if isinstance(ts, str):
                        try:
                            ts = datetime.fromisoformat(ts)
                        except Exception:
                            ts = datetime.now(timezone.utc)
                    meta = row[6]
                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except Exception:
                            meta = {}
                    return BackupInfo(
                        backup_id=row[0],
                        backup_type=BackupType(row[1]),
                        timestamp=ts,
                        size_bytes=row[3],
                        file_path=row[4],
                        status=row[5],
                        metadata=meta or {},
                    )
                return None
        except Exception as e:
            logger.error(f"PostgreSQL backup info retrieval failed: {e}")
            return None

    async def _get_backup_info_memory(self, backup_id: str) -> Optional[BackupInfo]:
        return None

    # -----------------------------------------------------------------------------
    # Extra helpers
    # -----------------------------------------------------------------------------
    async def get_positions_by_symbol(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return raw dict rows for convenience APIs."""
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_positions_by_symbol_sqlite(symbol, status)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_positions_by_symbol_postgresql(symbol, status)
            return []
        except Exception as e:
            logger.error(f"Error getting positions by symbol: {e}")
            return []

    async def _get_positions_by_symbol_sqlite(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                conn.row_factory = aiosqlite.Row
                if status:
                    async with conn.execute(
                        "SELECT * FROM positions WHERE symbol = ? AND status = ? ORDER BY entry_time DESC",
                        (symbol, status),
                    ) as cursor:
                        rows = await cursor.fetchall()
                else:
                    async with conn.execute(
                        "SELECT * FROM positions WHERE symbol = ? ORDER BY entry_time DESC",
                        (symbol,),
                    ) as cursor:
                        rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"SQLite get_positions_by_symbol failed: {e}")
            return []

    async def _get_positions_by_symbol_postgresql(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
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
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._get_position_info_sqlite(position_id)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._get_position_info_postgresql(position_id)
            return None
        except Exception as e:
            logger.error(f"Error getting position info: {e}")
            return None

    async def _get_position_info_sqlite(self, position_id: str) -> Optional[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute("SELECT * FROM positions WHERE position_id = ?", (position_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"SQLite get_position_info failed: {e}")
            return None

    async def _get_position_info_postgresql(self, position_id: str) -> Optional[Dict[str, Any]]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM positions WHERE position_id = $1", position_id)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"PostgreSQL get_position_info failed: {e}")
            return None

    async def update_position(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        try:
            if not self.connected:
                await self.connect()

            if self.config.storage_type == StorageType.SQLITE:
                return await self._update_position_sqlite(position_id, position_data)
            if self.config.storage_type == StorageType.POSTGRESQL:
                return await self._update_position_postgresql(position_id, position_data)
            return False
        except Exception as e:
            logger.error(f"Error updating position: {e}")
            return False

    async def _update_position_sqlite(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        try:
            async with aiosqlite.connect(self.config.connection_string) as conn:
                set_keys = [k for k in position_data.keys() if k != "position_id"]
                if not set_keys:
                    return True
                set_clause = ", ".join([f"{k} = ?" for k in set_keys])
                values = [position_data[k] for k in set_keys]
                values.append(position_id)
                query = f"UPDATE positions SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE position_id = ?"
                async with conn.execute(query, values) as cursor:
                    await conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"SQLite update_position failed: {e}")
            return False

    async def _update_position_postgresql(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        try:
            async with self.pool.acquire() as conn:
                set_keys = [k for k in position_data.keys() if k != "position_id"]
                if not set_keys:
                    return True
                set_clause = ", ".join([f"{k} = ${i+1}" for i, k in enumerate(set_keys)])
                values = [position_data[k] for k in set_keys] + [position_id]
                query = f"UPDATE positions SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE position_id = ${len(values)}"
                await conn.execute(query, *values)
                return True
        except Exception as e:
            logger.error(f"PostgreSQL update_position failed: {e}")
            return False

    # -----------------------------------------------------------------------------
    # Stats
    # -----------------------------------------------------------------------------
    async def get_storage_stats(self) -> Dict[str, Any]:
        try:
            if not self.connected:
                await self.connect()

            stats = {
                "storage_type": self.config.storage_type.value,
                "connected": self.connected,
                "backup_count": 0,
                "position_count": 0,
                "journal_entries": 0,
            }

            if self.config.storage_type == StorageType.SQLITE:
                async with aiosqlite.connect(self.config.connection_string) as conn:
                    async with conn.execute("SELECT COUNT(*) FROM positions") as c1:
                        row = await c1.fetchone()
                        stats["position_count"] = row[0] if row else 0

                    async with conn.execute("SELECT COUNT(*) FROM position_journal") as c2:
                        row = await c2.fetchone()
                        stats["journal_entries"] = row[0] if row else 0

                    async with conn.execute("SELECT COUNT(*) FROM backups") as c3:
                        row = await c3.fetchone()
                        stats["backup_count"] = row[0] if row else 0

            return stats
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {"error": str(e)}

    # -----------------------------------------------------------------------------
    # Convenience recorders (journal)
    # -----------------------------------------------------------------------------
    async def record_position_entry(
        self,
        position_id: str,
        symbol: str,
        action: str,
        timeframe: str,
        entry_price: float,
        size: float,
        strategy: str,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ):
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
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await self.log_position_event(position_id, "ENTRY", entry_data)
            logger.info(f"✅ Recorded position entry for {position_id}")
        except Exception as e:
            logger.error(f"Error recording position entry for {position_id}: {e}")

    async def record_position_exit(
        self,
        position_id: str,
        exit_price: float,
        exit_reason: str,
        pnl: float = 0.0,
    ):
        try:
            exit_data = {
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "pnl": pnl,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await self.log_position_event(position_id, "EXIT", exit_data)
            logger.info(f"✅ Recorded position exit for {position_id}")
        except Exception as e:
            logger.error(f"Error recording position exit for {position_id}: {e}")


# Factory function
def create_unified_storage(config: DatabaseConfig | None = None):
    return UnifiedStorage(config=config)
