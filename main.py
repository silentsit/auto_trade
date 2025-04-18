##############################################################################
# Enhanced Trading System
# 
# An institutional-grade trading platform with advanced risk management,
# machine learning capabilities, and comprehensive market analysis.
##############################################################################

import os
import sys
import json
import time
import uuid
import math
import random
import logging
import asyncio
import traceback
import statistics
import glob
import tarfile
import re
import asyncio
import asyncpg  # Add this line
import aiosqlite  # Keep this as we'll need it for the transition
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple, Optional, Any, Callable, Union
from decimal import Decimal
from contextlib import asynccontextmanager

import numpy as np
import pandas as pd
from pydantic import BaseModel, validator, ValidationError
from fastapi import FastAPI, Request, Depends, BackgroundTasks, Query, status, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_429_TOO_MANY_REQUESTS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trading_system.log"),
    ]
)

logger = logging.getLogger("trading_system")

##############################################################################
# Configuration Management
##############################################################################

class Config(BaseModel):
    """Configuration settings for the application"""
    # API and connection settings
    host: str = os.environ.get("HOST", "0.0.0.0")
    port: int = int(os.environ.get("PORT", 8000))
    allowed_origins: str = os.environ.get("ALLOWED_ORIGINS", "*")
    environment: str = os.environ.get("ENVIRONMENT", "production")
    connect_timeout: int = int(os.environ.get("CONNECT_TIMEOUT", 10))
    read_timeout: int = int(os.environ.get("READ_TIMEOUT", 30))
    
    # Trading settings
    oanda_account: str = os.environ.get("OANDA_ACCOUNT", "")
    oanda_token: str = os.environ.get("OANDA_TOKEN", "")
    oanda_environment: str = os.environ.get("OANDA_ENVIRONMENT", "practice")
    active_exchange: str = os.environ.get("ACTIVE_EXCHANGE", "oanda")
    
    # Risk parameters
    default_risk_percentage: float = float(os.environ.get("DEFAULT_RISK_PERCENTAGE", 2.0))
    max_risk_percentage: float = float(os.environ.get("MAX_RISK_PERCENTAGE", 5.0))
    max_portfolio_heat: float = float(os.environ.get("MAX_PORTFOLIO_HEAT", 15.0))
    max_daily_loss: float = float(os.environ.get("MAX_DAILY_LOSS", 5.0))
    
    # Database settings
    database_url: str = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/trading_system")
    db_min_connections: int = int(os.environ.get("DB_MIN_CONNECTIONS", 5))
    db_max_connections: int = int(os.environ.get("DB_MAX_CONNECTIONS", 20))

    # Backup settings
    backup_dir: str = os.environ.get("BACKUP_DIR", "./backups")
    backup_interval_hours: int = int(os.environ.get("BACKUP_INTERVAL_HOURS", 24))
    
    # Notification settings
    slack_webhook_url: Optional[str] = os.environ.get("SLACK_WEBHOOK_URL")
    telegram_bot_token: Optional[str] = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id: Optional[str] = os.environ.get("TELEGRAM_CHAT_ID")
    
    def dict(self):
        """Return dictionary representation with sensitive data removed"""
        result = super().dict()
        # Mask sensitive data
        for key in ["oanda_token", "slack_webhook_url", "telegram_bot_token"]:
            if result[key]:
                result[key] = "******"
        return result

# Initialize config
config = Config()

# Constants
MAX_DAILY_LOSS = config.max_daily_loss / 100  # Convert percentage to decimal
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 20

# Field mapping for TradingView webhook format
TV_FIELD_MAP = {
    "symbol": "ticker",
    "action": "action",
    "timeframe": "timeframe",
    "percentage": "percentage",
    "price": "price",
    "comment": "comment",
    "id": "alert_id"
}

##############################################################################
# Database Models
##############################################################################

class PostgresDatabaseManager:
    def __init__(self, db_url: str = config.database_url, 
                 min_connections: int = config.db_min_connections,
                 max_connections: int = config.db_max_connections):
        """Initialize PostgreSQL database manager"""
        self.db_url = db_url
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.pool = None
        self.logger = logging.getLogger("postgres_manager")

    async def initialize(self):
        """Initialize connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=60.0,
                timeout=10.0
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
                await conn.execute('''
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
                ''')

                # Create indexes for common query patterns
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)')
                
                self.logger.info("PostgreSQL database tables created or verified")
        except Exception as e:
            self.logger.error(f"Error creating database tables: {str(e)}")
            raise

    async def save_position(self, position_data: Dict[str, Any]) -> bool:
        """Save position to database"""
        try:
            # Process metadata to ensure it's in the right format for PostgreSQL
            position_data = position_data.copy()  # Create a copy to avoid modifying the original
            
            # Convert metadata to JSON if it exists and is a dict
            if "metadata" in position_data and isinstance(position_data["metadata"], dict):
                position_data["metadata"] = json.dumps(position_data["metadata"])
            
            # Convert datetime strings to datetime objects if needed
            for field in ["open_time", "close_time", "last_update"]:
                if field in position_data and isinstance(position_data[field], str):
                    try:
                        position_data[field] = datetime.fromisoformat(position_data[field].replace('Z', '+00:00'))
                    except ValueError:
                        # Keep as string if datetime parsing fails
                        pass
            
            async with self.pool.acquire() as conn:
                # Check if position already exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM positions WHERE position_id = $1",
                    position_data["position_id"]
                )
                
                if exists:
                    # Update existing position
                    return await self.update_position(position_data["position_id"], position_data)
                
                # Build the INSERT query dynamically
                columns = list(position_data.keys())
                placeholders = [f"${i+1}" for i in range(len(columns))]
                
                query = f"""
                INSERT INTO positions ({', '.join(columns)}) 
                VALUES ({', '.join(placeholders)})
                """
                
                values = [position_data[col] for col in columns]
                await conn.execute(query, *values)
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving position to database: {str(e)}")
            return False

    async def update_position(self, position_id: str, updates: Dict[str, Any]) -> bool:
        """Update position in database"""
        try:
            # Process updates to ensure compatibility with PostgreSQL
            updates = updates.copy()  # Create a copy to avoid modifying the original
            
            # Convert metadata to JSON if it exists and is a dict
            if "metadata" in updates and isinstance(updates["metadata"], dict):
                updates["metadata"] = json.dumps(updates["metadata"])
            
            # Convert datetime strings to datetime objects if needed
            for field in ["open_time", "close_time", "last_update"]:
                if field in updates and isinstance(updates[field], str):
                    try:
                        updates[field] = datetime.fromisoformat(updates[field].replace('Z', '+00:00'))
                    except ValueError:
                        # Keep as string if datetime parsing fails
                        pass
            
            async with self.pool.acquire() as conn:
                # Prepare the SET clause and values
                set_items = []
                values = []
                
                for i, (key, value) in enumerate(updates.items(), start=1):
                    set_items.append(f"{key} = ${i}")
                    values.append(value)
                
                # Add position_id as the last parameter
                values.append(position_id)
                
                query = f"""
                UPDATE positions 
                SET {', '.join(set_items)} 
                WHERE position_id = ${len(values)}
                """
                
                await conn.execute(query, *values)
                return True
                
        except Exception as e:
            self.logger.error(f"Error updating position in database: {str(e)}")
            return False

    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM positions WHERE position_id = $1",
                    position_id
                )
                
                if not row:
                    return None
                
                # Convert row to dictionary
                position_data = dict(row)
                
                # Parse metadata JSON if it exists
                if "metadata" in position_data and position_data["metadata"]:
                    try:
                        if isinstance(position_data["metadata"], str):
                            position_data["metadata"] = json.loads(position_data["metadata"])
                    except json.JSONDecodeError:
                        # If parsing fails, keep as string
                        pass
                
                # Convert timestamp objects to ISO format strings for consistency
                for field in ["open_time", "close_time", "last_update"]:
                    if position_data.get(field) and isinstance(position_data[field], datetime):
                        position_data[field] = position_data[field].isoformat()
                
                return position_data
                
        except Exception as e:
            self.logger.error(f"Error getting position from database: {str(e)}")
            return None

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM positions WHERE status = 'open' ORDER BY open_time DESC"
                )
                
                if not rows:
                    return []
                
                positions = []
                for row in rows:
                    # Convert row to dictionary
                    position_data = dict(row)
                    
                    # Parse metadata JSON if it exists
                    if "metadata" in position_data and position_data["metadata"]:
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(position_data["metadata"])
                        except json.JSONDecodeError:
                            # If parsing fails, keep as string
                            pass
                    
                    # Convert timestamp objects to ISO format strings
                    for field in ["open_time", "close_time", "last_update"]:
                        if position_data.get(field) and isinstance(position_data[field], datetime):
                            position_data[field] = position_data[field].isoformat()
                    
                    positions.append(position_data)
                
                return positions
                
        except Exception as e:
            self.logger.error(f"Error getting open positions from database: {str(e)}")
            return []

    async def get_closed_positions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get closed positions with limit"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM positions WHERE status = 'closed' ORDER BY close_time DESC LIMIT $1",
                    limit
                )
                
                if not rows:
                    return []
                
                positions = []
                for row in rows:
                    # Convert row to dictionary
                    position_data = dict(row)
                    
                    # Parse metadata JSON if it exists
                    if "metadata" in position_data and position_data["metadata"]:
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(position_data["metadata"])
                        except json.JSONDecodeError:
                            # If parsing fails, keep as string
                            pass
                    
                    # Convert timestamp objects to ISO format strings
                    for field in ["open_time", "close_time", "last_update"]:
                        if position_data.get(field) and isinstance(position_data[field], datetime):
                            position_data[field] = position_data[field].isoformat()
                    
                    positions.append(position_data)
                
                return positions
                
        except Exception as e:
            self.logger.error(f"Error getting closed positions from database: {str(e)}")
            return []

    async def delete_position(self, position_id: str) -> bool:
        """Delete position from database"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM positions WHERE position_id = $1",
                    position_id
                )
                return True
                
        except Exception as e:
            self.logger.error(f"Error deleting position from database: {str(e)}")
            return False
            
    async def get_positions_by_symbol(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions for a specific symbol"""
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT * FROM positions WHERE symbol = $1"
                params = [symbol]
                
                if status:
                    query += " AND status = $2"
                    params.append(status)
                    
                query += " ORDER BY open_time DESC"
                
                rows = await conn.fetch(query, *params)
                
                if not rows:
                    return []
                
                positions = []
                for row in rows:
                    # Convert row to dictionary
                    position_data = dict(row)
                    
                    # Parse metadata JSON if it exists
                    if "metadata" in position_data and position_data["metadata"]:
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(position_data["metadata"])
                        except json.JSONDecodeError:
                            # If parsing fails, keep as string
                            pass
                    
                    # Convert timestamp objects to ISO format strings
                    for field in ["open_time", "close_time", "last_update"]:
                        if position_data.get(field) and isinstance(position_data[field], datetime):
                            position_data[field] = position_data[field].isoformat()
                    
                    positions.append(position_data)
                
                return positions
                
        except Exception as e:
            self.logger.error(f"Error getting positions by symbol from database: {str(e)}")
            return []

    async def backup_database(self, backup_path: str) -> bool:
        """Create a backup of the database"""
        try:
            # PostgreSQL backup requires pg_dump, which is a system command
            # We'll implement this using subprocess to run pg_dump
            import subprocess
            import shlex
            
            # Parse database URL to get credentials
            if self.db_url.startswith('postgresql://'):
                # Extract connection parameters from URL
                db_params = {}
                connection_string = self.db_url.replace('postgresql://', '')
                auth_part, connection_part = connection_string.split('@', 1)
                
                if ':' in auth_part:
                    db_params['username'], db_params['password'] = auth_part.split(':', 1)
                else:
                    db_params['username'] = auth_part
                    db_params['password'] = None
                
                host_port, db_name = connection_part.split('/', 1)
                
                if ':' in host_port:
                    db_params['host'], db_params['port'] = host_port.split(':', 1)
                else:
                    db_params['host'] = host_port
                    db_params['port'] = '5432'
                
                db_params['dbname'] = db_name.split('?')[0]  # Remove query parameters if any
                
                # Build pg_dump command
                cmd = [
                    'pg_dump',
                    f"--host={db_params['host']}",
                    f"--port={db_params['port']}",
                    f"--username={db_params['username']}",
                    f"--dbname={db_params['dbname']}",
                    '--format=custom',
                    f"--file={backup_path}"
                ]
                
                # Set password environment variable for pg_dump
                env = os.environ.copy()
                if db_params['password']:
                    env['PGPASSWORD'] = db_params['password']
                
                # Execute pg_dump
                result = subprocess.run(cmd, env=env, capture_output=True, text=True)
                
                if result.returncode == 0:
                    self.logger.info(f"Database backup created at {backup_path}")
                    return True
                else:
                    self.logger.error(f"pg_dump failed: {result.stderr}")
                    return False
            else:
                self.logger.error("Database URL is not in the expected format")
                return False
                
        except Exception as e:
            self.logger.error(f"Error backing up database: {str(e)}")
            return False

async def restore_from_backup(backup_path: str) -> bool:
    """Restore database from a PostgreSQL backup file"""
    try:
        import subprocess
        import shlex
        
        # Parse database URL to get credentials
        if db_manager.db_url.startswith('postgresql://'):
            # Extract connection parameters from URL
            db_params = {}
            connection_string = db_manager.db_url.replace('postgresql://', '')
            auth_part, connection_part = connection_string.split('@', 1)
            
            if ':' in auth_part:
                db_params['username'], db_params['password'] = auth_part.split(':', 1)
            else:
                db_params['username'] = auth_part
                db_params['password'] = None
            
            host_port, db_name = connection_part.split('/', 1)
            
            if ':' in host_port:
                db_params['host'], db_params['port'] = host_port.split(':', 1)
            else:
                db_params['host'] = host_port
                db_params['port'] = '5432'
            
            db_params['dbname'] = db_name.split('?')[0]  # Remove query parameters if any
            
            # Build pg_restore command
            cmd = [
                'pg_restore',
                f"--host={db_params['host']}",
                f"--port={db_params['port']}",
                f"--username={db_params['username']}",
                f"--dbname={db_params['dbname']}",
                '--clean',  # Clean (drop) database objects before recreating
                '--no-owner',  # Do not set ownership of objects to match the original database
                backup_path
            ]
            
            # Set password environment variable for pg_restore
            env = os.environ.copy()
            if db_params['password']:
                env['PGPASSWORD'] = db_params['password']
            
            # Execute pg_restore
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Database restored from {backup_path}")
                return True
            else:
                logger.error(f"pg_restore failed: {result.stderr}")
                return False
        else:
            logger.error("Database URL is not in the expected format")
            return False
            
    except Exception as e:
        logger.error(f"Error restoring database from backup: {str(e)}")
        return False

##############################################################################
# Exception Handling & Error Recovery
##############################################################################

class TradingSystemError(Exception):
    """Base exception for trading system errors"""
    pass

class BrokerConnectionError(TradingSystemError):
    """Error connecting to broker API"""
    pass

class MarketClosedError(TradingSystemError):
    """Market is closed for trading"""
    pass

class OrderExecutionError(TradingSystemError):
    """Error executing order"""
    pass

class PositionNotFoundError(TradingSystemError):
    """Position not found"""
    pass

class SessionError(TradingSystemError):
    """Session-related error"""
    pass

class RateLimitError(TradingSystemError):
    """API rate limit exceeded"""
    pass

class InsufficientDataError(TradingSystemError):
    """Insufficient data for calculations"""
    pass

def async_error_handler(max_retries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY):
    """Decorator for handling errors in async functions with retry logic"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except (BrokerConnectionError, RateLimitError) as e:
                    retries += 1
                    logger.warning(f"Retry {retries}/{max_retries} for {func.__name__} due to: {str(e)}")
                    
                    if retries >= max_retries:
                        logger.error(f"Max retries ({max_retries}) reached for {func.__name__}")
                        raise
                        
                    # Exponential backoff
                    wait_time = delay * (2 ** (retries - 1))
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                    logger.error(traceback.format_exc())
                    raise
        return wrapper
    return decorator

class ErrorRecoverySystem:
    """
    Comprehensive error handling and recovery system that monitors
    for stalled operations and recovers from system failures.
    """
    def __init__(self):
        """Initialize error recovery system"""
        self.stale_position_threshold = 300  # seconds
        self.circuit_breaker_tripped = False
        self.circuit_breaker_reason = None
        self.circuit_breaker_reset_time = None
        self.max_daily_errors = 20
        self.daily_error_count = 0
        self.last_error_reset = datetime.now(timezone.utc)
        
    async def check_for_stale_positions(self):
        """Check for positions that haven't been updated recently"""
        try:
            current_time = datetime.now(timezone.utc)
            
            # Get all positions if alert_handler is initialized
            if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'position_tracker'):
                positions = await alert_handler.position_tracker.get_open_positions()
                
                for symbol, symbol_positions in positions.items():
                    for position_id, position in symbol_positions.items():
                        # Check last update time
                        last_update = position.get('last_update')
                        if not last_update:
                            continue
                            
                        if isinstance(last_update, str):
                            try:
                                last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                            except ValueError:
                                continue
                                
                        # Calculate time since last update
                        time_diff = (current_time - last_update).total_seconds()
                        
                        # Check if position is stale
                        if time_diff > self.stale_position_threshold:
                            logger.warning(f"Stale position detected: {position_id} (last updated {time_diff} seconds ago)")
                            
                            # Request position refresh
                            await self.recover_position(position_id, position)
            
            # Reset daily error count if needed
            if (current_time - self.last_error_reset).total_seconds() > 86400:  # 24 hours
                self.daily_error_count = 0
                self.last_error_reset = current_time
                
        except Exception as e:
            logger.error(f"Error checking for stale positions: {str(e)}")
            
    async def recover_position(self, position_id: str, position_data: Dict[str, Any]):
        """Attempt to recover a stale position"""
        try:
            symbol = position_data.get('symbol')
            if not symbol:
                logger.error(f"Cannot recover position {position_id}: Missing symbol")
                return
                
            # Get current price
            current_price = await get_current_price(symbol, position_data.get('action', 'BUY'))
            
            # Update position with current price
            if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'position_tracker'):
                await alert_handler.position_tracker.update_position_price(
                    position_id=position_id,
                    current_price=current_price
                )
                
                logger.info(f"Recovered position {position_id} with updated price: {current_price}")
                
        except Exception as e:
            logger.error(f"Error recovering position {position_id}: {str(e)}")
            
    async def record_error(self, error_type: str, details: Dict[str, Any]):
        """Record an error and check if circuit breaker should be tripped"""
        self.daily_error_count += 1
        
        # Trip circuit breaker if error threshold exceeded
        if self.daily_error_count >= self.max_daily_errors:
            await self.trip_circuit_breaker(f"Error threshold exceeded: {self.daily_error_count} errors")
            
        # Log the error
        logger.error(f"Error recorded: {error_type} - {json.dumps(details)}")
        
    async def trip_circuit_breaker(self, reason: str):
        """Trip the circuit breaker to pause trading operations"""
        if not self.circuit_breaker_tripped:
            self.circuit_breaker_tripped = True
            self.circuit_breaker_reason = reason
            self.circuit_breaker_reset_time = datetime.now(timezone.utc) + timedelta(hours=1)
            
            logger.critical(f"CIRCUIT BREAKER TRIPPED: {reason}. Trading paused until {self.circuit_breaker_reset_time.isoformat()}")
            
            # Notify if configured
            if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'notification_system'):
                await alert_handler.notification_system.send_notification(
                    f"CIRCUIT BREAKER TRIPPED: {reason}. Trading paused.",
                    "critical"
                )
                
    async def reset_circuit_breaker(self):
        """Reset the circuit breaker"""
        if self.circuit_breaker_tripped:
            self.circuit_breaker_tripped = False
            self.circuit_breaker_reason = None
            self.circuit_breaker_reset_time = None
            
            logger.info("Circuit breaker reset. Trading operations resumed.")
            
            # Notify if configured
            if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'notification_system'):
                await alert_handler.notification_system.send_notification(
                    "Circuit breaker reset. Trading operations resumed.",
                    "info"
                )
                
    async def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status"""
        # Auto-reset if reset time has passed
        if (self.circuit_breaker_tripped and 
            self.circuit_breaker_reset_time and 
            datetime.now(timezone.utc) >= self.circuit_breaker_reset_time):
            await self.reset_circuit_breaker()
            
        return {
            "tripped": self.circuit_breaker_tripped,
            "reason": self.circuit_breaker_reason,
            "reset_time": self.circuit_breaker_reset_time.isoformat() if self.circuit_breaker_reset_time else None,
            "daily_error_count": self.daily_error_count
        }
        
    async def schedule_stale_position_check(self, interval_seconds: int = 60):
        """Schedule regular checks for stale positions"""
        while True:
            try:
                await self.check_for_stale_positions()
            except Exception as e:
                logger.error(f"Error in scheduled stale position check: {str(e)}")
                
            await asyncio.sleep(interval_seconds)

##############################################################################
# Session & API Management
##############################################################################

# Sessions dictionary to track active API sessions
active_sessions = {}

async def get_session():
    """Get or create an aiohttp session for API requests"""
    import aiohttp
    
    session_key = "default"
    
    # Check if session exists and is not closed
    if session_key in active_sessions and not active_sessions[session_key].closed:
        return active_sessions[session_key]
        
    # Create new session
    timeout = aiohttp.ClientTimeout(
        connect=config.connect_timeout,
        total=config.read_timeout
    )
    
    session = aiohttp.ClientSession(timeout=timeout)
    active_sessions[session_key] = session
    
    logger.debug(f"Created new aiohttp session")
    return session

async def cleanup_stale_sessions():
    """Close and clean up stale sessions"""
    for key, session in list(active_sessions.items()):
        try:
            if not session.closed:
                await session.close()
            del active_sessions[key]
        except Exception as e:
            logger.error(f"Error closing session {key}: {str(e)}")

##############################################################################
# Market Data Functions
##############################################################################

async def get_current_price(symbol: str, side: str = "BUY") -> float:
    """Get current market price for a symbol"""
    try:
        # This would be implemented to connect to your broker's API
        # For now, return a simulated price
        import random
        base_price = 100.0
        
        # Adjust for symbol
        if symbol == "EUR_USD":
            base_price = 1.10
        elif symbol == "GBP_USD":
            base_price = 1.25
        elif symbol == "USD_JPY":
            base_price = 110.0
        elif symbol == "XAU_USD":
            base_price = 1900.0
            
        # Add small random variation
        price = base_price * (1 + random.uniform(-0.001, 0.001))
        
        # Adjust for side (bid/ask spread)
        if side.upper() == "BUY":
            price *= 1.0001  # Ask price
        else:
            price *= 0.9999  # Bid price
            
        return price
    except Exception as e:
        logger.error(f"Error getting price for {symbol}: {str(e)}")
        raise

@async_error_handler()
async def get_historical_data(symbol: str, timeframe: str, count: int = 100) -> Dict[str, Any]:
    """Get historical price data for a symbol"""
    try:
        # This would be implemented to connect to your broker's API
        # For now, return simulated data
        candles = []
        current_price = await get_current_price(symbol)
        
        # Generate simulated candles
        end_time = datetime.now(timezone.utc)
        
        # Determine candle interval in seconds
        interval_seconds = 3600  # Default to 1H
        if timeframe == "M1":
            interval_seconds = 60
        elif timeframe == "M5":
            interval_seconds = 300
        elif timeframe == "M15":
            interval_seconds = 900
        elif timeframe == "M30":
            interval_seconds = 1800
        elif timeframe == "H1":
            interval_seconds = 3600
        elif timeframe == "H4":
            interval_seconds = 14400
        elif timeframe == "D1":
            interval_seconds = 86400
            
        # Generate candles
        for i in range(count):
            candle_time = end_time - timedelta(seconds=interval_seconds * (count - i))
            
            # Generate price with some randomness but trending
            price_change = 0.001 * (0.5 - random.random())
            open_price = current_price * (1 + price_change * (count - i) / 10)
            close_price = open_price * (1 + price_change)
            high_price = max(open_price, close_price) * (1 + abs(price_change) * 0.5)
            low_price = min(open_price, close_price) * (1 - abs(price_change) * 0.5)
            volume = random.randint(10, 100)
            
            candle = {
                "time": candle_time.isoformat(),
                "mid": {
                    "o": str(round(open_price, 5)),
                    "h": str(round(high_price, 5)),
                    "l": str(round(low_price, 5)),
                    "c": str(round(close_price, 5))
                },
                "volume": volume,
                "complete": True
            }
            
            candles.append(candle)
            
        return {"candles": candles}
    except Exception as e:
        logger.error(f"Error getting historical data for {symbol}: {str(e)}")
        raise

async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    """Calculate Average True Range (ATR) for a symbol"""
    try:
        # Get historical data
        data = await get_historical_data(symbol, timeframe, period + 10)
        
        if "candles" not in data or len(data["candles"]) < period + 1:
            logger.warning(f"Insufficient data to calculate ATR for {symbol}")
            return 0.0
            
        # Calculate true ranges
        tr_values = []
        
        for i in range(1, len(data["candles"])):
            current = data["candles"][i]
            previous = data["candles"][i-1]
            
            high = float(current["mid"]["h"])
            low = float(current["mid"]["l"])
            prev_close = float(previous["mid"]["c"])
            
            # True Range is the greatest of:
            # 1. High - Low
            # 2. |High - Previous Close|
            # 3. |Low - Previous Close|
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            
            tr_values.append(tr)
            
        # Calculate ATR as average of last 'period' true ranges
        if len(tr_values) >= period:
            atr = sum(tr_values[-period:]) / period
            return atr
        else:
            return 0.0
    except Exception as e:
        logger.error(f"Error calculating ATR for {symbol}: {str(e)}")
        return 0.0

def get_instrument_type(symbol: str) -> str:
    """Determine instrument type from symbol"""
    if "_" not in symbol:
        return "other"
        
    # Extract base and quote currencies
    parts = symbol.split("_")
    
    # Check for common FX pairs
    if len(parts) == 2 and len(parts[0]) == 3 and len(parts[1]) == 3:
        # Check for specific types
        if parts[0] in ["XAU", "XAG", "XPT"]:
            return "metal"
        elif parts[1] == "JPY":
            return "jpy_pair"
        else:
            return "forex"
    
    # Check for indices
    if any(index in symbol for index in ["SPX", "NAS", "UK", "JP", "EU"]):
        return "index"
        
    # Default to 'other'
    return "other"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """Get appropriate ATR multiplier based on instrument type and timeframe"""
    # Base multipliers by instrument type
    base_multipliers = {
        "forex": 2.0,
        "jpy_pair": 2.5,
        "metal": 1.5,
        "index": 2.0,
        "other": 2.0
    }
    
    # Timeframe adjustments
    timeframe_factors = {
        "M1": 1.5,
        "M5": 1.3,
        "M15": 1.2,
        "M30": 1.1,
        "H1": 1.0,
        "H4": 0.9,
        "D1": 0.8,
        "W1": 0.7
    }
    
    # Get base multiplier
    base = base_multipliers.get(instrument_type, 2.0)
    
    # Apply timeframe adjustment
    factor = timeframe_factors.get(timeframe, 1.0)
    
    return base * factor

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format based on broker"""
    if config.active_exchange == "oanda":
        # OANDA uses underscore format (EUR_USD)
        return symbol.replace("/", "_").upper()
    elif config.active_exchange == "binance":
        # Binance uses no separator (EURUSD)
        return symbol.replace("_", "").replace("/", "").upper()
    else:
        return symbol.upper()

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable based on market hours"""
    now = datetime.now(timezone.utc)
    
    # Extract instrument type
    instrument_type = get_instrument_type(symbol)
    
    # FX pairs are generally tradeable 24/5
    if instrument_type in ["forex", "jpy_pair", "metal"]:
        # Check if it's weekend
        if now.weekday() >= 5:  # Saturday or Sunday
            return False, "Weekend - Market closed"
            
        # Check for forex market hours (rough approximation)
        if now.weekday() == 4 and now.hour >= 21:  # Friday after 21:00 UTC
            return False, "Weekend - Market closed"
        if now.weekday() == 0 and now.hour < 21:  # Monday before 21:00 UTC
            return False, "Market not yet open"
            
        return True, "Market open"
        
    # Indices have specific trading hours
    if instrument_type == "index":
        # Very rough approximation - implementation would depend on specific indices
        # For US indices
        if "SPX" in symbol or "NAS" in symbol:
            # US trading hours (simplified)
            if now.weekday() >= 5:  # Weekend
                return False, "Weekend - Market closed"
                
            if not (13 <= now.hour < 20):  # Not between 13:00-20:00 UTC
                return False, "Outside market hours"
                
        return True, "Market open"
        
    # Default to tradeable for other instruments
    return True, "Market assumed open"

async def get_account_balance() -> float:
    """Get current account balance"""
    # This would be implemented to connect to your broker's API
    # For now, return a simulated balance
    return 10000.0

@async_error_handler()
async def execute_trade(trade_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade with the broker"""
    try:
        # This would be implemented to connect to your broker's API
        # For now, simulate a successful trade
        symbol = trade_data.get('symbol', '')
        action = trade_data.get('action', '').upper()
        percentage = float(trade_data.get('percentage', 1.0))
        
        # Get account balance
        balance = await get_account_balance()
        
        # Calculate position size (simplified)
        position_size = balance * percentage / 100
        
        # Get current price
        price = await get_current_price(symbol, action)
        
        # Simulate order creation
        order_id = str(uuid.uuid4())
        
        # Simulate successful response
        response = {
            "orderCreateTransaction": {
                "id": order_id,
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "MARKET_ORDER",
                "instrument": symbol,
                "units": str(position_size) if action == "BUY" else str(-position_size)
            },
            "orderFillTransaction": {
                "id": str(uuid.uuid4()),
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "ORDER_FILL",
                "orderID": order_id,
                "instrument": symbol,
                "units": str(position_size) if action == "BUY" else str(-position_size),
                "price": str(price)
            },
            "lastTransactionID": str(uuid.uuid4())
        }
        
        logger.info(f"Executed trade: {action} {symbol} @ {price} ({percentage}%)")
        return True, response
    except Exception as e:
        logger.error(f"Error executing trade: {str(e)}")
        return False, {"error": str(e)}

@async_error_handler()
async def close_position(position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close a position with the broker"""
    try:
        # This would be implemented to connect to your broker's API
        # For now, simulate a successful close
        symbol = position_data.get('symbol', '')
        
        # Get current price
        price = await get_current_price(symbol, "SELL")  # Use sell price for closing
        
        # Simulate successful response
        response = {
            "orderCreateTransaction": {
                "id": str(uuid.uuid4()),
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "MARKET_ORDER",
                "instrument": symbol,
                "units": "0"  # Close all units
            },
            "orderFillTransaction": {
                "id": str(uuid.uuid4()),
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "ORDER_FILL",
                "instrument": symbol,
                "units": "0",
                "price": str(price)
            },
            "lastTransactionID": str(uuid.uuid4())
        }
        
        logger.info(f"Closed position: {symbol} @ {price}")
        return True, response
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")
        return False, {"error": str(e)}

async def initialize_db(db_path="positions.db"):
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                entry_price REAL NOT NULL,
                quantity REAL NOT NULL,
                status TEXT NOT NULL,
                open_time TIMESTAMP NOT NULL,
                close_time TIMESTAMP,
                exit_price REAL,
                profit_loss REAL,
                strategy TEXT,
                stop_loss REAL,
                take_profit REAL,
                metadata TEXT
            )
        ''')
        await db.commit()


##############################################################################
# Position Tracking
##############################################################################

class Position:
    """Represents a trading position with full lifecycle management"""
    
    def __init__(self, 
                position_id: str,
                symbol: str, 
                action: str,
                timeframe: str,
                entry_price: float,
                size: float,
                stop_loss: Optional[float] = None,
                take_profit: Optional[float] = None,
                metadata: Optional[Dict[str, Any]] = None):
        """Initialize a position"""
        self.position_id = position_id
        self.symbol = symbol
        self.action = action.upper()
        self.timeframe = timeframe
        self.entry_price = float(entry_price)
        self.size = float(size)
        self.stop_loss = float(stop_loss) if stop_loss is not None else None
        self.take_profit = float(take_profit) if take_profit is not None else None
        self.open_time = datetime.now(timezone.utc)
        self.close_time = None
        self.exit_price = None
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.status = "open"
        self.last_update = self.open_time
        self.current_price = self.entry_price
        self.metadata = metadata or {}
        self.exit_reason = None
        
    def update_price(self, current_price: float):
        """Update current price and calculate P&L"""
        self.current_price = float(current_price)
        self.last_update = datetime.now(timezone.utc)
        
        # Calculate unrealized P&L
        if self.action == "BUY":
            self.pnl = (self.current_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.current_price) * self.size
            
        # Calculate P&L percentage
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.current_price / self.entry_price - 1) * 100
            else:  # SELL
                self.pnl_percentage = (1 - self.current_price / self.entry_price) * 100
                
    def close(self, exit_price: float, exit_reason: str = "manual"):
        """Close the position"""
        self.exit_price = float(exit_price)
        self.close_time = datetime.now(timezone.utc)
        self.status = "closed"
        self.exit_reason = exit_reason
        
        # Calculate realized P&L
        if self.action == "BUY":
            self.pnl = (self.exit_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.exit_price) * self.size
            
        # Calculate P&L percentage
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.exit_price / self.entry_price - 1) * 100
            else:  # SELL
                self.pnl_percentage = (1 - self.exit_price / self.entry_price) * 100
                
        # Update last update time
        self.last_update = self.close_time
        
    def update_stop_loss(self, new_stop_loss: float):
        """Update stop loss level"""
        self.stop_loss = float(new_stop_loss)
        self.last_update = datetime.now(timezone.utc)
        
    def update_take_profit(self, new_take_profit: float):
        """Update take profit level"""
        self.take_profit = float(new_take_profit)
        self.last_update = datetime.now(timezone.utc)
        
    def update_metadata(self, metadata: Dict[str, Any]):
        """Update position metadata"""
        self.metadata.update(metadata)
        self.last_update = datetime.now(timezone.utc)

class PositionTracker:
    """
    Tracks all positions across different symbols and timeframes,
    providing a centralized registry for position management.
    With database persistence capability.
    """
    def __init__(self, db_manager=None):
        """Initialize position tracker"""
        self.positions = {}  # position_id -> Position
        self.open_positions_by_symbol = {}  # symbol -> {position_id -> Position}
        self.closed_positions = {}  # position_id -> position_data
        self.position_history = []  # list of all positions ever
        self._lock = asyncio.Lock()
        self.max_history = 1000
        self._running = False
        self.db_manager = db_manager
        
    async def start(self):
        """Start position tracker and load positions from database"""
        if self._running:
            return
        
        self._running = True
        
        # Load positions from database if available
        if self.db_manager:
            try:
                # Load open positions
                open_positions = await self.db_manager.get_open_positions()
                for position_data in open_positions:
                    await self.restore_position(position_data["position_id"], position_data)
                
                # Load closed positions (limited to recent ones)
                closed_positions = await self.db_manager.get_closed_positions(limit=1000)
                self.closed_positions = {p["position_id"]: p for p in closed_positions}
                
                # Add to position history for in-memory tracking
                self.position_history = []
                for position_data in open_positions:
                    self.position_history.append(position_data)
                for position_data in closed_positions:
                    self.position_history.append(position_data)
                
                # Sort history by open time
                self.position_history.sort(key=lambda x: x.get("open_time", ""), reverse=True)
                
                # Trim history if needed
                if len(self.position_history) > self.max_history:
                    self.position_history = self.position_history[:self.max_history]
                
                logger.info(f"Position tracker started with {len(open_positions)} open and {len(closed_positions)} closed positions loaded from database")
            except Exception as e:
                logger.error(f"Error loading positions from database: {str(e)}")
                logger.info("Position tracker started with empty position list")
        else:
            logger.info("Position tracker started (database persistence not available)")
        
    async def stop(self):
        """Stop position tracker"""
        if not self._running:
            return
            
        self._running = False
        logger.info("Position tracker stopped")
        
    async def record_position(self,
                            position_id: str,
                            symbol: str,
                            action: str,
                            timeframe: str,
                            entry_price: float,
                            size: float,
                            stop_loss: Optional[float] = None,
                            take_profit: Optional[float] = None,
                            metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Record a new position"""
        async with self._lock:
            # Check if position already exists
            if position_id in self.positions:
                logger.warning(f"Position {position_id} already exists")
                return False
                
            # Limit positions per symbol
            symbol_positions = self.open_positions_by_symbol.get(symbol, {})
            if len(symbol_positions) >= MAX_POSITIONS_PER_SYMBOL:
                logger.warning(f"Maximum positions for {symbol} reached: {MAX_POSITIONS_PER_SYMBOL}")
                return False
                
            # Create position
            position = Position(
                position_id=position_id,
                symbol=symbol,
                action=action,
                timeframe=timeframe,
                entry_price=entry_price,
                size=size,
                stop_loss=stop_loss,
                take_profit=take_profit,
                metadata=metadata
            )
            
            # Store position in memory
            self.positions[position_id] = position
            
            # Index by symbol
            if symbol not in self.open_positions_by_symbol:
                self.open_positions_by_symbol[symbol] = {}
                
            self.open_positions_by_symbol[symbol][position_id] = position
            
            # Add to history
            position_dict = self._position_to_dict(position)
            self.position_history.append(position_dict)
            
            # Trim history if needed
            if len(self.position_history) > self.max_history:
                self.position_history = self.position_history[-self.max_history:]
            
            # Save to database if available
            if self.db_manager:
                try:
                    await self.db_manager.save_position(position_dict)
                except Exception as e:
                    logger.error(f"Error saving position {position_id} to database: {str(e)}")
            
            logger.info(f"Recorded new position: {position_id} ({symbol} {action})")
            return True
            
    async def close_position(self,
                           position_id: str,
                           exit_price: float,
                           reason: str = "manual") -> Tuple[bool, Dict[str, Any]]:
        """Close a position"""
        async with self._lock:
            # Check if position exists
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found")
                return False, {"error": "Position not found"}
                
            # Get position
            position = self.positions[position_id]
            
            # Close position
            position.close(exit_price, reason)
            
            # Move to closed positions
            position_dict = self._position_to_dict(position)
            self.closed_positions[position_id] = position_dict
            
            # Remove from open positions
            symbol = position.symbol
            if symbol in self.open_positions_by_symbol and position_id in self.open_positions_by_symbol[symbol]:
                del self.open_positions_by_symbol[symbol][position_id]
                
                # Clean up empty symbol dictionary
                if not self.open_positions_by_symbol[symbol]:
                    del self.open_positions_by_symbol[symbol]
                    
            # Remove from positions
            del self.positions[position_id]
            
            # Update history
            for i, hist_pos in enumerate(self.position_history):
                if hist_pos.get("position_id") == position_id:
                    self.position_history[i] = position_dict
                    break
            
            # Update database if available
            if self.db_manager:
                try:
                    await self.db_manager.update_position(position_id, position_dict)
                except Exception as e:
                    logger.error(f"Error updating closed position {position_id} in database: {str(e)}")
            
            logger.info(f"Closed position: {position_id} ({symbol} @ {exit_price}, PnL: {position.pnl:.2f})")
            return True, position_dict
            
    async def close_partial_position(self,
                                   position_id: str,
                                   exit_price: float,
                                   percentage: float,
                                   reason: str = "partial") -> Tuple[bool, Dict[str, Any]]:
        """Close a partial position"""
        async with self._lock:
            # Check if position exists
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found")
                return False, {"error": "Position not found"}
                
            # Get position
            position = self.positions[position_id]
            
            # Calculate size to close
            percentage = min(100.0, max(0.1, percentage))  # Ensure between 0.1% and 100%
            close_size = position.size * percentage / 100
            
            # Calculate PnL for closed portion
            if position.action == "BUY":
                closed_pnl = (exit_price - position.entry_price) * close_size
            else:  # SELL
                closed_pnl = (position.entry_price - exit_price) * close_size
                
            # If closing everything, use regular close
            if percentage >= 99.9:
                return await self.close_position(position_id, exit_price, reason)
                
            # Update position size
            new_size = position.size - close_size
            position.size = new_size
            position.last_update = datetime.now(timezone.utc)
            
            # Update any metadata about partial closes
            if "partial_closes" not in position.metadata:
                position.metadata["partial_closes"] = []
                
            position.metadata["partial_closes"].append({
                "time": datetime.now(timezone.utc).isoformat(),
                "price": exit_price,
                "size": close_size,
                "percentage": percentage,
                "pnl": closed_pnl,
                "reason": reason
            })
            
            # Update database if available
            if self.db_manager:
                try:
                    position_dict = self._position_to_dict(position)
                    await self.db_manager.update_position(position_id, position_dict)
                except Exception as e:
                    logger.error(f"Error updating partially closed position {position_id} in database: {str(e)}")
            
            logger.info(f"Closed {percentage:.1f}% of position {position_id} ({position.symbol} @ {exit_price}, PnL: {closed_pnl:.2f})")
            
            # Update position's current price to recalculate PnL
            position.update_price(exit_price)
            
            # Return result
            return True, {
                "position_id": position_id,
                "symbol": position.symbol,
                "closed_size": close_size,
                "remaining_size": new_size,
                "percentage": percentage,
                "closed_pnl": closed_pnl,
                "price": exit_price,
                "reason": reason
            }
            
    async def update_position(self,
                            position_id: str,
                            stop_loss: Optional[float] = None,
                            take_profit: Optional[float] = None,
                            metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Update position parameters"""
        async with self._lock:
            # Check if position exists
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found")
                return False
                
            # Get position
            position = self.positions[position_id]
            
            # Update stop loss if provided
            if stop_loss is not None:
                position.update_stop_loss(stop_loss)
                
            # Update take profit if provided
            if take_profit is not None:
                position.update_take_profit(take_profit)
                
            # Update metadata if provided
            if metadata is not None:
                position.update_metadata(metadata)
                
            # Update history
            position_dict = self._position_to_dict(position)
            for i, hist_pos in enumerate(self.position_history):
                if hist_pos.get("position_id") == position_id:
                    self.position_history[i] = position_dict
                    break
            
            # Update database if available
            if self.db_manager:
                try:
                    await self.db_manager.update_position(position_id, position_dict)
                except Exception as e:
                    logger.error(f"Error updating position {position_id} in database: {str(e)}")
            
            return True
            
    async def update_position_price(self, position_id: str, current_price: float) -> bool:
        """Update position's current price"""
        async with self._lock:
            # Check if position exists
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found")
                return False
                
            # Get position
            position = self.positions[position_id]
            
            # Update price
            position.update_price(current_price)
            
            # Update database if available
            if self.db_manager:
                try:
                    position_dict = self._position_to_dict(position)
                    # We only update specific fields for price updates to reduce database load
                    update_data = {
                        "current_price": position.current_price,
                        "pnl": position.pnl,
                        "pnl_percentage": position.pnl_percentage,
                        "last_update": position.last_update.isoformat()
                    }
                    await self.db_manager.update_position(position_id, update_data)
                except Exception as e:
                    logger.error(f"Error updating position price for {position_id} in database: {str(e)}")
            
            return True
            
    async def get_position_info(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position information"""
        async with self._lock:
            # First check in-memory positions
            if position_id in self.positions:
                return self._position_to_dict(self.positions[position_id])
            elif position_id in self.closed_positions:
                return self.closed_positions[position_id]
            
            # If not found in memory and database manager is available, try database
            if self.db_manager:
                try:
                    position_data = await self.db_manager.get_position(position_id)
                    if position_data:
                        # If position was found in database but not in memory, cache it
                        if position_data.get("status") == "open":
                            await self.restore_position(position_id, position_data)
                        else:
                            self.closed_positions[position_id] = position_data
                        return position_data
                except Exception as e:
                    logger.error(f"Error getting position {position_id} from database: {str(e)}")
            
            # Position not found anywhere
            return None
                
    async def get_open_positions(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Get all open positions grouped by symbol"""
        async with self._lock:
            result = {}
            
            for symbol, positions in self.open_positions_by_symbol.items():
                result[symbol] = {}
                for position_id, position in positions.items():
                    result[symbol][position_id] = self._position_to_dict(position)
                    
            return result
            
    async def get_closed_positions(self, limit: int = 100) -> Dict[str, Dict[str, Any]]:
        """Get recent closed positions"""
        async with self._lock:
            # If database manager is available, get from database to ensure completeness
            if self.db_manager:
                try:
                    closed_positions = await self.db_manager.get_closed_positions(limit=limit)
                    return {p["position_id"]: p for p in closed_positions}
                except Exception as e:
                    logger.error(f"Error getting closed positions from database: {str(e)}")
            
            # Fall back to in-memory closed positions
            # Get closed positions (most recent first)
            sorted_positions = sorted(
                self.closed_positions.items(),
                key=lambda x: x[1].get("close_time", ""),
                reverse=True
            )
            
            # Limit results
            limited_positions = sorted_positions[:limit]
            
            # Convert to dictionary
            return dict(limited_positions)
            
    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all positions (open and closed)"""
        async with self._lock:
            result = {}
            
            # Add open positions
            for position_id, position in self.positions.items():
                result[position_id] = self._position_to_dict(position)
                
            # Add closed positions
            result.update(self.closed_positions)
            
            return result
            
    async def get_stats(self) -> Dict[str, Any]:
        """Get position statistics"""
        async with self._lock:
            # Count positions
            open_count = len(self.positions)
            closed_count = len(self.closed_positions)
            total_count = open_count + closed_count
            
            # Calculate P&L stats
            open_pnl = sum(p.pnl for p in self.positions.values())
            closed_pnl = sum(p.get("pnl", 0) for p in self.closed_positions.values())
            total_pnl = open_pnl + closed_pnl
            
            # Calculate win/loss stats
            if self.closed_positions:
                winning_positions = [p for p in self.closed_positions.values() if p.get("pnl", 0) > 0]
                losing_positions = [p for p in self.closed_positions.values() if p.get("pnl", 0) < 0]
                
                win_count = len(winning_positions)
                loss_count = len(losing_positions)
                win_rate = win_count / len(self.closed_positions) * 100 if self.closed_positions else 0
                
                avg_win = sum(p.get("pnl", 0) for p in winning_positions) / win_count if win_count > 0 else 0
                avg_loss = sum(abs(p.get("pnl", 0)) for p in losing_positions) / loss_count if loss_count > 0 else 0
                
                profit_factor = sum(p.get("pnl", 0) for p in winning_positions) / abs(sum(p.get("pnl", 0) for p in losing_positions)) if sum(p.get("pnl", 0) for p in losing_positions) != 0 else float('inf')
            else:
                win_count = 0
                loss_count = 0
                win_rate = 0
                avg_win = 0
                avg_loss = 0
                profit_factor = 0
                
            # Get position counts by symbol
            symbol_counts = {}
            for position in self.positions.values():
                symbol = position.symbol
                if symbol not in symbol_counts:
                    symbol_counts[symbol] = 0
                symbol_counts[symbol] += 1
                
            # Get position counts by timeframe
            timeframe_counts = {}
            for position in self.positions.values():
                timeframe = position.timeframe
                if timeframe not in timeframe_counts:
                    timeframe_counts[timeframe] = 0
                timeframe_counts[timeframe] += 1
                
            return {
                "open_positions": open_count,
                "closed_positions": closed_count,
                "total_positions": total_count,
                "open_pnl": open_pnl,
                "closed_pnl": closed_pnl,
                "total_pnl": total_pnl,
                "win_count": win_count,
                "loss_count": loss_count,
                "win_rate": win_rate,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "profit_factor": profit_factor,
                "symbol_counts": symbol_counts,
                "timeframe_counts": timeframe_counts,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
    def _position_to_dict(self, position: Position) -> Dict[str, Any]:
        """Convert Position object to dictionary"""
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "action": position.action,
            "timeframe": position.timeframe,
            "entry_price": position.entry_price,
            "size": position.size,
            "stop_loss": position.stop_loss,
            "take_profit": position.take_profit,
            "open_time": position.open_time.isoformat(),
            "close_time": position.close_time.isoformat() if position.close_time else None,
            "exit_price": position.exit_price,
            "current_price": position.current_price,
            "pnl": position.pnl,
            "pnl_percentage": position.pnl_percentage,
            "status": position.status,
            "last_update": position.last_update.isoformat(),
            "metadata": position.metadata,
            "exit_reason": position.exit_reason
        }
        
    async def restore_position(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        """Restore a position from saved data"""
        async with self._lock:
            # Skip if position already exists
            if position_id in self.positions:
                return True
                
            try:
                # Extract required fields
                symbol = position_data.get("symbol")
                action = position_data.get("action")
                timeframe = position_data.get("timeframe")
                entry_price = position_data.get("entry_price")
                size = position_data.get("size")
                
                if not all([symbol, action, timeframe, entry_price, size]):
                    logger.error(f"Cannot restore position {position_id}: Missing required fields")
                    return False
                    
                # Create position
                position = Position(
                    position_id=position_id,
                    symbol=symbol,
                    action=action,
                    timeframe=timeframe,
                    entry_price=entry_price,
                    size=size,
                    stop_loss=position_data.get("stop_loss"),
                    take_profit=position_data.get("take_profit"),
                    metadata=position_data.get("metadata", {})
                )
                
                # Set additional fields
                if "open_time" in position_data and position_data["open_time"]:
                    position.open_time = datetime.fromisoformat(position_data["open_time"].replace("Z", "+00:00"))
                
                if "current_price" in position_data:
                    position.current_price = position_data["current_price"]
                    
                if "last_update" in position_data and position_data["last_update"]:
                    position.last_update = datetime.fromisoformat(position_data["last_update"].replace("Z", "+00:00"))
                
                # Set status and closing data if position is closed
                if position_data.get("status") == "closed":
                    if "close_time" in position_data and position_data["close_time"]:
                        position.close_time = datetime.fromisoformat(position_data["close_time"].replace("Z", "+00:00"))
                    
                    if "exit_price" in position_data:
                        position.exit_price = position_data["exit_price"]
                    
                    if "exit_reason" in position_data:
                        position.exit_reason = position_data["exit_reason"]
                        
                    position.status = "closed"
                    
                    # Calculate PnL for closed position
                    if position.action == "BUY":
                        position.pnl = (position.exit_price - position.entry_price) * position.size
                    else:  # SELL
                        position.pnl = (position.entry_price - position.exit_price) * position.size
                    
                    # Calculate P&L percentage
                    if position.entry_price > 0:
                        if position.action == "BUY":
                            position.pnl_percentage = (position.exit_price / position.entry_price - 1) * 100
                        else:  # SELL
                            position.pnl_percentage = (1 - position.exit_price / position.entry_price) * 100
                    
                # Store position in appropriate collections based on status
                if position.status == "open":
                    # Store in open positions
                    self.positions[position_id] = position
                    
                    # Index by symbol
                    if symbol not in self.open_positions_by_symbol:
                        self.open_positions_by_symbol[symbol] = {}
                        
                    self.open_positions_by_symbol[symbol][position_id] = position
                else:
                    # Store in closed positions
                    self.closed_positions[position_id] = self._position_to_dict(position)
                
                # Add to history
                position_dict = self._position_to_dict(position)
                self.position_history.append(position_dict)
                
                logger.info(f"Restored position: {position_id} ({symbol} {action})")
                return True
                
            except Exception as e:
                logger.error(f"Error restoring position {position_id}: {str(e)}")
                return False

    async def clean_up_duplicate_positions(self):
        """Check for and clean up any duplicate positions in database vs memory"""
        if not self.db_manager:
            return
            
        try:
            # Get all positions from database
            async with self._lock:
                db_open_positions = await self.db_manager.get_open_positions()
                memory_position_ids = set(self.positions.keys())
                
                # Find database positions that should be open but aren't in memory
                for position_data in db_open_positions:
                    position_id = position_data["position_id"]
                    if position_id not in memory_position_ids:
                        logger.info(f"Restoring missing position {position_id} from database")
                        await self.restore_position(position_id, position_data)
                
                # Find positions that are open in memory but closed in database
                for position_id in list(self.positions.keys()):
                    db_position = await self.db_manager.get_position(position_id)
                    if db_position and db_position.get("status") == "closed":
                        logger.warning(f"Position {position_id} is open in memory but closed in database. Removing from memory.")
                        # Restore the closed state to memory
                        self.closed_positions[position_id] = db_position
                        
                        # Remove from open positions
                        symbol = self.positions[position_id].symbol
                        if symbol in self.open_positions_by_symbol and position_id in self.open_positions_by_symbol[symbol]:
                            del self.open_positions_by_symbol[symbol][position_id]
                            
                            # Clean up empty symbol dictionary
                            if not self.open_positions_by_symbol[symbol]:
                                del self.open_positions_by_symbol[symbol]
                                
                        # Remove from positions
                        del self.positions[position_id]
                        
        except Exception as e:
            logger.error(f"Error cleaning up duplicate positions: {str(e)}")
    
    async def sync_with_database(self):
        """Sync all in-memory positions with the database"""
        if not self.db_manager:
            return
            
        try:
            async with self._lock:
                # Sync open positions
                for position_id, position in self.positions.items():
                    position_dict = self._position_to_dict(position)
                    await self.db_manager.save_position(position_dict)
                
                # Sync closed positions
                for position_id, position_data in self.closed_positions.items():
                    await self.db_manager.save_position(position_data)
                    
                logger.info(f"Synced {len(self.positions)} open and {len(self.closed_positions)} closed positions with database")
        except Exception as e:
            logger.error(f"Error syncing positions with database: {str(e)}")
            
    async def purge_old_closed_positions(self, max_age_days: int = 30):
        """Remove old closed positions from memory to prevent memory growth"""
        if max_age_days <= 0:
            return
            
        try:
            async with self._lock:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
                positions_to_remove = []
                
                for position_id, position_data in self.closed_positions.items():
                    # Convert close_time string to datetime
                    close_time_str = position_data.get("close_time")
                    if not close_time_str:
                        continue
                        
                    try:
                        close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                        if close_time < cutoff_date:
                            positions_to_remove.append(position_id)
                    except ValueError:
                        pass  # Skip if we can't parse the date
                
                # Remove old positions
                for position_id in positions_to_remove:
                    del self.closed_positions[position_id]
                
                # Update position history
                self.position_history = [p for p in self.position_history 
                                      if p.get("position_id") not in positions_to_remove]
                
                logger.info(f"Removed {len(positions_to_remove)} closed positions older than {max_age_days} days")
        except Exception as e:
            logger.error(f"Error purging old closed positions: {str(e)}")
            
    async def get_positions_by_symbol(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions for a specific symbol, optionally filtered by status"""
        async with self._lock:
            result = []
            
            # Check database first if available
            if self.db_manager:
                try:
                    db_positions = await self.db_manager.get_positions_by_symbol(symbol, status)
                    return db_positions
                except Exception as e:
                    logger.error(f"Error getting positions for symbol {symbol} from database: {str(e)}")
            
            # Fall back to in-memory data
            if status == "open" or status is None:
                # Get open positions for this symbol
                if symbol in self.open_positions_by_symbol:
                    for position in self.open_positions_by_symbol[symbol].values():
                        result.append(self._position_to_dict(position))
            
            if status == "closed" or status is None:
                # Get closed positions for this symbol
                for position_data in self.closed_positions.values():
                    if position_data.get("symbol") == symbol:
                        result.append(position_data)
            
            # Sort by open time (newest first)
            result.sort(key=lambda x: x.get("open_time", ""), reverse=True)
            
            return result

##############################################################################
# Volatility Monitor
##############################################################################

class VolatilityMonitor:
    """
    Monitors market volatility and provides dynamic adjustments
    for position sizing, stop loss, and take profit levels.
    """
    def __init__(self):
        """Initialize volatility monitor"""
        self.market_conditions = {}  # symbol -> volatility data
        self.history_length = 20  # Number of ATR values to keep
        self.std_dev_factor = 2.0  # Standard deviations for high/low volatility
        
    async def initialize_market_condition(self, symbol: str, timeframe: str) -> bool:
        """Initialize market condition tracking for a symbol"""
        if symbol in self.market_conditions:
            return True
            
        try:
            # Get current ATR
            atr_value = await get_atr(symbol, timeframe)
            
            if atr_value > 0:
                # Initialize with current ATR
                self.market_conditions[symbol] = {
                    "atr_history": [atr_value],
                    "mean_atr": atr_value,
                    "std_dev": 0.0,
                    "current_atr": atr_value,
                    "volatility_ratio": 1.0,  # Neutral
                    "volatility_state": "normal",  # low, normal, high
                    "timeframe": timeframe,
                    "last_update": datetime.now(timezone.utc)
                }
                return True
            else:
                logger.warning(f"Could not initialize volatility for {symbol}: Invalid ATR")
                return False
        except Exception as e:
            logger.error(f"Error initializing volatility for {symbol}: {str(e)}")
            return False
            
    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str) -> bool:
        """Update volatility state for a symbol"""
        try:
            # Initialize if needed
            if symbol not in self.market_conditions:
                await self.initialize_market_condition(symbol, timeframe)
                
            # Settings for this calculation
            settings = {
                "std_dev": self.std_dev_factor,
                "history_length": self.history_length
            }
            
            # Get current data
            data = self.market_conditions[symbol]
            
            # Update ATR history
            data["atr_history"].append(current_atr)
            
            # Trim history if needed
            if len(data["atr_history"]) > settings["history_length"]:
                data["atr_history"] = data["atr_history"][-settings["history_length"]:]
                
            # Calculate mean and standard deviation
            mean_atr = sum(data["atr_history"]) / len(data["atr_history"])
            std_dev = 0.0
            
            if len(data["atr_history"]) > 1:
                variance = sum((x - mean_atr) ** 2 for x in data["atr_history"]) / len(data["atr_history"])
                std_dev = math.sqrt(variance)
                
            # Update data
            data["mean_atr"] = mean_atr
            data["std_dev"] = std_dev
            data["current_atr"] = current_atr
            data["timeframe"] = timeframe
            data["last_update"] = datetime.now(timezone.utc)
            
            # Calculate volatility ratio
            if mean_atr > 0:
                current_ratio = current_atr / mean_atr
            else:
                current_ratio = 1.0
                
            data["volatility_ratio"] = current_ratio
            
            # Determine volatility state
            if current_atr > (mean_atr + settings["std_dev"] * std_dev):
                data["volatility_state"] = "high"
            elif current_atr < (mean_atr - settings["std_dev"] * std_dev * 0.5):  # Less strict for low volatility
                data["volatility_state"] = "low"
            else:
                data["volatility_state"] = "normal"
                
            logger.info(f"Updated volatility for {symbol}: ratio={current_ratio:.2f}, state={data['volatility_state']}")
            return True
        except Exception as e:
            logger.error(f"Error updating volatility for {symbol}: {str(e)}")
            return False
            
    def get_volatility_state(self, symbol: str) -> Dict[str, Any]:
        """Get current volatility state for a symbol"""
        if symbol not in self.market_conditions:
            return {
                "volatility_state": "normal",
                "volatility_ratio": 1.0,
                "last_update": datetime.now(timezone.utc).isoformat()
            }
            
        # Create a copy to avoid external modification
        condition = self.market_conditions[symbol].copy()
        
        # Convert datetime to ISO format for JSON compatibility
        if isinstance(condition.get("last_update"), datetime):
            condition["last_update"] = condition["last_update"].isoformat()
            
        return condition
        
    def get_position_size_modifier(self, symbol: str) -> float:
        """Get position size modifier based on volatility state"""
        if symbol not in self.market_conditions:
            return 1.0
            
        vol_state = self.market_conditions[symbol]["volatility_state"]
        ratio = self.market_conditions[symbol]["volatility_ratio"]
        
        # Adjust position size based on volatility
        if vol_state == "high":
            # Reduce position size in high volatility
            return max(0.5, 1.0 / ratio)
        elif vol_state == "low":
            # Increase position size in low volatility, but cap at 1.5x
            return min(1.5, 1.0 + (1.0 - ratio))
        else:
            # Normal volatility
            return 1.0
            
    def get_stop_loss_modifier(self, symbol: str) -> float:
        """Get stop loss distance modifier based on volatility state"""
        if symbol not in self.market_conditions:
            return 1.0
            
        vol_state = self.market_conditions[symbol]["volatility_state"]
        ratio = self.market_conditions[symbol]["volatility_ratio"]
        
        # Adjust stop loss based on volatility
        if vol_state == "high":
            # Wider stops in high volatility
            return min(1.75, ratio)
        elif vol_state == "low":
            # Tighter stops in low volatility
            return max(0.8, ratio)
        else:
            # Normal volatility
            return 1.0
            
    def should_filter_trade(self, symbol: str, strategy_type: str) -> bool:
        """Determine if a trade should be filtered out based on volatility conditions"""
        if symbol not in self.market_conditions:
            return False
            
        vol_state = self.market_conditions[symbol]["volatility_state"]
        
        # Filter trades based on strategy type and volatility
        if strategy_type == "trend_following" and vol_state == "low":
            return True  # Filter out trend following trades in low volatility
        elif strategy_type == "mean_reversion" and vol_state == "high":
            return True  # Filter out mean reversion trades in high volatility
        
        return False
        
    async def update_all_symbols(self, symbols: List[str], timeframes: Dict[str, str], current_atrs: Dict[str, float]):
        """Update volatility for multiple symbols at once"""
        for symbol in symbols:
            if symbol in current_atrs and symbol in timeframes:
                await self.update_volatility(
                    symbol=symbol,
                    current_atr=current_atrs[symbol],
                    timeframe=timeframes[symbol]
                )
                
    def get_all_volatility_states(self) -> Dict[str, Dict[str, Any]]:
        """Get volatility states for all tracked symbols"""
        result = {}
        for symbol, condition in self.market_conditions.items():
            # Create a copy of the condition
            symbol_condition = condition.copy()
            
            # Convert datetime to ISO format
            if isinstance(symbol_condition.get("last_update"), datetime):
                symbol_condition["last_update"] = symbol_condition["last_update"].isoformat()
                
            result[symbol] = symbol_condition
            
        return result

##############################################################################
# Risk Management
##############################################################################

class EnhancedRiskManager:
    """
    Comprehensive risk management system that handles both position-level and 
    portfolio-level risk controls.
    """
    def __init__(self, max_risk_per_trade=0.02, max_portfolio_risk=0.15):
        self.max_risk_per_trade = max_risk_per_trade  # 2% per trade default
        self.max_portfolio_risk = max_portfolio_risk  # 15% total portfolio risk
        self.account_balance = 0.0
        self.positions = {}  # position_id -> risk data
        self.current_risk = 0.0  # Current portfolio risk exposure
        self.daily_loss = 0.0  # Track daily loss for circuit breaker
        self.drawdown = 0.0  # Current drawdown
        self._lock = asyncio.Lock()
        
        # Advanced risk features
        self.correlation_factor = 1.0  # Correlation risk factor
        self.volatility_factor = 1.0  # Market volatility risk factor
        self.win_streak = 0  # Current win streak
        self.loss_streak = 0  # Current loss streak
        
        # Risk model parameters
        self.portfolio_heat_limit = 0.15  # Maximum portfolio heat allowed
        self.portfolio_concentration_limit = 0.4  # Maximum concentration in single instrument
        self.correlation_limit = 0.7  # Correlation threshold for risk adjustment
        
        # Timeframe risk weightings
        self.timeframe_risk_weights = {
            "M1": 1.2,  # Higher weight for shorter timeframes
            "M5": 1.1,
            "M15": 1.0,
            "M30": 0.9,
            "H1": 0.8,
            "H4": 0.7,
            "D1": 0.6  # Lower weight for longer timeframes
        }
        
    async def initialize(self, account_balance: float):
        """Initialize the risk manager with account balance"""
        async with self._lock:
            self.account_balance = float(account_balance)
            logger.info(f"Risk manager initialized with balance: {self.account_balance}")
            return True

    async def update_account_balance(self, new_balance: float):
        """Update account balance"""
        async with self._lock:
            old_balance = self.account_balance
            self.account_balance = float(new_balance)
            
            # Calculate daily loss if balance decreased
            if new_balance < old_balance:
                loss = old_balance - new_balance
                self.daily_loss += loss
                
                # Calculate drawdown
                self.drawdown = max(self.drawdown, loss / old_balance * 100)
                
            logger.info(f"Updated account balance: {self.account_balance} (daily loss: {self.daily_loss})")
            return True
            
    async def reset_daily_stats(self):
        """Reset daily statistics"""
        async with self._lock:
            self.daily_loss = 0.0
            logger.info("Reset daily risk statistics")
            return True
            
    async def register_position(self,
                               position_id: str,
                               symbol: str,
                               action: str,
                               size: float,
                               entry_price: float,
                               stop_loss: Optional[float],
                               account_risk: float,
                               timeframe: str = "H1") -> Dict[str, Any]:
        """Register a new position with the risk manager"""
        async with self._lock:
            # Calculate risk amount
            if stop_loss:
                risk_amount = abs(entry_price - stop_loss) * size
            else:
                # Estimate risk based on account risk percentage
                risk_amount = self.account_balance * account_risk
                
            risk_percentage = risk_amount / self.account_balance if self.account_balance > 0 else 0
            
            # Apply timeframe risk weighting
            timeframe_weight = self.timeframe_risk_weights.get(timeframe, 1.0)
            adjusted_risk = risk_percentage * timeframe_weight
            
            # Check if risk exceeds per-trade limit
            if adjusted_risk > self.max_risk_per_trade:
                logger.warning(f"Position risk {adjusted_risk:.2%} exceeds per-trade limit {self.max_risk_per_trade:.2%}")
                
            # Check if adding this position would exceed portfolio risk limit
            if self.current_risk + adjusted_risk > self.max_portfolio_risk:
                logger.warning(f"Adding position would exceed portfolio risk limit {self.max_portfolio_risk:.2%}")
                
            # Store position risk data
            risk_data = {
                "symbol": symbol,
                "action": action,
                "size": size,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "risk_amount": risk_amount,
                "risk_percentage": risk_percentage,
                "adjusted_risk": adjusted_risk,
                "timeframe": timeframe,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.positions[position_id] = risk_data
            
            # Update portfolio risk
            self.current_risk += adjusted_risk
            
            logger.info(f"Registered position {position_id} with risk: {adjusted_risk:.2%} (total: {self.current_risk:.2%})")
            
            return risk_data
            
    async def close_position(self, position_id: str):
        """Close a position and update risk metrics"""
        async with self._lock:
            if position_id not in self.positions:
                logger.warning(f"Position not found in risk manager: {position_id}")
                return False
                
            position = self.positions[position_id]
            
            # Reduce portfolio risk
            self.current_risk -= position.get("adjusted_risk", 0)
            self.current_risk = max(0, self.current_risk)  # Ensure non-negative
            
            # Remove position
            del self.positions[position_id]
            
            logger.info(f"Closed position {position_id} in risk manager (remaining risk: {self.current_risk:.2%})")
            return True
            
    async def is_trade_allowed(self, risk_percentage: float, symbol: Optional[str] = None) -> Tuple[bool, str]:
        """Check if a trade with specified risk is allowed"""
        async with self._lock:
            # Check if daily loss exceeds limit
            max_daily_loss_amount = self.account_balance * MAX_DAILY_LOSS
            if self.daily_loss >= max_daily_loss_amount:
                return False, f"Daily loss limit reached: {self.daily_loss:.2f} >= {max_daily_loss_amount:.2f}"
                
            # Check if trade risk exceeds per-trade limit
            if risk_percentage > self.max_risk_per_trade:
                return False, f"Trade risk exceeds limit: {risk_percentage:.2%} > {self.max_risk_per_trade:.2%}"
                
            # Check if adding the trade would exceed portfolio risk limit
            if self.current_risk + risk_percentage > self.max_portfolio_risk:
                return False, f"Portfolio risk would exceed limit: {self.current_risk + risk_percentage:.2%} > {self.max_portfolio_risk:.2%}"
                
            # Check concentration limit if symbol is provided
            if symbol:
                # Calculate current exposure to this symbol
                symbol_exposure = sum(
                    p.get("adjusted_risk", 0) for p in self.positions.values() 
                    if p.get("symbol") == symbol
                )
                
                # Check if adding this position would exceed concentration limit
                if symbol_exposure + risk_percentage > self.portfolio_concentration_limit:
                    return False, f"Symbol concentration would exceed limit: {symbol_exposure + risk_percentage:.2%} > {self.portfolio_concentration_limit:.2%}"
                    
            return True, "Trade allowed"
    
    async def adjust_position_size(self,
                                 base_size: float,
                                 symbol: str,
                                 risk_percentage: float,
                                 account_balance: Optional[float] = None) -> float:
        """Adjust position size based on risk parameters"""
        async with self._lock:
            if account_balance is not None:
                self.account_balance = account_balance
                
            # Calculate remaining risk capacity
            remaining_capacity = self.max_portfolio_risk - self.current_risk
            
            # Calculate scale factor based on remaining capacity
            if remaining_capacity <= 0:
                scale = 0.0  # No capacity left
            elif remaining_capacity < risk_percentage:
                scale = remaining_capacity / risk_percentage  # Partial capacity
            else:
                scale = 1.0  # Full capacity
                
            # Apply correlation factor if applicable
            # In a real system, this would be calculated based on actual correlations
            correlated_instruments = self._get_correlated_instruments(symbol)
            if correlated_instruments:
                # Reduce size for correlated positions
                scale *= self.correlation_factor
                
            # Apply volatility adjustment
            # Again, placeholder for actual volatility calculation
            scale *= self.volatility_factor
            
            # Apply streak adjustment
            streak_factor = self._calculate_streak_factor()
            scale *= streak_factor
            
            # Calculate adjusted size
            adjusted_size = base_size * scale
            
            logger.debug(f"Adjusted position size for {symbol}: {base_size} -> {adjusted_size} (scale: {scale:.2f})")
            return adjusted_size
            
    def _get_correlated_instruments(self, symbol: str) -> List[str]:
        """Get list of instruments correlated with the given symbol"""
        # Placeholder for actual correlation logic
        # In a real system, this would check a correlation matrix
        correlated = []
        
        # Example correlations (very simplified)
        forex_pairs = {
            "EUR_USD": ["EUR_GBP", "EUR_JPY", "USD_CHF"],
            "GBP_USD": ["EUR_GBP", "GBP_JPY"],
            "USD_JPY": ["EUR_JPY", "GBP_JPY"]
        }
        
        # Get correlated instruments if any
        return forex_pairs.get(symbol, [])
        
    def _calculate_streak_factor(self) -> float:
        """Calculate adjustment factor based on win/loss streak"""
        if self.win_streak >= 3:
            # Gradual increase for winning streak
            return min(1.5, 1.0 + (self.win_streak - 2) * 0.1)
        elif self.loss_streak >= 2:
            # More aggressive decrease for losing streak
            return max(0.5, 1.0 - (self.loss_streak - 1) * 0.2)
        else:
            return 1.0  # No streak adjustment
            
    async def update_win_loss_streak(self, is_win: bool):
        """Update win/loss streak counters"""
        async with self._lock:
            if is_win:
                self.win_streak += 1
                self.loss_streak = 0  # Reset loss streak
            else:
                self.loss_streak += 1
                self.win_streak = 0  # Reset win streak
                
            logger.debug(f"Updated streaks: wins={self.win_streak}, losses={self.loss_streak}")
            
    async def clear_position(self, position_id: str):
        """Clear a position from risk tracking"""
        async with self._lock:
            if position_id in self.positions:
                position = self.positions[position_id]
                self.current_risk -= position.get("adjusted_risk", 0)
                self.current_risk = max(0, self.current_risk)  # Ensure non-negative
                del self.positions[position_id]
                logger.info(f"Cleared position {position_id} from risk tracking")
                return True
            return False
            
    async def get_risk_metrics(self) -> Dict[str, Any]:
        """Get current risk metrics"""
        async with self._lock:
            # Count positions by symbol
            symbol_counts = {}
            symbol_risks = {}
            
            for position in self.positions.values():
                symbol = position.get("symbol")
                if symbol:
                    symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
                    symbol_risks[symbol] = symbol_risks.get(symbol, 0) + position.get("adjusted_risk", 0)
                    
            # Calculate concentration metrics
            max_symbol = None
            max_risk = 0
            
            for symbol, risk in symbol_risks.items():
                if risk > max_risk:
                    max_risk = risk
                    max_symbol = symbol
                    
            return {
                "current_risk": self.current_risk,
                "max_risk": self.max_portfolio_risk,
                "remaining_risk": max(0, self.max_portfolio_risk - self.current_risk),
                "daily_loss": self.daily_loss,
                "daily_loss_limit": self.account_balance * MAX_DAILY_LOSS,
                "drawdown": self.drawdown,
                "position_count": len(self.positions),
                "symbols": list(symbol_counts.keys()),
                "symbol_counts": symbol_counts,
                "symbol_risks": symbol_risks,
                "highest_concentration": {
                    "symbol": max_symbol,
                    "risk": max_risk
                },
                "win_streak": self.win_streak,
                "loss_streak": self.loss_streak
            }

##############################################################################
# Exit Management
##############################################################################

class MultiStageTakeProfitManager:
    """
    Manages multi-level take profit levels with partial position
    closing at each level.
    """
    def __init__(self, position_tracker=None):
        """Initialize multi-stage take profit manager"""
        self.position_tracker = position_tracker
        self.take_profit_levels = {}  # position_id -> TP levels
        self._lock = asyncio.Lock()
        
    async def set_take_profit_levels(self,
                                   position_id: str,
                                   entry_price: float,
                                   stop_loss: Optional[float],
                                   position_direction: str,
                                   position_size: float,
                                   symbol: str,
                                   timeframe: str,
                                   atr_value: float = 0.0,
                                   volatility_multiplier: float = 1.0):
        """Set take profit levels for a position"""
        async with self._lock:
            # Default TP levels (percentage of risk)
            default_levels = [
                {"distance": 1.0, "percentage": 30},  # First TP at 1:1 risk/reward, close 30%
                {"distance": 2.0, "percentage": 40},  # Second TP at 2:1 risk/reward, close 40%
                {"distance": 3.0, "percentage": 30}   # Third TP at 3:1 risk/reward, close 30%
            ]
            
            # Adjust based on instrument type
            instrument_type = get_instrument_type(symbol)
            
            # Store take profit data
            self.take_profit_levels[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "direction": position_direction.upper(),
                "size": position_size,
                "timeframe": timeframe,
                "levels": []
            }
            
            # Calculate TP distances
            if stop_loss and entry_price:
                # Calculate risk distance
                if position_direction.upper() == "BUY":
                    risk_distance = entry_price - stop_loss
                else:
                    risk_distance = stop_loss - entry_price
                    
                # Risk distance must be positive
                risk_distance = abs(risk_distance)
                
                if risk_distance > 0:
                    # Calculate TP levels
                    for level in default_levels:
                        # Adjust distance by volatility
                        adjusted_distance = level["distance"] * volatility_multiplier
                        
                        # Calculate TP price
                        if position_direction.upper() == "BUY":
                            tp_price = entry_price + (risk_distance * adjusted_distance)
                        else:
                            tp_price = entry_price - (risk_distance * adjusted_distance)
                            
                        # Calculate size to close at this level
                        close_size = position_size * (level["percentage"] / 100)
                        
                        # Add to levels
                        self.take_profit_levels[position_id]["levels"].append({
                            "price": tp_price,
                            "percentage": level["percentage"],
                            "size": close_size,
                            "reached": False,
                            "distance_ratio": adjusted_distance,
                            "atr_multiple": adjusted_distance * (atr_value / risk_distance) if risk_distance > 0 and atr_value > 0 else 0
                        })
                else:
                    # Fallback to ATR-based levels if stop loss is not available or risk distance is zero
                    await self._set_atr_based_levels(
                        position_id, entry_price, position_direction, atr_value, volatility_multiplier
                    )
            else:
                # Use ATR-based levels if stop loss is not available
                await self._set_atr_based_levels(
                    position_id, entry_price, position_direction, atr_value, volatility_multiplier
                )
                
            logger.info(f"Set {len(self.take_profit_levels[position_id]['levels'])} take profit levels for {position_id}")
            return self.take_profit_levels[position_id]
            
    async def _set_atr_based_levels(self,
                                  position_id: str,
                                  entry_price: float,
                                  position_direction: str,
                                  atr_value: float,
                                  volatility_multiplier: float):
        """Set take profit levels based on ATR when stop loss is not available"""
        if atr_value <= 0:
            logger.warning(f"Cannot set ATR-based TP levels for {position_id}: Invalid ATR")
            return
            
        # Define ATR multiples for TP levels
        atr_multiples = [
            {"multiple": 1.5, "percentage": 30},  # First TP at 1.5 ATR, close 30%
            {"multiple": 3.0, "percentage": 40},  # Second TP at 3 ATR, close 40%
            {"multiple": 5.0, "percentage": 30}   # Third TP at 5 ATR, close 30%
        ]
        
        # Get position data
        position_data = self.take_profit_levels[position_id]
        position_size = position_data["size"]
        
        # Clear existing levels
        position_data["levels"] = []
        
        # Calculate TP levels
        for level in atr_multiples:
            # Adjust multiple by volatility
            adjusted_multiple = level["multiple"] * volatility_multiplier
            
            # Calculate TP price
            if position_direction.upper() == "BUY":
                tp_price = entry_price + (atr_value * adjusted_multiple)
            else:
                tp_price = entry_price - (atr_value * adjusted_multiple)
                
            # Calculate size to close at this level
            close_size = position_size * (level["percentage"] / 100)
            
            # Add to levels
            position_data["levels"].append({
                "price": tp_price,
                "percentage": level["percentage"],
                "size": close_size,
                "reached": False,
                "distance_ratio": 0,  # Not based on risk/reward
                "atr_multiple": adjusted_multiple
            })
            
        logger.info(f"Set {len(position_data['levels'])} ATR-based take profit levels for {position_id}")
        
    async def check_take_profit_levels(self, position_id: str, current_price: float) -> Optional[Dict[str, Any]]:
        """Check if any take profit levels have been reached"""
        async with self._lock:
            if position_id not in self.take_profit_levels:
                return None
                
            position_data = self.take_profit_levels[position_id]
            direction = position_data["direction"]
            
            # Check each level
            for i, level in enumerate(position_data["levels"]):
                if level["reached"]:
                    continue
                    
                # Check if level is reached
                if (direction == "BUY" and current_price >= level["price"]) or \
                   (direction == "SELL" and current_price <= level["price"]):
                    # Mark level as reached
                    self.take_profit_levels[position_id]["levels"][i]["reached"] = True
                    
                    # Return level data for action
                    return {
                        "position_id": position_id,
                        "level_index": i,
                        "price": level["price"],
                        "percentage": level["percentage"],
                        "size": level["size"]
                    }
                    
            return None
            
    async def execute_take_profit(self, position_id: str, level_data: Dict[str, Any]) -> bool:
        """Execute take profit by closing part of the position"""
        if not self.position_tracker:
            logger.error(f"Cannot execute take profit for {position_id}: Position tracker not available")
            return False
            
        try:
            # Close partial position
            success, result = await self.position_tracker.close_partial_position(
                position_id=position_id,
                exit_price=level_data["price"],
                percentage=level_data["percentage"],
                reason=f"take_profit_level_{level_data['level_index'] + 1}"
            )
            
            if success:
                logger.info(f"Executed take profit level {level_data['level_index'] + 1} for {position_id}")
                return True
            else:
                logger.error(f"Failed to execute take profit for {position_id}: {result.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing take profit for {position_id}: {str(e)}")
            return False
            
    def get_take_profit_levels(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get take profit levels for a position"""
        return self.take_profit_levels.get(position_id)

class TimeBasedExitManager:
    """
    Manages time-based exits for positions based on holding time
    or specific market sessions.
    """
    def __init__(self):
        """Initialize time-based exit manager"""
        self.positions = {}  # position_id -> time exit data
        self.time_rules = {}  # rule_id -> time rule
        self.default_max_holding_times = {
            "M1": 120,    # 2 hours
            "M5": 240,    # 4 hours
            "M15": 480,   # 8 hours
            "M30": 960,   # 16 hours
            "H1": 48,     # 48 hours
            "H4": 96,     # 96 hours
            "D1": 14      # 14 days
        }
        self._running = False
        self._lock = asyncio.Lock()
        
    async def start(self):
        """Start time-based exit manager"""
        if self._running:
            return
            
        self._running = True
        logger.info("Time-based exit manager started")
        
    async def stop(self):
        """Stop time-based exit manager"""
        if not self._running:
            return
            
        self._running = False
        logger.info("Time-based exit manager stopped")
        
    def register_position(self,
                         position_id: str,
                         symbol: str,
                         direction: str,
                         entry_time: datetime,
                         timeframe: str):
        """Register a position for time-based exit"""
        # Get max holding time based on timeframe
        max_hours = self.default_max_holding_times.get(timeframe, 24)
        
        # Convert to appropriate units
        if timeframe.startswith("M"):
            # For minute timeframes, convert hours to minutes
            max_holding_time = max_hours * 60
            holding_unit = "minutes"
        elif timeframe.startswith("H"):
            # For hour timeframes, use hours
            max_holding_time = max_hours
            holding_unit = "hours"
        elif timeframe.startswith("D"):
            # For day timeframes, convert hours to days
            max_holding_time = max_hours / 24
            holding_unit = "days"
        else:
            # Default to hours
            max_holding_time = max_hours
            holding_unit = "hours"
            
        # Store position data
        self.positions[position_id] = {
            "symbol": symbol,
            "direction": direction,
            "entry_time": entry_time,
            "timeframe": timeframe,
            "max_holding_time": max_holding_time,
            "holding_unit": holding_unit,
            "exit_time": self._calculate_exit_time(entry_time, holding_unit, max_holding_time),
            "rules_applied": []
        }
        
        logger.info(f"Registered position {position_id} for time-based exit after {max_holding_time} {holding_unit}")
        
    def add_time_rule(self,
                     rule_id: str,
                     rule_type: str,
                     parameters: Dict[str, Any],
                     symbols: Optional[List[str]] = None):
        """Add a time-based exit rule"""
        self.time_rules[rule_id] = {
            "rule_type": rule_type,
            "parameters": parameters,
            "symbols": symbols,
            "created_at": datetime.now(timezone.utc)
        }
        
        logger.info(f"Added time rule {rule_id} of type {rule_type}")
        
    def remove_position(self, position_id: str) -> bool:
        """Remove a position from time-based exit tracking"""
        if position_id in self.positions:
            del self.positions[position_id]
            logger.info(f"Removed position {position_id} from time-based exit tracking")
            return True
        return False
        
    def check_time_exits(self) -> List[Dict[str, Any]]:
        """Check for positions that should be exited based on time rules"""
        now = datetime.now(timezone.utc)
        exits = []
        
        for position_id, position in list(self.positions.items()):
            # Check max holding time exit
            exit_time = position.get("exit_time")
            if exit_time and now >= exit_time:
                exits.append({
                    "position_id": position_id,
                    "reason": "max_holding_time",
                    "details": f"Maximum holding time of {position['max_holding_time']} {position['holding_unit']} reached"
                })
                continue
                
            # Check for end-of-day exit
            # Implementation depends on which market sessions you want to handle
            # For FX, typically New York close at 22:00 UTC is used
            if "end_of_day" in self.time_rules:
                eod_rule = self.time_rules["end_of_day"]
                if self._should_apply_rule(position, eod_rule):
                    # Check if it's near end of trading day
                    eod_hour = eod_rule["parameters"].get("hour", 22)
                    eod_minute = eod_rule["parameters"].get("minute", 0)
                    
                    if now.hour == eod_hour and now.minute >= eod_minute:
                        if self._is_profitable(position):
                            exits.append({
                                "position_id": position_id,
                                "reason": "end_of_day",
                                "details": "End of trading day with profitable position"
                            })
                            
            # Add other time-based rules as needed
            
        # Remove exited positions
        for exit_info in exits:
            self.remove_position(exit_info["position_id"])
            
        return exits
        
    def _calculate_exit_time(self, entry_time: datetime, unit: str, amount: float) -> datetime:
        """Calculate exit time based on entry time and holding period"""
        if unit == "minutes":
            return entry_time + timedelta(minutes=int(amount))
        elif unit == "hours":
            return entry_time + timedelta(hours=int(amount))
        elif unit == "days":
            return entry_time + timedelta(days=int(amount))
        else:
            # Default to hours
            return entry_time + timedelta(hours=int(amount))
            
    def _should_apply_rule(self, position: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """Check if a rule should be applied to a position"""
        # Check symbol filter
        symbols = rule.get("symbols")
        if symbols and position["symbol"] not in symbols:
            return False
            
        return True
        
    def _is_profitable(self, position: Dict[str, Any]) -> bool:
        """Check if a position is currently profitable"""
        # In a real implementation, this would check current price vs entry
        # For now, assume 50% chance of profitability
        return random.random() > 0.5
        
    def record_exit_outcome(self, position_id: str, success: bool):
        """Record the outcome of a time-based exit for analysis"""
        # This would be implemented to track performance of time-based exits
        pass
        
    def get_status(self) -> Dict[str, Any]:
        """Get status of time-based exit manager"""
        now = datetime.now(timezone.utc)
        
        pending_exits = []
        for position_id, position in self.positions.items():
            exit_time = position.get("exit_time")
            if exit_time:
                time_to_exit = (exit_time - now).total_seconds() / 3600  # hours
                
                pending_exits.append({
                    "position_id": position_id,
                    "symbol": position["symbol"],
                    "timeframe": position["timeframe"],
                    "exit_time": exit_time.isoformat(),
                    "hours_remaining": time_to_exit
                })
                
        return {
            "positions_tracked": len(self.positions),
            "rules_active": len(self.time_rules),
            "pending_exits": pending_exits
        }

class DynamicExitManager:
    """
    Manages dynamic exits based on Lorentzian classifier market regimes.
    Adjusts stop losses, take profits, and trailing stops based on market conditions.
    """
    def __init__(self, position_tracker=None, multi_stage_tp_manager=None):
        """Initialize dynamic exit manager"""
        self.position_tracker = position_tracker
        self.multi_stage_tp_manager = multi_stage_tp_manager
        self.exit_levels = {}
        self.trailing_stops = {}
        self.exit_strategies = {}
        self.performance = {}
        self.lorentzian_classifier = MarketRegimeClassifier()
        self._lock = asyncio.Lock()
        self._running = False
        
    async def start(self):
        """Start the exit manager"""
        if not self._running:
            self._running = True
            logger.info("Dynamic Exit Manager started")
            
    async def stop(self):
        """Stop the exit manager"""
        self._running = False
        logger.info("Dynamic Exit Manager stopped")
            
    async def initialize_exits(self, 
                             position_id: str, 
                             symbol: str, 
                             entry_price: float, 
                             position_direction: str,
                             stop_loss: Optional[float] = None, 
                             take_profit: Optional[float] = None,
                             timeframe: str = "H1",
                             strategy_type: str = "general") -> Dict[str, Any]:
        """Initialize exit strategies based on market regime"""
        async with self._lock:
            # Get the current market regime
            regime_data = self.lorentzian_classifier.get_regime_data(symbol)
            regime = regime_data.get("regime", "unknown")
            
            # Create basic exit strategy record
            self.exit_strategies[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": position_direction,
                "timeframe": timeframe,
                "strategy_type": strategy_type,
                "market_regime": regime,
                "exits": {},
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Initialize trailing stop
            if stop_loss:
                await self._init_trailing_stop(position_id, entry_price, stop_loss, position_direction)
                
            # Initialize breakeven stop
            await self._init_breakeven_stop(position_id, entry_price, position_direction)
            
            # Choose appropriate specialized exit strategy based on regime and strategy type
            if "trending" in regime and strategy_type in ["trend_following", "general"]:
                await self._init_trend_following_exits(position_id, entry_price, stop_loss, position_direction)
            elif regime in ["ranging", "mixed"] and strategy_type in ["mean_reversion", "general"]:
                await self._init_mean_reversion_exits(position_id, entry_price, stop_loss, position_direction)
            elif regime in ["volatile", "momentum_up", "momentum_down"] and strategy_type in ["breakout", "general"]:
                await self._init_breakout_exits(position_id, entry_price, stop_loss, position_direction)
            else:
                # Standard exits for other cases
                await self._init_standard_exits(position_id, entry_price, stop_loss, position_direction)
                
            logger.info(f"Initialized exits for {position_id}")

                
            # Run every minute
            await asyncio.sleep(60)

##############################################################################
# Market Analysis
##############################################################################

class MarketRegimeClassifier:
    """
    Classifies market regimes (trending, ranging, volatile, etc.) using Lorentzian distance
    methodology to adapt trading strategies accordingly.
    """
    def __init__(self):
        """Initialize Lorentzian classifier"""
        self.price_history = {}  # symbol -> price history
        self.regime_history = {}  # symbol -> regime history
        self.volatility_history = {}  # symbol -> volatility history
        self.atr_history = {}  # symbol -> ATR history
        self.lookback_period = 20  # Default lookback period
        self._lock = asyncio.Lock()
        
    async def add_price_data(self, symbol: str, price: float, timeframe: str, atr: float = None):
        """Add price data for a symbol"""
        async with self._lock:
            # Initialize data structures if needed
            if symbol not in self.price_history:
                self.price_history[symbol] = []
                self.regime_history[symbol] = []
                self.volatility_history[symbol] = []
                self.atr_history[symbol] = []
                
            # Add price to history
            self.price_history[symbol].append(price)
            if len(self.price_history[symbol]) > self.lookback_period:
                self.price_history[symbol].pop(0)
                
            # Update ATR history if provided
            if atr is not None:
                self.atr_history[symbol].append(atr)
                if len(self.atr_history[symbol]) > self.lookback_period:
                    self.atr_history[symbol].pop(0)
                    
            # Update regime if we have enough data
            if len(self.price_history[symbol]) >= 2:
                await self.update_regime(symbol, timeframe)
                
    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """Calculate true Lorentzian distance using logarithmic scaling"""
        if not history:
            return 0.0
            
        distances = []
        for hist_price in history:
            # Proper Lorentzian distance formula with log scaling
            distance = np.log(1 + abs(price - hist_price))
            distances.append(distance)
            
        return float(np.mean(distances))
        
    async def update_regime(self, symbol: str, timeframe: str):
        """Update market regime for a symbol"""
        async with self._lock:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
                return
                
            try:
                # Get current price
                current_price = self.price_history[symbol][-1]
                
                # Calculate price-based metrics
                price_distance = await self.calculate_lorentzian_distance(
                    current_price, self.price_history[symbol][:-1]  # Compare current to history
                )
                
                # Calculate returns and volatility
                returns = [self.price_history[symbol][i] / self.price_history[symbol][i-1] - 1 
                          for i in range(1, len(self.price_history[symbol]))]
                volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
                
                # Calculate momentum (percentage change over lookback period)
                momentum = (current_price - self.price_history[symbol][0]) / self.price_history[symbol][0] if self.price_history[symbol][0] != 0 else 0.0
                
                # Get mean ATR if available
                mean_atr = 0.0
                if self.atr_history[symbol]:
                    mean_atr = sum(self.atr_history[symbol]) / len(self.atr_history[symbol])
                
                # Multi-factor regime classification
                regime = "unknown"
                regime_strength = 0.5  # Default medium strength
                
                # Use both price distance and volatility for classification
                if price_distance < 0.1 and volatility < 0.001:
                    regime = "ranging"
                    regime_strength = min(1.0, 0.7 + (0.1 - price_distance) * 3)
                elif price_distance > 0.3 and abs(momentum) > 0.002:
                    if momentum > 0:
                        regime = "trending_up"
                    else:
                        regime = "trending_down"
                    regime_strength = min(1.0, 0.6 + price_distance + abs(momentum) * 10)
                elif volatility > 0.003 or (mean_atr > 0 and self.atr_history[symbol][-1] > 1.5 * mean_atr):
                    regime = "volatile"
                    regime_strength = min(1.0, 0.6 + volatility * 100)
                elif abs(momentum) > 0.003:
                    if momentum > 0:
                        regime = "momentum_up"
                    else:
                        regime = "momentum_down"
                    regime_strength = min(1.0, 0.6 + abs(momentum) * 50)
                else:
                    regime = "mixed"
                    regime_strength = 0.5
                    
                # Update regime history
                self.regime_history[symbol].append(regime)
                if len(self.regime_history[symbol]) > self.lookback_period:
                    self.regime_history[symbol].pop(0)
                    
                # Update volatility history
                self.volatility_history[symbol].append(volatility)
                if len(self.volatility_history[symbol]) > self.lookback_period:
                    self.volatility_history[symbol].pop(0)
                
                # Store the regime and metrics
                result = {
                    "regime": regime,
                    "regime_strength": regime_strength,
                    "price_distance": price_distance,
                    "volatility": volatility,
                    "momentum": momentum,
                    "last_update": datetime.now(timezone.utc),
                    "metrics": {
                        "price_distance": price_distance,
                        "volatility": volatility,
                        "momentum": momentum,
                        "lookback_period": self.lookback_period
                    }
                }
                
                # Store the result for later retrieval
                if not hasattr(self, "regimes"):
                    self.regimes = {}
                if symbol not in self.regimes:
                    self.regimes[symbol] = {}
                
                self.regimes[symbol] = result
                
                # Also maintain a history of regime data (compatible with the original implementation)
                if "regime_history" not in self.regimes[symbol]:
                    self.regimes[symbol]["regime_history"] = []
                
                self.regimes[symbol]["regime_history"].append({
                    "regime": regime,
                    "strength": regime_strength,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                
                # Limit history length
                if len(self.regimes[symbol]["regime_history"]) > 20:
                    self.regimes[symbol]["regime_history"] = self.regimes[symbol]["regime_history"][-20:]
                
            except Exception as e:
                logger.error(f"Error updating regime for {symbol}: {str(e)}")
    
    def get_dominant_regime(self, symbol: str) -> str:
        """Get the dominant regime over recent history"""
        if symbol not in self.regime_history or len(self.regime_history[symbol]) < 3:
            return "unknown"
            
        recent_regimes = self.regime_history[symbol][-5:]
        regime_counts = {}
        
        for regime in recent_regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
            
        # Find most common regime
        dominant_regime = max(regime_counts.items(), key=lambda x: x[1])
        # Only consider it dominant if it appears more than 60% of the time
        if dominant_regime[1] / len(recent_regimes) >= 0.6:
            return dominant_regime[0]
        else:
            return "mixed"
    
    async def should_adjust_exits(self, symbol: str, current_regime: str = None) -> Tuple[bool, Dict[str, float]]:
        """Determine if exit levels should be adjusted based on regime stability and type"""
        # Get current regime if not provided
        if current_regime is None:
            if symbol not in self.regime_history or not self.regime_history[symbol]:
                return False, {"stop_loss": 1.0, "take_profit": 1.0, "trailing_stop": 1.0}
            current_regime = self.regime_history[symbol][-1]
        
        # Check regime stability (all 3 most recent regimes are the same)
        recent_regimes = self.regime_history.get(symbol, [])[-3:]
        is_stable = len(recent_regimes) >= 3 and len(set(recent_regimes)) == 1
        
        # Set specific adjustments based on regime
        adjustments = {
            "stop_loss": 1.0,
            "take_profit": 1.0,
            "trailing_stop": 1.0
        }
        
        if is_stable:
            if "volatile" in current_regime:
                adjustments["stop_loss"] = 1.5      # Wider stop loss in volatile markets
                adjustments["take_profit"] = 2.0    # More ambitious take profit
                adjustments["trailing_stop"] = 1.25  # Wider trailing stop
            elif "trending" in current_regime:
                adjustments["stop_loss"] = 1.25     # Slightly wider stop
                adjustments["take_profit"] = 1.5    # More room to run
                adjustments["trailing_stop"] = 1.1   # Slightly wider trailing stop
            elif "ranging" in current_regime:
                adjustments["stop_loss"] = 0.8      # Tighter stop loss
                adjustments["take_profit"] = 0.8    # Tighter take profit
                adjustments["trailing_stop"] = 0.9   # Tighter trailing stop
            elif "momentum" in current_regime:
                adjustments["stop_loss"] = 1.2      # Slightly wider stop
                adjustments["take_profit"] = 1.7    # More ambitious take profit
                adjustments["trailing_stop"] = 1.3   # Wider trailing to catch momentum
        
        should_adjust = is_stable and any(v != 1.0 for v in adjustments.values())
        return should_adjust, adjustments
            
    def get_regime_data(self, symbol: str) -> Dict[str, Any]:
        """Get market regime data for a symbol"""
        if not hasattr(self, "regimes") or symbol not in self.regimes:
            return {
                "regime": "unknown",
                "regime_strength": 0.0,
                "last_update": datetime.now(timezone.utc).isoformat()
            }
            
        # Make a copy to avoid accidental modification
        regime_data = self.regimes[symbol].copy()
        
        # Convert datetime to string for JSON compatibility
        if isinstance(regime_data.get("last_update"), datetime):
            regime_data["last_update"] = regime_data["last_update"].isoformat()
            
        return regime_data
        
    def is_suitable_for_strategy(self, symbol: str, strategy_type: str) -> bool:
        """Determine if the current market regime is suitable for a strategy"""
        if not hasattr(self, "regimes") or symbol not in self.regimes:
            return True  # Default to allowing trades if no regime data
            
        regime = self.regimes[symbol]["regime"]
        
        # Match strategy types to regimes
        if strategy_type == "trend_following":
            return "trending" in regime
        elif strategy_type == "mean_reversion":
            return regime in ["ranging", "mixed"]
        elif strategy_type == "breakout":
            return regime in ["ranging", "volatile"]  # Breakouts often occur after ranging or volatile periods
        elif strategy_type == "momentum":
            return "momentum" in regime
        else:
            return True  # Default strategy assumed to work in all regimes
            
    async def clear_history(self, symbol: str):
        """Clear historical data for a symbol"""
        async with self._lock:
            if symbol in self.price_history:
                del self.price_history[symbol]
            if symbol in self.regime_history:
                del self.regime_history[symbol]
            if symbol in self.volatility_history:
                del self.volatility_history[symbol]
            if symbol in self.atr_history:
                del self.atr_history[symbol]
            if hasattr(self, "regimes") and symbol in self.regimes:
                del self.regimes[symbol]
class SeasonalPatternAnalyzer:
    """
    Analyzes seasonal patterns in price data to identify recurring
    patterns by day of week, time of day, and month.
    """
    def __init__(self):
        """Initialize seasonal pattern analyzer"""
        self.data = {}  # symbol -> seasonal data
        self.min_samples = 20  # Minimum samples for reliable analysis
        self._lock = asyncio.Lock()
        
    async def add_price_data(self, symbol: str, timestamp: datetime, price: float):
        """Add price data for a symbol"""
        async with self._lock:
            if symbol not in self.data:
                self.data[symbol] = {
                    "day_of_week": {},  # 0-6 (Monday-Sunday)
                    "time_of_day": {},  # Hour (0-23)
                    "month": {},        # 1-12
                    "raw_data": []      # List of {timestamp, price} entries
                }
                
            # Add raw data
            self.data[symbol]["raw_data"].append({
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
                "price": price
            })
            
            # Limit raw data size
            if len(self.data[symbol]["raw_data"]) > 1000:
                self.data[symbol]["raw_data"] = self.data[symbol]["raw_data"][-1000:]
                
            # Extract date components
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                
            day_of_week = timestamp.weekday()  # 0-6
            hour = timestamp.hour
            month = timestamp.month
            
            # Update day of week data
            if day_of_week not in self.data[symbol]["day_of_week"]:
                self.data[symbol]["day_of_week"][day_of_week] = {
                    "prices": [],
                    "returns": []
                }
                
            # Update time of day data
            if hour not in self.data[symbol]["time_of_day"]:
                self.data[symbol]["time_of_day"][hour] = {
                    "prices": [],
                    "returns": []
                }
                
            # Update month data
            if month not in self.data[symbol]["month"]:
                self.data[symbol]["month"][month] = {
                    "prices": [],
                    "returns": []
                }
                
            # Add price to respective categories
            self.data[symbol]["day_of_week"][day_of_week]["prices"].append(price)
            self.data[symbol]["time_of_day"][hour]["prices"].append(price)
            self.data[symbol]["month"][month]["prices"].append(price)
            
            # Calculate returns if we have previous prices
            for category in ["day_of_week", "time_of_day", "month"]:
                category_key = day_of_week if category == "day_of_week" else hour if category == "time_of_day" else month
                
                prices = self.data[symbol][category][category_key]["prices"]
                returns = self.data[symbol][category][category_key]["returns"]
                
                if len(prices) > 1:
                    ret = (prices[-1] / prices[-2]) - 1
                    returns.append(ret)
                    
                    # Limit history size
                    if len(returns) > 100:
                        self.data[symbol][category][category_key]["returns"] = returns[-100:]
                    
                    # Limit prices history
                    if len(prices) > 100:
                        self.data[symbol][category][category_key]["prices"] = prices[-100:]
                        
    async def get_day_of_week_pattern(self, symbol: str) -> Dict[str, Any]:
        """Get day of week pattern for a symbol"""
        async with self._lock:
            if symbol not in self.data:
                return {
                    "status": "no_data",
                    "message": "No data available for this symbol"
                }
                
            # Get day of week data
            day_data = self.data[symbol]["day_of_week"]
            
            # Check if we have enough data
            total_samples = sum(len(day["returns"]) for day in day_data.values())
            if total_samples < self.min_samples:
                return {
                    "status": "insufficient_data",
                    "message": f"Insufficient data ({total_samples} < {self.min_samples})"
                }
                
            # Calculate average returns by day
            day_returns = {}
            day_volatility = {}
            day_samples = {}
            
            for day, data in day_data.items():
                returns = data["returns"]
                if returns:
                    day_returns[day] = sum(returns) / len(returns)
                    day_volatility[day] = (sum((r - day_returns[day]) ** 2 for r in returns) / len(returns)) ** 0.5
                    day_samples[day] = len(returns)
                    
            # Convert day numbers to names
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            named_returns = {day_names[int(day)]: ret for day, ret in day_returns.items()}
            named_volatility = {day_names[int(day)]: vol for day, vol in day_volatility.items()}
            named_samples = {day_names[int(day)]: samp for day, samp in day_samples.items()}
            
            return {
                "status": "success",
                "average_returns": named_returns,
                "volatility": named_volatility,
                "sample_counts": named_samples
            }
            
    async def get_time_of_day_pattern(self, symbol: str) -> Dict[str, Any]:
        """Get time of day pattern for a symbol"""
        async with self._lock:
            if symbol not in self.data:
                return {
                    "status": "no_data",
                    "message": "No data available for this symbol"
                }
                
            # Get time of day data
            hour_data = self.data[symbol]["time_of_day"]
            
            # Check if we have enough data
            total_samples = sum(len(hour["returns"]) for hour in hour_data.values())
            if total_samples < self.min_samples:
                return {
                    "status": "insufficient_data",
                    "message": f"Insufficient data ({total_samples} < {self.min_samples})"
                }
                
            # Calculate average returns by hour
            hour_returns = {}
            hour_volatility = {}
            hour_samples = {}
            
            for hour, data in hour_data.items():
                returns = data["returns"]
                if returns:
                    hour_returns[hour] = sum(returns) / len(returns)
                    hour_volatility[hour] = (sum((r - hour_returns[hour]) ** 2 for r in returns) / len(returns)) ** 0.5
                    hour_samples[hour] = len(returns)
                    
            # Format hour labels
            formatted_returns = {f"{int(h):02d}:00": ret for h, ret in hour_returns.items()}
            formatted_volatility = {f"{int(h):02d}:00": vol for h, vol in hour_volatility.items()}
            formatted_samples = {f"{int(h):02d}:00": samp for h, samp in hour_samples.items()}
            
            return {
                "status": "success",
                "average_returns": formatted_returns,
                "volatility": formatted_volatility,
                "sample_counts": formatted_samples
            }
            
    async def get_monthly_pattern(self, symbol: str) -> Dict[str, Any]:
        """Get monthly pattern for a symbol"""
        async with self._lock:
            if symbol not in self.data:
                return {
                    "status": "no_data",
                    "message": "No data available for this symbol"
                }
                
            # Get month data
            month_data = self.data[symbol]["month"]
            
            # Check if we have enough data
            total_samples = sum(len(month["returns"]) for month in month_data.values())
            if total_samples < self.min_samples:
                return {
                    "status": "insufficient_data",
                    "message": f"Insufficient data ({total_samples} < {self.min_samples})"
                }
                
            # Calculate average returns by month
            month_returns = {}
            month_volatility = {}
            month_samples = {}
            
            for month, data in month_data.items():
                returns = data["returns"]
                if returns:
                    month_returns[month] = sum(returns) / len(returns)
                    month_volatility[month] = (sum((r - month_returns[month]) ** 2 for r in returns) / len(returns)) ** 0.5
                    month_samples[month] = len(returns)
                    
            # Convert month numbers to names
            month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            named_returns = {month_names[int(month)-1]: ret for month, ret in month_returns.items()}
            named_volatility = {month_names[int(month)-1]: vol for month, vol in month_volatility.items()}
            named_samples = {month_names[int(month)-1]: samp for month, samp in month_samples.items()}
            
            return {
                "status": "success",
                "average_returns": named_returns,
                "volatility": named_volatility,
                "sample_counts": named_samples
            }
            
    async def get_position_timing_score(self, symbol: str, direction: str) -> Dict[str, Any]:
        """Get timing score for a potential trade based on seasonal patterns"""
        async with self._lock:
            if symbol not in self.data:
                return {
                    "status": "no_data",
                    "message": "No data available for this symbol",
                    "score": 0.5  # Neutral score
                }
                
            # Get current time components
            now = datetime.now(timezone.utc)
            day_of_week = now.weekday()
            hour = now.hour
            month = now.month
            
            # Initialize weights
            weights = {
                "day_of_week": 0.4,
                "time_of_day": 0.4,
                "month": 0.2
            }
            
            # Initialize scores
            scores = {
                "day_of_week": 0.5,  # Neutral
                "time_of_day": 0.5,  # Neutral
                "month": 0.5         # Neutral
            }
            
            # Check day of week pattern
            if day_of_week in self.data[symbol]["day_of_week"]:
                returns = self.data[symbol]["day_of_week"][day_of_week]["returns"]
                if len(returns) >= 10:  # Minimum threshold for reliability
                    avg_return = sum(returns) / len(returns)
                    
                    # For BUY positions, positive returns are good
                    # For SELL positions, negative returns are good
                    if direction.upper() == "BUY":
                        if avg_return > 0:
                            # Positive on average, good for BUY
                            scores["day_of_week"] = min(1.0, 0.5 + (avg_return / 0.01))  # Scale: 1% return = score of 1.0
                        else:
                            # Negative on average, bad for BUY
                            scores["day_of_week"] = max(0.0, 0.5 + (avg_return / 0.01))
                    else:
                        # SELL positions
                        if avg_return < 0:
                            # Negative on average, good for SELL
                            scores["day_of_week"] = min(1.0, 0.5 - (avg_return / 0.01))
                        else:
                            # Positive on average, bad for SELL
                            scores["day_of_week"] = max(0.0, 0.5 - (avg_return / 0.01))
                            
            # Check time of day pattern
            if hour in self.data[symbol]["time_of_day"]:
                returns = self.data[symbol]["time_of_day"][hour]["returns"]
                if len(returns) >= 10:
                    avg_return = sum(returns) / len(returns)
                    
                    if direction.upper() == "BUY":
                        if avg_return > 0:
                            scores["time_of_day"] = min(1.0, 0.5 + (avg_return / 0.005))  # Scale: 0.5% return = score of 1.0
                        else:
                            scores["time_of_day"] = max(0.0, 0.5 + (avg_return / 0.005))
                    else:
                        if avg_return < 0:
                            scores["time_of_day"] = min(1.0, 0.5 - (avg_return / 0.005))
                        else:
                            scores["time_of_day"] = max(0.0, 0.5 - (avg_return / 0.005))
                            
            # Check monthly pattern
            if month in self.data[symbol]["month"]:
                returns = self.data[symbol]["month"][month]["returns"]
                if len(returns) >= 5:
                    avg_return = sum(returns) / len(returns)
                    
                    if direction.upper() == "BUY":
                        if avg_return > 0:
                            scores["month"] = min(1.0, 0.5 + (avg_return / 0.02))  # Scale: 2% return = score of 1.0
                        else:
                            scores["month"] = max(0.0, 0.5 + (avg_return / 0.02))
                    else:
                        if avg_return < 0:
                            scores["month"] = min(1.0, 0.5 - (avg_return / 0.02))
                        else:
                            scores["month"] = max(0.0, 0.5 - (avg_return / 0.02))
                            
            # Calculate weighted score
            total_weight = sum(weights.values())
            weighted_score = sum(scores[k] * weights[k] for k in weights) / total_weight
            
            # Calculate confidence
            sample_counts = {
                "day_of_week": len(self.data[symbol]["day_of_week"].get(day_of_week, {}).get("returns", [])),
                "time_of_day": len(self.data[symbol]["time_of_day"].get(hour, {}).get("returns", [])),
                "month": len(self.data[symbol]["month"].get(month, {}).get("returns", []))
            }
            
            avg_samples = sum(sample_counts.values()) / len(sample_counts)
            confidence = min(1.0, avg_samples / 20)  # 20+ samples = full confidence
            
            # Adjust score based on confidence
            # With low confidence, move closer to neutral (0.5)
            adjusted_score = 0.5 + (weighted_score - 0.5) * confidence
            
            return {
                "status": "success",
                "score": adjusted_score,
                "confidence": confidence,
                "component_scores": scores,
                "sample_counts": sample_counts,
                "reason": self._generate_reason_text(scores, direction)
            }
            
    def _generate_reason_text(self, scores: Dict[str, float], direction: str) -> str:
        """Generate human-readable explanation for the timing score"""
        components = []
        
        # Map score ranges to descriptions
        score_descriptions = {
            (0.0, 0.2): "very poor",
            (0.2, 0.4): "poor",
            (0.4, 0.6): "neutral",
            (0.6, 0.8): "good",
            (0.8, 1.0): "very good"
        }
        
        # Day of week
        day_score = scores["day_of_week"]
        day_desc = next((desc for (lower, upper), desc in score_descriptions.items() 
                         if lower <= day_score < upper), "neutral")
        components.append(f"day of week is {day_desc}")
        
        # Time of day
        hour_score = scores["time_of_day"]
        hour_desc = next((desc for (lower, upper), desc in score_descriptions.items() 
                         if lower <= hour_score < upper), "neutral")
        components.append(f"time of day is {hour_desc}")
        
        # Month
        month_score = scores["month"]
        month_desc = next((desc for (lower, upper), desc in score_descriptions.items() 
                          if lower <= month_score < upper), "neutral")
        components.append(f"month is {month_desc}")
        
        # Combine into reason text
        direction_text = "buying" if direction.upper() == "BUY" else "selling"
        return f"Seasonal analysis for {direction_text}: {', '.join(components)}"

class CrossAssetCorrelationTracker:
    """
    Tracks correlations between different assets to manage portfolio risk
    and identify potential diversification opportunities.
    """
    def __init__(self):
        """Initialize correlation tracker"""
        self.price_data = {}  # symbol -> price history
        self.correlations = {}  # symbol_pair -> correlation
        self.max_history = 100  # Maximum price points to keep
        self.correlation_threshold = 0.7  # Threshold for high correlation
        self._lock = asyncio.Lock()
        
    async def add_price_data(self, symbol: str, price: float):
        """Add price data for a symbol"""
        async with self._lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = []
                
            # Add price data
            self.price_data[symbol].append({
                "price": price,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            # Limit history size
            if len(self.price_data[symbol]) > self.max_history:
                self.price_data[symbol] = self.price_data[symbol][-self.max_history:]
                
            # Update correlations if we have enough data
            if len(self.price_data[symbol]) >= 30:
                await self._update_correlations(symbol)
                
    async def _update_correlations(self, symbol: str):
        """Update correlations for a symbol with all other tracked symbols"""
        try:
            # Get symbols that have enough data
            valid_symbols = [s for s, data in self.price_data.items() 
                           if len(data) >= 30 and s != symbol]
            
            if not valid_symbols:
                return
                
            # Get price returns for the target symbol
            target_prices = [p["price"] for p in self.price_data[symbol]]
            target_returns = [target_prices[i] / target_prices[i-1] - 1 
                            for i in range(1, len(target_prices))]
            
            # Calculate correlations with each other symbol
            for other_symbol in valid_symbols:
                # Get price returns for the other symbol
                other_prices = [p["price"] for p in self.price_data[other_symbol]]
                other_returns = [other_prices[i] / other_prices[i-1] - 1 
                               for i in range(1, len(other_prices))]
                
                # Ensure we have the same length of data
                min_length = min(len(target_returns), len(other_returns))
                if min_length < 20:  # Need at least 20 points for meaningful correlation
                    continue
                    
                # Use the most recent data
                target_returns_subset = target_returns[-min_length:]
                other_returns_subset = other_returns[-min_length:]
                
                # Calculate correlation
                correlation = self._calculate_correlation(target_returns_subset, other_returns_subset)
                
                # Store correlation (in both directions)
                pair_key = f"{symbol}_{other_symbol}"
                reverse_key = f"{other_symbol}_{symbol}"
                
                self.correlations[pair_key] = correlation
                self.correlations[reverse_key] = correlation
                
        except Exception as e:
            logger.error(f"Error updating correlations for {symbol}: {str(e)}")
            
    def _calculate_correlation(self, series1: List[float], series2: List[float]) -> float:
        """Calculate Pearson correlation coefficient between two series"""
        if len(series1) != len(series2) or len(series1) < 2:
            return 0.0
            
        n = len(series1)
        
        # Calculate means
        mean1 = sum(series1) / n
        mean2 = sum(series2) / n
        
        # Calculate variances and covariance
        var1 = sum((x - mean1) ** 2 for x in series1) / n
        var2 = sum((x - mean2) ** 2 for x in series2) / n
        cov = sum((series1[i] - mean1) * (series2[i] - mean2) for i in range(n)) / n
        
        # Calculate correlation
        std1 = var1 ** 0.5
        std2 = var2 ** 0.5
        
        if std1 == 0 or std2 == 0:
            return 0.0
            
        return cov / (std1 * std2)
        
    async def get_correlated_symbols(self, symbol: str, threshold: Optional[float] = None) -> List[Tuple[str, float]]:
        """Get symbols that are correlated with the given symbol"""
        async with self._lock:
            if symbol not in self.price_data:
                return []
                
            threshold = threshold or self.correlation_threshold
            
            # Find symbols with correlation above threshold
            correlated_symbols = []
            
            for pair_key, correlation in self.correlations.items():
                if pair_key.startswith(f"{symbol}_") and abs(correlation) >= threshold:
                    other_symbol = pair_key.split("_")[1]
                    correlated_symbols.append((other_symbol, correlation))
                    
            # Sort by correlation (highest first)
            correlated_symbols.sort(key=lambda x: abs(x[1]), reverse=True)
            
            return correlated_symbols
            
    async def calculate_portfolio_correlation(self, positions: Dict[str, float]) -> float:
        """Calculate average correlation within a portfolio"""
        async with self._lock:
            symbols = list(positions.keys())
            
            if len(symbols) < 2:
                return 0.0  # No correlation with only one symbol
                
            # Calculate weighted average correlation
            total_weight = 0.0
            weighted_correlation_sum = 0.0
            
            for i in range(len(symbols)):
                for j in range(i+1, len(symbols)):
                    sym1 = symbols[i]
                    sym2 = symbols[j]
                    
                    # Skip if we don't have correlation data
                    pair_key = f"{sym1}_{sym2}"
                    if pair_key not in self.correlations:
                        continue
                        
                    # Get correlation
                    correlation = abs(self.correlations[pair_key])
                    
                    # Calculate weight (product of position sizes)
                    weight = positions[sym1] * positions[sym2]
                    
                    # Add to weighted sum
                    weighted_correlation_sum += correlation * weight
                    total_weight += weight
                    
            # Calculate weighted average
            if total_weight > 0:
                return weighted_correlation_sum / total_weight
            else:
                return 0.0
                
    def get_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """Get correlation matrix for all tracked symbols"""
        matrix = {}
        
        # Get unique symbols
        symbols = list(self.price_data.keys())
        
        # Build matrix
        for sym1 in symbols:
            matrix[sym1] = {}
            
            for sym2 in symbols:
                if sym1 == sym2:
                    matrix[sym1][sym2] = 1.0  # Self-correlation is always 1.0
                else:
                    pair_key = f"{sym1}_{sym2}"
                    matrix[sym1][sym2] = self.correlations.get(pair_key, 0.0)
                    
        return matrix

class MarketRegimeExitStrategy:
    """
    Adapts exit strategies based on the current market regime
    and volatility conditions.
    """
    def __init__(self, volatility_monitor=None, regime_classifier=None):
        """Initialize market regime exit strategy"""
        self.volatility_monitor = volatility_monitor
        self.regime_classifier = regime_classifier
        self.exit_configs = {}  # position_id -> exit configuration
        self._lock = asyncio.Lock()
        
    async def initialize_exit_strategy(self,
                                     position_id: str,
                                     symbol: str,
                                     entry_price: float,
                                     direction: str,
                                     atr_value: float,
                                     market_regime: str) -> Dict[str, Any]:
        """Initialize exit strategy based on market regime"""
        async with self._lock:
            # Get volatility state if available
            volatility_ratio = 1.0
            volatility_state = "normal"
            
            if self.volatility_monitor:
                vol_data = self.volatility_monitor.get_volatility_state(symbol)
                volatility_ratio = vol_data.get("volatility_ratio", 1.0)
                volatility_state = vol_data.get("volatility_state", "normal")
                
            # Determine exit strategy based on market regime
            if "trending" in market_regime:
                config = self._trend_following_exits(entry_price, direction, atr_value, volatility_ratio)
            elif market_regime == "ranging":
                config = self._mean_reversion_exits(entry_price, direction, atr_value, volatility_ratio)
            elif market_regime == "volatile":
                config = self._volatile_market_exits(entry_price, direction, atr_value, volatility_ratio)
            else:  # mixed or unknown
                config = self._standard_exits(entry_price, direction, atr_value, volatility_ratio)
                
            # Store config
            self.exit_configs[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": direction,
                "atr_value": atr_value,
                "market_regime": market_regime,
                "volatility_ratio": volatility_ratio,
                "volatility_state": volatility_state,
                "exit_config": config,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            return config
            
    def _trend_following_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure exits for trending markets"""
        # In trending markets:
        # 1. Wider initial stop loss
        # 2. Aggressive trailing stop once in profit
        # 3. Extended take profit targets
        
        # Calculate stop loss distance (wider in trending markets)
        atr_multiplier = 2.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels (extend for trending markets)
            tp_level_1 = entry_price + (atr_value * 2.0 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 4.0 * volatility_ratio)
            tp_level_3 = entry_price + (atr_value * 6.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2, tp_level_3]
            
        else:  # SELL
            stop_loss = entry_price + (atr_value * atr_multiplier)
            
            # Take profit levels
            tp_level_1 = entry_price - (atr_value * 2.0 * volatility_ratio)
            tp_level_2 = entry_price - (atr_value * 4.0 * volatility_ratio)
            tp_level_3 = entry_price - (atr_value * 6.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2, tp_level_3]
            
        # Configure trailing stops
        trailing_config = {
            "activation": 1.0,  # Activate at 1X ATR
            "trail_distance": 2.0,  # Trail by 2X ATR
            "lock_profit_at": [0.5, 1.0, 1.5],  # Lock in profit at these levels
            "lock_percentages": [0.3, 0.6, 0.9]  # Percentage of profit to lock
        }
        
        # Return config
        return {
            "strategy": "trend_following",
            "stop_loss": stop_loss,
            "take_profit_levels": take_profit_levels,
            "trailing_config": trailing_config,
            "time_exit": None  # No time-based exit for trend following
        }
        
    def _mean_reversion_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure exits for ranging markets"""
        # In ranging markets:
        # 1. Tighter stop loss
        # 2. Closer take profit targets
        # 3. Time-based exit to prevent overstaying
        
        # Calculate stop loss distance (tighter in ranging markets)
        atr_multiplier = 1.5 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels (closer for mean reversion)
            tp_level_1 = entry_price + (atr_value * 1.0 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 2.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        else:  # SELL
            stop_loss = entry_price + (atr_value * atr_multiplier)
            
            # Take profit levels
            tp_level_1 = entry_price - (atr_value * 1.0 * volatility_ratio)
            tp_level_2 = entry_price - (atr_value * 2.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        # Configure trailing stops
        trailing_config = {
            "activation": 0.7,  # Activate sooner in ranging markets
            "trail_distance": 1.0,  # Tighter trail
            "lock_profit_at": [0.3, 0.7],  # Lock in profit at these levels
            "lock_percentages": [0.5, 0.9]  # Aggressive profit locking
        }
        
        # Configure time-based exit (for mean reversion)
        time_exit = {
            "max_hours": 24,  # Exit after 24 hours
            "if_profitable": True  # Only if trade is profitable
        }
        
        # Return config
        return {
            "strategy": "mean_reversion",
            "stop_loss": stop_loss,
            "take_profit_levels": take_profit_levels,
            "trailing_config": trailing_config,
            "time_exit": time_exit
        }
        
    def _volatile_market_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure exits for volatile markets"""
        # In volatile markets:
        # 1. Wider stop loss
        # 2. Quick take profit
        # 3. Aggressive profit protection
        
        # Calculate stop loss distance (wider in volatile markets)
        atr_multiplier = 3.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels (quick exit in volatile markets)
            tp_level_1 = entry_price + (atr_value * 1.5 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 3.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        else:  # SELL
            stop_loss = entry_price + (atr_value * atr_multiplier)
            
            # Take profit levels
            tp_level_1 = entry_price - (atr_value * 1.5 * volatility_ratio)
            tp_level_2 = entry_price - (atr_value * 3.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        # Configure trailing stops (very aggressive in volatile markets)
        trailing_config = {
            "activation": 0.5,  # Activate quickly
            "trail_distance": 1.5,  # Wider trail due to volatility
            "lock_profit_at": [0.3, 0.6],  # Lock in profit at these levels
            "lock_percentages": [0.7, 0.9]  # Very aggressive profit locking
        }
        
        # Configure time-based exit
        time_exit = {
            "max_hours": 12,  # Exit after 12 hours
            "if_profitable": False  # Exit regardless of profitability
        }
        
        # Return config
        return {
            "strategy": "volatile_market",
            "stop_loss": stop_loss,
            "take_profit_levels": take_profit_levels,
            "trailing_config": trailing_config,
            "time_exit": time_exit
        }
        
    def _standard_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure standard exits for mixed or unknown market regimes"""
        # Standard exit strategy:
        # 1. Balanced stop loss
        # 2. Standard take profit targets
        # 3. Normal trailing stop
        
        # Calculate stop loss distance
        atr_multiplier = 2.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels
            tp_level_1 = entry_price + (atr_value * 2.0 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 3.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        else:  # SELL
            stop_loss = entry_price + (atr_value * atr_multiplier)
            
            # Take profit levels
            tp_level_1 = entry_price - (atr_value * 2.0 * volatility_ratio)
            tp_level_2 = entry_price - (atr_value * 3.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        # Configure trailing stops
        trailing_config = {
            "activation": 1.0,  # Standard activation
            "trail_distance": 1.5,  # Standard trail distance
            "lock_profit_at": [0.5, 1.0],  # Standard profit locking levels
            "lock_percentages": [0.5, 0.8]  # Standard profit locking
        }
        
        # Return config
        return {
            "strategy": "standard",
            "stop_loss": stop_loss,
            "take_profit_levels": take_profit_levels,
            "trailing_config": trailing_config,
            "time_exit": None  # No time-based exit for standard strategy
        }

class TimeBasedTakeProfitManager:
    """
    Manages take profit levels that adjust based on time in trade,
    allowing for holding positions longer in trending markets.
    """
    def __init__(self):
        """Initialize time-based take profit manager"""
        self.take_profits = {}  # position_id -> take profit data
        self._lock = asyncio.Lock()
        
    async def initialize_take_profits(self,
                                    position_id: str,
                                    symbol: str,
                                    entry_price: float,
                                    direction: str,
                                    timeframe: str,
                                    atr_value: float) -> List[float]:
        """Initialize time-based take profit levels for a position"""
        async with self._lock:
            # Define time periods based on timeframe
            time_periods = self._get_time_periods(timeframe)
            
            # Define take profit levels for each time period
            if direction == "BUY":
                tp_levels = [
                    entry_price + (atr_value * 1.0),  # Short-term TP
                    entry_price + (atr_value * 2.0),  # Medium-term TP
                    entry_price + (atr_value * 3.5),  # Long-term TP
                ]
            else:  # SELL
                tp_levels = [
                    entry_price - (atr_value * 1.0),  # Short-term TP
                    entry_price - (atr_value * 2.0),  # Medium-term TP
                    entry_price - (atr_value * 3.5),  # Long-term TP
                ]
                
            # Store take profit data
            self.take_profits[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": direction,
                "timeframe": timeframe,
                "atr_value": atr_value,
                "time_periods": time_periods,
                "tp_levels": tp_levels,
                "created_at": datetime.now(timezone.utc),
                "last_updated": datetime.now(timezone.utc),
                "status": "active"
            }
            
            return tp_levels
            
    def _get_time_periods(self, timeframe: str) -> List[int]:
        """Get time periods (in hours) based on timeframe"""
        # Define time periods for different timeframes
        if timeframe in ["M1", "M5", "M15"]:
            return [2, 8, 24]  # Short-term: 2h, Medium: 8h, Long: 24h
        elif timeframe in ["M30", "H1"]:
            return [6, 24, 72]  # Short-term: 6h, Medium: 24h, Long: 72h
        elif timeframe in ["H4", "D1"]:
            return [24, 72, 168]  # Short-term: 24h, Medium: 72h, Long: 168h (1 week)
        else:
            return [12, 48, 120]  # Default periods
            
    async def get_current_take_profit(self, position_id: str) -> Optional[float]:
        """Get current take profit level based on time in trade"""
        async with self._lock:
            if position_id not in self.take_profits:
                return None
                
            tp_data = self.take_profits[position_id]
            
            # Check if position is still active
            if tp_data["status"] != "active":
                return None
                
            # Calculate time in trade
            time_in_trade = (datetime.now(timezone.utc) - tp_data["created_at"]).total_seconds() / 3600  # hours
            
            # Determine which take profit level to use based on time in trade
            time_periods = tp_data["time_periods"]
            tp_levels = tp_data["tp_levels"]
            
            if time_in_trade < time_periods[0]:
                # Short-term period
                return tp_levels[0]
            elif time_in_trade < time_periods[1]:
                # Medium-term period
                return tp_levels[1]
            else:
                # Long-term period
                return tp_levels[2]
                
    async def check_take_profit(self, position_id: str, current_price: float) -> bool:
        """Check if current price has reached the time-based take profit level"""
        async with self._lock:
            if position_id not in self.take_profits:
                return False
                
            tp_data = self.take_profits[position_id]
            
            # Check if position is still active
            if tp_data["status"] != "active":
                return False
                
            # Get current take profit level
            current_tp = await self.get_current_take_profit(position_id)
            
            if current_tp is None:
                return False
                
            # Check if take profit is reached
            if tp_data["direction"] == "BUY":
                return current_price >= current_tp
            else:  # SELL
                return current_price <= current_tp
                
    async def mark_closed(self, position_id: str):
        """Mark a position as closed"""
        async with self._lock:
            if position_id in self.take_profits:
                self.take_profits[position_id]["status"] = "closed"
                self.take_profits[position_id]["last_updated"] = datetime.now(timezone.utc)

class VolatilityAdjustedTrailingStop:
    """
    Implements a trailing stop that adjusts its distance based on
    current market volatility.
    """
    def __init__(self):
        """Initialize volatility-adjusted trailing stop"""
        self.trailing_stops = {}  # position_id -> trailing stop data
        self._lock = asyncio.Lock()
        
    async def initialize_trailing_stop(self,
                                     position_id: str,
                                     symbol: str,
                                     entry_price: float,
                                     direction: str,
                                     atr_value: float,
                                     volatility_state: str = "normal_volatility") -> float:
        """Initialize volatility-adjusted trailing stop for a position"""
        async with self._lock:
            # Define multipliers for different volatility states
            volatility_multipliers = {
                "low_volatility": 1.5,      # Tighter stop for low volatility
                "normal_volatility": 2.0,   # Standard stop distance
                "high_volatility": 3.0      # Wider stop for high volatility
            }
            
            # Get multiplier for current volatility state
            multiplier = volatility_multipliers.get(volatility_state, 2.0)
            
            # Calculate initial stop loss
            if direction == "BUY":
                stop_level = entry_price - (atr_value * multiplier)
            else:  # SELL
                stop_level = entry_price + (atr_value * multiplier)
                
            # Store trailing stop data
            self.trailing_stops[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": direction,
                "atr_value": atr_value,
                "volatility_state": volatility_state,
                "multiplier": multiplier,
                "initial_stop": stop_level,
                "current_stop": stop_level,
                "highest_price": entry_price if direction == "BUY" else entry_price,
                "lowest_price": entry_price if direction == "SELL" else entry_price,
                "activated": False,
                "active": True,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            
            return stop_level
            
    async def update_trailing_stop(self,
                                 position_id: str,
                                 current_price: float,
                                 current_atr: Optional[float] = None,
                                 current_volatility_state: Optional[str] = None) -> Dict[str, Any]:
        """Update trailing stop based on current price and volatility"""
        async with self._lock:
            if position_id not in self.trailing_stops:
                return {
                    "status": "error",
                    "message": "Trailing stop not initialized for this position"
                }
                
            ts_data = self.trailing_stops[position_id]
            
            # Check if trailing stop is active
            if not ts_data["active"]:
                return {
                    "status": "inactive",
                    "stop_level": ts_data["current_stop"]
                }
                
            # Update ATR if provided
            if current_atr:
                ts_data["atr_value"] = current_atr
                
            # Update volatility state and multiplier if provided
            if current_volatility_state:
                ts_data["volatility_state"] = current_volatility_state
                
                # Update multiplier based on new volatility state
                volatility_multipliers = {
                    "low_volatility": 1.5,
                    "normal_volatility": 2.0,
                    "high_volatility": 3.0
                }
                ts_data["multiplier"] = volatility_multipliers.get(current_volatility_state, 2.0)
                
            # Calculate activation level (when trailing stop starts moving)
            # Typically activate after 1 ATR of movement in favorable direction
            if not ts_data["activated"]:
                if ts_data["direction"] == "BUY":
                    activation_level = ts_data["entry_price"] + ts_data["atr_value"]
                    if current_price >= activation_level:
                        ts_data["activated"] = True
                else:  # SELL
                    activation_level = ts_data["entry_price"] - ts_data["atr_value"]
                    if current_price <= activation_level:
                        ts_data["activated"] = True
                        
            # Update highest/lowest prices seen
            if ts_data["direction"] == "BUY":
                if current_price > ts_data["highest_price"]:
                    ts_data["highest_price"] = current_price
            else:  # SELL
                if current_price < ts_data["lowest_price"]:
                    ts_data["lowest_price"] = current_price
                    
            # Update trailing stop if activated
            if ts_data["activated"]:
                if ts_data["direction"] == "BUY":
                    # Calculate new stop level based on highest price and current volatility
                    new_stop = ts_data["highest_price"] - (ts_data["atr_value"] * ts_data["multiplier"])
                    
                    # Only move stop up, never down
                    if new_stop > ts_data["current_stop"]:
                        ts_data["current_stop"] = new_stop
                        ts_data["updated_at"] = datetime.now(timezone.utc)
                        
                else:  # SELL
                    # Calculate new stop level
                    new_stop = ts_data["lowest_price"] + (ts_data["atr_value"] * ts_data["multiplier"])
                    
                    # Only move stop down, never up
                    if new_stop < ts_data["current_stop"]:
                        ts_data["current_stop"] = new_stop
                        ts_data["updated_at"] = datetime.now(timezone.utc)
                        
            # Check if stop is hit
            stop_hit = False
            if ts_data["direction"] == "BUY":
                stop_hit = current_price <= ts_data["current_stop"]
            else:  # SELL
                stop_hit = current_price >= ts_data["current_stop"]
                
            # Return result
            return {
                "status": "hit" if stop_hit else "active",
                "activated": ts_data["activated"],
                "stop_level": ts_data["current_stop"],
                "initial_stop": ts_data["initial_stop"],
                "entry_price": ts_data["entry_price"],
                "direction": ts_data["direction"],
                "price_extreme": ts_data["highest_price"] if ts_data["direction"] == "BUY" else ts_data["lowest_price"],
                "updated_at": ts_data["updated_at"].isoformat()
            }
            
    async def mark_closed(self, position_id: str):
        """Mark a trailing stop as closed"""
        async with self._lock:
            if position_id in self.trailing_stops:
                self.trailing_stops[position_id]["active"] = False
                self.trailing_stops[position_id]["updated_at"] = datetime.now(timezone.utc)
                
    def get_trailing_stop(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get trailing stop data for a position"""
        if position_id not in self.trailing_stops:
            return None
            
        ts_data = self.trailing_stops[position_id].copy()
        
        # Convert datetime objects to strings
        for key in ["created_at", "updated_at"]:
            if isinstance(ts_data[key], datetime):
                ts_data[key] = ts_data[key].isoformat()
                
        return ts_data

##############################################################################
# Hedged Positions
##############################################################################

class HedgeManager:
    """
    Manages hedged positions to reduce risk in correlated instruments
    or implement complex strategies like grid trading.
    """
    def __init__(self):
        """Initialize hedge manager"""
        self.hedges = {}  # hedge_id -> hedge data
        self.position_map = {}  # position_id -> hedge_id
        self._lock = asyncio.Lock()
        
    async def create_hedge(self,
                         symbol: str,
                         primary_direction: str,
                         hedge_id: Optional[str] = None,
                         metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a new hedged position"""
        async with self._lock:
            # Generate hedge ID if not provided
            if not hedge_id:
                hedge_id = f"hedge_{uuid.uuid4()}"
                
            # Create hedge
            self.hedges[hedge_id] = {
                "hedge_id": hedge_id,
                "symbol": symbol,
                "primary_direction": primary_direction,
                "positions": {},
                "net_size": 0.0,
                "status": "open",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {}
            }
            
            logger.info(f"Created hedge {hedge_id} for {symbol}")
            return hedge_id
            
    async def add_position_to_hedge(self,
                                  hedge_id: str,
                                  position_id: str,
                                  direction: str,
                                  size: float,
                                  entry_price: float,
                                  metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a position to a hedge"""
        async with self._lock:
            if hedge_id not in self.hedges:
                logger.error(f"Hedge {hedge_id} not found")
                return False
                
            # Add position to hedge
            self.hedges[hedge_id]["positions"][position_id] = {
                "position_id": position_id,
                "direction": direction,
                "size": size,
                "entry_price": entry_price,
                "metadata": metadata or {},
                "added_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Map position to hedge
            self.position_map[position_id] = hedge_id
            
            # Update net size
            self._update_net_size(hedge_id)
            
            logger.info(f"Added position {position_id} to hedge {hedge_id}")
            return True
            
    async def remove_position_from_hedge(self, position_id: str) -> bool:
        """Remove a position from its hedge"""
        async with self._lock:
            if position_id not in self.position_map:
                logger.error(f"Position {position_id} not mapped to any hedge")
                return False
                
            hedge_id = self.position_map[position_id]
            
            if hedge_id not in self.hedges:
                logger.error(f"Hedge {hedge_id} not found")
                return False
                
            # Remove position from hedge
            if position_id in self.hedges[hedge_id]["positions"]:
                del self.hedges[hedge_id]["positions"][position_id]
                
            # Remove position mapping
            del self.position_map[position_id]
            
            # Update net size
            self._update_net_size(hedge_id)
            
            # Check if hedge is empty
            if not self.hedges[hedge_id]["positions"]:
                self.hedges[hedge_id]["status"] = "closed"
                
            logger.info(f"Removed position {position_id} from hedge {hedge_id}")
            return True
            
    def _update_net_size(self, hedge_id: str):
        """Update the net size of a hedge"""
        if hedge_id not in self.hedges:
            return
            
        net_size = 0.0
        
        for position_id, position in self.hedges[hedge_id]["positions"].items():
            if position["direction"] == self.hedges[hedge_id]["primary_direction"]:
                net_size += position["size"]
            else:
                net_size -= position["size"]
                
        self.hedges[hedge_id]["net_size"] = net_size
        self.hedges[hedge_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
        
    async def get_hedge_by_id(self, hedge_id: str) -> Optional[Dict[str, Any]]:
        """Get hedge data by ID"""
        async with self._lock:
            if hedge_id not in self.hedges:
                return None
                
            return self.hedges[hedge_id]
            
    async def get_hedge_for_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get hedge data for a position"""
        async with self._lock:
            if position_id not in self.position_map:
                return None
                
            hedge_id = self.position_map[position_id]
            
            if hedge_id not in self.hedges:
                return None
                
            return self.hedges[hedge_id]
            
    async def get_all_hedges(self) -> List[Dict[str, Any]]:
        """Get all hedges"""
        async with self._lock:
            return list(self.hedges.values())
            
    async def get_active_hedges(self) -> List[Dict[str, Any]]:
        """Get active hedges"""
        async with self._lock:
            return [h for h in self.hedges.values() if h["status"] == "open"]
            
    async def rebalance_hedge(self, hedge_id: str, target_net_size: float) -> Dict[str, Any]:
        """Rebalance a hedge to achieve a target net size"""
        async with self._lock:
            if hedge_id not in self.hedges:
                return {
                    "status": "error",
                    "message": f"Hedge {hedge_id} not found"
                }
                
            hedge = self.hedges[hedge_id]
            current_net_size = hedge["net_size"]
            
            # Calculate size adjustment needed
            adjustment_needed = target_net_size - current_net_size
            
            if abs(adjustment_needed) < 0.001:
                return {
                    "status": "success",
                    "message": "Hedge already balanced",
                    "current_net_size": current_net_size,
                    "target_net_size": target_net_size,
                    "adjustment_needed": 0
                }
                
            # Determine action needed
            if adjustment_needed > 0:
                # Need to increase net size (add to primary direction or reduce opposite)
                action = "increase"
            else:
                # Need to decrease net size (reduce primary direction or add to opposite)
                action = "decrease"
                
            # This is where you would implement the actual rebalancing logic
            # For now, return the adjustment plan
            
            return {
                "status": "plan",
                "hedge_id": hedge_id,
                "symbol": hedge["symbol"],
                "current_net_size": current_net_size,
                "target_net_size": target_net_size,
                "adjustment_needed": adjustment_needed,
                "action": action,
                "primary_direction": hedge["primary_direction"]
            }
            
    async def close_hedge(self, hedge_id: str) -> bool:
        """Mark a hedge as closed"""
        async with self._lock:
            if hedge_id not in self.hedges:
                logger.error(f"Hedge {hedge_id} not found")
                return False
                
            self.hedges[hedge_id]["status"] = "closed"
            self.hedges[hedge_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
            
            logger.info(f"Closed hedge {hedge_id}")
            return True

##############################################################################
# Position Journal
##############################################################################

class PositionJournal:
    """
    Keeps a detailed journal of all trading activity with performance metrics,
    annotations, and post-trade analysis.
    """
    def __init__(self):
        """Initialize position journal"""
        self.entries = {}  # position_id -> journal entries
        self.statistics = {
            "total_entries": 0,
            "total_exits": 0,
            "position_count": 0,
            "win_count": 0,
            "loss_count": 0
        }
        self._lock = asyncio.Lock()
        
    async def record_entry(self,
                         position_id: str,
                         symbol: str,
                         action: str,
                         timeframe: str,
                         entry_price: float,
                         size: float,
                         strategy: str,
                         execution_time: float = 0.0,
                         slippage: float = 0.0,
                         stop_loss: Optional[float] = None,
                         take_profit: Optional[float] = None,
                         market_regime: str = "unknown",
                         volatility_state: str = "normal",
                         metadata: Optional[Dict[str, Any]] = None):
        """Record a position entry in the journal"""
        async with self._lock:
            # Create entry if it doesn't exist
            if position_id not in self.entries:
                self.entries[position_id] = {
                    "position_id": position_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "strategy": strategy,
                    "journal": [],
                    "position_status": "open",
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                self.statistics["position_count"] += 1
                
            # Add entry to journal
            entry_record = {
                "type": "entry",
                "action": action,
                "price": entry_price,
                "size": size,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_time": execution_time,
                "slippage": slippage,
                "market_regime": market_regime,
                "volatility_state": volatility_state,
                "metadata": metadata or {}
            }
            
            self.entries[position_id]["journal"].append(entry_record)
            self.statistics["total_entries"] += 1
            
            logger.info(f"Recorded entry for position {position_id}")
            
    async def record_exit(self,
                        position_id: str,
                        exit_price: float,
                        exit_reason: str,
                        pnl: float,
                        execution_time: float = 0.0,
                        slippage: float = 0.0,
                        market_regime: str = "unknown",
                        volatility_state: str = "normal",
                        metadata: Optional[Dict[str, Any]] = None):
        """Record a position exit in the journal"""
        async with self._lock:
            if position_id not in self.entries:
                logger.error(f"Position {position_id} not found in journal")
                return
                
            # Add exit to journal
            exit_record = {
                "type": "exit",
                "price": exit_price,
                "reason": exit_reason,
                "pnl": pnl,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_time": execution_time,
                "slippage": slippage,
                "market_regime": market_regime,
                "volatility_state": volatility_state,
                "metadata": metadata or {}
            }
            
            self.entries[position_id]["journal"].append(exit_record)
            self.entries[position_id]["position_status"] = "closed"
            
            # Update statistics
            self.statistics["total_exits"] += 1
            
            if pnl > 0:
                self.statistics["win_count"] += 1
            else:
                self.statistics["loss_count"] += 1
                
            logger.info(f"Recorded exit for position {position_id}")
            
    async def add_note(self,
                     position_id: str,
                     note: str,
                     note_type: str = "general",
                     metadata: Optional[Dict[str, Any]] = None):
        """Add a note to a position journal"""
        async with self._lock:
            if position_id not in self.entries:
                logger.error(f"Position {position_id} not found in journal")
                return
                
            # Add note to journal
            note_record = {
                "type": "note",
                "note_type": note_type,
                "text": note,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {}
            }
            
            self.entries[position_id]["journal"].append(note_record)
            logger.info(f"Added {note_type} note to position {position_id}")
            
    async def record_adjustment(self,
                              position_id: str,
                              adjustment_type: str,
                              old_value: Any,
                              new_value: Any,
                              reason: str,
                              metadata: Optional[Dict[str, Any]] = None):
        """Record a position adjustment (stop loss, take profit, etc.)"""
        async with self._lock:
            if position_id not in self.entries:
                logger.error(f"Position {position_id} not found in journal")
                return
                
            # Add adjustment to journal
            adjustment_record = {
                "type": "adjustment",
                "adjustment_type": adjustment_type,
                "old_value": old_value,
                "new_value": new_value,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {}
            }
            
            self.entries[position_id]["journal"].append(adjustment_record)
            logger.info(f"Recorded {adjustment_type} adjustment for position {position_id}")
            
    async def get_position_journal(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get the journal for a position"""
        async with self._lock:
            if position_id not in self.entries:
                return None
                
            return self.entries[position_id]
            
    async def get_all_entries(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all journal entries, sorted by creation time"""
        async with self._lock:
            # Sort entries by creation time (newest first)
            sorted_entries = sorted(
                self.entries.values(),
                key=lambda x: x.get("created_at", ""),
                reverse=True
            )
            
            # Apply pagination
            return sorted_entries[offset:offset+limit]
            
    async def get_statistics(self) -> Dict[str, Any]:
        """Get journal statistics"""
        async with self._lock:
            # Calculate win rate
            win_rate = 0.0
            if self.statistics["win_count"] + self.statistics["loss_count"] > 0:
                win_rate = (self.statistics["win_count"] / 
                          (self.statistics["win_count"] + self.statistics["loss_count"])) * 100
                
            # Return statistics
            return {
                "total_positions": self.statistics["position_count"],
                "open_positions": self.statistics["position_count"] - (self.statistics["win_count"] + self.statistics["loss_count"]),
                "closed_positions": self.statistics["win_count"] + self.statistics["loss_count"],
                "winning_positions": self.statistics["win_count"],
                "losing_positions": self.statistics["loss_count"],
                "win_rate": win_rate,
                "total_entries": self.statistics["total_entries"],
                "total_exits": self.statistics["total_exits"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
    async def analyze_performance_by_factor(self, factor: str) -> Dict[str, Any]:
        """Analyze performance grouped by a specific factor (strategy, market_regime, etc.)"""
        async with self._lock:
            # Get closed positions
            closed_positions = [p for p in self.entries.values() 
                              if p["position_status"] == "closed"]
            
            if not closed_positions:
                return {
                    "status": "no_data",
                    "message": "No closed positions to analyze"
                }
                
            # Extract factor values and performance
            factor_performance = {}
            
            for position in closed_positions:
                # Find entry and exit records
                entry_record = next((r for r in position["journal"] if r["type"] == "entry"), None)
                exit_record = next((r for r in position["journal"] if r["type"] == "exit"), None)
                
                if not entry_record or not exit_record:
                    continue
                    
                # Get factor value
                if factor == "strategy":
                    factor_value = position.get("strategy", "unknown")
                elif factor == "market_regime":
                    factor_value = entry_record.get("market_regime", "unknown")
                elif factor == "volatility_state":
                    factor_value = entry_record.get("volatility_state", "normal")
                elif factor == "exit_reason":
                    factor_value = exit_record.get("reason", "unknown")
                elif factor == "symbol":
                    factor_value = position.get("symbol", "unknown")
                elif factor == "timeframe":
                    factor_value = position.get("timeframe", "unknown")
                else:
                    # Default to metadata
                    factor_value = entry_record.get("metadata", {}).get(factor, "unknown")
                    
                # Initialize factor stats if needed
                if factor_value not in factor_performance:
                    factor_performance[factor_value] = {
                        "count": 0,
                        "wins": 0,
                        "losses": 0,
                        "total_pnl": 0.0,
                        "avg_pnl": 0.0,
                        "win_rate": 0.0
                    }
                    
                # Update factor stats
                stats = factor_performance[factor_value]
                stats["count"] += 1
                
                pnl = exit_record.get("pnl", 0.0)
                stats["total_pnl"] += pnl
                
                if pnl > 0:
                    stats["wins"] += 1
                else:
                    stats["losses"] += 1
                    
                # Calculate average and win rate
                stats["avg_pnl"] = stats["total_pnl"] / stats["count"]
                
                if stats["wins"] + stats["losses"] > 0:
                    stats["win_rate"] = (stats["wins"] / (stats["wins"] + stats["losses"])) * 100
                    
            return {
                "status": "success",
                "factor": factor,
                "performance": factor_performance
            }

class BackupManager:
    """
    Manages database and position data backups
    """
    def __init__(self, db_manager=None):
        """Initialize backup manager"""
        self.db_manager = db_manager
        self.backup_dir = config.backup_dir
        self.last_backup_time = None
        self._lock = asyncio.Lock()
        
        # Create backup directory if it doesn't exist
        os.makedirs(self.backup_dir, exist_ok=True)
        
    async def create_backup(self, include_market_data: bool = False, compress: bool = True) -> bool:
        """Create a backup of the database and optionally market data"""
        async with self._lock:
            try:
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                backup_basename = f"trading_system_backup_{timestamp}"
                
                # Create a subdirectory for this backup
                backup_subdir = os.path.join(self.backup_dir, backup_basename)
                os.makedirs(backup_subdir, exist_ok=True)
                
                # Database backup
                if self.db_manager:
                    db_backup_path = os.path.join(backup_subdir, "database.db")
                    db_backed_up = await self.db_manager.backup_database(db_backup_path)
                    if not db_backed_up:
                        logger.error("Failed to backup database")
                        return False
                
                # Backup position data as JSON (just in case)
                if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'position_tracker'):
                    # Get all positions
                    all_positions = await alert_handler.position_tracker.get_all_positions()
                    
                    # Save to JSON file
                    positions_backup_path = os.path.join(backup_subdir, "positions.json")
                    with open(positions_backup_path, 'w') as f:
                        json.dump(all_positions, f, indent=2)
                    
                    logger.info(f"Backed up {len(all_positions)} positions to {positions_backup_path}")
                
                # Backup market data if requested
                if include_market_data and 'alert_handler' in globals() and alert_handler:
                    market_data = {}
                    
                    # Backup volatility data if available
                    if hasattr(alert_handler, 'volatility_monitor'):
                        market_data['volatility'] = alert_handler.volatility_monitor.get_all_volatility_states()
                    
                    # Backup regime data if available
                    if hasattr(alert_handler, 'regime_classifier'):
                        market_data['regimes'] = {}
                        if hasattr(alert_handler.regime_classifier, 'regimes'):
                            for symbol, regime_data in alert_handler.regime_classifier.regimes.items():
                                # Convert datetime to string if needed
                                if isinstance(regime_data.get('last_update'), datetime):
                                    regime_data = regime_data.copy()
                                    regime_data['last_update'] = regime_data['last_update'].isoformat()
                                market_data['regimes'][symbol] = regime_data
                    
                    # Save market data to JSON file
                    market_data_path = os.path.join(backup_subdir, "market_data.json")
                    with open(market_data_path, 'w') as f:
                        json.dump(market_data, f, indent=2)
                        
                    logger.info(f"Backed up market data to {market_data_path}")
                
                # Compress backup if requested
                if compress:
                    # Create a tar.gz archive
                    archive_path = os.path.join(self.backup_dir, f"{backup_basename}.tar.gz")
                    with tarfile.open(archive_path, "w:gz") as tar:
                        tar.add(backup_subdir, arcname=os.path.basename(backup_subdir))
                    
                    # Remove the uncompressed directory
                    import shutil
                    shutil.rmtree(backup_subdir)
                    
                    logger.info(f"Created compressed backup at {archive_path}")
                
                self.last_backup_time = datetime.now(timezone.utc)
                return True
                
            except Exception as e:
                logger.error(f"Error creating backup: {str(e)}")
                logger.error(traceback.format_exc())
                return False
    
    async def restore_from_backup(self, backup_path: str) -> bool:
        """Restore database and position data from a backup"""
        async with self._lock:
            try:
                # Check if the backup is compressed
                is_compressed = backup_path.endswith('.tar.gz')
                extract_dir = None
                
                if is_compressed:
                    # Extract the archive
                    extract_dir = os.path.join(self.backup_dir, "restore_temp")
                    os.makedirs(extract_dir, exist_ok=True)
                    
                    with tarfile.open(backup_path, "r:gz") as tar:
                        tar.extractall(path=extract_dir)
                    
                    # Find the extracted directory
                    subdirs = [d for d in os.listdir(extract_dir) if os.path.isdir(os.path.join(extract_dir, d))]
                    if not subdirs:
                        logger.error("No directories found in backup archive")
                        return False
                    
                    backup_dir = os.path.join(extract_dir, subdirs[0])
                else:
                    backup_dir = backup_path
                
                # Close existing database connection
                if self.db_manager:
                    await self.db_manager.close()
                
                # Replace the database file
                db_backup_path = os.path.join(backup_dir, "database.db")
                if os.path.exists(db_backup_path):
                    db_path = self.db_manager.db_url.replace("sqlite:///", "")
                    import shutil
                    shutil.copy2(db_backup_path, db_path)
                    
                    # Reopen the database connection
                    await self.db_manager.initialize()
                    logger.info("Restored database from backup")
                
                # Load positions from JSON if alert_handler exists
                positions_path = os.path.join(backup_dir, "positions.json")
                if os.path.exists(positions_path) and 'alert_handler' in globals() and alert_handler:
                    with open(positions_path, 'r') as f:
                        positions_data = json.load(f)
                    
                    # Clear existing positions
                    position_tracker = alert_handler.position_tracker
                    position_tracker.positions = {}
                    position_tracker.open_positions_by_symbol = {}
                    position_tracker.closed_positions = {}
                    position_tracker.position_history = []
                    
                    # Restore positions
                    for position_id, position_data in positions_data.items():
                        await position_tracker.restore_position(position_id, position_data)
                    
                    logger.info(f"Restored {len(positions_data)} positions from backup")
                
                # Load market data if available
                market_data_path = os.path.join(backup_dir, "market_data.json")
                if os.path.exists(market_data_path) and 'alert_handler' in globals() and alert_handler:
                    with open(market_data_path, 'r') as f:
                        market_data = json.load(f)
                    
                    # Restore volatility data
                    if 'volatility' in market_data and hasattr(alert_handler, 'volatility_monitor'):
                        alert_handler.volatility_monitor.market_conditions = market_data['volatility']
                    
                    # Restore regime data
                    if 'regimes' in market_data and hasattr(alert_handler, 'regime_classifier'):
                        if not hasattr(alert_handler.regime_classifier, 'regimes'):
                            alert_handler.regime_classifier.regimes = {}
                        
                        for symbol, regime_data in market_data['regimes'].items():
                            alert_handler.regime_classifier.regimes[symbol] = regime_data
                    
                    logger.info("Restored market data from backup")
                
                # Clean up extracted files if needed
                if extract_dir and os.path.exists(extract_dir):
                    import shutil
                    shutil.rmtree(extract_dir)
                
                return True
                
            except Exception as e:
                logger.error(f"Error restoring from backup: {str(e)}")
                logger.error(traceback.format_exc())
                return False
    
    async def list_backups(self) -> List[Dict[str, Any]]:
        """List available backups"""
        try:
            backups = []
            
            # Check for compressed backups
            compressed_pattern = os.path.join(self.backup_dir, "trading_system_backup_*.tar.gz")
            for backup_path in glob.glob(compressed_pattern):
                filename = os.path.basename(backup_path)
                # Extract timestamp from filename
                timestamp_str = re.search(r"trading_system_backup_(\d+_\d+)", filename)
                timestamp = None
                if timestamp_str:
                    try:
                        timestamp = datetime.strptime(timestamp_str.group(1), "%Y%m%d_%H%M%S")
                    except ValueError:
                        pass
                
                backups.append({
                    "filename": filename,
                    "path": backup_path,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                    "size": os.path.getsize(backup_path),
                    "type": "compressed"
                })
            
            # Check for uncompressed backups
            uncompressed_pattern = os.path.join(self.backup_dir, "trading_system_backup_*")
            for backup_path in glob.glob(uncompressed_pattern):
                if os.path.isdir(backup_path) and not backup_path.endswith(".tar.gz"):
                    dirname = os.path.basename(backup_path)
                    # Extract timestamp from dirname
                    timestamp_str = re.search(r"trading_system_backup_(\d+_\d+)", dirname)
                    timestamp = None
                    if timestamp_str:
                        try:
                            timestamp = datetime.strptime(timestamp_str.group(1), "%Y%m%d_%H%M%S")
                        except ValueError:
                            pass
                    
                    # Calculate directory size
                    total_size = 0
                    for dirpath, dirnames, filenames in os.walk(backup_path):
                        for f in filenames:
                            fp = os.path.join(dirpath, f)
                            total_size += os.path.getsize(fp)
                    
                    backups.append({
                        "filename": dirname,
                        "path": backup_path,
                        "timestamp": timestamp.isoformat() if timestamp else None,
                        "size": total_size,
                        "type": "directory"
                    })
            
            # Sort by timestamp (newest first)
            backups.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            return backups
            
        except Exception as e:
            logger.error(f"Error listing backups: {str(e)}")
            return []
    
    async def cleanup_old_backups(self, max_age_days: int = 30, keep_min: int = 5):
        """Clean up old backups to save disk space"""
        try:
            backups = await self.list_backups()
            
            # Sort by timestamp (oldest first)
            backups.sort(key=lambda x: x.get("timestamp", ""))
            
            # Calculate cutoff date
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
            
            # Find backups to delete (older than cutoff_date but keep at least keep_min)
            backups_to_delete = []
            for backup in backups[:-keep_min] if len(backups) > keep_min else []:
                if backup.get("timestamp", "") < cutoff_date:
                    backups_to_delete.append(backup)
            
            # Delete old backups
            for backup in backups_to_delete:
                path = backup["path"]
                try:
                    if backup["type"] == "compressed":
                        # Remove file
                        os.remove(path)
                    else:
                        # Remove directory
                        import shutil
                        shutil.rmtree(path)
                    
                    logger.info(f"Removed old backup: {os.path.basename(path)}")
                except Exception as e:
                    logger.error(f"Error removing backup {path}: {str(e)}")
            
            return len(backups_to_delete)
            
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {str(e)}")
            return 0
    
    async def schedule_backups(self, interval_hours: int = 24):
        """Schedule regular backups"""
        logger.info(f"Scheduling automatic backups every {interval_hours} hours")
        
        while True:
            try:
                # Create a backup
                success = await self.create_backup(include_market_data=True, compress=True)
                if success:
                    logger.info("Automatic backup created successfully")
                    
                    # Clean up old backups
                    deleted_count = await self.cleanup_old_backups()
                    if deleted_count > 0:
                        logger.info(f"Cleaned up {deleted_count} old backups")
                
                # Wait for the next backup
                await asyncio.sleep(interval_hours * 3600)
                
            except Exception as e:
                logger.error(f"Error in scheduled backup: {str(e)}")
                # Wait a bit before retrying
                await asyncio.sleep(3600)  # 1 hour

class EnhancedAlertHandler:
    """
    Enhanced alert handler with support for database persistence
    """
    def __init__(self):
        """Initialize alert handler"""
        # Initialize components
        self.position_tracker = None
        self.risk_manager = None
        self.volatility_monitor = None
        self.regime_classifier = None
        self.multi_stage_tp_manager = None
        self.time_based_exit_manager = None
        self.dynamic_exit_manager = None
        self.position_journal = None
        self.notification_system = None
        self.system_monitor = None
        
        # Track active alerts
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        
    async def start(self):
        """Initialize and start all components"""
        if self._running:
            return True
            
        try:
            # Initialize system monitor first for component tracking
            self.system_monitor = SystemMonitor()
            await self.system_monitor.register_component("alert_handler", "initializing")
            
            # Initialize position tracker with database
            self.position_tracker = PositionTracker(db_manager=db_manager)
            await self.system_monitor.register_component("position_tracker", "initializing")
            
            # Initialize risk manager
            self.risk_manager = EnhancedRiskManager()
            await self.system_monitor.register_component("risk_manager", "initializing")
            
            # Initialize market analysis components
            self.volatility_monitor = VolatilityMonitor()
            await self.system_monitor.register_component("volatility_monitor", "initializing")
            
            self.regime_classifier = MarketRegimeClassifier()
            await self.system_monitor.register_component("regime_classifier", "initializing")
            
            # Initialize exit management components
            self.multi_stage_tp_manager = MultiStageTakeProfitManager(position_tracker=self.position_tracker)
            await self.system_monitor.register_component("multi_stage_tp_manager", "initializing")
            
            self.time_based_exit_manager = TimeBasedExitManager()
            await self.system_monitor.register_component("time_based_exit_manager", "initializing")
            
            self.dynamic_exit_manager = DynamicExitManager(
                position_tracker=self.position_tracker,
                multi_stage_tp_manager=self.multi_stage_tp_manager
            )
            await self.system_monitor.register_component("dynamic_exit_manager", "initializing")
            
            # Initialize position journal
            self.position_journal = PositionJournal()
            await self.system_monitor.register_component("position_journal", "initializing")
            
            # Initialize notification system
            self.notification_system = NotificationSystem()
            await self.system_monitor.register_component("notification_system", "initializing")
            
            # Configure notification channels
            if config.slack_webhook_url:
                await self.notification_system.configure_channel("slack", {"webhook_url": config.slack_webhook_url})
            
            if config.telegram_bot_token and config.telegram_chat_id:
                await self.notification_system.configure_channel("telegram", {
                    "bot_token": config.telegram_bot_token,
                    "chat_id": config.telegram_chat_id
                })
                
            # Always configure console notification
            await self.notification_system.configure_channel("console", {})
            
            # Start components
            await self.position_tracker.start()
            await self.system_monitor.update_component_status("position_tracker", "ok")
            
            # Initialize risk manager with account balance
            account_balance = await get_account_balance()
            await self.risk_manager.initialize(account_balance)
            await self.system_monitor.update_component_status("risk_manager", "ok")
            
            # Start exit managers
            await self.time_based_exit_manager.start()
            await self.system_monitor.update_component_status("time_based_exit_manager", "ok")
            
            await self.dynamic_exit_manager.start()
            await self.system_monitor.update_component_status("dynamic_exit_manager", "ok")
            
            # Mark other components as ready
            await self.system_monitor.update_component_status("volatility_monitor", "ok")
            await self.system_monitor.update_component_status("regime_classifier", "ok")
            await self.system_monitor.update_component_status("multi_stage_tp_manager", "ok")
            await self.system_monitor.update_component_status("position_journal", "ok")
            await self.system_monitor.update_component_status("notification_system", "ok")
            
            # Check for any database inconsistencies and repair them
            await self.position_tracker.clean_up_duplicate_positions()
            
            # Mark alert handler as running
            self._running = True
            await self.system_monitor.update_component_status("alert_handler", "ok")
            
            # Send startup notification
            await self.notification_system.send_notification(
                f"Trading system started successfully with {len(self.position_tracker.positions)} open positions",
                "info"
            )
            
            logger.info("Alert handler started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting alert handler: {str(e)}")
            logger.error(traceback.format_exc())
            
            if self.system_monitor:
                await self.system_monitor.update_component_status(
                    "alert_handler", 
                    "error",
                    f"Failed to start: {str(e)}"
                )
                
            return False
            
    async def stop(self):
        """Stop all components"""
        if not self._running:
            return True
            
        try:
            # Update status
            if self.system_monitor:
                await self.system_monitor.update_component_status("alert_handler", "shutting_down")
                
            # Send notification
            if self.notification_system:
                await self.notification_system.send_notification(
                    "Trading system shutting down",
                    "info"
                )
                
            # Ensure all position data is saved to database
            if self.position_tracker:
                await self.position_tracker.sync_with_database()
                await self.position_tracker.stop()
                
            # Stop other components
            if self.dynamic_exit_manager:
                await self.dynamic_exit_manager.stop()
                
            if self.time_based_exit_manager:
                await self.time_based_exit_manager.stop()
                
            # Mark as not running
            self._running = False
            
            logger.info("Alert handler stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping alert handler: {str(e)}")
            return False
            
    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an incoming alert"""
        async with self._lock:
            try:
                # Extract key fields
                alert_id = alert_data.get("id", str(uuid.uuid4()))
                symbol = alert_data.get("symbol", "")
                action = alert_data.get("action", "").upper()
                
                # Check for duplicate alerts
                if alert_id in self.active_alerts:
                    logger.warning(f"Duplicate alert ignored: {alert_id}")
                    return {
                        "status": "ignored",
                        "message": "Duplicate alert",
                        "alert_id": alert_id
                    }
                    
                # Add to active alerts set
                self.active_alerts.add(alert_id)
                
                # Update system status
                if self.system_monitor:
                    await self.system_monitor.update_component_status(
                        "alert_handler", 
                        "processing",
                        f"Processing alert for {symbol} {action}"
                    )
                    
                try:
                    # Process based on action type
                    if action in ["BUY", "SELL"]:
                        result = await self._process_entry_alert(alert_data)
                    elif action in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT", "EXIT"]:
                        result = await self._process_exit_alert(alert_data)
                    elif action == "UPDATE":
                        result = await self._process_update_alert(alert_data)
                    else:
                        logger.warning(f"Unknown action in alert: {action}")
                        result = {
                            "status": "error",
                            "message": f"Unknown action: {action}",
                            "alert_id": alert_id
                        }
                
                finally:
                    # Remove from active alerts
                    self.active_alerts.discard(alert_id)
                    
                    # Update system status
                    if self.system_monitor:
                        await self.system_monitor.update_component_status(
                            "alert_handler", 
                            "ok",
                            ""
                        )
                        
                return result
                
            except Exception as e:
                logger.error(f"Error processing alert: {str(e)}")
                logger.error(traceback.format_exc())
                
                # Update error recovery
                if 'error_recovery' in globals() and error_recovery:
                    await error_recovery.record_error(
                        "alert_processing",
                        {
                            "error": str(e),
                            "alert": alert_data
                        }
                    )
                    
                return {
                    "status": "error",
                    "message": f"Error processing alert: {str(e)}",
                    "alert_id": alert_data.get("id", "unknown")
                }
                
    async def _process_entry_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an entry alert (BUY or SELL)"""
        # Extract fields
        alert_id = alert_data.get("id", str(uuid.uuid4()))
        symbol = alert_data.get("symbol", "")
        action = alert_data.get("action", "").upper()
        percentage = float(alert_data.get("percentage", 1.0))
        timeframe = alert_data.get("timeframe", "H1")
        comment = alert_data.get("comment", "")
        
        # Check if trading is allowed
        is_tradeable, reason = is_instrument_tradeable(symbol)
        if not is_tradeable:
            logger.warning(f"Trading not allowed for {symbol}: {reason}")
            return {
                "status": "rejected",
                "message": f"Trading not allowed: {reason}",
                "alert_id": alert_id
            }
            
        # Calculate position parameters
        position_id = f"{symbol}_{action}_{uuid.uuid4().hex[:8]}"
        account_balance = await get_account_balance()
        
        # Update risk manager balance
        if self.risk_manager:
            await self.risk_manager.update_account_balance(account_balance)
            
        # Calculate risk
        risk_percentage = min(percentage / 100, config.max_risk_percentage / 100)
        
        # Check if risk is allowed
        if self.risk_manager:
            is_allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage, symbol)
            if not is_allowed:
                logger.warning(f"Trade rejected due to risk limits: {reason}")
                return {
                    "status": "rejected",
                    "message": f"Risk check failed: {reason}",
                    "alert_id": alert_id
                }
                
        # Get current price
        price = alert_data.get("price")
        if price is None:
            price = await get_current_price(symbol, action)
        else:
            price = float(price)
            
        # Get ATR for stop loss calculation
        atr_value = await get_atr(symbol, timeframe)
        
        # Calculate stop loss
        instrument_type = get_instrument_type(symbol)
        atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
        
        # Apply volatility adjustment if available
        if self.volatility_monitor:
            volatility_multiplier = self.volatility_monitor.get_stop_loss_modifier(symbol)
            atr_multiplier *= volatility_multiplier
            
        if action == "BUY":
            stop_loss = price - (atr_value * atr_multiplier)
        else:  # SELL
            stop_loss = price + (atr_value * atr_multiplier)
            
        # Calculate position size
        risk_amount = account_balance * risk_percentage
        price_risk = abs(price - stop_loss)
        
        # Calculate size in units
        if price_risk > 0:
            # Risk-based sizing
            position_size = risk_amount / price_risk
        else:
            # Percentage-based sizing as fallback
            position_size = account_balance * percentage / 100 / price
            
        # Execute trade with broker
        success, trade_result = await execute_trade({
            "symbol": symbol,
            "action": action,
            "percentage": percentage,
            "price": price,
            "stop_loss": stop_loss
        })
        
        if not success:
            logger.error(f"Failed to execute trade: {trade_result.get('error', 'Unknown error')}")
            return {
                "status": "error",
                "message": f"Trade execution failed: {trade_result.get('error', 'Unknown error')}",
                "alert_id": alert_id
            }
            
        # Record position in tracker
        if self.position_tracker:
            # Extract metadata
            metadata = {
                "alert_id": alert_id,
                "comment": comment,
                "original_percentage": percentage,
                "atr_value": atr_value,
                "atr_multiplier": atr_multiplier
            }
            
            # Add any additional fields from alert
            for key, value in alert_data.items():
                if key not in ["id", "symbol", "action", "percentage", "price", "comment", "timeframe"]:
                    metadata[key] = value
                    
            # Record position
            await self.position_tracker.record_position(
                position_id=position_id,
                symbol=symbol,
                action=action,
                timeframe=timeframe,
                entry_price=price,
                size=position_size,
                stop_loss=stop_loss,
                take_profit=None,  # Will be set by exit manager
                metadata=metadata
            )
            
        # Register with risk manager
        if self.risk_manager:
            await self.risk_manager.register_position(
                position_id=position_id,
                symbol=symbol,
                action=action,
                size=position_size,
                entry_price=price,
                stop_loss=stop_loss,
                account_risk=risk_percentage,
                timeframe=timeframe
            )
            
        # Set take profit levels
        if self.multi_stage_tp_manager:
            await self.multi_stage_tp_manager.set_take_profit_levels(
                position_id=position_id,
                entry_price=price,
                stop_loss=stop_loss,
                position_direction=action,
                position_size=position_size,
                symbol=symbol,
                timeframe=timeframe,
                atr_value=atr_value,
                volatility_multiplier=volatility_multiplier if self.volatility_monitor else 1.0
            )
            
        # Register with time-based exit manager
        if self.time_based_exit_manager:
            self.time_based_exit_manager.register_position(
                position_id=position_id,
                symbol=symbol,
                direction=action,
                entry_time=datetime.now(timezone.utc),
                timeframe=timeframe
            )
            
        # Initialize dynamic exits
        if self.dynamic_exit_manager:
            # Get market regime
            market_regime = "unknown"
            if self.regime_classifier:
                regime_data = self.regime_classifier.get_regime_data(symbol)
                market_regime = regime_data.get("regime", "unknown")
                
            await self.dynamic_exit_manager.initialize_exits(
                position_id=position_id,
                symbol=symbol,
                entry_price=price,
                position_direction=action,
                stop_loss=stop_loss,
                timeframe=timeframe
            )
            
        # Record in position journal
        if self.position_journal:
            # Get market regime and volatility state
            market_regime = "unknown"
            volatility_state = "normal"
            
            if self.regime_classifier:
                regime_data = self.regime_classifier.get_regime_data(symbol)
                market_regime = regime_data.get("regime", "unknown")
                
            if self.volatility_monitor:
                vol_data = self.volatility_monitor.get_volatility_state(symbol)
                volatility_state = vol_data.get("volatility_state", "normal")
                
            await self.position_journal.record_entry(
                position_id=position_id,
                symbol=symbol,
                action=action,
                timeframe=timeframe,
                entry_price=price,
                size=position_size,
                strategy="primary",
                stop_loss=stop_loss,
                market_regime=market_regime,
                volatility_state=volatility_state,
                metadata=metadata
            )
            
        # Send notification
        if self.notification_system:
            await self.notification_system.send_notification(
                f"New position opened: {action} {symbol} @ {price:.5f} (Risk: {risk_percentage*100:.1f}%)",
                "info"
            )
            
        return {
            "status": "success",
            "message": f"Position opened: {action} {symbol} @ {price}",
            "position_id": position_id,
            "symbol": symbol,
            "action": action,
            "price": price,
            "size": position_size,
            "stop_loss": stop_loss,
            "alert_id": alert_id
        }
    
    async def _process_exit_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an exit alert (CLOSE, CLOSE_LONG, CLOSE_SHORT)"""
        # Extract fields
        alert_id = alert_data.get("id", str(uuid.uuid4()))
        symbol = alert_data.get("symbol", "")
        action = alert_data.get("action", "").upper()
        
        # Get all open positions for this symbol
        open_positions = {}
        if self.position_tracker:
            all_open = await self.position_tracker.get_open_positions()
            if symbol in all_open:
                open_positions = all_open[symbol]
        
        if not open_positions:
            logger.warning(f"No open positions found for {symbol}")
            return {
                "status": "warning",
                "message": f"No open positions found for {symbol}",
                "alert_id": alert_id
            }
            
        # Get current price
        price = alert_data.get("price")
        if price is None:
            price = await get_current_price(symbol, "SELL")  # Use SELL price for closing
        else:
            price = float(price)
            
        # Determine which positions to close
        positions_to_close = []
        
        for position_id, position in open_positions.items():
            # Check if position matches the close direction
            if action == "CLOSE":
                # Close any position for this symbol
                positions_to_close.append(position_id)
            elif action == "CLOSE_LONG" and position["action"] == "BUY":
                # Close only long positions
                positions_to_close.append(position_id)
            elif action == "CLOSE_SHORT" and position["action"] == "SELL":
                # Close only short positions
                positions_to_close.append(position_id)
        
        if not positions_to_close:
            logger.warning(f"No matching positions found for {symbol} {action}")
            return {
                "status": "warning",
                "message": f"No matching positions found for {symbol} {action}",
                "alert_id": alert_id
            }
            
        # Close positions
        closed_positions = []
        
        for position_id in positions_to_close:
            # Close with broker
            position_data = open_positions[position_id]
            success, close_result = await close_position({
                "symbol": symbol,
                "position_id": position_id
            })
            
            if not success:
                logger.error(f"Failed to close position {position_id}: {close_result.get('error', 'Unknown error')}")
                continue
                
            # Close in position tracker
            if self.position_tracker:
                success, result = await self.position_tracker.close_position(
                    position_id=position_id,
                    exit_price=price,
                    reason=action.lower()
                )
                
                if success:
                    closed_positions.append(result)
                    
                    # Close in risk manager
                    if self.risk_manager:
                        await self.risk_manager.close_position(position_id)
                        
                    # Remove from time-based exit manager
                    if self.time_based_exit_manager:
                        self.time_based_exit_manager.remove_position(position_id)
                        
                    # Record in position journal
                    if self.position_journal:
                        # Get market regime and volatility state
                        market_regime = "unknown"
                        volatility_state = "normal"
                        
                        if self.regime_classifier:
                            regime_data = self.regime_classifier.get_regime_data(symbol)
                            market_regime = regime_data.get("regime", "unknown")
                            
                        if self.volatility_monitor:
                            vol_data = self.volatility_monitor.get_volatility_state(symbol)
                            volatility_state = vol_data.get("volatility_state", "normal")
                            
                        await self.position_journal.record_exit(
                            position_id=position_id,
                            exit_price=price,
                            exit_reason=action.lower(),
                            pnl=result.get("pnl", 0.0),
                            market_regime=market_regime,
                            volatility_state=volatility_state
                        )
        
        # Send notification
        if closed_positions and self.notification_system:
            total_pnl = sum(position.get("pnl", 0) for position in closed_positions)
            
            # Determine notification level based on P&L
            level = "info"
            if total_pnl > 0:
                level = "info"
            elif total_pnl < 0:
                level = "warning"
                
            await self.notification_system.send_notification(
                f"Closed {len(closed_positions)} positions for {symbol} @ {price:.5f} (P&L: {total_pnl:.2f})",
                level
            )
            
        if closed_positions:
            return {
                "status": "success",
                "message": f"Closed {len(closed_positions)} positions for {symbol}",
                "positions": closed_positions,
                "symbol": symbol,
                "price": price,
                "alert_id": alert_id
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to close positions for {symbol}",
                "alert_id": alert_id
            }
    
    async def _process_update_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an update alert (update stop loss, take profit, etc.)"""
        # Extract fields
        alert_id = alert_data.get("id", str(uuid.uuid4()))
        symbol = alert_data.get("symbol", "")
        position_id = alert_data.get("position_id")
        stop_loss = alert_data.get("stop_loss")
        take_profit = alert_data.get("take_profit")
        
        # If position_id is provided, update that specific position
        if position_id:
            # Get position
            if not self.position_tracker:
                return {
                    "status": "error",
                    "message": "Position tracker not available",
                    "alert_id": alert_id
                }
                
            position = await self.position_tracker.get_position_info(position_id)
            
            if not position:
                return {
                    "status": "error",
                    "message": f"Position {position_id} not found",
                    "alert_id": alert_id
                }
                
            # Check if position is closed
            if position.get("status") == "closed":
                return {
                    "status": "error",
                    "message": f"Cannot update closed position {position_id}",
                    "alert_id": alert_id
                }
                
            # Convert stop loss and take profit to float if provided
            if stop_loss is not None:
                stop_loss = float(stop_loss)
                
            if take_profit is not None:
                take_profit = float(take_profit)
                
            # Update position
            success = await self.position_tracker.update_position(
                position_id=position_id,
                stop_loss=stop_loss,
                take_profit=take_profit
            )
            
            if not success:
                return {
                    "status": "error",
                    "message": f"Failed to update position {position_id}",
                    "alert_id": alert_id
                }
                
            # Get updated position
            updated_position = await self.position_tracker.get_position_info(position_id)
            
            # Record adjustment in journal
            if self.position_journal:
                if stop_loss is not None:
                    await self.position_journal.record_adjustment(
                        position_id=position_id,
                        adjustment_type="stop_loss",
                        old_value=position.get("stop_loss"),
                        new_value=stop_loss,
                        reason="manual_update"
                    )
                    
                if take_profit is not None:
                    await self.position_journal.record_adjustment(
                        position_id=position_id,
                        adjustment_type="take_profit",
                        old_value=position.get("take_profit"),
                        new_value=take_profit,
                        reason="manual_update"
                    )
                    
            return {
                "status": "success",
                "message": f"Updated position {position_id}",
                "position": updated_position,
                "alert_id": alert_id
            }
            
        # If symbol is provided but not position_id, update all positions for that symbol
        elif symbol:
            # Get all open positions for this symbol
            open_positions = {}
            if self.position_tracker:
                all_open = await self.position_tracker.get_open_positions()
                if symbol in all_open:
                    open_positions = all_open[symbol]
            
            if not open_positions:
                return {
                    "status": "warning",
                    "message": f"No open positions found for {symbol}",
                    "alert_id": alert_id
                }
                
            # Convert stop loss and take profit to float if provided
            if stop_loss is not None:
                stop_loss = float(stop_loss)
                
            if take_profit is not None:
                take_profit = float(take_profit)
                
            # Update positions
            updated_positions = []
            
            for position_id in open_positions:
                # Update position
                success = await self.position_tracker.update_position(
                    position_id=position_id,
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
                
                if success:
                    # Get updated position
                    updated_position = await self.position_tracker.get_position_info(position_id)
                    updated_positions.append(updated_position)
                    
                    # Record adjustment in journal
                    if self.position_journal:
                        if stop_loss is not None:
                            await self.position_journal.record_adjustment(
                                position_id=position_id,
                                adjustment_type="stop_loss",
                                old_value=open_positions[position_id].get("stop_loss"),
                                new_value=stop_loss,
                                reason="bulk_update"
                            )
                            
                        if take_profit is not None:
                            await self.position_journal.record_adjustment(
                                position_id=position_id,
                                adjustment_type="take_profit",
                                old_value=open_positions[position_id].get("take_profit"),
                                new_value=take_profit,
                                reason="bulk_update"
                            )
            
            if updated_positions:
                return {
                    "status": "success",
                    "message": f"Updated {len(updated_positions)} positions for {symbol}",
                    "positions": updated_positions,
                    "alert_id": alert_id
                }
            else:
                return {
                    "status": "error",
                    "message": f"Failed to update positions for {symbol}",
                    "alert_id": alert_id
                }
        else:
            return {
                "status": "error",
                "message": "Either position_id or symbol must be provided",
                "alert_id": alert_id
            }
    
    async def handle_scheduled_tasks(self):
        """Handle scheduled tasks like managing exits, updating prices, etc."""
        logger.info("Starting scheduled tasks handler")
        
        # Track the last time each task was run
        last_run = {
            "update_prices": datetime.now(timezone.utc),
            "check_exits": datetime.now(timezone.utc),
            "daily_reset": datetime.now(timezone.utc),
            "position_cleanup": datetime.now(timezone.utc),
            "database_sync": datetime.now(timezone.utc)
        }
        
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                
                # Update prices every minute
                if (current_time - last_run["update_prices"]).total_seconds() >= 60:
                    await self._update_position_prices()
                    last_run["update_prices"] = current_time
                
                # Check exits every 5 minutes
                if (current_time - last_run["check_exits"]).total_seconds() >= 300:
                    await self._check_position_exits()
                    last_run["check_exits"] = current_time
                
                # Daily reset tasks
                if current_time.day != last_run["daily_reset"].day:
                    await self._perform_daily_reset()
                    last_run["daily_reset"] = current_time
                
                # Position cleanup weekly
                if (current_time - last_run["position_cleanup"]).total_seconds() >= 604800:  # 7 days
                    await self._cleanup_old_positions()
                    last_run["position_cleanup"] = current_time
                
                # Database sync hourly
                if (current_time - last_run["database_sync"]).total_seconds() >= 3600:  # 1 hour
                    await self._sync_database()
                    last_run["database_sync"] = current_time
                    
                # Wait a short time before checking again
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in scheduled tasks: {str(e)}")
                logger.error(traceback.format_exc())
                
                # Record error
                if 'error_recovery' in globals() and error_recovery:
                    await error_recovery.record_error(
                        "scheduled_tasks",
                        {"error": str(e)}
                    )
                    
                # Wait before retrying
                await asyncio.sleep(60)
    
    async def _update_position_prices(self):
        """Update all open position prices"""
        if not self.position_tracker:
            return
            
        try:
            # Get all open positions
            open_positions = await self.position_tracker.get_open_positions()
            
            # Update price for each symbol (once per symbol to minimize API calls)
            updated_prices = {}
            position_count = 0
            
            for symbol, positions in open_positions.items():
                if not positions:
                    continue
                    
                # Get price for this symbol (use any position to determine direction)
                any_position = next(iter(positions.values()))
                direction = any_position.get("action")
                
                # Get current price
                try:
                    price = await get_current_price(symbol, "SELL" if direction == "BUY" else "BUY")
                    updated_prices[symbol] = price
                    
                    # Update volatility monitor and regime classifier
                    if self.volatility_monitor:
                        # Get ATR
                        timeframe = any_position.get("timeframe", "H1")
                        atr_value = await get_atr(symbol, timeframe)
                        
                        # Update volatility state
                        await self.volatility_monitor.update_volatility(symbol, atr_value, timeframe)
                        
                    if self.regime_classifier:
                        await self.regime_classifier.add_price_data(symbol, price, any_position.get("timeframe", "H1"))
                        
                    # Update position prices
                    for position_id in positions:
                        await self.position_tracker.update_position_price(position_id, price)
                        position_count += 1
                        
                except Exception as e:
                    logger.error(f"Error updating price for {symbol}: {str(e)}")
            
            if position_count > 0:
                logger.debug(f"Updated prices for {position_count} positions across {len(updated_prices)} symbols")
                
        except Exception as e:
            logger.error(f"Error updating position prices: {str(e)}")
    
    async def _check_position_exits(self):
        """Check all positions for exit conditions"""
        if not self.position_tracker:
            return
            
        try:
            # Get all open positions
            open_positions = await self.position_tracker.get_open_positions()
            if not open_positions:
                return
                
            # Check each position for exit conditions
            for symbol, positions in open_positions.items():
                for position_id, position in positions.items():
                    # Skip if position isn't fully initialized
                    if not position.get("current_price"):
                        continue
                        
                    current_price = position["current_price"]
                    
                    # Check stop loss
                    if self._check_stop_loss(position, current_price):
                        await self._exit_position(
                            position_id=position_id,
                            exit_price=current_price,
                            reason="stop_loss"
                        )
                        continue
                        
                    # Check take profit levels
                    if self.multi_stage_tp_manager:
                        tp_level = await self.multi_stage_tp_manager.check_take_profit_levels(position_id, current_price)
                        if tp_level:
                            # Execute take profit
                            await self.multi_stage_tp_manager.execute_take_profit(position_id, tp_level)
                            continue
                    
                    # Check time-based exits
                    if self.time_based_exit_manager:
                        exits = self.time_based_exit_manager.check_time_exits()
                        for exit_info in exits:
                            if exit_info["position_id"] == position_id:
                                await self._exit_position(
                                    position_id=position_id,
                                    exit_price=current_price,
                                    reason=f"time_exit_{exit_info['reason']}"
                                )
                                break
                    
                    # Check trailing stops and breakeven stops would go here
                    
            # Log summary
            total_positions = sum(len(positions) for positions in open_positions.values())
            logger.debug(f"Checked exits for {total_positions} open positions")
            
        except Exception as e:
            logger.error(f"Error checking position exits: {str(e)}")
    
    def _check_stop_loss(self, position: Dict[str, Any], current_price: float) -> bool:
        """Check if stop loss is hit"""
        stop_loss = position.get("stop_loss")
        if stop_loss is None:
            return False
            
        action = position.get("action", "").upper()
        
        if action == "BUY":
            return current_price <= stop_loss
        else:  # SELL
            return current_price >= stop_loss
    
    async def _exit_position(self, position_id: str, exit_price: float, reason: str) -> bool:
        """Exit a position with the given reason"""
        try:
            # Get position info
            position = await self.position_tracker.get_position_info(position_id)
            if not position:
                logger.warning(f"Position {position_id} not found for exit")
                return False
                
            # Check if already closed
            if position.get("status") == "closed":
                logger.warning(f"Position {position_id} already closed")
                return False
                
            # Close with broker
            symbol = position.get("symbol", "")
            success, close_result = await close_position({
                "symbol": symbol,
                "position_id": position_id
            })
            
            if not success:
                logger.error(f"Failed to close position {position_id} with broker: {close_result.get('error', 'Unknown error')}")
                return False
                
            # Close in position tracker
            success, result = await self.position_tracker.close_position(
                position_id=position_id,
                exit_price=exit_price,
                reason=reason
            )
            
            if not success:
                logger.error(f"Failed to close position {position_id} in tracker: {result.get('error', 'Unknown error')}")
                return False
                
            # Close in risk manager
            if self.risk_manager:
                await self.risk_manager.close_position(position_id)
                
            # Remove from time-based exit manager
            if self.time_based_exit_manager:
                self.time_based_exit_manager.remove_position(position_id)
                
            # Record in position journal
            if self.position_journal:
                # Get market regime and volatility state
                market_regime = "unknown"
                volatility_state = "normal"
                
                if self.regime_classifier:
                    regime_data = self.regime_classifier.get_regime_data(symbol)
                    market_regime = regime_data.get("regime", "unknown")
                    
                if self.volatility_monitor:
                    vol_data = self.volatility_monitor.get_volatility_state(symbol)
                    volatility_state = vol_data.get("volatility_state", "normal")
                    
                await self.position_journal.record_exit(
                    position_id=position_id,
                    exit_price=exit_price,
                    exit_reason=reason,
                    pnl=result.get("pnl", 0.0),
                    market_regime=market_regime,
                    volatility_state=volatility_state
                )
                
            # Send notification
            if self.notification_system:
                pnl = result.get("pnl", 0.0)
                
                # Determine notification level based on P&L
                level = "info"
                if pnl > 0:
                    level = "info"
                elif pnl < 0:
                    level = "warning"
                    
                await self.notification_system.send_notification(
                    f"Position {position_id} closed: {symbol} @ {exit_price:.5f} (P&L: {pnl:.2f}, Reason: {reason})",
                    level
                )
                
            logger.info(f"Position {position_id} exited at {exit_price} (Reason: {reason})")
            return True
            
        except Exception as e:
            logger.error(f"Error exiting position {position_id}: {str(e)}")
            return False
    
    async def _perform_daily_reset(self):
        """Perform daily reset tasks"""
        try:
            logger.info("Performing daily reset tasks")
            
            # Reset daily risk statistics
            if self.risk_manager:
                await self.risk_manager.reset_daily_stats()
                
            # Create a backup
            if 'backup_manager' in globals() and backup_manager:
                await backup_manager.create_backup(include_market_data=True, compress=True)
                
            # Send notification
            if self.notification_system:
                await self.notification_system.send_notification(
                    "Daily reset completed: Risk statistics reset and backup created",
                    "info"
                )
                
        except Exception as e:
            logger.error(f"Error in daily reset: {str(e)}")
    
    async def _cleanup_old_positions(self):
        """Clean up old closed positions to prevent memory growth"""
        try:
            if self.position_tracker:
                await self.position_tracker.purge_old_closed_positions(max_age_days=30)
                
            # Also clean up old backups
            if 'backup_manager' in globals() and backup_manager:
                await backup_manager.cleanup_old_backups(max_age_days=60, keep_min=10)
                
        except Exception as e:
            logger.error(f"Error cleaning up old positions: {str(e)}")
    
    async def _sync_database(self):
        """Ensure all data is synced with the database"""
        try:
            if self.position_tracker:
                await self.position_tracker.sync_with_database()
                await self.position_tracker.clean_up_duplicate_positions()
                
        except Exception as e:
            logger.error(f"Error syncing database: {str(e)}")

##############################################################################
# System Monitoring & Notifications
##############################################################################

class SystemMonitor:
    """
    Monitors system health, component status, and performance metrics.
    """
    def __init__(self):
        """Initialize system monitor"""
        self.component_status = {}  # component_name -> status
        self.performance_metrics = {}  # metric_name -> value
        self.error_counts = {}  # component_name -> error count
        self.last_update = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()
        
    async def register_component(self, component_name: str, initial_status: str = "initializing") -> bool:
        """Register a component for monitoring"""
        async with self._lock:
            self.component_status[component_name] = {
                "status": initial_status,
                "last_update": datetime.now(timezone.utc).isoformat(),
                "message": "",
                "error_count": 0
            }
            
            self.error_counts[component_name] = 0
            
            logger.info(f"Registered component {component_name} for monitoring")
            return True
            
    async def update_component_status(self, component_name: str, status: str, message: str = "") -> bool:
        """Update status for a monitored component"""
        async with self._lock:
            if component_name not in self.component_status:
                await self.register_component(component_name)
                
            old_status = self.component_status[component_name]["status"]
            
            self.component_status[component_name] = {
                "status": status,
                "last_update": datetime.now(timezone.utc).isoformat(),
                "message": message,
                "error_count": self.error_counts.get(component_name, 0)
            }
            
            # Update error count if status is error
            if status == "error":
                self.error_counts[component_name] = self.error_counts.get(component_name, 0) + 1
                self.component_status[component_name]["error_count"] = self.error_counts[component_name]
                
            # Log status change if significant
            if old_status != status:
                if status == "error":
                    logger.error(f"Component {component_name} status changed to {status}: {message}")
                elif status == "warning":
                    logger.warning(f"Component {component_name} status changed to {status}: {message}")
                else:
                    logger.info(f"Component {component_name} status changed to {status}")
                    
            self.last_update = datetime.now(timezone.utc)
            return True
            
    async def record_metric(self, metric_name: str, value: Any) -> bool:
        """Record a performance metric"""
        async with self._lock:
            self.performance_metrics[metric_name] = {
                "value": value,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.last_update = datetime.now(timezone.utc)
            return True
            
    async def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        async with self._lock:
            # Determine overall status
            status_counts = {
                "ok": 0,
                "warning": 0,
                "error": 0,
                "initializing": 0
            }
            
            for component in self.component_status.values():
                component_status = component["status"]
                status_counts[component_status] = status_counts.get(component_status, 0) + 1
                
            if status_counts["error"] > 0:
                overall_status = "error"
            elif status_counts["warning"] > 0:
                overall_status = "warning"
            elif status_counts["initializing"] > 0:
                overall_status = "initializing"
            else:
                overall_status = "ok"
                
            # Calculate error rate
            total_components = len(self.component_status)
            error_rate = 0.0
            
            if total_components > 0:
                error_rate = status_counts["error"] / total_components * 100
                
            return {
                "status": overall_status,
                "component_count": total_components,
                "status_counts": status_counts,
                "error_rate": error_rate,
                "last_update": self.last_update.isoformat(),
                "uptime": (datetime.now(timezone.utc) - self.last_update).total_seconds(),
                "components": self.component_status,
                "metrics": self.performance_metrics
            }
            
    async def reset_error_counts(self) -> bool:
        """Reset error counts for all components"""
        async with self._lock:
            for component in self.error_counts:
                self.error_counts[component] = 0
                
                if component in self.component_status:
                    self.component_status[component]["error_count"] = 0
                    
            logger.info("Reset error counts for all components")
            return True
            
    async def get_component_status(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Get status for a specific component"""
        async with self._lock:
            if component_name not in self.component_status:
                return None
                
            return self.component_status[component_name]

class NotificationSystem:
    """
    Sends notifications via multiple channels (console, email, Slack, Telegram).
    """
    def __init__(self):
        """Initialize notification system"""
        self.channels = {}  # channel_name -> config
        self._lock = asyncio.Lock()
        
    async def configure_channel(self, channel_name: str, config: Dict[str, Any]) -> bool:
        """Configure a notification channel"""
        async with self._lock:
            self.channels[channel_name] = {
                "config": config,
                "enabled": True,
                "last_notification": None
            }
            
            logger.info(f"Configured notification channel: {channel_name}")
            return True
            
    async def disable_channel(self, channel_name: str) -> bool:
        """Disable a notification channel"""
        async with self._lock:
            if channel_name not in self.channels:
                return False
                
            self.channels[channel_name]["enabled"] = False
            logger.info(f"Disabled notification channel: {channel_name}")
            return True
            
    async def enable_channel(self, channel_name: str) -> bool:
        """Enable a notification channel"""
        async with self._lock:
            if channel_name not in self.channels:
                return False
                
            self.channels[channel_name]["enabled"] = True
            logger.info(f"Enabled notification channel: {channel_name}")
            return True
            
    async def send_notification(self, message: str, level: str = "info") -> Dict[str, Any]:
        """Send notification to all enabled channels"""
        async with self._lock:
            if not self.channels:
                logger.warning("No notification channels configured")
                return {
                    "status": "error",
                    "message": "No notification channels configured"
                }
                
            results = {}
            timestamp = datetime.now(timezone.utc).isoformat()
            
            for channel_name, channel in self.channels.items():
                if not channel["enabled"]:
                    continue
                    
                try:
                    # Send notification through channel
                    if channel_name == "console":
                        await self._send_console_notification(message, level)
                        success = True
                    elif channel_name == "slack":
                        success = await self._send_slack_notification(message, level, channel["config"])
                    elif channel_name == "telegram":
                        success = await self._send_telegram_notification(message, level, channel["config"])
                    elif channel_name == "email":
                        success = await self._send_email_notification(message, level, channel["config"])
                    else:
                        logger.warning(f"Unknown notification channel: {channel_name}")
                        success = False
                        
                    # Update channel's last notification
                    if success:
                        self.channels[channel_name]["last_notification"] = timestamp
                        
                    results[channel_name] = success
                    
                except Exception as e:
                    logger.error(f"Error sending notification via {channel_name}: {str(e)}")
                    results[channel_name] = False
                    
            return {
                "status": "success" if any(results.values()) else "error",
                "timestamp": timestamp,
                "results": results
            }
            
    async def _send_console_notification(self, message: str, level: str) -> bool:
        """Send notification to console (log)"""
        if level == "info":
            logger.info(f"NOTIFICATION: {message}")
        elif level == "warning":
            logger.warning(f"NOTIFICATION: {message}")
        elif level == "error":
            logger.error(f"NOTIFICATION: {message}")
        elif level == "critical":
            logger.critical(f"NOTIFICATION: {message}")
        else:
            logger.info(f"NOTIFICATION: {message}")
            
        return True
        
    async def _send_slack_notification(self, message: str, level: str, config: Dict[str, Any]) -> bool:
        """Send notification to Slack"""
        webhook_url = config.get("webhook_url")
        
        if not webhook_url:
            logger.error("Slack webhook URL not configured")
            return False
            
        try:
            # Get or create session
            session = await get_session()
            
            # Prepare message payload
            payload = {
                "text": message,
                "attachments": [{
                    "color": self._get_level_color(level),
                    "text": message,
                    "footer": f"Trading System • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }]
            }
            
            # Send message
            async with session.post(webhook_url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Failed to send Slack notification: {response.status}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error sending Slack notification: {str(e)}")
            return False
            
    def _get_level_color(self, level: str) -> str:
        """Get color for notification level"""
        colors = {
            "info": "#36a64f",  # green
            "warning": "#ffcc00",  # yellow
            "error": "#ff0000",  # red
            "critical": "#7b0000"  # dark red
        }
        
        return colors.get(level, "#36a64f")
        
    async def _send_telegram_notification(self, message: str, level: str, config: Dict[str, Any]) -> bool:
        """Send notification to Telegram"""
        bot_token = config.get("bot_token")
        chat_id = config.get("chat_id")
        
        if not bot_token or not chat_id:
            logger.error("Telegram bot token or chat ID not configured")
            return False
            
        try:
            # Get or create session
            session = await get_session()
            
            # Prepare API URL
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            
            # Add level emoji
            emoji = {
                "info": "ℹ️",
                "warning": "⚠️",
                "error": "🔴",
                "critical": "🚨"
            }.get(level, "ℹ️")
            
            # Prepare message payload
            payload = {
                "chat_id": chat_id,
                "text": f"{emoji} {message}",
                "parse_mode": "Markdown"
            }
            
            # Send message
            async with session.post(api_url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Failed to send Telegram notification: {response.status}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error sending Telegram notification: {str(e)}")
            return False
            
    async def _send_email_notification(self, message: str, level: str, config: Dict[str, Any]) -> bool:
        """Send notification via email"""
        # Email implementation would go here
        # For now, just log that this would send an email
        logger.info(f"Would send email notification: {message} (level: {level})")
        return True
        
    async def get_channel_status(self) -> Dict[str, Any]:
        """Get status of all notification channels"""
        async with self._lock:
            status = {}
            
            for channel_name, channel in self.channels.items():
                status[channel_name] = {
                    "enabled": channel["enabled"],
                    "last_notification": channel.get("last_notification")
                }
                
            return status

class PortfolioOptimizer:
    """
    Optimizes portfolio allocations based on performance,
    volatility, correlation, and risk-adjusted returns.
    """
    def __init__(self):
        """Initialize portfolio optimizer"""
        self.price_data = {}  # symbol -> price data
        self.optimization_cache = {}  # key -> optimization result
        self.cache_expiry = 3600  # 1 hour
        self._lock = asyncio.Lock()
        
    async def add_price_data(self, symbol: str, price: float):
        """Add price data for a symbol"""
        async with self._lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = []
                
            # Add price data with timestamp
            self.price_data[symbol].append({
                "price": price,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            # Limit history size
            if len(self.price_data[symbol]) > 100:
                self.price_data[symbol] = self.price_data[symbol][-100:]
                
    async def get_portfolio_recommendation(self, risk_level: str = "moderate") -> Dict[str, Any]:
        """Get portfolio allocation recommendation based on risk level"""
        async with self._lock:
            # Check cache first
            cache_key = f"portfolio_recommendation_{risk_level}"
            if cache_key in self.optimization_cache:
                cached_result = self.optimization_cache[cache_key]
                cache_age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached_result["timestamp"])).total_seconds()
                
                if cache_age < self.cache_expiry:
                    return cached_result
                    
            # Get symbols with enough data
            valid_symbols = {s: data for s, data in self.price_data.items() if len(data) >= 30}
            
            if len(valid_symbols) < 2:
                return {
                    "status": "error",
                    "message": "Not enough symbols with price data for optimization"
                }
                
            try:
                # Calculate returns for each symbol
                returns = {}
                volatilities = {}
                correlations = {}
                
                for symbol, data in valid_symbols.items():
                    prices = [d["price"] for d in data]
                    returns[symbol] = (prices[-1] / prices[0]) - 1
                    
                    # Calculate volatility (standard deviation of daily returns)
                    daily_returns = [(prices[i] / prices[i-1]) - 1 for i in range(1, len(prices))]
                    volatilities[symbol] = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0
                    
                # Calculate correlation matrix
                symbols = list(valid_symbols.keys())
                for i in range(len(symbols)):
                    for j in range(i+1, len(symbols)):
                        sym1 = symbols[i]
                        sym2 = symbols[j]
                        
                        prices1 = [d["price"] for d in valid_symbols[sym1]]
                        prices2 = [d["price"] for d in valid_symbols[sym2]]
                        
                        # Ensure equal length by using the minimum length
                        min_length = min(len(prices1), len(prices2))
                        prices1 = prices1[-min_length:]
                        prices2 = prices2[-min_length:]
                        
                        # Calculate daily returns
                        returns1 = [(prices1[i] / prices1[i-1]) - 1 for i in range(1, len(prices1))]
                        returns2 = [(prices2[i] / prices2[i-1]) - 1 for i in range(1, len(prices2))]
                        
                        # Calculate correlation if we have enough data
                        if len(returns1) > 1 and len(returns2) > 1:
                            correlation = self._calculate_correlation(returns1, returns2)
                            correlations[f"{sym1}_{sym2}"] = correlation
                            correlations[f"{sym2}_{sym1}"] = correlation
                            
                # Apply optimization based on risk level
                weights = self._optimize_portfolio(returns, volatilities, correlations, risk_level)
                
                # Cache the result
                result = {
                    "status": "success",
                    "risk_level": risk_level,
                    "returns": returns,
                    "volatilities": volatilities,
                    "weights": weights,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                self.optimization_cache[cache_key] = result
                
                return result
                
            except Exception as e:
                logger.error(f"Error in portfolio optimization: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Optimization error: {str(e)}"
                }
                
    def _calculate_correlation(self, series1: List[float], series2: List[float]) -> float:
        """Calculate Pearson correlation coefficient between two series"""
        if len(series1) != len(series2) or len(series1) < 2:
            return 0.0
            
        n = len(series1)
        
        # Calculate means
        mean1 = sum(series1) / n
        mean2 = sum(series2) / n
        
        # Calculate variances and covariance
        var1 = sum((x - mean1) ** 2 for x in series1) / n
        var2 = sum((x - mean2) ** 2 for x in series2) / n
        cov = sum((series1[i] - mean1) * (series2[i] - mean2) for i in range(n)) / n
        
        # Calculate correlation
        std1 = var1 ** 0.5
        std2 = var2 ** 0.5
        
        if std1 == 0 or std2 == 0:
            return 0.0
            
        return cov / (std1 * std2)
        
    def _optimize_portfolio(self,
                          returns: Dict[str, float],
                          volatilities: Dict[str, float],
                          correlations: Dict[str, float],
                          risk_level: str) -> Dict[str, float]:
        """
        Optimize portfolio weights based on risk level
        
        This is a simplified optimization that weighs assets based on
        their risk-adjusted returns and correlation structure.
        """
        # Define risk factors based on risk level
        risk_factors = {
            "conservative": {
                "return_weight": 0.3,
                "volatility_weight": 0.7,
                "max_allocation": 0.2
            },
            "moderate": {
                "return_weight": 0.5,
                "volatility_weight": 0.5,
                "max_allocation": 0.3
            },
            "aggressive": {
                "return_weight": 0.7,
                "volatility_weight": 0.3,
                "max_allocation": 0.4
            }
        }
        
        # Use moderate as default if risk level not recognized
        factors = risk_factors.get(risk_level, risk_factors["moderate"])
        
        # Calculate Sharpe ratios (simplified, without risk-free rate)
        sharpe_ratios = {}
        for symbol in returns:
            if volatilities[symbol] > 0:
                sharpe_ratios[symbol] = returns[symbol] / volatilities[symbol]
            else:
                sharpe_ratios[symbol] = 0
                
        # Calculate diversification benefits
        diversification_scores = {}
        symbols = list(returns.keys())
        
        for symbol in symbols:
            # Calculate average correlation with other assets
            correlations_with_symbol = []
            
            for other_symbol in symbols:
                if symbol != other_symbol:
                    pair_key = f"{symbol}_{other_symbol}"
                    if pair_key in correlations:
                        correlations_with_symbol.append(correlations[pair_key])
                        
            # Lower average correlation is better for diversification
            avg_correlation = sum(correlations_with_symbol) / len(correlations_with_symbol) if correlations_with_symbol else 0
            diversification_scores[symbol] = 1 - abs(avg_correlation)  # 1 is best (uncorrelated), 0 is worst (perfectly correlated)
            
        # Calculate combined score
        combined_scores = {}
        for symbol in symbols:
            return_score = returns[symbol] if returns[symbol] > 0 else 0
            volatility_penalty = -volatilities[symbol]
            
            combined_scores[symbol] = (
                factors["return_weight"] * return_score +
                factors["volatility_weight"] * volatility_penalty +
                0.2 * diversification_scores[symbol]  # Add diversification benefit
            )
            
        # If all scores are negative, shift to make minimum score zero
        min_score = min(combined_scores.values())
        if min_score < 0:
            for symbol in combined_scores:
                combined_scores[symbol] -= min_score
                
        # Convert scores to weights
        total_score = sum(combined_scores.values())
        weights = {}
        
        if total_score > 0:
            for symbol, score in combined_scores.items():
                weights[symbol] = score / total_score
        else:
            # Equal weights if total score is zero or negative
            equal_weight = 1.0 / len(symbols)
            weights = {symbol: equal_weight for symbol in symbols}
            
        # Apply maximum allocation constraint
        max_allocation = factors["max_allocation"]
        need_rebalance = False
        
        for symbol, weight in weights.items():
            if weight > max_allocation:
                weights[symbol] = max_allocation
                need_rebalance = True
                
        # Redistribute excess weight if needed
        if need_rebalance:
            # Calculate total excess and remaining symbols
            excess_weight = 1.0 - sum(weights.values())
            remaining_symbols = [s for s, w in weights.items() if w < max_allocation]
            
            if remaining_symbols and excess_weight > 0:
                # Redistribute proportionally
                remaining_total = sum(weights[s] for s in remaining_symbols)
                
                for symbol in remaining_symbols:
                    if remaining_total > 0:
                        weights[symbol] += excess_weight * (weights[symbol] / remaining_total)
                    else:
                        # Equal distribution if all remaining weights are zero
                        weights[symbol] += excess_weight / len(remaining_symbols)
                        
        # Normalize weights to ensure they sum to 1.0
        weight_sum = sum(weights.values())
        if weight_sum > 0:
            weights = {symbol: weight / weight_sum for symbol, weight in weights.items()}
            
        return weights
        
    async def recommend_position_sizes(self,
                                     account_balance: float,
                                     max_portfolio_risk: float = 0.15) -> Dict[str, Any]:
        """Recommend position sizes based on account balance and risk limit"""
        async with self._lock:
            # Get portfolio recommendation
            recommendation = await self.get_portfolio_recommendation("moderate")
            
            if recommendation["status"] != "success":
                return recommendation
                
            weights = recommendation["weights"]
            volatilities = recommendation["volatilities"]
            
            # Calculate position sizes
            position_sizes = {}
            for symbol, weight in weights.items():
                # Allocate capital based on weight
                capital_allocation = account_balance * weight
                
                # Adjust for volatility to target consistent risk per position
                volatility = volatilities.get(symbol, 0.01)  # Default to 1% if unknown
                
                # Scale position size based on volatility
                # Higher volatility = smaller position
                size_multiplier = 1.0 / (1.0 + volatility * 10)  # Scale volatility impact
                
                # Calculate position size
                position_sizes[symbol] = capital_allocation * size_multiplier
                
            # Calculate expected risk
            expected_risk = sum(volatilities.get(symbol, 0.01) * (size / account_balance) 
                             for symbol, size in position_sizes.items())
            
            # Scale all positions if risk limit exceeded
            if expected_risk > max_portfolio_risk and expected_risk > 0:
                scaling_factor = max_portfolio_risk / expected_risk
                position_sizes = {symbol: size * scaling_factor for symbol, size in position_sizes.items()}
                expected_risk = max_portfolio_risk
                
            return {
                "status": "success",
                "account_balance": account_balance,
                "max_portfolio_risk": max_portfolio_risk,
                "expected_risk": expected_risk,
                "weights": weights,
                "position_sizes": position_sizes,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

##############################################################################
# API Endpoints
##############################################################################

# Initialize FastAPI application
app = FastAPI(
    title="Enhanced Trading System API",
    description="Institutional-grade trading system with advanced risk management",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global resources
alert_handler = None
error_recovery = None
db_manager = None
backup_manager = None

class DatabaseManager:
    def __init__(self, db_url: str = config.database_url):
        self.db_url = db_url
        self.connection = None

    async def initialize(self):
        try:
            db_path = self.db_url.replace("sqlite:///", "")
            self.connection = await aiosqlite.connect(db_path)
            logger.info("Database connection initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise

    async def ensure_connection(self):
        if self.connection is None:
            logger.warning("Database connection is None. Reinitializing...")
            await self.initialize()
        elif self.connection._conn is None or self.connection._conn.closed:
            logger.warning("Database connection was closed. Reinitializing...")
            await self.initialize()

    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("Database connection closed.")

# Lifespan context manager
@asynccontextmanager
async def enhanced_lifespan(app: FastAPI):
    """Enhanced lifespan context manager with all components"""
    # Create global resources
    global alert_handler, error_recovery, db_manager, backup_manager

    # Initialize database manager with PostgreSQL
    db_manager = PostgresDatabaseManager()
    await db_manager.initialize()

    # Initialize backup manager
    backup_manager = BackupManager(db_manager=db_manager)

    # Initialize error recovery system
    error_recovery = ErrorRecoverySystem()

    # Initialize enhanced alert handler
    alert_handler = EnhancedAlertHandler()

    # Load configuration from environment
    logger.info(f"Starting application with config: {config.dict()}")

    # Initialize components
    try:
        # Create backup directory if it doesn't exist
        os.makedirs(config.backup_dir, exist_ok=True)
    
        # Start error recovery monitoring
        asyncio.create_task(error_recovery.schedule_stale_position_check())

        # Start alert handler
        await alert_handler.start()

        # Start scheduled tasks
        alert_task = asyncio.create_task(alert_handler.handle_scheduled_tasks())
        backup_task = asyncio.create_task(backup_manager.schedule_backups(24))  # Daily backups

        # Start rate limiter cleanup
        if hasattr(app.state, "rate_limiter"):
            await app.state.rate_limiter.start_cleanup()

        logger.info("Application startup completed successfully")
        yield
        logger.info("Shutting down application")

        # Cancel scheduled tasks
        alert_task.cancel()
        backup_task.cancel()
        try:
            await alert_task
            await backup_task
        except asyncio.CancelledError:
            pass

        # Shutdown alert handler
        await alert_handler.stop()

        # Create final backup before shutdown
        await backup_manager.create_backup(include_market_data=True, compress=True)

        # Clean up sessions
        await cleanup_stale_sessions()

    except Exception as e:
        logger.error(f"Error during lifecycle: {str(e)}")
        logger.error(traceback.format_exc())
        yield
    finally:
        # Close database connection
        if db_manager:
            await db_manager.close()
        logger.info("Application shutdown complete")

# Set up lifespan
app.router.lifespan_context = enhanced_lifespan

# Health check endpoint
@app.get("/api/health", tags=["system"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "ok",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# Status endpoint
@app.get("/api/status", tags=["system"])
async def get_status():
    """Get system status"""
    try:
        status_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
            "environment": config.environment
        }
        
        # Get component statuses if available
        if alert_handler and hasattr(alert_handler, "system_monitor"):
            status_data["system"] = await alert_handler.system_monitor.get_system_status()
            
        if error_recovery:
            status_data["circuit_breaker"] = await error_recovery.get_circuit_breaker_status()
            
        return status_data
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# TradingView webhook endpoint
@app.post("/tradingview", tags=["trading"])
async def tradingview_webhook(request: Request):
    """Endpoint for TradingView alerts"""
    try:
        # Get alert data
        data = await request.json()
        
        # Map fields if needed
        alert_data = {}
        
        for api_field, tv_field in TV_FIELD_MAP.items():
            if tv_field in data:
                alert_data[api_field] = data[tv_field]
                
        # Use direct field if mapping not found
        for field in data:
            if field not in TV_FIELD_MAP.values() and field not in alert_data:
                alert_data[field] = data[field]
                
        # Check for required fields
        required_fields = ["symbol", "action"]
        for field in required_fields:
            if field not in alert_data:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": f"Missing required field: {field}"}
                )
                
        # Process alert
        if alert_handler:
            # Standardize symbol format
            alert_data["symbol"] = standardize_symbol(alert_data["symbol"])
            
            # Process alert (in background)
            asyncio.create_task(alert_handler.process_alert(alert_data))
            
            return {
                "status": "success",
                "message": f"Alert received for {alert_data['symbol']} {alert_data['action']}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Alert handler not initialized"}
            )
    except Exception as e:
        logger.error(f"Error processing TradingView webhook: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Manual trade endpoint
@app.post("/api/trade", tags=["trading"])
async def manual_trade(request: Request):
    """Endpoint for manual trade execution"""
    try:
        # Get trade data
        data = await request.json()
        
        # Check for required fields
        required_fields = ["symbol", "action", "percentage"]
        for field in required_fields:
            if field not in data:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": f"Missing required field: {field}"}
                )
                
        # Validate action
        valid_actions = ["BUY", "SELL", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]
        if data["action"].upper() not in valid_actions:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": f"Invalid action: {data['action']}. Must be one of: {', '.join(valid_actions)}"}
            )
            
        # Validate percentage
        try:
            percentage = float(data["percentage"])
            if percentage <= 0:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Percentage must be positive"}
                )
        except ValueError:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Percentage must be a number"}
            )
            
        # Process trade
        if alert_handler:
            # Standardize symbol format
            data["symbol"] = standardize_symbol(data["symbol"])
            
            # Add timestamp
            data["timestamp"] = datetime.now(timezone.utc).isoformat()
            
            # Process alert
            result = await alert_handler.process_alert(data)
            
            return result
        else:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Alert handler not initialized"}
            )
    except Exception as e:
        logger.error(f"Error processing manual trade: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Get positions endpoint
@app.get("/api/positions", tags=["positions"])
async def get_positions(
    status: Optional[str] = Query(None, description="Filter by position status (open, closed)"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of positions to return"),
    offset: int = Query(0, ge=0, description="Number of positions to skip")
):
    """Get positions"""
    try:
        if not alert_handler or not hasattr(alert_handler, "position_tracker"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Position tracker not initialized"}
            )
            
        # Get positions based on status
        if status == "open":
            positions = await alert_handler.position_tracker.get_open_positions()
            
            # Flatten positions
            flattened = []
            for symbol_positions in positions.values():
                flattened.extend(symbol_positions.values())
                
            # Sort by open time (newest first)
            flattened.sort(key=lambda x: x.get("open_time", ""), reverse=True)
            
        elif status == "closed":
            positions = await alert_handler.position_tracker.get_closed_positions(limit * 2)  # Get more to allow for filtering
            flattened = list(positions.values())
            
            # Sort by close time (newest first)
            flattened.sort(key=lambda x: x.get("close_time", ""), reverse=True)
            
        else:
            # Get all positions
            open_positions = await alert_handler.position_tracker.get_open_positions()
            closed_positions = await alert_handler.position_tracker.get_closed_positions(limit * 2)
            
            # Flatten open positions
            open_flattened = []
            for symbol_positions in open_positions.values():
                open_flattened.extend(symbol_positions.values())
                
            # Combine and flatten
            flattened = open_flattened + list(closed_positions.values())
            
            # Sort by open time (newest first)
            flattened.sort(key=lambda x: x.get("open_time", ""), reverse=True)
            
        # Filter by symbol if provided
        if symbol:
            symbol = standardize_symbol(symbol)
            flattened = [p for p in flattened if p.get("symbol") == symbol]
            
        # Apply pagination
        paginated = flattened[offset:offset + limit]
        
        return {
            "positions": paginated,
            "count": len(paginated),
            "total": len(flattened),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting positions: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Get position by ID endpoint
@app.get("/api/positions/{position_id}", tags=["positions"])
async def get_position(position_id: str):
    """Get position by ID"""
    try:
        if not alert_handler or not hasattr(alert_handler, "position_tracker"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Position tracker not initialized"}
            )
            
        # Get position
        position = await alert_handler.position_tracker.get_position_info(position_id)
        
        if not position:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"error": f"Position {position_id} not found"}
            )
            
        return position
    except Exception as e:
        logger.error(f"Error getting position {position_id}: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Update position endpoint
@app.put("/api/positions/{position_id}", tags=["positions"])
async def update_position(position_id: str, request: Request):
    """Update position (e.g., stop loss, take profit)"""
    try:
        if not alert_handler or not hasattr(alert_handler, "position_tracker"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Position tracker not initialized"}
            )
            
        # Get update data
        data = await request.json()
        
        # Get current position
        position = await alert_handler.position_tracker.get_position_info(position_id)
        
        if not position:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"error": f"Position {position_id} not found"}
            )
            
        # Check if position is closed
        if position.get("status") == "closed":
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Cannot update closed position"}
            )
            
        # Extract updatable fields
        stop_loss = data.get("stop_loss")
        take_profit = data.get("take_profit")
        
        # Convert to float if provided
        if stop_loss is not None:
            try:
                stop_loss = float(stop_loss)
            except ValueError:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Invalid stop loss value"}
                )
                
        if take_profit is not None:
            try:
                take_profit = float(take_profit)
            except ValueError:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Invalid take profit value"}
                )
                
        # Extract metadata
        metadata = data.get("metadata")
        
        # Update position
        success = await alert_handler.position_tracker.update_position(
            position_id=position_id,
            stop_loss=stop_loss,
            take_profit=take_profit,
            metadata=metadata
        )
        
        if not success:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"error": "Failed to update position"}
            )
            
        # Get updated position
        updated_position = await alert_handler.position_tracker.get_position_info(position_id)
        
        return {
            "status": "success",
            "message": "Position updated",
            "position": updated_position
        }
    except Exception as e:
        logger.error(f"Error updating position {position_id}: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Close position endpoint
@app.post("/api/positions/{position_id}/close", tags=["positions"])
async def close_position(position_id: str, request: Request):
    """Close a position"""
    try:
        if not alert_handler or not hasattr(alert_handler, "position_tracker"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Position tracker not initialized"}
            )
            
        # Get request data
        data = await request.json()
        
        # Get current position
        position = await alert_handler.position_tracker.get_position_info(position_id)
        
        if not position:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"error": f"Position {position_id} not found"}
            )
            
        # Check if position is already closed
        if position.get("status") == "closed":
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Position is already closed"}
            )
            
        # Get current price if not provided
        exit_price = data.get("price")
        if exit_price is None:
            exit_price = await get_current_price(position["symbol"], "SELL" if position["action"] == "BUY" else "BUY")
        else:
            try:
                exit_price = float(exit_price)
            except ValueError:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Invalid price value"}
                )
                
        # Get reason
        reason = data.get("reason", "manual")
        
        # Close position
        success, result = await alert_handler.position_tracker.close_position(
            position_id=position_id,
            exit_price=exit_price,
            reason=reason
        )
        
        if not success:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"error": "Failed to close position", "details": result.get("error", "Unknown error")}
            )
            
        # Update risk manager
        if hasattr(alert_handler, "risk_manager"):
            await alert_handler.risk_manager.close_position(position_id)
            
        # Update intraday risk monitor
        if hasattr(alert_handler, "intraday_risk_monitor"):
            await alert_handler.intraday_risk_monitor.update_position(position_id, result.get("pnl", 0))
            
        # Log exit in journal
        if hasattr(alert_handler, "position_journal"):
            await alert_handler.position_journal.record_exit(
                position_id=position_id,
                exit_price=exit_price,
                exit_reason=reason,
                pnl=result.get("pnl", 0),
                execution_time=0.0,  # No execution time for manual close
                slippage=0.0  # No slippage for manual close
            )
            
        return {
            "status": "success",
            "message": f"Position {position_id} closed",
            "position": result
        }
    except Exception as e:
        logger.error(f"Error closing position {position_id}: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Get risk metrics endpoint
@app.get("/api/risk/metrics", tags=["risk"])
async def get_risk_metrics():
    """Get risk management metrics"""
    try:
        if not alert_handler or not hasattr(alert_handler, "risk_manager"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Risk manager not initialized"}
            )
            
        # Get risk metrics
        metrics = await alert_handler.risk_manager.get_risk_metrics()
        
        return {
            "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting risk metrics: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Get market regime endpoint
@app.get("/api/market/regime/{symbol}", tags=["market"])
async def get_market_regime(symbol: str):
    """Get market regime for a symbol"""
    try:
        if not alert_handler or not hasattr(alert_handler, "regime_classifier"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Regime classifier not initialized"}
            )
            
        # Standardize symbol
        symbol = standardize_symbol(symbol)
        
        # Get regime data
        regime_data = alert_handler.regime_classifier.get_regime_data(symbol)
        
        return {
            "symbol": symbol,
            "regime_data": regime_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting market regime for {symbol}: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

# Get volatility state endpoint
@app.get("/api/market/volatility/{symbol}", tags=["market"])
async def get_volatility_state(symbol: str):
    """Get volatility state for a symbol"""
    try:
        if not alert_handler or not hasattr(alert_handler, "volatility_monitor"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Volatility monitor not initialized"}
            )
            
        # Standardize symbol
        symbol = standardize_symbol(symbol)
        
        # Get volatility state
        volatility_state = alert_handler.volatility_monitor.get_volatility_state(symbol)
        
        return {
            "symbol": symbol,
            "volatility_state": volatility_state,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting volatility state for {symbol}: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )

@app.get("/api/database/test", tags=["system"])
async def test_database_connection():
    """Test PostgreSQL database connection"""
    try:
        if not db_manager:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "message": "Database manager not initialized"}
            )
            
        # Test query - count positions
        async with db_manager.pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM positions")
            
        return {
            "status": "ok",
            "message": "PostgreSQL connection successful",
            "positions_count": count,
            "database_url": db_manager.db_url.replace(db_manager.db_url.split('@')[0], '***'),  # Hide credentials
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error", 
                "message": f"Database test failed: {str(e)}"
            }
        )

@app.post("/api/database/test-position", tags=["system"])
async def test_database_position():
    """Test saving and retrieving a position from the PostgreSQL database"""
    try:
        if not db_manager:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "message": "Database manager not initialized"}
            )
        
        # Create a test position
        test_id = f"test_{uuid.uuid4().hex[:8]}"
        test_position = {
            "position_id": test_id,
            "symbol": "TEST_SYMBOL",
            "action": "BUY",
            "timeframe": "H1",
            "entry_price": 100.0,
            "size": 1.0,
            "stop_loss": 95.0,
            "take_profit": 110.0,
            "open_time": datetime.now(timezone.utc),
            "close_time": None,
            "exit_price": None,
            "current_price": 100.0,
            "pnl": 0.0,
            "pnl_percentage": 0.0,
            "status": "open",
            "last_update": datetime.now(timezone.utc),
            "metadata": {"test": True, "note": "This is a test position"},
            "exit_reason": None
        }
        
        # Save position
        await db_manager.save_position(test_position)
        
        # Retrieve position
        retrieved = await db_manager.get_position(test_id)
        
        # Clean up - delete test position
        await db_manager.delete_position(test_id)
        
        if retrieved and retrieved["position_id"] == test_id:
            return {
                "status": "ok",
                "message": "PostgreSQL position test successful",
                "test_id": test_id,
                "retrieved": retrieved is not None,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": "error", 
                    "message": "Failed to retrieve test position"
                }
            )
            
    except Exception as e:
        logger.error(f"Database position test failed: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error", 
                "message": f"Database position test failed: {str(e)}"
            }
        )

# Main entry point
if __name__ == "__main__":
    import uvicorn
    
    # Get host and port from config
    host = config.host
    port = config.port
    
    # Start server
    uvicorn.run("main:app", host=host, port=port, reload=False)
