##############################################################################
# An institutional-grade trading platform with advanced risk management,
# machine learning capabilities, and comprehensive market analysis.
##############################################################################

import os
import json
import uuid
import math
import random
import logging
import traceback
import statistics
import glob
import tarfile
import re
import asyncio
import aiohttp
import asyncpg
import configparser

import oandapyV20
from oandapyV20 import API
import oandapyV20.endpoints.orders as orders
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.accounts as accounts
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any
from contextlib import asynccontextmanager
import numpy as np
from pydantic import BaseModel
from fastapi import FastAPI, Request, Query, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

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
    oanda_account: str = os.environ.get("OANDA_ACCOUNT_ID", "")
    OANDA_ACCESS_TOKEN: str = os.environ.get("OANDA_ACCESS_TOKEN", "")
    oanda_environment: str = os.environ.get("OANDA_ENVIRONMENT", "practice")
    active_exchange: str = os.environ.get("ACTIVE_EXCHANGE", "oanda")

    # Risk parameters
    default_risk_percentage: float = float(os.environ.get("DEFAULT_RISK_PERCENTAGE", 20.0))
    max_risk_percentage: float = float(os.environ.get("MAX_RISK_PERCENTAGE", 20.0))
    max_portfolio_heat: float = float(os.environ.get("MAX_PORTFOLIO_HEAT", 70.0))
    max_daily_loss: float = float(os.environ.get("MAX_DAILY_LOSS", 50.0))

    # Database settings
    database_url: str = os.environ["DATABASE_URL"]  # No default fallback
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
        for key in ["OANDA_ACCESS_TOKEN", "slack_webhook_url", "telegram_bot_token"]:
            if result.get(key):
                result[key] = "******"
        return result

# Initialize config
config = Config()

# Constants
MAX_DAILY_LOSS = config.max_daily_loss / 100  # Convert percentage to decimal
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 20

######################
# Globals and Helpers
######################

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

# Define leverages for different instruments
INSTRUMENT_LEVERAGES = {
    'XAU/USD': 20,
    'XAG/USD': 20,
    'EUR/USD': 30,
    'GBP/USD': 30,
    'USD/JPY': 30,
    'USD/CHF': 30,
    'AUD/USD': 30,
    'NZD/USD': 30,
    'USD/CAD': 30,
    'BTC/USD': 2,
    'ETH/USD': 5,
    'default': 20  # Default leverage for other instruments
}

# Direct Crypto Mapping
CRYPTO_MAPPING = {
    'BTCUSD': 'BTC/USD',
    'ETHUSD': 'ETH/USD',
    'LTCUSD': 'LTC/USD',
    'XRPUSD': 'XRP/USD',
    'BCHUSD': 'BCH/USD',
    'DOTUSD': 'DOT/USD',
    'ADAUSD': 'ADA/USD',
    'SOLUSD': 'SOL/USD'
}

def get_instrument_type(instrument: str) -> str:
        """Return one of: 'FOREX', 'CRYPTO', 'COMMODITY', 'INDICES'."""
        inst = instrument.upper()
        if any(c in inst for c in ['BTC','ETH','XRP','LTC','BCH','DOT','ADA','SOL']):
            return "CRYPTO"
        if any(c in inst for c in ['XAU','XAG','OIL','NATGAS']):
            return "COMMODITY"
        if any(i in inst for i in ['SPX','NAS','US30','UK100','DE30']):
            return "INDICES"
        return "FOREX"

######################
# FastAPI Apps
######################

app = FastAPI()

@app.post("/tradingview", status_code=status.HTTP_200_OK)
async def tradingview_webhook(request: Request):
    raw = await request.json()   # ← no more undefined `request`
    
    # ── FX slash‑insertion logic ──
    raw_ticker = raw.get('ticker', '').upper()
    if '/' not in raw_ticker and len(raw_ticker) == 6 and get_instrument_type(raw_ticker) == "FOREX":
        # e.g. "GBPUSD" → "GBP/USD"
        raw_ticker = raw_ticker[:3] + '/' + raw_ticker[3:]

    # ── Now normalize via crypto‑mapping and underscore ──
    instr = CRYPTO_MAPPING.get(raw_ticker, raw_ticker)  # maps BTCUSD→BTC/USD
    oanda_instrument = instr.replace('/', '_')           # maps GBP/USD→GBP_USD
    
    # 2) Build the unified payload
    payload = {
        'instrument':   instr,
        'direction':    raw.get('strategy.order.action', '').upper(),
        'risk_percent': float(raw.get('strategy.risk.size', 5)),
        'timeframe':    raw.get('timeframe', '1H'),
        'entry_price':  raw.get('strategy.order.price'),
        'stop_loss':    raw.get('strategy.order.stop_loss'),
        'take_profit':  raw.get('strategy.order.take_profit'),
    }
    
    # 3) Call your processor
    result = process_tradingview_alert(payload)
        
    # FastAPI will serialize this dict automatically,
    # but wrapping in JSONResponse lets you set status or headers if you like
    return JSONResponse(content=result)

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

# —— Start OANDA credential loading —— 
# 1) Read from environment first
OANDA_ACCESS_TOKEN       = os.getenv('OANDA_ACCESS_TOKEN')
OANDA_ENVIRONMENT = os.getenv('OANDA_ENVIRONMENT', 'practice')
OANDA_ACCOUNT_ID  = os.getenv('OANDA_ACCOUNT_ID')

# 2) If any are missing, fall back to config.ini (for local dev)
if not (OANDA_ACCESS_TOKEN and OANDA_ACCOUNT_ID):
    config = configparser.ConfigParser()
    config.read('config.ini')
    try:
        OANDA_ACCESS_TOKEN       = OANDA_ACCESS_TOKEN       or config.get('oanda', 'access_token')
        OANDA_ENVIRONMENT = OANDA_ENVIRONMENT or config.get('oanda', 'environment')
        OANDA_ACCOUNT_ID  = OANDA_ACCOUNT_ID  or config.get('oanda', 'account_id')
    except configparser.NoSectionError:
        raise RuntimeError(
            "Missing OANDA credentials: set env vars OANDA_ACCESS_TOKEN & OANDA_ACCOUNT_ID, "
            "or add an [oanda] section in config.ini."
        )

# 3) Instantiate the OANDA client
oanda = oandapyV20.API(
    access_token=OANDA_ACCESS_TOKEN,
    environment=OANDA_ENVIRONMENT
)
# —— End credential loading ——

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
    Error handling and recovery system that monitors
    for stalled operations and recovers from system failures.
    """
    def __init__(self):
        """Initialize error recovery system"""
        self.stale_position_threshold = 300  # seconds
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
        """Record an error"""
        self.daily_error_count += 1
        
        # Log the error
        logger.error(f"Error recorded: {error_type} - {json.dumps(details)}")
        
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
    
    logger.debug("Created new aiohttp session")
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

def get_current_market_session() -> str:
        """Return 'asian', 'london', 'new_york', or 'weekend' by UTC now."""
        now = datetime.utcnow()
        if now.weekday() >= 5:
            return 'weekend'
        h = now.hour
        if 22 <= h or h < 7:
            return 'asian'
        if 7 <= h < 16:
            return 'london'
        return 'new_york'

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

    async def restore_from_backup(self, backup_path: str) -> bool:
        """Restore database from a PostgreSQL backup file"""
        try:
            import subprocess
            
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
                    self.logger.info(f"Database restored from {backup_path}")
                    return True
                else:
                    self.logger.error(f"pg_restore failed: {result.stderr}")
                    return False
            else:
                self.logger.error("Database URL is not in the expected format")
                return False
                
        except Exception as e:
            self.logger.error(f"Error restoring database from backup: {str(e)}")
            return False

async def restore_from_backup(backup_path: str) -> bool:
    """Restore database from a PostgreSQL backup file"""
    try:
        import subprocess
        
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
# Market Data Functions / Trade Execution
##############################################################################

def instrument_is_commodity(instrument: str) -> bool:
        return get_instrument_type(instrument) == "COMMODITY"
    
def get_commodity_pip_value(instrument: str) -> float:
        inst = instrument.upper()
        if 'XAU' in inst:   return 0.01
        if 'XAG' in inst:   return 0.001
        if 'OIL' in inst or 'WTICO' in inst: return 0.01
        if 'NATGAS' in inst: return 0.001
        return 0.0001

def execute_oanda_order(
    instrument: str, direction: str, risk_percent: float,
    entry_price: float = None, stop_loss: float = None,
    take_profit: float = None, timeframe: str = '1H',
    atr_multiplier: float = 1.5, **_
) -> dict:
    """
    Places a MARKET order on Oanda with SL/TP based on static ATR.
    """
    try:
        account_id = config.get('oanda','account_id')
        balance = float(oanda.account.get(account_id)['account']['balance'])
        oanda_inst = instrument.replace('/','_')
        dir_mult = -1 if direction.upper()=='SELL' else 1
        risk_amt = balance * (risk_percent/100)

        # Fetch current price if needed
        if not entry_price:
            pr = oanda.pricing.get(accountID=account_id, instruments=[oanda_inst])
            prices = pr['prices'][0]
            entry_price = float(
                prices['bids'][0]['price'] 
                if direction.upper()=='SELL' 
                else prices['asks'][0]['price']
            )

        # Compute stop_loss via ATR if missing
        if not stop_loss:
            atr = asyncio.run(get_atr(instrument, timeframe))
            stop_dist = (atr * atr_multiplier)
            stop_loss = entry_price - dir_mult * stop_dist

        # Compute pip value & units
        pip = 0.0001
        if 'JPY' in oanda_inst:
            pip = 0.01
        elif instrument_is_commodity(instrument):
            pip = get_commodity_pip_value(instrument)

        dist_pips = abs(entry_price - stop_loss) / pip
        units = int(risk_amt / (dist_pips * pip)) * dir_mult

        # ── **NEW**: guard against zero‑unit orders ──
        if units == 0:
            logger.warning(f"[OANDA] Not sending order for {oanda_inst}: calculated units=0")
            return {"success": False, "error": "units_zero"}

        # Build order payload (you can also switch FOK→IOC here)
        order_data = {
            "order": {
                "type": "MARKET",
                "instrument": oanda_inst,
                "units": str(units),
                "timeInForce": "IOC",      # Changed from FOK → IOC
                "positionFill": "DEFAULT"
            }
        }
        if stop_loss:
            order_data["order"]["stopLossOnFill"] = {
                "price": str(round(stop_loss,5)),
                "timeInForce": "GTC"
            }
        if take_profit:
            order_data["order"]["takeProfitOnFill"] = {
                "price": str(round(take_profit,5)),
                "timeInForce": "GTC"
            }

        # ── **NEW**: log payload & response for full visibility ──
        logger.debug(f"[OANDA] ORDER PAYLOAD: {order_data}")
        response = oanda.order.create(accountID=account_id, data=order_data)
        logger.debug(f"[OANDA] ORDER RESPONSE: {response}")

        tx = response.get('orderFillTransaction')
        if tx:
            return {
                "success": True,
                "order_id": tx['id'],
                "instrument": oanda_inst,
                "direction": direction,
                "entry_price": float(tx['price']),
                "units": int(tx['units']),
                "stop_loss": stop_loss,
                "take_profit": take_profit
            }

        return {"success": False, "error": "Order creation failed", "details": response}

    except Exception as e:
        logger.error(f"Oanda execution error: {e}")
        return {"success": False, "error": str(e)}

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

async def get_atr(instrument: str, timeframe: str, period: int = 14) -> float:
    """
    Return a static ATR value based on instrument type & timeframe,
    with a safe fallback to FOREX when the type is unrecognized.
    """
    # 1) Default ATR tables
    default_atr_values = {
        "FOREX":    {"15M":0.0010, "1H":0.0025, "4H":0.0050, "1D":0.0100},
        "CRYPTO":   {"15M":0.20,   "1H":0.50,   "4H":1.00,   "1D":2.00},
        "COMMODITY":{
            "XAU_USD": {"15M":0.10, "1H":0.25, "4H":0.50, "1D":1.00},
            "XAG_USD": {"15M":0.006,"1H":0.015,"4H":0.030,"1D":0.060},
            "OIL":     {"15M":0.03, "1H":0.08, "4H":0.15, "1D":0.30},
            "NATGAS":  {"15M":0.01, "1H":0.025,"4H":0.05, "1D":0.10}
        },
        "INDICES": {
            "US30":   {"15M":15,  "1H":40,  "4H":80,  "1D":150},
            "SPX500": {"15M":5,   "1H":12,  "4H":25,  "1D":45},
            "NAS100": {"15M":20,  "1H":50,  "4H":100, "1D":200}
        }
    }

    # 2) Determine type & safe‑fallback
    inst_type = get_instrument_type(instrument)  # your helper
    if inst_type not in default_atr_values:
        inst_type = "FOREX"

    # 3) Handle COMMODITY / INDICES lookups
    if inst_type in ("COMMODITY", "INDICES"):
        for key, table in default_atr_values[inst_type].items():
            if key in instrument.upper():
                return table.get(timeframe, table["1H"])
        # fallback to first table
        first_key = next(iter(default_atr_values[inst_type]))
        table = default_atr_values[inst_type][first_key]
        return table.get(timeframe, table["1H"])

    # 4) Standard FOREX/CRYPTO lookup
    return default_atr_values[inst_type].get(
        timeframe,
        default_atr_values[inst_type]["1H"]
    )

async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    """
    Calculate ATR for a given symbol/timeframe using OANDA's 
    mid-price candles (Option 3).
    """
    try:
        # 1) Fetch the last (period + 1) candles
        params = {
            "count": period + 1,
            "granularity": timeframe,
            "price": "M"
        }
        req = pricing.PricingInfo(accountID=OANDA_ACCOUNT_ID, params={"instruments": [symbol]})
        # If you're using the InstrumentsCandles endpoint instead:
        # req = candles.InstrumentsCandles(instrument=symbol, params=params)
        resp = oanda.request(req)
        candles = resp.get("candles", [])
        
        # 2) Not enough data?
        if len(candles) < period + 1:
            logger.warning(f"Not enough candles for ATR({symbol}): need {period+1}, got {len(candles)}")
            return 0.0
        
        # 3) Extract mid‑price arrays
        highs  = np.array([float(c["mid"]["h"]) for c in candles])
        lows   = np.array([float(c["mid"]["l"]) for c in candles])
        closes = np.array([float(c["mid"]["c"]) for c in candles])
        
        # 4) True range calculations
        tr1 = highs[1:] - lows[1:]
        tr2 = np.abs(highs[1:] - closes[:-1])
        tr3 = np.abs(lows[1:]  - closes[:-1])
        true_ranges = np.maximum.reduce([tr1, tr2, tr3])
        
        # 5) Average them
        return float(np.mean(true_ranges))
    
    except KeyError as e:
        logger.error(f"ATR KeyError for {symbol}: missing {e}")
        return 0.0
    except IndexError as e:
        logger.error(f"ATR IndexError for {symbol}: {e}")
        return 0.0
    except Exception as e:
        logger.exception(f"Error getting ATR for {symbol}: {e}")
        return 0.0

def process_tradingview_alert(data: dict) -> dict:
    """Map TV fields, normalize symbol, then execute via Oanda."""
    mapped = {}
    for tv_field, fld in TV_FIELD_MAP.items():
        if tv_field in data:
            mapped[fld] = data[tv_field]
    # Ensure required keys
    for req in ('instrument','direction','risk_percent'):
        if req not in mapped:
            return {"success":False, "error":f"Missing {req}"}

    # Normalize instrument
    inst = mapped['instrument']
    if inst in CRYPTO_MAPPING:
        inst = CRYPTO_MAPPING[inst]
    mapped['instrument'] = inst

    # Add defaults
    mapped.setdefault('session', get_current_market_session())
    mapped.setdefault('timeframe', data.get('timeframe','1H'))

    return execute_oanda_order(
        instrument=inst,
        direction=mapped['direction'],
        risk_percent=float(mapped['risk_percent']),
        entry_price=float(mapped.get('entry_price')) if mapped.get('entry_price') else None,
        stop_loss=float(mapped.get('stop_loss')) if mapped.get('stop_loss') else None,
        take_profit=float(mapped.get('take_profit')) if mapped.get('take_profit') else None,
        timeframe=mapped['timeframe']
    )

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

@async_error_handler()
async def get_account_balance() -> float:
    """Get current account balance from Oanda"""
    try:
        # Determine API URL based on environment (practice/live)
        base_url = "https://api-fxpractice.oanda.com" if config.oanda_environment == "practice" else "https://api-fxtrade.oanda.com"
        endpoint = f"/v3/accounts/{config.oanda_account}/summary"
        
        # Set up headers with authentication
        headers = {
            "Authorization": f"Bearer {config.OANDA_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Make API request
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base_url}{endpoint}", headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    # Extract account equity (NAV) from response
                    balance = float(data["account"]["NAV"])
                    logger.info(f"Current account balance: {balance}")
                    return balance
                else:
                    error_data = await response.text()
                    logger.error(f"Error fetching account balance: {response.status} - {error_data}")
                    # Fall back to last known balance or default
                    return 10000.0
    except Exception as e:
        logger.error(f"Failed to get account balance: {str(e)}")
        # Fall back to default in case of error
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
    def __init__(self, max_risk_per_trade=0.20, max_portfolio_risk=0.70):
        self.max_risk_per_trade = max_risk_per_trade  # 20% per trade default
        self.max_portfolio_risk = max_portfolio_risk  # 70% total portfolio risk
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
        self.portfolio_heat_limit = 0.70  # Maximum portfolio heat allowed
        self.portfolio_concentration_limit = 0.20  # Maximum concentration in single instrument
        self.correlation_limit = 0.70  # Correlation threshold for risk adjustment
        
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
# Enhanced Alert Handler
##############################################################################

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
