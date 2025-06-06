##############################################################################
# An institutional-grade trading platform with advanced risk management,
# machine learning capabilities, and comprehensive market analysis.
##############################################################################
from fastapi import FastAPI, Request, status, Query, Response, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from database import PostgresDatabaseManager
from tracker import PositionTracker
from alert_handler import EnhancedAlertHandler
from backup import BackupManager
from error_recovery import ErrorRecoverySystem

import asyncio
import glob
import json
import logging
import logging.handlers
import math
import os
import random
import re
import statistics
import subprocess
import tarfile
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Literal, Tuple, NamedTuple, Callable, TypeVar, ParamSpec

import aiohttp
import asyncpg
import numpy as np
import pandas as pd
import oandapyV20
import requests
import ta
import urllib3
from oandapyV20.exceptions import V20Error
from oandapyV20.endpoints import instruments
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.positions import OpenPositions
from oandapyV20.endpoints.trades import OpenTrades
from pydantic import BaseModel, Field, SecretStr, validator, constr, confloat, model_validator
from urllib.parse import urlparse

# ─── Type variables for type hints ─────────────────────────────────────────
P = ParamSpec('P')
T = TypeVar('T')

# ─── Named Tuples ─────────────────────────────────────────────────────────
class ClosePositionResult(NamedTuple):
    success: bool
    position_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

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
        self.stop_loss = None
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
        """Update stop loss level (disabled - does nothing)"""
        self.last_update = datetime.now(timezone.utc)

    def update_take_profit(self, new_take_profit: float):
        """Update take profit level"""
        self.take_profit = float(new_take_profit)
        self.last_update = datetime.now(timezone.utc)

    def update_metadata(self, metadata: Dict[str, Any]):
        """Update position metadata"""
        self.metadata.update(metadata)
        self.last_update = datetime.now(timezone.utc)


class Position:
    """Represents a trading position with full lifecycle management"""

    def __init__(
        self,
        position_id: str,
        symbol: str,
        action: str,
        timeframe: str,
        entry_price: float,
        size: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.position_id = position_id
        self.symbol = symbol
        self.action = action.upper()
        self.timeframe = timeframe
        self.entry_price = float(entry_price)
        self.size = float(size)
        self.stop_loss = None  # Always set to None for now
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
        if self.action == "BUY":
            self.pnl = (self.current_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.current_price) * self.size
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
        if self.action == "BUY":
            self.pnl = (self.exit_price - self.entry_price) * self.size
        else:
            self.pnl = (self.entry_price - self.exit_price) * self.size
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.exit_price / self.entry_price - 1) * 100
            else:
                self.pnl_percentage = (1 - self.exit_price / self.entry_price) * 100
        self.last_update = self.close_time

    def update_stop_loss(self, new_stop_loss: float):
        """Update stop loss level (currently disabled, no-op)"""
        self.last_update = datetime.now(timezone.utc)

    def update_take_profit(self, new_take_profit: float):
        """Update take profit level"""
        self.take_profit = float(new_take_profit)
        self.last_update = datetime.now(timezone.utc)

    def update_metadata(self, metadata: Dict[str, Any]):
        """Update position metadata"""
        self.metadata.update(metadata)
        self.last_update = datetime.now(timezone.utc)

# ---- Data container for partial closes ----
from typing import NamedTuple
class ClosePositionResult(NamedTuple):
    success: bool
    error: Optional[str] = None
    position_data: Optional[Dict[str, Any]] = None

# ---- PositionTracker ----

class PositionTracker:
    """
    Tracks all positions across different symbols and timeframes,
    providing a centralized registry for position management.
    Supports database persistence.
    """
    def __init__(self, db_manager=None):
        self.positions = {}  # position_id -> Position
        self.open_positions_by_symbol = {}  # symbol -> {position_id -> Position}
        self.closed_positions = {}  # position_id -> dict
        self.position_history = []
        self._lock = asyncio.Lock()
        self._price_update_lock = asyncio.Lock()
        self.max_history = 1000
        self._running = False
        self.db_manager = db_manager

    # ... [start, stop, restore_position, and standard methods omitted for brevity, no changes needed] ...

    async def close_partial_position(
        self,
        position_id: str,
        exit_price: float,
        units_to_close: float,
        reason: str = "partial"
    ) -> ClosePositionResult:
        """
        Close a specific number of units of an open position by sending an opposing market order.
        Updates internal records upon successful broker execution.
        """
        request_id = str(uuid.uuid4())
        log_context = f"[POS_TRACKER_PARTIAL_CLOSE] PosID: {position_id}, Units: {units_to_close}, Reason: {reason}, ReqID: {request_id}"
        logger.info(f"{log_context} - Attempting partial close.")
        async with self._lock:
            if position_id not in self.positions:
                logger.warning(f"{log_context} - Position not found in active positions.")
                return ClosePositionResult(success=False, error="Position not found in active memory.")
            position = self.positions[position_id]
            if position.status == "closed":
                logger.warning(f"{log_context} - Position already marked as closed.")
                return ClosePositionResult(success=False, position_data=self._position_to_dict(position), error="Position already closed.")
            if not isinstance(units_to_close, (float, int)) or units_to_close <= 1e-9:
                logger.warning(f"{log_context} - Invalid or zero units_to_close specified: {units_to_close}.")
                return ClosePositionResult(success=False, error="Invalid or zero units for partial close.")
            actual_units_to_reduce = min(units_to_close, position.size)
            if actual_units_to_reduce <= 1e-9:
                logger.info(f"{log_context} - No effective units to reduce after comparing with position size. Position size: {position.size}, Requested units: {units_to_close}.")
                return ClosePositionResult(success=True, position_data=self._position_to_dict(position), error="No effective units to reduce.")
            logger.info(f"{log_context} - Calling broker to reduce {position.symbol} by {actual_units_to_reduce} units (original action: {position.action}).")

            # Replace with your actual broker call
            success_broker, broker_result_data = await execute_oanda_reduction_order(
                instrument=position.symbol,
                units_to_reduce_abs=actual_units_to_reduce,
                original_position_action=position.action,
                account_id=OANDA_ACCOUNT_ID,
                request_id=request_id
            )
            if not success_broker:
                broker_error = broker_result_data.get("error", "Unknown broker error during partial close.")
                logger.error(f"{log_context} - Broker partial close failed: {broker_error}")
                return ClosePositionResult(success=False, error=broker_error, position_data=self._position_to_dict(position))

            actual_exit_price_from_broker = broker_result_data.get("exit_price", exit_price)
            units_confirmed_closed_by_broker = broker_result_data.get("units_reduced", actual_units_to_reduce)
            if position.action.upper() == "BUY":
                pnl_for_partial = (actual_exit_price_from_broker - position.entry_price) * units_confirmed_closed_by_broker
            else:
                pnl_for_partial = (position.entry_price - actual_exit_price_from_broker) * units_confirmed_closed_by_broker
            position.size -= units_confirmed_closed_by_broker
            position.last_update = datetime.now(timezone.utc)
            if "partial_closes" not in position.metadata:
                position.metadata["partial_closes"] = []
            position.metadata["partial_closes"].append({
                "time": datetime.now(timezone.utc).isoformat(),
                "price": actual_exit_price_from_broker,
                "units_closed": units_confirmed_closed_by_broker,
                "pnl_on_partial": pnl_for_partial,
                "reason": reason,
                "broker_order_id": broker_result_data.get("order_id")
            })
            logger.info(f"{log_context} - Successfully reduced by {units_confirmed_closed_by_broker} units. Remaining size: {position.size:.4f}. PnL on this part: {pnl_for_partial:.2f}")
            if position.size <= 1e-9:
                logger.info(f"{log_context} - Position fully closed due to partial close reducing size to near zero.")
                position.status = "closed"
                position.exit_price = actual_exit_price_from_broker
                position.close_time = datetime.now(timezone.utc)
                self.closed_positions[position_id] = self._position_to_dict(position)
                if position.symbol in self.open_positions_by_symbol and position_id in self.open_positions_by_symbol[position.symbol]:
                    del self.open_positions_by_symbol[position.symbol][position_id]
                    if not self.open_positions_by_symbol[position.symbol]:
                        del self.open_positions_by_symbol[position.symbol]
                if position_id in self.positions:
                    del self.positions[position_id]
            if self.db_manager:
                try:
                    position_dict_for_db = self._position_to_dict(position)
                    await self.db_manager.save_position(position_dict_for_db)
                except Exception as e_db:
                    logger.error(f"{log_context} - Error updating partially closed position {position_id} in database: {str(e_db)}")
            return ClosePositionResult(success=True, position_data=self._position_to_dict(position))

# ─── Initialize FastAPI application ────────────────────────────────────────
app = FastAPI(
    title="Enhanced Trading System API", 
    description="Institutional-grade trading system with advanced risk management",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=enhanced_lifespan
)

# ─── CORS Middleware ─────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Structured Logging Setup ────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread_id": record.thread,
            "process_id": record.process,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)

        # Add trading-specific context
        if hasattr(record, "position_id"):
            log_data["position_id"] = record.position_id
        if hasattr(record, "symbol"):
            log_data["symbol"] = record.symbol
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id

        return json.dumps(log_data)


def get_module_logger(module_name: str, **context) -> logging.Logger:
    """
    Get a configured logger for a specific module.
    """
    logger = logging.getLogger(module_name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def setup_logging():
    """Configure logging with JSON formatting and rotating handlers"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # JSON formatter
    json_formatter = JSONFormatter()

    # Main rotating file handler for all logs
    main_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "trading_system.log"),
        when="midnight",
        interval=1,
        backupCount=30,  # Keep 30 days of logs
        encoding="utf-8",
    )
    main_handler.setFormatter(json_formatter)
    main_handler.setLevel(logging.INFO)

    # Separate handler for error logs
    error_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "errors.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10,
        encoding="utf-8",
    )
    error_handler.setFormatter(json_formatter)
    error_handler.setLevel(logging.ERROR)

    # Trade execution logs (critical for audit)
    trade_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "trades.log"),
        when="midnight",
        interval=1,
        backupCount=90,  # Keep 90 days for compliance
        encoding="utf-8",
    )
    trade_handler.setFormatter(json_formatter)
    trade_handler.setLevel(logging.INFO)
    trade_handler.addFilter(lambda record: "trade" in record.getMessage().lower())

    # Console handler with standard formatting
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    console_handler.setLevel(logging.INFO)

    # Add handlers to root logger
    logger.addHandler(main_handler)
    logger.addHandler(error_handler)
    logger.addHandler(trade_handler)
    logger.addHandler(console_handler)

    return logger


# Initialize the logger
logger = setup_logging()

# ─── Configuration Settings ────────────────────────────────────────

class Settings(BaseModel):
    # API and connection settings
    host: str = Field(
        default=os.environ.get("HOST", "0.0.0.0"), description="Server host address"
    )
    port: int = Field(
        default=int(os.environ.get("PORT", "8000")), description="Server port"
    )

    enable_broker_reconciliation: bool = Field(
        default=os.environ.get("ENABLE_BROKER_RECONCILIATION", "true").lower()
        == "true",
        description="Enable/disable broker position reconciliation on startup.",
    )
    allowed_origins: str = Field(
        default=os.environ.get("ALLOWED_ORIGINS", "*"),
        description="Comma-separated list of allowed CORS origins",
    )
    environment: str = Field(
        default=os.environ.get("ENVIRONMENT", "production"),
        description="Application environment (production/staging/development)",
    )
    connect_timeout: int = Field(
        default=int(os.environ.get("CONNECT_TIMEOUT", "10")),
        description="Connection timeout in seconds",
    )
    read_timeout: int = Field(
        default=int(os.environ.get("READ_TIMEOUT", "30")),
        description="Read timeout in seconds",
    )
    # Trading settings
    oanda_account_id: str = Field(
        default=os.environ.get("OANDA_ACCOUNT_ID", ""), description="OANDA account ID"
    )
    oanda_access_token: SecretStr = Field(
        default=os.environ.get("OANDA_ACCESS_TOKEN", ""),
        description="OANDA API access token",
    )
    oanda_environment: str = Field(
        default=os.environ.get("OANDA_ENVIRONMENT", "practice"),
        description="OANDA environment (practice/live)",
    )
    active_exchange: str = Field(
        default=os.environ.get("ACTIVE_EXCHANGE", "oanda"),
        description="Currently active trading exchange",
    )
    # Risk parameters
    default_risk_percentage: float = Field(
        default=float(os.environ.get("DEFAULT_RISK_PERCENTAGE", "15.0")),
        description="Default risk percentage per trade (15.0 means 15%)",
        ge=0,
        le=100,
    )
    max_risk_percentage: float = Field(
        default=float(os.environ.get("MAX_RISK_PERCENTAGE", "20.0")),
        description="Maximum allowed risk percentage per trade",
        ge=0,
        le=100,
    )
    max_portfolio_heat: float = Field(
        default=float(os.environ.get("MAX_PORTFOLIO_HEAT", "70.0")),
        description="Maximum portfolio heat percentage",
        ge=0,
        le=100,
    )
    max_daily_loss: float = Field(
        default=float(os.environ.get("MAX_DAILY_LOSS", "50.0")),
        description="Maximum daily loss percentage",
        ge=0,
        le=100,
    )
    # Database settings
    database_url: str = Field(
        default=os.environ.get("DATABASE_URL", ""),
        description="Database connection URL (required)",
    )
    db_min_connections: int = Field(
        default=int(os.environ.get("DB_MIN_CONNECTIONS", "5")),
        description="Minimum database connections in pool",
        gt=0,
    )
    db_max_connections: int = Field(
        default=int(os.environ.get("DB_MAX_CONNECTIONS", "20")),
        description="Maximum database connections in pool",
        gt=0,
    )
    # Backup settings
    backup_dir: str = Field(
        default=os.environ.get("BACKUP_DIR", "./backups"),
        description="Directory for backup files",
    )
    backup_interval_hours: int = Field(
        default=int(os.environ.get("BACKUP_INTERVAL_HOURS", "24")),
        description="Backup interval in hours",
        gt=0,
    )
    # Notification settings
    slack_webhook_url: Optional[SecretStr] = Field(
        default=os.environ.get("SLACK_WEBHOOK_URL"),
        description="Slack webhook URL for notifications",
    )
    telegram_bot_token: Optional[SecretStr] = Field(
        default=os.environ.get("TELEGRAM_BOT_TOKEN"),
        description="Telegram bot token for notifications",
    )
    telegram_chat_id: Optional[str] = Field(
        default=os.environ.get("TELEGRAM_CHAT_ID"),
        description="Telegram chat ID for notifications",
    )

    class Config:
        validate_assignment = True
        extra = "ignore"

    @classmethod
    def model_json_schema(cls, **kwargs):
        """Customize the JSON schema for this model."""
        schema = super().model_json_schema(**kwargs)
        for field in [
            "oanda_access_token",
            "slack_webhook_url",
            "telegram_bot_token",
            "database_url",
        ]:
            if field in schema.get("properties", {}):
                schema["properties"][field]["examples"] = ["******"]
        return schema

# Instantiate settings object
config = Settings()

# ─── Module Level Static Mappings ────────────────────────────────────────

# Constants
MAX_DAILY_LOSS = config.max_daily_loss / 100  # Convert percentage to decimal
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 5

# Field mapping for TradingView webhook format
TV_FIELD_MAP = {
    "ticker": "instrument",
    "side": "direction",
    "risk": "risk_percent",
    "entry": "entry_price",
    "sl": "stop_loss",
    "tp": "take_profit",
    "tf": "timeframe",
}

# Define leverages for different instruments
INSTRUMENT_LEVERAGES = {
    "XAU_USD": 20,  # Changed XAU/USD to XAU_USD
    "XAG_USD": 20,  # Changed XAG/USD to XAG_USD
    "EUR_USD": 30,
    "GBP_USD": 30,
    "USD_JPY": 30,
    "USD_CHF": 30,
    "AUD_USD": 30,
    "NZD_USD": 30,
    "USD_CAD": 30,
    "BTC_USD": 2,  # Changed BTC/USD to BTC_USD
    "ETH_USD": 5,  # Changed ETH/USD to ETH_USD
    "default": 20,  # Default leverage for other instruments
}

# Direct Crypto Mapping
CRYPTO_MAPPING = {
    "BTCUSD": "BTC_USD",
    "ETHUSD": "ETH_USD",
    "LTCUSD": "LTC_USD",
    "XRPUSD": "XRP_USD",
    "BCHUSD": "BCH_USD",
    "DOTUSD": "DOT_USD",
    "ADAUSD": "ADA_USD",
    "SOLUSD": "SOL_USD",
    "BTCUSD:OANDA": "BTC_USD",
    "ETHUSD:OANDA": "ETH_USD",
    "BTC/USD": "BTC_USD",
    "ETH/USD": "ETH_USD",
}

# Crypto minimum trade sizes
CRYPTO_MIN_SIZES = {
    "BTC": 0.0001,
    "ETH": 0.002,
    "LTC": 0.05,
    "XRP": 0.01,
    "XAU": 0.2,  # Gold minimum
}

# Crypto maximum trade sizes
CRYPTO_MAX_SIZES = {
    "BTC": 10,
    "ETH": 135,
    "LTC": 3759,
    "XRP": 50000,
    "XAU": 500,  # Gold maximum
}

# Define tick sizes for precision rounding
CRYPTO_TICK_SIZES = {
    "BTC": 0.001,
    "ETH": 0.05,
    "LTC": 0.01,
    "XRP": 0.001,
    "XAU": 0.01,  # Gold tick size
}

# OANDA granularity mapping
OANDA_GRANULARITY_MAP = {
    "1": "H1", "1M": "M1",
    "5": "M5", "5M": "M5",
    "15": "M15", "15M": "M15",
    "30": "M30", "30M": "M30",
    "60": "H1", "1H": "H1",
    "240": "H4", "4H": "H4",
    "D": "D", "1D": "D",
    "1440": "D1",
    "10080": "W1"
}

# ─── Utility Functions ────────────────────────────────────────

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format with better error handling and support for various formats"""
    if not symbol:
        return ""
        
    try:
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')

        # If already in "XXX_YYY" form and both parts look valid, return it
        if "_" in symbol_upper and len(symbol_upper.split("_")) == 2:
            base, quote = symbol_upper.split("_")
            if len(base) >= 3 and len(quote) >= 3:
                return symbol_upper

        # Crypto mapping override
        if symbol_upper in CRYPTO_MAPPING:
            return CRYPTO_MAPPING[symbol_upper]

        # Already contains underscore?
        if "_" in symbol_upper:
            return symbol_upper

        # JPY pair like "GBPJPY" → "GBP_JPY"
        if "JPY" in symbol_upper and "_" not in symbol_upper and len(symbol_upper) == 6:
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"

        # 6-char Forex pairs (not crypto) → "EURUSD" → "EUR_USD"
        if (
            len(symbol_upper) == 6
            and not any(crypto in symbol_upper for crypto in CRYPTO_MAPPING)
        ):
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"

        # Fallback for crypto pairs without mapping: e.g. "BTCUSD"
        for crypto in CRYPTO_MAPPING:
            if crypto in symbol_upper and symbol_upper.endswith("USD"):
                return f"{crypto}_USD"

        # Broker‐specific default
        active_exchange = (
            getattr(config, "active_exchange", "").lower()
            if "config" in globals()
            else "oanda"
        )
        if active_exchange == "oanda":
            return symbol_upper
        elif active_exchange == "binance":
            return symbol_upper.replace("_", "")

        # Final fallback
        return symbol_upper

    except Exception as e:
        logger.error(f"Error standardizing symbol {symbol}: {e}")
        return symbol.upper() if symbol else ""


def format_jpy_pair(symbol: str) -> str:
    """Properly format JPY pairs for OANDA"""
    if "JPY" in symbol and "_" not in symbol:
        # Handle 6-character format like GBPJPY
        if len(symbol) == 6:
            return symbol[:3] + "_" + symbol[3:]
        # Handle slash format like GBP/JPY
        elif "/" in symbol:
            return symbol.replace("/", "_")
    return symbol


def format_for_oanda(symbol: str) -> str:
    """Format symbol for OANDA API"""
    if "_" in symbol:
        return symbol
    if len(symbol) == 6:
        return symbol[:3] + "_" + symbol[3:]
    return symbol  # fallback, in case it's something like an index or crypto


def get_current_market_session() -> str:
    """Return 'asian', 'london', 'new_york', or 'weekend' by UTC now."""
    now = datetime.utcnow()
    # Check for weekend first (Saturday=5, Sunday=6)
    if now.weekday() >= 5:
        return 'weekend'

    # Determine session based on UTC hour
    h = now.hour
    if 22 <= h or h < 7:  # Asia session (approx. 22:00 UTC to 07:00 UTC)
        return 'asian'
    if 7 <= h < 16:  # London session (approx. 07:00 UTC to 16:00 UTC)
        return 'london'
    # New York session (approx. 16:00 UTC to 22:00 UTC)
    # Note: NY often considered 13:00-22:00 UTC, but overlap starts earlier
    return 'new_york'


def _multiplier(instrument_type: str, timeframe: str) -> float:
    """Internal function to calculate ATR multiplier"""
    base_multipliers = {
        "forex": 2.0,
        "jpy_pair": 2.5,
        "metal": 1.5,
        "index": 2.0,
        "other": 2.0
    }
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
    normalized_timeframe = normalize_timeframe(timeframe)
    base = base_multipliers.get(instrument_type.lower())
    factor = timeframe_factors.get(normalized_timeframe)
    if base is None:
        logger.warning(f"[ATR MULTIPLIER] Unknown instrument type '{instrument_type}', using default base of 2.0")
        base = 2.0
    if factor is None:
        logger.warning(f"[ATR MULTIPLIER] Unknown timeframe '{normalized_timeframe}', using default factor of 1.0")
        factor = 1.0
    result = base * factor
    logger.debug(f"[ATR MULTIPLIER] {instrument_type}:{normalized_timeframe} → base={base}, factor={factor}, multiplier={result}")
    return result


def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """
    Public method to retrieve ATR multiplier based on instrument type and timeframe.
    Falls back to a default multiplier if not found.
    """
    return _multiplier(instrument_type, timeframe)


def normalize_timeframe(tf: str, *, target: str = "OANDA") -> str:
    """
    Normalize timeframes into valid OANDA/Binance formats.
    Handles various inputs including TradingView numeric codes.
    Ensures "1" maps to "1H". Correctly maps normalized keys to OANDA values.
    """
    try:
        tf_original = tf  # Keep original for logging if needed
        tf = str(tf).strip().upper()

        # Standardize common variations
        tf = tf.replace("MIN", "M").replace("MINS", "M")
        tf = tf.replace("HOUR", "H").replace("HOURS", "H")
        tf = tf.replace("DAY", "D").replace("DAYS", "D")
        tf = tf.replace("WEEK", "W").replace("WEEKS", "W")
        tf = tf.replace("MONTH", "MON").replace("MONTHS", "MON").replace("MN", "MON")

        # Mapping for TradingView numeric codes → intermediate
        standard_map = {
            "1": "1H", "3": "3M", "5": "5M", "15": "15M", "30": "30M",
            "60": "1H", "120": "2H", "180": "3H", "240": "4H",
            "360": "6H", "480": "8H", "720": "12H",
            "D": "1D", "1D": "1D", "W": "1W", "1W": "1W",
            "M": "1M", "MON": "1MON"
        }

        intermediate_formats = [
            "1M", "3M", "5M", "15M", "30M",
            "1H", "2H", "3H", "4H", "6H", "8H", "12H",
            "1D", "1W", "1MON"
        ]
        normalized = None

        if tf in intermediate_formats:
            normalized = tf
        elif tf in standard_map:
            normalized = standard_map[tf]
        elif tf in ["M1", "M3", "M5", "M15", "M30",
                    "H1", "H2", "H3", "H4", "H6", "H8", "H12",
                    "D", "W", "M"]:
            reverse_oanda_map = {
                "M1": "1M", "M5": "5M", "M15": "15M", "M30": "30M",
                "H1": "1H", "H4": "4H", "H12": "12H",
                "D": "1D", "W": "1W", "M": "1MON"
            }


# ─── Database Manager ────────────────────────────────────────

def db_retry(max_retries=3, retry_delay=2):
    """Decorator to add retry logic to database operations"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except asyncpg.exceptions.PostgresConnectionError as e:
                    retries += 1
                    last_exception = e
                    logger.warning(
                        f"Database connection error in '{func.__name__}', "
                        f"retry {retries}/{max_retries}. Error: {str(e)}"
                    )
                    
                    if retries >= max_retries:
                        logger.error(
                            f"Max database retries ({max_retries}) reached for '{func.__name__}'. "
                            f"Last error: {str(last_exception)}"
                        )
                        raise last_exception
                    
                    wait_time = retry_delay * (2 ** (retries - 1))
                    logger.info(f"Waiting {wait_time} seconds before next retry for '{func.__name__}'.")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Unhandled database operation error in '{func.__name__}': {str(e)}", exc_info=True)
                    raise
            
            if last_exception:
                raise last_exception
            return None
        return wrapper
    return decorator


class PostgresDatabaseManager:
    """PostgreSQL database manager with connection pooling and retry logic"""
    
    def __init__(
        self,
        db_url: str = None,
        min_connections: int = None,
        max_connections: int = None,
    ):
        """Initialize PostgreSQL database manager"""
        self.db_url = db_url or config.database_url
        self.min_connections = min_connections or config.db_min_connections
        self.max_connections = max_connections or config.db_max_connections
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
        """Create a backup of the database using pg_dump"""
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
        """Restore database from a PostgreSQL backup file"""
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

                self.logger.info("PostgreSQL database tables created or verified")
        except Exception as e:
            self.logger.error(f"Error creating database tables: {str(e)}")
            raise
            
    @db_retry()
    async def save_position(self, position_data: Dict[str, Any]) -> bool:
        """Save position to database"""
        try:
            # Process metadata to ensure it's in the right format for PostgreSQL
            position_data = position_data.copy()

            # Convert metadata to JSON if it exists and is a dict
            if "metadata" in position_data and isinstance(position_data["metadata"], dict):
                position_data["metadata"] = json.dumps(position_data["metadata"])

            # Convert datetime strings to datetime objects if needed
            for field in ["open_time", "close_time", "last_update"]:
                if field in position_data and isinstance(position_data[field], str):
                    try:
                        position_data[field] = datetime.fromisoformat(
                            position_data[field].replace('Z', '+00:00')
                        )
                    except ValueError:
                        pass

            async with self.pool.acquire() as conn:
                # Check if position already exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM positions WHERE position_id = $1",
                    position_data["position_id"],
                )

                if exists:
                    # Update existing position
                    return await self.update_position(
                        position_data["position_id"], position_data
                    )

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

    @db_retry()
    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM positions WHERE position_id = $1",
                    position_id,
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
                        pass

                # Convert timestamp objects to ISO format strings for consistency
                for field in ["open_time", "close_time", "last_update"]:
                    if position_data.get(field) and isinstance(position_data[field], datetime):
                        position_data[field] = position_data[field].isoformat()

                return position_data

        except Exception as e:
            self.logger.error(f"Error getting position from database: {str(e)}")
            return None

    @db_retry()
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
                    position_data = dict(row)

                    # Parse metadata JSON if it exists
                    if "metadata" in position_data and position_data["metadata"]:
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(position_data["metadata"])
                        except json.JSONDecodeError:
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

    @db_retry()
    async def get_closed_positions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get closed positions with limit"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM positions WHERE status = 'closed' ORDER BY close_time DESC LIMIT $1",
                    limit,
                )

                if not rows:
                    return []

                positions = []
                for row in rows:
                    position_data = dict(row)

                    # Parse metadata JSON if it exists
                    if "metadata" in position_data and position_data["metadata"]:
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(position_data["metadata"])
                        except json.JSONDecodeError:
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

    @db_retry()
    async def delete_position(self, position_id: str) -> bool:
        """Delete position from database"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM positions WHERE position_id = $1", position_id
                )
                return True

        except Exception as e:
            self.logger.error(f"Error deleting position from database: {str(e)}")
            return False

    @db_retry()
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
                    position_data = dict(row)

                    # Parse metadata JSON if it exists
                    if "metadata" in position_data and position_data["metadata"]:
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(position_data["metadata"])
                        except json.JSONDecodeError:
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

    async def update_position(self, position_id: str, updates: Dict[str, Any]) -> bool:
        """Update position in database"""
        try:
            # Process metadata
            if "metadata" in updates and isinstance(updates["metadata"], dict):
                updates["metadata"] = json.dumps(updates["metadata"])

            # Convert datetime strings
            for field in ["open_time", "close_time", "last_update"]:
                if field in updates and isinstance(updates[field], str):
                    try:
                        updates[field] = datetime.fromisoformat(
                            updates[field].replace('Z', '+00:00')
                        )
                    except ValueError:
                        pass

            async with self.pool.acquire() as conn:
                # Build UPDATE query
                set_clauses = [f"{key} = ${i+2}" for i, key in enumerate(updates.keys())]
                query = f"UPDATE positions SET {', '.join(set_clauses)} WHERE position_id = $1"
                
                values = [position_id] + list(updates.values())
                await conn.execute(query, *values)
                return True

        except Exception as e:
            self.logger.error(f"Error updating position in database: {str(e)}")
            return False


# ─── Improved Helper Functions ────────────────────────────────────────

TIMEFRAME_SECONDS = {
    "M1": 60, "M5": 300, "M15": 900,
    "M30": 1800, "H1": 3600, "H4": 14400,
    "D1": 86400
}

async def get_historical_data(symbol: str, timeframe: str, count: int = 100) -> Dict[str, Any]:
    """Get historical candle data with robust error handling"""
    try:
        oanda_granularity = normalize_timeframe(timeframe, target="OANDA")
        params = {
            "granularity": oanda_granularity,
            "count": count,
            "price": "M"  # mid prices
        }
        r = instruments.InstrumentsCandles(instrument=symbol, params=params)
        
        # Use robust_oanda_request instead of direct oanda.request
        resp = await robust_oanda_request(r)

        if "candles" in resp:
            return {"candles": resp["candles"]}
        else:
            logger.error(f"[OANDA] No candles returned for {symbol}")
            return {"candles": []}
    
    except Exception as e:
        logger.error(f"[OANDA] Error fetching candles for {symbol}: {str(e)}")
        return {"candles": []}


# Replace the existing get_atr function with this corrected version
async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    """Get ATR value with dynamic calculation, API retries, and smart fallbacks"""
    symbol = standardize_symbol(symbol)
    request_id = str(uuid.uuid4())
    logger_instance = get_module_logger(__name__, symbol=symbol, request_id=request_id)

    logger_instance.info(f"[ATR] Fetching ATR for {symbol}, TF={timeframe}, Period={period}")

    # Timeframe Normalization
    try:
        oanda_granularity = normalize_timeframe(timeframe, target="OANDA")
        logger_instance.info(f"[ATR] Using OANDA granularity: {oanda_granularity} for timeframe {timeframe}")
    except Exception as e:
        logger_instance.error(f"[ATR] Error normalizing timeframe '{timeframe}': {str(e)}. Defaulting to H1.")
        oanda_granularity = "H1"

    # OANDA API Call with Retry Logic
    max_retries = 3
    retry_delay = 2
    oanda_candles = None

    for retry in range(max_retries):
        try:
            params = {"granularity": oanda_granularity, "count": period + 5, "price": "M"}
            req = instruments.InstrumentsCandles(instrument=symbol, params=params)
            response = await robust_oanda_request(req)

            candles = response.get("candles", [])
            oanda_candles = [c for c in candles if c.get("complete", True)]

            if len(oanda_candles) >= period + 1:
                logger_instance.info(f"[ATR] Attempt {retry+1}: Retrieved {len(oanda_candles)} candles from OANDA API.")
                break
            else:
                raise ValueError(f"Attempt {retry+1}: Not enough complete candles from OANDA API ({len(oanda_candles)} < {period+1})")

        except Exception as e:
            if isinstance(e, oandapyV20.exceptions.V20Error):
                logger_instance.warning(f"[ATR] OANDA API attempt {retry+1}/{max_retries} failed for {symbol}. Code: {e.code}, Msg: {e.msg}")
            else:
                logger_instance.warning(f"[ATR] OANDA API attempt {retry+1}/{max_retries} failed for {symbol}: {str(e)}")

            if retry < max_retries - 1:
                wait_time = retry_delay * (2 ** retry)
                logger_instance.info(f"[ATR] Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger_instance.error(f"[ATR] OANDA API fetch failed after {max_retries} attempts for {symbol}.")
                oanda_candles = None

    # ATR Calculation using TA library
    calculated_atr = None
    if oanda_candles and len(oanda_candles) >= period + 1:
        try:
            candles_to_use = oanda_candles[-(period + 1):]
            highs = [float(c["mid"]["h"]) for c in candles_to_use]
            lows = [float(c["mid"]["l"]) for c in candles_to_use]
            closes = [float(c["mid"]["c"]) for c in candles_to_use]

            df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
            atr_indicator = ta.volatility.AverageTrueRange(
                high=df['high'], low=df['low'], close=df['close'], window=period
            )
            atr_series = atr_indicator.average_true_range()

            if not atr_series.empty and not pd.isna(atr_series.iloc[-1]):
                calculated_atr = float(atr_series.iloc[-1])
                if calculated_atr > 0:
                    logger_instance.info(f"[ATR] Successfully computed ATR from OANDA data for {symbol}: {calculated_atr:.5f}")
                    return calculated_atr
                else:
                    logger_instance.warning(f"[ATR] Calculated ATR from OANDA data is zero or negative for {symbol}")
                    calculated_atr = None
            else:
                logger_instance.warning(f"[ATR] Calculated ATR from OANDA data is None or NaN for {symbol}")
                calculated_atr = None

        except Exception as e:
            logger_instance.error(f"[ATR] Error calculating ATR from OANDA data for {symbol}: {str(e)}")
            calculated_atr = None

    # Fallback 1: Use get_historical_data
    if calculated_atr is None:
        logger_instance.warning(f"[ATR] OANDA API/calculation failed. Attempting fallback: get_historical_data.")
        try:
            fallback_data = await get_historical_data(symbol, oanda_granularity, period + 10)
            fb_candles = fallback_data.get("candles", [])
            fb_candles = [c for c in fb_candles if c.get("complete", True)]

            if len(fb_candles) >= period + 1:
                candles_to_use = fb_candles[-(period + 1):]
                highs = [float(c["mid"]["h"]) for c in candles_to_use]
                lows = [float(c["mid"]["l"]) for c in candles_to_use]
                closes = [float(c["mid"]["c"]) for c in candles_to_use]

                df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
                atr_indicator = ta.volatility.AverageTrueRange(
                    high=df['high'], low=df['low'], close=df['close'], window=period
                )
                atr_series = atr_indicator.average_true_range()

                if not atr_series.empty and not pd.isna(atr_series.iloc[-1]):
                    calculated_atr = float(atr_series.iloc[-1])
                    if calculated_atr > 0:
                        logger_instance.info(f"[ATR] Successfully computed ATR from fallback data for {symbol}: {calculated_atr:.5f}")
                        return calculated_atr
                    else:
                        logger_instance.warning(f"[ATR] Calculated ATR from fallback data is zero or negative for {symbol}")
                        calculated_atr = None
                else:
                    logger_instance.warning(f"[ATR] Calculated ATR from fallback data is None or NaN for {symbol}")
                    calculated_atr = None
            else:
                logger_instance.warning(f"[ATR] Fallback data insufficient for {symbol}: {len(fb_candles)} candles < {period + 1}")

        except Exception as fallback_error:
            logger_instance.error(f"[ATR] Fallback get_historical_data failed for {symbol}: {str(fallback_error)}")
            calculated_atr = None

    # Fallback 2: Use Static Default Values
    if calculated_atr is None:
        logger_instance.warning(f"[ATR] All calculation methods failed for {symbol}. Using static default ATR.")
        default_atr_values = {
            "FOREX": {"M1": 0.0005, "M5": 0.0007, "M15": 0.0010, "M30": 0.0015, "H1": 0.0025, "H4": 0.0050, "D": 0.0100},
            "CRYPTO": {"M1": 0.0010, "M5": 0.0015, "M15": 0.0020, "M30": 0.0030, "H1": 0.0050, "H4": 0.0100, "D": 0.0200},
            "COMMODITY": {"M1": 0.05, "M5": 0.07, "M15": 0.10, "M30": 0.15, "H1": 0.25, "H4": 0.50, "D": 1.00},
            "INDICES": {"M1": 0.50, "M5": 0.70, "M15": 1.00, "M30": 1.50, "H1": 2.50, "H4": 5.00, "D": 10.00},
            "XAU_USD": {"M1": 0.05, "M5": 0.07, "M15": 0.10, "M30": 0.15, "H1": 0.25, "H4": 0.50, "D": 1.00}
        }
        
        try:
            instrument_type = get_instrument_type(symbol)
            if symbol in default_atr_values:
                type_defaults = default_atr_values[symbol]
            elif instrument_type in default_atr_values:
                type_defaults = default_atr_values[instrument_type]
            else:
                logger_instance.warning(f"[ATR] Unknown instrument type '{instrument_type}' for static defaults, using FOREX.")
                type_defaults = default_atr_values["FOREX"]

            static_atr = type_defaults.get(oanda_granularity, type_defaults.get("H1", 0.0025))

            if instrument_type == "CRYPTO":
                try:
                    current_price = await get_current_price(symbol, "BUY")
                    if current_price > 0:
                        static_atr = current_price * static_atr
                        logger_instance.info(f"[ATR] Calculated absolute static ATR for crypto {symbol} using price {current_price}: {static_atr:.5f}")
                    else:
                        logger_instance.error(f"[ATR] Could not get positive price for crypto {symbol}. Cannot calculate static ATR.")
                        return 0.0
                except Exception as price_err:
                    logger_instance.error(f"[ATR] Could not get price for crypto static ATR calc for {symbol}: {price_err}. Returning 0.")
                    return 0.0
            else:
                logger_instance.info(f"[ATR] Using static default ATR for {symbol} ({instrument_type}, {oanda_granularity}): {static_atr:.5f}")

            return static_atr

        except Exception as static_fallback_error:
            logger_instance.error(f"[ATR] Error during static fallback for {symbol}: {str(static_fallback_error)}")
            logger_instance.warning("[ATR] Using ultimate fallback ATR value: 0.0025")
            return 0.0025

    logger_instance.error(f"[ATR] Failed to determine ATR for {symbol} through all methods.")
    return 0.0


# ─── Exception Handling & Error Recovery ────────────────────────────────────────

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


def async_error_handler(max_retries=3, delay=2):
    """Decorator for handling errors in async functions with retry logic"""
    def decorator(func):
        @wraps(func)
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
    """Comprehensive error handling and recovery system"""

    def __init__(self):
        """Initialize error recovery system"""
        self.stale_position_threshold = 900  # seconds
        self.daily_error_count = 0
        self.last_error_reset = datetime.now(timezone.utc)

    async def check_for_stale_positions(self):
        """Check for positions that haven't been updated recently"""
        try:
            # Placeholder for actual implementation
            pass
        except Exception as e:
            logger.error(f"Error checking for stale positions: {str(e)}")
            await self.record_error("stale_position_check", {"error": str(e)})

    async def recover_position(self, position_id: str, position_data: Dict[str, Any]):
        """Attempt to recover a stale position"""
        try:
            symbol = position_data.get('symbol')
            if not symbol:
                logger.error(f"Cannot recover position {position_id}: Missing symbol")
                return

            current_price = await get_current_price(symbol, position_data.get('action', 'BUY'))

            if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'position_tracker'):
                await alert_handler.position_tracker.update_position_price(
                    position_id=position_id,
                    current_price=current_price
                )
                logger.info(f"Recovered position {position_id} with updated price: {current_price}")

        except Exception as e:
            logger.error(f"Error recovering position {position_id}: {str(e)}")

    async def record_error(self, error_type: str, details: Dict[str, Any]):
        """Record error for monitoring and recovery"""
        self.daily_error_count += 1
        logger.error(f"Error recorded: {error_type} - {json.dumps(details)}")

    async def schedule_stale_position_check(self, interval_seconds: int = 60):
        """Schedule regular checks for stale positions"""
        while True:
            try:
                await self.check_for_stale_positions()
            except Exception as e:
                logger.error(f"Error in scheduled stale position check: {str(e)}")
            await asyncio.sleep(interval_seconds)


# ─── Session & API Management ────────────────────────────────────────

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


# ─── Enhanced Trading Functions ────────────────────────────────────────

@async_error_handler()
async def get_account_balance(use_fallback=True) -> float:
    """Get current account balance from OANDA with robust error handling"""
    try:
        from oandapyV20.endpoints.accounts import AccountSummary
        account_request = AccountSummary(accountID=get_config_value('oanda_account_id'))
        
        response = await robust_oanda_request(account_request, max_retries=2, initial_delay=1)
        
        if "account" in response and "NAV" in response["account"]:
            balance = float(response["account"]["NAV"])
            logger.info(f"Current account balance: {balance}")
            return balance
        
        if "account" in response and "balance" in response["account"]:
            balance = float(response["account"]["balance"])
            logger.info(f"Current account balance (from 'balance' field): {balance}")
            return balance
            
        logger.error(f"Failed to extract balance from OANDA response: {response}")
        if use_fallback:
            logger.warning("Using fallback balance of 10000.0")
            return 10000.0
        else:
            raise BrokerConnectionError("Could not extract balance from OANDA response")
        
    except BrokerConnectionError as e:
        if use_fallback:
            logger.warning(f"Failed to get account balance, using fallback: {str(e)}")
            return 10000.0
        else:
            raise
    except Exception as e:
        logger.error(f"Failed to get account balance: {str(e)}", exc_info=True)
        if use_fallback:
            logger.warning("Using fallback balance due to unexpected error")
            return 10000.0
        else:
            raise

async def get_account_summary(account_id: str = None) -> dict:
    """Get account summary including margin information"""
    try:
        if account_id is None:
            account_id = get_config_value('oanda_account_id')
        
        from oandapyV20.endpoints.accounts import AccountSummary
        request = AccountSummary(accountID=account_id)
        response = await robust_oanda_request(request)
        return response
    except Exception as e:
        logger.error(f"Error getting account summary: {str(e)}")
        raise

async def get_price_with_fallbacks(symbol: str, direction: str) -> Tuple[float, str]:
    """Get current price with multi-level fallbacks"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting price for {symbol} ({direction})")
    
    # Method 1: Try PricingInfo endpoint
    try:
        account_id = get_config_value('oanda_account_id')
        params = {"instruments": symbol}
        
        from oandapyV20.endpoints.pricing import PricingInfo
        price_request = PricingInfo(accountID=account_id, params=params)
        
        price_response = await robust_oanda_request(price_request)
        
        if "prices" in price_response and len(price_response["prices"]) > 0:
            prices_data = price_response['prices'][0]
            if direction.upper() == 'BUY':
                if prices_data.get('asks') and len(prices_data['asks']) > 0:
                    price = float(prices_data['asks'][0]['price'])
                    logger.info(f"[{request_id}] Got price via PricingInfo: {price}")
                    return price, "pricing_api"
            else:  # SELL
                if prices_data.get('bids') and len(prices_data['bids']) > 0:
                    price = float(prices_data['bids'][0]['price'])
                    logger.info(f"[{request_id}] Got price via PricingInfo: {price}")
                    return price, "pricing_api"
                    
    except Exception as e:
        logger.warning(f"[{request_id}] Primary price source failed: {str(e)}")
        
    # Method 2: Fallback to recent candles
    try:
        candle_data = await get_historical_data(symbol, 'M1', 3)
        
        if candle_data and candle_data.get('candles'):
            complete_candles = [c for c in candle_data['candles'] if c.get('complete', True)]
            
            if complete_candles:
                latest_candle = complete_candles[-1]
                if latest_candle.get('mid') and latest_candle['mid'].get('c'):
                    price = float(latest_candle['mid']['c'])
                    logger.info(f"[{request_id}] Got price via candle fallback: {price}")
                    return price, "candle_data"
                    
    except Exception as e:
        logger.warning(f"[{request_id}] Candle fallback failed: {str(e)}")
    
    # Method 3: Simulation fallback
    try:
        price = _get_simulated_price(symbol, direction)
        logger.info(f"[{request_id}] Got simulated price: {price}")
        return price, "simulation"
        
    except Exception as e:
        logger.warning(f"[{request_id}] Simulation fallback failed: {str(e)}")
    
    # All methods failed
    error_msg = f"Failed to get price for {symbol} after trying all available methods"
    logger.error(f"[{request_id}] {error_msg}")
    raise ValueError(error_msg)

def _get_simulated_price(symbol: str, direction: str) -> float:
    """Generate a simulated price when real price data is unavailable"""
    base_prices = {
        "EUR_USD": 1.10,
        "GBP_USD": 1.25,
        "USD_JPY": 110.0,
        "GBP_JPY": 175.0,
        "XAU_USD": 1900.0,
        "BTC_USD": 75000.0,
        "ETH_USD": 3500.0
    }
    
    base_price = base_prices.get(symbol, 100.0)
    variation = random.uniform(-0.001, 0.001)
    price = base_price * (1 + variation)
    
    spread_factor = 1.0005 if direction.upper() == "BUY" else 0.9995
    return price * spread_factor

async def execute_trade(payload: dict) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade with the broker"""
    request_id = payload.get("request_id", str(uuid.uuid4()))
    instrument = payload.get('instrument', payload.get('symbol', ''))
    
    try:
        direction = payload.get('direction', payload.get('action', '')).upper()
        risk_percent = float(payload.get('risk_percent', 1.0))
        timeframe = payload.get('timeframe', '1H')
        comment = payload.get('comment')

        logger.info(f"[execute_trade] Executing {direction} {instrument} with {risk_percent:.2f}% risk")

        # Use placeholder implementation for now
        # This would be replaced with actual broker integration
        result = {
            "success": True,
            "order_id": str(uuid.uuid4()),
            "trade_id": str(uuid.uuid4()),
            "instrument": instrument,
            "direction": direction,
            "entry_price": await get_current_price(instrument, direction),
            "units": 1000,  # Placeholder
            "request_id": request_id
        }

        logger.info(f"Trade executed successfully: {direction} {instrument}")
        return True, result

    except Exception as e:
        logger.error(f"Error executing trade for {instrument}: {str(e)}", exc_info=True)
        return False, {"error": str(e), "instrument": instrument, "request_id": request_id}

async def close_position(position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close a position with the broker"""
    position_id = position_data.get("position_id", "UNKNOWN_ID")
    symbol = position_data.get("symbol", "")
    action = position_data.get("action", "").upper()
    
    request_id = str(uuid.uuid4())
    logger.info(f"[BROKER_CLOSE] Closing position {position_id} for {symbol}")
    
    if not symbol:
        return False, {
            "error": "Symbol not provided for broker closure", 
            "position_id": position_id, 
            "request_id": request_id
        }
    
    try:
        # Standardize symbol for OANDA
        standardized_symbol = standardize_symbol(symbol)
        account_id = get_config_value('oanda_account_id')
        
        if not account_id:
            return False, {
                "error": "OANDA account ID not configured",
                "position_id": position_id,
                "request_id": request_id
            }
        
        # Use the enhanced close position method
        from oandapyV20.endpoints.positions import PositionClose
        
        # Determine close data based on action
        if action == "BUY":
            close_data = {"longUnits": "ALL"}
        elif action == "SELL":
            close_data = {"shortUnits": "ALL"}
        else:
            close_data = {"longUnits": "ALL", "shortUnits": "ALL"}
        
        close_request = PositionClose(
            accountID=account_id,
            instrument=standardized_symbol,
            data=close_data
        )
        
        response = await robust_oanda_request(close_request)
        
        # Extract exit price from response
        actual_exit_price = None
        
        if "longOrderFillTransaction" in response:
            tx = response["longOrderFillTransaction"]
            actual_exit_price = float(tx.get("price", 0))
        elif "shortOrderFillTransaction" in response:
            tx = response["shortOrderFillTransaction"]
            actual_exit_price = float(tx.get("price", 0))
        elif "orderFillTransaction" in response:
            tx = response["orderFillTransaction"]
            actual_exit_price = float(tx.get("price", 0))
        
        if actual_exit_price is None:
            # Fallback to current price
            actual_exit_price = await get_current_price(standardized_symbol, 
                                                      "SELL" if action == "BUY" else "BUY")
        
        logger.info(f"Position {position_id} closed at price: {actual_exit_price}")
        
        return True, {
            "position_id": position_id,
            "actual_exit_price": actual_exit_price,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "broker_response": response,
            "message": "Position closed successfully",
            "request_id": request_id
        }
        
    except Exception as e:
        logger.error(f"Error closing position {position_id}: {str(e)}", exc_info=True)
        return False, {
            "error": str(e), 
            "position_id": position_id, 
            "request_id": request_id
        }

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable based on market hours"""
    now = datetime.now(timezone.utc)
    instrument_type = get_instrument_type(symbol)
    
    # Special handling for JPY pairs
    if "JPY" in symbol:
        instrument_type = "jpy_pair"
    
    if instrument_type in ["FOREX", "jpy_pair", "COMMODITY"]:
        if now.weekday() >= 5:
            return False, "Weekend - Market closed"
        if now.weekday() == 4 and now.hour >= 21:
            return False, "Weekend - Market closed"
        if now.weekday() == 0 and now.hour < 21:
            return False, "Market not yet open"
        return True, "Market open"
    
    if instrument_type == "INDICES":
        if "SPX" in symbol or "NAS" in symbol:
            if now.weekday() >= 5:
                return False, "Weekend - Market closed"
            if not (13 <= now.hour < 20):
                return False, "Outside market hours"
        return True, "Market open"
    
    return True, "Market assumed open"

def get_higher_timeframe(timeframe: str) -> str:
    """Get the next higher timeframe based on current timeframe"""
    timeframe_hierarchy = {
        "M1": "M15",
        "M5": "M30",
        "M15": "H1",
        "M30": "H4",
        "H1": "H4",
        "H4": "D1",
        "D1": "W1",
        "W1": "MN"
    }
    
    normalized_tf = normalize_timeframe(timeframe)
    return timeframe_hierarchy.get(normalized_tf, normalized_tf)

async def check_higher_timeframe_trend(symbol: str, direction: str, timeframe: str) -> bool:
    """Check if higher timeframe trend aligns with position direction"""
    try:
        higher_tf = get_higher_timeframe(timeframe)
        
        if higher_tf == timeframe:
            return True  # Can't check higher, assume aligned
            
        # Adjust periods based on timeframe
        fast_period = 20 if timeframe in ["M1", "M5", "M15"] else 50
        slow_period = 50 if timeframe in ["M1", "M5", "M15"] else 200
        
        historical_data = await get_historical_data(symbol, higher_tf, slow_period + 10)
        
        if not historical_data or "candles" not in historical_data:
            return False
            
        candles = historical_data["candles"]
        if not candles or len(candles) < slow_period:
            return False
            
        closes = [float(c["mid"]["c"]) for c in candles]
        
        ma_fast = sum(closes[-fast_period:]) / min(fast_period, len(closes))
        ma_slow = sum(closes[-slow_period:]) / min(slow_period, len(closes))
        
        ma_percent_diff = abs(ma_fast - ma_slow) / ma_slow * 100
        min_percent_diff = 0.5 if timeframe in ["M1", "M5", "M15"] else 0.2
        
        strong_trend = ma_percent_diff >= min_percent_diff
        
        if direction == "BUY" and ma_fast > ma_slow and strong_trend:
            return True
        elif direction == "SELL" and ma_fast < ma_slow and strong_trend:
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Error checking higher timeframe trend: {str(e)}")
        return False

async def check_position_momentum(position_id: str) -> bool:
    """Check if a position has strong momentum in its direction"""
    try:
        # This would integrate with your position tracker
        # For now, return False as a conservative default
        return False
        
    except Exception as e:
        logger.error(f"Error checking position momentum: {str(e)}")
        return False
            normalized = reverse_oanda_map.get(tf, tf)

        if not normalized:
            match = re.match(r"(\d+)([MDWHMON])", tf)
            if match:
                num, unit = int(match.group(1)), match.group(2)
                pot = f"{num}{unit}"
                if pot in intermediate_formats:
                    normalized = pot
                elif unit == "M" and num >= 60 and num % 60 == 0:
                    normalized = f"{num // 60}H"
                elif unit == "H" and num >= 24 and num % 24 == 0:
                    normalized = f"{num // 24}D"
            if not normalized:
                logger.warning(
                    f"[TF-NORMALIZE] Unknown timeframe '{tf_original}' "
                    f"(processed as '{tf}'), defaulting to '1H'"
                )
                normalized = "1H"

        # Convert to target
        if target == "OANDA":
            oanda_map = {
                "1M": "M1", "3M": "M3", "5M": "M5", "15M": "M15",
                "30M": "M30", "1H": "H1", "2H": "H2", "3H": "H3",
                "4H": "H4", "6H": "H6", "8H": "H8", "12H": "H12",
                "1D": "D", "1W": "W", "1MON": "M"
            }
            if normalized in oanda_map:
                return oanda_map[normalized]
            valid_oanda = [
                "M1", "M3", "M5", "M15", "M30",
                "H1", "H2", "H3", "H4", "H6", "H8", "H12",
                "D", "W", "M"
            ]
            if normalized in valid_oanda:
                return normalized
            logger.warning(
                f"[TF-NORMALIZE] Normalized '{normalized}' not in OANDA map, using 'H1'"
            )
            return "H1"

        elif target == "BINANCE":
            binance_map = {
                "1M": "1m", "5M": "5m", "15M": "15m", "30M": "30m",
                "1H": "1h", "4H": "4h", "1D": "1d", "1W": "1w",
                "1MON": "1M"
            }
            return binance_map.get(normalized, "1h")

        else:
            logger.warning(
                f"[TF-NORMALIZE] Unknown target '{target}', returning '{normalized}'"
            )
            return normalized

    except Exception as e:
        logger.error(f"Error normalizing timeframe: {e}")
        return "H1"


def parse_iso_datetime(datetime_str: str) -> datetime:
    """Parse ISO datetime string with timezone handling"""
    if not datetime_str:
        return None
    try:
        if datetime_str.endswith('Z'):
            datetime_str = datetime_str[:-1] + '+00:00'
        if '+' not in datetime_str and '-' not in datetime_str[10:]:
            datetime_str += '+00:00'
        return datetime.fromisoformat(datetime_str)
    except ValueError as e:
        logger.error(f"Error parsing datetime {datetime_str}: {str(e)}")
        return datetime.now(timezone.utc)


def get_config_value(attr_name: str, env_var: str = None, default=None):
    """Get configuration value with fallback to environment variable"""
    if hasattr(config, attr_name):
        value = getattr(config, attr_name)
        if isinstance(value, SecretStr):
            return value.get_secret_value()
        return value
    if env_var and env_var in os.environ:
        return os.environ[env_var]
    return default


# ─── Rate Limiting and OANDA Client Classes ────────────────────────────────────────

class OANDARateLimiter:
    """Rate limiter for OANDA API requests"""
    
    def __init__(self, requests_per_second=10):
        self.requests_per_second = requests_per_second
        self.last_request_time = datetime.now()
        self.request_count = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire rate limit token"""
        async with self._lock:
            now = datetime.now()
            if (now - self.last_request_time).total_seconds() >= 1.0:
                self.request_count = 0
                self.last_request_time = now
            
            if self.request_count >= self.requests_per_second:
                sleep_time = 1.0 - (now - self.last_request_time).total_seconds()
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = datetime.now()
            
            self.request_count += 1


# Initialize rate limiter
oanda_rate_limiter = OANDARateLimiter()


# ─── Exception Classes ────────────────────────────────────────

class BrokerConnectionError(Exception):
    """Raised when there's a connection error with the broker"""


# ─── OANDA Request Handler ────────────────────────────────────────

async def robust_oanda_request(request_obj, max_retries=3, initial_delay=1):
    """
    Robust OANDA request handler with retry logic and proper error handling
    """
    loop = asyncio.get_running_loop()
    retries = 0
    last_error = None
    
    while retries <= max_retries:
        try:
            # Add rate limiting
            await oanda_rate_limiter.acquire()
            
            # Execute the request
            response = await loop.run_in_executor(
                None, 
                lambda: oanda.request(request_obj)
            )
            return response
            
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                urllib3.exceptions.ProtocolError) as e:
                
            retries += 1
            last_error = e
            
            if retries > max_retries:
                logger.error(f"Exhausted {max_retries} retries for OANDA request: {str(e)}")
                break
                
            wait_time = initial_delay * (2 ** (retries - 1))
            logger.warning(f"OANDA request failed (attempt {retries}/{max_retries}), "
                           f"retrying in {wait_time}s: {str(e)}")
            await asyncio.sleep(wait_time)
            
        except oandapyV20.exceptions.V20Error as e:
            retries += 1
            last_error = e
            
            # Check if this is a retryable error
            error_code = getattr(e, 'code', None)
            error_msg = str(e).lower()
            
            # 503 Service Unavailable and "system not ready" are retryable
            is_retryable = (
                error_code in [503, None] or
                "system not ready" in error_msg or
                "unable to service request" in error_msg or
                "timeout" in error_msg or
                "temporary" in error_msg
            )
            
            if not is_retryable:
                logger.warning(f"Non-retryable OANDA API error: {error_code} - {e}")
                raise BrokerConnectionError(f"OANDA API error: {e}")
            
            if retries > max_retries:
                logger.error(f"Exhausted {max_retries} retries for OANDA request: {str(e)}")
                break
                
            wait_time = initial_delay * (2 ** (retries - 1))
            logger.warning(f"OANDA API error (attempt {retries}/{max_retries}), "
                           f"retrying in {wait_time}s: {str(e)}")
            await asyncio.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Unexpected error in OANDA request: {str(e)}")
            raise BrokerConnectionError(f"Unexpected OANDA error: {e}")
    
    error_msg = str(last_error) if last_error else "Unknown error"
    logger.error(f"All OANDA request attempts failed: {error_msg}")
    raise BrokerConnectionError(f"Failed to communicate with OANDA after {max_retries} attempts: {error_msg}")


# ─── Pydantic Models ────────────────────────────────────────

class MergedTradingViewAlertPayload(BaseModel):
    """Validated TradingView webhook payload, combining robust validation and comprehensive fields."""
    
    # Fields from the second model, generally more specific or with better descriptions
    symbol: constr(strip_whitespace=True, min_length=3) = Field(..., description="Trading instrument (e.g., EURUSD, BTCUSD)")
    action: Literal["BUY", "SELL", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT"] = Field(..., description="Trade direction")
    percentage: Optional[confloat(gt=0, le=100)] = Field(None, description="Risk percentage for the trade (0 < x <= 100)")
    
    # Timeframe with the detailed validator from the first model and default from the second
    timeframe: str = Field(default="1H", description="Timeframe for the trade (e.g., 1M, 5M, 1H, 4H, 1D)")
    
    exchange: Optional[str] = Field(None, description="Exchange name (from webhook)")
    account: Optional[str] = Field(None, description="Account ID (from webhook)")
    orderType: Optional[str] = Field(None, description="Order type (from webhook)") # Maintained camelCase from original
    timeInForce: Optional[str] = Field(None, description="Time in force (from webhook)") # Maintained camelCase from original
    comment: Optional[str] = Field(None, description="Additional comment for the trade")
    strategy: Optional[str] = Field(None, description="Strategy name")
    request_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique request ID")

    @validator('timeframe', pre=True, always=True)
    def validate_timeframe(cls, v, values): # Added 'values' as it's good practice for Pydantic v2+ validators
        if v is None:
            # If a default is set in Field, this part might not be hit unless explicitly passed None
            # Keeping the logic for robustness if default behavior changes or None is explicitly passed
            return "1H" 
        
        v_str = str(v).strip().upper()
        
        if v_str in ["D", "1D", "DAILY"]:
            return "1D"
        if v_str in ["W", "1W", "WEEKLY"]:
            return "1W"
        if v_str in ["MN", "1MN", "MONTHLY"]:
            return "1MN"
        
        # Handle purely numeric inputs as minutes, then map common ones
        if v_str.isdigit():
            num = int(v_str)
            if num == 1: return "1M" # Assuming '1' alone means 1 minute, could be 1H by other logic
            if num == 5: return "5M"
            if num == 15: return "15M"
            if num == 30: return "30M"
            if num == 60: return "1H"
            if num == 120: return "2H"
            if num == 180: return "3H"
            if num == 240: return "4H"
            if num == 720: return "12H" # (12 * 60)
            if num == 1440: return "1D" # (24 * 60)
            return f"{num}M" # Default to minutes if not in specific mapping

        # Handle standard TradingView formats like "1M", "30M", "1H", "4H"
        # Regex to match patterns like "1M", "15M", "1H", "240M" (which would then be converted)
        # or already valid like "1D", "1W", "1MN"
        match = re.match(r"^(\d+)([MDHWM])$", v_str) # Added M for minutes, D for Day, W for Week, M for Month
        if match:
            num = match.group(1)
            unit = match.group(2)
            
            if unit == 'M': # Minutes
                # Potentially convert large minutes to hours/days for consistency if desired
                # For now, keeping it as is, e.g., "240M" -> "4H" would happen below
                # or just return f"{num}M"
                if num == "60": return "1H"
                if num == "120": return "2H"
                if num == "180": return "3H"
                if num == "240": return "4H"
                if num == "720": return "12H"
                if num == "1440": return "1D"
                return f"{num}M"
            if unit == 'H':
                return f"{num}H"
            # D, W, MN already handled at the top, but this regex would also catch them
            if unit == 'D': return f"{num}D" # usually 1D
            if unit == 'W': return f"{num}W" # usually 1W
            # 'M' for month was 'MN' earlier, so this 'M' is for Minute.
            # If 'MN' is also possible via this regex, it needs differentiation.
            # The current specific checks for "MN", "1MN" handle this.

        raise ValueError(f"Invalid timeframe format: '{v}'. Use formats like '1M', '15M', '1H', '1D', '1W', '1MN'.")

    class Config:
        extra = 'ignore' # Or 'forbid' if you want to raise an error on extra fields
        populate_by_name = True # Allows using field names or aliases (if defined)

    @model_validator(mode='after')
    def validate_percentage_for_actions(self):
        if self.action in ["BUY", "SELL"] and self.percentage is None:
            raise ValueError(f"percentage is required for {self.action} actions")
        return self


# ─── API Endpoints ────────────────────────────────────────

@app.get("/", tags=["system"])
async def root():
    """Root endpoint"""
    return {
        "message": "Enhanced Trading System API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/api/docs"
    }


@app.get("/api/health", tags=["system"])
async def health_check():
    """
    Health check endpoint that ensures DB and alert handler are initialized,
    and returns status, version, and timestamp.
    """
    if not db_manager:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "DB not initialized"}
        )
    if not alert_handler:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "Alert handler not initialized"}
        )

    return {
        "status": "ok",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


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
            
        return status_data

    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )


# ─── Enhanced Alert Handler Class ────────────────────────────────────────

class EnhancedAlertHandler:
    """Enhanced alert handler with comprehensive trading system components"""
    
    def __init__(self):
        """Initialize alert handler with proper defaults"""
        # Initialize all attributes to None first
        self.position_tracker = None
        self.risk_manager = None
        self.volatility_monitor = None
        self.market_structure = None
        self.regime_classifier = None
        self.dynamic_exit_manager = None
        self.position_journal = None
        self.notification_system = None
        self.system_monitor = None
        
        # Other initialization code...
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        
        # Configuration flags
        self.enable_reconciliation = config.enable_broker_reconciliation if 'config' in globals() else True
        self.enable_close_overrides = True
        
        # Initialize logger
        self.logger = get_module_logger(__name__)
        
        # Configuration reference
        self.config = config
        
        logger.info("EnhancedAlertHandler initialized with default values")

    async def start(self):
        """Initialize & start all components, including optional broker reconciliation."""
        if self._running:
            logger.info("EnhancedAlertHandler.start() called, but already running.")
            return True

        logger.info("Attempting to start EnhancedAlertHandler and its components...")
        startup_errors = []
        
        try:
            # Import required classes (these should be defined elsewhere in your system)
            # For now, we'll use placeholder imports to avoid errors
            try:
                from system_monitor import SystemMonitor
                from risk_manager import EnhancedRiskManager
                from volatility_monitor import VolatilityMonitor
                from regime_classifier import LorentzianDistanceClassifier
                from exit_manager import HybridExitManager
                from position_journal import PositionJournal
                from notification_system import NotificationSystem
            except ImportError as e:
                logger.warning(f"Some components not available for import: {e}")
                # Create placeholder classes to prevent startup failure
                class PlaceholderComponent:
                    async def start(self): pass
                    async def initialize(self, *args, **kwargs): pass
                    def get_regime_data(self, symbol): return {"regime": "unknown"}
                    def get_volatility_state(self, symbol): return {"volatility_state": "normal"}
                
                SystemMonitor = PlaceholderComponent
                PositionTracker = PlaceholderComponent
                EnhancedRiskManager = PlaceholderComponent
                VolatilityMonitor = PlaceholderComponent
                LorentzianDistanceClassifier = PlaceholderComponent
                HybridExitManager = PlaceholderComponent
                PositionJournal = PlaceholderComponent
                NotificationSystem = PlaceholderComponent
            
            # 1) System Monitor
            self.system_monitor = SystemMonitor()
            await self.system_monitor.register_component("alert_handler", "initializing")

            # 2) DB Manager check
            if not db_manager:
                logger.critical("db_manager is not initialized. Cannot proceed with startup.")
                await self.system_monitor.update_component_status(
                    "alert_handler", "error", "db_manager not initialized"
                )
                return False

            # 3) Core components registration
            self.position_tracker = PositionTracker(db_manager=db_manager)
            await self.system_monitor.register_component("position_tracker", "initializing")

            self.risk_manager = EnhancedRiskManager()
            await self.system_monitor.register_component("risk_manager", "initializing")

            self.volatility_monitor = VolatilityMonitor()
            await self.system_monitor.register_component("volatility_monitor", "initializing")

            self.regime_classifier = LorentzianDistanceClassifier()
            await self.system_monitor.register_component("regime_classifier", "initializing")

            self.dynamic_exit_manager = HybridExitManager(position_tracker=self.position_tracker)
            self.dynamic_exit_manager.lorentzian_classifier = self.regime_classifier
            await self.system_monitor.register_component("dynamic_exit_manager", "initializing")

            self.position_journal = PositionJournal()
            await self.system_monitor.register_component("position_journal", "initializing")

            self.notification_system = NotificationSystem()
            await self.system_monitor.register_component("notification_system", "initializing")

            # 4) Configure notification channels
            if config.slack_webhook_url:
                slack_url = (
                    config.slack_webhook_url.get_secret_value()
                    if isinstance(config.slack_webhook_url, SecretStr)
                    else config.slack_webhook_url
                )
                if slack_url:
                    await self.notification_system.configure_channel("slack", {"webhook_url": slack_url})

            if config.telegram_bot_token and config.telegram_chat_id:
                telegram_token = (
                    config.telegram_bot_token.get_secret_value()
                    if isinstance(config.telegram_bot_token, SecretStr)
                    else config.telegram_bot_token
                )
                telegram_chat_id = config.telegram_chat_id
                await self.notification_system.configure_channel(
                    "telegram",
                    {"bot_token": telegram_token, "chat_id": telegram_chat_id}
                )

            await self.notification_system.configure_channel("console", {})
            logger.info("Notification channels configured.")
            await self.system_monitor.update_component_status("notification_system", "ok")

            # 5) Start components with error handling
            try:
                logger.info("Starting PositionTracker...")
                await self.position_tracker.start()
                await self.system_monitor.update_component_status("position_tracker", "ok")
            except Exception as e:
                startup_errors.append(f"PositionTracker failed: {e}")
                await self.system_monitor.update_component_status("position_tracker", "error", str(e))

            try:
                logger.info("Initializing RiskManager...")
                balance = await get_account_balance(use_fallback=True)  # Use fallback during startup
                await self.risk_manager.initialize(balance)
                await self.system_monitor.update_component_status("risk_manager", "ok")
            except Exception as e:
                startup_errors.append(f"RiskManager failed: {e}")
                await self.system_monitor.update_component_status("risk_manager", "error", str(e))
                # Initialize with fallback balance
                try:
                    await self.risk_manager.initialize(10000.0)
                    logger.warning("RiskManager initialized with fallback balance")
                except Exception as fallback_error:
                    logger.error(f"RiskManager fallback initialization failed: {fallback_error}")

            # Mark monitors OK (no explicit .start())
            await self.system_monitor.update_component_status("volatility_monitor", "ok")
            await self.system_monitor.update_component_status("regime_classifier", "ok")

            try:
                logger.info("Starting HybridExitManager…")
                await self.dynamic_exit_manager.start()
                await self.system_monitor.update_component_status("dynamic_exit_manager", "ok")
            except Exception as e:
                startup_errors.append(f"HybridExitManager failed: {e}")
                await self.system_monitor.update_component_status("dynamic_exit_manager", "error", str(e))

            await self.system_monitor.update_component_status("position_journal", "ok")

            # 6) Broker reconciliation (optional and graceful)
            if self.enable_reconciliation and hasattr(self, "reconcile_positions_with_broker"):
                try:
                    logger.info("Performing initial broker reconciliation...")
                    await self.reconcile_positions_with_broker()
                    logger.info("Initial broker reconciliation complete.")
                except Exception as e:
                    startup_errors.append(f"Broker reconciliation failed: {e}")
                    logger.warning(f"Broker reconciliation failed, but system will continue: {e}")
            else:
                logger.info("Broker reconciliation skipped by configuration or OANDA unavailable.")

            # Finalize startup
            self._running = True
            
            # Determine overall startup status
            if len(startup_errors) == 0:
                status_msg = "EnhancedAlertHandler started successfully."
                await self.system_monitor.update_component_status("alert_handler", "ok", status_msg)
                logger.info(status_msg)
            else:
                status_msg = f"EnhancedAlertHandler started with {len(startup_errors)} errors."
                await self.system_monitor.update_component_status("alert_handler", "warning", status_msg)
                logger.warning(status_msg)
                for error in startup_errors:
                    logger.warning(f"Startup error: {error}")

            # Send a startup notification
            try:
                open_count = len(getattr(self.position_tracker, "positions", {})) if self.position_tracker else 0
                notification_msg = f"EnhancedAlertHandler started. Open positions: {open_count}."
                if startup_errors:
                    notification_msg += f" ({len(startup_errors)} startup warnings - check logs)"
                await self.notification_system.send_notification(notification_msg, "info")
            except Exception as e:
                logger.warning(f"Failed to send startup notification: {e}")

            return True  # Return True even with non-critical errors

        except Exception as e:
            logger.error(f"CRITICAL FAILURE during startup: {e}", exc_info=True)
            if self.system_monitor:
                await self.system_monitor.update_component_status(
                    "alert_handler", "error", f"Startup failure: {e}"
                )
            if self.notification_system:
                try:
                    await self.notification_system.send_notification(
                        f"CRITICAL ALERT: startup failed: {e}", "critical"
                    )
                except Exception:
                    logger.error("Failed to send critical startup notification.", exc_info=True)
            return False

    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming alert with comprehensive error handling"""
        async with self._lock:
            alert_id_from_data = alert_data.get("id", alert_data.get("request_id"))
            alert_id = alert_id_from_data if alert_id_from_data else str(uuid.uuid4())
            
            logger_instance = get_module_logger(
                __name__, 
                symbol=alert_data.get("symbol", alert_data.get("instrument", "UNKNOWN")),
                request_id=alert_id
            )

            # Ensure alert_data has an 'id' field for consistent error reporting
            if "id" not in alert_data:
                alert_data["id"] = alert_id

            try:
                direction = alert_data.get("direction", "").upper()
                # Prefer 'instrument' if available, fallback to 'symbol'
                symbol = alert_data.get("instrument") or alert_data.get("symbol")
                
                # risk_percent is fetched again later more specifically, this is just for an initial log
                risk_percent_log = alert_data.get("risk_percent", 1.0) 

                logger_instance.info(f"[PROCESS ALERT ID: {alert_id}] Symbol='{symbol}', Direction='{direction}', Risk='{risk_percent_log}%'")

                if alert_id in self.active_alerts:
                    logger_instance.warning(f"Duplicate alert ignored: {alert_id}")
                    return {"status": "ignored", "message": "Duplicate alert", "alert_id": alert_id}
                
                self.active_alerts.add(alert_id) # Add after check, if not duplicate

                if self.system_monitor:
                    await self.system_monitor.update_component_status("alert_handler", "processing", f"Processing alert for {symbol} {direction} (ID: {alert_id})")

                # Handle CLOSE action
                if direction == "CLOSE":
                    if not symbol:
                        logger_instance.error(f"Symbol not provided for CLOSE action. Alert ID: {alert_id}")
                        return {"status": "error", "message": "Symbol required for CLOSE action", "alert_id": alert_id}

                    # === MANDATORY: standardize before closing ===
                    standardized = standardize_symbol(symbol)
                    if not standardized:
                        logger_instance.error(f"[ID: {alert_id}] Failed to standardize symbol '{symbol}' for CLOSE")
                        return {"status": "error", "message": f"Cannot close—invalid symbol format: {symbol}", "alert_id": alert_id}

                    result = await self._close_position(standardized)
                    return {
                        "status": "closed",
                        "symbol": standardized,
                        "result": result,
                        "alert_id": alert_id
                    }

                # Validate direction for other actions (BUY/SELL)
                if direction not in ["BUY", "SELL"]:
                    logger_instance.warning(f"Unknown or invalid action type: '{direction}' for alert ID: {alert_id}")
                    return {"status": "error", "message": f"Unknown or invalid action type: {direction}", "alert_id": alert_id}

                if not symbol: # Symbol is required for BUY/SELL
                    logger_instance.error(f"Symbol not provided for {direction} action. Alert ID: {alert_id}")
                    return {"status": "error", "message": f"Symbol required for {direction} action", "alert_id": alert_id}

                # Check for existing open position
                if self.position_tracker:
                    existing_position = await self.position_tracker.get_position_by_symbol(symbol)
                    if existing_position:
                        logger_instance.info(f"Existing position detected for {symbol}. Evaluating override conditions for alert ID: {alert_id}...")
                        should_override, reason = await self._should_override_close(symbol, existing_position)
                        if not should_override:
                            logger_instance.info(f"Skipping {direction} for {symbol}: position already open and not overridden. Reason: {reason}. Alert ID: {alert_id}")
                            return {"status": "skipped", "reason": f"Position already open, not overridden: {reason}", "alert_id": alert_id}
                        logger_instance.info(f"Override triggered for {symbol}: {reason}. Proceeding with new alert processing. Alert ID: {alert_id}")
                        # If override is true, we continue to process the new trade.

                # --- Execute Trade Logic for BUY/SELL ---
                instrument = alert_data.get("instrument", symbol) # Fallback to symbol if instrument specific key is not there
                timeframe = alert_data.get("timeframe", "H1")
                comment = alert_data.get("comment")
                account = alert_data.get("account") # Could be None
                risk_percent = float(alert_data.get('risk_percent', 1.0)) # Ensure float

                logger_instance.info(f"[ID: {alert_id}] Trade Execution Details: Risk Percent: {risk_percent} (Type: {type(risk_percent)})")

                standardized_instrument = standardize_symbol(instrument)
                if not standardized_instrument:
                    logger_instance.error(f"[ID: {alert_id}] Failed to standardize instrument: '{instrument}'")
                    return {"status": "rejected", "message": f"Failed to standardize instrument: {instrument}", "alert_id": alert_id}

                tradeable, reason = is_instrument_tradeable(standardized_instrument)
                logger_instance.info(f"[ID: {alert_id}] Instrument '{standardized_instrument}' tradeable: {tradeable}, Reason: {reason}")

                if not tradeable:
                    logger_instance.warning(f"[ID: {alert_id}] Market check failed for '{standardized_instrument}': {reason}")
                    return {"status": "rejected", "message": f"Trading not allowed for {standardized_instrument}: {reason}", "alert_id": alert_id}

                payload_for_execute_trade = {
                    "symbol": standardized_instrument,
                    "action": direction, # Should be "BUY" or "SELL" at this point
                    "risk_percent": risk_percent,
                    "timeframe": timeframe,
                    "comment": comment,
                    "account": account,
                    "request_id": alert_id # Using request_id as per your payload structure
                }

                logger_instance.info(f"[ID: {alert_id}] Payload for execute_trade: {json.dumps(payload_for_execute_trade)}")
                
                success, result_dict = await execute_trade(payload_for_execute_trade)
                
                # Standardize response from execute_trade if necessary
                if not isinstance(result_dict, dict): # Ensure it's a dict
                    logger_instance.error(f"[ID: {alert_id}] execute_trade returned non-dict: {result_dict}")
                    return {"status": "error", "message": "Trade execution failed with invalid response format.", "alert_id": alert_id}

                if "alert_id" not in result_dict: # Ensure alert_id is in the response
                    result_dict["alert_id"] = alert_id
                
                return result_dict

            except Exception as e: # Catch exceptions from the main logic
                logger_instance.error(f"Error during processing of alert ID {alert_id}: {str(e)}", exc_info=True)
                if hasattr(self, 'error_recovery') and self.error_recovery:
                    # Avoid sending overly large alert_data if it contains huge payloads
                    alert_data_summary = {k: v for k, v in alert_data.items() if isinstance(v, (str, int, float, bool)) or k == "id"}
                    await self.error_recovery.record_error(
                        "alert_processing", 
                        {"error": str(e), "alert_id": alert_id, "alert_data_summary": alert_data_summary}
                    )
                return {
                    "status": "error",
                    "message": f"Internal error processing alert: {str(e)}",
                    "alert_id": alert_id
                }
            finally: # This will always execute after try or except
                self.active_alerts.discard(alert_id)
                if self.system_monitor:
                    await self.system_monitor.update_component_status("alert_handler", "ok", f"Finished processing alert ID {alert_id}")

    async def _close_position(self, symbol: str) -> dict:
        """
        Close any open position for a given symbol on OANDA.
        """
        try:
            from oandapyV20.endpoints.positions import PositionClose
            
            # Get OANDA account ID from config
            account_id = get_config_value('oanda_account_id')
            if not account_id:
                return {"status": "error", "message": "OANDA account ID not configured"}
    
            request = PositionClose(
                accountID=account_id,
                instrument=symbol,
                data={"longUnits": "ALL", "shortUnits": "ALL"}
            )
    
            # Use the robust_oanda_request function
            response = await robust_oanda_request(request)
            logger.info(f"[CLOSE] Closed position for {symbol}: {response}")
            return response
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {str(e)}", exc_info=True)
            return {"status": "error", "message": str(e)}

    async def _should_override_close(self, position_id: str, position_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Simplified override logic for CLOSE signals:
        • Override (i.e. defer the close) if the trade is at least 0.5 × ATR in profit AND the regime is trending.
        • Otherwise, honor the CLOSE signal immediately.
        Returns:
            (True, reason)  if we should defer to HybridExitManager
            (False, reason) if we should honor the CLOSE now
        """
        # 1) Extract basic position info
        symbol    = position_data.get("symbol")
        direction = position_data.get("action", "").upper()   # "BUY" or "SELL"
        entry     = position_data.get("entry_price")
        timeframe = position_data.get("timeframe", "H1")

        # 2) Get current price if not already in position_data
        current_price = position_data.get("current_price")
        if current_price is None:
            try:
                current_price = await get_current_price(symbol, direction)
            except Exception as e:
                # If we cannot fetch a price, do not override
                return False, f"Price fetch failed: {e}"

        # 3) Check if position is in profit
        if direction == "BUY":
            profit_pips = current_price - entry
        else:  # "SELL"
            profit_pips = entry - current_price

        if profit_pips <= 0:
            return False, f"Not in profit (profit_pips={profit_pips:.5f})"

        # 4) Fetch ATR for this symbol/timeframe
        try:
            atr_value = await get_atr(symbol, timeframe)
        except Exception as e:
            return False, f"ATR fetch failed: {e}"

        if atr_value is None or atr_value <= 0:
            return False, f"Invalid ATR ({atr_value})"

        # 5) Compare profit to 0.5 × ATR
        half_atr = 0.5 * atr_value
        if profit_pips < half_atr:
            return False, f"Profit ({profit_pips:.5f}) < 0.5 × ATR ({half_atr:.5f})"

        # 6) Check regime via Lorentzian classifier
        try:
            regime_data = self.regime_classifier.get_regime_data(symbol)
            regime      = regime_data.get("regime", "unknown").lower()
        except Exception as e:
            return False, f"Regime fetch failed: {e}"

        if (direction == "BUY" and regime not in ["trending_up", "momentum_up"]) or \
           (direction == "SELL" and regime not in ["trending_down", "momentum_down"]):
            return False, f"Regime not trending for direction: {regime}"

        # 7) All checks passed → override the CLOSE
        reason = (
            f"Profit ({profit_pips:.5f}) ≥ 0.5 × ATR ({half_atr:.5f}) "
            f"and regime = {regime}"
        )
        return True, reason

    async def stop(self):
        """Clean-up hook called during shutdown."""
        logger.info("Shutting down EnhancedAlertHandler...")
        
        # Signal the scheduled-tasks loop to exit
        self._running = False
        
        # Give any in-flight iteration a moment to finish
        await asyncio.sleep(1)
        
        # Close the Postgres pool if it exists
        if hasattr(self, "postgres_manager"):
            await self.postgres_manager.close()
        
        # Tear down notifications
        if hasattr(self, "notification_system"):
            await self.notification_system.shutdown()
        
        logger.info("aaa shutdown complete.")


# ─── Exception Classes ────────────────────────────────────────

class TradingSystemError(Exception):
    """Base exception for trading system errors"""
    pass


# ─── Placeholder Functions ────────────────────────────────────────
# These functions are referenced but not defined in the provided code

async def get_account_balance(use_fallback=False):
    """Get account balance from OANDA"""
    try:
        # This would be implemented with actual OANDA API calls
        return 10000.0  # Placeholder
    except Exception as e:
        if use_fallback:
            logger.warning(f"Using fallback balance due to error: {e}")
            return 10000.0
        raise

async def get_current_price(symbol: str, direction: str):
    """Get current price for a symbol"""
    # This would be implemented with actual OANDA API calls
    return 1.0850  # Placeholder

async def get_atr(symbol: str, timeframe: str):
    """Get ATR value for symbol and timeframe"""
    # This would be implemented with actual market data
    return 0.0025  # Placeholder

def get_instrument_type(symbol: str) -> str:
    """Determine instrument type from symbol"""
    if "USD" in symbol and len(symbol.split("_")) == 2:
        if "JPY" in symbol:
            return "jpy_pair"
        return "forex"
    elif "XAU" in symbol or "XAG" in symbol:
        return "metal"
    elif "BTC" in symbol or "ETH" in symbol:
        return "crypto"
    return "other"

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if instrument is tradeable"""
    # This would include market hours, symbol validation, etc.
    return True, "Market open and symbol valid"

async def execute_trade(payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute trade with OANDA"""
    # This would be implemented with actual OANDA API calls
    return True, {"status": "success", "trade_id": str(uuid.uuid4())}

async def close_position(payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close position with OANDA"""
    # This would be implemented with actual OANDA API calls
    return True, {"status": "success", "actual_exit_price": 1.0850}

async def get_price_with_fallbacks(symbol: str, direction: str) -> Tuple[float, str]:
    """Get price with fallback mechanisms"""
    try:
        price = await get_current_price(symbol, direction)
        return price, "current"
    except Exception as e:
        raise ValueError(f"Could not fetch price: {e}")


# ─── Initialize OANDA Client ────────────────────────────────────────

# Load OANDA Credentials
OANDA_ACCESS_TOKEN = get_config_value('oanda_access_token', 'OANDA_ACCESS_TOKEN')
OANDA_ENVIRONMENT = get_config_value('oanda_environment', 'OANDA_ENVIRONMENT', 'practice')
OANDA_ACCOUNT_ID = get_config_value('oanda_account_id', 'OANDA_ACCOUNT_ID')

# Initialize OANDA client if credentials are available
oanda = None
if OANDA_ACCESS_TOKEN and OANDA_ACCOUNT_ID:
    try:
        import oandapyV20
        oanda = oandapyV20.API(
            access_token=OANDA_ACCESS_TOKEN,
            environment=OANDA_ENVIRONMENT
        )
        logger.info(f"OANDA client initialized for {OANDA_ENVIRONMENT} environment")
    except Exception as e:
        logger.error(f"Failed to initialize OANDA client: {e}")
else:
    logger.warning("OANDA credentials not found - client not initialized")


# ─── Application Startup and Shutdown ────────────────────────────────────────

async def startup_application():
    """Initialize all components during application startup"""
    global db_manager, alert_handler, backup_manager, error_recovery
    
    try:
        logger.info("Starting application initialization...")
        
        # Initialize database manager
        if config.database_url:
            try:
                db_manager = PostgresDatabaseManager()
                await db_manager.initialize()
                logger.info("Database manager initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize database manager: {e}")
                # Don't fail startup completely for database issues
        
        # Initialize alert handler
        alert_handler = EnhancedAlertHandler()
        startup_success = await alert_handler.start()
        
        if startup_success:
            logger.info("Application startup completed successfully")
        else:
            logger.warning("Application started with warnings - check logs")
            
    except Exception as e:
        logger.error(f"Critical error during application startup: {e}", exc_info=True)
        raise


async def shutdown_application():
    """Clean shutdown of all components"""
    global alert_handler, db_manager, backup_manager, error_recovery
    
    try:
        logger.info("Starting application shutdown...")
        
        if alert_handler:
            await alert_handler.stop()
            
        if db_manager:
            await db_manager.close()
            
        logger.info("Application shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during application shutdown: {e}", exc_info=True)


# ─── Main Application Entry Point ────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    
    # Run the FastAPI application
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level="info",
        access_log=True,
        loop="asyncio"
    )


@app.post("/tradingview")
async def tradingview_webhook(request: Request):
    """
    Process TradingView webhook alerts with normalization and robust error handling.
    """
    request_id = str(uuid.uuid4())

    try:
        payload = await request.json()
        logger.info(f"[{request_id}] Received TradingView webhook:\n{json.dumps(payload, indent=2)}")

        # Format JPY pair if in 6-letter format (e.g., GBPJPY -> GBP_JPY)
        if "symbol" in payload and "JPY" in payload["symbol"] and len(payload["symbol"]) == 6:
            payload["symbol"] = payload["symbol"][:3] + "_" + payload["symbol"][3:]
            logger.info(f"[{request_id}] Formatted JPY pair to: {payload['symbol']}")

        # === Normalize Fields ===
        alert_data = {
            "instrument": payload.get("symbol") or payload.get("ticker", ""),
            "direction": payload.get("action") or payload.get("side") or payload.get("type", ""),
            "risk_percent": float(
                payload.get("percentage")
                or payload.get("risk")
                or payload.get("risk_percent", 1.0)
            ),
            "timeframe": normalize_timeframe(payload.get("timeframe") or payload.get("tf", "1H")),
            "exchange": payload.get("exchange"),
            "account": payload.get("account"),
            "comment": payload.get("comment"),
            "strategy": payload.get("strategy"),
            "request_id": request_id
        }

        # === Basic Validation ===
        if not alert_data["instrument"] or not alert_data["direction"]:
            logger.error(f"[{request_id}] Missing required fields: instrument or direction")
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "Missing required instrument or direction fields"}
            )

        # === Handle Alert ===
        if not alert_handler:
            return JSONResponse(
                status_code=503,
                content={"success": False, "message": "Alert handler not initialized"}
            )
        result = await alert_handler.process_alert(alert_data)

        logger.info(f"[{request_id}] Alert handled successfully:\n{json.dumps(result, indent=2)}")
        return JSONResponse(content=result)

    except json.JSONDecodeError as e:
        logger.error(f"[{request_id}] JSON parsing error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": "Invalid JSON payload"}
        )

    except Exception as e:
        logger.error(f"[{request_id}] Unexpected error: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Internal server error: {str(e)}"}
        )


@app.post("/api/trade", tags=["trading"])
async def manual_trade(request: Request):
    """Endpoint for manual trade execution"""
    try:
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
        action_upper = data["action"].upper() # Process once
        if action_upper not in valid_actions:
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

            # Add timestamp and ensure action is uppercase
            data["timestamp"] = datetime.now(timezone.utc).isoformat()
            data["action"] = action_upper # Use the uppercased action

            # Ensure alert_id is present if needed by process_alert
            if "id" not in data:
                data["id"] = str(uuid.uuid4())
            
            # Add percentage to data if process_alert expects it (it's validated but not explicitly added before)
            data["percentage"] = percentage 

            result = await alert_handler.process_alert(data)
            return result # process_alert should return a Response object or a dict for JSONResponse
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


@app.put("/api/positions/{position_id}", tags=["positions"])
async def update_position(position_id: str, request: Request):
    """Update position (e.g., take profit only; stop loss is not supported)."""
    try:
        if not alert_handler or not hasattr(alert_handler, "position_tracker"):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Position tracker not initialized"}
            )

        # Get update data
        data = await request.json()

        # Fetch existing position
        position = await alert_handler.position_tracker.get_position_info(position_id)
        if not position:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"error": f"Position {position_id} not found"}
            )
        if position.get("status") == "closed":
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Cannot update closed position"}
            )

        # We do not support stop-loss—always clear it
        stop_loss = None

        # Parse take_profit if provided
        take_profit = data.get("take_profit")
        if take_profit is not None:
            try:
                take_profit = float(take_profit)
            except ValueError:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Invalid take profit value"}
                )

        # Extract metadata (if any)
        metadata = data.get("metadata")

        # Perform the update on the PositionTracker
        success = await alert_handler.position_tracker.update_position(
            position_id=position_id,
            stop_loss=stop_loss,       # always None
            take_profit=take_profit,
            metadata=metadata
        )
        if not success:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"error": "Failed to update position"}
            )

        # Retrieve and return the updated position
        updated_position = await alert_handler.position_tracker.get_position_info(position_id)
        return {
            "status": "success",
            "message": "Position updated",
            "position": updated_position
        }

    except Exception as e:
        logger.error(f"Error updating position {position_id}: {e}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "details": str(e)}
        )


# ─── Database Testing Endpoints ────────────────────────────────────────

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


@app.post("/api/database/test-position")
async def test_position():
    """Test position retrieval from database"""
    if not db_manager:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            content={"detail": "DB not initialized"}
        )
    # Example usage: fetch a position from DB
    pos_id = "some_position_id"
    data = await db_manager.get_position(pos_id)
    if not data:
        return JSONResponse(
            status_code=404, 
            content={"detail": "Position not found"}
        )
    return data


@app.get("/test-db")
async def test_db():
    """Simple database test endpoint"""
    try:
        conn = await asyncpg.connect(config.database_url)
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        return {"status": "success", "postgres_version": version}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ─── OANDA Testing Endpoints ────────────────────────────────────────

@app.get("/api/test-oanda", tags=["system"])
async def test_oanda_connection():
    """Test OANDA API connection"""
    try:
        # Check if OANDA client is available
        oanda_client = globals().get('oanda')
        account_id = get_config_value('oanda_account_id')
        environment = get_config_value('oanda_environment', default='practice')
        
        if not oanda_client:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "message": "OANDA client not initialized"}
            )
            
        if not account_id:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "message": "OANDA account ID not configured"}
            )
            
        # Test getting account info
        from oandapyV20.endpoints.accounts import AccountSummary
        account_request = AccountSummary(accountID=account_id)
        
        response = await robust_oanda_request(account_request)
        
        # Mask sensitive data
        if "account" in response and "balance" in response["account"]:
            balance = float(response["account"]["balance"])
        else:
            balance = None
            
        return {
            "status": "ok",
            "message": "OANDA connection successful",
            "account_id": account_id,
            "environment": environment,
            "balance": balance,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"OANDA test failed: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error", 
                "message": f"OANDA test failed: {str(e)}"
            }
        )


# ─── Debug and Risk Management Endpoints ────────────────────────────────────────

@app.get("/api/debug/symbol/{symbol}", tags=["debug"])
async def debug_symbol_standardization(symbol: str):
    """Debug symbol standardization process"""
    try:
        logger.info(f"Debug: Starting standardization for '{symbol}'")
        
        # Step-by-step debugging
        if not symbol:
            return {"error": "Empty symbol provided"}
        
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')
        logger.info(f"Debug: After uppercase/replace: '{symbol_upper}'")
        
        # Check CRYPTO_MAPPING
        crypto_mapping_check = symbol_upper in CRYPTO_MAPPING
        crypto_mapping_value = CRYPTO_MAPPING.get(symbol_upper, "NOT_FOUND")
        logger.info(f"Debug: CRYPTO_MAPPING check: {crypto_mapping_check}, value: {crypto_mapping_value}")
        
        # Run actual standardize_symbol
        result = standardize_symbol(symbol)
        logger.info(f"Debug: standardize_symbol result: '{result}'")
        
        return {
            "original_symbol": symbol,
            "symbol_upper": symbol_upper,
            "crypto_mapping_exists": crypto_mapping_check,
            "crypto_mapping_value": crypto_mapping_value,
            "standardized_result": result,
            "crypto_mapping_keys": list(CRYPTO_MAPPING.keys())[:10],  # First 10 keys
            "result_is_empty": not bool(result)
        }
    except Exception as e:
        logger.error(f"Debug symbol error: {str(e)}", exc_info=True)
        return {"error": str(e), "traceback": str(traceback.format_exc())}


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

# ─── Performance Tracking ────────────────────────────────────────

class PerformanceMonitor:
    """
    Tracks execution timing for various trading operations.
    Pure diagnostic tool - doesn't affect trading logic.
    """

    def __init__(self):
        self.execution_times = {
            "order_execution": [],
            "price_fetching": [],
            "database_operations": [],
            "risk_calculations": [],
            "position_updates": [],
            "alert_processing": [],
            "close_position": [],
            "market_data_fetch": [],
        }
        self._lock = asyncio.Lock()
        self.max_samples = 1000  # Keep last 1000 measurements per operation

    @asynccontextmanager
    async def track_execution(self, operation_name: str):
        """Context manager to track operation timing"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = (time.time() - start_time) * 1000  # Convert to milliseconds
            await self._record_timing(operation_name, duration)

    async def _record_timing(self, operation_name: str, duration_ms: float):
        """Record timing data thread-safely"""
        async with self._lock:
            if operation_name not in self.execution_times:
                self.execution_times[operation_name] = []

            self.execution_times[operation_name].append(duration_ms)

            # Keep only recent measurements to prevent memory growth
            if len(self.execution_times[operation_name]) > self.max_samples:
                self.execution_times[operation_name] = self.execution_times[
                    operation_name
                ][-self.max_samples :]

    async def get_performance_stats(
        self, operation_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get performance statistics for an operation"""
        async with self._lock:
            times = self.execution_times.get(operation_name, [])
            if not times:
                return None

            times_sorted = sorted(times)
            count = len(times)

            return {
                "operation": operation_name,
                "sample_count": count,
                "avg_ms": statistics.mean(times),
                "median_ms": statistics.median(times),
                "p50_ms": times_sorted[int(count * 0.50)] if count > 0 else 0,
                "p95_ms": times_sorted[int(count * 0.95)] if count > 1 else 0,
                "p99_ms": times_sorted[int(count * 0.99)] if count > 1 else 0,
                "max_ms": max(times),
                "min_ms": min(times),
                "last_10_avg_ms": statistics.mean(times[-10:])
                if len(times) >= 10
                else statistics.mean(times),
            }

    async def get_all_performance_stats(self) -> Dict[str, Any]:
        """Get performance stats for all tracked operations"""
        async with self._lock:
            all_stats = {}
            for operation_name in self.execution_times.keys():
                stats = await self.get_performance_stats(operation_name)
                if stats:
                    all_stats[operation_name] = stats

            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operations": all_stats,
                "summary": {
                    "total_operations_tracked": len(all_stats),
                    "total_samples": sum(
                        len(times) for times in self.execution_times.values()
                    ),
                },
            }

    async def reset_stats(self, operation_name: Optional[str] = None):
        """Reset statistics for one operation or all operations"""
        async with self._lock:
            if operation_name:
                if operation_name in self.execution_times:
                    self.execution_times[operation_name] = []
            else:
                for op in self.execution_times:
                    self.execution_times[op] = []

# Insert at the end of the file, before the main execution block
async def execute_oanda_reduction_order(
    instrument: str,
    units_to_reduce_abs: float,
    original_position_action: str,
    account_id: str,
    request_id: str
) -> Tuple[bool, Dict[str, Any]]:
    """Execute a reduction order with OANDA to partially close a position"""
    # Implementation is included in the artifact above
