##############################################################################
# An institutional-grade trading platform with advanced risk management,
# machine learning capabilities, and comprehensive market analysis.
##############################################################################

import asyncio
import aiohttp
import configparser
import glob
import json
import logging
logger = logging.getLogger(__name__)
import logging.handlers
import math
import random
import re
import statistics
import tarfile
import traceback
import uuid
import pandas as pd
import ta
import os, configparser
import oandapyV20
import asyncpg
import subprocess
import numpy as np
import requests
import urllib3
import http.client
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Literal, Tuple, NamedTuple, Callable, TypeVar, ParamSpec
from fastapi import FastAPI, Query, Request, status, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from oandapyV20.endpoints import instruments
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.positions import OpenPositions
from oandapyV20.endpoints.trades import OpenTrades
from pydantic import BaseModel, Field, SecretStr
from typing import Optional
from urllib.parse import urlparse
from functools import wraps
from pydantic import BaseModel, Field, validator, constr, confloat, model_validator, SecretStr

# Add this near the beginning of your code, with your other imports and class definitions
class ClosePositionResult(NamedTuple):
    success: bool
    position_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

##############################################################################
# Structured Logging Setup (INSERT HERE)
##############################################################################

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

def setup_logging():
    """Configure logging with JSON formatting and rotating handlers"""
    
    # Create logs directory if it doesn't exist
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
        encoding="utf-8"
    )
    main_handler.setFormatter(json_formatter)
    main_handler.setLevel(logging.INFO)
    
    # Separate handler for error logs
    error_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "errors.log"),
        maxBytes=10*1024*1024,  # 10MB
        backupCount=10,
        encoding="utf-8"
    )
    error_handler.setFormatter(json_formatter)
    error_handler.setLevel(logging.ERROR)
    
    # Trade execution logs (critical for audit)
    trade_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "trades.log"),
        when="midnight",
        interval=1,
        backupCount=90,  # Keep 90 days for compliance
        encoding="utf-8"
    )
    trade_handler.setFormatter(json_formatter)
    trade_handler.setLevel(logging.INFO)
    trade_handler.addFilter(lambda record: "trade" in record.getMessage().lower())
    
    # Console handler with standard formatting
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    ))
    console_handler.setLevel(logging.INFO)
    
    # Add handlers to root logger
    logger.addHandler(main_handler)
    logger.addHandler(error_handler)
    logger.addHandler(trade_handler)
    logger.addHandler(console_handler)
    
    return logger
    
# Custom logging adapter for adding context
class TradingLogger(logging.LoggerAdapter):
    """Custom logger adapter for adding trading context"""
    
    def process(self, msg, kwargs):
        # Add extra context to all log messages
        extra = kwargs.get('extra', {})
        if 'position_id' in self.extra:
            extra['position_id'] = self.extra['position_id']
        if 'symbol' in self.extra:
            extra['symbol'] = self.extra['symbol']
        if 'request_id' in self.extra:
            extra['request_id'] = self.extra['request_id']
        kwargs['extra'] = extra
        return msg, kwargs

def get_module_logger(module_name: str, **context) -> TradingLogger:
    """Get a logger with trading context"""
    base_logger = logging.getLogger(module_name)
    return TradingLogger(base_logger, context)

# Initialize the logger
logger = setup_logging()

##############################################################################
# Performance Tracking
##############################################################################

class PerformanceMonitor:
    """
    Tracks execution timing for various trading operations.
    Pure diagnostic tool - doesn't affect trading logic.
    """
    def __init__(self):
        self.execution_times = {
            'order_execution': [],
            'price_fetching': [],
            'database_operations': [],
            'risk_calculations': [],
            'position_updates': [],
            'alert_processing': [],
            'close_position': [],
            'market_data_fetch': []
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
                self.execution_times[operation_name] = self.execution_times[operation_name][-self.max_samples:]
    
    async def get_performance_stats(self, operation_name: str) -> Optional[Dict[str, Any]]:
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
                "last_10_avg_ms": statistics.mean(times[-10:]) if len(times) >= 10 else statistics.mean(times)
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
                    "total_samples": sum(len(times) for times in self.execution_times.values())
                }
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


##############################################################################
# Configuration Management
##############################################################################

class Config(BaseModel):
    """Configuration settings for the application."""

    # API and connection settings
    host: str = Field(default=os.environ.get("HOST", "0.0.0.0"), description="Server host address")
    port: int = Field(default=int(os.environ.get("PORT", 8000)), description="Server port") # This line is good
    
    enable_broker_reconciliation: bool = Field(
        default=True, # Default to True, meaning reconciliation runs unless explicitly disabled
        description="Enable/disable broker position reconciliation on startup."
    )

    allowed_origins: str = Field(
        default=os.environ.get("ALLOWED_ORIGINS", "*"), 
        description="Comma-separated list of allowed CORS origins"
    )
    environment: str = Field(
        default=os.environ.get("ENVIRONMENT", "production"), 
        description="Application environment (production/staging/development)"
    )
    connect_timeout: int = Field(
        default=int(os.environ.get("CONNECT_TIMEOUT", 10)),
        description="Connection timeout in seconds"
    )
    read_timeout: int = Field(
        default=int(os.environ.get("READ_TIMEOUT", 30)),
        description="Read timeout in seconds"
    )

    # Trading settings
    oanda_account: str = Field(
        default=os.environ.get("OANDA_ACCOUNT_ID", ""),
        description="OANDA account ID"
    )
    oanda_access_token: SecretStr = Field(
        default=os.environ.get("OANDA_ACCESS_TOKEN", ""),
        description="OANDA API access token"
    )
    oanda_environment: str = Field(
        default=os.environ.get("OANDA_ENVIRONMENT", "practice"),
        description="OANDA environment (practice/live)"
    )
    active_exchange: str = Field(
        default=os.environ.get("ACTIVE_EXCHANGE", "oanda"),
        description="Currently active trading exchange"
    )

    # Risk parameters
    default_risk_percentage: float = Field(
        default=float(os.environ.get("DEFAULT_RISK_PERCENTAGE", 20.0)),
        description="Default risk percentage per trade",
        ge=0,
        le=100
    )
    max_risk_percentage: float = Field(
        default=float(os.environ.get("MAX_RISK_PERCENTAGE", 20.0)),
        description="Maximum allowed risk percentage per trade",
        ge=0,
        le=100
    )
    max_portfolio_heat: float = Field(
        default=float(os.environ.get("MAX_PORTFOLIO_HEAT", 70.0)),
        description="Maximum portfolio heat percentage",
        ge=0,
        le=100
    )
    max_daily_loss: float = Field(
        default=float(os.environ.get("MAX_DAILY_LOSS", 50.0)),
        description="Maximum daily loss percentage",
        ge=0,
        le=100
    )

    # Database settings
    database_url: str = Field(
        default=os.environ["DATABASE_URL"],
        description="Database connection URL (required)"
    )
    db_min_connections: int = Field(
        default=int(os.environ.get("DB_MIN_CONNECTIONS", 5)),
        description="Minimum database connections in pool",
        gt=0
    )
    db_max_connections: int = Field(
        default=int(os.environ.get("DB_MAX_CONNECTIONS", 20)),
        description="Maximum database connections in pool",
        gt=0
    )

    # Backup settings
    backup_dir: str = Field(
        default=os.environ.get("BACKUP_DIR", "./backups"),
        description="Directory for backup files"
    )
    backup_interval_hours: int = Field(
        default=int(os.environ.get("BACKUP_INTERVAL_HOURS", 24)),
        description="Backup interval in hours",
        gt=0
    )

    # Notification settings
    slack_webhook_url: Optional[SecretStr] = Field(
        default=os.environ.get("SLACK_WEBHOOK_URL"),
        description="Slack webhook URL for notifications"
    )
    telegram_bot_token: Optional[SecretStr] = Field(
        default=os.environ.get("TELEGRAM_BOT_TOKEN"),
        description="Telegram bot token for notifications"
    )
    telegram_chat_id: Optional[str] = Field(
        default=os.environ.get("TELEGRAM_CHAT_ID"),
        description="Telegram chat ID for notifications"
    )

    model_config = {
        "case_sensitive": True,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }
    
    @classmethod
    def model_json_schema(cls, **kwargs):
        """Customize the JSON schema for this model."""
        schema = super().model_json_schema(**kwargs)
        
        # Remove sensitive fields from schema examples
        for field in ["oanda_access_token", "slack_webhook_url", "telegram_bot_token"]:
            if field in schema.get("properties", {}):
                schema["properties"][field]["examples"] = ["******"]
        
        return schema

# Initialize config
config = Config()

# Constants
MAX_DAILY_LOSS = config.max_daily_loss / 100  # Convert percentage to decimal
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 5


######################
# Globals and Helpers
######################

# Field mapping for TradingView webhook format
TV_FIELD_MAP = {
    "ticker": "instrument",
    "side": "direction",
    "risk": "risk_percent",
    "entry": "entry_price",
    "sl": "stop_loss",
    "tp": "take_profit",
    "tf": "timeframe"
}

# Define leverages for different instruments
INSTRUMENT_LEVERAGES = {
    'XAU_USD': 20,       # Changed XAU/USD to XAU_USD
    'XAG_USD': 20,       # Changed XAG/USD to XAG_USD
    'EUR_USD': 30,
    'GBP_USD': 30,
    'USD_JPY': 30,
    'USD_CHF': 30,
    'AUD_USD': 30,
    'NZD_USD': 30,
    'USD_CAD': 30,
    'BTC_USD': 2,        # Changed BTC/USD to BTC_USD
    'ETH_USD': 5,        # Changed ETH/USD to ETH_USD
    'default': 20,       # Default leverage for other instruments
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
    "ETH/USD": "ETH_USD"
}

# Crypto minimum trade sizes 
CRYPTO_MIN_SIZES = {
    "BTC": 0.0001,
    "ETH": 0.002,
    "LTC": 0.05,
    "XRP": 0.01,
    "XAU": 0.2  # Gold minimum
}
    
# Crypto maximum trade sizes
CRYPTO_MAX_SIZES = {
    "BTC": 10,
    "ETH": 135,
    "LTC": 3759,
    "XRP": 50000,
    "XAU": 500  # Gold maximum
}
    
# Define tick sizes for precision rounding
CRYPTO_TICK_SIZES = {
    "BTC": 0.001,
    "ETH": 0.05,
    "LTC": 0.01,
    "XRP": 0.001,
    "XAU": 0.01  # Gold tick size
}

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format with better error handling and support for various formats"""
    if not symbol:
        return ""

    try:
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')

        # If already in “XXX_YYY” form and both parts look valid, return it
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

        # JPY pair like “GBPJPY” → “GBP_JPY”
        if "JPY" in symbol_upper and "_" not in symbol_upper and len(symbol_upper) == 6:
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"

        # 6-char Forex pairs (not crypto) → “EURUSD” → “EUR_USD”
        if (
            len(symbol_upper) == 6
            and not any(crypto in symbol_upper for crypto in CRYPTO_MAPPING)
        ):
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"

        # Fallback for crypto pairs without mapping: e.g. “BTCUSD”
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
    if "_" in symbol:
        return symbol
    if len(symbol) == 6:
        return symbol[:3] + "_" + symbol[3:]
    return symbol  # fallback, in case it's something like an index or crypto

# Replace BOTH existing get_current_market_session functions with this one
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

async def robust_oanda_request(request_obj, max_retries=3, initial_delay=1):
    """
    Execute OANDA requests with robust error handling and exponential backoff
    
    Args:
        request_obj: An OANDA request object (e.g., from PricingInfo)
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds (will be doubled each retry)
    
    Returns:
        Response from OANDA API
    
    Raises:
        BrokerConnectionError: If all retries fail
    """
    loop = asyncio.get_running_loop()
    retries = 0
    last_error = None
    
    while retries <= max_retries:
        try:
            # Run synchronous OANDA request in a thread pool to avoid blocking
            response = await loop.run_in_executor(
                None, 
                lambda: oanda.request(request_obj)
            )
            return response
            
        except (requests.exceptions.ConnectionError, 
                http.client.RemoteDisconnected,
                urllib3.exceptions.ProtocolError,
                oandapyV20.exceptions.V20Error) as e:
                
            retries += 1
            last_error = e
            
            # Check if error is worth retrying
            if isinstance(e, oandapyV20.exceptions.V20Error):
                # Some API errors might not be worth retrying (e.g., invalid instrument)
                if not (e.code in [None, "TIMEOUT", "SOCKET_ERROR", "MARKET_HALTED"]):
                    logger.warning(f"Non-retryable OANDA API error: {e.code} - {e.msg}")
                    raise BrokerConnectionError(f"OANDA API error: {e.msg}")
            
            if retries > max_retries:
                logger.error(f"Exhausted {max_retries} retries for OANDA request: {str(e)}")
                break
                
            # Exponential backoff
            wait_time = initial_delay * (2 ** (retries - 1))
            logger.warning(f"OANDA request failed (attempt {retries}/{max_retries}), "
                          f"retrying in {wait_time}s: {str(e)}")
            await asyncio.sleep(wait_time)
    
    # If we get here, all retries failed
    error_msg = str(last_error) if last_error else "Unknown error"
    logger.error(f"All OANDA request attempts failed: {error_msg}")
    raise BrokerConnectionError(f"Failed to communicate with OANDA after {max_retries} attempts: {error_msg}")


def parse_iso_datetime(datetime_str: str) -> datetime:
    """Parse ISO formatted datetime string to datetime object with proper timezone handling"""
    if not datetime_str:
        return None
        
    try:
        # Handle UTC Z suffix
        if datetime_str.endswith('Z'):
            datetime_str = datetime_str[:-1] + '+00:00'
            
        # Handle strings without timezone info
        if '+' not in datetime_str and '-' not in datetime_str[10:]:
            datetime_str += '+00:00'
            
        return datetime.fromisoformat(datetime_str)
    except ValueError as e:
        logger.error(f"Error parsing datetime {datetime_str}: {str(e)}")
        # Return current time as fallback
        return datetime.now(timezone.utc)

def get_config_value(attr_name: str, env_var: str = None, default = None):
    """Get configuration value with consistent fallback strategy"""
    # Try config object first
    if hasattr(config, attr_name):
        value = getattr(config, attr_name)
        # Handle SecretStr
        if isinstance(value, SecretStr):
            return value.get_secret_value()
        return value
        
    # Try environment variable
    if env_var and env_var in os.environ:
        return os.environ[env_var]
        
    # Return default
    return default


class TradingViewAlertPayload(BaseModel):
    """Validated TradingView webhook payload matching TradingView's exact field names"""
    symbol: constr(strip_whitespace=True, min_length=3) = Field(..., description="Trading instrument (e.g., EURUSD, BTCUSD)")
    action: Literal["BUY", "SELL", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT"] = Field(..., description="Trade direction")
    percentage: Optional[confloat(gt=0, le=100)] = Field(None, description="Risk percentage for the trade (0 < x <= 100)")
    timeframe: str = Field(default="1H", description="Timeframe for the trade")
    exchange: Optional[str] = Field(None, description="Exchange name (from webhook)")
    account: Optional[str] = Field(None, description="Account ID (from webhook)")
    orderType: Optional[str] = Field(None, description="Order type (from webhook)")
    timeInForce: Optional[str] = Field(None, description="Time in force (from webhook)")
    comment: Optional[str] = Field(None, description="Additional comment for the trade")
    strategy: Optional[str] = Field(None, description="Strategy name")
    request_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique request ID")

    @validator('timeframe', pre=True, always=True)
    def validate_timeframe(cls, v):
        """Normalize and validate timeframe input"""
        if v is None:
            return "1H"

        v = str(v).strip().upper()
        if v in ["D", "1D", "DAILY"]:
            return "1D"
        if v in ["W", "1W", "WEEKLY"]:
            return "1W"
        if v in ["MN", "1MN", "MONTHLY"]:
            return "1MN"

        # Handle digit-only inputs like "15", "60"
        if v.isdigit():
            mapping = {
                "1": "1H",
                "15": "15M",
                "30": "30M",
                "60": "1H",
                "240": "4H",
                "720": "12H"
            }
            return mapping.get(v, f"{v}M")

        # Regex match for formats like "15M", "1H"
        if not re.match(r"^\d+[MH]$", v):
            raise ValueError("Invalid timeframe format. Use '15M', '1H', '4H', etc.")

        return v

    @model_validator(mode='after')
    def validate_percentage_for_actions(self):
        """Validate that percentage is provided for BUY/SELL but optional for CLOSE actions"""
        if self.action in ["BUY", "SELL"] and self.percentage is None:
            raise ValueError(f"percentage is required for {self.action} actions")
            
        # For CLOSE actions, percentage is optional
        return self

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        # Keep as "ignore" to maintain compatibility with various TradingView alert formats
        extra = "ignore"

######################
# FastAPI Apps
######################

# Initialize FastAPI application
app = FastAPI(
    title="Enhanced Trading System API",
    description="Institutional-grade trading system with advanced risk management",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trading_system.log"),
    ],
)
logger = logging.getLogger("trading_system")


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# —— Start OANDA credential loading ——
# 1) Read from environment first
OANDA_ACCESS_TOKEN = os.getenv('OANDA_ACCESS_TOKEN')
OANDA_ENVIRONMENT = os.getenv('OANDA_ENVIRONMENT', 'practice')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')

# 2) If any are missing, fall back to config.ini (for local dev)
if not (OANDA_ACCESS_TOKEN and OANDA_ACCOUNT_ID):
    config = configparser.ConfigParser()
    config.read('config.ini')
    try:
        OANDA_ACCESS_TOKEN = OANDA_ACCESS_TOKEN or config.get(
            'oanda', 'access_token'
        )
        OANDA_ENVIRONMENT = OANDA_ENVIRONMENT or config.get(
            'oanda', 'environment'
        )
        OANDA_ACCOUNT_ID = OANDA_ACCOUNT_ID or config.get(
            'oanda', 'account_id'
        )
    except configparser.NoSectionError:
        raise RuntimeError(
            "Missing OANDA credentials: set env vars OANDA_ACCESS_TOKEN & OANDA_ACCOUNT_ID, "
            "or add an [oanda] section in config.ini."
        )

# 3) Instantiate the OANDA client
oanda = oandapyV20.API(
    access_token=OANDA_ACCESS_TOKEN, environment=OANDA_ENVIRONMENT
)
# —— End credential loading ——


# Timeframe seconds mapping
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
    """
    Get ATR value with dynamic calculation, API retries, and smart fallbacks.
    Combines features from both previous versions. Corrected oanda.request call.
    """
    symbol = standardize_symbol(symbol)
    request_id = str(uuid.uuid4()) # Add a unique ID for logging this request
    logger = get_module_logger(__name__, symbol=symbol, request_id=request_id)

    logger.info(f"[ATR] Fetching ATR for {symbol}, TF={timeframe}, Period={period}")

    # --- Timeframe Normalization ---
    try:
        # Use the existing normalize_timeframe helper function
        oanda_granularity = normalize_timeframe(timeframe, target="OANDA")
        logger.info(f"[ATR] Using OANDA granularity: {oanda_granularity} for timeframe {timeframe}")
    except Exception as e:
        logger.error(f"[ATR] Error normalizing timeframe '{timeframe}': {str(e)}. Defaulting to H1.")
        oanda_granularity = "H1" # Default fallback granularity

    # --- OANDA API Call with Retry Logic ---
    max_retries = 3  # Changed from MAX_RETRY_ATTEMPTS to hardcoded 3
    retry_delay = 2 # seconds
    oanda_candles = None

    for retry in range(max_retries):
        try:
            params = {"granularity": oanda_granularity, "count": period + 5, "price": "M"}
            req = instruments.InstrumentsCandles(instrument=symbol, params=params)
            response = await robust_oanda_request(req)

            candles = response.get("candles", [])
            oanda_candles = [c for c in candles if c.get("complete", True)]

            if len(oanda_candles) >= period + 1:
                logger.info(f"[ATR] Attempt {retry+1}: Retrieved {len(oanda_candles)} candles from OANDA API.")
                break # Success
            else:
                # Raise error here to trigger retry or move to fallback after loop
                 raise ValueError(f"Attempt {retry+1}: Not enough complete candles from OANDA API ({len(oanda_candles)} < {period+1})")

        except Exception as e:
            # Log specific OANDA V20 errors if available
            if isinstance(e, oandapyV20.exceptions.V20Error):
                 logger.warning(f"[ATR] OANDA API attempt {retry+1}/{max_retries} failed for {symbol}. Code: {e.code}, Msg: {e.msg}")
            else:
                 logger.warning(f"[ATR] OANDA API attempt {retry+1}/{max_retries} failed for {symbol}: {str(e)}")

            if retry < max_retries - 1:
                wait_time = retry_delay * (2 ** retry) # Exponential backoff
                logger.info(f"[ATR] Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"[ATR] OANDA API fetch failed after {max_retries} attempts for {symbol}.")
                oanda_candles = None # Ensure it's None if all retries fail

    # --- ATR Calculation using TA library ---
    calculated_atr = None
    if oanda_candles and len(oanda_candles) >= period + 1:
        try:
            candles_to_use = oanda_candles[-(period + 1):]
            highs = [float(c["mid"]["h"]) for c in candles_to_use]
            lows = [float(c["mid"]["l"]) for c in candles_to_use]
            closes = [float(c["mid"]["c"]) for c in candles_to_use]

            df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
            atr_indicator = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period)
            atr_series = atr_indicator.average_true_range()

            if not atr_series.empty and not pd.isna(atr_series.iloc[-1]):
                calculated_atr = float(atr_series.iloc[-1])
                if calculated_atr > 0:
                    logger.info(f"[ATR] Successfully computed ATR from OANDA data for {symbol}: {calculated_atr:.5f}")
                    return calculated_atr
                else:
                     logger.warning(f"[ATR] Calculated ATR from OANDA data is zero or negative for {symbol}")
                     calculated_atr = None
            else:
                 logger.warning(f"[ATR] Calculated ATR from OANDA data is None or NaN for {symbol}")
                 calculated_atr = None

        except Exception as e:
            logger.error(f"[ATR] Error calculating ATR from OANDA data for {symbol}: {str(e)}")
            calculated_atr = None

    # --- Fallback 1: Use get_historical_data ---
    if calculated_atr is None:
        logger.warning(f"[ATR] OANDA API/calculation failed. Attempting fallback: get_historical_data.")
        try:
            # Ensure get_historical_data is async and handles errors
            fallback_data = await get_historical_data(symbol, oanda_granularity, period + 10)
            fb_candles = fallback_data.get("candles", [])
            fb_candles = [c for c in fb_candles if c.get("complete", True)]

            if len(fb_candles) >= period + 1:
                candles_to_use = fb_candles[-(period + 1):]
                highs = [float(c["mid"]["h"]) for c in candles_to_use]
                lows = [float(c["mid"]["l"]) for c in candles_to_use]
                closes = [float(c["mid"]["c"]) for c in candles_to_use]

                df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
                atr_indicator = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period)
                atr_series = atr_indicator.average_true_range()

                if not atr_series.empty and not pd.isna(atr_series.iloc[-1]):
                    calculated_atr = float(atr_series.iloc[-1])
                    if calculated_atr > 0:
                        logger.info(f"[ATR] Successfully computed ATR from fallback data for {symbol}: {calculated_atr:.5f}")
                        return calculated_atr
                    else:
                        logger.warning(f"[ATR] Calculated ATR from fallback data is zero or negative for {symbol}")
                        calculated_atr = None
                else:
                    logger.warning(f"[ATR] Calculated ATR from fallback data is None or NaN for {symbol}")
                    calculated_atr = None
            else:
                logger.warning(f"[ATR] Fallback data insufficient for {symbol}: {len(fb_candles)} candles < {period + 1}")

        except Exception as fallback_error:
             # Log specific error from get_historical_data if possible
            logger.error(f"[ATR] Fallback get_historical_data failed for {symbol}: {str(fallback_error)}")
            # Also check the logs for [OANDA] Error fetching candles within get_historical_data
            # Example: If get_historical_data had the same await issue, it would log there.
            calculated_atr = None


    # --- Fallback 2: Use Static Default Values ---
    if calculated_atr is None:
        logger.warning(f"[ATR] All calculation methods failed for {symbol}. Using static default ATR.")
        default_atr_values = {
            "FOREX":   {"M1": 0.0005, "M5": 0.0007, "M15": 0.0010, "M30": 0.0015, "H1": 0.0025, "H4": 0.0050, "D": 0.0100},
            "CRYPTO":  {"M1": 0.0010, "M5": 0.0015, "M15": 0.0020, "M30": 0.0030, "H1": 0.0050, "H4": 0.0100, "D": 0.0200}, # Relative %
            "COMMODITY": {"M1": 0.05, "M5": 0.07, "M15": 0.10, "M30": 0.15, "H1": 0.25, "H4": 0.50, "D": 1.00},
            "INDICES": {"M1": 0.50, "M5": 0.70, "M15": 1.00, "M30": 1.50, "H1": 2.50, "H4": 5.00, "D": 10.00},
            "XAU_USD": {"M1": 0.05, "M5": 0.07, "M15": 0.10, "M30": 0.15, "H1": 0.25, "H4": 0.50, "D": 1.00}
        }
        try:
            instrument_type = get_instrument_type(symbol) # Ensure this function is corrected
            if symbol in default_atr_values:
                type_defaults = default_atr_values[symbol]
            elif instrument_type in default_atr_values:
                 type_defaults = default_atr_values[instrument_type]
            else:
                 logger.warning(f"[ATR] Unknown instrument type '{instrument_type}' for static defaults, using FOREX.")
                 type_defaults = default_atr_values["FOREX"]

            static_atr = type_defaults.get(oanda_granularity, type_defaults.get("H1", 0.0025))

            if instrument_type == "CRYPTO":
                try:
                    current_price = await get_current_price(symbol, "BUY")
                    if current_price > 0:
                        static_atr = current_price * static_atr
                        logger.info(f"[ATR] Calculated absolute static ATR for crypto {symbol} using price {current_price}: {static_atr:.5f}")
                    else:
                        logger.error(f"[ATR] Could not get positive price for crypto {symbol}. Cannot calculate static ATR.")
                        return 0.0
                except Exception as price_err:
                    logger.error(f"[ATR] Could not get price for crypto static ATR calc for {symbol}: {price_err}. Returning 0.")
                    return 0.0
            else:
                 logger.info(f"[ATR] Using static default ATR for {symbol} ({instrument_type}, {oanda_granularity}): {static_atr:.5f}")

            return static_atr

        except Exception as static_fallback_error:
            logger.error(f"[ATR] Error during static fallback for {symbol}: {str(static_fallback_error)}")
            logger.warning("[ATR] Using ultimate fallback ATR value: 0.0025")
            return 0.0025

    logger.error(f"[ATR] Failed to determine ATR for {symbol} through all methods.")
    return 0.0


async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
    async with self._lock:
        alert_id_from_data = alert_data.get("id", alert_data.get("request_id"))
        alert_id = alert_id_from_data if alert_id_from_data else str(uuid.uuid4())
        logger_instance = get_module_logger(
            __name__, 
            symbol=alert_data.get("symbol", "UNKNOWN"), 
            request_id=alert_id
        )

        try:
            symbol = alert_data.get("symbol", "")
            action = alert_data.get("action", "").upper()
            
            if alert_id in self.active_alerts:
                logger_instance.warning(f"Duplicate alert ignored: {alert_id}")
                return {"status": "ignored", "message": "Duplicate alert", "alert_id": alert_id}
            self.active_alerts.add(alert_id)
            
            if self.system_monitor:
                await self.system_monitor.update_component_status("alert_handler", "processing", f"Processing alert for {symbol} {action}")
                
            try:
                if action in ["BUY", "SELL"]:
                    instrument = alert_data.get("instrument", symbol)
                    # request_id is already alert_id
                    timeframe = alert_data.get("timeframe", "H1")
                    comment_from_alert = alert_data.get("comment")
                    # Use 'risk_percent' from alert_data, which should be mapped from TV's 'percentage'
                    risk_percent_from_alert = float(alert_data.get('risk_percent', 1.0)) 

                    standardized_instrument = standardize_symbol(instrument)
                    if not standardized_instrument:
                        logger_instance.error(f"Failed to standardize instrument: {instrument}")
                        return {"status": "rejected", "message": f"Failed to standardize instrument: {instrument}", "alert_id": alert_id}

                    tradeable, reason = is_instrument_tradeable(standardized_instrument)
                    logger_instance.info(f"Instrument {standardized_instrument} tradeable: {tradeable}, Reason: {reason}")
                    
                    if not tradeable:
                        logger_instance.warning(f"Market check failed: {reason}")
                        return {"status": "rejected", "message": f"Trading not allowed: {reason}", "alert_id": alert_id}
                        
                    payload_for_execute_trade = {
                        "symbol": standardized_instrument,
                        "action": action,
                        "risk_percent": risk_percent_from_alert, # Pass the correct risk percent
                        "timeframe": timeframe,
                        "comment": comment_from_alert, # Pass the comment
                        "account": alert_data.get("account"),
                        "request_id": alert_id 
                        # DO NOT pass 'units' or 'entry_price' here. Let execute_oanda_order handle them.
                    }
                    
                    success, result_dict = await execute_trade(payload_for_execute_trade)
                    return result_dict # This is the dict part from execute_trade's tuple return
                    
                elif action in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]:
                    return await self._process_exit_alert(alert_data)
                    
                elif action == "UPDATE":
                    return await self._process_update_alert(alert_data)
                    
                else:
                    logger_instance.warning(f"Unknown action type: {action}")
                    return {"status": "error", "message": f"Unknown action type: {action}", "alert_id": alert_id}
                    
            finally:
                self.active_alerts.discard(alert_id)
                if self.system_monitor:
                    await self.system_monitor.update_component_status("alert_handler", "ok", "")
                
        except Exception as e:
            logger_instance.error(f"Error processing alert: {str(e)}", exc_info=True)
            if hasattr(self, 'error_recovery') and self.error_recovery:
                await self.error_recovery.record_error("alert_processing", {"error": str(e), "alert": alert_data})
            return {"status": "error", "message": f"Internal error processing alert: {str(e)}", "alert_id": alert_data.get("id", "unknown_id_on_error")}

def db_retry(max_retries=3, retry_delay=2):
    """Decorator to add retry logic to database operations"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except asyncpg.exceptions.PostgresConnectionError as e:
                    retries += 1
                    logger.warning(f"Database connection error in {func.__name__}, retry {retries}/{max_retries}: {str(e)}")
                    
                    if retries >= max_retries:
                        logger.error(f"Max database retries reached for {func.__name__}")
                        raise
                        
                    # Exponential backoff
                    wait_time = retry_delay * (2 ** (retries - 1))
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Database error in {func.__name__}: {str(e)}")
                    raise
        return wrapper
    return decorator


##############################################################################
# Database Models
##############################################################################

class PostgresDatabaseManager:
    def __init__(
        self,
        db_url: str = config.database_url,
        min_connections: int = config.db_min_connections,
        max_connections: int = config.db_max_connections,
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
    async def update_position(
        self, position_id: str, updates: Dict[str, Any]
    ) -> bool:
        """Update position in database"""
        try:
            # Process updates to ensure compatibility with PostgreSQL
            updates = (
                updates.copy()
            )  # Create a copy to avoid modifying the original

            # Convert metadata to JSON if it exists and is a dict
            if "metadata" in updates and isinstance(updates["metadata"], dict):
                updates["metadata"] = json.dumps(updates["metadata"])

            # Convert datetime strings to datetime objects if needed
            for field in ["open_time", "close_time", "last_update"]:
                if field in updates and isinstance(updates[field], str):
                    try:
                        updates[field] = datetime.fromisoformat(
                            updates[field].replace('Z', '+00:00')
                        )
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
                            position_data["metadata"] = json.loads(
                                position_data["metadata"]
                            )
                    except json.JSONDecodeError:
                        # If parsing fails, keep as string
                        pass

                # Convert timestamp objects to ISO format strings for consistency
                for field in ["open_time", "close_time", "last_update"]:
                    if position_data.get(field) and isinstance(
                        position_data[field], datetime
                    ):
                        position_data[field] = position_data[field].isoformat()

                return position_data

        except Exception as e:
            self.logger.error(
                f"Error getting position from database: {str(e)}"
            )
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
                    if (
                        "metadata" in position_data
                        and position_data["metadata"]
                    ):
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(
                                    position_data["metadata"]
                                )
                        except json.JSONDecodeError:
                            # If parsing fails, keep as string
                            pass

                    # Convert timestamp objects to ISO format strings
                    for field in ["open_time", "close_time", "last_update"]:
                        if position_data.get(field) and isinstance(
                            position_data[field], datetime
                        ):
                            position_data[field] = position_data[
                                field
                            ].isoformat()

                    positions.append(position_data)

                return positions

        except Exception as e:
            self.logger.error(
                f"Error getting open positions from database: {str(e)}"
            )
            return []

    async def get_closed_positions(
        self, limit: int = 100
    ) -> List[Dict[str, Any]]:
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
                    # Convert row to dictionary
                    position_data = dict(row)

                    # Parse metadata JSON if it exists
                    if (
                        "metadata" in position_data
                        and position_data["metadata"]
                    ):
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(
                                    position_data["metadata"]
                                )
                        except json.JSONDecodeError:
                            # If parsing fails, keep as string
                            pass

                    # Convert timestamp objects to ISO format strings
                    for field in ["open_time", "close_time", "last_update"]:
                        if position_data.get(field) and isinstance(
                            position_data[field], datetime
                        ):
                            position_data[field] = position_data[
                                field
                            ].isoformat()

                    positions.append(position_data)

                return positions

        except Exception as e:
            self.logger.error(
                f"Error getting closed positions from database: {str(e)}"
            )
            return []

    async def delete_position(self, position_id: str) -> bool:
        """Delete position from database"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM positions WHERE position_id = $1", position_id
                )
                return True

        except Exception as e:
            self.logger.error(
                f"Error deleting position from database: {str(e)}"
            )
            return False

    async def get_positions_by_symbol(
        self, symbol: str, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
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
                    if (
                        "metadata" in position_data
                        and position_data["metadata"]
                    ):
                        try:
                            if isinstance(position_data["metadata"], str):
                                position_data["metadata"] = json.loads(
                                    position_data["metadata"]
                                )
                        except json.JSONDecodeError:
                            # If parsing fails, keep as string
                            pass

                    # Convert timestamp objects to ISO format strings
                    for field in ["open_time", "close_time", "last_update"]:
                        if position_data.get(field) and isinstance(
                            position_data[field], datetime
                        ):
                            position_data[field] = position_data[
                                field
                            ].isoformat()

                    positions.append(position_data)

                return positions

        except Exception as e:
            self.logger.error(
                f"Error getting positions by symbol from database: {str(e)}"
            )
            return []


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

def async_error_handler(max_retries=3, delay=RETRY_DELAY):  # Changed default from MAX_RETRY_ATTEMPTS to 3
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

from datetime import datetime, timezone
from typing import Dict, Any
import asyncio
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ErrorRecoverySystem:
    """
    Comprehensive error handling and recovery system that monitors
    for stalled operations and recovers from system failures.
    """

    def __init__(self):
        """Initialize error recovery system"""
        self.stale_position_threshold = 900  # seconds
        self.daily_error_count = 0
        self.last_error_reset = datetime.now(timezone.utc)

    async def check_for_stale_positions(self):
        """Check for positions that haven't been updated recently."""
        try:
            # Your logic here
            pass  # Placeholder for actual implementation
        except Exception as e:
            logger.error(f"Error checking for stale positions: {str(e)}")

            # Optionally record the error
            error_type = "UnknownError"
            details = {}
            await self.record_error(error_type, details)
            logger.error(f"Error recorded: {error_type} - {json.dumps(details)}")

    async def recover_position(self, position_id: str, position_data: Dict[str, Any]):
        """Attempt to recover a stale position."""
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
        """Increment error counter and optionally log or store the error details."""
        self.daily_error_count += 1
        logger.error(f"Error recorded: {error_type} - {json.dumps(details)}")

    async def schedule_stale_position_check(self, interval_seconds: int = 60):
        """Schedule regular checks for stale positions."""
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


def is_position_open(pos: dict) -> bool:
    return pos.get("status") == "OPEN" and not pos.get("closed")

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
        return 0.

async def execute_oanda_order(
    instrument: str,
    direction: str,
    risk_percent: float,
    comment: Optional[str] = None,  # <<< THIS LINE IS CRUCIAL
    entry_price: Optional[float] = None,
    take_profit: Optional[float] = None,
    timeframe: str = 'H1',
    atr_multiplier: float = 1.5,
    units: Optional[float] = None,
    _retry_count: int = 0,
    **kwargs
) -> dict:
    """
    Places a market order on OANDA with dynamic equity allocation based on TradingView risk signal
    (constrained by safety limits), calculated TP, and handles TAKE_PROFIT_ON_FILL_LOSS 
    errors with retries. Stop Loss is intentionally disabled (set to None).
    """
    # Create a contextual logger
    request_id = str(uuid.uuid4())
    logger = get_module_logger(__name__, symbol=instrument, request_id=request_id)
    logger.info(f"[execute_oanda_order] Received parameters - instrument: '{instrument}', direction: '{direction}', risk_percent: {risk_percent}, comment: '{comment}', timeframe: '{timeframe}', entry_price: {entry_price}, units: {units}")

    # DEBUGGING: Log OANDA credentials being used
    logger.info(f"OANDA execution using account: {OANDA_ACCOUNT_ID}, environment: {OANDA_ENVIRONMENT}")

    try:
        # 1. Standardize Instrument & Basic Setup
        instrument_standard = standardize_symbol(instrument)
        if not instrument_standard:
             logger.error(f"Failed to standardize instrument: {instrument}")
             return {"success": False, "error": "Failed to standardize instrument"}

        account_id = OANDA_ACCOUNT_ID
        oanda_inst = instrument_standard.replace('/', '_') # Format for OANDA API
        dir_mult = -1 if direction.upper() == 'SELL' else 1
        logger.info(f"Standardized instrument: {oanda_inst} for input {instrument}")

        # Determine instrument type early for later use
        instrument_type = get_instrument_type(instrument_standard)

        # 2. Determine Entry Price if not provided
        if entry_price is None:
            try:
                logger.info(f"Fetching current price for {oanda_inst} as entry_price was not provided.")
                entry_price, price_source = await get_price_with_fallbacks(oanda_inst, direction)
                logger.info(f"Using {price_source} price for {oanda_inst}: {entry_price}")
            except ValueError as e:
                logger.error(f"Failed to get price for {oanda_inst}: {str(e)}")
                return {"success": False, "error": f"Failed to get price: {str(e)}"}
                
            if not isinstance(entry_price, (float, int)) or entry_price <= 0:
                logger.error(f"Entry price is not valid after fetch/check: {entry_price} (Type: {type(entry_price)})")
                return {"success": False, "error": "Invalid entry price obtained or not positive"}
        elif not isinstance(entry_price, (float, int)) or entry_price <= 0:
             logger.error(f"Provided entry price is not valid or not positive: {entry_price} (Type: {type(entry_price)})")
             return {"success": False, "error": "Invalid provided entry price or not positive"}

        # 3. Fetch Account Balance
        try:
            logger.info(f"Fetching account balance for account {account_id}")
            balance = await get_account_balance() 
            if not isinstance(balance, (float, int)) or balance <= 0:
                 raise ValueError(f"Invalid balance received: {balance}")
            logger.info(f"Account balance: {balance}")
        except Exception as e:
            logger.error(f"Failed to get account balance: {str(e)}", exc_info=True)
            return {"success": False, "error": f"Failed to get account balance: {str(e)}"}

        # 4. Calculate Dynamic Equity Allocation Based on TradingView Risk Signal
        def get_dynamic_equity_allocation(instrument_type_local: str, tv_risk_percent: float) -> tuple[float, str]:
            """
            Calculate equity allocation based on TradingView risk percentage with safety limits.
            Returns: (equity_percentage_decimal, reason_string)
            """
            
            # Define safety limits by instrument type (ADJUST THESE AS PER YOUR STRATEGY)
            risk_limits = {
                "CRYPTO":    {"min": 2.0, "max": 10.0, "default": 7.0},  # Example: 2-10% for Crypto, default 7%
                "FOREX":     {"min": 5.0, "max": 20.0, "default": 15.0}, # Example: 5-20% for Forex, default 15%
                "COMMODITY": {"min": 3.0, "max": 12.0, "default": 8.0},  # Example: 3-12% for Commodities, default 8%
                "INDICES":   {"min": 5.0, "max": 15.0, "default": 10.0}  # Example: 5-15% for Indices, default 10%
            }
            
            limits = risk_limits.get(instrument_type_local.upper(), risk_limits["FOREX"]) # Fallback to FOREX
            
            final_percentage_value = 0.0
            reason = ""

            if tv_risk_percent <= 0:
                final_percentage_value = limits["default"]
                reason = f"TradingView risk signal was {tv_risk_percent}%, using default {final_percentage_value}% for {instrument_type_local}"
            elif tv_risk_percent < limits["min"]:
                final_percentage_value = limits["min"]
                reason = f"TradingView risk signal {tv_risk_percent}% was below min {limits['min']}%, using min {final_percentage_value}% for {instrument_type_local}"
            elif tv_risk_percent > limits["max"]:
                final_percentage_value = limits["max"]
                reason = f"TradingView risk signal {tv_risk_percent}% was above max {limits['max']}%, using max {final_percentage_value}% for {instrument_type_local}"
            else:
                final_percentage_value = tv_risk_percent
                reason = f"Using TradingView risk signal: {final_percentage_value}% for {instrument_type_local}"
            
            return final_percentage_value / 100.0, reason

        # Apply dynamic equity allocation
        # 'risk_percent' is the argument passed to execute_oanda_order, originating from the TradingView alert
        equity_percentage_decimal, allocation_reason_str = get_dynamic_equity_allocation(instrument_type, risk_percent)

        logger.info(f"DYNAMIC EQUITY ALLOCATION for {instrument_standard}:")
        logger.info(f"  • Instrument Type: {instrument_type}")
        logger.info(f"  • TradingView Risk Signal Received: {risk_percent}%")
        logger.info(f"  • Allocation Logic: {allocation_reason_str}")
        logger.info(f"  • Final Equity Percentage Applied: {equity_percentage_decimal*100:.2f}%")

        def apply_market_condition_multiplier(equity_percentage: float, comment: str, timeframe: str) -> tuple[float, str]:
            """Apply additional multipliers based on market conditions from TradingView"""
            
            multiplier = 1.0
            multiplier_reasons = []
            
            # Adjust based on signal strength keywords in comment
            comment_lower = comment.lower() if comment else ""
            
            if any(word in comment_lower for word in ["strong", "high confidence", "breakout"]):
                multiplier *= 1.15  # +15% for strong signals
                multiplier_reasons.append("+15% for strong signal")
            elif any(word in comment_lower for word in ["weak", "low confidence", "uncertain"]):
                multiplier *= 0.85  # -15% for weak signals  
                multiplier_reasons.append("-15% for weak signal")
            
            # Adjust based on timeframe (longer timeframes = higher confidence)
            if timeframe in ["H4", "D1"]:
                multiplier *= 1.1   # +10% for longer timeframes
                multiplier_reasons.append("+10% for longer timeframe")
            elif timeframe in ["M1", "M5"]:
                multiplier *= 0.9   # -10% for shorter timeframes
                multiplier_reasons.append("-10% for shorter timeframe")
            
            # Apply session-based adjustments
            current_hour = datetime.now(timezone.utc).hour
            if 13 <= current_hour <= 16:  # London-NY overlap (high liquidity)
                multiplier *= 1.05  # +5% during high liquidity
                multiplier_reasons.append("+5% for high liquidity session")
            elif current_hour < 6 or current_hour > 21:  # Low liquidity hours
                multiplier *= 0.95  # -5% during low liquidity
                multiplier_reasons.append("-5% for low liquidity session")
            
            # Cap the total multiplier between 0.5x and 1.5x for safety
            multiplier = max(0.5, min(1.5, multiplier))
            
            adjusted_percentage = equity_percentage * multiplier
            
            # Create reason string
            if multiplier_reasons:
                reason = f"Applied multipliers: {', '.join(multiplier_reasons)} (total: {multiplier:.2f}x)"
            else:
                reason = "No market condition adjustments applied"
            
            return adjusted_percentage, reason
        
        # Apply market condition multipliers (optional - you need the comment and timeframe from payload)
        if comment or timeframe:  # Only if we have additional context
            original_equity_percentage = equity_percentage  # Store original for comparison
            equity_percentage, multiplier_reason = apply_market_condition_multiplier(
                equity_percentage, comment, timeframe
            )
            logger.info(f"  • {multiplier_reason}")
            if abs(equity_percentage - original_equity_percentage) > 0.001:  # If changed
                logger.info(f"  • Adjusted from {original_equity_percentage*100:.1f}% to {equity_percentage*100:.1f}%")

        equity_amount = balance * equity_percentage_decimal
        
        logger.info(f"Executing order: {direction} {oanda_inst} with equity allocation: {equity_amount:.2f} ({equity_percentage_decimal*100:.2f}% of {balance:.2f})")

        # 5. Calculate Take Profit (Corrected Logic)
        calculated_tp = None 
        if take_profit is None: 
            if instrument_type == "CRYPTO": tp_percent = 0.03 # 3% TP for Crypto
            elif instrument_type == "COMMODITY": tp_percent = 0.02 # 2% TP for Commodity
            else: tp_percent = 0.01 # 1% TP for Forex and others

            tp_distance = entry_price * tp_percent
            if direction.upper() == 'BUY':
                calculated_tp = entry_price + tp_distance
            else: 
                calculated_tp = entry_price - tp_distance 
            logger.info(f"Calculated take profit: {calculated_tp} (using {tp_percent*100:.1f}% fixed percentage of entry price)")
            take_profit = calculated_tp 
        else:
            logger.info(f"Using provided take profit: {take_profit}")

        # 6. Calculate Position Size (Units)
        final_units = 0.0 # Initialize as float
        if units is None:
            # Ensure INSTRUMENT_LEVERAGES uses "XXX_YYY" format for keys
            leverage = INSTRUMENT_LEVERAGES.get(instrument_standard, INSTRUMENT_LEVERAGES.get('default', 20))
            logger.info(f"Using leverage: {leverage}:1 for {instrument_standard} (from INSTRUMENT_LEVERAGES dict)")

            if entry_price <= 0:
                 logger.error(f"Cannot calculate size: Invalid entry price {entry_price}")
                 return {"success": False, "error": "Invalid entry price for size calculation"}

            if instrument_type in ["CRYPTO", "COMMODITY"]:
                size = (equity_amount / entry_price) * leverage
                logger.info(f"Calculated initial size for {instrument_type}: {size:.8f} units")
                
                crypto_symbol = None
                for symbol_key_lookup in CRYPTO_MIN_SIZES.keys():
                    if symbol_key_lookup in instrument_standard:
                        crypto_symbol = symbol_key_lookup
                        break
                
                # Ensure CRYPTO_MIN_SIZES uses correct values for your broker
                min_size = CRYPTO_MIN_SIZES.get(crypto_symbol, 0.0001) if crypto_symbol else (0.2 if instrument_type == "COMMODITY" else 0.0001)
                if size < min_size:
                    logger.warning(f"Size {size:.8f} below minimum {min_size:.8f} for {instrument_standard}, adjusting to minimum.")
                    size = min_size
                
                min_notional = 10.0 # Example: $10 minimum notional value
                trade_notional = size * entry_price
                if trade_notional < min_notional:
                    logger.warning(f"Trade notional value ${trade_notional:.2f} (for {size:.8f} units) is below minimum ${min_notional:.2f}. Adjusting size.")
                    size = (min_notional / entry_price) * 1.01 # Add 1% buffer
                    logger.info(f"Adjusted size to {size:.8f} to meet minimum notional value.")
                
                # Ensure CRYPTO_TICK_SIZES["BTC"] is 0.001 or similar for desired precision
                if crypto_symbol and crypto_symbol in CRYPTO_TICK_SIZES:
                    tick_size = CRYPTO_TICK_SIZES.get(crypto_symbol, 0.01) # Default to 0.01 if not found (check this default)
                    precision = len(str(tick_size).split('.')[-1]) if '.' in str(tick_size) else 0
                    logger.info(f"For {crypto_symbol}, using tick_size: {tick_size}, calculated precision: {precision}")
                    size = round(size, precision)
                    logger.info(f"Size after precision rounding for {crypto_symbol}: {size:.8f}")
                elif instrument_type == "COMMODITY":
                    precision = CRYPTO_TICK_SIZES.get(instrument_standard, {}).get("precision", 2) # Example for commodities
                    size = round(size, precision)
                    logger.info(f"Size after precision rounding for {instrument_type}: {size:.8f}")
                
                final_units = size * dir_mult
            else: # Forex
                # Forex sizing: (equity_amount_in_account_currency / price_of_one_unit_of_base_in_account_currency) * leverage
                # Assuming balance & equity_amount are in the account currency (e.g. USD)
                # If trading EUR_USD, entry_price is USD per EUR.
                # Units of base currency (EUR) = (equity_amount_USD / entry_price_USD_per_EUR) * leverage
                size = (equity_amount / entry_price) * leverage # This calculates units of the base currency
                logger.info(f"Calculated initial size for Forex (base units): {size:.4f} units")
                final_units = int(round(size)) * dir_mult # OANDA typically wants whole units for Forex
            
            logger.info(f"Final calculated trade size: {abs(final_units)} units for {oanda_inst} (direction: {direction})")
        else: # units were provided directly
            if instrument_type in ["CRYPTO", "COMMODITY"]:
                final_units = float(abs(units)) * dir_mult
            else:
                final_units = int(round(abs(units))) * dir_mult
            logger.info(f"Using provided units: {final_units} for {oanda_inst}")

        # 7. Guard against Zero-Unit Orders
        if abs(final_units) < (CRYPTO_MIN_SIZES.get(crypto_symbol, 0.00001) if instrument_type == "CRYPTO" else 1e-8) : # More robust check for effectively zero
            logger.warning(f"[OANDA] Not sending order for {oanda_inst}: calculated units {final_units} are effectively zero or below minimum.")
            return {"success": False, "error": "Calculated units are effectively zero or too small"}

        # 8. Pre-check margin requirements
        try:
            price_for_margin_check = entry_price 
            leverage_for_margin_check = INSTRUMENT_LEVERAGES.get(instrument_standard, INSTRUMENT_LEVERAGES.get('default', 20)) # Use the same leverage source as for sizing
            
            account_summary = await get_account_summary(account_id)
            if not account_summary or "account" not in account_summary or "marginAvailable" not in account_summary["account"]:
                raise ValueError("Invalid account summary received for margin check")
            available_margin = float(account_summary["account"]["marginAvailable"])
            
            margin_ratio = 1 / leverage_for_margin_check 
            estimated_margin = (price_for_margin_check * abs(final_units) * margin_ratio) * 1.1  # Add 10% buffer
            
            logger.info(f"Margin pre-check: Units={abs(final_units)}, Price={price_for_margin_check}, LeverageUsedInCalc={leverage_for_margin_check}, MarginRatio={margin_ratio:.4f}")

            if available_margin < estimated_margin:
                logger.warning(f"Pre-check: Available margin ({available_margin:.2f}) likely insufficient for trade ({estimated_margin:.2f}) for {oanda_inst}")
                return {"success": False, "error": f"Pre-check: Insufficient margin available ({available_margin:.2f}). Estimated need: {estimated_margin:.2f}"}
            else:
                logger.info(f"Pre-check: Margin appears sufficient. Available: {available_margin:.2f}, Estimated need: {estimated_margin:.2f}")
        except Exception as margin_e:
            logger.warning(f"Could not perform margin pre-check: {str(margin_e)}. Proceeding with caution.")

        # 9. Build Market Order Payload
        order_payload_dict = {
            "type": "MARKET",
            "instrument": oanda_inst,
            "timeInForce": "FOK",
            "positionFill": "DEFAULT"
        }
        
        if instrument_type in ["CRYPTO", "COMMODITY"]:
            # Determine precision for payload based on tick size for consistency
            payload_precision_crypto_symbol = None
            for sym_key_fmt in CRYPTO_TICK_SIZES.keys():
                if sym_key_fmt in instrument_standard:
                    payload_precision_crypto_symbol = sym_key_fmt
                    break
            
            if payload_precision_crypto_symbol and payload_precision_crypto_symbol in CRYPTO_TICK_SIZES:
                payload_tick_size = CRYPTO_TICK_SIZES.get(payload_precision_crypto_symbol, 0.01)
                payload_precision = len(str(payload_tick_size).split('.')[-1]) if '.' in str(payload_tick_size) else 0
                order_payload_dict["units"] = f"{final_units:.{payload_precision}f}"
            else: 
                 order_payload_dict["units"] = f"{final_units:.8f}" # Default to 8 decimal places
            logger.info(f"Order payload units for {instrument_type} {oanda_inst}: {order_payload_dict['units']}")
        else: # Forex
            order_payload_dict["units"] = str(int(final_units))
            logger.info(f"Order payload units for Forex {oanda_inst}: {order_payload_dict['units']}")


        if take_profit is not None and isinstance(take_profit, (float, int)) and take_profit > 0:
            tp_price_precision = 5 
            if 'JPY' in oanda_inst: tp_price_precision = 3
            elif instrument_type == "CRYPTO": 
                tp_price_precision = CRYPTO_TICK_SIZES.get(oanda_inst.split('_')[0], {}).get("tp_precision", 2) # Example, refine this
                if oanda_inst == "BTC_USD": tp_price_precision = 1 
            elif instrument_type == "COMMODITY": 
                tp_price_precision = CRYPTO_TICK_SIZES.get(oanda_inst.split('_')[0], {}).get("tp_precision", 2)

            is_tp_valid = (direction.upper() == 'BUY' and take_profit > entry_price) or \
                          (direction.upper() == 'SELL' and take_profit < entry_price)
            if not is_tp_valid:
                 logger.warning(f"Take Profit ({take_profit}) seems invalid relative to Entry ({entry_price}) for {direction}. Proceeding but OANDA might reject.")

            order_payload_dict["takeProfitOnFill"] = {
                "price": f"{take_profit:.{tp_price_precision}f}",
                "timeInForce": "GTC"
            }
        elif take_profit is not None:
             logger.warning(f"Take profit value provided but invalid ({take_profit}), omitting from order.")

        final_order_payload = {"order": order_payload_dict}

        # 10. Log Payload & Send Order Request
        logger.info(f"OANDA order payload: {json.dumps(final_order_payload)}")
        order_request_obj = OrderCreate(accountID=account_id, data=final_order_payload)

        try:
            logger.info(f"Sending order to OANDA API for {oanda_inst}")
            response = await robust_oanda_request(order_request_obj)
            logger.info(f"OANDA API response: {json.dumps(response)}")

            if "orderFillTransaction" in response:
                tx = response["orderFillTransaction"]
                filled_price = float(tx.get('price', entry_price)) 
                
                filled_units_str = tx.get('units', str(final_units))
                try:
                    if instrument_type in ["CRYPTO", "COMMODITY"]:
                        filled_units_resp = float(filled_units_str)
                    else:
                        filled_units_resp = int(float(filled_units_str))
                except ValueError:
                    logger.error(f"Could not parse filled_units from response: {filled_units_str}")
                    filled_units_resp = final_units 

                logger.info(f"Order successfully executed: Order ID {tx.get('id', 'N/A')}, Trade ID {tx.get('tradeOpened', {}).get('tradeID', 'N/A')}")
                return {
                    "success": True,
                    "order_id": tx.get('id'),
                    "trade_id": tx.get('tradeOpened', {}).get('tradeID'),
                    "instrument": oanda_inst,
                    "direction": direction,
                    "entry_price": filled_price,
                    "units": filled_units_resp, # Use units from response
                    "take_profit": take_profit 
                }
            # ... (rest of the retry and error handling logic from your previous full version) ...
            elif "orderCancelTransaction" in response:
                cancel_reason = response["orderCancelTransaction"].get("reason", "UNKNOWN")
                logger.error(f"OANDA order canceled: {cancel_reason}. Full response: {json.dumps(response)}")

                if cancel_reason == "TAKE_PROFIT_ON_FILL_LOSS":
                    max_retries_tp = 2 
                    if _retry_count >= max_retries_tp:
                        logger.error(f"Max retries ({_retry_count + 1}) reached for TAKE_PROFIT_ON_FILL_LOSS adjustment.")
                        logger.warning("Attempting order without Take Profit as final fallback.")
                        
                        final_order_data_no_tp = final_order_payload.copy() 
                        if "takeProfitOnFill" in final_order_data_no_tp["order"]:
                            del final_order_data_no_tp["order"]["takeProfitOnFill"]
                        logger.info(f"Final fallback order payload (no TP): {json.dumps(final_order_data_no_tp)}")
                        final_order_request_no_tp = OrderCreate(accountID=account_id, data=final_order_data_no_tp)
                        try:
                            final_response = await robust_oanda_request(final_order_request_no_tp)
                            logger.info(f"Final fallback OANDA response: {json.dumps(final_response)}")
                            if "orderFillTransaction" in final_response:
                                tx_final = final_response["orderFillTransaction"]
                                logger.info(f"Final fallback order executed successfully: Order ID {tx_final.get('id', 'N/A')}, Trade ID {tx_final.get('tradeOpened', {}).get('tradeID', 'N/A')}")
                                
                                filled_units_str_final = tx_final.get('units', str(final_units))
                                try:
                                    if instrument_type in ["CRYPTO", "COMMODITY"]:
                                        filled_units_final_resp = float(filled_units_str_final)
                                    else:
                                        filled_units_final_resp = int(float(filled_units_str_final))
                                except ValueError:
                                    filled_units_final_resp = final_units

                                return {
                                    "success": True, 
                                    "order_id": tx_final.get('id'),
                                    "trade_id": tx_final.get('tradeOpened', {}).get('tradeID'),
                                    "instrument": oanda_inst,
                                    "direction": direction, 
                                    "entry_price": float(tx_final.get('price', entry_price)),
                                    "units": filled_units_final_resp,
                                    "take_profit": None 
                                }
                            else:
                                cancel_reason_final = final_response.get("orderCancelTransaction", {}).get("reason", "UNKNOWN_FINAL_FALLBACK")
                                logger.error(f"Final fallback order also failed. Reason: {cancel_reason_final}. Response: {json.dumps(final_response)}")
                                return {"success": False, "error": f"Final fallback order failed: {cancel_reason_final}", "details": final_response}
                        except Exception as final_e:
                            logger.error(f"Exception during final fallback order attempt: {str(final_e)}", exc_info=True)
                            return {"success": False, "error": f"Exception in final fallback order: {str(final_e)}"}
                    else: 
                        logger.warning(f"TAKE_PROFIT_ON_FILL_LOSS occurred. Retry attempt {_retry_count + 1}/{max_retries_tp + 1}.")
                        try:
                            current_atr_value = await get_atr(instrument_standard, timeframe) 
                            if not isinstance(current_atr_value, (float, int)) or current_atr_value <= 0:
                                 raise ValueError("Invalid ATR received for TP retry adjustment")
                        except Exception as atr_err:
                            current_atr_value = entry_price * 0.005 
                            logger.warning(f"Failed to get ATR for retry ({atr_err}), using fallback adjustment base for TP: {current_atr_value}")

                        tp_precision_retry = 5 # Default
                        if 'JPY' in oanda_inst: tp_precision_retry = 3
                        elif instrument_type == "CRYPTO": tp_precision_retry = CRYPTO_TICK_SIZES.get(oanda_inst.split('_')[0], {}).get("tp_precision", 2)
                        
                        tp_adjustment = current_atr_value * (1.0 + _retry_count * 0.5) 
                        
                        if not isinstance(take_profit, (float, int)): # Ensure original take_profit was valid
                            logger.error(f"Cannot adjust take_profit as it's not a valid number: {take_profit}. Aborting TP retry.")
                            return {"success": False, "error": f"Order canceled: {cancel_reason} (TP adjustment failed due to invalid original TP)", "details": response}

                        new_take_profit = take_profit + (tp_adjustment * dir_mult) 

                        logger.warning(f"Retrying with wider take profit: {new_take_profit:.{tp_precision_retry}f} (Adjustment: {tp_adjustment * dir_mult:.{tp_precision_retry}f})")
                        
                        return await execute_oanda_order(
                            instrument=instrument, 
                            direction=direction,
                            risk_percent=risk_percent, 
                            entry_price=entry_price, 
                            take_profit=new_take_profit, 
                            timeframe=timeframe,
                            units=abs(final_units), 
                            _retry_count=_retry_count + 1, 
                            **kwargs
                        )
                else: 
                    return {"success": False, "error": f"Order canceled by OANDA: {cancel_reason}", "details": response}
            else:
                reject_reason = response.get("orderRejectTransaction", {}).get("reason", response.get("errorMessage", "UNKNOWN_REJECTION"))
                logger.error(f"Order failed or rejected: Reason: {reject_reason}. Response: {json.dumps(response)}")
                return {"success": False, "error": f"Order failed/rejected: {reject_reason}", "details": response}

        except oandapyV20.exceptions.V20Error as api_err:
            logger.error(f"OANDA API error during order placement: Code {api_err.code}, Msg: {api_err.msg}", exc_info=True)
            error_msg = f"OANDA API Error ({api_err.code}): {api_err.msg}"
            details_from_err = str(api_err)
            try:
                if hasattr(api_err, 'response') and api_err.response is not None: 
                    details_from_err = api_err.response.json() if hasattr(api_err.response, 'json') else api_err.response.text
            except Exception: 
                pass 
            return {"success": False, "error": error_msg, "details": details_from_err }
        except Exception as e_req:
            logger.error(f"Unexpected error during order placement request: {str(e_req)}", exc_info=True)
            return {"success": False, "error": f"Unexpected error during order placement: {str(e_req)}"}

    except Exception as outer_e:
        logger.error(f"[execute_oanda_order] Pre-execution setup error: {str(outer_e)}", exc_info=True)
        return {"success": False, "error": f"Pre-execution setup error: {str(outer_e)}"}

async def set_oanda_take_profit(
    trade_id: str,
    account_id: str,
    take_profit_price: float,
    instrument: str
) -> dict:
    """
    Set take profit for a trade using a direct request to OANDA API.
    This bypasses the oandapyV20 library endpoints that may be causing issues.
    """
    request_id = str(uuid.uuid4())
    logger = get_module_logger(__name__, symbol=instrument, request_id=request_id)
    
    try:
        # Determine precision for price formatting
        instrument_type = get_instrument_type(instrument)
        precision = 3 if 'JPY' in instrument else 5
        if instrument_type == "CRYPTO":
            precision = 2
        elif instrument_type == "COMMODITY" and 'XAU' in instrument:
            precision = 2
            
        # Format take profit price
        formatted_tp = f"{take_profit_price:.{precision}f}"
        
        # Construct API URL (depends on environment)
        base_url = "https://api-fxpractice.oanda.com" if OANDA_ENVIRONMENT == "practice" else "https://api-fxtrade.oanda.com"
        endpoint = f"/v3/accounts/{account_id}/trades/{trade_id}/orders"
        
        # Prepare headers
        headers = {
            "Authorization": f"Bearer {OANDA_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Prepare request body
        data = {
            "takeProfit": {
                "price": formatted_tp,
                "timeInForce": "GTC"
            }
        }
        
        logger.info(f"[{request_id}] Setting take profit for trade {trade_id} to {formatted_tp}")
        
        # Get or create a session
        session = await get_session()
        
        # Send the request
        async with session.put(f"{base_url}{endpoint}", headers=headers, json=data) as response:
            response_json = await response.json()
            
            if response.status == 200 or response.status == 201:
                logger.info(f"[{request_id}] Successfully set take profit: {json.dumps(response_json)}")
                return {
                    "success": True,
                    "trade_id": trade_id,
                    "take_profit": take_profit_price,
                    "message": "Take profit set successfully",
                    "response": response_json
                }
            else:
                logger.error(f"[{request_id}] Failed to set take profit: {json.dumps(response_json)}")
                return {
                    "success": False,
                    "trade_id": trade_id,
                    "error": f"API error (status {response.status}): {json.dumps(response_json)}",
                    "response": response_json
                }
    
    except Exception as e:
        logger.error(f"[{request_id}] Error setting take profit: {str(e)}", exc_info=True)
        return {
            "success": False,
            "trade_id": trade_id,
            "error": f"Exception: {str(e)}"
        }

async def set_take_profit_after_execution(
    trade_id: str,
    instrument: str,
    direction: str,
    entry_price: float,
    position_id: str = None,
    timeframe: str = 'H1'
) -> dict:
    """Set take profit levels after a position has been executed."""
    request_id = str(uuid.uuid4())
    logger = get_module_logger(__name__, symbol=instrument, request_id=request_id)
    
    try:
        # Standardize instrument
        instrument = standardize_symbol(instrument)
        
        # Get ATR for TP calculations
        atr_value = await get_atr(instrument, timeframe)
        if atr_value <= 0:
            logger.warning(f"[{request_id}] Invalid ATR value for {instrument}: {atr_value}, using default")
            # Use a default percentage of price for TP
            atr_value = entry_price * 0.005  # 0.5% of price as fallback
        
        logger.info(f"[{request_id}] Using ATR value for {instrument}: {atr_value}")
        
        # Get instrument type for precision and TP calculations
        instrument_type = get_instrument_type(instrument)
        
        # Calculate TP level based on instrument type
        if instrument_type == "CRYPTO":
            tp_percent = 0.03  # 3% for crypto
        elif instrument_type == "COMMODITY":
            tp_percent = 0.02  # 2% for commodities
        else:
            tp_percent = 0.01  # 1% for forex and others
        
        # Calculate take profit price
        if direction.upper() == 'BUY':
            take_profit = entry_price * (1 + tp_percent)
        else:  # SELL
            take_profit = entry_price * (1 - tp_percent)
        
        logger.info(f"[{request_id}] Calculated take profit for {instrument}: {take_profit} ({tp_percent*100}%)")
        
        # Set take profit using direct API call
        result = await set_oanda_take_profit(
            trade_id=trade_id,
            account_id=OANDA_ACCOUNT_ID,
            take_profit_price=take_profit,
            instrument=instrument
        )
        
        # Add position_id to result for caller's use
        result["position_id"] = position_id
        
        return result
        
    except Exception as e:
        logger.error(f"[{request_id}] Error setting take profit: {str(e)}", exc_info=True)
        return {
            "success": False,
            "trade_id": trade_id,
            "position_id": position_id,
            "error": f"Failed to set take profit: {str(e)}"
        }
        

async def get_price_with_fallbacks(symbol: str, direction: str) -> Tuple[float, str]:
    """
    Get current price with multi-level fallbacks
    
    Args:
        symbol: Trading symbol (e.g., "EUR_USD")
        direction: "BUY" or "SELL" to determine ask/bid price
    
    Returns:
        Tuple of (price, source) where source indicates which method provided the price
    
    Raises:
        ValueError: If all price-fetching methods fail
    """
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting price for {symbol} ({direction})")
    
    # Method 1: Try PricingInfo endpoint (preferred method)
    try:
        params = {"instruments": symbol}
        price_request = PricingInfo(accountID=OANDA_ACCOUNT_ID, params=params)
        
        price_response = await robust_oanda_request(price_request)
        
        if "prices" in price_response and len(price_response["prices"]) > 0:
            prices_data = price_response['prices'][0]
            if direction.upper() == 'BUY':
                if prices_data.get('asks') and len(prices_data['asks']) > 0 and prices_data['asks'][0].get('price'):
                    price = float(prices_data['asks'][0]['price'])
                    logger.info(f"[{request_id}] Got price via PricingInfo: {price}")
                    return price, "pricing_api"
            else:  # SELL
                if prices_data.get('bids') and len(prices_data['bids']) > 0 and prices_data['bids'][0].get('price'):
                    price = float(prices_data['bids'][0]['price'])
                    logger.info(f"[{request_id}] Got price via PricingInfo: {price}")
                    return price, "pricing_api"
                    
        logger.warning(f"[{request_id}] PricingInfo response did not contain valid price data")
        
    except BrokerConnectionError as e:
        logger.warning(f"[{request_id}] Primary price source failed: {str(e)}")
    except Exception as e:
        logger.warning(f"[{request_id}] Error with primary price source: {str(e)}")
        
    # Method 2: Fallback to recent candles
    try:
        logger.info(f"[{request_id}] Attempting candle fallback for {symbol}")
        candle_data = await get_historical_data(symbol, 'M1', 3)  # Get 3 recent one-minute candles
        
        if candle_data and candle_data.get('candles') and len(candle_data['candles']) > 0:
            # Use the latest complete candle
            complete_candles = [c for c in candle_data['candles'] if c.get('complete', True)]
            
            if complete_candles:
                latest_candle = complete_candles[-1]
                if latest_candle.get('mid') and latest_candle['mid'].get('c'):
                    price = float(latest_candle['mid']['c'])
                    logger.info(f"[{request_id}] Got price via candle fallback: {price}")
                    return price, "candle_data"
                    
        logger.warning(f"[{request_id}] Candle fallback failed: no valid candle data")
        
    except Exception as e:
        logger.warning(f"[{request_id}] Candle fallback failed: {str(e)}")
    
    # Method 3: Fallback to simulated price for known instruments
    try:
        logger.info(f"[{request_id}] Attempting simulation fallback for {symbol}")
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
    # Base prices for common symbols
    base_prices = {
        "EUR_USD": 1.10,
        "GBP_USD": 1.25,
        "USD_JPY": 110.0,
        "GBP_JPY": 175.0,
        "XAU_USD": 1900.0,
        "BTC_USD": 75000.0,
        "ETH_USD": 3500.0
    }
    
    # Get base price or use default
    base_price = base_prices.get(symbol, 100.0)
    
    # Add small random variation
    variation = random.uniform(-0.001, 0.001)
    price = base_price * (1 + variation)
    
    # Apply bid/ask spread
    spread_factor = 1.0005 if direction.upper() == "BUY" else 0.9995
    return price * spread_factor
    

async def get_current_price(symbol: str, side: str = "BUY") -> float:
    """Get current price for a symbol with robust error handling"""
    try:
        # Standardize symbol first
        symbol = standardize_symbol(symbol)
        
        # Use the new function with fallbacks
        price, source = await get_price_with_fallbacks(symbol, side)
        return price
    except ValueError as e:
        logger.error(f"Error getting price for {symbol}: {str(e)}")
        # Return a reasonable default price to avoid breaking the system
        return 1.0 if "USD" in symbol else 100.0
        

async def check_position_momentum(position_id: str) -> bool:
    """Check if a position has strong momentum in its direction."""
    try:
        # Get position info
        position = await position_tracker.get_position_info(position_id)
        if not position:
            return False
            
        symbol = position["symbol"]
        direction = position["action"]
        timeframe = position["timeframe"]
        entry_price = position["entry_price"]
        current_price = position["current_price"]
        
        # Initialize momentum score
        score = 0
        decision_factors = {
            "htf_aligned": False,
            "regime_aligned": False,
            "price_aligned": False,
            "volatility_aligned": False,
            "price_gain_pct": 0,
            "price_strength": 0
        }
        
        # Create a request ID for logging
        request_id = str(uuid.uuid4())
        
        # 1. CHECK HIGHER TIMEFRAME TREND (CRITICAL REQUIREMENT)
        # This is now our primary filter - if higher timeframe doesn't align, don't override
        htf_aligned = await check_higher_timeframe_trend(symbol, direction, timeframe)
        decision_factors["htf_aligned"] = htf_aligned
        
        if not htf_aligned:
            # Higher timeframe trend doesn't align, don't override close signal
            logger.info(f"[{request_id}] Higher timeframe trend doesn't align with {position_id} {direction}, honoring close signal")
            return False
        
        # 2. CHECK REGIME CLASSIFICATION
        # This is secondary but still valuable
        regime_aligned = False
        if hasattr(alert_handler, "regime_classifier"):
            regime_data = alert_handler.regime_classifier.get_regime_data(symbol)
            regime = regime_data.get("regime", "unknown")
            
            # Check if regime aligns with position direction
            if ((direction == "BUY" and regime in ["trending_up", "momentum_up"]) or 
                (direction == "SELL" and regime in ["trending_down", "momentum_down"])):
                score += 1
                regime_aligned = True
                decision_factors["regime_aligned"] = True
        
        # 3. CHECK PRICE PERFORMANCE (PRIMARY INDICATOR)
        # Get ATR for this symbol/timeframe for volatility-adjusted measurement
        atr_value = await get_atr(symbol, timeframe)
        
        # Define timeframe-specific thresholds
        momentum_thresholds = {
            "M1": {"multiplier": 0.5, "min_score": 1},
            "M5": {"multiplier": 0.5, "min_score": 1},
            "M15": {"multiplier": 0.75, "min_score": 1},
            "M30": {"multiplier": 0.75, "min_score": 2},
            "H1": {"multiplier": 1.0, "min_score": 2},
            "H4": {"multiplier": 1.25, "min_score": 2},
            "D1": {"multiplier": 1.5, "min_score": 2},
            "W1": {"multiplier": 2.0, "min_score": 2},
            "default": {"multiplier": 1.0, "min_score": 2}
        }
        
        # Get appropriate thresholds for this timeframe
        thresholds = momentum_thresholds.get(timeframe, momentum_thresholds["default"])
        
        # Calculate price gain percentage
        if direction == "BUY":
            price_gain_pct = (current_price / entry_price - 1) * 100
        else:  # SELL
            price_gain_pct = (1 - current_price / entry_price) * 100
            
        decision_factors["price_gain_pct"] = price_gain_pct
            
        # Calculate ATR as percentage of entry price
        atr_percent = (atr_value / entry_price) * 100
        
        # Calculate price strength relative to volatility
        price_strength = price_gain_pct / atr_percent if atr_percent and atr_percent > 0.0001 else 0
        decision_factors["price_strength"] = price_strength
        
        # Check if gain exceeds ATR threshold
        if price_gain_pct > atr_percent * thresholds["multiplier"]:
            score += 1
            decision_factors["price_aligned"] = True
        
        # 4. CHECK VOLATILITY STATE (SUPPORTING INDICATOR)
        vol_aligned = False
        if hasattr(alert_handler, "volatility_monitor"):
            vol_data = alert_handler.volatility_monitor.get_volatility_state(symbol)
            vol_state = vol_data.get("volatility_state", "normal")
            
            # If market is in high volatility and position is profitable
            if vol_state == "high" and price_gain_pct > 0:
                score += 1
                vol_aligned = True
                decision_factors["volatility_aligned"] = True
        
        # 5. MAKE FINAL DECISION
        # Get minimum required score based on timeframe
        min_score_required = thresholds["min_score"]
        
        # Log comprehensive decision metrics
        logger.info(f"[{request_id}] Position {position_id} momentum analysis: "
                   f"HTF={htf_aligned}, Regime={regime_aligned}, "
                   f"Price={decision_factors['price_aligned']}, Volatility={vol_aligned}, "
                   f"Gain={price_gain_pct:.2f}%, PriceStrength={price_strength:.2f}x ATR, "
                   f"Score={score}/{min_score_required}")
        
        # Return result based on score threshold
        override_decision = score >= min_score_required
        
        # Add enhanced override logging with detailed reasoning
        if override_decision:
            logger.info(f"[{request_id}] Override reasons for {position_id}: HTF={htf_aligned}, " 
                      f"Regime={regime_aligned}, Price={decision_factors['price_aligned']}, "
                      f"Volatility={vol_aligned}, Gain={price_gain_pct:.2f}%, "
                      f"PriceStrength={price_strength:.2f}x ATR")
        
        logger.info(f"[{request_id}] Final override decision for {position_id}: {override_decision}")
        return override_decision

        return score >= min_score_required
        
    except Exception as e:
        logger.error(f"Error checking position momentum: {str(e)}")
        return False
        

def get_higher_timeframe(timeframe: str) -> str:
    """Get the next higher timeframe based on current timeframe."""
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
    
    # Normalize the timeframe format if needed
    normalized_tf = normalize_timeframe(timeframe)
    
    # Return the next higher timeframe, or the same if it's already at the top
    return timeframe_hierarchy.get(normalized_tf, normalized_tf)

async def check_higher_timeframe_trend(symbol: str, direction: str, timeframe: str) -> bool:
    """Check if higher timeframe trend aligns with position direction."""
    try:
        # Get higher timeframe
        higher_tf = get_higher_timeframe(timeframe)
        
        # If already at highest timeframe, use same timeframe
        if higher_tf == timeframe:
            return True  # Can't check higher, assume aligned
            
        # Adjust MA periods based on the timeframe relationship
        fast_period = 20 if timeframe in ["M1", "M5", "M15"] else 50
        slow_period = 50 if timeframe in ["M1", "M5", "M15"] else 200
        
        # Get enough historical data for the higher timeframe
        historical_data = await get_historical_data(symbol, higher_tf, slow_period + 10)
        
        if not historical_data:
            logger.warning(f"No historical data returned for {symbol} on {higher_tf} timeframe")
            return False
            
        if "candles" not in historical_data:
            logger.warning(f"No candles in historical data for {symbol} on {higher_tf} timeframe")
            return False
            
        candles = historical_data["candles"]
        if not candles or len(candles) < slow_period:
            logger.warning(f"Insufficient candle data for {symbol} on {higher_tf} timeframe: {len(candles) if candles else 0} < {slow_period}")
            return False
            
        # Calculate moving averages
        candles = historical_data["candles"]
        closes = [float(c["mid"]["c"]) for c in candles]
        
        # Simple moving average calculation
        ma_fast = sum(closes[-fast_period:]) / min(fast_period, len(closes))
        ma_slow = sum(closes[-slow_period:]) / min(slow_period, len(closes))
        
        # Calculate percentage difference between MAs to measure trend strength
        ma_percent_diff = abs(ma_fast - ma_slow) / ma_slow * 100
        min_percent_diff = 0.5 if timeframe in ["M1", "M5", "M15"] else 0.2
        
        strong_trend = ma_percent_diff >= min_percent_diff
        
        # Check alignment with position
        if direction == "BUY" and ma_fast > ma_slow and strong_trend:
            logger.info(f"Higher timeframe {higher_tf} trend aligned with BUY position (MA diff: {ma_percent_diff:.2f}%)")
            return True
        elif direction == "SELL" and ma_fast < ma_slow and strong_trend:
            logger.info(f"Higher timeframe {higher_tf} trend aligned with SELL position (MA diff: {ma_percent_diff:.2f}%)")
            return True
            
        logger.info(f"Higher timeframe {higher_tf} trend NOT aligned with {direction} position (MA diff: {ma_percent_diff:.2f}%)")
        return False
        
    except Exception as e:
        logger.error(f"Error checking higher timeframe trend: {str(e)}")
        return False  # Conservative approach on error

def get_instrument_type(instrument: str) -> str:
    """
    Determine instrument type from symbol.
    Returns one of: 'FOREX', 'CRYPTO', 'COMMODITY', 'INDICES'.
    """
    try:
        # Handle None or empty input
        if not instrument:
            logger.warning("Empty instrument provided, defaulting to FOREX")
            return "FOREX"
            
        inst = instrument.upper()
        
        # Define comprehensive lists for identification
        crypto_list = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']
        commodity_list = ['XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BCO', 'NATGAS', 'OIL']
        index_list = ['SPX', 'NAS', 'US30', 'UK100', 'DE30', 'JP225', 'AUS200', 'DAX']

        # Check for underscore format (e.g., EUR_USD, BTC_USD)
        if '_' in inst:
            parts = inst.split('_')
            if len(parts) == 2:
                base, quote = parts
                
                # Check Crypto (Base only, e.g., BTC_USD)
                if base in crypto_list:
                    return "CRYPTO"
                    
                # Check Commodity (Base only, e.g., XAU_USD)
                if base in commodity_list:
                    return "COMMODITY"
                    
                # Check Index (Base only, e.g., US30_USD)
                if base in index_list:
                    return "INDICES"
                    
                # Check Forex (standard 3-letter codes)
                if len(base) == 3 and len(quote) == 3 and base.isalpha() and quote.isalpha():
                    # Exclude if base is a commodity (e.g., XAU_CAD) - should be COMMODITY
                    if base not in commodity_list:
                        return "FOREX"
                    else:
                        return "COMMODITY"  # e.g., XAU_EUR is a commodity trade
        
        # Handle no-underscore format
        else:
            # Check Crypto (e.g., BTCUSD, ETHUSD)
            for crypto in crypto_list:
                if inst.startswith(crypto):
                    # Basic check: Starts with crypto and has common quote
                    if any(inst.endswith(q) for q in ["USD", "EUR", "USDT", "GBP", "JPY"]):
                        return "CRYPTO"
            
            # Check Commodity (e.g., XAUUSD, WTICOUSD)
            for comm in commodity_list:
                if inst.startswith(comm):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                        return "COMMODITY"
            
            # Check Index (e.g., US30USD, NAS100USD)
            for index in index_list:
                if inst.startswith(index):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                        return "INDICES"
            
            # Check standard 6-char Forex (e.g., EURUSD)
            if len(inst) == 6 and inst.isalpha():
                # Additional check for commodity pairs without underscore
                for comm in commodity_list:
                    if inst.startswith(comm):
                        return "COMMODITY"
                return "FOREX"

        # Default if no specific type matched
        logger.warning(f"Could not determine specific instrument type for '{instrument}', defaulting to FOREX.")
        return "FOREX"
        
    except Exception as e:
        logger.error(f"Error determining instrument type for '{instrument}': {str(e)}")
        return "FOREX"  # Default fallback

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable based on market hours"""
    now = datetime.now(timezone.utc)
    instrument_type = get_instrument_type(symbol)
    
    # Special handling for JPY pairs
    if "JPY" in symbol:
        instrument_type = "jpy_pair"
    
    if instrument_type in ["forex", "jpy_pair", "metal"]:
        if now.weekday() >= 5:
            return False, "Weekend - Market closed"
        if now.weekday() == 4 and now.hour >= 21:
            return False, "Weekend - Market closed"
        if now.weekday() == 0 and now.hour < 21:
            return False, "Market not yet open"
        return True, "Market open"
    
    if instrument_type == "index":
        if "SPX" in symbol or "NAS" in symbol:
            if now.weekday() >= 5:
                return False, "Weekend - Market closed"
            if not (13 <= now.hour < 20):
                return False, "Outside market hours"
        return True, "Market open"
    
    return True, "Market assumed open"


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
                    # Market condition check with detailed logging
                    instrument = alert_data.get("instrument", symbol)
                    request_id = alert_data.get("request_id", str(uuid.uuid4()))
                    timeframe = alert_data.get("timeframe", "H1")
                    
                    tradeable, reason = is_instrument_tradeable(instrument)
                    logger.info(f"[{request_id}] Instrument {instrument} tradeable: {tradeable}, Reason: {reason}")
                    
                    if not tradeable:
                        logger.warning(f"[{request_id}] Market check failed: {reason}")
                        return {
                            "status": "rejected",
                            "message": f"Trading not allowed: {reason}",
                            "alert_id": alert_id
                        }
                        
                    # Get market data
                    current_price = await get_current_price(instrument, action)
                    atr = await get_atr(instrument, timeframe)
                    
                    stop_price = None

                    try:
                        instrument_type = get_instrument_type(instrument)
                        atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
                        
                        if action == 'BUY':
                            stop_price = current_price - (atr * atr_multiplier)
                        else:
                            stop_price = current_price + (atr * atr_multiplier)
                        logger.info(f"[{request_id}] Using ATR-based stop loss: {stop_price} (ATR: {atr}, multiplier: {atr_multiplier})")
                    except Exception as e:
                        logger.error(f"[{request_id}] Error calculating stop loss: {str(e)}")

                        stop_price = current_price * (0.95 if action == 'BUY' else 1.05)
                    
                    # PRIORITY 2: If no suitable structure level found, use ATR-based stop
                    if not stop_price:
                        instrument_type = get_instrument_type(instrument)
                        atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
                        
                        if action == 'BUY':
                            stop_price = current_price - (atr * atr_multiplier)
                        else:
                            stop_price = current_price + (atr * atr_multiplier)
                        logger.info(f"[{request_id}] Using ATR-based stop loss: {stop_price} (ATR: {atr}, multiplier: {atr_multiplier})")

                    dir_mult = -1 if action.upper() == 'SELL' else 1
                    
                    # Define minimum distance based on instrument type
                    min_distance = 0.01  # 100 pips for forex
                    if instrument_is_commodity(instrument):
                        min_distance = 0.01  # 100 pips for commodities
                    elif 'BTC' in instrument or 'ETH' in instrument or get_instrument_type(instrument) == "CRYPTO":
                        min_distance = current_price * 0.10  # 10% for crypto
                    

                    min_distance = min_distance * 2.0

                    
                    current_distance = abs(current_price - stop_price)
                    if current_distance < min_distance:

                        old_stop = stop_price
                        stop_price = current_price - dir_mult * min_distance
                        logger.warning(f"[{request_id}] Adjusted stop loss from {old_stop} to {stop_price} to meet minimum distance requirement ({min_distance})")
                    
                    # Calculate account balance for position sizing
                    try:
                        account_balance = await get_account_balance()
                        logger.info(f"[{request_id}] Account balance: {account_balance}")
                    except Exception as e:
                        logger.error(f"[{request_id}] Error getting account balance: {str(e)}")
                        account_balance = 10000.0  # Default fallback
                    
                    # Calculate position size using PURE-STATE method
                    try:
                        units, precision = await calculate_pure_position_size(
                            instrument, float(alert_data.get('risk_percent', 1.0)), account_balance, action
                        )
                        logger.info(f"[{request_id}] Calculated position size: {units} units")
                    except Exception as e:
                        logger.error(f"[{request_id}] Error calculating position size: {str(e)}")
                        return {
                            "status": "error",
                            "message": f"Position size calculation failed: {str(e)}",
                            "alert_id": alert_id
                        }
                    
                    # Execute trade with calculated units
                    standardized_symbol = standardize_symbol(instrument)
                    success, result = await execute_trade({
                        "symbol": standardized_symbol,
                        "action": action,
                        "entry_price": current_price,
                        "timeframe": timeframe,
                        "account": alert_data.get("account"),
                        "units": units  # Pass the calculated units
                    })
                    
                    return result
                    
                elif action in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]:
                    # Handle close action
                    return await self._process_exit_alert(alert_data)
                    
                elif action == "UPDATE":
                    # Handle update action
                    return await self._process_update_alert(alert_data)
                    
                else:
                    logger.warning(f"Unknown action type: {action}")
                    return {
                        "status": "error",
                        "message": f"Unknown action type: {action}",
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
    """
    Process an entry alert (BUY or SELL) with comprehensive error handling.
    
    Note: Stop losses are intentionally disabled in this implementation.
    """
    request_id = str(uuid.uuid4())
    
    try:
        # Extract fields with validation
        if not alert_data:
            logger.error(f"[{request_id}] Empty alert data received")
            return {
                "status": "rejected",
                "message": "Empty alert data",
                "alert_id": request_id
            }
            
        alert_id = alert_data.get("id", request_id)
        symbol = alert_data.get("symbol", "")
        action = alert_data.get("action", "").upper()
        percentage = float(alert_data.get("percentage", 1.0))
        timeframe = alert_data.get("timeframe", "H1")
        comment = alert_data.get("comment", "")
        
        # Validate essential fields
        if not symbol:
            logger.error(f"[{request_id}] Missing required field: symbol")
            return {
                "status": "rejected",
                "message": "Missing required field: symbol",
                "alert_id": alert_id
            }
            
        if not action:
            logger.error(f"[{request_id}] Missing required field: action")
            return {
                "status": "rejected",
                "message": "Missing required field: action",
                "alert_id": alert_id
            }
            
        if action not in ["BUY", "SELL"]:
            logger.error(f"[{request_id}] Invalid action for entry alert: {action}")
            return {
                "status": "rejected",
                "message": f"Invalid action for entry: {action}. Must be BUY or SELL",
                "alert_id": alert_id
            }
        
        logger.info(f"[{request_id}] Processing entry alert: {symbol} {action} ({percentage}%)")
        
        # Standardize symbol
        standardized_symbol = standardize_symbol(symbol)
        logger.info(f"[{request_id}] Standardized symbol: {standardized_symbol}")
        
        # Check if trading is allowed
        is_tradeable, reason = is_instrument_tradeable(standardized_symbol)
        if not is_tradeable:
            logger.warning(f"[{request_id}] Trading not allowed for {standardized_symbol}: {reason}")
            return {
                "status": "rejected",
                "message": f"Trading not allowed: {reason}",
                "alert_id": alert_id
            }
            
        # Calculate position parameters
        position_id = f"{standardized_symbol}_{action}_{uuid.uuid4().hex[:8]}"
        
        try:
            # Get account balance
            account_balance = await get_account_balance()
            
            # Update risk manager balance
            if self.risk_manager:
                await self.risk_manager.update_account_balance(account_balance)
                logger.info(f"[{request_id}] Updated risk manager with balance: {account_balance}")
        except Exception as e:
            logger.error(f"[{request_id}] Error getting account balance: {str(e)}")
            return {
                "status": "error",
                "message": f"Error getting account balance: {str(e)}",
                "alert_id": alert_id
            }
        
        # Calculate risk
        risk_percentage = min(percentage / 100, config.max_risk_percentage / 100)
        
        # Check if risk is allowed
        if self.risk_manager:
            try:
                is_allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage, standardized_symbol)
                if not is_allowed:
                    logger.warning(f"[{request_id}] Trade rejected due to risk limits: {reason}")
                    return {
                        "status": "rejected",
                        "message": f"Risk check failed: {reason}",
                        "alert_id": alert_id
                    }
            except Exception as e:
                logger.error(f"[{request_id}] Error in risk check: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error in risk check: {str(e)}",
                    "alert_id": alert_id
                }
        
        # Get current price
        try:
            price = alert_data.get("price")
            if price is None:
                price = await get_current_price(standardized_symbol, action)
                logger.info(f"[{request_id}] Got current price for {standardized_symbol}: {price}")
            else:
                price = float(price)
                logger.info(f"[{request_id}] Using provided price for {standardized_symbol}: {price}")
        except Exception as e:
            logger.error(f"[{request_id}] Error getting price for {standardized_symbol}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error getting price: {str(e)}",
                "alert_id": alert_id
            }
                
        # Get ATR for later use with take profit calculations
        try:
            atr_value = await get_atr(standardized_symbol, timeframe)
            if atr_value <= 0:
                logger.warning(f"[{request_id}] Invalid ATR value for {standardized_symbol}: {atr_value}")
                atr_value = 0.0025  # Default fallback value
            
            logger.info(f"[{request_id}] ATR for {standardized_symbol}: {atr_value}")
            
            # Calculate instrument type and volatility multiplier for later use
            instrument_type = get_instrument_type(standardized_symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            # Apply volatility adjustment if available
            volatility_multiplier = 1.0
            if self.volatility_monitor:
                volatility_multiplier = self.volatility_monitor.get_stop_loss_modifier(standardized_symbol)
                logger.info(f"[{request_id}] Volatility multiplier: {volatility_multiplier}")
            
        except Exception as e:
            logger.error(f"[{request_id}] Error calculating ATR: {str(e)}")
            atr_value = 0.0025  # Default value
            volatility_multiplier = 1.0
            instrument_type = get_instrument_type(standardized_symbol)
            atr_multiplier = 1.5  # Default multiplier
        
        # Calculate position size using percentage-based sizing
        try:
            # Percentage-based sizing without stop loss dependency
            position_size = account_balance * percentage / 100 / price
            logger.info(f"[{request_id}] Calculated position size: {position_size}")
                
        except Exception as e:
            logger.error(f"[{request_id}] Error calculating position size: {str(e)}")
            return {
                "status": "error",
                "message": f"Error calculating position size: {str(e)}",
                "alert_id": alert_id
            }
        
        # Execute trade with broker
        try:
            success, trade_result = await execute_trade({
                "symbol": standardized_symbol,
                "action": action,
                "percentage": percentage,
                "price": price,
                "stop_loss": None  # Explicitly set stop_loss to None as it's disabled
            })
            
            if not success:
                error_message = trade_result.get('error', 'Unknown error')
                logger.error(f"[{request_id}] Failed to execute trade: {error_message}")
                return {
                    "status": "error",
                    "message": f"Trade execution failed: {error_message}",
                    "alert_id": alert_id
                }
                
            logger.info(f"[{request_id}] Trade executed successfully: {json.dumps(trade_result)}")
                
        except Exception as e:
            logger.error(f"[{request_id}] Error executing trade: {str(e)}")
            return {
                "status": "error",
                "message": f"Error executing trade: {str(e)}",
                "alert_id": alert_id
            }
        
        # Record position in tracker
        try:
            if self.position_tracker:
                # Extract metadata
                metadata = {
                    "alert_id": alert_id,
                    "comment": comment,
                    "original_percentage": percentage,
                    "atr_value": atr_value,
                    "instrument_type": instrument_type,
                    "atr_multiplier": atr_multiplier
                }
                
                # Add any additional fields from alert
                for key, value in alert_data.items():
                    if key not in ["id", "symbol", "action", "percentage", "price", "comment", "timeframe"]:
                        metadata[key] = value
                        
                # Record position
                position_recorded = await self.position_tracker.record_position(
                    position_id=position_id,
                    symbol=standardized_symbol,
                    action=action,
                    timeframe=timeframe,
                    entry_price=price,
                    size=position_size,
                    stop_loss=None,  # Stop loss is disabled
                    take_profit=None,  # Will be set by exit manager
                    metadata=metadata
                )
                
                if not position_recorded:
                    logger.warning(f"[{request_id}] Failed to record position in tracker")
                else:
                    logger.info(f"[{request_id}] Position recorded in tracker: {position_id}")
            
        except Exception as e:
            logger.error(f"[{request_id}] Error recording position: {str(e)}")
            # Don't return error here - trade has already executed
        
        # Register with risk manager
        try:
            if self.risk_manager:
                await self.risk_manager.register_position(
                    position_id=position_id,
                    symbol=standardized_symbol,
                    action=action,
                    size=position_size,
                    entry_price=price,
                    stop_loss=None,  # Stop loss is disabled
                    account_risk=risk_percentage,
                    timeframe=timeframe
                )
                logger.info(f"[{request_id}] Position registered with risk manager")
        except Exception as e:
            logger.error(f"[{request_id}] Error registering with risk manager: {str(e)}")
            # Continue despite error
            
        # Initialize dynamic exits
        try:
            if self.dynamic_exit_manager:
                # Get market regime
                market_regime = "unknown"
                if self.regime_classifier:
                    regime_data = self.regime_classifier.get_regime_data(standardized_symbol)
                    market_regime = regime_data.get("regime", "unknown")
                    
                await self.dynamic_exit_manager.initialize_exits(
                    position_id=position_id,
                    symbol=standardized_symbol,
                    entry_price=price,
                    position_direction=action,
                    stop_loss=None,  # Stop loss is disabled
                    timeframe=timeframe
                )
                logger.info(f"[{request_id}] Dynamic exits initialized (market regime: {market_regime})")
        except Exception as e:
            logger.error(f"[{request_id}] Error initializing dynamic exits: {str(e)}")
            # Continue despite error
            
        # Record in position journal
        try:
            if self.position_journal:
                # Get market regime and volatility state
                market_regime = "unknown"
                volatility_state = "normal"
                
                if self.regime_classifier:
                    regime_data = self.regime_classifier.get_regime_data(standardized_symbol)
                    market_regime = regime_data.get("regime", "unknown")
                    
                if self.volatility_monitor:
                    vol_data = self.volatility_monitor.get_volatility_state(standardized_symbol)
                    volatility_state = vol_data.get("volatility_state", "normal")
                    
                await self.position_journal.record_entry(
                    position_id=position_id,
                    symbol=standardized_symbol,
                    action=action,
                    timeframe=timeframe,
                    entry_price=price,
                    size=position_size,
                    strategy="primary",
                    stop_loss=None,  # Stop loss is disabled
                    market_regime=market_regime,
                    volatility_state=volatility_state,
                    metadata=metadata if 'metadata' in locals() else None
                )
                logger.info(f"[{request_id}] Position recorded in journal")
        except Exception as e:
            logger.error(f"[{request_id}] Error recording in position journal: {str(e)}")
            # Continue despite error
            
        # Send notification
        try:
            if self.notification_system:
                await self.notification_system.send_notification(
                    f"New position opened: {action} {standardized_symbol} @ {price:.5f} (Risk: {risk_percentage*100:.1f}%)",
                    "info"
                )
                logger.info(f"[{request_id}] Position notification sent")
        except Exception as e:
            logger.error(f"[{request_id}] Error sending notification: {str(e)}")
            # Continue despite error
            
        logger.info(f"[{request_id}] Entry alert processing completed successfully")
            
        # Return successful result
        result = {
            "status": "success",
            "message": f"Position opened: {action} {standardized_symbol} @ {price}",
            "position_id": position_id,
            "symbol": standardized_symbol,
            "action": action,
            "price": price,
            "size": position_size,
            "stop_loss": None,  # Stop loss is disabled
            "alert_id": alert_id
        }
        
        # Merge with trade_result if available
        if isinstance(trade_result, dict):
            result.update({k: v for k, v in trade_result.items() if k not in result})
            
        return result
            
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled exception in entry alert processing: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": f"Internal error: {str(e)}",
            "alert_id": alert_data.get("id", "unknown")
        }

@async_error_handler()
async def get_account_balance() -> float:
    """Get current account balance from Oanda with robust error handling"""
    try:
        # Use the OANDA API directly for account summary
        from oandapyV20.endpoints.accounts import AccountSummary
        account_request = AccountSummary(accountID=OANDA_ACCOUNT_ID)
        
        # Use robust request wrapper
        response = await robust_oanda_request(account_request)
        
        if "account" in response and "NAV" in response["account"]:
            balance = float(response["account"]["NAV"])
            logger.info(f"Current account balance: {balance}")
            return balance
        
        # Try alternate fields
        if "account" in response and "balance" in response["account"]:
            balance = float(response["account"]["balance"])
            logger.info(f"Current account balance (from 'balance' field): {balance}")
            return balance
            
        # If we get here, couldn't find balance in response
        logger.error(f"Failed to extract balance from OANDA response: {response}")
        return 10000.0  # Default fallback
        
    except Exception as e:
        logger.error(f"Failed to get account balance: {str(e)}", exc_info=True)
        return 10000.0  # Default fallback

async def get_account_summary(account_id: str = OANDA_ACCOUNT_ID) -> dict:
    """Get account summary including margin information"""
    try:
        from oandapyV20.endpoints.accounts import AccountSummary
        request = AccountSummary(accountID=account_id)
        response = await robust_oanda_request(request)
        return response
    except Exception as e:
        logger.error(f"Error getting account summary: {str(e)}")
        raise
        
async def execute_trade(payload: dict) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade with the broker"""
    # request_id can be taken from payload if passed, or generated if needed
    request_id = payload.get("request_id", str(uuid.uuid4()))
    instrument = payload.get('instrument', payload.get('symbol', ''))
    # Create a contextual logger if get_module_logger is available at this scope
    # Otherwise, use the global logger or pass logger instance if refactoring
    current_logger = get_module_logger(__name__, symbol=instrument, request_id=request_id)

    try:
        direction = payload.get('direction', payload.get('action', '')).upper()
        risk_percent_received_in_payload = payload.get('risk_percent')
        risk_percent = float(payload.get('risk_percent', 1.0)) 
        timeframe = payload.get('timeframe', '1H')
        comment_from_payload = payload.get('comment') 

        current_logger.info(f"[execute_trade] Payload received: {json.dumps(payload)}")
        current_logger.info(f"[execute_trade] risk_percent_received_in_payload: {risk_percent_received_in_payload}, type: {type(risk_percent_received_in_payload)}")
        
        risk_percent = float(risk_percent_received_in_payload if risk_percent_received_in_payload is not None else 1.0)

        current_logger.info(f"[execute_trade] Final risk_percent to be used for execute_oanda_order: {risk_percent}")
        current_logger.info(f"Executing trade: {direction} {instrument} with {risk_percent:.2f}% risk. Comment: {comment_from_payload}")

        result = await execute_oanda_order(
            instrument=instrument,
            direction=direction,
            risk_percent=risk_percent, # This value is passed
            timeframe=timeframe,
            comment=comment_from_payload,
            entry_price=payload.get("price"),
            units=None
        )

        success = result.get("success", False)

        if success:
            current_logger.info(f"Trade executed successfully: {direction} {instrument}")
        else:
            current_logger.error(f"Failed to execute trade: {json.dumps(result)}")

        return success, result

    except Exception as e:
        current_logger.error(f"Error executing trade for {instrument}: {str(e)}", exc_info=True)
        return False, {"error": str(e), "instrument": instrument, "request_id": request_id}

async def close_position(position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close a position with the broker. Returns (success_bool, result_dict)"""
    position_id = position_data.get("position_id", "UNKNOWN_ID") # Get ID for logging
    symbol_from_payload = position_data.get("symbol", "")
    action_from_payload = position_data.get("action", "").upper() # Original action BUY/SELL

    request_id = str(uuid.uuid4()) # For correlating logs for this specific close operation
    log_context_short = f"[BROKER_CLOSE] PosID: {position_id}, Symbol: {symbol_from_payload}, ReqID: {request_id}"

    logger.info(f"{log_context_short} - Attempting to close position with broker. Payload received: {position_data}")

    if not symbol_from_payload:
        logger.error(f"{log_context_short} - Symbol not provided in position_data. Cannot close.")
        return False, {"error": "Symbol not provided for broker closure", "position_id": position_id, "request_id": request_id}

    standardized_symbol_for_broker = standardize_symbol(symbol_from_payload)
    if not standardized_symbol_for_broker:
        logger.error(f"{log_context_short} - Failed to standardize symbol '{symbol_from_payload}'. Cannot close.")
        return False, {"error": f"Failed to standardize symbol for broker closure: {symbol_from_payload}", "position_id": position_id, "request_id": request_id}

    logger.debug(f"{log_context_short} - Standardized symbol for OANDA: {standardized_symbol_for_broker}")

    try:
        if 'oanda' in globals() and OANDA_ACCOUNT_ID:
            from oandapyV20.endpoints.positions import PositionClose # Ensure this import is correct
            
            # Determine OANDA close payload based on the position's original action
            if action_from_payload == "BUY":
                oanda_close_data = {"longUnits": "ALL"}
            elif action_from_payload == "SELL":
                oanda_close_data = {"shortUnits": "ALL"}
            else:
                # If action isn't known, this is problematic for a targeted close.
                # Fetching current position details from OANDA first would be more robust
                # to determine if it's long or short, or if it even exists.
                logger.warning(f"{log_context_short} - Original action (BUY/SELL) not specified in position_data. Attempting a general 'ALL units' close. This might be imprecise or fail if no position exists.")
                # As a fallback, you could try to close ALL, but OANDA might prefer specific direction.
                # For now, let's assume this indicates an issue or a need to fetch details first.
                # A better approach might be to require 'action' in position_data for this function.
                # However, to match existing potential behavior:
                oanda_close_data = {"longUnits": "ALL", "shortUnits": "ALL"} # This will try to close any net position

            close_request_oanda = PositionClose(
                accountID=OANDA_ACCOUNT_ID,
                instrument=standardized_symbol_for_broker, # OANDA uses '_' in symbols like EUR_USD
                data=oanda_close_data
            )
            logger.info(f"{log_context_short} - Sending OANDA PositionClose request. Instrument: {standardized_symbol_for_broker}, Data: {oanda_close_data}")
            
            # Use robust_oanda_request, assuming it's defined and accessible
            broker_response = await robust_oanda_request(close_request_oanda)
            
            logger.info(f"{log_context_short} - OANDA PositionClose RAW response: {json.dumps(broker_response)}")

            # Extract actual exit price and confirm closure from broker_response
            # OANDA's PositionClose can result in multiple transactions.
            # We need to find the fill transaction(s) to get the price.
            actual_exit_price = None
            filled_units = 0
            transactions_in_response = []

            if "longOrderFillTransaction" in broker_response:
                tx = broker_response["longOrderFillTransaction"]
                transactions_in_response.append(tx)
                if tx.get("price"): actual_exit_price = float(tx["price"])
                if tx.get("units"): filled_units += abs(float(tx["units"])) # Assuming these are negative for closing a long
                logger.debug(f"{log_context_short} - Found longOrderFillTransaction. Price: {actual_exit_price}, Units closed: {tx.get('units')}")

            if "shortOrderFillTransaction" in broker_response:
                tx = broker_response["shortOrderFillTransaction"]
                transactions_in_response.append(tx)
                if tx.get("price"): actual_exit_price = float(tx["price"]) # OANDA might provide fill for one side
                if tx.get("units"): filled_units += abs(float(tx["units"])) # Assuming these are positive for closing a short
                logger.debug(f"{log_context_short} - Found shortOrderFillTransaction. Price: {actual_exit_price}, Units closed: {tx.get('units')}")
            
            # Sometimes a general orderFillTransaction if it's a simple market order to close
            if not transactions_in_response and "orderFillTransaction" in broker_response:
                tx = broker_response["orderFillTransaction"]
                transactions_in_response.append(tx)
                if tx.get("price"): actual_exit_price = float(tx["price"])
                if tx.get("units"): filled_units = abs(float(tx["units"]))
                logger.debug(f"{log_context_short} - Found general orderFillTransaction. Price: {actual_exit_price}, Units closed: {tx.get('units')}")


            if not transactions_in_response and ("longOrderCreateTransaction" in broker_response or "shortOrderCreateTransaction" in broker_response):
                 # This means an order to close was created, but we might not have the fill info immediately in *this* response.
                 # The fill might come in a separate transaction stream if using async order processing.
                 # For a simple blocking request, we usually expect a fill or rejection.
                 logger.warning(f"{log_context_short} - PositionClose created an order, but fill transaction not found directly in response. Closure might be pending or in a subsequent transaction. Response: {broker_response}")
                 # Fallback: fetch current price. This is NOT the actual exit price but a last resort.
                 price_fetch_side = "SELL" if action_from_payload == "BUY" else ("BUY" if action_from_payload == "SELL" else "SELL")
                 actual_exit_price = await get_current_price(standardized_symbol_for_broker, price_fetch_side)
                 logger.warning(f"{log_context_short} - Using current fetched price {actual_exit_price} as a fallback exit price since fill was not in immediate response.")

            elif actual_exit_price is None:
                logger.error(f"{log_context_short} - Could not determine actual exit price from OANDA response. This is critical. Response: {broker_response}")
                # Fallback, but flag as an issue
                price_fetch_side = "SELL" if action_from_payload == "BUY" else ("BUY" if action_from_payload == "SELL" else "SELL")
                actual_exit_price = await get_current_price(standardized_symbol_for_broker, price_fetch_side)
                logger.warning(f"{log_context_short} - Critical: Using current fetched price {actual_exit_price} as a fallback exit price due to missing fill price in response.")
                # It might be better to return False here if fill price is essential and not found.
                # For now, we proceed but this state is risky.

            logger.info(f"{log_context_short} - Position closed with broker. Determined Exit Price: {actual_exit_price}, Units effectively closed: {filled_units if filled_units > 0 else 'ALL'}")
            return True, {
                "position_id": position_id,
                "actual_exit_price": actual_exit_price, # Key name changed for clarity
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "broker_response": broker_response,
                "message": "Position closed successfully with broker.",
                "request_id": request_id
            }
        else:
            err_msg = "OANDA client/account not configured or symbol invalid for broker closure."
            logger.error(f"{log_context_short} - {err_msg}")
            return False, {"error": err_msg, "position_id": position_id, "request_id": request_id}
            
    except oandapyV20.exceptions.V20Error as v20_err:
        logger.error(f"{log_context_short} - OANDA API error during PositionClose: {v20_err.msg} (Code: {v20_err.code})", exc_info=True)
        return False, {"error": f"OANDA API Error: {v20_err.msg}", "details": str(v20_err), "position_id": position_id, "request_id": request_id}
    except Exception as e:
        logger.error(f"{log_context_short} - General error during PositionClose with broker: {str(e)}", exc_info=True)
        return False, {"error": str(e), "position_id": position_id, "request_id": request_id}

@async_error_handler()
async def internal_close_position(position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close a position with the broker"""
    try:
        symbol = position_data.get('symbol', '')
        price = await get_current_price(symbol, "SELL")
        response = {
            "orderCreateTransaction": {
                "id": str(uuid.uuid4()),
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "MARKET_ORDER",
                "instrument": symbol,
                "units": "0"
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

# Add near line ~1300 in the Trading Execution section
async def calculate_pure_position_size(instrument: str, risk_percentage: float, balance: float, action: str) -> Tuple[float, int]:
    """Calculate trade size using fixed 15% equity allocation with leverage consideration"""
    
    # Normalize the instrument symbol
    normalized_instrument = standardize_symbol(instrument)
    
    try:
        # FIXED: Always use 15% of equity regardless of provided risk percentage
        equity_percentage = 0.15  # Fixed at 15%
        equity_amount = balance * equity_percentage
        
        # Get the correct leverage based on instrument type
        leverage = INSTRUMENT_LEVERAGES.get(normalized_instrument, 20)  # Default to 20 if not found
        
        # Get current price for the instrument
        price = await get_current_price(normalized_instrument, action)  # Use action here, not direction
        
        # Extract the crypto/instrument symbol for size constraints
        crypto_symbol = None
        for symbol in CRYPTO_MIN_SIZES.keys():
            if symbol in normalized_instrument:
                crypto_symbol = symbol
                break
        
        # Determine instrument type
        instrument_type = get_instrument_type(normalized_instrument)
        
        # Calculate position size differently based on asset type
        if instrument_type == "CRYPTO" or instrument_type == "COMMODITY":
            # For crypto/commodities: (equity_amount / price) * leverage
            trade_size = (equity_amount / price) * leverage
            
            # Apply precision based on instrument
            precision = 2
            if crypto_symbol:
                tick_size = CRYPTO_TICK_SIZES.get(crypto_symbol, 0.01)
                precision = len(str(tick_size).split('.')[-1]) if '.' in str(tick_size) else 0
                
            min_size = CRYPTO_MIN_SIZES.get(crypto_symbol, 0.0001) if crypto_symbol else 0.2
            max_size = CRYPTO_MAX_SIZES.get(crypto_symbol, float('inf')) if crypto_symbol else float('inf')
            
        else:  # Standard forex pairs
            # For forex: (equity_amount * leverage)
            trade_size = equity_amount * leverage
            precision = 0
            min_size = 1200  # Default minimum units for forex
            max_size = float('inf')
            tick_size = 1
        
        # Apply minimum and maximum size constraints
        trade_size = max(min_size, min(max_size, trade_size))
        
        # Round to the nearest tick size
        if tick_size > 0:
            trade_size = round(trade_size / tick_size) * tick_size
            # After rounding to tick size, also apply precision for display
            if precision > 0:
                trade_size = round(trade_size, precision)
            else:
                trade_size = int(round(trade_size))
        
        logger.info(f"Using fixed 15% equity allocation with {leverage}:1 leverage. " 
                    f"Calculated trade size: {trade_size} for {normalized_instrument}, " 
                    f"equity: ${balance}, min_size: {min_size}, max_size: {max_size}, tick_size: {tick_size}")
        
        # Set direction multiplier based on action
        if action.upper() == 'SELL':
            trade_size = -abs(trade_size)
        
        return trade_size, precision
        
    except Exception as e:
        logger.error(f"Error calculating trade size: {str(e)}")
        raise


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
        self._price_update_lock = asyncio.Lock()
        
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
                stop_loss=None,
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
            
    from typing import Any, Dict, Optional, NamedTuple


    class ClosePositionResult(NamedTuple):
        success: bool
        position_data: Optional[Dict[str, Any]] = None
        error: Optional[str] = None
        
    async def close_position(
        self,
        position_id: str,
        exit_price: float, # This should be the actual_exit_price from the broker call
        reason: str = "manual"
    ) -> ClosePositionResult:
        """Close a position in internal records, update metrics, and persist changes to DB."""
        
        request_id = str(uuid.uuid4()) # For correlating logs for this specific internal close operation
        log_context_short = f"[INTERNAL_CLOSE] PosID: {position_id}, ReqID: {request_id}"

        async with self._lock:
            logger.info(f"{log_context_short} - Attempting to update internal state for position closure. ExitPrice: {exit_price}, Reason: {reason}")
            if position_id not in self.positions:
                logger.warning(f"{log_context_short} - Position not found in active 'self.positions' for closure.")
                # It might already be closed or never existed in memory.
                # Check if it's in closed_positions already.
                if position_id in self.closed_positions:
                    logger.info(f"{log_context_short} - Position was already in 'self.closed_positions'. No action needed for internal state.")
                    return ClosePositionResult(success=True, position_data=self.closed_positions[position_id], error="Position already marked as closed in memory.")
                return ClosePositionResult(success=False, error="Position not found in active memory.")
            
            position_obj = self.positions[position_id] # Renamed from 'position' to 'position_obj' for clarity
            symbol = position_obj.symbol
            logger.debug(f"{log_context_short} - Found active position: Symbol={symbol}, Action={position_obj.action}, Entry={position_obj.entry_price}")
    
            try:
                # Update the in-memory Position object's state
                position_obj.close(exit_price=exit_price, exit_reason=reason) # 'exit_reason' was 'reason'
                logger.info(f"{log_context_short} - In-memory Position object updated to closed. PnL: {position_obj.pnl:.2f}, PnL%: {position_obj.pnl_percentage:.2f}%")
            except Exception as e:
                logger.error(f"{log_context_short} - Failed to update in-memory Position object state during close: {str(e)}", exc_info=True)
                return ClosePositionResult(success=False, error=f"In-memory Position object .close() method failed: {str(e)}")
    
            # Prepare dictionary for DB and history (using the updated position_obj)
            final_position_dict_for_db = self._position_to_dict(position_obj)
    
            # Move from active positions to closed positions in memory
            self.closed_positions[position_id] = final_position_dict_for_db
            logger.debug(f"{log_context_short} - Position moved to 'self.closed_positions' in memory.")
    
            # Remove from open_positions_by_symbol structure
            if symbol in self.open_positions_by_symbol and position_id in self.open_positions_by_symbol[symbol]:
                del self.open_positions_by_symbol[symbol][position_id]
                if not self.open_positions_by_symbol[symbol]: # Clean up empty symbol dict
                    del self.open_positions_by_symbol[symbol]
                logger.debug(f"{log_context_short} - Position removed from 'self.open_positions_by_symbol'.")
            else:
                logger.warning(f"{log_context_short} - Position {position_id} (symbol {symbol}) not found in self.open_positions_by_symbol during close. This might indicate prior inconsistency.")
    
            # Remove from the primary active positions dictionary
            if position_id in self.positions:
                del self.positions[position_id]
                logger.debug(f"{log_context_short} - Position removed from 'self.positions' (active list).")
            else:
                # This should not happen if the first check passed, but good to be defensive
                logger.warning(f"{log_context_short} - Position {position_id} was not in 'self.positions' at final removal stage. State might have changed.")

            # Update position history (find and replace, or append if somehow missing)
            history_updated_flag = False
            for i, hist_pos_entry in enumerate(self.position_history):
                if hist_pos_entry.get("position_id") == position_id:
                    self.position_history[i] = final_position_dict_for_db
                    history_updated_flag = True
                    logger.debug(f"{log_context_short} - Updated position entry in 'self.position_history'.")
                    break
            if not history_updated_flag:
                self.position_history.append(final_position_dict_for_db) # Add if it was missing
                logger.warning(f"{log_context_short} - Position was not found in history, appended. This might indicate an issue with initial recording.")
    
            # Update risk metrics (ensure attributes exist on position_obj or self)
            # adjusted_risk_val = getattr(position_obj, "adjusted_risk", 0) # From the Position object itself
            # if hasattr(self, "current_risk"): # current_risk on PositionTracker
            #     self.current_risk = max(0, self.current_risk - adjusted_risk_val)
            # else:
            #     logger.warning(f"{log_context_short} - 'current_risk' attribute not found on PositionTracker. Cannot update portfolio risk.")
            # logger.debug(f"{log_context_short} - Portfolio risk updated (if applicable). Current risk: {getattr(self, 'current_risk', 'N/A')}")

            # Update database if db manager is available
            db_update_successful = False
            if self.db_manager:
                try:
                    # Only send fields that need to be updated for a closure
                    db_update_payload = {
                        "status": position_obj.status,
                        "close_time": position_obj.close_time.isoformat() if position_obj.close_time else None,
                        "exit_price": position_obj.exit_price,
                        "pnl": position_obj.pnl,
                        "pnl_percentage": position_obj.pnl_percentage,
                        "exit_reason": position_obj.exit_reason,
                        "last_update": position_obj.last_update.isoformat(),
                        "current_price": position_obj.exit_price # current_price should reflect the exit_price upon closure
                    }
                    logger.info(f"{log_context_short} - Attempting DB update for closure. Payload: {db_update_payload}")
                    
                    db_update_successful = await self.db_manager.update_position(position_id, db_update_payload) # update_position should return bool
                    
                    if db_update_successful:
                        logger.info(f"{log_context_short} - Successfully updated position in DB to 'closed'.")
                    else:
                        # This means update_position in db_manager returned False after retries, but didn't raise an exception
                        logger.error(f"{log_context_short} - CRITICAL: DB update to close position returned False (after retries). DB INCONSISTENT.")
                        # This is a critical state. The position is closed in memory but not confirmed in DB.
                        return ClosePositionResult(success=False, position_data=final_position_dict_for_db, error="Database update failed to confirm closure after retries.")
                except Exception as e_db:
                    logger.error(f"{log_context_short} - CRITICAL: Exception during DB update for closure: {str(e_db)}. DB INCONSISTENT!", exc_info=True)
                    # The position is closed in memory, but DB update failed.
                    return ClosePositionResult(success=False, position_data=final_position_dict_for_db, error=f"DB update exception during closure: {str(e_db)}")
            else:
                logger.warning(f"{log_context_short} - DB manager not available. Closure processed in-memory only.")
                # If no DB manager, we consider the in-memory operation successful for the purpose of this method's contract.
                db_update_successful = True # No DB to fail

            if db_update_successful: # Only if in-memory AND DB (if present) are fine
                logger.info(f"{log_context_short} - Internal state and DB (if applicable) successfully updated for position closure.")
                return ClosePositionResult(success=True, position_data=final_position_dict_for_db)
            else:
                # This path should ideally not be reached if errors are returned above, but as a fallback.
                logger.error(f"{log_context_short} - Internal closure failed due to DB update issues. See logs above.")
                return ClosePositionResult(success=False, position_data=final_position_dict_for_db, error="Internal closure failed, likely due to DB update issues.")

            
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
        async with self._price_update_lock:
            async with self._lock:
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
            "stop_loss": None,  # Always set to None regardless of what's in the Position object
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
                               account_risk: float,  # Moved non-default argument earlier
                               stop_loss: Optional[float] = None,  # Default argument now follows non-default
                               timeframe: str = "H1") -> Dict[str, Any]: # Default argument
        """Register a new position with the risk manager"""
        async with self._lock:
            # Calculate risk amount directly from account percentage
            risk_amount = self.account_balance * account_risk

            # Calculate risk percentage
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
                "stop_loss": None,  # Always set to None
                "risk_amount": risk_amount,
                "risk_percentage": risk_percentage,
                "adjusted_risk": adjusted_risk,
                "timeframe": timeframe,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Apply correlation factor if applicable
            correlated_instruments = self._get_correlated_instruments(symbol)
            if correlated_instruments:
                # Add correlation info to risk data
                risk_data["correlated_instruments"] = correlated_instruments
                risk_data["correlation_factor"] = self.correlation_factor

                # Adjust risk for correlation
                risk_data["correlation_adjusted_risk"] = adjusted_risk * self.correlation_factor
                adjusted_risk = risk_data["correlation_adjusted_risk"]

            # Apply streak adjustment
            streak_factor = self._calculate_streak_factor()
            risk_data["streak_factor"] = streak_factor
            risk_data["streak_adjusted_risk"] = adjusted_risk * streak_factor
            adjusted_risk = risk_data["streak_adjusted_risk"]

            # Store the final adjusted risk
            risk_data["final_adjusted_risk"] = adjusted_risk

            self.positions[position_id] = risk_data

            # Update portfolio risk
            self.current_risk += adjusted_risk

            logger.info(f"Registered position {position_id} with risk: {adjusted_risk:.2%} (total: {self.current_risk:.2%})")

            return risk_data
            
            
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

class DynamicExitManager:
    """
    Manages dynamic exits based on Lorentzian classifier market regimes.
    Adjusts stop losses, take profits, and trailing stops based on market conditions.
    """
    def __init__(self, position_tracker=None, multi_stage_tp_manager=None):
        """Initialize dynamic exit manager"""
        self.position_tracker = position_tracker
        self.exit_levels = {}
        self.trailing_stops = {}
        self.performance = {}
        self._running = False
        self.lorentzian_classifier = LorentzianDistanceClassifier()
        self.exit_strategies = {}
        self._lock = asyncio.Lock()
        self.logger = get_module_logger(__name__)
        

        self.TIMEFRAME_TAKE_PROFIT_LEVELS = {
            "1H": {"first_exit": 0.3, "second_exit": 0.3, "runner": 0.4},
            "4H": {"first_exit": 0.35, "second_exit": 0.35, "runner": 0.3},
            "15M": {"first_exit": 0.4, "second_exit": 0.4, "runner": 0.2},
            # Add other timeframe configurations
        }

        self.TIMEFRAME_TRAILING_SETTINGS = {
            "1H": {"initial_multiplier": 1.5, "profit_levels": [1.0, 2.0]},
            "4H": {"initial_multiplier": 2.0, "profit_levels": [1.5, 3.0]},
            "15M": {"initial_multiplier": 1.2, "profit_levels": [0.8, 1.6]},
            # Add other timeframe configurations
        }

        self.TIMEFRAME_TIME_STOPS = {
            "15M": {
                "optimal_duration": 4,  # hours
                "max_duration": 8,  # hours
                "stop_adjustment": 0.5  # tighten by 50% after max duration
            },
            "1H": {
                "optimal_duration": 8,  # hours
                "max_duration": 24,  # hours
                "stop_adjustment": 0.5
            },
            "4H": {
                "optimal_duration": 24,  # hours
                "max_duration": 72,  # hours
                "stop_adjustment": 0.5
            },
            "1D": {
                "optimal_duration": 72,  # hours
                "max_duration": 168,  # hours
                "stop_adjustment": 0.5
            }
        }


    async def start(self):
        """Start the exit manager"""
        if not self._running:
            self._running = True
            logger.info("Dynamic Exit Manager started")
            
    async def stop(self):
        """Stop the exit manager"""
        self._running = False
        logger.info("Dynamic Exit Manager stopped")

                                      
    async def _init_breakeven_stop(self, position_id, entry_price, position_direction, stop_loss=None):
        """Initialize breakeven stop loss functionality.
        Calculates distance for breakeven based on provided stop_loss or ATR if stop_loss is None.
        """
        log_message_prefix = f"Position {position_id}:"
        self.logger.debug(f"{log_message_prefix} _init_breakeven_stop called. entry_price={entry_price}, stop_loss={stop_loss}, direction={position_direction}")

        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}

        distance = None  # Initialize distance

        if stop_loss is not None:
            # If a valid stop_loss value is provided, calculate distance using it
            if not isinstance(stop_loss, (int, float)):
                self.logger.error(f"{log_message_prefix} Provided stop_loss is not a number: {stop_loss}. Cannot calculate distance for breakeven.")
                return None
            if entry_price is None or not isinstance(entry_price, (int, float)):
                self.logger.error(f"{log_message_prefix} entry_price is invalid: {entry_price}. Cannot calculate distance for breakeven.")
                return None
                
            distance = abs(entry_price - stop_loss)
            self.logger.info(f"{log_message_prefix} Using provided stop_loss {stop_loss} to calculate distance for breakeven: {distance}")
        else:
            # If stop_loss is None, calculate distance using ATR
            self.logger.info(f"{log_message_prefix} No initial stop-loss provided (stop_loss is None). Calculating distance for breakeven based on ATR.")
            
            if not self.position_tracker: # Ensure position_tracker is available
                self.logger.error(f"{log_message_prefix} Position tracker not available. Cannot fetch position data for ATR calculation.")
                return None

            position_data = await self.position_tracker.get_position_info(position_id)
            if not position_data:
                self.logger.warning(f"{log_message_prefix} Position data not found. Cannot calculate ATR-based distance for breakeven.")
                return None

            symbol = position_data.get("symbol")
            timeframe = position_data.get("timeframe", "H1") # Default to H1 if not found
            
            if not symbol:
                self.logger.warning(f"{log_message_prefix} Symbol not found in position data. Cannot calculate ATR-based distance.")
                return None

            atr = await get_atr(symbol, timeframe) 
            if atr is None or not isinstance(atr, (int, float)) or atr <= 0:
                self.logger.warning(f"{log_message_prefix} Invalid ATR ({atr}) for {symbol} on {timeframe}. Cannot calculate ATR-based distance.")
                return None

            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            calculated_atr_distance = atr * atr_multiplier
            distance = calculated_atr_distance # Use this directly
            self.logger.info(f"{log_message_prefix} Calculated ATR-based distance for breakeven: {distance} (ATR: {atr}, Multiplier: {atr_multiplier})")

        # Ensure distance was successfully calculated and is positive
        if distance is None or not isinstance(distance, (int, float)) or distance <= 0:
            self.logger.error(f"{log_message_prefix} Failed to determine a valid positive distance ({distance}) for breakeven. Aborting breakeven setup.")
            return None

        # Activate breakeven at 1R profit by default, adjustable by timeframe
        activation_multiplier = 1.0  # Default to 1:1 risk:reward

        # Fetch position_data again if it wasn't fetched in the ATR path, or use previously fetched.
        # This is to get timeframe for activation_multiplier adjustment.
        # To avoid re-fetching if already done:
        if 'position_data' not in locals() or not position_data: # If not defined or None from ATR path
             if not self.position_tracker:
                self.logger.error(f"{log_message_prefix} Position tracker not available for timeframe lookup.")
                # Fallback to default activation_multiplier or return
             else:
                position_data = await self.position_tracker.get_position_info(position_id)

        if position_data: # Check if position_data was successfully fetched
            timeframe_for_activation = position_data.get("timeframe", "H1")
            if timeframe_for_activation == "15M":
                activation_multiplier = 0.8
            elif timeframe_for_activation == "1H":
                activation_multiplier = 1.0
            elif timeframe_for_activation == "4H":
                activation_multiplier = 1.2
            elif timeframe_for_activation == "1D":
                activation_multiplier = 1.5
        else:
            self.logger.warning(f"{log_message_prefix} Position data not available for timeframe-based activation multiplier adjustment. Using default ({activation_multiplier}).")


        # Calculate activation level
        if entry_price is None: # Should have been caught earlier but good for safety
            self.logger.error(f"{log_message_prefix} Entry price is None. Cannot calculate activation level.")
            return None

        activation_level = None
        if position_direction.upper() == "LONG" or position_direction.upper() == "BUY": # Handle both BUY and LONG
            activation_level = entry_price + (distance * activation_multiplier)
        elif position_direction.upper() == "SELL" or position_direction.upper() == "SHORT": # Handle both SELL and SHORT
            activation_level = entry_price - (distance * activation_multiplier)
        else:
            self.logger.error(f"{log_message_prefix} Invalid position_direction: {position_direction}. Cannot calculate activation level.")
            return None
        
        # Store breakeven configuration
        # Note: self.exit_levels[position_id]["breakeven"]["stop_loss"] is intentionally set to None.
        # The 'stop_loss' parameter of this function is used to determine the 'distance',
        # not necessarily to set the breakeven stop-loss price itself in this config.
        self.exit_levels[position_id]["breakeven"] = {
            "entry_price": entry_price,
            "initial_risk_distance_for_activation": distance, # Added for clarity
            "stop_loss": None,  # This specific breakeven configuration does not store a stop-loss price.
            "activation_level": activation_level,
            "activated": False,
            "buffer_pips": 0,  # Optional buffer above/below entry
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        
        self.logger.info(f"{log_message_prefix} Initialized breakeven stop: Entry={entry_price}, RiskDistanceForActivation={distance}, ActivationLevel={activation_level}, ActivationMultiplier={activation_multiplier}")
        
        return entry_price # Or perhaps return True or the activation_level if more useful

    async def _init_trend_following_exits(self, position_id, entry_price, stop_loss, position_direction):
        """Initialize exits optimized for trend-following strategies"""
        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}

        # Get position data for context
        position_data = await self.position_tracker.get_position_info(position_id)
        if not position_data:
            logger.warning(f"Position {position_id} not found for trend exit initialization") # Added warning
            return False # Added return False

        symbol = position_data.get("symbol")
        timeframe = position_data.get("timeframe", "H1")

        # Get ATR data for calculations
        atr = await get_atr(symbol, timeframe)
        if atr <= 0: # Handle case where ATR is invalid
             logger.warning(f"Invalid ATR value ({atr}) for {symbol}, cannot initialize trend exits.")
             return False

        stop_loss = None # Keeping this line as it was in the original code

        # Calculate take profit levels based on ATR
        take_profit_multiples = [2.0, 3.0, 4.5]  # Higher targets for trend following

        # Calculate take profit levels
        if position_direction == "LONG" or position_direction == "BUY":
            take_profits = [
                entry_price + (atr * multiple)
                for multiple in take_profit_multiples
            ]
        else: # Assumed SELL
            take_profits = [
                entry_price - (atr * multiple)
                for multiple in take_profit_multiples
            ]

        # Define specific percentages for trend following strategy
        percentages = {
            "first_exit": 0.3,   # 30% at 2R
            "second_exit": 0.3,  # 30% at 3R
            "runner": 0.4        # 40% with trailing
        }

        # Store take profit configuration
        self.exit_levels[position_id]["take_profits"] = {
            "levels": [
                {"price": take_profits[0], "percentage": percentages["first_exit"] * 100, "hit": False, "r_multiple": take_profit_multiples[0]},
                {"price": take_profits[1], "percentage": percentages["second_exit"] * 100, "hit": False, "r_multiple": take_profit_multiples[1]},
                {"price": take_profits[2], "percentage": percentages["runner"] * 100, "hit": False, "r_multiple": take_profit_multiples[2]}
            ],
            "strategy": "trend_following"
        }

        # Add time-based exit (longer for trend following)
        time_settings = self.TIMEFRAME_TIME_STOPS.get(
            timeframe, self.TIMEFRAME_TIME_STOPS["1H"]
        )

        # For trend following, use max duration
        if timeframe == "15M":
            max_hours = 24  # 1 day for 15M trend trades
        elif timeframe == "1H":
            max_hours = 72  # 3 days for 1H trend trades
        elif timeframe == "4H":
            max_hours = 168  # 7 days for 4H trend trades
        else:  # Daily
            max_hours = 336  # 14 days for 1D trend trades

        current_time = datetime.now(timezone.utc)
        exit_time = current_time + timedelta(hours=max_hours)

        self.exit_levels[position_id]["time_exit"] = {
            "exit_time": exit_time.isoformat(),
            "reason": "trend_following_max_time",
            "adjustable": True  # Can be extended if trend is still going
        }

        logger.info(f"Initialized trend following exits for {position_id}: Stop loss: {stop_loss}, "
                    f"Take profits: {take_profits}, Strategy: trend_following (NO TRAILING STOP)") # Updated log

        return True

    async def _init_mean_reversion_exits(self, position_id, entry_price, stop_loss, position_direction):
        """Initialize exits optimized for mean-reversion strategies"""
        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}
        
        # Get position data for context
        position_data = await self.position_tracker.get_position_info(position_id)
        if not position_data:
            logger.warning(f"Position {position_id} not found for mean reversion exit initialization")
            return False
        
        symbol = position_data.get("symbol")
        timeframe = position_data.get("timeframe", "H1")
        
        # Get ATR data if needed for stop loss calculation
        if stop_loss is None:
            atr = await get_atr(symbol, timeframe)
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            # Use tighter stops for mean reversion (80% of standard)
            adjusted_multiplier = atr_multiplier * 0.8
            
            if position_direction == "LONG":
                stop_loss = None # entry_price - (atr * adjusted_multiplier)
            else:
                stop_loss = None # entry_price + (atr * adjusted_multiplier)
        
        # Calculate risk distance (R value)
        risk_distance = abs(entry_price - stop_loss)
        
        # For mean reversion, use lower R-multiples (quicker targets)
        take_profit_multiples = [0.8, 1.5]  # Lower, quicker targets for mean reversion
        
        # Calculate take profit levels
        if position_direction == "LONG":
            take_profits = [
                entry_price + (risk_distance * multiple)
                for multiple in take_profit_multiples
            ]
        else:
            take_profits = [
                entry_price - (risk_distance * multiple)
                for multiple in take_profit_multiples
            ]
        
        # Define specific percentages for mean reversion strategy
        percentages = {
            "first_exit": 0.6,   # 60% at first target (quick profit)
            "second_exit": 0.4,  # 40% at second target
        }
        
        # Store take profit configuration
        self.exit_levels[position_id]["take_profits"] = {
            "levels": [
                {"price": take_profits[0], "percentage": percentages["first_exit"] * 100, "hit": False, "r_multiple": take_profit_multiples[0]},
                {"price": take_profits[1], "percentage": percentages["second_exit"] * 100, "hit": False, "r_multiple": take_profit_multiples[1]}
            ],
            "strategy": "mean_reversion"
        }
        
        # For mean reversion, initialize breakeven stop (activated quickly)
        if position_direction == "LONG":
            # Activate breakeven at 0.5R for mean reversion
            activation_level = entry_price + (risk_distance * 0.5)
        else:
            activation_level = entry_price - (risk_distance * 0.5)
        
        # Add breakeven configuration
        self.exit_levels[position_id]["breakeven"] = {
            "entry_price": entry_price,
            "activation_level": activation_level,
            "activated": False,
            "buffer_pips": 0
        }
        
        # Add time-based exit (shorter for mean reversion since these moves are quicker)
        # Use the optimal duration from your config
        time_settings = self.TIMEFRAME_TIME_STOPS.get(
            timeframe, self.TIMEFRAME_TIME_STOPS["1H"]
        )
        
        current_time = datetime.now(timezone.utc)
        exit_time = current_time + timedelta(hours=time_settings["optimal_duration"])
        
        self.exit_levels[position_id]["time_exit"] = {
            "exit_time": exit_time.isoformat(),
            "reason": "mean_reversion_time_limit",
            "adjustable": False  # Strict time exit for mean reversion
        }
        
        logger.info(f"Initialized mean reversion exits for {position_id}: Stop loss: {stop_loss}, "
                    f"Take profits: {take_profits}, Strategy: mean_reversion")
        
        return True

    async def _init_breakout_exits(self, position_id, entry_price, stop_loss, position_direction):
        """Initialize exits optimized for breakout trading strategies"""
        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}

        # Get position data for context
        position_data = await self.position_tracker.get_position_info(position_id)
        if not position_data:
            logger.warning(f"Position {position_id} not found for breakout exit initialization")
            return False

        symbol = position_data.get("symbol")
        timeframe = position_data.get("timeframe", "H1")

        # Get ATR data if needed for stop loss calculation
        atr = 0.0 # Initialize atr
        if stop_loss is None:
            atr = await get_atr(symbol, timeframe)
            if atr <= 0:
                logger.warning(f"Invalid ATR ({atr}) for {symbol}, cannot calculate stop loss for breakout strategy.")
                # Handle this case, maybe return False or use a default stop?
                # Using a default percentage for now
                stop_loss = None # entry_price * 0.98 if position_direction == "LONG" else entry_price * 1.02
            else:
                instrument_type = get_instrument_type(symbol)
                atr_multiplier = get_atr_multiplier(instrument_type, timeframe)

                # Slightly tighter stops for breakouts (90% of standard)
                adjusted_multiplier = atr_multiplier * 0.9

                if position_direction == "LONG":
                    stop_loss = None # entry_price - (atr * adjusted_multiplier)
                else:
                    stop_loss = None # entry_price + (atr * adjusted_multiplier)

        # Calculate risk distance (R value)
        risk_distance = abs(entry_price - stop_loss)
        if risk_distance <= 0: # Avoid division by zero if SL calculation failed
             logger.error(f"Invalid risk distance ({risk_distance}) for {position_id}. Cannot initialize breakout exits.")
             return False

        # For breakouts, use moderate R-multiples with first target sooner
        take_profit_multiples = [1.0, 2.0, 4.0]  # Quick first target, extended last target

        # Calculate take profit levels
        if position_direction == "LONG":
            take_profits = [
                entry_price + (risk_distance * multiple)
                for multiple in take_profit_multiples
            ]
        else:
            take_profits = [
                entry_price - (risk_distance * multiple)
                for multiple in take_profit_multiples
            ]

        # Define specific percentages for breakout strategy - take more profit early
        percentages = {
            "first_exit": 0.4,   # 40% at 1R
            "second_exit": 0.3,  # 30% at 2R
            "runner": 0.3        # 30% for potential runner (4R)
        }

        # Store take profit configuration
        self.exit_levels[position_id]["take_profits"] = {
            "levels": [
                {"price": take_profits[0], "percentage": percentages["first_exit"] * 100, "hit": False, "r_multiple": take_profit_multiples[0]},
                {"price": take_profits[1], "percentage": percentages["second_exit"] * 100, "hit": False, "r_multiple": take_profit_multiples[1]},
                {"price": take_profits[2], "percentage": percentages["runner"] * 100, "hit": False, "r_multiple": take_profit_multiples[2]}
            ],
            "strategy": "breakout"
        }

        # For breakouts, use breakeven (activated after first target hit)
        self.exit_levels[position_id]["breakeven"] = {
            "entry_price": entry_price,
            "activation_level": take_profits[0],  # Activate at first TP
            "activated": False,
            "buffer_pips": 0,
            "active_after_tp": 0  # Activate after first TP hit (index 0)
        }

        # Add time-based exit (medium duration for breakouts)
        time_settings = self.TIMEFRAME_TIME_STOPS.get(
            timeframe, self.TIMEFRAME_TIME_STOPS["1H"]
        )

        # Use a value between optimal and max duration
        hours = (time_settings["optimal_duration"] + time_settings["max_duration"]) / 2

        current_time = datetime.now(timezone.utc)
        exit_time = current_time + timedelta(hours=hours)

        self.exit_levels[position_id]["time_exit"] = {
            "exit_time": exit_time.isoformat(),
            "reason": "breakout_time_limit",
            "adjustable": True  # Can be adjusted based on momentum
        }

        logger.info(f"Initialized breakout exits for {position_id}: Stop loss: {stop_loss}, "
                    f"Take profits: {take_profits}, Strategy: breakout (NO TRAILING STOP)") # Updated log

        return True

    async def _init_standard_exits(self, position_id, entry_price, stop_loss, position_direction):
        """Initialize standard exit strategy for when no specific strategy is identified"""
        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}

        # Get position data for context
        position_data = await self.position_tracker.get_position_info(position_id)
        if not position_data:
            logger.warning(f"Position {position_id} not found for standard exit initialization")
            return False

        symbol = position_data.get("symbol")
        timeframe = position_data.get("timeframe", "H1")

        # Get ATR data if needed for stop loss calculation
        atr = 0.0 # Initialize atr
        if stop_loss is None:
            atr = await get_atr(symbol, timeframe)
            if atr <= 0:
                 logger.warning(f"Invalid ATR ({atr}) for {symbol}, cannot calculate stop loss for standard strategy.")
                 # Handle this case, maybe return False or use a default stop?
                 stop_loss = None # entry_price * 0.98 if position_direction == "LONG" else entry_price * 1.02
            else:
                instrument_type = get_instrument_type(symbol)
                atr_multiplier = get_atr_multiplier(instrument_type, timeframe)

                if position_direction == "LONG":
                    stop_loss = None # entry_price - (atr * atr_multiplier)
                else:
                    stop_loss = None # entry_price + (atr * atr_multiplier)

        # Calculate risk distance (R value)
        risk_distance = abs(entry_price - stop_loss)
        if risk_distance <= 0: # Avoid division by zero if SL calculation failed
             logger.error(f"Invalid risk distance ({risk_distance}) for {position_id}. Cannot initialize standard exits.")
             return False

        # Standard R-multiples (1:1, 2:1, 3:1)
        take_profit_multiples = [1.0, 2.0, 3.0]

        # Calculate take profit levels
        if position_direction == "LONG":
            take_profits = [
                entry_price + (risk_distance * multiple)
                for multiple in take_profit_multiples
            ]
        else:
            take_profits = [
                entry_price - (risk_distance * multiple)
                for multiple in take_profit_multiples
            ]

        # Use class-defined constants
        tp_levels = self.TIMEFRAME_TAKE_PROFIT_LEVELS.get(
            timeframe, self.TIMEFRAME_TAKE_PROFIT_LEVELS["1H"]
        )

        # Use the percentages from your config
        percentages = {
            "first_exit": tp_levels["first_exit"] * 100,
            "second_exit": tp_levels["second_exit"] * 100,
            "runner": tp_levels["runner"] * 100
        }

        # Store take profit configuration
        self.exit_levels[position_id]["take_profits"] = {
            "levels": [
                {"price": take_profits[0], "percentage": percentages["first_exit"], "hit": False, "r_multiple": take_profit_multiples[0]},
                {"price": take_profits[1], "percentage": percentages["second_exit"], "hit": False, "r_multiple": take_profit_multiples[1]},
                {"price": take_profits[2], "percentage": percentages["runner"], "hit": False, "r_multiple": take_profit_multiples[2]}
            ],
            "strategy": "standard"
        }

        time_settings = self.TIMEFRAME_TIME_STOPS.get(
            timeframe, self.TIMEFRAME_TIME_STOPS["1H"]
        )

        # Use optimal duration from your config
        hours = time_settings["optimal_duration"]

        current_time = datetime.now(timezone.utc)
        exit_time = current_time + timedelta(hours=hours)

        self.exit_levels[position_id]["time_exit"] = {
            "exit_time": exit_time.isoformat(),
            "reason": "standard_time_limit",
            "adjustable": True
        }

        logger.info(f"Initialized standard exits for {position_id}: Stop loss: {stop_loss}, "
                    f"Take profits: {take_profits}, Strategy: standard (NO TRAILING STOP)") # Updated log

        return True
            
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
    
            # Initialize trailing stop if stop loss is provided
            if stop_loss:
                await self._init_trailing_stop(position_id, entry_price, stop_loss, position_direction)
    
            # Debug log
            self.logger.debug(
                f"[BREAKEVEN INIT] PosID={position_id} "
                f"entry={entry_price} dir={position_direction} sl={stop_loss!r}"
            )
    
       # Initialize breakeven stop
        await self._init_breakeven_stop(position_id, entry_price, position_direction, stop_loss)
        
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
        
        self.logger.info(f"Initialized exits for {position_id} with {regime} regime and {strategy_type} strategy")
        
        return self.exit_strategies[position_id]

    async def _init_trailing_stop(self, position_id: str, entry_price: float, stop_loss: float, position_direction: str):
        """Initialize trailing stop functionality"""
        if position_id not in self.trailing_stops:
            self.trailing_stops[position_id] = {}
            
        try:
            # Calculate initial trailing distance
            initial_distance = abs(entry_price - stop_loss)
            
            self.trailing_stops[position_id] = {
                "entry_price": entry_price,
                "current_stop": stop_loss,
                "trail_distance": initial_distance,
                "highest_price": entry_price if position_direction.upper() in ["BUY", "LONG"] else None,
                "lowest_price": entry_price if position_direction.upper() in ["SELL", "SHORT"] else None,
                "activated": False,
                "last_update": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"Initialized trailing stop for {position_id}: distance={initial_distance}")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing trailing stop for {position_id}: {str(e)}")
            return False


    ##############################################################################
    # Market Analysis
##############################################################################

# Consolidated Class - Replace BOTH existing classes
# (LorentzianDistanceClassifier and MarketRegimeClassifier) with this one.
# Ensure necessary imports like numpy, statistics, asyncio, etc. are present at the top of the file.

class LorentzianDistanceClassifier:
    def __init__(self, lookback_period: int = 20, max_history: int = 1000):
        """Initialize with history limits"""
        self.lookback_period = lookback_period
        self.max_history = max_history
        self.price_history = {}  # symbol -> List[float]
        self.regime_history = {} # symbol -> List[str] (history of classified regimes)
        self.volatility_history = {} # symbol -> List[float]
        self.atr_history = {}  # symbol -> List[float]
        self.regimes = {}  # symbol -> Dict[str, Any] (stores latest regime data)
        self._lock = asyncio.Lock()
        self.logger = get_module_logger(__name__) # Assuming get_module_logger is available

    async def add_price_data(self, symbol: str, price: float, timeframe: str, atr: Optional[float] = None):
        """Add price data with limits"""
        async with self._lock:
            # Initialize if needed
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            
            # Add new data
            self.price_history[symbol].append(price)
            
            # Enforce history limit
            if len(self.price_history[symbol]) > self.max_history:
                self.price_history[symbol] = self.price_history[symbol][-self.max_history:]

            # Update ATR history if provided
            if atr is not None:
                self.atr_history[symbol].append(atr)
                if len(self.atr_history[symbol]) > self.lookback_period:
                    self.atr_history[symbol].pop(0)

            # Update regime if we have enough data
            if len(self.price_history[symbol]) >= 2:
                await self.classify_market_regime(symbol, price, atr) # Use the unified classification method

    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """Calculate Lorentzian distance between current price and historical prices"""
        if not history:
            return 0.0

        # Calculate using Lorentzian formula with log scaling
        distances = [np.log(1 + abs(price - hist_price)) for hist_price in history]

        # Return average distance (check for empty list for robustness)
        return float(np.mean(distances)) if distances else 0.0

    async def classify_market_regime(self, symbol: str, current_price: float, atr: Optional[float] = None) -> Dict[str, Any]:
        """Classify current market regime using multiple factors"""
        async with self._lock:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
                # Not enough data, return default unknown state
                unknown_state = {"regime": "unknown", "volatility": 0.0, "momentum": 0.0, "price_distance": 0.0, "regime_strength": 0.0, "last_update": datetime.now(timezone.utc).isoformat()}
                self.regimes[symbol] = unknown_state # Store the unknown state
                return unknown_state

            try:
                # Calculate price-based metrics
                price_distance = await self.calculate_lorentzian_distance(
                    current_price, self.price_history[symbol][:-1]  # Compare current to history
                )

                # Calculate returns and volatility
                returns = []
                prices = self.price_history[symbol]
                for i in range(1, len(prices)):
                    if prices[i-1] != 0: # Avoid division by zero
                        returns.append(prices[i] / prices[i-1] - 1)

                volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0

                # Calculate momentum (percentage change over lookback period)
                momentum = (current_price - prices[0]) / prices[0] if prices[0] != 0 else 0.0

                # Get mean ATR if available
                mean_atr = np.mean(self.atr_history[symbol]) if self.atr_history.get(symbol) else 0.0

                # Multi-factor regime classification logic (identical in both original classes)
                regime = "unknown"
                regime_strength = 0.5  # Default medium strength

                if price_distance < 0.1 and volatility < 0.001:
                    regime = "ranging"
                    regime_strength = min(1.0, 0.7 + (0.1 - price_distance) * 3)
                elif price_distance > 0.3 and abs(momentum) > 0.002:
                    regime = "trending_up" if momentum > 0 else "trending_down"
                    regime_strength = min(1.0, 0.6 + price_distance + abs(momentum) * 10)
                elif volatility > 0.003 or (mean_atr > 0 and atr is not None and atr > 1.5 * mean_atr):
                    regime = "volatile"
                    regime_strength = min(1.0, 0.6 + volatility * 100)
                elif abs(momentum) > 0.003:
                     regime = "momentum_up" if momentum > 0 else "momentum_down"
                     regime_strength = min(1.0, 0.6 + abs(momentum) * 50)
                else:
                    regime = "mixed"
                    regime_strength = 0.5

                # Update regime history (internal state)
                self.regime_history[symbol].append(regime)
                if len(self.regime_history[symbol]) > self.lookback_period:
                    self.regime_history[symbol].pop(0)

                # Update volatility history (internal state)
                self.volatility_history[symbol].append(volatility)
                if len(self.volatility_history[symbol]) > self.lookback_period:
                    self.volatility_history[symbol].pop(0)

                # Store the latest regime data for retrieval
                result = {
                    "regime": regime,
                    "regime_strength": regime_strength,
                    "price_distance": price_distance,
                    "volatility": volatility,
                    "momentum": momentum,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "metrics": { # Include metrics for potential analysis
                        "price_distance": price_distance,
                        "volatility": volatility,
                        "momentum": momentum,
                        "lookback_period": self.lookback_period
                    }
                }
                self.regimes[symbol] = result # Update the main storage

                # Store recent regime classifications for get_dominant_regime
                if "classification_history" not in self.regimes[symbol]:
                     self.regimes[symbol]["classification_history"] = []

                self.regimes[symbol]["classification_history"].append({
                    "regime": regime,
                    "strength": regime_strength,
                    "timestamp": result["last_update"]
                })
                # Limit history length
                hist_limit = 20
                if len(self.regimes[symbol]["classification_history"]) > hist_limit:
                    self.regimes[symbol]["classification_history"] = self.regimes[symbol]["classification_history"][-hist_limit:]


                return result

            except Exception as e:
                 self.logger.error(f"Error classifying regime for {symbol}: {str(e)}", exc_info=True)
                 error_state = {"regime": "error", "regime_strength": 0.0, "last_update": datetime.now(timezone.utc).isoformat(), "error": str(e)}
                 self.regimes[symbol] = error_state
                 return error_state


    def get_dominant_regime(self, symbol: str) -> str:
        """Get the dominant regime over recent history (uses internal state)"""
        if symbol not in self.regime_history or len(self.regime_history[symbol]) < 3:
            return "unknown"

        # Look at the last 5 classified regimes stored internally
        recent_regimes = self.regime_history[symbol][-5:]
        regime_counts = {}
        for regime in recent_regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1

        # Find most common regime if it meets threshold
        if regime_counts:
            dominant_regime, count = max(regime_counts.items(), key=lambda item: item[1])
            if count / len(recent_regimes) >= 0.6: # Requires > 60% dominance
                return dominant_regime

        return "mixed" # Default if no single regime dominates

    async def should_adjust_exits(self, symbol: str, current_regime: Optional[str] = None) -> Tuple[bool, Dict[str, float]]:
        """Determine if exit levels should be adjusted based on regime stability and type"""
        # Get current regime if not provided
        if current_regime is None:
            if symbol not in self.regime_history or not self.regime_history[symbol]:
                # Cannot determine stability without history
                return False, {"take_profit": 1.0, "trailing_stop": 1.0}
            current_regime = self.regime_history[symbol][-1]

        # Check regime stability (e.g., last 3 regimes are the same)
        recent_regimes = self.regime_history.get(symbol, [])[-3:]
        is_stable = len(recent_regimes) >= 3 and len(set(recent_regimes)) == 1

        # Default adjustments (no change)
        adjustments = {"take_profit": 1.0, "trailing_stop": 1.0}

        # Apply adjustments only if the regime is stable
        if is_stable:
            if "volatile" in current_regime:
                adjustments = {"take_profit": 2.0, "trailing_stop": 1.25}
            elif "trending" in current_regime:
                adjustments = {"take_profit": 1.5, "trailing_stop": 1.1}
            elif "ranging" in current_regime:
                adjustments = {"take_profit": 0.8, "trailing_stop": 0.9}
            elif "momentum" in current_regime:
                adjustments = {"take_profit": 1.7, "trailing_stop": 1.3}

        # --- Start of Corrected Indentation ---
        # Determine if any adjustment is actually needed
        should_adjust = is_stable and any(v != 1.0 for v in adjustments.values()) # Now indented correctly
        return should_adjust, adjustments # Now indented correctly
        # --- End of Corrected Indentation ---

    def get_regime_data(self, symbol: str) -> Dict[str, Any]:
        """Get the latest calculated market regime data for a symbol"""
        # Return the latest stored regime data
        regime_data = self.regimes.get(symbol, {})

        # Ensure last_update is always present and formatted correctly
        if "last_update" not in regime_data:
            regime_data["last_update"] = datetime.now(timezone.utc).isoformat()
        elif isinstance(regime_data["last_update"], datetime):
             regime_data["last_update"] = regime_data["last_update"].isoformat()


        # Provide defaults if completely missing
        if not regime_data:
             return {
                "regime": "unknown",
                "regime_strength": 0.0,
                "last_update": datetime.now(timezone.utc).isoformat()
            }

        return regime_data.copy() # Return a copy

    def is_suitable_for_strategy(self, symbol: str, strategy_type: str) -> bool:
        """Determine if the current market regime is suitable for a strategy"""
        # Retrieve the latest regime data stored for the symbol
        regime_data = self.regimes.get(symbol)
        if not regime_data or "regime" not in regime_data:
            self.logger.warning(f"No regime data found for {symbol}, allowing strategy '{strategy_type}' by default.")
            return True  # Default to allowing trades if no regime data

        regime = regime_data["regime"]

        # Match strategy types to regimes
        self.logger.debug(f"Checking strategy suitability for {symbol}: Strategy='{strategy_type}', Regime='{regime}'")
        if strategy_type == "trend_following":
            is_suitable = "trending" in regime
        elif strategy_type == "mean_reversion":
            is_suitable = regime in ["ranging", "mixed"]
        elif strategy_type == "breakout":
            is_suitable = regime in ["ranging", "volatile"]
        elif strategy_type == "momentum":
             is_suitable = "momentum" in regime
        else:
            # Default strategy assumed to work in all regimes or strategy type unknown
            self.logger.warning(f"Unknown or default strategy type '{strategy_type}', allowing trade by default.")
            is_suitable = True

        self.logger.info(f"Strategy '{strategy_type}' suitability for {symbol} in regime '{regime}': {is_suitable}")
        return is_suitable

    async def clear_history(self, symbol: str):
        """Clear historical data and current regime for a symbol"""
        async with self._lock:
            if symbol in self.price_history: del self.price_history[symbol]
            if symbol in self.regime_history: del self.regime_history[symbol]
            if symbol in self.volatility_history: del self.volatility_history[symbol]
            if symbol in self.atr_history: del self.atr_history[symbol]
            if symbol in self.regimes: del self.regimes[symbol]
            self.logger.info(f"Cleared history and regime data for {symbol}")
            

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
                
            if "trending" in market_regime:
                config = self._trend_following_exits(entry_price, direction, atr_value, volatility_ratio)
            elif market_regime == "ranging":
                config = self._mean_reversion_exits(entry_price, direction, atr_value, volatility_ratio)
            elif market_regime == "volatile":
                config = self._volatile_market_exits(entry_price, direction, atr_value, volatility_ratio)
            else: 
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
            stop_loss = None # entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels (extend for trending markets)
            tp_level_1 = entry_price + (atr_value * 2.0 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 4.0 * volatility_ratio)
            tp_level_3 = entry_price + (atr_value * 6.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2, tp_level_3]
            
        else:  # SELL
            stop_loss = None # entry_price + (atr_value * atr_multiplier)
            
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
            "stop_loss": None,
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
            stop_loss = None # entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels (closer for mean reversion)
            tp_level_1 = entry_price + (atr_value * 1.0 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 2.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        else:  # SELL
            stop_loss = None # entry_price + (atr_value * atr_multiplier)
            
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
            "stop_loss": None,
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
            stop_loss = None # entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels (quick exit in volatile markets)
            tp_level_1 = entry_price + (atr_value * 1.5 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 3.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        else:  # SELL
            stop_loss = None # entry_price + (atr_value * atr_multiplier)
            
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
            "stop_loss": None,
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
            stop_loss = None # entry_price - (atr_value * atr_multiplier)
            
            # Take profit levels
            tp_level_1 = entry_price + (atr_value * 2.0 * volatility_ratio)
            tp_level_2 = entry_price + (atr_value * 3.0 * volatility_ratio)
            
            take_profit_levels = [tp_level_1, tp_level_2]
            
        else:  # SELL
            stop_loss = None # entry_price + (atr_value * atr_multiplier)
            
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
            "stop_loss": None,
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
                "stop_loss": None,
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
        self.position_tracker = None
        self.risk_manager = None
        self.volatility_monitor = None
        self.market_structure = None
        self.regime_classifier = None
        self.dynamic_exit_manager = None
        self.position_journal = None
        self.notification_system = None
        self.system_monitor = None

        # Track active alerts
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False

        # Broker reconciliation flag
        self.enable_reconciliation = config.enable_broker_reconciliation
        logger.info(f"Broker reconciliation on startup is set to: {self.enable_reconciliation}")

        # Override configuration
        self.enable_close_overrides = True
        self.override_timeframes = ["H1", "H4", "D1"]
        self.override_min_profit_pct = 0.5
        self.override_max_age_hours = 72
        self.override_symbols = []
        
        # Statistics tracking for overrides
        self.override_stats = {
            "total_overrides": 0,
            "successful_overrides": 0,
            "failed_overrides": 0
        }
        
        logger.info(f"Close signal overrides enabled: {self.enable_close_overrides}")

    async def start(self):
        """Initialize & start all components, including optional broker reconciliation."""
        if self._running:
            logger.info("EnhancedAlertHandler.start() called, but already running.")
            return True

        logger.info("Attempting to start EnhancedAlertHandler and its components...")
        try:
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

            self.dynamic_exit_manager = DynamicExitManager(position_tracker=self.position_tracker)
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

            # 5) Start components
            logger.info("Starting PositionTracker...")
            await self.position_tracker.start()
            await self.system_monitor.update_component_status("position_tracker", "ok")

            logger.info("Initializing RiskManager...")
            balance = await get_account_balance()
            await self.risk_manager.initialize(balance)
            await self.system_monitor.update_component_status("risk_manager", "ok")

            # Mark monitors OK (no explicit .start())
            await self.system_monitor.update_component_status("volatility_monitor", "ok")
            await self.system_monitor.update_component_status("regime_classifier", "ok")

            logger.info("Starting DynamicExitManager...")
            await self.dynamic_exit_manager.start()
            await self.system_monitor.update_component_status("dynamic_exit_manager", "ok")

            await self.system_monitor.update_component_status("position_journal", "ok")

            # 6) Broker reconciliation
            if self.enable_reconciliation and hasattr(self, "reconcile_positions_with_broker"):
                logger.info("Performing initial broker reconciliation...")
                await self.reconcile_positions_with_broker()
                logger.info("Initial broker reconciliation complete.")
            else:
                logger.info("Broker reconciliation skipped by configuration.")

            # Finalize startup
            self._running = True
            await self.system_monitor.update_component_status(
                "alert_handler", "ok", "EnhancedAlertHandler started successfully."
            )

            # Send a startup notification
            open_count = len(getattr(self.position_tracker, "positions", {}))
            await self.notification_system.send_notification(
                f"EnhancedAlertHandler started. Open positions: {open_count}.", "info"
            )

            return True

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

# ─── Module‐level instantiation ─────────────────────────────────────────────────
alert_handler = EnhancedAlertHandler()

# ─── FastAPI app and lifecycle wiring ────────────────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    success = await alert_handler.start()
    if not success:
        raise RuntimeError("EnhancedAlertHandler failed to start on application startup")

@app.post("/tradingview")
async def tradingview_webhook(request: Request):
    payload = await request.json()
    return await alert_handler.handle(payload)

    
    async def handle_scheduled_tasks(self):
        """
        Placeholder for any periodic background tasks.
        If you don’t have any scheduled work, this can just return immediately.
        """
        return  # or “await asyncio.sleep(0)” if you need to yield control

    async def stop(self):
        """
        Clean‐up hook called during shutdown.
        Close any open connections or cancel background tasks here.
        """
        return

            
    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an incoming alert"""
        async with self._lock: # Ensure self._lock is an asyncio.Lock initialized in __init__
            alert_id_from_data = alert_data.get("id", alert_data.get("request_id")) # Try 'id' then 'request_id'
            alert_id = alert_id_from_data if alert_id_from_data else str(uuid.uuid4()) # Generate if still not found
    
            # Use get_module_logger to create a contextual logger for this operation
            # Assuming get_module_logger is defined globally or accessible
            logger_instance = get_module_logger(
                __name__, 
                symbol=alert_data.get("symbol", "UNKNOWN"), 
                request_id=alert_id # Use the determined or generated alert_id as request_id for logs
            )
    
            try:
                # Extract key fields
                symbol = alert_data.get("symbol", "") # Original symbol from alert
                action = alert_data.get("action", "").upper()
                
                # Check for duplicate alerts using the final alert_id
                if alert_id in self.active_alerts:
                    logger_instance.warning(f"Duplicate alert ignored: {alert_id}")
                    return {
                        "status": "ignored",
                        "message": "Duplicate alert",
                        "alert_id": alert_id
                    }
                self.active_alerts.add(alert_id)
                
                if self.system_monitor:
                    await self.system_monitor.update_component_status(
                        "alert_handler", 
                        "processing",
                        f"Processing alert for {symbol} {action}"
                    )
                    
                try:
                    if action in ["BUY", "SELL"]:
                        # Extract necessary data for execute_trade from alert_data
                        # alert_data here is the mapped payload from tradingview_webhook
                        instrument_from_alert = alert_data.get("instrument", symbol) 
                        risk_percent_from_alert = float(alert_data.get('risk_percent', 1.0)) # Default if missing
                        timeframe_from_alert = alert_data.get("timeframe", "H1")
                        comment_from_alert = alert_data.get("comment")
                        account_from_alert = alert_data.get("account")

                        logger_instance.info(f"[EnhancedAlertHandler.process_alert] alert_data received: {alert_data}")
                        logger_instance.info(f"[EnhancedAlertHandler.process_alert] risk_percent_from_alert: {risk_percent_from_alert}, type: {type(risk_percent_from_alert)}")
                        
                        # Perform tradability check first
                        standardized_instrument = standardize_symbol(instrument_from_alert)
                        if not standardized_instrument:
                            logger_instance.error(f"Failed to standardize instrument: {instrument_from_alert}")
                            return {"status": "rejected", "message": f"Failed to standardize instrument: {instrument_from_alert}", "alert_id": alert_id}
    
                        tradeable, reason = is_instrument_tradeable(standardized_instrument)
                        logger_instance.info(f"Instrument {standardized_instrument} tradeable: {tradeable}, Reason: {reason}")
                        
                        if not tradeable:
                            logger_instance.warning(f"Market check failed: {reason}")
                            return {
                                "status": "rejected",
                                "message": f"Trading not allowed: {reason}",
                                "alert_id": alert_id
                            }
                            
                        # Prepare payload for execute_trade
                        # execute_oanda_order will fetch its own price if not provided by execute_trade,
                        # and will calculate units based on risk_percent and its dynamic allocation.
                        payload_for_execute_trade = {
                            "symbol": standardized_instrument,
                            "action": action,
                            "risk_percent": risk_percent_from_alert, # This should carry the intended risk
                            "timeframe": timeframe_from_alert,
                            "comment": comment_from_alert,
                            "account": account_from_alert,
                            "request_id": alert_id
                        }

                        logger_instance.info(f"[EnhancedAlertHandler.process_alert] payload_for_execute_trade before calling execute_trade: {json.dumps(payload_for_execute_trade)}")

                        success, result_dict = await execute_trade(payload_for_execute_trade)
                        
                        # The process_alert method should return the dictionary part
                        return result_dict
                        
                    elif action in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]:
                        # alert_data is already the payload expected by _process_exit_alert
                        return await self._process_exit_alert(alert_data)
                        
                    elif action == "UPDATE":
                        return await self._process_update_alert(alert_data)
                        
                    else:
                        logger_instance.warning(f"Unknown action type: {action}")
                        return {
                            "status": "error",
                            "message": f"Unknown action type: {action}",
                            "alert_id": alert_id
                        }
                        
                finally:
                    self.active_alerts.discard(alert_id)
                    if self.system_monitor:
                        await self.system_monitor.update_component_status("alert_handler", "ok", "")
                    
            except Exception as e:
                logger_instance.error(f"Error processing alert: {str(e)}", exc_info=True)
                if hasattr(self, 'error_recovery') and self.error_recovery: # Check if error_recovery is initialized
                    await self.error_recovery.record_error(
                        "alert_processing",
                        {"error": str(e), "alert": alert_data}
                    )
                return {
                    "status": "error",
                    "message": f"Internal error processing alert: {str(e)}",
                    "alert_id": alert_data.get("id", "unknown_id_on_error") # Use a fallback if alert_data itself is problematic
                }
                
                
    async def _process_entry_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an entry alert (BUY or SELL) with comprehensive error handling.
        
        Note: Stop losses are intentionally disabled in this implementation.
        All stop_loss parameters are accepted for compatibility but will be set to None.
        """
        request_id = str(uuid.uuid4())
        
        try:
            # Extract fields with validation
            if not alert_data:
                logger.error(f"[{request_id}] Empty alert data received")
                return {
                    "status": "rejected",
                    "message": "Empty alert data",
                    "alert_id": request_id
                }
                
            alert_id = alert_data.get("id", request_id)
            symbol = alert_data.get("symbol", "")
            action = alert_data.get("action", "").upper()
            percentage = float(alert_data.get("percentage", 1.0))
            timeframe = alert_data.get("timeframe", "H1")
            comment = alert_data.get("comment", "")
            
            # Validate essential fields
            if not symbol:
                logger.error(f"[{request_id}] Missing required field: symbol")
                return {
                    "status": "rejected",
                    "message": "Missing required field: symbol",
                    "alert_id": alert_id
                }
                
            if not action:
                logger.error(f"[{request_id}] Missing required field: action")
                return {
                    "status": "rejected",
                    "message": "Missing required field: action",
                    "alert_id": alert_id
                }
                
            if action not in ["BUY", "SELL"]:
                logger.error(f"[{request_id}] Invalid action for entry alert: {action}")
                return {
                    "status": "rejected",
                    "message": f"Invalid action for entry: {action}. Must be BUY or SELL",
                    "alert_id": alert_id
                }
            
            logger.info(f"[{request_id}] Processing entry alert: {symbol} {action} ({percentage}%)")
            
            # Standardize symbol
            standardized_symbol = standardize_symbol(symbol)
            logger.info(f"[{request_id}] Standardized symbol: {standardized_symbol}")
            
            # Check if trading is allowed
            is_tradeable, reason = is_instrument_tradeable(standardized_symbol)
            if not is_tradeable:
                logger.warning(f"[{request_id}] Trading not allowed for {standardized_symbol}: {reason}")
                return {
                    "status": "rejected",
                    "message": f"Trading not allowed: {reason}",
                    "alert_id": alert_id
                }
                
            # Calculate position parameters
            position_id = f"{standardized_symbol}_{action}_{uuid.uuid4().hex[:8]}"
            
            try:
                # Get account balance
                account_balance = await get_account_balance()
                
                # Update risk manager balance
                if self.risk_manager:
                    await self.risk_manager.update_account_balance(account_balance)
                    logger.info(f"[{request_id}] Updated risk manager with balance: {account_balance}")
            except Exception as e:
                logger.error(f"[{request_id}] Error getting account balance: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error getting account balance: {str(e)}",
                    "alert_id": alert_id
                }
            
            # Calculate risk
            risk_percentage = min(percentage / 100, config.max_risk_percentage / 100)
            
            # Check if risk is allowed
            if self.risk_manager:
                try:
                    is_allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage, standardized_symbol)
                    if not is_allowed:
                        logger.warning(f"[{request_id}] Trade rejected due to risk limits: {reason}")
                        return {
                            "status": "rejected",
                            "message": f"Risk check failed: {reason}",
                            "alert_id": alert_id
                        }
                except Exception as e:
                    logger.error(f"[{request_id}] Error in risk check: {str(e)}")
                    return {
                        "status": "error",
                        "message": f"Error in risk check: {str(e)}",
                        "alert_id": alert_id
                    }
            
            # Get current price
            try:
                price = alert_data.get("price")
                if price is None:
                    price = await get_current_price(standardized_symbol, action)
                    logger.info(f"[{request_id}] Got current price for {standardized_symbol}: {price}")
                else:
                    price = float(price)
                    logger.info(f"[{request_id}] Using provided price for {standardized_symbol}: {price}")
            except Exception as e:
                logger.error(f"[{request_id}] Error getting price for {standardized_symbol}: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error getting price: {str(e)}",
                    "alert_id": alert_id
                }
                    
            # Get ATR for later use with take profit calculations
            try:
                atr_value = await get_atr(standardized_symbol, timeframe)
                if atr_value <= 0:
                    logger.warning(f"[{request_id}] Invalid ATR value for {standardized_symbol}: {atr_value}")
                    atr_value = 0.0025  # Default fallback value
                
                logger.info(f"[{request_id}] ATR for {standardized_symbol}: {atr_value}")
                
                # Calculate instrument type and volatility multiplier for later use
                instrument_type = get_instrument_type(standardized_symbol)
                atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
                
                # Apply volatility adjustment if available
                volatility_multiplier = 1.0
                if self.volatility_monitor:
                    volatility_multiplier = self.volatility_monitor.get_stop_loss_modifier(standardized_symbol)
                    logger.info(f"[{request_id}] Volatility multiplier: {volatility_multiplier}")
                
            except Exception as e:
                logger.error(f"[{request_id}] Error calculating ATR: {str(e)}")
                atr_value = 0.0025  # Default value
                volatility_multiplier = 1.0
                instrument_type = get_instrument_type(standardized_symbol)
                atr_multiplier = 1.5  # Default multiplier
            
            # Calculate position size using percentage-based sizing
            try:
                # Percentage-based sizing without stop loss dependency
                position_size = account_balance * percentage / 100 / price
                logger.info(f"[{request_id}] Calculated position size: {position_size}")
                    
            except Exception as e:
                logger.error(f"[{request_id}] Error calculating position size: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error calculating position size: {str(e)}",
                    "alert_id": alert_id
                }
            
            # Execute trade with broker
            try:
                success, trade_result = await execute_trade({
                    "symbol": standardized_symbol,
                    "action": action,
                    "percentage": percentage,
                    "price": price,
                    "stop_loss": None,
                    "timeframe": timeframe, 
                    "comment": comment
                })
                
                if not success:
                    error_message = trade_result.get('error', 'Unknown error')
                    logger.error(f"[{request_id}] Failed to execute trade: {error_message}")
                    return {
                        "status": "error",
                        "message": f"Trade execution failed: {error_message}",
                        "alert_id": alert_id
                    }
                    
                logger.info(f"[{request_id}] Trade executed successfully: {json.dumps(trade_result)}")
                    
            except Exception as e:
                logger.error(f"[{request_id}] Error executing trade: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error executing trade: {str(e)}",
                    "alert_id": alert_id
                }
            
            # Record position in tracker
            try:
                if self.position_tracker:
                    # Extract metadata
                    metadata = {
                        "alert_id": alert_id,
                        "comment": comment,
                        "original_percentage": percentage,
                        "atr_value": atr_value,
                        "instrument_type": instrument_type,
                        "atr_multiplier": atr_multiplier
                    }
                    
                    # Add any additional fields from alert
                    for key, value in alert_data.items():
                        if key not in ["id", "symbol", "action", "percentage", "price", "comment", "timeframe"]:
                            metadata[key] = value
                            
                    # Record position
                    position_recorded = await self.position_tracker.record_position(
                        position_id=position_id,
                        symbol=standardized_symbol,
                        action=action,
                        timeframe=timeframe,
                        entry_price=price,
                        size=position_size,
                        stop_loss=None,  # Stop loss is disabled
                        take_profit=None,  # Will be set by exit manager
                        metadata=metadata
                    )
                    
                    if not position_recorded:
                        logger.warning(f"[{request_id}] Failed to record position in tracker")
                    else:
                        logger.info(f"[{request_id}] Position recorded in tracker: {position_id}")
                
            except Exception as e:
                logger.error(f"[{request_id}] Error recording position: {str(e)}")
                # Don't return error here - trade has already executed
            
            # Register with risk manager
            try:
                if self.risk_manager:
                    await self.risk_manager.register_position(
                        position_id=position_id,
                        symbol=standardized_symbol,
                        action=action,
                        size=position_size,
                        entry_price=price,
                        stop_loss=None,  # Stop loss is disabled
                        account_risk=risk_percentage,
                        timeframe=timeframe
                    )
                    logger.info(f"[{request_id}] Position registered with risk manager")
            except Exception as e:
                logger.error(f"[{request_id}] Error registering with risk manager: {str(e)}")
                
                
            # Initialize dynamic exits
            try:
                if self.dynamic_exit_manager:
                    # Get market regime
                    market_regime = "unknown"
                    if self.regime_classifier:
                        regime_data = self.regime_classifier.get_regime_data(standardized_symbol)
                        market_regime = regime_data.get("regime", "unknown")
                        
                    await self.dynamic_exit_manager.initialize_exits(
                        position_id=position_id,
                        symbol=standardized_symbol,
                        entry_price=price,
                        position_direction=action,
                        stop_loss=None,  # Stop loss is disabled
                        timeframe=timeframe
                    )
                    logger.info(f"[{request_id}] Dynamic exits initialized (market regime: {market_regime})")
            except Exception as e:
                logger.error(f"[{request_id}] Error initializing dynamic exits: {str(e)}")
                # Continue despite error
                
            # Record in position journal
            try:
                if self.position_journal:
                    # Get market regime and volatility state
                    market_regime = "unknown"
                    volatility_state = "normal"
                    
                    if self.regime_classifier:
                        regime_data = self.regime_classifier.get_regime_data(standardized_symbol)
                        market_regime = regime_data.get("regime", "unknown")
                        
                    if self.volatility_monitor:
                        vol_data = self.volatility_monitor.get_volatility_state(standardized_symbol)
                        volatility_state = vol_data.get("volatility_state", "normal")
                        
                    await self.position_journal.record_entry(
                        position_id=position_id,
                        symbol=standardized_symbol,
                        action=action,
                        timeframe=timeframe,
                        entry_price=price,
                        size=position_size,
                        strategy="primary",
                        stop_loss=None,  # Stop loss is disabled
                        market_regime=market_regime,
                        volatility_state=volatility_state,
                        metadata=metadata if 'metadata' in locals() else None
                    )
                    logger.info(f"[{request_id}] Position recorded in journal")
            except Exception as e:
                logger.error(f"[{request_id}] Error recording in position journal: {str(e)}")
                # Continue despite error
                
            # Send notification
            try:
                if self.notification_system:
                    await self.notification_system.send_notification(
                        f"New position opened: {action} {standardized_symbol} @ {price:.5f} (Risk: {risk_percentage*100:.1f}%)",
                        "info"
                    )
                    logger.info(f"[{request_id}] Position notification sent")
            except Exception as e:
                logger.error(f"[{request_id}] Error sending notification: {str(e)}")
                # Continue despite error
                
            logger.info(f"[{request_id}] Entry alert processing completed successfully")
                
            # Return successful result
            result = {
                "status": "success",
                "message": f"Position opened: {action} {standardized_symbol} @ {price}",
                "position_id": position_id,
                "symbol": standardized_symbol,
                "action": action,
                "price": price,
                "size": position_size,
                "stop_loss": None,  # Stop loss is disabled
                "alert_id": alert_id
            }
            
            # Merge with trade_result if available
            if isinstance(trade_result, dict):
                result.update({k: v for k, v in trade_result.items() if k not in result})
                
            return result
                
        except Exception as e:
            logger.error(f"[{request_id}] Unhandled exception in entry alert processing: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Internal error: {str(e)}",
                "alert_id": alert_data.get("id", "unknown")
            }

    async def _process_exit_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process exit alert with enhanced override logic, timeframe comparison for generic CLOSE,
        and resilient per-position processing.
        """
        alert_id = alert_data.get("id", str(uuid.uuid4()))
        symbol_from_alert = alert_data.get("symbol", "")
        action_from_alert = alert_data.get("action", "").upper()
        signal_timeframe = alert_data.get("timeframe")
    
        logger_instance = get_module_logger(__name__, symbol=symbol_from_alert, request_id=alert_id)
    
        standardized_symbol = standardize_symbol(symbol_from_alert)
        logger_instance(f"Close signal received: Original='{symbol_from_alert}', Standardized='{standardized_symbol}', Signal Action='{action_from_alert}', Signal Timeframe='{signal_timeframe}'")
    
        open_positions_for_symbol_dict = {}
        if self.position_tracker: # type: ignore
            all_open_positions_by_symbol = await self.position_tracker.get_open_positions() # type: ignore
            if standardized_symbol in all_open_positions_by_symbol:
                open_positions_for_symbol_dict = all_open_positions_by_symbol[standardized_symbol]
    
        if not open_positions_for_symbol_dict:
            logger_instance(f"WARNING: No open positions found for {standardized_symbol} to apply {action_from_alert} signal.")
            return {"status": "warning", "message": f"No open positions found for {standardized_symbol} to close.", "alert_id": alert_id}
    
        price_to_close_at = alert_data.get("price")
        if price_to_close_at is None:
            sample_pos_action_for_price_fetch = "BUY"
            for position_id_temp, pos_details_temp in open_positions_for_symbol_dict.items():
                pos_original_action_temp = pos_details_temp.get("action", "").upper()
                if (action_from_alert == "CLOSE" or
                        (action_from_alert == "CLOSE_LONG" and pos_original_action_temp == "BUY") or
                        (action_from_alert == "CLOSE_SHORT" and pos_original_action_temp == "SELL")):
                    sample_pos_action_for_price_fetch = pos_original_action_temp
                    break
    
            price_fetch_direction_for_close = "SELL" if sample_pos_action_for_price_fetch == "BUY" else "BUY"
            try:
                price_to_close_at, _ = await get_price_with_fallbacks(standardized_symbol, price_fetch_direction_for_close)
                logger_instance(f"Fetched current price {price_to_close_at} for closing {standardized_symbol}")
            except ValueError as e:
                logger_instance(f"ERROR: Failed to fetch price for closing {standardized_symbol}: {e}")
                return {"status": "error", "message": f"Failed to fetch price for closing: {e}", "alert_id": alert_id}
        else:
            try:
                price_to_close_at = float(price_to_close_at)
                logger_instance(f"Using provided price {price_to_close_at} for closing {standardized_symbol}")
            except ValueError:
                logger_instance(f"ERROR: Invalid price provided for closing {standardized_symbol}: {alert_data.get('price')}")
                return {"status": "error", "message": f"Invalid price provided: {alert_data.get('price')}", "alert_id": alert_id}
    
        positions_to_attempt_close_ids = []
    
        for position_id, pos_details in open_positions_for_symbol_dict.items():
            pos_original_action = pos_details.get("action", "").upper()
            pos_original_timeframe = pos_details.get("timeframe")
    
            match = False
            if action_from_alert == "CLOSE":
                if signal_timeframe and pos_original_timeframe:
                    if normalize_timeframe(signal_timeframe, target="OANDA") == normalize_timeframe(pos_original_timeframe, target="OANDA"):
                        match = True
                        logger_instance(f"Generic CLOSE for {standardized_symbol} on signal TF '{signal_timeframe}' matches open position {position_id} on TF '{pos_original_timeframe}'.")
                    else:
                        logger_instance(f"Generic CLOSE for {standardized_symbol} on signal TF '{signal_timeframe}' does NOT match open position {position_id} TF '{pos_original_timeframe}'. Skipping.")
                else:
                    match = True
                    logger_instance(f"WARNING: Generic CLOSE for {standardized_symbol}: Signal TF='{signal_timeframe}', Position TF='{pos_original_timeframe}'. Defaulting to close {position_id} due to missing timeframe info for comparison.")
            elif action_from_alert == "CLOSE_LONG" and pos_original_action == "BUY":
                match = True
            elif action_from_alert == "CLOSE_SHORT" and pos_original_action == "SELL":
                match = True
    
            if match:
                positions_to_attempt_close_ids.append(position_id)
    
        if not positions_to_attempt_close_ids:
            logger_instance(f"WARNING: No specific positions identified for closure matching {standardized_symbol} {action_from_alert} (Signal TF: {signal_timeframe}) criteria.")
            return {"status": "warning", "message": f"No specific positions to close for {standardized_symbol} {action_from_alert} (Signal TF: {signal_timeframe})", "alert_id": alert_id}
    
        closed_positions_results_list = []
        overridden_positions_details_list = []
    
        for position_id in positions_to_attempt_close_ids:
            try:  # Main try for processing each position
                position_data_for_this_id = open_positions_for_symbol_dict[position_id]
                should_override, override_reason = await self._should_override_close(position_id, position_data_for_this_id) # type: ignore
    
                if should_override:
                    logger_instance(f"OVERRIDING close signal for {position_id} - Reason: {override_reason}")
                    overridden_positions_details_list.append({
                        "position_id": position_id,
                        "symbol": position_data_for_this_id.get('symbol'),
                        "action": position_data_for_this_id.get('action'),
                        "override_reason": override_reason,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
    
                    if hasattr(self, 'override_stats') and self.override_stats: # type: ignore
                        self.override_stats["total_overrides"] += 1 # type: ignore
                    continue
    
                logger_instance(f"No override for {position_id}. Proceeding to close {position_data_for_this_id.get('action')} position with broker.")
    
                actual_exit_price_for_tracker = price_to_close_at
                exit_reason_for_tracker = f"{action_from_alert.lower()}_signal_broker_failed"
                is_pos_not_exist_error = False
    
                try:  # For broker close operation
                    broker_close_payload = {
                        "symbol": position_data_for_this_id.get("symbol"),
                        "position_id": position_id,
                        "action": position_data_for_this_id.get("action")
                    }
                    success_broker, broker_close_result = await close_position(broker_close_payload)
    
                    if success_broker:
                        actual_exit_price_for_tracker = broker_close_result.get("actual_exit_price", price_to_close_at) # type: ignore
                        exit_reason_for_tracker = f"{action_from_alert.lower()}_signal"
                    else:
                        error_msg_lower = str(broker_close_result.get("error", "")).lower() # type: ignore
                        details_lower = str(broker_close_result.get("details", "")).lower() # type: ignore
                        is_pos_not_exist_error = (
                            "closeout_position_doesnt_exist" in error_msg_lower or
                            "closeout_position_doesnt_exist" in details_lower or
                            "position requested to be closed out does not exist" in error_msg_lower or
                            "position requested to be closed out does not exist" in details_lower
                        )
                        if not is_pos_not_exist_error:
                            broker_error_message = broker_close_result.get('error', 'Unknown broker error') # type: ignore
                            logger_instance(f"ERROR: Broker close failed for {position_id}: {broker_error_message}")
                            raise TradingSystemError(f"Broker close failed: {broker_error_message}")
                
                    logger_instance(f"Position {position_id} broker interaction completed. Exit price for tracker: {actual_exit_price_for_tracker}")
    
                except TradingSystemError as e_broker_sys_error:
                    logger_instance(f"ERROR: Broker system error for {position_id}: {e_broker_sys_error}, exc_info=True")
                    raise
                
                except Exception as e_broker_close:
                    error_str_lower = str(e_broker_close).lower()
                    if "closeout_position_doesnt_exist" in error_str_lower or \
                       "position requested to be closed out does not exist" in error_str_lower or \
                       (locals().get("is_pos_not_exist_error") and is_pos_not_exist_error):
                        logger_instance(f"Position {position_id} didn't exist on broker side (or error indicates it). Setting for DB reconciliation. Error: {e_broker_close}")
                        is_pos_not_exist_error = True
                        exit_reason_for_tracker = "reconciliation_broker_not_found"
                        actual_exit_price_for_tracker = price_to_close_at
                    else:
                        logger_instance(f"ERROR: Unexpected error during broker close for position {position_id}: {e_broker_close}, exc_info=True")
                        raise
    
                if self.position_tracker: # type: ignore
                    if is_pos_not_exist_error:
                        logger_instance(f"Reconciling {position_id} in tracker as broker reported it doesn't exist or process failed indicating such.")
    
                    close_tracker_result_obj = await self.position_tracker.close_position( # type: ignore
                        position_id=position_id,
                        exit_price=actual_exit_price_for_tracker,
                        reason=exit_reason_for_tracker
                    )
    
                    if close_tracker_result_obj.success and close_tracker_result_obj.position_data:
                        closed_positions_results_list.append(close_tracker_result_obj.position_data)
                        if self.risk_manager: # type: ignore
                            await self.risk_manager.clear_position(position_id) # type: ignore
    
                        if self.position_journal: # type: ignore
                            market_regime = "unknown"
                            volatility_state = "normal"
                            current_symbol_for_context = position_data_for_this_id.get("symbol", standardized_symbol)
                            if self.regime_classifier: # type: ignore
                                regime_data = self.regime_classifier.get_regime_data(current_symbol_for_context) # type: ignore
                                market_regime = regime_data.get("regime", "unknown")
                            if self.volatility_monitor: # type: ignore
                                vol_data = self.volatility_monitor.get_volatility_state(current_symbol_for_context) # type: ignore
                                volatility_state = vol_data.get("volatility_state", "normal")
    
                            await self.position_journal.record_exit( # type: ignore
                                position_id,
                                actual_exit_price_for_tracker,
                                exit_reason_for_tracker,
                                close_tracker_result_obj.position_data.get("pnl", 0.0),
                                market_regime=market_regime,
                                volatility_state=volatility_state
                            )
                    else:
                        tracker_error_message = close_tracker_result_obj.error if close_tracker_result_obj else 'Tracker error object was None'
                        logger_instance(f"ERROR: Failed to close position {position_id} in tracker: {tracker_error_message}")
    
            except Exception as e_position_processing:
                logger_instance(f"ERROR: Error processing position {position_id} for close signal: {str(e_position_processing)}, exc_info=True")
                continue
    
        try:
            if self.notification_system: # type: ignore
                total_pnl = sum(p.get("pnl", 0.0) for p in closed_positions_results_list if p) # type: ignore
                level = "info" if total_pnl >= 0 else "warning"
                price_display = f"{price_to_close_at:.5f}" if isinstance(price_to_close_at, float) else str(price_to_close_at) # type: ignore
        
                if closed_positions_results_list and overridden_positions_details_list: # type: ignore
                    notif_message = (
                        f"Close Signal Results for {standardized_symbol}:\n" # type: ignore
                        f"✅ Closed {len(closed_positions_results_list)} positions @ {price_display} " # type: ignore
                        f"(Net P&L: {total_pnl:.2f})\n"
                        f"🚫 Overridden {len(overridden_positions_details_list)} positions" # type: ignore
                    )
                elif closed_positions_results_list: # type: ignore
                    notif_message = (
                        f"Closed {len(closed_positions_results_list)} positions for {standardized_symbol} " # type: ignore
                        f"@ {price_display} (Net P&L: {total_pnl:.2f})"
                    )
                elif overridden_positions_details_list: # type: ignore
                    notif_message = (
                        f"All {len(overridden_positions_details_list)} matching positions for " # type: ignore
                        f"{standardized_symbol} were overridden" # type: ignore
                    )
                else:
                    notif_message = (
                        f"No positions were ultimately closed or overridden for {standardized_symbol} from signal " # type: ignore
                        f"(Signal TF: {signal_timeframe}). Check logs if positions were expected." # type: ignore
                    )
        
                await self.notification_system.send_notification(notif_message, level) # type: ignore
        except Exception as e_notif:
            logger_instance(f"ERROR: Error sending notification: {str(e_notif)}, exc_info=True") # type: ignore
        
        if closed_positions_results_list or overridden_positions_details_list: # type: ignore
            return {
                "status": "success",
                "message": f"Processed close signal for {standardized_symbol}. Closed: {len(closed_positions_results_list)}, Overridden: {len(overridden_positions_details_list)}.", # type: ignore
                "closed_positions": closed_positions_results_list, # type: ignore
                "overridden_positions": overridden_positions_details_list, # type: ignore
                "total_closed": len(closed_positions_results_list), # type: ignore
                "total_overridden": len(overridden_positions_details_list), # type: ignore
                "symbol": standardized_symbol, # type: ignore
                "price_at_signal": price_to_close_at, # type: ignore
                "alert_id": alert_id # type: ignore
            }
        else:
            logger_instance(f"WARNING: No positions were closed or overridden for {standardized_symbol} {action_from_alert} (Signal TF: {signal_timeframe}) despite processing attempts. Check logs.") # type: ignore
            return {
                "status": "warning",
                "message": f"No positions were ultimately closed or overridden for {standardized_symbol}. Check logs for details.", # type: ignore
                "closed_positions": [],
                "overridden_positions": [],
                "total_closed": 0,
                "total_overridden": 0,
                "alert_id": alert_id # type: ignore
            }
        # End of the _process_exit_alert method.
        # The following methods should be at the same indentation level as _process_exit_alert,
        # assuming they are part of the same class.
        
        async def _should_override_close(self, position_id: str, position_data: Dict[str, Any]) -> tuple[bool, str]: # type: ignore
            """
            Determine if a close signal should be overridden based on multiple criteria
            Returns: (should_override: bool, reason: str)
            """
            # Check if overrides are globally enabled
            if not getattr(self, 'enable_close_overrides', True):
                return False, "overrides_disabled"
        
            # Check timeframe restrictions
            timeframe = position_data.get('timeframe', 'H1')
            if hasattr(self, 'override_timeframes') and timeframe not in self.override_timeframes: # type: ignore
                return False, f"timeframe_{timeframe}_not_eligible"
        
            # Check symbol restrictions
            symbol = position_data.get('symbol', '')
            if hasattr(self, 'override_symbols') and self.override_symbols and symbol not in self.override_symbols: # type: ignore
                return False, f"symbol_{symbol}_not_eligible"
        
            # Check position age
            if hasattr(self, 'override_max_age_hours'):
                try:
                    open_time_str = position_data.get('open_time')
                    if open_time_str:
                        open_time = datetime.fromisoformat(open_time_str.replace('Z', '+00:00')) # type: ignore
                        age_hours = (datetime.now(timezone.utc) - open_time).total_seconds() / 3600 # type: ignore
        
                        if age_hours > self.override_max_age_hours: # type: ignore
                            return False, f"position_too_old_{age_hours:.1f}h"
                except Exception as e:
                    logger.warning(f"Could not check position age: {str(e)}") # Use self.logger or appropriate logger
        
            # Check minimum profit requirement
            if hasattr(self, 'override_min_profit_pct'):
                pnl_pct = position_data.get('pnl_percentage', 0)
                if pnl_pct < self.override_min_profit_pct: # type: ignore
                    return False, f"insufficient_profit_{pnl_pct:.2f}%"
        
            # Check momentum using existing function
            try:
                has_momentum = await check_position_momentum(position_id) # Ensure check_position_momentum is defined/imported
                if not has_momentum:
                    return False, "no_momentum_detected"
        
                return True, "strong_momentum_confirmed" # This is the success path if momentum is detected
        
            except Exception as e:
                logger.error(f"Error checking momentum for {position_id}: {str(e)}") # Use self.logger or appropriate logger
                return False, f"momentum_check_error_{str(e)}" # Fallback if momentum check fails
        
        def _calculate_position_age_hours(self, position_data: Dict[str, Any]) -> float:
            """Calculate position age in hours"""
            try:
                open_time_str = position_data.get('open_time')
                if open_time_str:
                    open_time = datetime.fromisoformat(open_time_str.replace('Z', '+00:00'))
                    return (datetime.now(timezone.utc) - open_time).total_seconds() / 3600
            except Exception:  # Consider more specific exception handling
                # Or log the error: logger.warning(f"Could not parse open_time: {open_time_str}", exc_info=True)
                pass
            return 0.0
        
        async def _process_update_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
            """Process an update alert (update stop loss, take profit, etc.)"""
            alert_id = alert_data.get("id", str(uuid.uuid4()))
            symbol = alert_data.get("symbol", "")
            position_id = alert_data.get("position_id")
            stop_loss_input = alert_data.get("stop_loss") # Store original input
            take_profit_input = alert_data.get("take_profit") # Store original input
        
            # Initialize with None, will be set if valid input is provided
            stop_loss = None
            take_profit = None
        
            if position_id:
                if not self.position_tracker: # type: ignore
                    return {
                        "status": "error",
                        "message": "Position tracker not available",
                        "alert_id": alert_id
                    }
        
                position = await self.position_tracker.get_position_info(position_id) # type: ignore
        
                if not position:
                    return {
                        "status": "error",
                        "message": f"Position {position_id} not found",
                        "alert_id": alert_id
                    }
        
                if position.get("status") == "closed":
                    return {
                        "status": "error",
                        "message": f"Cannot update closed position {position_id}",
                        "alert_id": alert_id
                    }
        
                # Convert stop loss and take profit to float if provided
                # Note: The original code had 'stop_loss = None # float(stop_loss)'.
                # If the intention is to allow disabling SL by passing None, this is okay.
                # If it's to process an SL value, it should be `stop_loss = float(stop_loss_input)`
                if stop_loss_input is not None:
                    try:
                        stop_loss = float(stop_loss_input)
                    except ValueError:
                        logger.warning(f"Invalid stop_loss value for position {position_id}: {stop_loss_input}")
                        # Decide: return error or ignore invalid SL? For now, it will be None.
                # else: stop_loss remains None as initialized
        
                if take_profit_input is not None:
                    try:
                        take_profit = float(take_profit_input)
                    except ValueError:
                        logger.warning(f"Invalid take_profit value for position {position_id}: {take_profit_input}")
                        # Decide: return error or ignore invalid TP? For now, it will be None.
                # else: take_profit remains None as initialized
        
                success = await self.position_tracker.update_position( # type: ignore
                    position_id=position_id,
                    stop_loss=stop_loss, # Pass the processed value
                    take_profit=take_profit # Pass the processed value
                )
        
                if not success:
                    return {
                        "status": "error",
                        "message": f"Failed to update position {position_id}",
                        "alert_id": alert_id
                    }
        
                updated_position = await self.position_tracker.get_position_info(position_id) # type: ignore
        
                if self.position_journal: # type: ignore
                    if stop_loss is not None: # Check against processed value
                        await self.position_journal.record_adjustment( # type: ignore
                            position_id=position_id,
                            adjustment_type="stop_loss",
                            old_value=position.get("stop_loss"),
                            new_value=stop_loss,
                            reason="manual_update"
                        )
        
                    if take_profit is not None: # Check against processed value
                        await self.position_journal.record_adjustment( # type: ignore
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
        
            elif symbol:
                open_positions_for_symbol = {} # Renamed for clarity
                if self.position_tracker: # type: ignore
                    all_open = await self.position_tracker.get_open_positions() # type: ignore
                    if symbol in all_open:
                        open_positions_for_symbol = all_open[symbol]
        
                if not open_positions_for_symbol:
                    return {
                        "status": "warning",
                        "message": f"No open positions found for {symbol}",
                        "alert_id": alert_id
                    }
        
                if stop_loss_input is not None:
                    try:
                        stop_loss = float(stop_loss_input)
                    except ValueError:
                        logger.warning(f"Invalid stop_loss value for symbol {symbol}: {stop_loss_input}")
                # else: stop_loss remains None as initialized
        
                if take_profit_input is not None:
                    try:
                        take_profit = float(take_profit_input)
                    except ValueError:
                        logger.warning(f"Invalid take_profit value for symbol {symbol}: {take_profit_input}")
                # else: take_profit remains None as initialized
        
                updated_positions_list = [] # Renamed for clarity
        
                for pos_id, original_pos_data in open_positions_for_symbol.items():
                    success = await self.position_tracker.update_position( # type: ignore
                        position_id=pos_id,
                        stop_loss=stop_loss, # Pass processed value
                        take_profit=take_profit # Pass processed value
                    )
        
                    if success:
                        updated_pos_info = await self.position_tracker.get_position_info(pos_id) # type: ignore
                        updated_positions_list.append(updated_pos_info)
        
                        if self.position_journal: # type: ignore
                            if stop_loss is not None:
                                await self.position_journal.record_adjustment( # type: ignore
                                    position_id=pos_id,
                                    adjustment_type="stop_loss",
                                    old_value=original_pos_data.get("stop_loss"),
                                    new_value=stop_loss,
                                    reason="bulk_update"
                                )
        
                            if take_profit is not None:
                                await self.position_journal.record_adjustment( # type: ignore
                                    position_id=pos_id,
                                    adjustment_type="take_profit",
                                    old_value=original_pos_data.get("take_profit"),
                                    new_value=take_profit,
                                    reason="bulk_update"
                                )
        
                if updated_positions_list:
                    return {
                        "status": "success",
                        "message": f"Updated {len(updated_positions_list)} positions for {symbol}",
                        "positions": updated_positions_list,
                        "alert_id": alert_id
                    }
                else:
                    return {
                        "status": "error", # Or warning if some attempts were made but none succeeded
                        "message": f"Failed to update any positions for {symbol}",
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
        
            last_run = {
                "update_prices": datetime.now(timezone.utc),
                "check_exits": datetime.now(timezone.utc),
                "daily_reset": datetime.now(timezone.utc),
                "position_cleanup": datetime.now(timezone.utc),
                "database_sync": datetime.now(timezone.utc)
            }
        
            while self._running: # type: ignore
                try:
                    current_time = datetime.now(timezone.utc)
        
                    if (current_time - last_run["update_prices"]).total_seconds() >= 60:
                        await self._update_position_prices() # type: ignore
                        last_run["update_prices"] = current_time
        
                    if (current_time - last_run["check_exits"]).total_seconds() >= 300:
                        await self._check_position_exits() # type: ignore
                        last_run["check_exits"] = current_time
        
                    if current_time.day != last_run["daily_reset"].day:
                        await self._perform_daily_reset() # type: ignore
                        last_run["daily_reset"] = current_time
        
                    if (current_time - last_run["position_cleanup"]).total_seconds() >= 604800:  # 7 days
                        await self._cleanup_old_positions() # type: ignore
                        last_run["position_cleanup"] = current_time
        
                    if (current_time - last_run["database_sync"]).total_seconds() >= 3600:  # 1 hour
                        await self._sync_database() # type: ignore
                        last_run["database_sync"] = current_time
        
                    await asyncio.sleep(10)
        
                except Exception as e:
                    logger.error(f"Error in scheduled tasks: {str(e)}")
                    logger.error(traceback.format_exc())
        
                    if 'error_recovery' in globals() and error_recovery: # type: ignore
                        await error_recovery.record_error( # type: ignore
                            "scheduled_tasks",
                            {"error": str(e)}
                        )
        
                    await asyncio.sleep(60)
        
        async def _update_position_prices(self):
            """Update all open position prices"""
            if not self.position_tracker: # type: ignore
                return
        
            try:
                open_positions = await self.position_tracker.get_open_positions() # type: ignore
        
                updated_prices_symbols = {} # Renamed for clarity
                position_count = 0
        
                for symbol, positions_data in open_positions.items(): # Renamed for clarity
                    if not positions_data:
                        continue
        
                    any_position = next(iter(positions_data.values()))
                    direction = any_position.get("action")
        
                    try:
                        price = await get_current_price(symbol, "SELL" if direction == "BUY" else "BUY")
                        updated_prices_symbols[symbol] = price
        
                        if self.volatility_monitor: # type: ignore
                            timeframe = any_position.get("timeframe", "H1")
                            atr_value = await get_atr(symbol, timeframe) # type: ignore
                            await self.volatility_monitor.update_volatility(symbol, atr_value, timeframe) # type: ignore
        
                        if self.regime_classifier: # type: ignore
                            await self.regime_classifier.add_price_data(symbol, price, any_position.get("timeframe", "H1")) # type: ignore
        
                        for position_id in positions_data:
                            await self.position_tracker.update_position_price(position_id, price) # type: ignore
                            position_count += 1
        
                    except Exception as e:
                        logger.error(f"Error updating price for {symbol}: {str(e)}")
        
                if position_count > 0:
                    logger.debug(f"Updated prices for {position_count} positions across {len(updated_prices_symbols)} symbols")
        
            except Exception as e:
                logger.error(f"Error updating position prices: {str(e)}")
        
        async def _check_position_exits(self):
            """Check all positions for dynamic exit conditions based on DynamicExitManager rules"""
            if not self.position_tracker or not self.dynamic_exit_manager: # type: ignore
                return
        
            try:
                all_open_positions = await self.position_tracker.get_open_positions() # type: ignore
                if not all_open_positions:
                    return
        
                positions_checked = 0
                exits_triggered = 0
        
                for symbol, positions_in_symbol in all_open_positions.items(): # Renamed for clarity
                    for position_id, position_data in positions_in_symbol.items(): # Renamed for clarity
                        try:
                            if not position_data.get("current_price"):
                                continue
        
                            positions_checked += 1
        
                            if position_id not in self.dynamic_exit_manager.exit_levels: # type: ignore
                                continue
        
                            exit_config = self.dynamic_exit_manager.exit_levels[position_id] # type: ignore
                            current_price = position_data["current_price"]
                            # entry_price = position_data["entry_price"] # Unused in this scope
                            # action = position_data["action"] # Unused in this scope
        
                            if await self._check_take_profit_levels(position_id, position_data, exit_config, current_price): # type: ignore
                                exits_triggered += 1
                                continue
        
                            if await self._check_breakeven_stop(position_id, position_data, exit_config, current_price): # type: ignore
                                # Breakeven stop updates SL, doesn't necessarily exit, so don't increment exits_triggered unless it does.
                                # The _check_breakeven_stop returns False (meaning no exit)
                                pass # No exit, just potential SL update
        
                            if await self._check_time_based_exit(position_id, position_data, exit_config): # type: ignore
                                exits_triggered += 1
                                continue
        
                            if exit_config.get("override_enhanced", False):
                                if await self._check_enhanced_override_exits(position_id, position_data, exit_config, current_price): # type: ignore
                                    exits_triggered += 1
                                    continue
        
                        except Exception as e:
                            logger.error(f"Error checking exits for position {position_id}: {str(e)}")
                            continue # Continue with the next position
        
                if positions_checked > 0:
                    logger.debug(f"Checked dynamic exits for {positions_checked} positions, triggered {exits_triggered} exits")
        
            except Exception as e:
                logger.error(f"Error in dynamic exit checking: {str(e)}")
        
        async def _check_take_profit_levels(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any], current_price: float) -> bool:
            """Check if any take profit levels are hit. Returns True if an exit occurred."""
            try:
                if "take_profits" not in exit_config:
                    return False
        
                tp_config = exit_config["take_profits"]
                levels = tp_config.get("levels", [])
                action = position["action"]
                exit_occurred = False # Flag to track if any exit action was taken
        
                for i, level_config in enumerate(levels): # Renamed 'level' to 'level_config'
                    if level_config.get("hit", False):
                        continue
        
                    tp_price = level_config.get("price", 0)
                    percentage_to_close = level_config.get("percentage", 0) # Renamed for clarity
        
                    hit = False
                    if action == "BUY":
                        hit = current_price >= tp_price
                    else:  # SELL
                        hit = current_price <= tp_price
        
                    if hit:
                        logger.info(f"Take profit level {i+1} hit for {position_id} at {current_price}")
                        level_config["hit"] = True # Mark as hit in the config
                        close_success = False
        
                        if percentage_to_close < 100 and percentage_to_close > 0:
                            close_success, _ = await self.position_tracker.close_partial_position( # type: ignore
                                position_id, current_price, percentage_to_close, f"take_profit_level_{i+1}"
                            )
                        elif percentage_to_close >= 100: # Full close
                            close_success = await self._exit_position(position_id, current_price, f"take_profit_level_{i+1}") # type: ignore
                        
                        if close_success:
                            exit_occurred = True # An exit action (partial or full) was successful
                            if percentage_to_close >= 100: # If full close, no more TPs to check for this position
                                return True 
                        # If partial close was successful, loop continues for other TP levels on remaining position
        
                return exit_occurred # True if any partial/full close happened, False otherwise
        
            except Exception as e:
                logger.error(f"Error checking take profit levels for {position_id}: {str(e)}")
                return False
        
        async def _check_breakeven_stop(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any], current_price: float) -> bool:
            """Check if breakeven stop should be activated. Returns False as it only updates SL."""
            try:
                if "breakeven" not in exit_config:
                    return False
        
                be_config = exit_config["breakeven"]
                if be_config.get("activated", False): # Already activated
                    return False
        
                activation_price_level = be_config.get("activation_level", 0) # Renamed for clarity
                entry_price = position["entry_price"]
                action = position["action"]
        
                activated_now = False # Renamed for clarity
                if action == "BUY":
                    activated_now = current_price >= activation_price_level
                else:  # SELL
                    activated_now = current_price <= activation_price_level
        
                if activated_now:
                    logger.info(f"Breakeven stop activated for {position_id} at {current_price}")
                    be_config["activated"] = True # Update the config state
        
                    buffer_pips = be_config.get("buffer_pips", 0)
                    # Assuming standard pip definition (0.0001 for most FX, adjust if needed for other assets)
                    pip_value = 0.0001 # TODO: Make this configurable or symbol-dependent
                    
                    new_stop_loss = 0
                    if action == "BUY":
                        new_stop_loss = entry_price + (buffer_pips * pip_value)
                    else: #SELL
                        new_stop_loss = entry_price - (buffer_pips * pip_value)
        
                    if self.position_tracker: # type: ignore
                        await self.position_tracker.update_position( # type: ignore
                            position_id,
                            stop_loss=new_stop_loss,
                            # No take_profit change here
                            metadata={"breakeven_activated_at": datetime.now(timezone.utc).isoformat()} # type: ignore
                        )
                return False # Breakeven activation itself doesn't cause an immediate exit by this function
        
            except Exception as e:
                logger.error(f"Error checking breakeven stop for {position_id}: {str(e)}")
                return False
        
        async def _check_time_based_exit(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any]) -> bool:
            """Check if time-based exit should trigger. Returns True if an exit occurred."""
            try:
                if "time_exit" not in exit_config:
                    return False
        
                time_config = exit_config["time_exit"]
                exit_time_str = time_config.get("exit_time")
        
                if not exit_time_str:
                    return False
        
                exit_time = parse_iso_datetime(exit_time_str) # Use the helper
                current_time = datetime.now(timezone.utc)
        
                if current_time >= exit_time:
                    reason = time_config.get("reason", "time_based_exit")
                    logger.info(f"Time-based exit triggered for {position_id}")
                    current_price = position["current_price"] # Price should be up-to-date
                    return await self._exit_position(position_id, current_price, reason) # type: ignore
        
                return False
        
            except Exception as e:
                logger.error(f"Error checking time-based exit for {position_id}: {str(e)}")
                return False
        
        async def _check_enhanced_override_exits(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any], current_price: float) -> bool:
            """Check enhanced exit conditions for overridden positions. Returns True if an exit occurred."""
            try:
                # entry_price = position["entry_price"] # Unused
                action = position["action"]
                pnl_pct = position.get("pnl_percentage", 0.0) # Ensure float for comparison
        
                # Exit if position has reversed by specified percentage (e.g., -1.0%)
                reversal_threshold = exit_config.get("override_reversal_pct", -1.0)
                if pnl_pct <= reversal_threshold:
                    logger.info(f"Enhanced override exit: Position {position_id} reversed by {pnl_pct:.2f}% (threshold: {reversal_threshold:.2f}%)")
                    return await self._exit_position(position_id, current_price, "override_reversal") # type: ignore
        
                # Check momentum loss if regime classifier is available
                if hasattr(self, 'regime_classifier') and self.regime_classifier: # type: ignore
                    regime_data = self.regime_classifier.get_regime_data(position["symbol"]) # type: ignore
                    momentum = regime_data.get("momentum", 0.0) # Ensure float
                    momentum_reversal_threshold = exit_config.get("override_momentum_reversal_threshold", 0.001)
        
                    # Exit if momentum has reversed against the trade direction
                    if (action == "BUY" and momentum < -momentum_reversal_threshold) or \
                       (action == "SELL" and momentum > momentum_reversal_threshold):
                        logger.info(f"Enhanced override exit: Momentum reversed for {position_id} (Momentum: {momentum:.4f})")
                        return await self._exit_position(position_id, current_price, "momentum_reversal") # type: ignore
        
                return False
        
            except Exception as e:
                logger.error(f"Error checking enhanced override exits for {position_id}: {str(e)}")
                return False
        
        def _check_stop_loss(self, position: Dict[str, Any], current_price: float, stop_loss: float) -> bool: # Added stop_loss param
            """Check if stop loss is hit.
            NOTE: The original code had a hardcoded 'return False' making logic unreachable.
            This version assumes stop_loss is passed or retrieved correctly.
            """
            # Original line: logger.debug(f"Stop loss check skipped - functionality disabled")
            # Original line: return False # This made the rest unreachable
        
            if stop_loss is None: # No stop loss set for this position
                return False
        
            action = position.get("action", "").upper()
        
            if action == "BUY":
                return current_price <= stop_loss
            elif action == "SELL": # Explicitly SELL
                return current_price >= stop_loss
            return False # Should not happen if action is always BUY or SELL
        
        async def _exit_position(self, position_id: str, exit_price: float, reason: str) -> bool:
            """Exit a position with the given reason"""
            try:
                position = await self.position_tracker.get_position_info(position_id) # type: ignore
                if not position:
                    logger.warning(f"Position {position_id} not found for exit (reason: {reason})")
                    return False
        
                if position.get("status") == "closed":
                    logger.warning(f"Position {position_id} already closed (exit attempt reason: {reason})")
                    return False # Or True if considered successful as it's already in desired state
        
                symbol = position.get("symbol", "")
                # Assuming close_position is your broker interaction function
                success_broker, broker_result = await close_position({ # type: ignore
                    "symbol": symbol,
                    "position_id": position_id,
                    "action": position.get("action") # Broker might need original action
                    # "price": exit_price # Broker might accept a target price for market/limit close
                })
        
                if not success_broker:
                    logger.error(f"Failed to close position {position_id} with broker: {broker_result.get('error', 'Unknown error')}") # type: ignore
                    # Decide: should we still try to close in tracker? For now, returning False.
                    return False
        
                # If broker close is successful, then update internal state
                tracker_close_result = await self.position_tracker.close_position( # type: ignore
                    position_id=position_id,
                    exit_price=broker_result.get("actual_exit_price", exit_price), # Use actual price from broker if available
                    reason=reason
                )
        
                if not tracker_close_result.success: # type: ignore
                    logger.error(f"Failed to close position {position_id} in tracker: {tracker_close_result.error}") # type: ignore
                    # This is a state inconsistency: closed at broker but not in tracker. Critical error.
                    return False # Or raise an exception
        
                if self.risk_manager: # type: ignore
                    await self.risk_manager.clear_position(position_id) # type: ignore
        
                if self.position_journal: # type: ignore
                    market_regime = "unknown"
                    volatility_state = "normal"
        
                    if self.regime_classifier: # type: ignore
                        regime_data = self.regime_classifier.get_regime_data(symbol) # type: ignore
                        market_regime = regime_data.get("regime", "unknown")
        
                    if self.volatility_monitor: # type: ignore
                        vol_data = self.volatility_monitor.get_volatility_state(symbol) # type: ignore
                        volatility_state = vol_data.get("volatility_state", "normal")
        
                    await self.position_journal.record_exit( # type: ignore
                        position_id=position_id,
                        exit_price=tracker_close_result.position_data.get("exit_price", exit_price), # type: ignore
                        exit_reason=reason,
                        pnl=tracker_close_result.position_data.get("pnl", 0.0), # type: ignore
                        market_regime=market_regime,
                        volatility_state=volatility_state
                    )
        
                try:
                    if self.notification_system: # type: ignore
                        pnl = tracker_close_result.position_data.get("pnl", 0.0) # type: ignore
                        level = "info"
                        if pnl < 0: # Only change to warning for losses
                            level = "warning"
                        # Ensure exit_price in notification is consistently formatted
                        formatted_exit_price = f"{tracker_close_result.position_data.get('exit_price', exit_price):.5f}"
                        await self.notification_system.send_notification( # type: ignore
                            f"Position {position_id} closed: {symbol} @ {formatted_exit_price} (P&L: {pnl:.2f}, Reason: {reason})",
                            level
                        )
                except Exception as e_notify:
                    logger.error(f"Error sending notification for position {position_id} exit: {str(e_notify)}")
        
                logger.info(f"Position {position_id} exited at {tracker_close_result.position_data.get('exit_price', exit_price)} (Reason: {reason})") # type: ignore
                return True
        
            except Exception as e:
                logger.error(f"Error exiting position {position_id}: {str(e)}", exc_info=True)
                return False
        
        async def _perform_daily_reset(self):
            """Perform daily reset tasks"""
            try:
                logger.info("Performing daily reset tasks")
        
                if self.risk_manager: # type: ignore
                    await self.risk_manager.reset_daily_stats() # type: ignore
        
                if 'backup_manager' in globals() and backup_manager: # type: ignore
                    await backup_manager.create_backup(include_market_data=True, compress=True) # type: ignore
        
                if self.notification_system: # type: ignore
                    await self.notification_system.send_notification( # type: ignore
                        "Daily reset completed: Risk statistics reset and backup created",
                        "info"
                    )
        
            except Exception as e:
                logger.error(f"Error in daily reset: {str(e)}")
        
        async def _cleanup_old_positions(self):
            """Clean up old closed positions to prevent memory growth"""
            try:
                logger.info("Cleaning up old closed positions and backups.") # Added log
                if self.position_tracker: # type: ignore
                    await self.position_tracker.purge_old_closed_positions(max_age_days=30) # type: ignore
        
                if 'backup_manager' in globals() and backup_manager: # type: ignore
                    await backup_manager.cleanup_old_backups(max_age_days=60, keep_min=10) # type: ignore
                logger.info("Cleanup of old data finished.") # Added log
            except Exception as e:
                logger.error(f"Error cleaning up old positions/backups: {str(e)}")
        
        async def _sync_database(self):
            """Ensure all data is synced with the database"""
            try:
                logger.info("Starting database sync.") # Added log
                if self.position_tracker: # type: ignore
                    await self.position_tracker.sync_with_database() # type: ignore
                    await self.position_tracker.clean_up_duplicate_positions() # type: ignore
                logger.info("Database sync finished.") # Added log
            except Exception as e:
                logger.error(f"Error syncing database: {str(e)}")
        
        async def reconcile_positions_with_broker(self):
            """Reconcile positions between database and broker"""
            try:
                logger.info("Starting position reconciliation with OANDA...")
        
                r_positions = OpenPositions(accountID=OANDA_ACCOUNT_ID) # type: ignore # Requires oandapyV20.endpoints.positions.OpenPositions
                broker_positions_response = await robust_oanda_request(r_positions)
        
                r_trades = OpenTrades(accountID=OANDA_ACCOUNT_ID) # type: ignore # Requires oandapyV20.endpoints.trades.OpenTrades
                broker_trades_response = await robust_oanda_request(r_trades)
        
                broker_open_details = {}
        
                if 'trades' in broker_trades_response:
                    for trade in broker_trades_response['trades']:
                        instrument_raw = trade.get('instrument')
                        if not instrument_raw:
                            logger.warning(f"Trade {trade.get('id')} missing instrument, skipping.")
                            continue
                        instrument = standardize_symbol(instrument_raw)
                        units_str = trade.get('currentUnits', '0')
                        try:
                            units = float(units_str)
                        except (TypeError, ValueError):
                            logger.warning(f"Invalid units '{units_str}' for trade {trade.get('id')}, skipping.")
                            continue
                        
                        action = "BUY" if units > 0 else "SELL"
                        broker_key = f"{instrument}_{action}"
        
                        price_str = trade.get('price', '0')
                        try:
                            price = float(price_str)
                        except (TypeError, ValueError):
                            price = 0.0 # Or decide on a better default/error handling
                            logger.warning(f"Invalid price value '{price_str}' in trade {trade.get('id')}, using {price}.")
        
                        open_time_str = trade.get('openTime')
                        open_time_iso = datetime.now(timezone.utc).isoformat() # Default
                        if open_time_str:
                            try:
                                open_time_iso = parse_iso_datetime(open_time_str).isoformat()
                            except Exception as time_err:
                                logger.warning(f"Error parsing openTime '{open_time_str}' for trade {trade.get('id')}: {time_err}, using current time.")
                        
                        # Handling multiple trades for the same instrument/direction (e.g. FIFO hedging account)
                        # This simple logic picks the one with largest absolute units. More complex logic might be needed.
                        if broker_key not in broker_open_details or abs(units) > abs(broker_open_details[broker_key].get('units', 0)):
                            broker_open_details[broker_key] = {
                                "broker_trade_id": trade.get('id', ''),
                                "instrument": instrument,
                                "action": action,
                                "entry_price": price,
                                "units": abs(units),
                                "open_time": open_time_iso
                            }
        
                logger.info(f"Broker open positions (from trades endpoint): {json.dumps(broker_open_details, indent=2)}")
        
                db_open_positions_data = await self.position_tracker.db_manager.get_open_positions() # type: ignore
                db_open_positions_map = {}
        
                for p_data in db_open_positions_data: # Renamed 'p' to 'p_data'
                    symbol_db = p_data.get('symbol', '') # Renamed
                    action_db = p_data.get('action', '') # Renamed
                    if symbol_db and action_db:
                        db_key = f"{symbol_db}_{action_db}"
                        db_open_positions_map[db_key] = p_data # Store full position data
        
                logger.info(f"Database open positions before reconciliation (symbol_ACTION): {list(db_open_positions_map.keys())}")
        
                # Phase A: Positions in DB but not on Broker (stale DB entries)
                for db_key, db_pos_data in db_open_positions_map.items():
                    position_id = db_pos_data.get('position_id')
                    if not position_id: continue # Should not happen if DB data is consistent
        
                    if db_key not in broker_open_details:
                        logger.warning(f"Position {position_id} ({db_key}) is open in DB but not on OANDA. Attempting to close in DB.")
                        try:
                            # Use symbol directly from db_pos_data, which should be the correct, standardized one
                            full_symbol_for_price = db_pos_data.get('symbol')
                            if not full_symbol_for_price:
                                logger.error(f"Stale position {position_id} in DB has no symbol, cannot fetch price to close.")
                                continue
        
                            price_fetch_side_for_close = "SELL" if db_pos_data.get('action') == "BUY" else "BUY"
                            exit_price = await get_current_price(full_symbol_for_price, price_fetch_side_for_close)
        
                            if exit_price is not None:
                                close_result_obj = await self.position_tracker.close_position( # type: ignore
                                    position_id=position_id,
                                    exit_price=exit_price,
                                    reason="reconciliation_broker_not_found" # Changed reason
                                )
                                if close_result_obj.success: # type: ignore
                                    logger.info(f"Successfully closed stale position {position_id} ({db_key}) in DB.")
                                    if self.risk_manager: # type: ignore
                                        await self.risk_manager.clear_position(position_id) # type: ignore
                                else:
                                    logger.error(f"Failed to close stale position {position_id} ({db_key}) in DB: {close_result_obj.error}") # type: ignore
                            else:
                                logger.error(f"Cannot close stale position {position_id} ({db_key}): Failed to get current price for {full_symbol_for_price}")
                        except Exception as e_close_stale:
                            logger.error(f"Error during DB closure of stale position {position_id} ({db_key}): {e_close_stale}", exc_info=True)
        
                # Phase B: Positions on Broker but not in DB (or not marked open) - requires creating/updating DB entries
                # This part of the logic needs to be implemented based on how you want to handle discrepancies.
                # For example, if a position exists on the broker but not in your DB, you might want to:
                # 1. Create it in your DB.
                # 2. Or, if your system should be the master, close it on the broker.
                # logger.info("# --- Phase B: Positions on Broker but not in DB (or not marked open) --- Not Implemented ---")
                # For each broker_key, broker_detail in broker_open_details.items():
                # if broker_key not in db_open_positions_map:
                # logger.warning(f"Position {broker_detail['instrument']} {broker_detail['action']} exists on broker but not in DB. Reconciliation action needed.")
                # Potentially: await self.position_tracker.open_position(...) or similar
        
                logger.info("Position reconciliation with OANDA finished.")
        
            except oandapyV20.exceptions.V20Error as v20_err:
                logger.error(f"OANDA API error during reconciliation: {v20_err.msg} (Code: {v20_err.code})", exc_info=True)
            except Exception as e:
                logger.error(f"General error during position reconciliation: {str(e)}", exc_info=True)
        
        async def _determine_partial_close_percentage(self,
                                                      position_id: str,
                                                      position_data: Dict[str, Any],
                                                      override_reason: str,
                                                      exit_config: Optional[Dict[str, Any]] = None) -> float:
            """Determine what percentage to partially close based on override reason and exit strategy"""
            try:
                pnl_pct = position_data.get('pnl_percentage', 0.0)
                timeframe = position_data.get('timeframe', 'H1')
                # symbol = position_data.get('symbol', '') # Unused in this logic
        
                base_percentage = 0.0
                if "strong_momentum_confirmed" in override_reason:
                    if pnl_pct > 2.0:
                        base_percentage = 30.0
                    elif pnl_pct > 1.0:
                        base_percentage = 20.0
                    else:
                        base_percentage = 0.0 # Let it run if not much profit yet
        
                elif "higher_timeframe_aligned" in override_reason:
                    base_percentage = 15.0 if pnl_pct > 1.0 else 0.0
        
                else: # Default partial close for other override reasons
                    base_percentage = 25.0 if pnl_pct > 0.5 else 0.0
        
                # Adjust based on timeframe
                if timeframe in ["M1", "M5", "M15"]:
                    base_percentage *= 1.2
                elif timeframe in ["H4", "D1"]:
                    base_percentage *= 0.8
        
                # Adjust based on exit strategy if available
                if exit_config:
                    strategy = exit_config.get("strategy", "standard")
                    if strategy == "trend_following":
                        base_percentage *= 0.7
                    elif strategy == "mean_reversion":
                        base_percentage *= 1.3
                    # elif strategy == "breakout": base_percentage *= 1.0 (no change)
        
                return min(50.0, max(0.0, round(base_percentage, 2))) # Cap and round
        
            except Exception as e:
                logger.error(f"Error determining partial close percentage for {position_id}: {str(e)}")
                return 0.0
        
        async def _activate_dynamic_exit_monitoring(self, position_id: str, position_data: Dict[str, Any]):
            """Activate enhanced dynamic exit monitoring for an overridden position"""
            try:
                if not self.dynamic_exit_manager: # type: ignore
                    logger.warning(f"Dynamic exit manager not available for position {position_id}")
                    return
        
                symbol = position_data.get('symbol', '')
                entry_price = position_data.get('entry_price', 0.0) # Ensure float
                action = position_data.get('action', '')
                timeframe = position_data.get('timeframe', 'H1')
        
                if not all([symbol, entry_price, action, timeframe]): # Basic validation
                    logger.error(f"Cannot activate dynamic exits for {position_id}: missing essential data (symbol, entry_price, action, or timeframe).")
                    return
        
                # Check if position already has exit configuration, if not, initialize
                if position_id not in self.dynamic_exit_manager.exit_levels: # type: ignore
                    logger.info(f"Initializing dynamic exits for overridden position {position_id}")
                    await self.dynamic_exit_manager.initialize_exits( # type: ignore
                        position_id=position_id,
                        symbol=symbol,
                        entry_price=entry_price,
                        position_direction=action,
                        stop_loss=None,  # Overridden positions might not use a traditional SL initially
                        timeframe=timeframe,
                        strategy_type="override_enhanced" # Specific strategy type
                    )
        
                # Mark this position for enhanced monitoring, or update if already exists
                if position_id in self.dynamic_exit_manager.exit_levels: # type: ignore
                    self.dynamic_exit_manager.exit_levels[position_id]["override_enhanced"] = True # type: ignore
                    self.dynamic_exit_manager.exit_levels[position_id]["override_activated_at"] = datetime.now(timezone.utc).isoformat() # type: ignore
                    logger.info(f"Dynamic exit monitoring (enhanced) activated/updated for overridden position {position_id}")
                else:
                    logger.error(f"Failed to initialize or find dynamic exit config for {position_id} after attempting initialization.")
        
            except Exception as e:
                logger.error(f"Error activating dynamic exit monitoring for {position_id}: {str(e)}", exc_info=True)

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

##############################################################################
# API Endpoints
##############################################################################

@app.get("/test-db")
async def test_db():
    try:
        conn = await asyncpg.connect(config.database_url)
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        return {"status": "success", "postgres_version": version}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Global resources
alert_handler = None
error_recovery = None
db_manager = None
backup_manager = None


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

    # Log port information
    logger.info(f"Starting application on port {os.environ.get('PORT', 'default')}")

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


#Manual trade endpoint
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
            data["symbol"] = standardize_symbol(data["symbol"]) # type: ignore

            # Add timestamp and ensure action is uppercase
            data["timestamp"] = datetime.now(timezone.utc).isoformat()
            data["action"] = action_upper # Use the uppercased action

            # Ensure alert_id is present if needed by process_alert
            if "id" not in data:
                data["id"] = str(uuid.uuid4())
            
            # Add percentage to data if process_alert expects it (it's validated but not explicitly added before)
            data["percentage"] = percentage 

            result = await alert_handler.process_alert(data) # type: ignore
            return result # process_alert should return a Response object or a dict for JSONResponse
        else:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "Alert handler not initialized"}
            )
    except Exception as e:
        logger.error(f"Error processing manual trade: {str(e)}") # type: ignore
        logger.error(traceback.format_exc()) # type: ignore
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

from fastapi import Request, status
from fastapi.responses import JSONResponse

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

# Close position endpoint
@app.post("/api/positions/{position_id}/close", tags=["positions"])
async def api_close_position(position_id: str, request: Request):
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
            "stop_loss": None,
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

@app.post("/api/admin/cleanup-positions")
async def cleanup_positions():
    """Admin endpoint to clean up stale positions"""
    try:
        if not db_manager:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "message": "Database not initialized"}
            )
            
        async with db_manager.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE positions 
                SET status = 'closed', 
                    close_time = CURRENT_TIMESTAMP, 
                    exit_reason = 'manual_cleanup' 
                WHERE status = 'open'
                """
            )
            
        return {
            "status": "success", 
            "message": "Cleaned up stale positions", 
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error cleaning up positions: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": str(e)}
        )

# — Load OANDA Credentials —
OANDA_ACCESS_TOKEN = os.getenv('OANDA_ACCESS_TOKEN')
OANDA_ENVIRONMENT = os.getenv('OANDA_ENVIRONMENT', 'practice')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')

if not (OANDA_ACCESS_TOKEN and OANDA_ACCOUNT_ID):
    config = configparser.ConfigParser()
    config.read('config.ini')
    try:
        OANDA_ACCESS_TOKEN = OANDA_ACCESS_TOKEN or config.get('oanda', 'access_token')
        OANDA_ENVIRONMENT = OANDA_ENVIRONMENT or config.get('oanda', 'environment')
        OANDA_ACCOUNT_ID = OANDA_ACCOUNT_ID or config.get('oanda', 'account_id')
    except configparser.NoSectionError:
        raise RuntimeError("Missing OANDA credentials: set env vars or config.ini")

oanda = oandapyV20.API(
    access_token=OANDA_ACCESS_TOKEN,
    environment=OANDA_ENVIRONMENT
)

@app.post("/tradingview")
async def tradingview_webhook(request: Request):
    """Process TradingView webhook alerts with improved error handling and mapping"""
    request_id = str(uuid.uuid4())
    
    try:
        # Get the raw JSON payload
        payload = await request.json()
        logger.info(f"[{request_id}] Received TradingView webhook: {json.dumps(payload, indent=2)}")
        
        # Special handling for JPY pairs
        if "symbol" in payload and "JPY" in payload["symbol"]:
            # Handle 6-character format like GBPJPY
            if len(payload["symbol"]) == 6:
                payload["symbol"] = payload["symbol"][:3] + "_" + payload["symbol"][3:]
                logger.info(f"[{request_id}] Formatted JPY pair to: {payload['symbol']}")
        
        # Map incoming fields to internal format with comprehensive field mapping
        alert_data = {}
        
        # Core fields with multiple fallback options
        alert_data['instrument'] = payload.get('symbol', payload.get('ticker', ''))
        alert_data['direction'] = payload.get('action', payload.get('side', payload.get('type', '')))
        
        # Handle various risk percentage fields
        if 'percentage' in payload:
            alert_data['risk_percent'] = float(payload.get('percentage', 0))
        elif 'risk' in payload:
            alert_data['risk_percent'] = float(payload.get('risk', 0))
        elif 'risk_percent' in payload:
            alert_data['risk_percent'] = float(payload.get('risk_percent', 0))
        else:
            alert_data['risk_percent'] = 1.0  # Default to 1%
            
        # Handle timeframe with normalization
        tf_raw = payload.get('timeframe', payload.get('tf', '1H'))
        alert_data['timeframe'] = normalize_timeframe(tf_raw)
        
        # Map other fields directly
        alert_data['exchange'] = payload.get('exchange')
        alert_data['account'] = payload.get('account')
        alert_data['comment'] = payload.get('comment')
        alert_data['strategy'] = payload.get('strategy')
        
        # Add request ID
        alert_data["request_id"] = request_id
        
        # Add debug log for mapped data
        logger.info(f"[{request_id}] Mapped alert data: {json.dumps(alert_data)}")
        
        # Validate required fields
        if not alert_data.get('instrument') or not alert_data.get('direction'):
            logger.error(f"[{request_id}] Missing required fields after mapping")
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "Missing required instrument or direction fields"}
            )
        
        # Process the alert with the mapped data
        # assume you injected or imported your handler instance as `alert_handler`
        result = await alert_handler.process_alert(alert_data)

        logger.info(f"[{request_id}] Alert processing result: {json.dumps(result)}")
        return JSONResponse(content=result)
            
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in webhook payload: {str(e)}"
        logger.error(f"[{request_id}] {error_msg}")
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": error_msg}
        )
    except Exception as e:
        error_msg = f"Error processing TradingView webhook: {str(e)}"
        logger.error(f"[{request_id}] {error_msg}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": error_msg}
        )


@app.get("/api/test-oanda", tags=["system"])
async def test_oanda_connection():
    """Test OANDA API connection"""
    try:
        if not oanda:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "message": "OANDA client not initialized"}
            )
            
        # Test getting account info
        from oandapyV20.endpoints.accounts import AccountSummary
        account_request = AccountSummary(accountID=OANDA_ACCOUNT_ID)
        
        response = oanda.request(account_request)
        
        # Mask sensitive data
        if "account" in response and "balance" in response["account"]:
            balance = float(response["account"]["balance"])
        else:
            balance = None
            
        return {
            "status": "ok",
            "message": "OANDA connection successful",
            "account_id": OANDA_ACCOUNT_ID,
            "environment": OANDA_ENVIRONMENT,
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

async def validate_system_state():
    """Validate the system state and log any issues."""
    logger.info("Validating system state...")
    issues = []
    
    # Check essential configuration
    if not config.oanda_access_token or not isinstance(config.oanda_access_token, SecretStr):
        issues.append("Missing or invalid OANDA access token")
    
    if not config.oanda_account:
        issues.append("Missing OANDA account ID")
    
    # Check database configuration
    try:
        if not config.database_url:
            issues.append("Missing database URL")
        elif 'postgres' not in config.database_url.lower():
            issues.append("Database URL does not appear to be for PostgreSQL")
    except Exception as e:
        issues.append(f"Error validating database configuration: {str(e)}")
    
    # Test OANDA connection
    try:
        from oandapyV20.endpoints.accounts import AccountSummary
        account_request = AccountSummary(accountID=OANDA_ACCOUNT_ID)
        
        response = oanda.request(account_request)
        logger.info("OANDA connection test successful")
    except Exception as e:
        issues.append(f"OANDA connection test failed: {str(e)}")
    
    # Test database connection
    if db_manager:
        try:
            async with db_manager.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    issues.append("Database connection test returned unexpected result")
                else:
                    logger.info("Database connection test successful")
        except Exception as e:
            issues.append(f"Database connection test failed: {str(e)}")
    else:
        issues.append("Database manager not initialized")
    
    # Log results
    if issues:
        for issue in issues:
            logger.error(f"System validation issue: {issue}")
        logger.warning(f"System validation completed with {len(issues)} issues")
    else:
        logger.info("System validation completed successfully with no issues")
    
    return issues

# Main entry point
if __name__ == "__main__":
    import uvicorn
    
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))

    logger.info(f"Attempting to start Uvicorn server on {host}:{port}")
    print(f"Starting Uvicorn on {host}:{port}")

    uvicorn.run("main:app", host=host, port=port, reload=False)
