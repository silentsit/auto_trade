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
import re
from datetime import datetime, timedelta, timezone
from oandapyV20.endpoints import instruments
from oandapyV20.exceptions import V20Error
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.accounts import AccountSummary # For balance fetching
from oandapyV20.endpoints.pricing import PricingInfo # Alternative price fetching if needed
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple, NamedTuple, Callable, TypeVar, ParamSpec
from fastapi import FastAPI, Query, Request, status, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from oandapyV20.endpoints import instruments
from pydantic import BaseModel, Field, SecretStr
from urllib.parse import urlparse
from functools import wraps

# Add this near the beginning of your code, with your other imports and class definitions
class ClosePositionResult(NamedTuple):
    success: bool
    position_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class OrderResult(TypedDict, total=False):
    success: bool
    order_id: Optional[str]
    instrument: Optional[str]
    direction: Optional[str]
    entry_price: Optional[float]
    units: Optional[int]
    stop_loss: Optional[float]           # The SL level submitted/accepted
    take_profit: Optional[float]         # The TP level submitted/accepted
    warning: Optional[str]
    error: Optional[str]
    details: Optional[Any]
    sl_omitted_due_to_rejection: bool # Flag if SL was omitted
    tp_omitted_due_to_rejection: bool # Flag if TP was omitted

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
# Configuration Management
##############################################################################

class Config(BaseModel):
    """Configuration settings for the application."""

    # API and connection settings
    host: str = Field(default=os.environ.get("HOST", "0.0.0.0"), description="Server host address")
    port: int = Field(default=int(os.environ.get("PORT", 8000)), description="Server port")
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
MAX_POSITIONS_PER_SYMBOL = 20

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
    'default': 20,  # Default leverage for other instruments
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
    "BTC": 0.25,
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
        # Convert to uppercase and handle common separators
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')
        
        # Direct crypto mapping
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
        
        if symbol_upper in CRYPTO_MAPPING:
            return CRYPTO_MAPPING[symbol_upper]
        
        # If already contains underscore, return as is
        if "_" in symbol_upper:
            return symbol_upper
        
        # For 6-character forex pairs (like EURUSD), split into base/quote
        if len(symbol_upper) == 6 and not any(c in symbol_upper for c in ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']):
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"
        
        # Handle crypto pairs that weren't in the direct mapping
        for crypto in ["BTC", "ETH", "LTC", "XRP", "BCH", "DOT", "ADA", "SOL"]:
            if crypto in symbol_upper and "USD" in symbol_upper:
                return f"{crypto}_USD"
        
        # Check if active_exchange config is available for broker-specific formatting
        active_exchange = getattr(config, "active_exchange", "").lower() if 'config' in globals() else "oanda"
        
        if active_exchange == "oanda":
            # OANDA uses underscore format (EUR_USD)
            return symbol_upper
        elif active_exchange == "binance":
            # Binance uses no separator (EURUSD)
            return symbol_upper.replace("_", "")
        
        # Default return if no transformation applied
        return symbol_upper
    
    except Exception as e:
        logger.error(f"Error standardizing symbol {symbol}: {str(e)}")
        # Return original symbol if standardization fails
        return symbol.upper() if symbol else ""


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

# Replace the existing normalize_timeframe function with this corrected version
import re # Ensure re is imported at the top of your file

def normalize_timeframe(tf: str, *, target: str = "OANDA") -> str:
    """
    Normalize timeframes into valid OANDA/Binance formats.
    Handles various inputs including TradingView numeric codes.
    Ensures "1" maps to "1H". Correctly maps normalized keys to OANDA values.
    """
    try:
        tf_original = tf # Keep original for logging if needed
        tf = str(tf).strip().upper()

        # Standardize common variations
        tf = tf.replace("MIN", "M").replace("MINS", "M")
        tf = tf.replace("HOUR", "H").replace("HOURS", "H")
        tf = tf.replace("DAY", "D").replace("DAYS", "D")
        tf = tf.replace("WEEK", "W").replace("WEEKS", "W")
        # Standardize Month consistently to MON to avoid clash with Minute M
        tf = tf.replace("MONTH", "MON").replace("MONTHS", "MON").replace("MN", "MON")

        # Mapping for TradingView numeric codes and common variants to a standard intermediate format
        standard_map = {
            "1": "1H",   # User request: 1 -> 1 Hour
            "3": "3M", "5": "5M", "15": "15M", "30": "30M",
            "60": "1H", "120": "2H", "180": "3H", "240": "4H", "360": "6H", "480": "8H", "720": "12H",
            "D": "1D", "1D": "1D",
            "W": "1W", "1W": "1W",
            "M": "1M",   # Assume standalone 'M' is Minute unless standardized to MON
            "MON": "1MON" # Map standardized MON to 1MON intermediate
        }

        # Intermediate formats we aim for before final mapping
        intermediate_formats = ["1M", "3M", "5M", "15M", "30M", "1H", "2H", "3H", "4H", "6H", "8H", "12H", "1D", "1W", "1MON"]
        normalized = None

        if tf in intermediate_formats:
            normalized = tf
        elif tf in standard_map:
            normalized = standard_map[tf]
        # Handle direct inputs like H1, M15 etc. if they weren't an intermediate format target
        elif tf in ["M1", "M3", "M5", "M15", "M30", "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D", "W", "M"]:
             # If it's already a direct OANDA format, map it back to our intermediate standard where possible
             reverse_oanda_map = {
                 "M1":"1M", "M5":"5M", "M15":"15M", "M30":"30M",
                 "H1":"1H", "H4":"4H", "H12":"12H", # Add others as needed
                 "D":"1D", "W":"1W", "M":"1MON"
             }
             if tf in reverse_oanda_map:
                  normalized = reverse_oanda_map[tf]
             else: # If it's like H2, H3, etc., treat it as already normalized intermediate
                  normalized = tf

        # If still not normalized, log warning and default
        if not normalized:
            # Final check for patterns like '30m', '4h' etc. before defaulting
            match = re.match(r"(\d+)([MDWHMON])", tf)
            if match:
                 num = int(match.group(1))
                 unit = match.group(2)
                 potential_norm = f"{num}{unit}"
                 if potential_norm in intermediate_formats:
                      normalized = potential_norm
                 # Handle conversions like 60M -> 1H
                 elif unit == 'M' and num >= 60 and num % 60 == 0 and f"{num // 60}H" in intermediate_formats:
                      normalized = f"{num // 60}H"
                 elif unit == 'H' and num >= 24 and num % 24 == 0 and f"{num // 24}D" in intermediate_formats:
                       normalized = f"{num // 24}D"

            if not normalized:
                 logger.warning(f"[TF-NORMALIZE] Unknown timeframe '{tf_original}' (processed as '{tf}'), defaulting to '1H'")
                 normalized = "1H" # Default to 1H

        # --- Convert intermediate normalized format to target format ---
        if target == "OANDA":
            # Correct OANDA mapping: keys are intermediate formats, values are OANDA formats
            oanda_map = {
                "1M": "M1", "3M": "M3", "5M": "M5", "15M": "M15", "30M": "M30",
                "1H": "H1", "2H": "H2", "3H": "H3", "4H": "H4", "6H": "H6", "8H": "H8", "12H": "H12",
                "1D": "D",
                "1W": "W",
                "1MON": "M" # Map our intermediate Month '1MON' to OANDA 'M'
            }

            if normalized in oanda_map:
                return oanda_map[normalized]
            else:
                # Maybe normalized is already H2, H3 etc. which are valid OANDA formats
                valid_oanda_formats = ["M1", "M3", "M5", "M15", "M30", "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D", "W", "M"]
                if normalized in valid_oanda_formats:
                     return normalized
                else:
                     logger.warning(f"[TF-NORMALIZE] Normalized timeframe '{normalized}' could not be mapped to OANDA, using H1.")
                     return "H1" # Default OANDA granularity

        elif target == "BINANCE":
            # Simplified example for Binance - requires adjustment based on exact needs
            binance_map = {
                 "1M":"1m", "5M":"5m", "15M":"15m", "30M":"30m", "1H":"1h", "4H":"4h", "1D":"1d", "1W":"1w", "1MON":"1M"
            }
            return binance_map.get(normalized, "1h") # Default to 1h for Binance

        else:
            logger.warning(f"[TF-NORMALIZE] Unknown target '{target}', returning intermediate normalized value: {normalized}")
            return normalized # Return the intermediate format if target is unknown
    except Exception as e:
        logger.error(f"Error normalizing timeframe: {str(e)}")
        return "H1"  # Return a safe default


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

from typing import Optional, Literal
from pydantic import BaseModel, Field, validator, constr, confloat
import re
import uuid

# Replace the existing TradingViewAlertPayload class (around line 451) with this version

class TradingViewAlertPayload(BaseModel):
    """Validated TradingView webhook payload - now includes known extra fields"""
    instrument: constr(strip_whitespace=True, min_length=3) = Field(..., description="Trading instrument (e.g., EURUSD, BTCUSD)")
    direction: Literal["BUY", "SELL", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT"] = Field(..., description="Trade direction")
    risk_percent: confloat(gt=0, le=100) = Field(..., description="Risk percentage for the trade (0 < x <= 100)")
    timeframe: str = Field(default="1H", description="Timeframe for the trade")
    entry_price: Optional[float] = Field(None, description="Entry price (optional)")
    stop_loss: Optional[float] = Field(None, description="Stop loss price (optional)")
    take_profit: Optional[float] = Field(None, description="Take profit price (optional)")
    comment: Optional[str] = Field(None, description="Additional comment for the trade")
    strategy: Optional[str] = Field(None, description="Strategy name")
    request_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique request ID")

    # --- Added extra fields from TradingView webhook ---
    exchange: Optional[str] = Field(None, description="Exchange name (from webhook)")
    account: Optional[str] = Field(None, description="Account ID (from webhook)")
    orderType: Optional[str] = Field(None, description="Order type (from webhook)")
    timeInForce: Optional[str] = Field(None, description="Time in force (from webhook)")
    # -------------------------------------------------

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

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        # Keep 'forbid' to ensure only defined fields (including the added optional ones) are allowed
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
    try:
        oanda_granularity = normalize_timeframe(timeframe, target="OANDA")
        params = {
            "granularity": oanda_granularity,
            "count": count,
            "price": "M"  # mid prices
        }
        r = instruments.InstrumentsCandles(instrument=symbol, params=params)
        resp = await oanda.request(r)

        if "candles" in resp:
            return {"candles": resp["candles"]}
        else:
            logger.error(f"[OANDA] No candles returned for {symbol}")
            return {"candles": []}  # <- not synth candles!
    
    except Exception as e:
        logger.error(f"[OANDA] Error fetching candles for {symbol}: {str(e)}")
        return {"candles": []}  # <- not synth candles!


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
            params = {"granularity": oanda_granularity, "count": period + 5, "price": "M"} # Get a few extra candles
            req = instruments.InstrumentsCandles(instrument=symbol, params=params)
            # ----- CORRECTED LINE: Removed 'await' -----
            response = oanda.request(req)
            # -------------------------------------------

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

# Function to process TradingView alerts
async def process_tradingview_alert(payload: dict) -> dict:
    """Process TradingView alert payload and return a response."""
    request_id = payload.get("request_id", str(uuid.uuid4()))
    symbol = payload.get('instrument', 'UNKNOWN')
    # Make sure get_module_logger is defined and accessible
    logger = get_module_logger(__name__, request_id=request_id, symbol=symbol)

    logger.info("Processing TradingView alert", extra={
        "payload": payload,
        "market_session": get_current_market_session() # Ensure get_current_market_session is defined
    })

    try:
        # Extract key fields
        instrument = payload['instrument']
        direction = payload['direction']
        risk_percent = float(payload['risk_percent'])

        # Get timeframe from payload, with default
        timeframe = payload.get('timeframe', '1H')
        # Ensure normalize_timeframe is defined and accessible
        normalized_tf = normalize_timeframe(timeframe, target="OANDA")
        payload['timeframe'] = normalized_tf # Update payload if needed elsewhere

        # Check market hours
        # Ensure is_instrument_tradeable is defined and accessible
        tradeable, reason = is_instrument_tradeable(instrument)
        if not tradeable:
            logger.warning(f"[{request_id}] Market not tradeable: {reason}")
            return {"success": False, "error": f"Market not tradeable: {reason}"}

        # Get current market session
        current_session = get_current_market_session()
        logger.info(f"[{request_id}] Current market session: {current_session}")

        # Get current price if not provided
        entry_price = payload.get('entry_price')
        if entry_price is None:
            try:
                # Ensure get_current_price is defined and accessible
                entry_price = await get_current_price(instrument, direction)
                logger.info(f"[{request_id}] Got current price for {instrument}: {entry_price}")
            except Exception as e:
                logger.error(f"[{request_id}] Error getting price: {str(e)}")
                return {"success": False, "error": f"Error getting price: {str(e)}"}
        else:
            entry_price = float(entry_price)

        # Get ATR for stop loss calculation
        try:
            # Ensure get_atr is defined and accessible
            atr = await get_atr(instrument, timeframe)
            if atr <= 0:
                logger.warning(f"[{request_id}] Invalid ATR value: {atr}, using default")
                # Use default ATR values based on instrument type
                # Ensure get_instrument_type is defined and accessible
                instrument_type = get_instrument_type(instrument)
                if instrument_type == "CRYPTO":
                    atr = 0.02  # 2% for crypto
                elif instrument_type == "FOREX":
                    atr = 0.0025  # 25 pips for forex
                else:
                    atr = 0.01  # Default fallback
        except Exception as e:
            logger.error(f"[{request_id}] Error getting ATR: {str(e)}")
            # Use fallback values
            atr = 0.01

        logger.info(f"[{request_id}] Using ATR value: {atr} for {instrument}")

        # Analyze market structure for potential support/resistance levels
        # Ensure alert_handler and its market_structure component are initialized
        market_structure = None # Default to None
        if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'market_structure'):
            try:
                market_structure = await alert_handler.market_structure.analyze_market_structure(
                    instrument, timeframe, entry_price, entry_price * 0.99, entry_price
                )
                logger.info(f"[{request_id}] Market structure analysis complete")
            except Exception as e:
                logger.error(f"[{request_id}] Error analyzing market structure: {str(e)}")
                market_structure = None
        else:
            logger.warning(f"[{request_id}] Alert handler or market structure analyzer not available.")


        # Calculate stop loss using structure-based method with ATR fallback
        stop_loss = payload.get('stop_loss')
        if stop_loss is None:
            # Ensure calculate_structure_based_stop_loss is defined and accessible
            stop_loss = await calculate_structure_based_stop_loss(
                instrument, entry_price, direction, timeframe, market_structure, atr
            )
            logger.info(f"[{request_id}] Calculated stop loss: {stop_loss}")
        else:
            stop_loss = float(stop_loss)

        # Calculate take profit if not provided
        take_profit = payload.get('take_profit')
        if take_profit is None:
            # Default to 2:1 risk:reward
            if direction.upper() == "BUY":
                take_profit = entry_price + (abs(entry_price - stop_loss) * 2)
            else:  # SELL
                take_profit = entry_price - (abs(entry_price - stop_loss) * 2)

            logger.info(f"[{request_id}] Calculated take profit: {take_profit}")
        else:
            take_profit = float(take_profit)

        # Get account balance for position sizing
        try:
            # Ensure get_account_balance is defined and accessible
            balance = await get_account_balance()
            logger.info(f"[{request_id}] Account balance: {balance}")
        except Exception as e:
            logger.error(f"[{request_id}] Error getting account balance: {str(e)}")
            balance = 10000.0  # Default fallback

        # Calculate position size using PURE-STATE method
        # Ensure calculate_pure_position_size is defined and accessible
        units, precision = await calculate_pure_position_size(
            instrument, risk_percent, balance, direction
        )
        logger.info(f"[{request_id}] Calculated position size: {units} units")

        # Execute OANDA order
        try:
            # Ensure execute_oanda_order is defined and accessible
            result = await execute_oanda_order(
                instrument=instrument,
                direction=direction,
                risk_percent=risk_percent,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                timeframe=timeframe,
                units=units
            )

            # If successful and alert_handler is available, register with position tracker
            # Ensure alert_handler and its position_tracker component are initialized
            if result.get("success") and 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'position_tracker'):
                # Generate position_id if not already in result
                position_id = result.get("position_id", f"{instrument}_{direction}_{uuid.uuid4().hex[:8]}")

                # --- Indentation Corrected in this block ---
                # Extract metadata from payload
                metadata = {
                    "request_id": request_id,
                    "comment": payload.get("comment"),
                    "strategy": payload.get("strategy"),
                    "atr_value": atr,
                    "market_structure": market_structure is not None
                }

                # Add any additional fields from payload
                for field, value in payload.items():
                    if field not in ["instrument", "direction", "risk_percent", "entry_price",
                                    "stop_loss", "take_profit", "timeframe", "comment", "strategy",
                                    "request_id"] and field not in metadata:
                        metadata[field] = value

                # Record position with position tracker
                try:
                    await alert_handler.position_tracker.record_position(
                        position_id=position_id,
                        symbol=instrument,
                        action=direction,
                        timeframe=timeframe,
                        entry_price=float(result.get("entry_price", entry_price)),
                        size=float(result.get("units", units)),
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        metadata=metadata
                    )
                    # Add position_id to result only if recording succeeds
                    result["position_id"] = position_id

                except Exception as track_error:
                    logger.error(f"[{request_id}] Error recording position: {str(track_error)}")
                    # Decide if you still want to return success even if recording failed

                # Log and return result (still inside the main 'if', but outside the try/except for recording)
                logger.info(f"[{request_id}] Order execution result: {json.dumps(result, indent=2)}")
                return result
            elif not result.get("success"):
                 # If trade execution failed, just return the failure result
                 logger.warning(f"[{request_id}] OANDA order execution failed: {result.get('error')}")
                 return result
            else:
                 # Trade succeeded but couldn't record - log and return success
                 logger.warning(f"[{request_id}] Trade executed but alert_handler or position_tracker not available for recording.")
                 return result


        except Exception as order_error:
            logger.error(f"[{request_id}] Order execution error: {str(order_error)}")
            return {"success": False, "error": f"Order execution error: {str(order_error)}"}

    except Exception as e:
        logger.error(f"[{request_id}] Unhandled error in process_tradingview_alert: {str(e)}", exc_info=True)
        return {"success": False, "error": f"Unhandled error: {str(e)}"}


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
    risk_percent: float, # Note: This parameter is technically unused due to fixed 15% rule, but kept for signature consistency
    entry_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    timeframe: str = 'H1',
    # atr_multiplier: float = 1.5, # Unused if SL/TP are fixed distance
    units: Optional[float] = None,
    _retry_count: int = 0, # Explicitly track SL retries
    _tp_rejected_initial: bool = False, # Internal flag for TP rejection status
    **kwargs
) -> OrderResult: # Use the defined TypedDict for return type hint
    """
    Place an order on OANDA with 100 pip initial SL/TP,
    15% equity allocation, linear SL retries (+50 pips),
    and refined rejection handling.
    """
    request_id = str(uuid.uuid4())
    logger = get_module_logger(__name__, symbol=instrument, request_id=request_id)
    # Use original timeframe string for logging if needed, but normalize for internal use
    normalized_timeframe = normalize_timeframe(timeframe, target="OANDA") # Normalize early

    try:
        instrument_orig = instrument # Keep original for logging if needed
        instrument = standardize_symbol(instrument) # Standardize for internal use
        account_id = OANDA_ACCOUNT_ID
        oanda_inst = instrument.replace('/', '_')
        dir_mult = -1 if direction.upper() == 'SELL' else 1
        instrument_type = get_instrument_type(instrument) # Determine type early

        # --- Price Fetching ---
        if entry_price is None:
            try:
                # Use PricingInfo for potentially more reliable price fetching
                pricing_params = {"instruments": oanda_inst}
                price_request = PricingInfo(accountID=account_id, params=pricing_params)
                price_response = oanda.request(price_request)
                # OandapyV20 >= 0.7.0 returns 'prices' list
                prices_list = price_response.get('prices')
                if not prices_list:
                     # Fallback for older versions or different structure
                     prices_list = price_response.get('pricing')
                if not prices_list:
                    raise ValueError("Could not find price data in OANDA response")

                prices = prices_list[0] # Get the first instrument's pricing
                # Ensure keys exist before accessing
                ask_price = prices.get('asks', [{}])[0].get('price')
                bid_price = prices.get('bids', [{}])[0].get('price')

                if ask_price is None or bid_price is None:
                     raise ValueError(f"Missing ask/bid price in OANDA response for {oanda_inst}")

                entry_price = float(bid_price if direction.upper() == 'SELL' else ask_price)
                logger.info(f"Using current fetched price: {entry_price}") #
            except Exception as price_err:
                logger.error(f"Failed to fetch current price for {oanda_inst}: {price_err}", exc_info=True)
                return OrderResult(success=False, error=f"Failed to fetch price: {price_err}") #

        # --- Pip Value Calculation ---
        pip_value = 0.0001 # Default
        if 'JPY' in oanda_inst: pip_value = 0.01
        elif instrument_type == "CRYPTO": pip_value = entry_price * 0.0001 # % based pip
        elif instrument_type == "COMMODITY":
             # Use more robust check for commodities
             if oanda_inst.startswith('XAU_'): pip_value = 0.01 # Gold
             elif oanda_inst.startswith('XAG_'): pip_value = 0.001 # Silver
             elif oanda_inst.startswith(('WTI_', 'BCO_', 'NATGAS_')): pip_value = 0.01 # Oil/Gas
             else: pip_value = 0.01 # Default commodity

        # --- Initial SL/TP Calculation (100 pips) ---
        initial_sl_distance = 100 * pip_value
        initial_tp_distance = 100 * pip_value

        if stop_loss is None:
            stop_loss = entry_price - dir_mult * initial_sl_distance
            logger.info(f"Calculated initial 100 pip stop loss: {stop_loss}")
        # (Optional: Add check here if provided SL is < 100 pips and adjust if needed)

        if take_profit is None:
            take_profit = entry_price + dir_mult * initial_tp_distance # Opposite direction
            logger.info(f"Calculated initial 100 pip take profit: {take_profit}")
        # (Optional: Add check here if provided TP is < 100 pips and adjust if needed)

        # --- Position Sizing (15% Equity) ---
        if units is None:
            try:
                # Fetch balance using AccountSummary
                summary_req = AccountSummary(accountID=account_id)
                balance_resp = oanda.request(summary_req)
                balance = float(balance_resp['account']['balance'])
                logger.info(f"Account balance: {balance}")
            except Exception as balance_err:
                logger.error(f"Failed to get account balance: {balance_err}", exc_info=True)
                return OrderResult(success=False, error=f"Failed to get balance: {balance_err}")

            equity_percentage = 0.15
            equity_amount = balance * equity_percentage
            leverage = INSTRUMENT_LEVERAGES.get(instrument, INSTRUMENT_LEVERAGES['default']) # Use default from dict

            # Calculate units based on instrument type
            if instrument_type == "CRYPTO" or instrument_type == "COMMODITY":
                if entry_price == 0: return OrderResult(success=False, error="Entry price is zero, cannot calculate size")
                size = (equity_amount / entry_price) * leverage
            else: # Forex
                 size = equity_amount * leverage # Simplified Forex calculation

            units = int(size) * (1 if direction.upper() == 'BUY' else -1)
            logger.info(f"Using 15% equity ({equity_amount:.2f}) with {leverage}x leverage. Calculated units: {abs(units)}") #

        if units == 0:
             logger.warning(f"Calculated units are zero for {oanda_inst}. Order not placed.")
             return OrderResult(success=False, error="units_zero")

        # --- Build Order Payload ---
        order_data = {
            "order": { "type": "MARKET", "instrument": oanda_inst, "units": str(int(units)),
                       "timeInForce": "FOK", "positionFill": "DEFAULT" }
        }
        # Determine which SL/TP to actually submit for this attempt
        sl_to_submit = stop_loss # This might be the original or a widened one from a previous retry
        tp_to_submit = take_profit if not _tp_rejected_initial else None # Don't submit TP if it was already rejected

        if sl_to_submit:
            precision = 3 if 'JPY' in oanda_inst else 5
            order_data["order"]["stopLossOnFill"] = {"price": str(round(sl_to_submit, precision)), "timeInForce": "GTC"}
        if tp_to_submit:
            precision = 3 if 'JPY' in oanda_inst else 5
            order_data["order"]["takeProfitOnFill"] = {"price": str(round(tp_to_submit, precision)), "timeInForce": "GTC"}

        logger.info(f"Attempt {_retry_count + 1}: Sending order to OANDA", extra={"order_data": order_data})

        # --- Submit Order and Handle Response ---
        try:
            order_request = OrderCreate(accountID=account_id, data=order_data)
            response = oanda.request(order_request)
            logger.info(f"Attempt {_retry_count + 1}: Response received", extra={"response": response})

            # --- Success Case ---
            if "orderFillTransaction" in response:
                tx = response["orderFillTransaction"]
                final_sl = sl_to_submit if order_data["order"].get("stopLossOnFill") else None
                final_tp = tp_to_submit if order_data["order"].get("takeProfitOnFill") else None
                return OrderResult(
                    success=True, order_id=tx['id'], instrument=oanda_inst, direction=direction,
                    entry_price=float(tx['price']), units=int(tx['units']),
                    stop_loss=final_sl, take_profit=final_tp,
                    sl_omitted_due_to_rejection=(final_sl is None and stop_loss is not None), # Check if SL was intended but omitted
                    tp_omitted_due_to_rejection=(final_tp is None and take_profit is not None and _tp_rejected_initial) # Check if TP was intended but omitted
                )

            # --- Rejection Case ---
            elif "orderCancelTransaction" in response and "reason" in response["orderCancelTransaction"]:
                cancel_reason = response["orderCancelTransaction"]["reason"]
                error_message = f"Order canceled on attempt {_retry_count + 1}: {cancel_reason}"
                logger.warning(error_message)

                # --- Handle TP Rejection (First Attempt Only) ---
                if cancel_reason == "TAKE_PROFIT_ON_FILL_LOSS" and _retry_count == 0:
                    logger.warning("Take Profit rejected on first attempt. Retrying immediately without Take Profit.")
                    # Immediately retry WITHOUT TP, keep original SL. Pass _tp_rejected_initial=True.
                    filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ['_retry_count', '_tp_rejected_initial']}
                    return await execute_oanda_order(
                        instrument=instrument_orig, direction=direction, entry_price=entry_price,
                        stop_loss=stop_loss, # Keep original SL intention
                        take_profit=None,    # *** Submit without TP ***
                        timeframe=timeframe, units=units,
                        _retry_count=_retry_count, # Retry count stays same for TP modification
                        _tp_rejected_initial=True, # *** Mark TP as rejected ***
                        **filtered_kwargs
                    )

                # --- Handle SL Rejection ---
                elif cancel_reason == "STOP_LOSS_ON_FILL_LOSS":
                    if _retry_count >= 3:
                        logger.error(f"Stop Loss rejected after 3 retries. Attempting final order without SL and TP.")
                        # --- Final Fallback: Order without SL and TP ---
                        final_order_data = {
                            "order": { "type": "MARKET", "instrument": oanda_inst, "units": str(int(units)),
                                       "timeInForce": "FOK", "positionFill": "DEFAULT" }
                        }
                        # Explicitly DO NOT add stopLossOnFill or takeProfitOnFill
                        order_request_final = OrderCreate(accountID=account_id, data=final_order_data)
                        try:
                            response_final = oanda.request(order_request_final)
                            if "orderFillTransaction" in response_final:
                                tx_final = response_final["orderFillTransaction"]
                                logger.warning("Successfully placed order without SL and TP after final rejection.")
                                return OrderResult(
                                    success=True, order_id=tx_final['id'], instrument=oanda_inst, direction=direction,
                                    entry_price=float(tx_final['price']), units=int(tx_final['units']),
                                    stop_loss=None, take_profit=None, # Both omitted
                                    sl_omitted_due_to_rejection=True, # Mark SL as omitted
                                    tp_omitted_due_to_rejection=True, # Mark TP as omitted
                                    warning="Order placed without StopLoss and TakeProfit due to rejections."
                                )
                            else:
                                logger.error("Final fallback order without SL/TP also failed.", extra={"details": response_final})
                                return OrderResult(success=False, error="Failed final fallback order (no SL/TP)", details=response_final, sl_omitted_due_to_rejection=True, tp_omitted_due_to_rejection=True)
                        except Exception as final_fallback_e:
                             logger.error(f"Error during final fallback order submission: {str(final_fallback_e)}")
                             return OrderResult(success=False, error=f"Final fallback submission error: {str(final_fallback_e)}", sl_omitted_due_to_rejection=True, tp_omitted_due_to_rejection=True)
                    else:
                        # --- Calculate Wider Stop for Next SL Retry ---
                        current_retry_num = _retry_count + 1 # This is attempt #1, #2, or #3
                        base_pips = 100
                        wider_pips = base_pips + (50 * current_retry_num) # 150, 200, 250 pips
                        wider_distance = wider_pips * pip_value
                        new_stop_loss = entry_price - dir_mult * wider_distance
                        logger.warning(f"Retrying SL (Attempt {current_retry_num + 1}) with wider stop: {new_stop_loss} ({wider_pips} pips)")

                        # --- Recursive Call for SL Retry ---
                        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ['_retry_count', '_tp_rejected_initial']}
                        return await execute_oanda_order(
                            instrument=instrument_orig, direction=direction, risk_percent=0.15, # Keep passing dummy risk % or remove
                            entry_price=entry_price,
                            stop_loss=new_stop_loss, # Use the new wider stop
                            take_profit=take_profit, # Keep original TP unless it was rejected
                            timeframe=timeframe, units=units,
                            _retry_count=current_retry_num, # Pass incremented count
                            _tp_rejected_initial=_tp_rejected_initial, # Pass TP rejection flag
                            **filtered_kwargs
                        )
                else:
                    # Other cancellation reason - fail without retry
                    return OrderResult(success=False, error=error_message, details=response)

            else:
                # No transaction info, general failure
                 logger.error("Order failed: No orderFillTransaction or orderCancelTransaction in response.", extra={"details": response})
                 return OrderResult(success=False, error="Order failed: Unexpected response structure", details=response)

        # Handle specific V20 errors or general exceptions during request
        except V20Error as v20_err:
             # Log specific OANDA errors for better debugging
             error_details = {"oanda_code": v20_err.code, "oanda_msg": v20_err.msg, "raw_response": v20_err.raw}
             logger.error(f"[OANDA] V20 API Error: {v20_err.msg} (Code: {v20_err.code})", extra=error_details)
             return OrderResult(success=False, error=f"OANDA API Error: {v20_err.msg}", details=error_details)
        except Exception as e:
            logger.error(f"[OANDA] General Error sending order: {str(e)}", exc_info=True)
            return OrderResult(success=False, error=f"Order submission error: {str(e)}")

    # --- Outer exception handling for the whole function ---
    except Exception as e:
        logger.error(f"[execute_oanda_order] Unexpected outer error: {str(e)}", exc_info=True)
        return OrderResult(success=False, error=f"Internal function error: {str(e)}")
        

# In get_current_price
async def get_current_price(symbol: str, side: str = "BUY") -> float:
    """Get current price for a symbol (placeholder implementation)"""
    try:
        symbol = standardize_symbol(symbol)
        # Default base price (corrected variable name and removed typo)
        base_price = 100.0

        # Set specific base prices (corrected variable name)
        if symbol == "EUR_USD":
            base_price = 1.10
        elif symbol == "GBP_USD":
            base_price = 1.25
        elif symbol == "USD_JPY":
            base_price = 110.0
        elif symbol == "XAU_USD":
            base_price = 1900.0
        # Add elif for other symbols if needed, otherwise base_price remains 100.0

        # --- Corrected Indentation Below ---
        # These lines should be at the same level as the 'if/elif' block starts
        # Calculate random variation
        price = base_price * (1 + random.uniform(-0.001, 0.001))
        # Apply bid/ask spread simulation
        price *= 1.0001 if side.upper() == "BUY" else 0.9999
        return price

    # --- Except block alignment corrected ---
    except Exception as e:
        # Ensure logger is defined and accessible in this scope
        # If logger is defined globally or passed appropriately:
        # logger.error(f"Error getting price for {symbol}: {str(e)}")
        # Otherwise, you might need to handle logging differently here
        print(f"Error getting price for {symbol}: {str(e)}") # Fallback to print
        raise

# Replace BOTH existing get_instrument_type functions with this one
def get_instrument_type(instrument: str) -> str:
    """Return one of: 'FOREX', 'CRYPTO', 'COMMODITY', 'INDICES'."""
    inst = instrument.upper()
    
    # Define lists for identification
    crypto_list = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']
    commodity_list = ['XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BCO', 'NATGAS']
    index_list = ['SPX', 'NAS', 'US30', 'UK100', 'DE30', 'JP225', 'AUS200']

    # Check for underscore format (e.g., EUR_USD, BTC_USD)
    if '_' in inst:
        parts = inst.split('_')
        if len(parts) == 2:
            base, quote = parts
            # Check Crypto
            if base in crypto_list:
                return "CRYPTO"
            # Check Commodity
            if base in commodity_list:
                return "COMMODITY"
            # Check Index
            if base in index_list:
                return "INDICES"
            # Otherwise, it's forex (no special treatment for JPY pairs)
            if len(base) == 3 and len(quote) == 3 and base.isalpha() and quote.isalpha():
                return "FOREX"

    # No underscore format
    else:
        # Check Crypto
        for crypto in crypto_list:
            if inst.startswith(crypto) and any(inst.endswith(q) for q in ["USD", "EUR", "USDT", "GBP", "JPY"]):
                return "CRYPTO"
        
        # Check Commodity
        for comm in commodity_list:
            if inst.startswith(comm) and any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                return "COMMODITY"
        
        # Check Index
        for index in index_list:
            if inst.startswith(index) and any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                return "INDICES"
        
        # Check standard forex (6-char)
        if len(inst) == 6 and inst.isalpha():
            return "FOREX"

    # Default if no specific type matched
    logger.warning(f"Could not determine specific instrument type for '{instrument}', defaulting to FOREX.")
    return "FOREX"

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable based on market hours"""
    now = datetime.now(timezone.utc)
    instrument_type = (symbol)

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


# In process_alert method, update the part that calculates stop loss to prefer market structure
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
                    
                    # Analyze market structure
                    stop_price = None
                    try:
                        market_structure = await self.market_structure.analyze_market_structure(
                            instrument, timeframe, current_price, current_price * 0.99, current_price
                        )
                        logger.info(f"[{request_id}] Market structure analysis complete")
                        
                        # PRIORITY 1: Try to use market structure for stop loss placement
                        if market_structure:
                            if action == 'BUY' and market_structure.get('nearest_support'):
                                stop_price = market_structure['nearest_support']
                                logger.info(f"[{request_id}] Using structure-based stop loss: {stop_price} (support level)")
                            elif action == 'SELL' and market_structure.get('nearest_resistance'):
                                stop_price = market_structure['nearest_resistance']
                                logger.info(f"[{request_id}] Using structure-based stop loss: {stop_price} (resistance level)")
                    except Exception as e:
                        logger.error(f"[{request_id}] Error analyzing market structure: {str(e)}")
                        market_structure = None
                    
                    # PRIORITY 2: If no suitable structure level found, use ATR-based stop
                    if not stop_price:
                        instrument_type = get_instrument_type(instrument)
                        atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
                        
                        if action == 'BUY':
                            stop_price = current_price - (atr * atr_multiplier)
                        else:
                            stop_price = current_price + (atr * atr_multiplier)
                        logger.info(f"[{request_id}] Using ATR-based stop loss: {stop_price} (ATR: {atr}, multiplier: {atr_multiplier})")
                    
                    # PRIORITY 3: Ensure minimum distance requirements are met
                    dir_mult = -1 if action.upper() == 'SELL' else 1
                    
                    # Define minimum distance based on instrument type
                    min_distance = 0.01  # 100 pips for forex
                    if instrument_is_commodity(instrument):
                        min_distance = 0.01  # 100 pips for commodities
                    elif 'BTC' in instrument or 'ETH' in instrument or get_instrument_type(instrument) == "CRYPTO":
                        min_distance = current_price * 0.10  # 10% for crypto
                    
                    # Double the minimum for extra safety
                    min_distance = min_distance * 2.0
                    
                    # Check if stop is too close and adjust if needed
                    current_distance = abs(current_price - stop_price)
                    if current_distance < min_distance:
                        # Adjust stop loss to meet minimum distance requirement
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
                        "stop_loss": stop_price,
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
    """Process an entry alert (BUY or SELL) with comprehensive error handling"""
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
            
        # Determine stop loss using multi-step approach
        # 1. Start with market structure
        # 2. Fallback to ATR
        # 3. Ensure minimum distance requirements
        
        # Step 1: Try market structure
        stop_loss = None
        try:
            market_structure = await self.market_structure.analyze_market_structure(
                standardized_symbol, timeframe, price, price * 0.99, price
            )
            
            if market_structure:
                if action == "BUY" and market_structure.get('nearest_support'):
                    stop_loss = market_structure['nearest_support']
                    logger.info(f"[{request_id}] Using structure-based stop loss: {stop_loss} (support level)")
                elif action == "SELL" and market_structure.get('nearest_resistance'):
                    stop_loss = market_structure['nearest_resistance']
                    logger.info(f"[{request_id}] Using structure-based stop loss: {stop_loss} (resistance level)")
        except Exception as e:
            logger.error(f"[{request_id}] Error analyzing market structure: {str(e)}")
            market_structure = None
        
        # Step 2: If no structure-based stop, use ATR
        if stop_loss is None:
            try:
                atr_value = await get_atr(standardized_symbol, timeframe)
                if atr_value <= 0:
                    logger.warning(f"[{request_id}] Invalid ATR value for {standardized_symbol}: {atr_value}")
                    atr_value = 0.0025  # Default fallback value
                
                instrument_type = get_instrument_type(standardized_symbol)
                atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
                
                # Apply volatility adjustment if available
                volatility_multiplier = 1.0
                if self.volatility_monitor:
                    volatility_multiplier = self.volatility_monitor.get_stop_loss_modifier(standardized_symbol)
                    logger.info(f"[{request_id}] Volatility multiplier: {volatility_multiplier}")
                    
                atr_multiplier *= volatility_multiplier
                
                if action == "BUY":
                    stop_loss = price - (atr_value * atr_multiplier)
                else:  # SELL
                    stop_loss = price + (atr_value * atr_multiplier)
                    
                logger.info(f"[{request_id}] Using ATR-based stop loss: {stop_loss} (ATR: {atr_value}, Multiplier: {atr_multiplier})")
                    
            except Exception as e:
                logger.error(f"[{request_id}] Error calculating stop loss: {str(e)}")
                # Use a default percentage-based stop if all else fails
                stop_loss = price * 0.99 if action == "BUY" else price * 1.01
                logger.info(f"[{request_id}] Using fallback stop loss: {stop_loss} (1% of price)")
        
        # Step 3: Ensure minimum distance requirements
        dir_mult = -1 if action == "SELL" else 1
        
        # Define minimum distance based on instrument type
        min_distance = 0.01  # 100 pips for forex
        if instrument_is_commodity(standardized_symbol):
            min_distance = 0.01  # 100 pips for commodities
        elif 'BTC' in standardized_symbol or 'ETH' in standardized_symbol or get_instrument_type(standardized_symbol) == "CRYPTO":
            min_distance = price * 0.10  # 10% for crypto
        
        # Double the minimum for extra safety
        min_distance = min_distance * 2.0
        
        # Check if stop is too close and adjust if needed
        current_distance = abs(price - stop_loss)
        if current_distance < min_distance:
            # Adjust stop loss to meet minimum distance requirement
            old_stop = stop_loss
            stop_loss = price - dir_mult * min_distance
            logger.warning(f"[{request_id}] Adjusted stop loss from {old_stop} to {stop_loss} to meet minimum distance requirement ({min_distance})")
            
        # Calculate position size
        try:
            risk_amount = account_balance * risk_percentage
            price_risk = abs(price - stop_loss)
            
            # Calculate size in units
            if price_risk > 0:
                # Risk-based sizing
                position_size = risk_amount / price_risk
            else:
                # Percentage-based sizing as fallback
                position_size = account_balance * percentage / 100 / price
                logger.warning(f"[{request_id}] Using fallback position sizing method: {position_size}")
                
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
                "entry_price": price,
                "stop_loss": stop_loss,
                "timeframe": timeframe,
                "account": alert_data.get("account"),
                "units": position_size  # Use calculated position size
            })
            
            if not success:
                logger.error(f"[{request_id}] Trade execution failed: {trade_result.get('error', 'Unknown error')}")
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
                    "risk_percentage": risk_percentage,
                    "atr_value": atr_value if 'atr_value' in locals() else None,
                    "min_distance": min_distance
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
                    stop_loss=stop_loss,
                    take_profit=None,  # Will be set by exit manager
                    metadata=metadata
                )
                
                if not position_recorded:
                    logger.warning(f"[{request_id}] Failed to record position in tracker")
                else:
                    logger.info(f"[{request_id}] Position recorded in tracker: {position_id}")
            
            # Register with risk manager
            if self.risk_manager:
                await self.risk_manager.register_position(
                    position_id=position_id,
                    symbol=standardized_symbol,
                    action=action,
                    size=position_size,
                    entry_price=price,
                    stop_loss=stop_loss,
                    account_risk=risk_percentage,
                    timeframe=timeframe
                )
                logger.info(f"[{request_id}] Position registered with risk manager")
                
            # Set take profit levels
            if self.multi_stage_tp_manager:
                await self.multi_stage_tp_manager.set_take_profit_levels(
                    position_id=position_id,
                    entry_price=price,
                    stop_loss=stop_loss,
                    position_direction=action,
                    position_size=position_size,
                    symbol=standardized_symbol,
                    timeframe=timeframe,
                    atr_value=atr_value if 'atr_value' in locals() else 0.0,
                    volatility_multiplier=volatility_multiplier if 'volatility_multiplier' in locals() else 1.0
                )
                logger.info(f"[{request_id}] Take profit levels set")
                
            # Register with time-based exit manager
            if self.time_based_exit_manager:
                self.time_based_exit_manager.register_position(
                    position_id=position_id,
                    symbol=standardized_symbol,
                    direction=action,
                    entry_time=datetime.now(timezone.utc),
                    timeframe=timeframe
                )
                logger.info(f"[{request_id}] Position registered with time-based exit manager")
                
            # Initialize dynamic exits
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
                    stop_loss=stop_loss,
                    timeframe=timeframe
                )
                logger.info(f"[{request_id}] Dynamic exits initialized (market regime: {market_regime})")
                
            # Record in position journal
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
                    stop_loss=stop_loss,
                    market_regime=market_regime,
                    volatility_state=volatility_state,
                    metadata=metadata if 'metadata' in locals() else None
                )
                logger.info(f"[{request_id}] Position recorded in journal")
                
            # Send notification
            if self.notification_system:
                await self.notification_system.send_notification(
                    f"New position opened: {action} {standardized_symbol} @ {price:.5f} (Risk: {risk_percentage*100:.1f}%)",
                    "info"
                )
                logger.info(f"[{request_id}] Position notification sent")
                
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
                "stop_loss": stop_loss,
                "alert_id": alert_id
            }
            
            # Merge with trade_result if available
            if isinstance(trade_result, dict):
                result.update({k: v for k, v in trade_result.items() if k not in result})
                
            return result
                
        except Exception as e:
            logger.error(f"[{request_id}] Error executing trade: {str(e)}")
            return {
                "status": "error",
                "message": f"Error executing trade: {str(e)}",
                "alert_id": alert_id
            }
    
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled exception in entry alert processing: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": f"Internal error: {str(e)}",
            "alert_id": alert_data.get("id", "unknown")
        }

# Replace BOTH existing get_instrument_type functions with this one
def get_instrument_type(instrument: str) -> str:
    """Return one of: 'FOREX', 'CRYPTO', 'COMMODITY', 'INDICES'."""
    try:
        inst = instrument.upper()
        crypto_list = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']
        commodity_list = ['XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BCO', 'NATGAS'] # Added more common oil/gas
        index_list = ['SPX', 'NAS', 'US30', 'UK100', 'DE30', 'JP225', 'AUS200'] # Added more common indices

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
                # Check Index (Base only, e.g., US30_USD) - less common format
                if base in index_list:
                     return "INDICES"
                # Check Forex (standard 3-letter codes)
                if len(base) == 3 and len(quote) == 3 and base.isalpha() and quote.isalpha():
                    # Exclude if base is a commodity (e.g., XAU_CAD) - should be COMMODITY
                    if base not in commodity_list:
                        return "FOREX"
                    else:
                        return "COMMODITY" # e.g., XAU_EUR is a commodity trade

        # Check for specific no-underscore formats
        else:
            # Check Crypto (e.g., BTCUSD, ETHUSD)
            for crypto in crypto_list:
                if inst.startswith(crypto):
                    # Basic check: Starts with crypto and has common quote like USD, EUR, USDT
                    if any(inst.endswith(q) for q in ["USD", "EUR", "USDT", "GBP", "JPY"]):
                         return "CRYPTO"
            # Check Commodity (e.g., XAUUSD, WTICOUSD)
            for comm in commodity_list:
                 if inst.startswith(comm):
                     if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                          return "COMMODITY"
            # Check Index (e.g., US30USD, NAS100USD) - may need adjustment based on broker naming
            for index in index_list:
                 if inst.startswith(index):
                     if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]): # Or specific broker suffix
                          return "INDICES"
            # Check standard 6-char Forex (e.g., EURUSD)
            if len(inst) == 6 and inst.isalpha():
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
    instrument_type = (symbol)

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

@async_error_handler()
async def get_account_balance() -> float:
    """Get current account balance from Oanda"""
    try:
        base_url = "https://api-fxpractice.oanda.com" if OANDA_ENVIRONMENT == "practice" else "https://api-fxtrade.oanda.com"
        endpoint = f"/v3/accounts/{OANDA_ACCOUNT_ID}/summary"
        headers = {
            "Authorization": f"Bearer {OANDA_ACCESS_TOKEN}",  # Fixed: using global variable instead of config attribute
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base_url}{endpoint}", headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    balance = float(data["account"]["NAV"])
                    logger.info(f"Current account balance: {balance}")
                    return balance
                else:
                    error_data = await response.text()
                    logger.error(f"Error fetching account balance: {response.status} - {error_data}")
                    return 10000.0
    except Exception as e:
        logger.error(f"Failed to get account balance: {str(e)}")
        return 10000.0
        
@async_error_handler()
async def execute_trade(trade_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade with the broker"""
    try:
        symbol = trade_data.get('symbol', '')
        action = trade_data.get('action', '').upper()
        entry_price = trade_data.get('entry_price')
        stop_loss = trade_data.get('stop_loss')
        timeframe = trade_data.get('timeframe', '1H')
        
        # Use provided units or calculate from percentage
        units = trade_data.get('units')
        if units is None:
            percentage = float(trade_data.get('percentage', 1.0))
            # Get balance
            balance = await get_account_balance()
            # Calculate units using PURE-STATE method
            units, _ = await calculate_pure_position_size(symbol, percentage, balance, action)
        
        # Get current price if not provided
        if entry_price is None:
            entry_price = await get_current_price(symbol, action)
        
        order_id = str(uuid.uuid4())

        response = {
            "orderCreateTransaction": {
                "id": order_id,
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "MARKET_ORDER",
                "instrument": symbol,
                "units": str(units)
            },
            "orderFillTransaction": {
                "id": str(uuid.uuid4()),
                "time": datetime.now(timezone.utc).isoformat(),
                "type": "ORDER_FILL",
                "orderID": order_id,
                "instrument": symbol,
                "units": str(units),
                "price": str(entry_price)
            },
            "lastTransactionID": str(uuid.uuid4())
        }

        logger.info(f"Executed trade: {action} {symbol} @ {entry_price} (Units: {units})")
        return True, response
    except Exception as e:
        logger.error(f"Error executing trade: {str(e)}")
        return False, {"error": str(e)}

async def close_position(position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close a position with the broker"""
    try:
        # This should use your broker API to close the position
        position_id = position_data.get("position_id")
        symbol = position_data.get("symbol", "")
        
        # If using OANDA, you might do something like:
        if 'oanda' in globals() and OANDA_ACCOUNT_ID:
            # Example OANDA close code - adjust to your actual API
            from oandapyV20.endpoints.positions import PositionClose
            data = {"longUnits": "ALL"} if position_data.get("action") == "BUY" else {"shortUnits": "ALL"}
            close_request = PositionClose(accountID=OANDA_ACCOUNT_ID, 
                                         instrumentID=symbol,
                                         data=data)
            response = oanda.request(close_request)
            
            price = await get_current_price(symbol, "SELL" if position_data.get("action") == "BUY" else "BUY")
            
            return True, {
                "position_id": position_id,
                "price": price,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "response": response
            }
        
        # Fallback implementation for testing
        price = await get_current_price(symbol, "SELL")
        return True, {
            "position_id": position_id,
            "price": price,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")
        return False, {"error": str(e)}

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

# Add near line ~1380 in the Trading Execution section
async def calculate_structure_based_stop_loss(
    instrument: str, 
    entry_price: float, 
    action: str, 
    timeframe: str,
    market_structure: Optional[Dict[str, Any]] = None,
    atr_value: Optional[float] = None
) -> float:
    """Calculate simplified trailing stop loss, starting at 100 pips from entry"""
    
    # Get instrument type - only to determine pip value
    instrument_type = get_instrument_type(instrument)
    
    # Determine pip value based on instrument type
    pip_value = 0.0001  # Default pip value for most forex pairs
    if instrument_type == "CRYPTO":
        # For cryptos, use a percentage of price instead of fixed pips
        pip_value = entry_price * 0.0001  # 0.01% of price as "pip" equivalent
    elif instrument_type == "COMMODITY":
        if 'XAU' in instrument:
            pip_value = 0.01  # Gold pip value
        elif 'XAG' in instrument:
            pip_value = 0.001  # Silver pip value
        else:
            pip_value = 0.01  # Default for other commodities
    
    # Initial stop distance is 100 pips
    initial_stop_distance = 100 * pip_value
    
    # Calculate stop loss price
    if action.upper() == "BUY":
        stop_loss = entry_price - initial_stop_distance
    else:  # SELL
        stop_loss = entry_price + initial_stop_distance
    
    # Use ATR to adjust if available, but keep within constraints
    if atr_value is not None and atr_value > 0:
        # Adjust stop based on volatility, but never wider than initial 100 pips
        # and never tighter than 50 pips
        min_distance = 50 * pip_value
        max_distance = initial_stop_distance  # 100 pips
        
        # Suggest a volatility-based distance
        volatility_distance = atr_value * 2  # 2 x ATR
        
        # Apply constraints
        adjusted_distance = max(min_distance, min(volatility_distance, max_distance))
        
        # Recalculate stop with adjusted distance
        if action.upper() == "BUY":
            stop_loss = entry_price - adjusted_distance
        else:  # SELL
            stop_loss = entry_price + adjusted_distance
    
    # Round to appropriate precision
    price_precision = 5  # Default precision
    if instrument_type == "COMMODITY" and 'XAU' in instrument:
        price_precision = 2
    elif instrument_type == "CRYPTO":
        price_precision = 2
        
    stop_loss = round(stop_loss, price_precision)
    
    logger.info(f"Calculated simplified trailing stop for {instrument}: {stop_loss} (distance: {initial_stop_distance/pip_value} pips)")
    return stop_loss


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
            
    from typing import Any, Dict, Optional, NamedTuple


    class ClosePositionResult(NamedTuple):
        success: bool
        position_data: Optional[Dict[str, Any]] = None
        error: Optional[str] = None
    
    
    async def close_position(
        self,
        position_id: str,
        exit_price: float,
        reason: str = "manual"
    ) -> ClosePositionResult:
        """Close a position, update internal records, risk metrics, and persist changes."""
        
        async with self._lock:
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found")
                return ClosePositionResult(success=False, error="Position not found")
            
            position = self.positions[position_id]
            symbol = position.symbol
    
            try:
                # Close the position
                position.close(exit_price=exit_price, reason=reason)
            except Exception as e:
                logger.error(f"Failed to close position {position_id}: {str(e)}")
                return ClosePositionResult(success=False, error=f"Close operation failed: {str(e)}")
    
            # Prepare closed position dictionary
            position_dict = self._position_to_dict(position)
    
            # Move position to closed_positions
            self.closed_positions[position_id] = position_dict
    
            # Remove from open positions by symbol
            if symbol in self.open_positions_by_symbol and position_id in self.open_positions_by_symbol[symbol]:
                del self.open_positions_by_symbol[symbol][position_id]
                if not self.open_positions_by_symbol[symbol]:  # Clean up empty symbol dict
                    del self.open_positions_by_symbol[symbol]
    
            # Remove from active positions
            del self.positions[position_id]
    
            # Update position history
            for i, hist_pos in enumerate(self.position_history):
                if hist_pos.get("position_id") == position_id:
                    self.position_history[i] = position_dict
                    break
    
            # Update risk metrics if present
            adjusted_risk = getattr(position, "adjusted_risk", 0)
            if hasattr(self, "current_risk"):
                self.current_risk = max(0, self.current_risk - adjusted_risk)
    
            # Update database if db manager available
            if self.db_manager:
                try:
                    await self.db_manager.update_position(position_id, position_dict)
                except Exception as e:
                    logger.error(f"Database update failed for position {position_id}: {str(e)}")
                    # Continue even if DB update fails
    
            logger.info(f"Closed position: {position_id} ({symbol} @ {exit_price}, PnL: {position.pnl:.2f}, Remaining Risk: {getattr(self, 'current_risk', 0):.2%})")
            
            return ClosePositionResult(success=True, position_data=position_dict)

            
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
        self.performance = {}
        self._running = False
        self.lorentzian_classifier = LorentzianDistanceClassifier()
        self.exit_strategies = {}
        self._lock = asyncio.Lock()
        
        # Add strategy configuration constants
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

    async def initialize_trailing_stop(self,
                                 position_id: str,
                                 symbol: str,
                                 entry_price: float,
                                 direction: str,
                                 atr_value: float = 0.0,
                                 volatility_state: str = "normal_volatility") -> float:
        """Initialize simplified trailing stop with 100 pip initial distance"""
        async with self._lock:
            # Determine pip value based on instrument type
            instrument_type = get_instrument_type(symbol)
            pip_value = 0.0001  # Default pip value for most forex pairs
            
            if instrument_type == "CRYPTO":
                # For cryptos, use a percentage of price instead of fixed pips
                pip_value = entry_price * 0.0001  # 0.01% of price as "pip" equivalent
            elif instrument_type == "COMMODITY":
                if 'XAU' in symbol:
                    pip_value = 0.01  # Gold pip value
                elif 'XAG' in symbol:
                    pip_value = 0.001  # Silver pip value
                else:
                    pip_value = 0.01  # Default for other commodities
            
            # Initial stop distance is 100 pips
            initial_stop_distance = 100 * pip_value
            
            # Apply ATR for volatility adjustment if available, but keep within constraints
            min_distance = 50 * pip_value  # Minimum 50 pips
            max_distance = initial_stop_distance  # Maximum 100 pips
            
            adjusted_distance = initial_stop_distance
            if atr_value > 0:
                # Use ATR as a potential distance
                volatility_distance = atr_value * 2  # 2 x ATR
                adjusted_distance = max(min_distance, min(volatility_distance, max_distance))
            
            # Calculate initial stop loss
            if direction == "BUY":
                stop_level = entry_price - adjusted_distance
            else:  # SELL
                stop_level = entry_price + adjusted_distance
            
            # Store trailing stop data
            self.trailing_stops[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": direction,
                "atr_value": atr_value,
                "volatility_state": volatility_state,
                "pip_value": pip_value,
                "initial_stop": stop_level,
                "current_stop": stop_level,
                "highest_price": entry_price if direction == "BUY" else entry_price,
                "lowest_price": entry_price if direction == "SELL" else entry_price,
                "activated": True,  # Immediately active
                "active": True,
                "min_distance": min_distance,  # Store min distance (50 pips)
                "max_distance": max_distance,  # Store max distance (100 pips)
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            
            logger.info(f"Initialized simplified trailing stop for {position_id} at {stop_level} (distance: {adjusted_distance/pip_value} pips)")
            return stop_level

    async def _init_breakeven_stop(self, position_id, entry_price, position_direction, stop_loss=None):
        """Initialize breakeven stop loss functionality"""
        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}
        
        # If stop loss not provided, calculate it
        if stop_loss is None:
            # Get position data
            position_data = await self.position_tracker.get_position_info(position_id)
            if not position_data:
                logger.warning(f"Position {position_id} not found for breakeven initialization")
                return None
            
            symbol = position_data.get("symbol")
            timeframe = position_data.get("timeframe", "H1")
            
            # Get ATR data
            atr = await get_atr(symbol, timeframe)
            
            # Calculate stop distance
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            # Calculate initial stop loss
            if position_direction == "LONG":
                stop_loss = entry_price - (atr * atr_multiplier)
            else:
                stop_loss = entry_price + (atr * atr_multiplier)
        
        # Calculate distance for breakeven activation
        distance = abs(entry_price - stop_loss)
        
        # Activate breakeven at 1R profit by default
        activation_multiplier = 1.0  # 1:1 risk:reward
        
        # Use timeframe settings if available
        position_data = await self.position_tracker.get_position_info(position_id)
        if position_data:
            timeframe = position_data.get("timeframe", "H1")
            # Adjust based on timeframe
            if timeframe == "15M":
                activation_multiplier = 0.8  # 0.8R for short timeframes
            elif timeframe == "1H":
                activation_multiplier = 1.0  # 1R for 1H
            elif timeframe == "4H":
                activation_multiplier = 1.2  # 1.2R for 4H
            elif timeframe == "1D":
                activation_multiplier = 1.5  # 1.5R for 1D
        
        # Calculate activation level
        if position_direction == "LONG":
            activation_level = entry_price + (distance * activation_multiplier)
        else:
            activation_level = entry_price - (distance * activation_multiplier)
        
        # Store breakeven configuration
        self.exit_levels[position_id]["breakeven"] = {
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "activation_level": activation_level,
            "activated": False,
            "buffer_pips": 0,  # Optional buffer above/below entry
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Initialized breakeven stop for {position_id}: Entry price: {entry_price}, "
                    f"Activation level: {activation_level}")
        
        return entry_price

    async def _init_trend_following_exits(self, position_id, entry_price, stop_loss, position_direction):
        """Initialize exits optimized for trend-following strategies"""
        if position_id not in self.exit_levels:
            self.exit_levels[position_id] = {}
        
        # Get position data for context
        position_data = await self.position_tracker.get_position_info(position_id)
        if not position_data:
            logger.warning(f"Position {position_id} not found for trend exit initialization")
            return False
        
        symbol = position_data.get("symbol")
        timeframe = position_data.get("timeframe", "H1")
        
        # Get ATR data if needed for stop loss calculation
        if stop_loss is None:
            atr = await get_atr(symbol, timeframe)
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            if position_direction == "LONG":
                stop_loss = entry_price - (atr * atr_multiplier)
            else:
                stop_loss = entry_price + (atr * atr_multiplier)
        
        # Calculate risk distance (R value)
        risk_distance = abs(entry_price - stop_loss)
        
        # Use the trend-following take profit levels from your config
        tp_levels = self.TIMEFRAME_TAKE_PROFIT_LEVELS.get(
            timeframe, self.TIMEFRAME_TAKE_PROFIT_LEVELS["1H"]
        )
        
        # For trend following, use higher R-multiples
        take_profit_multiples = [2.0, 3.0, 4.5]  # Higher targets for trend following
        
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
        
        # Define specific percentages for trend following strategy
        percentages = {
            "first_exit": 0.3,   # 30% at 2R
            "second_exit": 0.3,  # 30% at 3R
            "runner": 0.4        # 40% with trailing (hold for extended trend)
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
        
        # For trend following, initialize a trailing stop with wider settings
        trailing_settings = self.TIMEFRAME_TRAILING_SETTINGS.get(
            timeframe, self.TIMEFRAME_TRAILING_SETTINGS["1H"]
        )
        
        # Initialize trailing stop (activated after first target hit)
        initial_multiplier = trailing_settings["initial_multiplier"] * 1.25  # 25% wider for trend following
        
        trailing_stop_distance = risk_distance * initial_multiplier
        
        if position_direction == "LONG":
            trailing_stop = entry_price - trailing_stop_distance
        else:
            trailing_stop = entry_price + trailing_stop_distance
        
        # Add trailing stop configuration for trend following
        self.exit_levels[position_id]["custom_trailing"] = {
            "stop": trailing_stop,
            "distance": trailing_stop_distance,
            "multiplier": initial_multiplier,
            "active_after_tp": 0,  # Activate after first TP hit
            "activated": False,
            "profit_levels": trailing_settings["profit_levels"]
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
                    f"Take profits: {take_profits}, Strategy: trend_following")
        
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
                stop_loss = entry_price - (atr * adjusted_multiplier)
            else:
                stop_loss = entry_price + (atr * adjusted_multiplier)
        
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
        if stop_loss is None:
            atr = await get_atr(symbol, timeframe)
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            # Slightly tighter stops for breakouts (90% of standard)
            adjusted_multiplier = atr_multiplier * 0.9
            
            if position_direction == "LONG":
                stop_loss = entry_price - (atr * adjusted_multiplier)
            else:
                stop_loss = entry_price + (atr * adjusted_multiplier)
        
        # Calculate risk distance (R value)
        risk_distance = abs(entry_price - stop_loss)
        
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
        
        # For breakouts, use both breakeven and trailing stop
        # Breakeven activates after first target hit (1R)
        self.exit_levels[position_id]["breakeven"] = {
            "entry_price": entry_price,
            "activation_level": take_profits[0],  # Activate at first TP
            "activated": False,
            "buffer_pips": 0,
            "active_after_tp": 0  # Activate after first TP hit
        }
        
        # Get trailing settings
        trailing_settings = self.TIMEFRAME_TRAILING_SETTINGS.get(
            timeframe, self.TIMEFRAME_TRAILING_SETTINGS["1H"]
        )
        
        # Initialize trailing stop (activated after second target hit)
        initial_multiplier = trailing_settings["initial_multiplier"]
        trailing_stop_distance = risk_distance * initial_multiplier
        
        if position_direction == "LONG":
            trailing_stop = entry_price - trailing_stop_distance
        else:
            trailing_stop = entry_price + trailing_stop_distance
        
        # Add trailing stop configuration for breakouts
        self.exit_levels[position_id]["custom_trailing"] = {
            "stop": trailing_stop,
            "distance": trailing_stop_distance,
            "multiplier": initial_multiplier,
            "active_after_tp": 1,  # Activate after second TP hit
            "activated": False,
            "profit_levels": trailing_settings["profit_levels"]
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
                    f"Take profits: {take_profits}, Strategy: breakout")
        
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
        if stop_loss is None:
            atr = await get_atr(symbol, timeframe)
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            if position_direction == "LONG":
                stop_loss = entry_price - (atr * atr_multiplier)
            else:
                stop_loss = entry_price + (atr * atr_multiplier)
        
        # Calculate risk distance (R value)
        risk_distance = abs(entry_price - stop_loss)

        # Get time-based exit
        time_settings = self.TIMEFRAME_TIME_STOPS.get(
            timeframe, self.TIMEFRAME_TIME_STOPS["1H"]
        )
        
        # Use optimal duration from time settings
        hours = time_settings["optimal_duration"]
        
        # UTC timezone reference
        current_time = datetime.now(timezone.utc)
        exit_time = current_time + timedelta(hours=hours)
        
        self.exit_strategies[position_id]["exits"]["time_exit"] = {
            "exit_time": exit_time.isoformat(),
            "reason": "standard_time_limit",
            "adjustable": True
        }     
        
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
        
        # Add trailing stop configuration
        trailing_settings = self.TIMEFRAME_TRAILING_SETTINGS.get(
            timeframe, self.TIMEFRAME_TRAILING_SETTINGS["1H"]
        )
        
        # Use default settings from your config
        initial_multiplier = trailing_settings["initial_multiplier"]
        
        # Initialize trailing stop with standard settings
        trailing_stop_distance = risk_distance * initial_multiplier
        
        if position_direction == "LONG":
            trailing_stop = entry_price - trailing_stop_distance
        else:
            trailing_stop = entry_price + trailing_stop_distance
        
        # Add trailing stop
        self.exit_levels[position_id]["trailing_stop"] = {
            "initial_stop": stop_loss,
            "current_stop": stop_loss,  # Start at initial stop loss
            "activation_level": take_profits[1],  # Activate at second take profit (2R)
            "activated": False,
            "distance": trailing_stop_distance,
            "multiplier": initial_multiplier,
            "profit_levels": trailing_settings["profit_levels"]
        }
        
        # Add time-based exit
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
                    f"Take profits: {take_profits}, Strategy: standard")
        
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
                
            logger.info(f"Initialized exits for {position_id} with {regime} regime and {strategy_type} strategy")
                
            return self.exit_strategies[position_id]

##############################################################################
# Market Analysis
##############################################################################

# Consolidated Class - Replace BOTH existing classes
# (LorentzianDistanceClassifier and MarketRegimeClassifier) with this one.
# Ensure necessary imports like numpy, statistics, asyncio, etc. are present at the top of the file.

class LorentzianDistanceClassifier:
    """
    Classifies market regimes using Lorentzian distance methodology.
    Combines features from previous LorentzianDistanceClassifier and MarketRegimeClassifier.
    """
    def __init__(self, lookback_period: int = 20):
        """Initialize Lorentzian classifier"""
        self.lookback_period = lookback_period
        self.price_history = {}  # symbol -> List[float]
        self.regime_history = {} # symbol -> List[str] (history of classified regimes)
        self.volatility_history = {} # symbol -> List[float]
        self.atr_history = {}  # symbol -> List[float]
        self.regimes = {}  # symbol -> Dict[str, Any] (stores latest regime data)
        self._lock = asyncio.Lock()
        self.logger = get_module_logger(__name__) # Assuming get_module_logger is available

    async def add_price_data(self, symbol: str, price: float, timeframe: str, atr: Optional[float] = None):
        """Add price data for a symbol and update regime classification"""
        async with self._lock:
            # Initialize data structures if needed
            if symbol not in self.price_history:
                self.price_history[symbol] = []
                self.regime_history[symbol] = []
                self.volatility_history[symbol] = []
                self.atr_history[symbol] = []
                self.regimes[symbol] = {} # Initialize regime storage for the symbol

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
                return False, {"stop_loss": 1.0, "take_profit": 1.0, "trailing_stop": 1.0}
            current_regime = self.regime_history[symbol][-1]

        # Check regime stability (e.g., last 3 regimes are the same)
        recent_regimes = self.regime_history.get(symbol, [])[-3:]
        is_stable = len(recent_regimes) >= 3 and len(set(recent_regimes)) == 1

        # Default adjustments (no change)
        adjustments = {"stop_loss": 1.0, "take_profit": 1.0, "trailing_stop": 1.0}

        # Apply adjustments only if the regime is stable
        if is_stable:
            if "volatile" in current_regime:
                adjustments = {"stop_loss": 1.5, "take_profit": 2.0, "trailing_stop": 1.25}
            elif "trending" in current_regime:
                adjustments = {"stop_loss": 1.25, "take_profit": 1.5, "trailing_stop": 1.1}
            elif "ranging" in current_regime:
                adjustments = {"stop_loss": 0.8, "take_profit": 0.8, "trailing_stop": 0.9}
            elif "momentum" in current_regime:
                adjustments = {"stop_loss": 1.2, "take_profit": 1.7, "trailing_stop": 1.3}

        # Determine if any adjustment is actually needed
        should_adjust = is_stable and any(v != 1.0 for v in adjustments.values())
        return should_adjust, adjustments

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


class MarketStructureAnalyzer:
    def __init__(self):
        self.support_levels = {}
        self.resistance_levels = {}
        self.swing_points = {}
        
    async def analyze_market_structure(self, symbol: str, timeframe: str, 
                                     high: float, low: float, close: float) -> Dict[str, Any]:
        """Analyze market structure for better stop loss placement"""
        if symbol not in self.support_levels:
            self.support_levels[symbol] = []
        if symbol not in self.resistance_levels:
            self.resistance_levels[symbol] = []
        if symbol not in self.swing_points:
            self.swing_points[symbol] = []
            
        # Update swing points
        self._update_swing_points(symbol, high, low)
        
        # Identify support and resistance levels
        self._identify_levels(symbol)
        
        # Get nearest levels for stop loss calculation
        nearest_support = self._get_nearest_support(symbol, close)
        nearest_resistance = self._get_nearest_resistance(symbol, close)
        
        return {
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'swing_points': self.swing_points[symbol][-5:] if len(self.swing_points[symbol]) >= 5 else self.swing_points[symbol],
            'support_levels': self.support_levels[symbol],
            'resistance_levels': self.resistance_levels[symbol]
        }
        
    def _update_swing_points(self, symbol: str, high: float, low: float):
        """Update swing high and low points"""
        if symbol not in self.swing_points:
            self.swing_points[symbol] = []
            
        if len(self.swing_points[symbol]) < 2:
            self.swing_points[symbol].append({'high': high, 'low': low})
            return
            
        last_point = self.swing_points[symbol][-1]
        if high > last_point['high']:
            self.swing_points[symbol].append({'high': high, 'low': low})
        elif low < last_point['low']:
            self.swing_points[symbol].append({'high': high, 'low': low})
            
    def _identify_levels(self, symbol: str):
        """Identify support and resistance levels from swing points"""
        points = self.swing_points.get(symbol, [])
        if len(points) < 3:
            return
            
        # Identify support levels (local minima)
        for i in range(1, len(points)-1):
            if points[i]['low'] < points[i-1]['low'] and points[i]['low'] < points[i+1]['low']:
                if points[i]['low'] not in self.support_levels[symbol]:
                    self.support_levels[symbol].append(points[i]['low'])
                    
        # Identify resistance levels (local maxima)
        for i in range(1, len(points)-1):
            if points[i]['high'] > points[i-1]['high'] and points[i]['high'] > points[i+1]['high']:
                if points[i]['high'] not in self.resistance_levels[symbol]:
                    self.resistance_levels[symbol].append(points[i]['high'])
                    
    def _get_nearest_support(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest support level below current price"""
        supports = sorted([s for s in self.support_levels.get(symbol, []) if s < current_price])
        return supports[-1] if supports else None
        
    def _get_nearest_resistance(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest resistance level above current price"""
        resistances = sorted([r for r in self.resistance_levels.get(symbol, []) if r > current_price])
        return resistances[0] if resistances else None


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
                                 current_atr: Optional[float] = None) -> Dict[str, Any]:
        """Update trailing stop based on current price, keeping between 50-100 pips"""
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
                
            # Update highest/lowest prices seen
            if ts_data["direction"] == "BUY":
                if current_price > ts_data["highest_price"]:
                    ts_data["highest_price"] = current_price
            else:  # SELL
                if current_price < ts_data["lowest_price"]:
                    ts_data["lowest_price"] = current_price
                    
            # Get constraints
            min_distance = ts_data["min_distance"]  # 50 pips
            pip_value = ts_data["pip_value"]
            
            # Calculate new trail distance based on ATR
            trail_distance = min_distance  # Default to minimum 50 pips
            
            if ts_data["atr_value"] > 0:
                # Consider ATR-based distance but ensure it's at least 50 pips
                atr_distance = ts_data["atr_value"] * 2  # 2 x ATR
                trail_distance = max(min_distance, min(atr_distance, ts_data["max_distance"]))
            
            # Update trailing stop if price has moved favorably
            if ts_data["direction"] == "BUY":
                # Calculate new stop level based on highest price and trail distance
                new_stop = ts_data["highest_price"] - trail_distance
                
                # Only move stop up, never down
                if new_stop > ts_data["current_stop"]:
                    ts_data["current_stop"] = new_stop
                    ts_data["updated_at"] = datetime.now(timezone.utc)
                    logger.info(f"Updated trailing stop for {position_id} to {new_stop} (distance: {trail_distance/pip_value} pips)")
                    
            else:  # SELL
                # Calculate new stop level
                new_stop = ts_data["lowest_price"] + trail_distance
                
                # Only move stop down, never up
                if new_stop < ts_data["current_stop"]:
                    ts_data["current_stop"] = new_stop
                    ts_data["updated_at"] = datetime.now(timezone.utc)
                    logger.info(f"Updated trailing stop for {position_id} to {new_stop} (distance: {trail_distance/pip_value} pips)")
                    
            # Check if stop is hit
            stop_hit = False
            if ts_data["direction"] == "BUY":
                stop_hit = current_price <= ts_data["current_stop"]
            else:  # SELL
                stop_hit = current_price >= ts_data["current_stop"]
                
            # Return result
            return {
                "status": "hit" if stop_hit else "active",
                "stop_level": ts_data["current_stop"],
                "initial_stop": ts_data["initial_stop"],
                "entry_price": ts_data["entry_price"],
                "direction": ts_data["direction"],
                "price_extreme": ts_data["highest_price"] if ts_data["direction"] == "BUY" else ts_data["lowest_price"],
                "trail_distance_pips": trail_distance / pip_value,
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

class HLC3ExitManager:
    """
    Manages the specific HLC3 timed exit for positions opened without a stop loss.
    """
    def __init__(self, alert_handler_instance):
        """
        Initialize HLC3 Exit Manager.
        Requires an instance of EnhancedAlertHandler to trigger exits.
        """
        self.alert_handler = alert_handler_instance # Reference to the main handler
        self.positions_to_monitor = {} # position_id -> {symbol, timeframe, entry_ts_utc}
        self._lock = asyncio.Lock()
        self.candle_cache = {} # Simple cache: (instrument, tf, from_ts) -> {'data': candles, 'timestamp': now}
        self.cache_ttl = 60 # Cache candles for 60 seconds
        self.logger = get_module_logger("HLC3ExitManager") # Get a dedicated logger

    async def register_position(self, position_id: str, symbol: str, timeframe: str, entry_timestamp_utc_iso: str):
        """Register a position that requires the HLC3 4th candle exit."""
        async with self._lock:
            try:
                # Parse the ISO timestamp string back into a datetime object
                entry_dt = datetime.fromisoformat(entry_timestamp_utc_iso.replace("Z", "+00:00"))
                if entry_dt.tzinfo is None:
                     entry_dt = entry_dt.replace(tzinfo=timezone.utc) # Ensure timezone aware

                self.positions_to_monitor[position_id] = {
                    "symbol": symbol,
                    "timeframe": timeframe, # Expecting OANDA format (e.g., 'H1')
                    "entry_ts_utc": entry_dt
                }
                self.logger.info(f"Registered position {position_id} for HLC3 4th candle exit monitoring.")
            except Exception as e:
                 self.logger.error(f"Failed to register {position_id} for HLC3 exit: {e}", exc_info=True)

    def _get_candle_duration(self, timeframe: str) -> Optional[timedelta]:
        """Convert OANDA timeframe string to timedelta."""
        # Simple mapping (adjust or expand as needed for your supported timeframes)
        try:
            unit = timeframe[0].upper()
            value = int(timeframe[1:])
            if unit == 'M': return timedelta(minutes=value)
            if unit == 'H': return timedelta(hours=value)
            if unit == 'D': return timedelta(days=1) # OANDA 'D' is daily
            if unit == 'W': return timedelta(weeks=1) # OANDA 'W' is weekly
            if unit == 'M': return timedelta(days=30) # OANDA 'M' is monthly (approx)
            self.logger.warning(f"Unsupported timeframe unit for duration calculation: {timeframe}")
            return None
        except:
             # Handle cases like 'D' without a number or invalid formats
            if timeframe == 'D': return timedelta(days=1)
            if timeframe == 'W': return timedelta(weeks=1)
            if timeframe == 'M': return timedelta(days=30) # Approx month
            self.logger.error(f"Could not parse timeframe '{timeframe}' for duration.")
            return None

    async def check_exits(self):
        """Check monitored positions and trigger HLC3 exits if conditions are met."""
        async with self._lock:
            if not self.positions_to_monitor:
                return

            now_utc = datetime.now(timezone.utc)
            positions_to_remove = []

            # Create a copy of items to avoid issues if dict changes during iteration
            items_to_check = list(self.positions_to_monitor.items())

            for position_id, data in items_to_check:
                # Ensure data is still valid (might have been removed by another task)
                if position_id not in self.positions_to_monitor:
                    continue

                try:
                    symbol = data["symbol"]
                    oanda_tf = data["timeframe"] # OANDA format like 'H1'
                    entry_ts = data["entry_ts_utc"]

                    candle_duration = self._get_candle_duration(oanda_tf)
                    if not candle_duration:
                        self.logger.warning(f"Cannot determine candle duration for {position_id}, tf={oanda_tf}. Removing from monitor.")
                        positions_to_remove.append(position_id)
                        continue

                    # Calculate the expected END time of the 4th candle after entry
                    target_candle_end_time = entry_ts + (5 * candle_duration)

                    # Check if the current time is past the candle's end time + buffer
                    if now_utc < (target_candle_end_time + timedelta(minutes=1)):
                        continue # Not time yet

                    self.logger.info(f"Time to check HLC3 exit for {position_id}. 4th candle end: {target_candle_end_time}")

                    # --- Fetch the Specific 4th Candle ---
                    target_candle_start_time = entry_ts + (4 * candle_duration)
                    oanda_inst_name = symbol.replace('/', '_')
                    params = {
                        "granularity": oanda_tf,
                        "from": target_candle_start_time.isoformat(timespec='seconds') + "Z",
                        "count": 1
                    }

                    try:
                        # --- Fetch Candle Data ---
                        cache_key = (oanda_inst_name, oanda_tf, params['from'])
                        cached_entry = self.candle_cache.get(cache_key)
                        candles = None
                        if cached_entry and (now_utc - cached_entry['timestamp']).total_seconds() < self.cache_ttl:
                            candles = cached_entry['data']
                            self.logger.debug(f"Using cached 4th candle for {position_id}")
                        else:
                            r = instruments.InstrumentsCandles(instrument=oanda_inst_name, params=params)
                            resp = oanda.request(r) # Use the global oanda client instance
                            candles = resp.get("candles", [])
                            self.candle_cache[cache_key] = {'data': candles, 'timestamp': now_utc}
                            self.logger.debug(f"Fetched 4th candle ({len(candles)}) for {position_id}")

                        if not candles:
                            self.logger.warning(f"HLC3 Check: Could not fetch 4th candle for {position_id} at {target_candle_start_time}.")
                            continue # Try again next cycle

                        target_candle = candles[0]

                        # --- Verify Candle and Calculate HLC3 ---
                        if target_candle and target_candle.get('complete', False):
                            candle_time_str = target_candle.get('time', '').split('.')[0] + 'Z'
                            candle_time = datetime.strptime(candle_time_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

                            # Verify it's the correct candle (allow small difference)
                            if abs((candle_time - target_candle_start_time).total_seconds()) > candle_duration.total_seconds() * 0.1: # Allow 10% deviation
                                 self.logger.warning(f"HLC3 Check: Fetched candle time {candle_time} doesn't match target {target_candle_start_time} closely for {position_id}. Skipping.")
                                 continue

                            h = float(target_candle['mid']['h'])
                            l = float(target_candle['mid']['l'])
                            c = float(target_candle['mid']['c'])
                            hlc3_price = (h + l + c) / 3.0

                            self.logger.info(f"Triggering HLC3 exit for {position_id} based on CLOSED 4th candle ({target_candle.get('time')}) at price {hlc3_price:.5f}")

                            # --- Trigger Exit ---
                            # Use the passed alert_handler instance to call the internal exit method
                            exit_success = await self.alert_handler._exit_position(
                                position_id=position_id,
                                exit_price=hlc3_price,
                                reason="timed_exit_hlc3_4th_candle_close"
                            )

                            if exit_success:
                                positions_to_remove.append(position_id) # Mark for removal AFTER exit success
                            else:
                                self.logger.error(f"HLC3 Check: Failed to execute HLC3 exit via alert_handler for {position_id}.")
                                # Keep monitoring, maybe it will succeed next time? Or add retry limit here?

                        elif target_candle:
                            self.logger.info(f"HLC3 Check: 4th candle for {position_id} fetched but not complete yet.")
                        else:
                             # This case should ideally not happen if candles list wasn't empty
                             self.logger.warning(f"HLC3 Check: Target candle logic failed for {position_id}")


                    except V20Error as v20e:
                        self.logger.error(f"HLC3 Check: OANDA V20Error fetching 4th candle for {position_id}: {v20e.msg}")
                    except Exception as fetch_e:
                        self.logger.error(f"HLC3 Check: Error fetching/processing 4th candle for {position_id}: {fetch_e}", exc_info=True)

                except Exception as outer_e:
                    self.logger.error(f"HLC3 Check: Outer error processing check for {position_id}: {outer_e}", exc_info=True)
                    # Decide if position should be removed on error (e.g., if data is invalid)
                    # positions_to_remove.append(position_id)

            # --- Clean up exited positions ---
            for pid in positions_to_remove:
                if pid in self.positions_to_monitor:
                    del self.positions_to_monitor[pid]
                    self.logger.info(f"Successfully removed {pid} from HLC3 monitoring after exit.")

    async def remove_position(self, position_id: str):
        """Remove position explicitly if closed by other means."""
        async with self._lock:
            if position_id in self.positions_to_monitor:
                del self.positions_to_monitor[position_id]
                self.logger.info(f"Removed {position_id} from HLC3 monitoring (likely closed externally).")

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


# Assumed imports for other managers and helpers:
# from .managers import SystemMonitor, PositionTracker, EnhancedRiskManager, ...
# from .helpers import get_account_balance, standardize_symbol, ...
# from .execution import execute_oanda_order, OrderResult, close_position, ...
# from .hlc3_manager import HLC3ExitManager # Make sure HLC3ExitManager class is defined before this

# Get logger instance
logger = get_module_logger(__name__) # Assuming get_module_logger is defined

class EnhancedAlertHandler:
    """
    Enhanced alert handler with integrated HLC3 timed exit logic for positions
    opened without a stop loss due to broker rejection.
    """
    def __init__(self):
        """Initialize alert handler and its components."""
        self.logger = get_module_logger("EnhancedAlertHandler") # Use specific logger
        # Initialize components (replace placeholders with actual initializations)
        self.system_monitor: Optional[SystemMonitor] = None
        self.position_tracker: Optional[PositionTracker] = None
        self.risk_manager: Optional[EnhancedRiskManager] = None
        self.volatility_monitor: Optional[VolatilityMonitor] = None
        self.market_structure: Optional[MarketStructureAnalyzer] = None
        self.regime_classifier: Optional[LorentzianDistanceClassifier] = None
        self.multi_stage_tp_manager: Optional[MultiStageTakeProfitManager] = None
        self.time_based_exit_manager: Optional[TimeBasedExitManager] = None
        self.dynamic_exit_manager: Optional[DynamicExitManager] = None
        self.position_journal: Optional[PositionJournal] = None
        self.notification_system: Optional[NotificationSystem] = None

        # NEW: Initialize HLC3 Exit Manager, passing self (the handler instance)
        self.hlc3_exit_manager: Optional[HLC3ExitManager] = HLC3ExitManager(self)

        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        self.scheduled_task_handle: Optional[asyncio.Task] = None # To hold the task

    async def start(self):
        """Initialize and start all components."""
        if self._running:
            self.logger.info("Alert handler already running.")
            return True

        self.logger.info("Starting EnhancedAlertHandler...")
        try:
            # Initialize system monitor first
            self.system_monitor = SystemMonitor() # Assume initialized
            await self.system_monitor.register_component("alert_handler", "initializing")

            # Initialize other core components (DB, Position Tracker, Risk Manager)
            # These initializations depend on your global setup (e.g., db_manager)
            self.position_tracker = PositionTracker(db_manager=db_manager) # Assume db_manager is global/accessible
            await self.system_monitor.register_component("position_tracker", "initializing")
            await self.position_tracker.start() # Start tracker (loads from DB)
            await self.system_monitor.update_component_status("position_tracker", "ok")

            self.risk_manager = EnhancedRiskManager() # Assume initialized
            await self.system_monitor.register_component("risk_manager", "initializing")
            account_balance = await get_account_balance() # Assume helper exists
            await self.risk_manager.initialize(account_balance)
            await self.system_monitor.update_component_status("risk_manager", "ok")

            # Initialize Analysis Components
            self.volatility_monitor = VolatilityMonitor() # Assume initialized
            await self.system_monitor.register_component("volatility_monitor", "ok")
            self.market_structure = MarketStructureAnalyzer() # Assume initialized
            await self.system_monitor.register_component("market_structure", "ok")
            self.regime_classifier = LorentzianDistanceClassifier() # Assume initialized
            await self.system_monitor.register_component("regime_classifier", "ok")

            # Initialize Exit Management Components
            self.multi_stage_tp_manager = MultiStageTakeProfitManager(position_tracker=self.position_tracker)
            await self.system_monitor.register_component("multi_stage_tp_manager", "ok")
            self.time_based_exit_manager = TimeBasedExitManager() # Assume initialized
            await self.system_monitor.register_component("time_based_exit_manager", "initializing")
            await self.time_based_exit_manager.start()
            await self.system_monitor.update_component_status("time_based_exit_manager", "ok")
            self.dynamic_exit_manager = DynamicExitManager( # Assume initialized
                position_tracker=self.position_tracker,
                multi_stage_tp_manager=self.multi_stage_tp_manager
            )
            self.dynamic_exit_manager.lorentzian_classifier = self.regime_classifier # Assign classifier
            await self.system_monitor.register_component("dynamic_exit_manager", "initializing")
            await self.dynamic_exit_manager.start()
            await self.system_monitor.update_component_status("dynamic_exit_manager", "ok")

            # NEW: Register HLC3 Manager
            await self.system_monitor.register_component("hlc3_exit_manager", "ok") # No async start needed currently

            # Initialize Other Components
            self.position_journal = PositionJournal() # Assume initialized
            await self.system_monitor.register_component("position_journal", "ok")
            self.notification_system = NotificationSystem() # Assume initialized
            await self.system_monitor.register_component("notification_system", "initializing")
            # Configure notification channels (as shown previously)
            # ... channel config ...
            await self.notification_system.configure_channel("console", {}) # Ensure console is active
            await self.system_monitor.update_component_status("notification_system", "ok")


            # Perform initial sync/cleanup
            await self.position_tracker.clean_up_duplicate_positions()

            # Start scheduled tasks handler in the background
            self.scheduled_task_handle = asyncio.create_task(self.handle_scheduled_tasks())

            self._running = True
            await self.system_monitor.update_component_status("alert_handler", "ok")

            # Send startup notification
            open_pos_count = len(self.position_tracker.positions) if self.position_tracker else 0
            await self.notification_system.send_notification(
                f"Trading system started successfully with {open_pos_count} open positions",
                "info"
            )

            self.logger.info("EnhancedAlertHandler started successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error starting EnhancedAlertHandler: {e}", exc_info=True)
            if self.system_monitor:
                await self.system_monitor.update_component_status("alert_handler", "error", f"Startup failed: {e}")
            self._running = False
            return False

    async def stop(self):
        """Stop all components gracefully."""
        if not self._running:
            self.logger.info("Alert handler already stopped.")
            return True

        self.logger.info("Stopping EnhancedAlertHandler...")
        self._running = False # Signal loops to stop

        # Cancel the scheduled task
        if self.scheduled_task_handle:
            self.scheduled_task_handle.cancel()
            try:
                await self.scheduled_task_handle
            except asyncio.CancelledError:
                self.logger.info("Scheduled tasks handler stopped.")
            except Exception as e:
                 self.logger.error(f"Error during scheduled task cancellation: {e}", exc_info=True)
            self.scheduled_task_handle = None


        try:
            # Update status
            if self.system_monitor:
                await self.system_monitor.update_component_status("alert_handler", "shutting_down")

            # Send shutdown notification
            if self.notification_system:
                await self.notification_system.send_notification("Trading system shutting down", "info")

            # Ensure all position data is saved
            if self.position_tracker:
                await self.position_tracker.sync_with_database()
                await self.position_tracker.stop()

            # Stop other components that have stop methods
            if self.dynamic_exit_manager and hasattr(self.dynamic_exit_manager, 'stop'):
                 await self.dynamic_exit_manager.stop()
            if self.time_based_exit_manager and hasattr(self.time_based_exit_manager, 'stop'):
                 await self.time_based_exit_manager.stop()

            # Mark alert handler status as stopped
            if self.system_monitor:
                 await self.system_monitor.update_component_status("alert_handler", "stopped")

            self.logger.info("EnhancedAlertHandler stopped successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping EnhancedAlertHandler: {e}", exc_info=True)
            if self.system_monitor:
                 await self.system_monitor.update_component_status("alert_handler", "error", f"Shutdown error: {e}")
            return False

    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for processing alerts (webhook or manual)."""
        # Use a lock to prevent processing multiple alerts simultaneously if needed
        # async with self._lock: # Consider if needed based on alert frequency/complexity
        request_id = alert_data.get("request_id", str(uuid.uuid4())) # Ensure unique ID
        alert_id = alert_data.get("id", request_id) # Use provided ID or request ID
        symbol = alert_data.get("symbol", alert_data.get("instrument", "UNKNOWN"))
        action = alert_data.get("action", alert_data.get("direction", "UNKNOWN")).upper()
        self.logger.info(f"[{request_id}] Received alert", extra={"alert_data": alert_data})

        # Check for duplicate alerts (optional but good practice)
        if alert_id in self.active_alerts:
            self.logger.warning(f"[{request_id}] Duplicate alert ignored: {alert_id}")
            return {"status": "ignored", "message": "Duplicate alert", "alert_id": alert_id}
        self.active_alerts.add(alert_id)

        if not self._running:
             self.logger.warning(f"[{request_id}] Alert received but handler is not running.")
             self.active_alerts.discard(alert_id)
             return {"status": "rejected", "message": "System not running", "alert_id": alert_id}


        # Update system status
        if self.system_monitor:
            await self.system_monitor.update_component_status(
                "alert_handler", "processing", f"Processing alert {alert_id} for {symbol} {action}"
            )

        try:
            # Route based on action type
            if action in ["BUY", "SELL"]:
                result = await self._process_entry_alert(alert_data)
            elif action in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]:
                result = await self._process_exit_alert(alert_data)
            elif action == "UPDATE":
                result = await self._process_update_alert(alert_data)
            else:
                self.logger.warning(f"[{request_id}] Unknown action type in alert: {action}")
                result = {"status": "error", "message": f"Unknown action type: {action}", "alert_id": alert_id}

            # Log final result
            self.logger.info(f"[{request_id}] Alert processing completed.", extra={"result": result})
            return result

        except Exception as e:
            self.logger.error(f"[{request_id}] Unhandled error processing alert {alert_id}: {e}", exc_info=True)
            # Record error if error recovery system is available
            if 'error_recovery' in globals() and error_recovery: # Check if global error_recovery exists
                await error_recovery.record_error("alert_processing", {"error": str(e), "alert": alert_data})
            return {"status": "error", "message": f"Internal Server Error: {e}", "alert_id": alert_id}

        finally:
            self.active_alerts.discard(alert_id)
            # Update system status back to ok (or appropriate state)
            if self.system_monitor:
                # Check overall system health before setting back to 'ok'
                # For now, assume ok if no exception was raised during processing
                 current_status_info = await self.system_monitor.get_component_status("alert_handler")
                 if current_status_info and current_status_info.get("status") == "processing":
                     await self.system_monitor.update_component_status("alert_handler", "ok", "")


    # --- Internal Processing Methods ---

    async def _process_entry_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an entry alert (BUY or SELL) with HLC3 flagging."""
        request_id = alert_data.get("request_id", str(uuid.uuid4())) # Use existing or generate
        self.logger.info(f"[{request_id}] Processing entry alert: {alert_data.get('symbol')} {alert_data.get('action')}")

        # Validate alert data (symbol, action, risk_percent/percentage)
        symbol = alert_data.get("instrument", alert_data.get("symbol"))
        action = alert_data.get("direction", alert_data.get("action")).upper()
        # Use 'risk_percent' from TradingViewAlertPayload if validated, else try 'percentage'
        risk_input = alert_data.get("risk_percent", alert_data.get("percentage", 20.0)) # Default if missing

        if not symbol or action not in ["BUY", "SELL"]:
            msg = "Missing or invalid symbol/action in entry alert."
            self.logger.error(f"[{request_id}] {msg}")
            return {"status": "rejected", "message": msg, "alert_id": request_id}
        try:
            risk_percentage_input = float(risk_input)
        except ValueError:
             msg = f"Invalid risk/percentage value: {risk_input}"
             self.logger.error(f"[{request_id}] {msg}")
             return {"status": "rejected", "message": msg, "alert_id": request_id}

        standardized_symbol = standardize_symbol(symbol)
        timeframe = alert_data.get("timeframe", "1H") # Default timeframe
        normalized_tf = normalize_timeframe(timeframe, target="OANDA") # For consistency

        # Check if tradeable
        is_tradeable, reason = is_instrument_tradeable(standardized_symbol)
        if not is_tradeable:
            self.logger.warning(f"[{request_id}] Trading not allowed for {standardized_symbol}: {reason}")
            return {"status": "rejected", "message": f"Trading not allowed: {reason}", "alert_id": request_id}

        # --- Prepare for Trade Execution ---
        position_id = f"{standardized_symbol}_{action}_{uuid.uuid4().hex[:8]}"
        metadata = { # Base metadata
            "alert_id": request_id,
            "comment": alert_data.get("comment"),
            "strategy": alert_data.get("strategy"),
            "alert_timestamp": alert_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            # Add any other relevant fields from alert_data not used directly
            **{k: v for k, v in alert_data.items() if k not in ['instrument','symbol','direction','action','risk_percent','percentage','timeframe','comment','strategy','request_id','id']}
        }

        try:
            # Get necessary data (price, SL, units) before execution
            entry_price = alert_data.get("entry_price")
            if entry_price is None: entry_price = await get_current_price(standardized_symbol, action)

            # Calculate SL using the simplified 100 pip logic (or get from alert)
            stop_loss = alert_data.get("stop_loss")
            if stop_loss is None:
                 atr_value = await get_atr(standardized_symbol, timeframe) # Needed for calculation helper
                 stop_loss = await calculate_structure_based_stop_loss( # This now implements the 100 pip logic
                     standardized_symbol, entry_price, action, timeframe, atr_value=atr_value
                 )
            else:
                 stop_loss = float(stop_loss) # Use provided SL if available

            # Calculate units using 15% equity logic
            account_balance = await get_account_balance()
            units, _ = await calculate_pure_position_size(
                standardized_symbol, 0.15, account_balance, action # Pass 0.15 directly
            )
            metadata["calculated_units"] = units # Store calculated units

            # Calculate TP using 100 pip logic (or get from alert)
            take_profit = alert_data.get("take_profit")
            if take_profit is None and stop_loss is not None:
                 tp_distance = 100 * (0.01 if 'JPY' in standardized_symbol else 0.0001) # 100 pips
                 take_profit = entry_price + (tp_distance * (1 if action == "BUY" else -1))
            elif take_profit is not None:
                 take_profit = float(take_profit)


            # --- Execute Trade ---
            trade_result: OrderResult = await execute_oanda_order(
                instrument=standardized_symbol,
                direction=action,
                risk_percent=0.15, # Pass dummy value
                entry_price=entry_price,
                stop_loss=stop_loss, # Pass calculated/provided SL
                take_profit=take_profit, # Pass calculated/provided TP
                timeframe=timeframe, # Pass original timeframe
                units=units # Pass calculated units
            )

            # --- Process Execution Result ---
            if not trade_result.get("success", False):
                error_msg = trade_result.get('error', 'Unknown execution error')
                self.logger.error(f"[{request_id}] Trade execution failed: {error_msg}", extra={"details": trade_result.get("details")})
                return {"status": "error", "message": f"Trade execution failed: {error_msg}", "alert_id": request_id}

            # Success - update with actual filled data
            filled_price = trade_result.get("entry_price", entry_price)
            filled_units = trade_result.get("units", int(units))
            actual_sl = trade_result.get("stop_loss") # SL actually set
            actual_tp = trade_result.get("take_profit") # TP actually set
            oanda_order_id = trade_result.get("order_id")
            metadata["oanda_order_id"] = oanda_order_id # Add OANDA ID to metadata

            # --- Flag for HLC3 Exit if SL was omitted ---
            sl_omitted = trade_result.get("sl_omitted_due_to_rejection", False)
            entry_ts_iso = datetime.now(timezone.utc).isoformat() # Use current time as entry time

            if sl_omitted:
                self.logger.warning(f"[{request_id}] Position {position_id} opened without Stop Loss. Flagging for HLC3 exit.")
                metadata["timed_exit_required"] = True
                metadata["timed_exit_type"] = "hlc3_4th_candle"
                metadata["entry_timestamp_utc"] = entry_ts_iso

                if self.hlc3_exit_manager:
                    await self.hlc3_exit_manager.register_position(
                        position_id, standardized_symbol, normalized_tf, entry_ts_iso
                    )
                else:
                     self.logger.error(f"[{request_id}] HLC3ExitManager not available for {position_id}")

            # --- Record and Register ---
            if self.position_tracker:
                await self.position_tracker.record_position(
                    position_id=position_id, symbol=standardized_symbol, action=action,
                    timeframe=normalized_tf, entry_price=filled_price, size=abs(filled_units),
                    stop_loss=actual_sl, take_profit=actual_tp, metadata=metadata
                )

            if self.risk_manager:
                await self.risk_manager.register_position(
                    position_id=position_id, symbol=standardized_symbol, action=action,
                    size=abs(filled_units), entry_price=filled_price, stop_loss=actual_sl,
                    account_risk=0.15, timeframe=normalized_tf # Register with 15% risk assumption
                )

            # (Register with TimeBasedExitManager, DynamicExitManager - keep this logic if needed)
            if self.time_based_exit_manager and not sl_omitted: # Don't register standard time exit if HLC3 is active
                 self.time_based_exit_manager.register_position(
                      position_id, standardized_symbol, action, datetime.fromisoformat(entry_ts_iso), normalized_tf
                 )
            if self.dynamic_exit_manager and not sl_omitted: # Don't init dynamic exits if HLC3 is active? (Decide policy)
                await self.dynamic_exit_manager.initialize_exits(
                     position_id, standardized_symbol, filled_price, action, actual_sl, normalized_tf
                )


            if self.position_journal:
                 # Fetch market context
                 market_regime = self.regime_classifier.get_regime_data(standardized_symbol).get("regime", "unknown") if self.regime_classifier else "unknown"
                 volatility_state = self.volatility_monitor.get_volatility_state(standardized_symbol).get("volatility_state", "normal") if self.volatility_monitor else "normal"
                 await self.position_journal.record_entry(
                     position_id, standardized_symbol, action, normalized_tf, filled_price, abs(filled_units),
                     strategy=metadata.get("strategy", "unknown"), stop_loss=actual_sl, take_profit=actual_tp,
                     market_regime=market_regime, volatility_state=volatility_state, metadata=metadata
                 )

            if self.notification_system:
                 risk_display = 15.0 # Fixed 15%
                 await self.notification_system.send_notification(
                     f"✅ New Position: {action} {standardized_symbol} @ {filled_price:.5f} ({abs(filled_units)} units, Risk: {risk_display:.1f}%). SL={actual_sl}, TP={actual_tp}. ID: {position_id}",
                     "info"
                 )
                 if sl_omitted:
                      await self.notification_system.send_notification(
                           f"⚠️ Position {position_id} ({standardized_symbol}) opened WITHOUT STOP LOSS. HLC3 exit active.", "warning"
                      )


            self.logger.info(f"[{request_id}] Entry alert processing completed successfully for {position_id}")
            return {
                "status": "success", "message": f"Position {position_id} opened.",
                "position_id": position_id, "symbol": standardized_symbol, "action": action,
                "price": filled_price, "size": abs(filled_units), "stop_loss": actual_sl, "take_profit": actual_tp,
                "alert_id": request_id, "warning": trade_result.get("warning")
            }

        except Exception as e:
            self.logger.error(f"[{request_id}] Unhandled exception processing entry alert: {e}", exc_info=True)
            return {"status": "error", "message": f"Internal error: {e}", "alert_id": request_id}

    # --- Other internal methods (_process_exit_alert, _process_update_alert) ---
    # Ensure these methods correctly interact with HLC3ExitManager if needed,
    # especially _exit_position should call self.hlc3_exit_manager.remove_position(position_id)

    async def _process_exit_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an exit alert (CLOSE, CLOSE_LONG, CLOSE_SHORT)"""
        request_id = alert_data.get("request_id", str(uuid.uuid4()))
        alert_id = alert_data.get("id", request_id)
        symbol = alert_data.get("instrument", alert_data.get("symbol"))
        action = alert_data.get("direction", alert_data.get("action")).upper()
        self.logger.info(f"[{request_id}] Processing exit alert for {symbol}, action: {action}")

        if not symbol or action not in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]:
            msg = "Missing or invalid symbol/action in exit alert."
            self.logger.error(f"[{request_id}] {msg}")
            return {"status": "rejected", "message": msg, "alert_id": alert_id}

        standardized_symbol = standardize_symbol(symbol)

        # Find positions to close
        positions_to_target = []
        if self.position_tracker:
            open_positions_data = await self.position_tracker.get_positions_by_symbol(standardized_symbol, status="open")
            for pos_data in open_positions_data:
                pos_id = pos_data.get("position_id")
                pos_action = pos_data.get("action")
                if not pos_id: continue

                if action == "CLOSE":
                    positions_to_target.append(pos_id)
                elif action == "CLOSE_LONG" and pos_action == "BUY":
                    positions_to_target.append(pos_id)
                elif action == "CLOSE_SHORT" and pos_action == "SELL":
                    positions_to_target.append(pos_id)
        else:
             self.logger.error(f"[{request_id}] Position Tracker not available to find positions for {standardized_symbol}.")
             return {"status": "error", "message": "Position Tracker unavailable", "alert_id": alert_id}


        if not positions_to_target:
            self.logger.warning(f"[{request_id}] No matching open positions found for {standardized_symbol} {action} to close.")
            return {"status": "warning", "message": "No matching open positions found", "alert_id": alert_id}

        # Determine exit price (use alert price or current price)
        exit_price_input = alert_data.get("price", alert_data.get("entry_price")) # Allow 'price' or 'entry_price' from alert
        exit_price = None
        if exit_price_input is not None:
             try:
                 exit_price = float(exit_price_input)
             except ValueError:
                  self.logger.warning(f"[{request_id}] Invalid exit price '{exit_price_input}' in alert, fetching current price.")
                  exit_price = None # Fallback to fetching

        if exit_price is None:
             try:
                  # Fetch price based on first position's direction (assuming all are same if using CLOSE_LONG/SHORT)
                  first_pos_data = await self.position_tracker.get_position_info(positions_to_target[0])
                  exit_side = "SELL" if first_pos_data.get("action") == "BUY" else "BUY"
                  exit_price = await get_current_price(standardized_symbol, exit_side)
             except Exception as e:
                  self.logger.error(f"[{request_id}] Failed to fetch current price for exit: {e}", exc_info=True)
                  return {"status": "error", "message": f"Failed to get exit price: {e}", "alert_id": alert_id}

        # Close targeted positions
        closed_count = 0
        failed_count = 0
        closed_results = []

        for position_id in positions_to_target:
            # Call the internal unified exit method
            success = await self._exit_position(
                position_id=position_id,
                exit_price=exit_price,
                reason=f"alert_{action.lower()}" # Reason based on alert action
            )
            if success:
                closed_count += 1
                # Optionally fetch final position data to include in response
                # final_pos_data = await self.position_tracker.get_position_info(position_id)
                # closed_results.append(final_pos_data)
            else:
                failed_count += 1

        # Prepare response
        if closed_count > 0:
            msg = f"Successfully closed {closed_count} position(s) for {standardized_symbol} via {action} alert."
            if failed_count > 0: msg += f" Failed to close {failed_count} position(s)."
            self.logger.info(f"[{request_id}] {msg}")
            return {"status": "success", "message": msg, "closed_count": closed_count, "failed_count": failed_count, "alert_id": alert_id}
        else:
             msg = f"Failed to close any positions for {standardized_symbol} via {action} alert."
             self.logger.error(f"[{request_id}] {msg}")
             return {"status": "error", "message": msg, "closed_count": 0, "failed_count": failed_count, "alert_id": alert_id}


    async def _process_update_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an update alert (stop loss, take profit)."""
        request_id = alert_data.get("request_id", str(uuid.uuid4()))
        alert_id = alert_data.get("id", request_id)
        symbol = alert_data.get("instrument", alert_data.get("symbol"))
        position_id = alert_data.get("position_id") # Specific ID takes precedence
        self.logger.info(f"[{request_id}] Processing update alert", extra={"alert_data": alert_data})

        stop_loss_input = alert_data.get("stop_loss")
        take_profit_input = alert_data.get("take_profit")

        if stop_loss_input is None and take_profit_input is None:
             return {"status": "rejected", "message": "No update values (stop_loss/take_profit) provided", "alert_id": alert_id}

        # Validate SL/TP values if provided
        stop_loss, take_profit = None, None
        try:
            if stop_loss_input is not None: stop_loss = float(stop_loss_input)
            if take_profit_input is not None: take_profit = float(take_profit_input)
        except ValueError:
             return {"status": "rejected", "message": "Invalid stop_loss or take_profit value", "alert_id": alert_id}

        targets = []
        if position_id:
             # Target specific position
             pos_data = await self.position_tracker.get_position_info(position_id) if self.position_tracker else None
             if pos_data and pos_data.get("status") == "open":
                 targets.append(position_id)
             elif pos_data:
                 self.logger.warning(f"[{request_id}] Position {position_id} is already closed. Cannot update.")
                 return {"status": "ignored", "message": f"Position {position_id} already closed", "alert_id": alert_id}
             else:
                 self.logger.warning(f"[{request_id}] Position {position_id} not found.")
                 return {"status": "error", "message": f"Position {position_id} not found", "alert_id": alert_id}
        elif symbol:
             # Target all open positions for the symbol
             standardized_symbol = standardize_symbol(symbol)
             open_positions_data = await self.position_tracker.get_positions_by_symbol(standardized_symbol, status="open") if self.position_tracker else []
             targets = [p.get("position_id") for p in open_positions_data if p.get("position_id")]
             if not targets:
                  self.logger.warning(f"[{request_id}] No open positions found for symbol {standardized_symbol} to update.")
                  return {"status": "warning", "message": f"No open positions for {standardized_symbol}", "alert_id": alert_id}
        else:
            return {"status": "rejected", "message": "Update alert requires either 'position_id' or 'symbol'", "alert_id": alert_id}

        # --- Apply Updates ---
        updated_count = 0
        failed_count = 0
        updated_positions_info = []

        for pid in targets:
             # Get current state before update for logging/journaling
             current_pos_data = await self.position_tracker.get_position_info(pid) if self.position_tracker else {}
             current_sl = current_pos_data.get("stop_loss")
             current_tp = current_pos_data.get("take_profit")

             # Apply update via PositionTracker
             success = await self.position_tracker.update_position(
                 position_id=pid,
                 stop_loss=stop_loss, # Pass None if not provided in alert
                 take_profit=take_profit # Pass None if not provided in alert
                 # metadata=metadata # Can add metadata updates here if needed
             )

             if success:
                 updated_count += 1
                 # Record adjustment in journal
                 if self.position_journal:
                      reason = f"alert_update_{alert_id}"
                      if stop_loss is not None and stop_loss != current_sl:
                           await self.position_journal.record_adjustment(pid, "stop_loss", current_sl, stop_loss, reason)
                      if take_profit is not None and take_profit != current_tp:
                           await self.position_journal.record_adjustment(pid, "take_profit", current_tp, take_profit, reason)
                 # Optional: Fetch updated position details for response
                 # updated_pos = await self.position_tracker.get_position_info(pid)
                 # updated_positions_info.append(updated_pos)
             else:
                 failed_count += 1
                 self.logger.error(f"[{request_id}] Failed to update position {pid}.")


        # Prepare response
        if updated_count > 0:
            msg = f"Successfully updated {updated_count} position(s)."
            if failed_count > 0: msg += f" Failed to update {failed_count} position(s)."
            self.logger.info(f"[{request_id}] {msg}")
            return {"status": "success", "message": msg, "updated_count": updated_count, "failed_count": failed_count, "alert_id": alert_id}
        else:
             msg = "Failed to update any targeted positions."
             self.logger.error(f"[{request_id}] {msg}")
             return {"status": "error", "message": msg, "updated_count": 0, "failed_count": failed_count, "alert_id": alert_id}


    # Internal helper to exit positions (ensure it removes from HLC3 manager)
    async def _exit_position(self, position_id: str, exit_price: float, reason: str) -> bool:
        """Internal unified method to process position exit."""
        self.logger.info(f"Processing exit for {position_id}, Reason: {reason}, Price: {exit_price}")
        success = False
        try:
            # 1. Get Position Info (needed for closing with broker and context)
            position_info = await self.position_tracker.get_position_info(position_id)
            if not position_info:
                self.logger.warning(f"_exit_position: Position {position_id} not found in tracker.")
                # If HLC3 manager still has it, try removing it
                if self.hlc3_exit_manager: await self.hlc3_exit_manager.remove_position(position_id)
                return False
            if position_info.get("status") == "closed":
                 self.logger.warning(f"_exit_position: Position {position_id} already closed.")
                 # If HLC3 manager still has it, try removing it
                 if self.hlc3_exit_manager: await self.hlc3_exit_manager.remove_position(position_id)
                 return True # Treat as success if already closed


            symbol = position_info.get("symbol")
            action_to_close = position_info.get("action") # BUY or SELL

            # 2. Close with Broker
            # Replace placeholder with your actual broker closing call
            # broker_success, broker_result = await close_position(position_info)
            broker_success, broker_result = await internal_close_position(position_info) # Placeholder

            if not broker_success:
                self.logger.error(f"Broker close failed for {position_id}: {broker_result.get('error')}")
                # Should we still update internal state? Depends on policy. Let's assume No for now.
                return False

            self.logger.info(f"Broker successfully closed position {position_id}.")

            # 3. Update Position Tracker
            close_result_tracker = await self.position_tracker.close_position(position_id, exit_price, reason)
            if not close_result_tracker.success:
                 self.logger.error(f"Failed updating position tracker for closed {position_id}: {close_result_tracker.error}")
                 # Log error but continue cleanup

            # 4. Update Risk Manager
            if self.risk_manager:
                await self.risk_manager.clear_position(position_id)

            # 5. Remove from Time-Based Exit Manager
            if self.time_based_exit_manager:
                 self.time_based_exit_manager.remove_position(position_id)

            # 6. Remove from Dynamic Exit Manager (if applicable)
            # if self.dynamic_exit_manager: await self.dynamic_exit_manager.remove_position(position_id)

            # 7. >>> Remove from HLC3 Exit Manager <<<
            if self.hlc3_exit_manager:
                await self.hlc3_exit_manager.remove_position(position_id) # Ensure removal

            # 8. Record Exit in Journal
            if self.position_journal:
                 pnl = close_result_tracker.position_data.get("pnl", 0.0) if close_result_tracker.position_data else 0.0
                 market_regime = "unknown"
                 volatility_state = "normal"
                 # Fetch context if possible
                 if self.regime_classifier: market_regime = self.regime_classifier.get_regime_data(symbol).get("regime", "unknown")
                 if self.volatility_monitor: volatility_state = self.volatility_monitor.get_volatility_state(symbol).get("volatility_state", "normal")

                 await self.position_journal.record_exit(
                     position_id, exit_price, reason, pnl,
                     market_regime=market_regime, volatility_state=volatility_state
                 )

            # 9. Send Notification
            if self.notification_system:
                 pnl = close_result_tracker.position_data.get("pnl", 0.0) if close_result_tracker.position_data else 0.0
                 level = "info" if pnl >= 0 else "warning"
                 await self.notification_system.send_notification(
                     f"✅ Position Closed: {position_id} ({symbol}) via {reason} @ {exit_price:.5f}. PnL: {pnl:.2f}", level
                 )

            self.logger.info(f"Successfully processed internal exit for {position_id}.")
            success = True # Mark as successful overall

        except Exception as e:
            self.logger.error(f"Error during _exit_position for {position_id}: {e}", exc_info=True)
            success = False

        return success


    # --- Scheduled Tasks ---
    async def handle_scheduled_tasks(self):
        """Handle scheduled tasks like managing exits, updating prices, etc."""
        self.logger.info("Starting scheduled tasks handler loop.")
        # Use instance variable for last_run times
        self.last_run_times = {
            "update_prices": datetime.now(timezone.utc) - timedelta(seconds=50),
            "check_exits": datetime.now(timezone.utc) - timedelta(minutes=4),
            "hlc3_exits": datetime.now(timezone.utc) - timedelta(seconds=55),
            "daily_reset": datetime.now(timezone.utc),
            "position_cleanup": datetime.now(timezone.utc),
            "database_sync": datetime.now(timezone.utc),
        }

        while self._running:
            try:
                current_time = datetime.now(timezone.utc)

                # --- Task Execution Checks ---
                tasks_to_run = []
                if (current_time - self.last_run_times["update_prices"]).total_seconds() >= 60:
                    tasks_to_run.append(self._update_position_prices())
                    self.last_run_times["update_prices"] = current_time

                if (current_time - self.last_run_times["check_exits"]).total_seconds() >= 300: # Check standard exits every 5 mins
                    tasks_to_run.append(self._check_position_exits())
                    self.last_run_times["check_exits"] = current_time

                if (current_time - self.last_run_times["hlc3_exits"]).total_seconds() >= 60: # Check HLC3 every 1 min
                    if self.hlc3_exit_manager: tasks_to_run.append(self.hlc3_exit_manager.check_exits())
                    self.last_run_times["hlc3_exits"] = current_time

                if current_time.day != self.last_run_times["daily_reset"].day:
                    tasks_to_run.append(self._perform_daily_reset())
                    self.last_run_times["daily_reset"] = current_time

                if (current_time - self.last_run_times["position_cleanup"]).total_seconds() >= 604800: # Weekly cleanup
                    tasks_to_run.append(self._cleanup_old_positions())
                    self.last_run_times["position_cleanup"] = current_time

                if (current_time - self.last_run_times["database_sync"]).total_seconds() >= 3600: # Hourly sync
                    tasks_to_run.append(self._sync_database())
                    self.last_run_times["database_sync"] = current_time

                # Run scheduled tasks concurrently if any are due
                if tasks_to_run:
                     self.logger.debug(f"Running {len(tasks_to_run)} scheduled tasks...")
                     await asyncio.gather(*tasks_to_run, return_exceptions=True) # Use gather to run concurrently, capture exceptions


                # Wait before the next loop iteration
                await asyncio.sleep(10) # Main check interval

            except asyncio.CancelledError:
                 self.logger.info("Scheduled tasks handler loop cancelled.")
                 break # Exit loop if cancelled
            except Exception as e:
                self.logger.error(f"Error in scheduled tasks loop: {str(e)}", exc_info=True)
                if 'error_recovery' in globals() and error_recovery:
                     await error_recovery.record_error("scheduled_tasks", {"error": str(e)})
                await asyncio.sleep(60) # Wait longer after an error

        self.logger.info("Scheduled tasks handler loop finished.")


    # --- Add placeholder/implementations for helper methods used above ---
    # These methods were called within the scheduled tasks loop

    async def _update_position_prices(self):
        """Placeholder: Update prices for all open positions."""
        if not self.position_tracker: return
        self.logger.debug("Running scheduled task: Update Position Prices")
        try:
            open_positions = await self.position_tracker.get_open_positions()
            symbols_to_update = set(pos_data['symbol'] for positions in open_positions.values() for pos_data in positions.values())

            for symbol in symbols_to_update:
                 try:
                      # Fetch price (determine side based on *any* open position for the symbol)
                      positions_for_symbol = open_positions.get(symbol, {})
                      if not positions_for_symbol: continue
                      any_pos_action = next(iter(positions_for_symbol.values())).get('action')
                      price_side = "SELL" if any_pos_action == "BUY" else "BUY"
                      current_price = await get_current_price(symbol, price_side)

                      # Update tracker for all positions of this symbol
                      for pos_id in positions_for_symbol.keys():
                           await self.position_tracker.update_position_price(pos_id, current_price)

                      # Update analysis components (volatility, regime)
                      tf = next(iter(positions_for_symbol.values())).get('timeframe', 'H1') # Use one timeframe
                      if self.volatility_monitor:
                          atr = await get_atr(symbol, tf)
                          await self.volatility_monitor.update_volatility(symbol, atr, tf)
                      if self.regime_classifier:
                          await self.regime_classifier.add_price_data(symbol, current_price, tf)


                 except Exception as inner_e:
                      self.logger.error(f"Failed to update price/analysis for symbol {symbol}: {inner_e}")
            self.logger.debug(f"Finished updating prices for {len(symbols_to_update)} symbols.")
        except Exception as e:
            self.logger.error(f"Error in _update_position_prices: {e}", exc_info=True)

    async def _check_position_exits(self):
        """Placeholder: Check standard SL/TP and TimeBasedExitManager exits."""
        if not self.position_tracker: return
        self.logger.debug("Running scheduled task: Check Standard Exits")
        try:
            open_positions_dict = await self.position_tracker.get_open_positions() # Gets {symbol: {pos_id: data}}
            positions_to_check = [pos_data for positions in open_positions_dict.values() for pos_data in positions.values()]

            exited_ids = set() # Track positions exited in this cycle

            # 1. Check TimeBasedExitManager first
            if self.time_based_exit_manager:
                 time_exits_triggered = self.time_based_exit_manager.check_time_exits() # This should be sync or made async
                 for exit_info in time_exits_triggered:
                      pid = exit_info["position_id"]
                      if pid not in exited_ids: # Avoid double exit attempts
                           pos_data = await self.position_tracker.get_position_info(pid)
                           if pos_data and pos_data.get("status") == "open":
                                price = await get_current_price(pos_data['symbol'], "SELL" if pos_data['action'] == "BUY" else "BUY")
                                reason = f"time_exit_{exit_info['reason']}"
                                success = await self._exit_position(pid, price, reason)
                                if success: exited_ids.add(pid)

            # 2. Check standard SL/TP for remaining open positions
            for pos_data in positions_to_check:
                 pid = pos_data.get("position_id")
                 if not pid or pid in exited_ids or pos_data.get("status") != "open": continue

                 current_price = pos_data.get("current_price")
                 if current_price is None: continue # Cannot check without price

                 # Check Stop Loss
                 if self._check_stop_loss(pos_data, current_price):
                      success = await self._exit_position(pid, pos_data['stop_loss'], "stop_loss_hit") # Exit at SL price
                      if success: exited_ids.add(pid)
                      continue # Move to next position

                 # Check MultiStage Take Profit
                 if self.multi_stage_tp_manager:
                      tp_level_hit = await self.multi_stage_tp_manager.check_take_profit_levels(pid, current_price)
                      if tp_level_hit:
                           await self.multi_stage_tp_manager.execute_take_profit(pid, tp_level_hit) # Handles partial close
                           # Don't add to exited_ids here, as position might still be partially open

                 # Check DynamicExitManager (if separate from SL/TP)
                 # if self.dynamic_exit_manager: await self.dynamic_exit_manager.check_exits(pid, current_price)

            self.logger.debug(f"Finished checking standard exits. Triggered exits for: {exited_ids}")
        except Exception as e:
             self.logger.error(f"Error in _check_position_exits: {e}", exc_info=True)


    async def _perform_daily_reset(self):
        """Placeholder: Perform daily reset tasks."""
        self.logger.info("Running scheduled task: Daily Reset")
        try:
            if self.risk_manager: await self.risk_manager.reset_daily_stats()
            if 'backup_manager' in globals() and backup_manager: # Use global backup_manager if exists
                 await backup_manager.create_backup(compress=True)
            if self.notification_system:
                 await self.notification_system.send_notification("Daily reset completed.", "info")
        except Exception as e:
             self.logger.error(f"Error in _perform_daily_reset: {e}", exc_info=True)


    async def _cleanup_old_positions(self):
        """Placeholder: Clean up old closed positions."""
        self.logger.debug("Running scheduled task: Cleanup Old Positions")
        try:
            if self.position_tracker: await self.position_tracker.purge_old_closed_positions(max_age_days=30)
            if 'backup_manager' in globals() and backup_manager: # Use global backup_manager
                await backup_manager.cleanup_old_backups(max_age_days=60)
        except Exception as e:
            self.logger.error(f"Error in _cleanup_old_positions: {e}", exc_info=True)


    async def _sync_database(self):
        """Placeholder: Sync in-memory data with database."""
        self.logger.debug("Running scheduled task: Sync Database")
        try:
            if self.position_tracker:
                await self.position_tracker.sync_with_database()
                await self.position_tracker.clean_up_duplicate_positions()
        except Exception as e:
            self.logger.error(f"Error in _sync_database: {e}", exc_info=True)

    # Make sure _check_stop_loss is defined correctly
    def _check_stop_loss(self, position: Dict[str, Any], current_price: float) -> bool:
        """Check if stop loss is hit (internal helper)."""
        stop_loss = position.get("stop_loss")
        if stop_loss is None: return False
        action = position.get("action", "").upper()
        if action == "BUY": return current_price <= stop_loss
        elif action == "SELL": return current_price >= stop_loss
        return False

# --- End of EnhancedAlertHandler Class ---

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
    """Process TradingView webhook alerts"""
    request_id = str(uuid.uuid4())
    
    try:
        # Get the raw JSON payload
        payload = await request.json()
        logger.info(f"[{request_id}] Received TradingView webhook: {json.dumps(payload, indent=2)}")
        
        # Process field mappings from TradingView
        # TradingView might use different field names than our schema
        mapped_payload = {}
        
        # Map common TradingView field names to our schema
        field_mappings = {
            "symbol": "instrument",
            "ticker": "instrument",
            "action": "direction",
            "side": "direction",
            "percentage": "risk_percent",
            "risk": "risk_percent",
            "tf": "timeframe",
            "price": "entry_price",
            "sl": "stop_loss",
            "tp": "take_profit"
        }
        
        # Apply mappings
        for tv_field, schema_field in field_mappings.items():
            if tv_field in payload:
                mapped_payload[schema_field] = payload[tv_field]
                
        # Copy any additional fields not in the mapping
        for field, value in payload.items():
            if field not in field_mappings and field not in mapped_payload:
                mapped_payload[field] = value
                
        # Create and validate the alert payload
        try:
            alert_payload = TradingViewAlertPayload(**mapped_payload)
            logger.info(f"[{request_id}] Processed webhook payload: {alert_payload.model_dump()}")
        except Exception as validation_error:
            logger.error(f"[{request_id}] Validation error: {str(validation_error)}")
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "success": False,
                    "error": f"Invalid payload: {str(validation_error)}",
                    "request_id": request_id
                }
            )
        
        # Convert to dict for processing
        alert_data = alert_payload.model_dump()
        alert_data["request_id"] = request_id
        
        # Execute the trade via process_tradingview_alert
        try:
            result = await process_tradingview_alert(alert_data)
            logger.info(f"[{request_id}] Trade execution result: {json.dumps(result, indent=2)}")
            return JSONResponse(content=result)
        except Exception as trade_error:
            logger.error(f"[{request_id}] Trade execution error: {str(trade_error)}", exc_info=True)
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                content={
                    "success": False, 
                    "error": f"Trade execution error: {str(trade_error)}",
                    "request_id": request_id
                }
            )
            
    except json.JSONDecodeError as e:
        logger.error(f"[{request_id}] Invalid JSON in webhook payload: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST, 
            content={
                "success": False, 
                "error": "Invalid JSON payload",
                "request_id": request_id
            }
        )
    except Exception as e:
        logger.error(f"[{request_id}] Error processing TradingView webhook: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            content={
                "success": False, 
                "error": f"Internal server error: {str(e)}",
                "request_id": request_id
            }
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
    
    # Get host and port from config
    host = config.host
    port = config.port
    
    # Start server
    uvicorn.run("main:app", host=host, port=port, reload=False)
