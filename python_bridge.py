import asyncio
import aiohttp
import json
import signal
import time
import uuid
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
from pytz import timezone
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import holidays

# Local/project-specific imports (update paths if needed)
from config import config
from utils.session_manager import get_session
from utils.logger import logger
from utils.decorators import handle_async_errors, handle_sync_errors
from utils.symbols import standardize_symbol, get_instrument_type
from utils.pricing import get_current_price, get_atr
from utils.market_hours import is_instrument_tradeable, get_current_market_session
from risk.risk_manager import EnhancedRiskManager
from risk.dynamic_exit import DynamicExitManager
from risk.loss_manager import LossManager
from risk.analytics import RiskAnalytics
from risk.sizing import PositionSizingManager
from monitoring.volatility import VolatilityMonitor
from structure.market_structure import MarketStructureAnalyzer
from recovery.error_handler import ErrorRecoveryManager
from schemas.alert_schema import AlertData  # Assuming you use Pydantic for validation


# Type variables for type hints
P = ParamSpec("P")
T = TypeVar("T")

# Prometheus metrics
TRADE_REQUESTS = Counter("trade_requests", "Total trade requests")
TRADE_LATENCY = Histogram("trade_latency", "Trade processing latency")

# Redis for shared state
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))


# Configuration management
class Settings(BaseSettings):
    oanda_account: str = Field(alias="OANDA_ACCOUNT_ID")
    oanda_token: str = Field(alias="OANDA_API_TOKEN")
    oanda_api_url: str = Field(
        default="https://api-fxtrade.oanda.com/v3", alias="OANDA_API_URL"
    )
    oanda_environment: str = Field(default="practice", alias="OANDA_ENVIRONMENT")
    allowed_origins: str = "http://localhost"
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    base_position: int = 5000
    max_daily_loss: float = 0.20
    host: str = "0.0.0.0"
    port: int = 8000
    environment: str = "production"
    max_requests_per_minute: int = 100
    trade_24_7: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True


config = Settings()
MAX_DAILY_LOSS = config.max_daily_loss


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record):
        return json.dumps(
            {
                "ts": datetime.utcnow().isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "request_id": getattr(record, "request_id", None),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }
        )


def setup_logging():
    """Setup logging with rotation and proper formatting."""
    try:
        log_dir = "/opt/render/project/src/logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "trading_bot.log")
    except Exception as e:
        log_file = "trading_bot.log"
        logging.warning(f"Using default log file due to error: {str(e)}")
    formatter = JSONFormatter()
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    return logging.getLogger("trading_bot")


logger = setup_logging()


# Alert Data Model
class AlertData(BaseModel):
    symbol: str
    action: str
    timeframe: Optional[str] = "1M"
    orderType: Optional[str] = "MARKET"
    timeInForce: Optional[str] = "FOK"
    percentage: Optional[float] = 15.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None

    @validator("timeframe", pre=True, always=True)
    def validate_timeframe(cls, v):
        if v is None:
            return "15M"
        if not isinstance(v, str):
            v = str(v)
        if v.upper() in ["1D", "D", "DAILY"]:
            return "1440"
        elif v.upper() in ["W", "1W", "WEEKLY"]:
            return "10080"
        elif v.upper() in ["MN", "1MN", "MONTHLY"]:
            return "43200"
        if v.isdigit():
            mapping = {1: "1H", 4: "4H", 12: "12H", 5: "5M", 15: "15M", 30: "30M"}
            try:
                num = int(v)
                v = mapping.get(num, f"{v}M")
            except ValueError as e:
                raise ValueError("Invalid timeframe value") from e
        pattern = re.compile(r"^(\d+)([mMhH])$")
        match = pattern.match(v)
        if not match:
            if v.isdigit():
                return f"{v}M"
            raise ValueError("Invalid timeframe format. Use '15M' or '1H' format")
        value, unit = match.groups()
        value = int(value)
        if unit.upper() == "H":
            if value > 24:
                raise ValueError("Maximum timeframe is 24H")
            return str(value * 60)
        if unit.upper() == "M":
            if value > 1440:
                raise ValueError("Maximum timeframe is 1440M (24H)")
            return str(value)
        raise ValueError("Invalid timeframe format")

    @validator("action")
    def validate_action(cls, v):
        valid_actions = ["BUY", "SELL", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]
        v = v.upper()
        if v not in valid_actions:
            raise ValueError(f"Action must be one of {valid_actions}")
        return v

    @validator("symbol")
    def validate_symbol(cls, v):
        if not v or len(v) < 3:
            raise ValueError("Symbol must be at least 3 characters")
        instrument = standardize_symbol(v)
        is_crypto = any(
            crypto in instrument.upper() for crypto in ["BTC", "ETH", "XRP", "LTC"]
        )
        if is_crypto:
            return v
        if instrument not in INSTRUMENT_LEVERAGES:
            alternate_formats = [
                instrument.replace("_", ""),
                instrument[:3] + "_" + instrument[3:]
                if len(instrument) >= 6
                else instrument,
            ]
            if not any(alt in INSTRUMENT_LEVERAGES for alt in alternate_formats):
                raise ValueError(f"Invalid instrument: {instrument}")
        return v

    @validator("percentage")
    def validate_percentage(cls, v):
        if v is None:
            return 1.0
        if not 0 < v <= 100:
            raise ValueError("Percentage must be between 0 and 100")
        return float(v)

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"


_session: Optional[aiohttp.ClientSession] = None


async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    global _session
    try:
        if _session is None or _session.closed or force_new:
            if _session and not _session.closed:
                await _session.close()
            _session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=config.total_timeout,
                    connect=config.connect_timeout,
                    sock_read=config.read_timeout,
                ),
                headers={
                    "Authorization": f"Bearer {config.oanda_token}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "RFC3339",
                },
            )
        return _session
    except Exception as e:
        logger.error(f"Session creation error: {str(e)}")
        raise


async def cleanup_stale_sessions():
    try:
        if _session and not _session.closed:
            await _session.close()
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")


def standardize_symbol(symbol: str) -> str:
    if not symbol:
        return symbol
    try:
        symbol_upper = symbol.upper().replace("-", "_").replace("/", "_")
        if symbol_upper in ["BTCUSD", "BTCUSD:OANDA", "BTC/USD"]:
            return "BTC_USD"
        elif symbol_upper in ["ETHUSD", "ETHUSD:OANDA", "ETH/USD"]:
            return "ETH_USD"
        elif symbol_upper in ["XRPUSD", "XRPUSD:OANDA", "XRP/USD"]:
            return "XRP_USD"
        elif symbol_upper in ["LTCUSD", "LTCUSD:OANDA", "LTC/USD"]:
            return "LTC_USD"
        if "_" in symbol_upper:
            return symbol_upper
        if len(symbol_upper) == 6:
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"
        for crypto in ["BTC", "ETH", "LTC", "XRP"]:
            if crypto in symbol_upper and "USD" in symbol_upper:
                return f"{crypto}_USD"
        return symbol_upper
    except Exception as e:
        logger.error(f"Error standardizing symbol {symbol}: {str(e)}")
        return symbol


@handle_async_errors
def check_market_hours(session_config: dict) -> bool:
    try:
        if config.trade_24_7:
            return True
        tz = timezone(session_config["timezone"])
        now = datetime.now(tz)
        if session_config["holidays"]:
            holiday_cal = getattr(holidays, session_config["holidays"])()
            if now.date() in holiday_cal:
                return False
        if "24/7" in session_config["hours"]:
            return True
        if "24/5" in session_config["hours"]:
            return now.weekday() < 5
        time_ranges = session_config["hours"].split("|")
        for time_range in time_ranges:
            start_str, end_str = time_range.split("-")
            start = datetime.strptime(start_str, "%H:%M").time()
            end = datetime.strptime(end_str, "%H:%M").time()
            if start <= end:
                if start <= now.time() <= end:
                    return True
            else:
                if now.time() >= start or now.time() <= end:
                    return True
        return False
    except Exception as e:
        logger.error(f"Error checking market hours: {str(e)}")
        raise


def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    try:
        instrument = standardize_symbol(instrument)
        logger.info(f"Checking if {instrument} is tradeable")
        if any(c in instrument for c in ["BTC", "ETH", "XRP", "LTC"]):
            session_type = "CRYPTO"
        elif "XAU" in instrument:
            session_type = "XAU_USD"
        else:
            session_type = "FOREX"
        logger.info(f"Determined session type for {instrument}: {session_type}")
        if session_type not in MARKET_SESSIONS:
            return False, f"Unknown session type for instrument {instrument}"
        market_open = check_market_hours(MARKET_SESSIONS[session_type])
        logger.info(
            f"Market for {instrument} ({session_type}) is {'open' if market_open else 'closed'}"
        )
        if market_open:
            return True, "Market open"
        return False, f"Instrument {instrument} outside market hours"
    except Exception as e:
        logger.error(f"Error checking instrument tradeable status: {str(e)}")
        return False, f"Error checking trading status: {str(e)}"


async def get_atr(instrument: str, timeframe: str) -> float:
    try:
        default_atr_values = {
            "FOREX": {"15M": 0.0010, "1H": 0.0025, "4H": 0.0050, "1D": 0.0100},
            "CRYPTO": {"15M": 0.20, "1H": 0.50, "4H": 1.00, "1D": 2.00},
            "XAU_USD": {"15M": 0.10, "1H": 0.25, "4H": 0.50, "1D": 1.00},
        }
        instrument_type = get_instrument_type(instrument)
        return default_atr_values[instrument_type].get(
            timeframe, default_atr_values[instrument_type]["1H"]
        )
    except Exception as e:
        logger.error(f"Error getting ATR for {instrument}: {str(e)}")
        return 0.0025


def get_instrument_type(symbol: str) -> str:
    normalized_symbol = standardize_symbol(symbol)
    if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return "CRYPTO"
    elif "XAU" in normalized_symbol:
        return "XAU_USD"
    else:
        return "FOREX"


def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    multipliers = {
        "FOREX": {"15M": 1.5, "1H": 1.75, "4H": 2.0, "1D": 2.25},
        "CRYPTO": {"15M": 2.0, "1H": 2.25, "4H": 2.5, "1D": 2.75},
        "XAU_USD": {"15M": 1.75, "1H": 2.0, "4H": 2.25, "1D": 2.5},
    }
    return multipliers[instrument_type].get(
        timeframe, multipliers[instrument_type]["1H"]
    )


@handle_async_errors
async def get_current_price(instrument: str, action: str) -> float:
    try:
        instrument = standardize_symbol(instrument)
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account}/pricing"
        params = {"instruments": instrument}
        async with session.get(
            url, params=params, timeout=HTTP_REQUEST_TIMEOUT
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise ValueError(f"Price fetch failed: {error_text}")
            data = await response.json()
            if not data.get("prices"):
                raise ValueError("No price data received")
            bid = float(data["prices"][0]["bids"][0]["price"])
            ask = float(data["prices"][0]["asks"][0]["price"])
            return ask if action.upper() == "BUY" else bid
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting price for {instrument}")
        raise
    except Exception as e:
        logger.error(f"Error getting price for {instrument}: {str(e)}")
        raise


def get_current_market_session(current_time: datetime) -> str:
    hour = current_time.hour
    weekday = current_time.weekday()
    if weekday >= 5:
        return "WEEKEND"
    elif 0 <= hour < 8:
        return "ASIAN"
    elif 8 <= hour < 12:
        return "ASIAN_EUROPEAN_OVERLAP"
    elif 12 <= hour < 16:
        return "EUROPEAN"
    elif 16 <= hour < 20:
        return "EUROPEAN_AMERICAN_OVERLAP"
    else:
        return "AMERICAN"


# Market Session Configuration and Instrument Leverages
MARKET_SESSIONS = {
    "FOREX": {"hours": "24/5", "timezone": "Asia/Bangkok", "holidays": "US"},
    "XAU_USD": {"hours": "23:00-21:59", "timezone": "UTC", "holidays": []},
    "CRYPTO": {"hours": "24/7", "timezone": "UTC", "holidays": []},
}

INSTRUMENT_LEVERAGES = {
    "USD_CHF": 33.3,
    "EUR_USD": 50,
    "GBP_USD": 20,
    "USD_JPY": 20,
    "AUD_USD": 33.3,
    "USD_THB": 20,
    "CAD_CHF": 33.3,
    "NZD_USD": 33.3,
    "AUD_CAD": 33.3,
    "AUD_JPY": 20,
    "USD_SGD": 20,
    "EUR_JPY": 20,
    "GBP_JPY": 20,
    "USD_CAD": 50,
    "NZD_JPY": 20,
    "BTC_USD": 2,
    "ETH_USD": 2,
    "XRP_USD": 2,
    "LTC_USD": 2,
    "BTCUSD": 2,
    "XAU_USD": 10,
}


# Volatility Monitor
class VolatilityMonitor:
    def __init__(self):
        self.volatility_history = {}
        self.volatility_thresholds = {
            "15M": {"std_dev": 2.0, "lookback": 20},
            "1H": {"std_dev": 2.5, "lookback": 24},
            "4H": {"std_dev": 3.0, "lookback": 30},
            "1D": {"std_dev": 3.5, "lookback": 20},
        }
        self.market_conditions = {}

    async def initialize_market_condition(self, symbol: str, timeframe: str):
        if symbol not in self.market_conditions:
            self.market_conditions[symbol] = {
                "timeframe": timeframe,
                "volatility_state": "normal",
                "last_update": datetime.now(timezone("Asia/Bangkok")),
                "volatility_ratio": 1.0,
            }

    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str):
        if symbol not in self.volatility_history:
            self.volatility_history[symbol] = []
        settings = self.volatility_thresholds.get(
            timeframe, self.volatility_thresholds["1H"]
        )
        self.volatility_history[symbol].append(current_atr)
        if len(self.volatility_history[symbol]) > settings["lookback"]:
            self.volatility_history[symbol].pop(0)
        if len(self.volatility_history[symbol]) >= settings["lookback"]:
            mean_atr = sum(self.volatility_history[symbol]) / len(
                self.volatility_history[symbol]
            )
            std_dev = statistics.stdev(self.volatility_history[symbol])
            current_ratio = current_atr / mean_atr
            self.market_conditions[symbol] = self.market_conditions.get(symbol, {})
            self.market_conditions[symbol]["volatility_ratio"] = current_ratio
            self.market_conditions[symbol]["last_update"] = datetime.now(
                timezone("Asia/Bangkok")
            )
            if current_atr > (mean_atr + settings["std_dev"] * std_dev):
                self.market_conditions[symbol]["volatility_state"] = "high"
            elif current_atr < (mean_atr - settings["std_dev"] * std_dev):
                self.market_conditions[symbol]["volatility_state"] = "low"
            else:
                self.market_conditions[symbol]["volatility_state"] = "normal"

    async def get_market_condition(self, symbol: str) -> Dict[str, Any]:
        return self.market_conditions.get(
            symbol, {"volatility_state": "unknown", "volatility_ratio": 1.0}
        )

    async def should_adjust_risk(
        self, symbol: str, timeframe: str
    ) -> Tuple[bool, float]:
        condition = await self.get_market_condition(symbol)
        if condition["volatility_state"] == "high":
            return True, 0.75
        elif condition["volatility_state"] == "low":
            return True, 1.25
        return False, 1.0


# Lorentzian Distance Classifier
class LorentzianDistanceClassifier:
    def __init__(self, lookback_period: int = 20):
        self.lookback_period = lookback_period
        self.price_history = {}
        self.regime_history = {}
        self.volatility_history = {}
        self.atr_history = {}

    async def calculate_lorentzian_distance(
        self, price: float, history: List[float]
    ) -> float:
        if not history:
            return 0.0
        distances = [np.log(1 + abs(price - hist_price)) for hist_price in history]
        return float(np.mean(distances))

    async def classify_market_regime(
        self, symbol: str, current_price: float, atr: float = None
    ) -> Dict[str, Any]:
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            self.regime_history[symbol] = []
            self.volatility_history[symbol] = []
            self.atr_history[symbol] = []
        self.price_history[symbol].append(current_price)
        if len(self.price_history[symbol]) > self.lookback_period:
            self.price_history[symbol].pop(0)
        if len(self.price_history[symbol]) < 2:
            return {
                "regime": "UNKNOWN",
                "volatility": 0.0,
                "momentum": 0.0,
                "price_distance": 0.0,
            }
        price_distance = await self.calculate_lorentzian_distance(
            current_price, self.price_history[symbol][:-1]
        )
        returns = [
            self.price_history[symbol][i] / self.price_history[symbol][i - 1] - 1
            for i in range(1, len(self.price_history[symbol]))
        ]
        volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
        momentum = (
            (current_price - self.price_history[symbol][0])
            / self.price_history[symbol][0]
            if self.price_history[symbol][0] != 0
            else 0.0
        )
        if atr is not None:
            self.atr_history[symbol].append(atr)
            if len(self.atr_history[symbol]) > self.lookback_period:
                self.atr_history[symbol].pop(0)
        regime = "UNKNOWN"
        if price_distance < 0.1 and volatility < 0.001:
            regime = "RANGING"
        elif price_distance > 0.3 and abs(momentum) > 0.002:
            regime = "TRENDING"
        elif volatility > 0.003 or (
            atr is not None and atr > 1.5 * np.mean(self.atr_history[symbol])
            if self.atr_history[symbol]
            else 0
        ):
            regime = "VOLATILE"
        elif abs(momentum) > 0.003:
            regime = "MOMENTUM"
        else:
            regime = "NEUTRAL"
        self.regime_history[symbol].append(regime)
        self.volatility_history[symbol].append(volatility)
        if len(self.regime_history[symbol]) > self.lookback_period:
            self.regime_history[symbol].pop(0)
            self.volatility_history[symbol].pop(0)
        return {
            "regime": regime,
            "volatility": volatility,
            "momentum": momentum,
            "price_distance": price_distance,
            "is_high_volatility": volatility > 0.002,
            "atr": atr,
        }

    async def get_regime_history(self, symbol: str) -> Dict[str, List[Any]]:
        return {
            "regimes": self.regime_history.get(symbol, []),
            "volatility": self.volatility_history.get(symbol, []),
            "atr": self.atr_history.get(symbol, []),
            "dominant_regime": self.get_dominant_regime(symbol),
        }

    def get_dominant_regime(self, symbol: str) -> str:
        if symbol not in self.regime_history or len(self.regime_history[symbol]) < 3:
            return "UNKNOWN"
        recent_regimes = self.regime_history[symbol][-5:]
        regime_counts = {}
        for regime in recent_regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        dominant_regime = max(regime_counts.items(), key=lambda x: x[1])
        if dominant_regime[1] / len(recent_regimes) >= 0.6:
            return dominant_regime[0]
        else:
            return "MIXED"

    async def should_adjust_exits(
        self, symbol: str, current_regime: str = None
    ) -> Tuple[bool, Dict[str, float]]:
        if current_regime is None:
            if symbol not in self.regime_history or not self.regime_history[symbol]:
                return False, {
                    "stop_loss": 1.0,
                    "take_profit": 1.0,
                    "trailing_stop": 1.0,
                }
            current_regime = self.regime_history[symbol][-1]
        recent_regimes = self.regime_history.get(symbol, [])[-3:]
        is_stable = len(recent_regimes) >= 3 and len(set(recent_regimes)) == 1
        adjustments = {"stop_loss": 1.0, "take_profit": 1.0, "trailing_stop": 1.0}
        if is_stable:
            if current_regime == "VOLATILE":
                adjustments["stop_loss"] = 1.5
                adjustments["take_profit"] = 2.0
                adjustments["trailing_stop"] = 1.25
            elif current_regime == "TRENDING":
                adjustments["stop_loss"] = 1.25
                adjustments["take_profit"] = 1.5
                adjustments["trailing_stop"] = 1.1
            elif current_regime == "RANGING":
                adjustments["stop_loss"] = 0.8
                adjustments["take_profit"] = 0.8
                adjustments["trailing_stop"] = 0.9
            elif current_regime == "MOMENTUM":
                adjustments["stop_loss"] = 1.2
                adjustments["take_profit"] = 1.7
                adjustments["trailing_stop"] = 1.3
        should_adjust = is_stable and any(v != 1.0 for v in adjustments.values())
        return should_adjust, adjustments

    async def clear_history(self, symbol: str):
        if symbol in self.price_history:
            del self.price_history[symbol]
        if symbol in self.regime_history:
            del self.regime_history[symbol]
        if symbol in self.volatility_history:
            del self.volatility_history[symbol]
        if symbol in self.atr_history:
            del self.atr_history[symbol]


# DynamicExitManager (Refactored to use only PositionTracker for state)
class DynamicExitManager:
    def __init__(self, position_tracker: "PositionTracker"):
        self.position_tracker = position_tracker
        self.ldc = LorentzianDistanceClassifier()

    async def initialize_exits(
        self,
        symbol: str,
        entry_price: float,
        position_type: str,
        initial_stop: float,
        initial_tp: float,
    ):
        """
        Initialize exit levels for a position by updating the central PositionTracker.
        """
        position = await self.position_tracker.get_exit_view(symbol)
        if not position:
            logger.warning(
                f"Cannot initialize exits for {symbol} - position not found in tracker"
            )
            return
        await self.position_tracker.update_risk_parameters(
            symbol, initial_stop, [initial_tp]
        )
        logger.info(
            f"Initialized exits for {symbol} with stop {initial_stop} and initial TP {initial_tp}"
        )

    async def update_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """
        Update exit levels based on market regime and current price.
        """
        position = await self.position_tracker.get_exit_view(symbol)
        if not position:
            return {}
        regime_data = await self.ldc.classify_market_regime(symbol, current_price)
        should_adjust, adjustments = await self.ldc.should_adjust_exits(
            symbol, regime_data["regime"]
        )
        if not should_adjust:
            return {}
        new_stop, new_tp, new_trailing = None, None, None
        if position["position_type"] == "LONG":
            new_stop = current_price - (
                abs(current_price - (position["stop_loss"] or current_price))
                * adjustments["stop_loss"]
            )
            new_tp = current_price + (
                abs(
                    (
                        position["take_profits"][0]
                        if position["take_profits"]
                        else current_price
                    )
                    - current_price
                )
                * adjustments["take_profit"]
            )
            if position["trailing_stop"] is None:
                new_trailing = current_price - (
                    abs(current_price - (position["stop_loss"] or current_price))
                    * adjustments["trailing_stop"]
                )
            else:
                new_trailing = max(
                    position["trailing_stop"],
                    current_price
                    - (
                        abs(current_price - (position["stop_loss"] or current_price))
                        * adjustments["trailing_stop"]
                    ),
                )
        else:  # SHORT
            new_stop = current_price + (
                abs((position["stop_loss"] or current_price) - current_price)
                * adjustments["stop_loss"]
            )
            new_tp = current_price - (
                abs(
                    current_price
                    - (
                        position["take_profits"][0]
                        if position["take_profits"]
                        else current_price
                    )
                )
                * adjustments["take_profit"]
            )
            if position["trailing_stop"] is None:
                new_trailing = current_price + (
                    abs((position["stop_loss"] or current_price) - current_price)
                    * adjustments["trailing_stop"]
                )
            else:
                new_trailing = min(
                    position["trailing_stop"],
                    current_price
                    + (
                        abs((position["stop_loss"] or current_price) - current_price)
                        * adjustments["trailing_stop"]
                    ),
                )
        await self.position_tracker.update_risk_parameters(
            symbol, new_stop, position["take_profits"], new_trailing
        )
        return {
            "stop_loss": new_stop,
            "take_profit": new_tp,
            "trailing_stop": new_trailing,
            "regime": regime_data["regime"],
            "volatility": regime_data["volatility"],
        }

    async def clear_exits(self, symbol: str):
        async with self.position_tracker._lock:
            if symbol in self.position_tracker.positions:
                pos = self.position_tracker.positions[symbol]
                pos["stop_loss"] = None
                pos["take_profits"] = []
                pos["trailing_stop"] = None
                pos["exit_levels_hit"] = []
                logger.info(f"Cleared exit levels for {symbol}")


# AdvancedLossManager (Refactored to use PositionTracker only)
class AdvancedLossManager:
    def __init__(self, position_tracker: "PositionTracker"):
        self.position_tracker = position_tracker
        self.daily_pnl = 0.0
        self.max_daily_loss = 0.20
        self.max_drawdown = 0.20
        self.peak_balance = 0.0
        self.current_balance = 0.0
        self.correlation_matrix = {}

    async def initialize_position(
        self,
        symbol: str,
        entry_price: float,
        position_type: str,
        timeframe: str,
        units: float,
        atr: float,
    ) -> bool:
        instrument_type = self.position_tracker._get_instrument_type(symbol)
        atr_multiplier = self._get_atr_multiplier(instrument_type, timeframe)
        if position_type.upper() == "LONG":
            stop_loss = entry_price - (atr * atr_multiplier)
            take_profits = [
                entry_price + (atr * atr_multiplier),
                entry_price + (atr * atr_multiplier * 2),
                entry_price + (atr * atr_multiplier * 3),
            ]
        else:
            stop_loss = entry_price + (atr * atr_multiplier)
            take_profits = [
                entry_price - (atr * atr_multiplier),
                entry_price - (atr * atr_multiplier * 2),
                entry_price - (atr * atr_multiplier * 3),
            ]
        await self.position_tracker.update_risk_parameters(
            symbol, stop_loss, take_profits
        )
        logger.info(
            f"AdvancedLossManager initialized {symbol} with Stop Loss: {stop_loss} and Take Profits: {take_profits}"
        )
        return True

    def _get_atr_multiplier(self, instrument_type: str, timeframe: str) -> float:
        multipliers = {
            "FOREX": {"15M": 1.5, "1H": 1.75, "4H": 2.0, "1D": 2.25},
            "CRYPTO": {"15M": 2.0, "1H": 2.25, "4H": 2.5, "1D": 2.75},
            "XAU_USD": {"15M": 1.75, "1H": 2.0, "4H": 2.25, "1D": 2.5},
        }
        return multipliers[instrument_type].get(
            timeframe, multipliers[instrument_type]["1H"]
        )

    async def update_position_loss(
        self, symbol: str, current_price: float
    ) -> Dict[str, Any]:
        position = await self.position_tracker.get_risk_view(symbol)
        if not position:
            return {}
        entry_price = position["entry_price"]
        units = position["current_units"]
        if position["position_type"] == "LONG":
            current_loss = (entry_price - current_price) * units
        else:
            current_loss = (current_price - entry_price) * units
        async with self.position_tracker._lock:
            self.position_tracker.positions[symbol]["current_loss"] = current_loss
        actions = {}
        if abs(current_loss) > position.get("max_loss", 0):
            actions["position_limit"] = True
        if self.peak_balance > 0:
            daily_loss_percentage = abs(self.daily_pnl) / self.peak_balance
            if daily_loss_percentage > self.max_daily_loss:
                actions["daily_limit"] = True
            drawdown = (self.peak_balance - self.current_balance) / self.peak_balance
            if drawdown > self.max_drawdown:
                actions["drawdown_limit"] = True
        return actions

    async def update_daily_pnl(self, pnl: float):
        self.daily_pnl += pnl
        self.current_balance += pnl
        if self.current_balance > self.peak_balance:
            self.peak_balance = self.current_balance
        logger.info(f"AdvancedLossManager updated daily P&L: {self.daily_pnl}")

    async def should_reduce_risk(self) -> Tuple[bool, float]:
        if self.peak_balance == 0:
            return False, 1.0
        daily_loss_percentage = abs(self.daily_pnl) / self.peak_balance
        drawdown = (self.peak_balance - self.current_balance) / self.peak_balance
        if daily_loss_percentage > self.max_daily_loss * 0.75:
            return True, 0.75
        elif drawdown > self.max_drawdown * 0.75:
            return True, 0.75
        return False, 1.0

    async def clear_position(self, symbol: str):
        logger.info(f"AdvancedLossManager: Cleared risk management data for {symbol}")


# EnhancedRiskManager (Refactored to rely solely on PositionTracker)
class EnhancedRiskManager:
    def __init__(self, position_tracker: "PositionTracker"):
        self.position_tracker = position_tracker
        self.atr_period = 14
        self.take_profit_levels = TIMEFRAME_TAKE_PROFIT_LEVELS
        self.trailing_settings = TIMEFRAME_TRAILING_SETTINGS
        self.time_stops = TIMEFRAME_TIME_STOPS
        self.atr_multipliers = {
            "FOREX": {"15M": 1.5, "1H": 1.75, "4H": 2.0, "1D": 2.25},
            "CRYPTO": {"15M": 2.0, "1H": 2.25, "4H": 2.5, "1D": 2.75},
            "XAU_USD": {"15M": 1.75, "1H": 2.0, "4H": 2.25, "1D": 2.5},
        }

    async def initialize_position(
        self,
        symbol: str,
        entry_price: float,
        position_type: str,
        timeframe: str,
        units: float,
        atr: float,
    ):
        instrument_type = self._get_instrument_type(symbol)
        atr_multiplier = self.atr_multipliers[instrument_type].get(
            timeframe, self.atr_multipliers[instrument_type]["1H"]
        )
        if position_type == "LONG":
            stop_loss = entry_price - (atr * atr_multiplier)
            take_profits = [
                entry_price + (atr * atr_multiplier),
                entry_price + (atr * atr_multiplier * 2),
                entry_price + (atr * atr_multiplier * 3),
            ]
        else:
            stop_loss = entry_price + (atr * atr_multiplier)
            take_profits = [
                entry_price - (atr * atr_multiplier),
                entry_price - (atr * atr_multiplier * 2),
                entry_price - (atr * atr_multiplier * 3),
            ]
        await self.position_tracker.update_risk_parameters(
            symbol, stop_loss, take_profits
        )
        logger.info(
            f"Initialized risk parameters for {symbol}: Stop Loss: {stop_loss}, Take Profits: {take_profits}"
        )

    def _get_instrument_type(self, symbol: str) -> str:
        normalized_symbol = standardize_symbol(symbol)
        if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            return "CRYPTO"
        elif "XAU" in normalized_symbol:
            return "XAU_USD"
        else:
            return "FOREX"

    async def update_position(
        self, symbol: str, current_price: float
    ) -> Dict[str, Any]:
        position = await self.position_tracker.get_risk_view(symbol)
        if not position:
            return {}
        actions = {}
        if self._check_stop_loss_hit(position, current_price):
            actions["stop_loss"] = True
            return actions
        tp_actions = self._check_take_profits(position, current_price)
        if tp_actions:
            actions["take_profits"] = tp_actions
        trailing_action = self._update_trailing_stop(position, current_price)
        if trailing_action:
            actions["trailing_stop"] = trailing_action
            await self.position_tracker.update_risk_parameters(
                symbol,
                position["stop_loss"],
                position["take_profits"],
                position["trailing_stop"],
            )
        time_action = self._check_time_adjustments(position)
        if time_action:
            actions["time_adjustment"] = time_action
        return actions

    def _check_stop_loss_hit(
        self, position: Dict[str, Any], current_price: float
    ) -> bool:
        if not position.get("stop_loss"):
            return False
        if position["position_type"] == "LONG":
            return current_price <= position["stop_loss"]
        else:
            return current_price >= position["stop_loss"]

    def _check_take_profits(
        self, position: Dict[str, Any], current_price: float
    ) -> Optional[Dict[str, Any]]:
        actions = {}
        if not position.get("take_profits") or not position.get("exit_levels_hit"):
            return None
        timeframe = position.get("timeframe", "1H")
        tp_levels = self.take_profit_levels.get(
            timeframe, self.take_profit_levels["1H"]
        )
        for i, tp in enumerate(position["take_profits"]):
            if i not in position["exit_levels_hit"]:
                if position["position_type"] == "LONG" and current_price >= tp:
                    tp_key = (
                        "first_exit"
                        if i == 0
                        else "second_exit"
                        if i == 1
                        else "runner"
                    )
                    actions[i] = {"price": tp, "percentage": tp_levels[tp_key] * 100}
                elif position["position_type"] == "SHORT" and current_price <= tp:
                    tp_key = (
                        "first_exit"
                        if i == 0
                        else "second_exit"
                        if i == 1
                        else "runner"
                    )
                    actions[i] = {"price": tp, "percentage": tp_levels[tp_key] * 100}
        return actions if actions else None

    def _update_trailing_stop(
        self, position: Dict[str, Any], current_price: float
    ) -> Optional[Dict[str, Any]]:
        if not position.get("exit_levels_hit"):
            return None
        timeframe = position.get("timeframe", "1H")
        settings = self.trailing_settings.get(timeframe, self.trailing_settings["1H"])
        current_multiplier = settings["initial_multiplier"]
        for level in settings["profit_levels"]:
            if (
                self._get_current_rr_ratio(position, current_price)
                >= level["threshold"]
            ):
                current_multiplier = level["multiplier"]
        atr = position.get("atr", 0.001)
        if position["position_type"] == "LONG":
            new_stop = current_price - (atr * current_multiplier)
            if (
                position.get("trailing_stop") is None
                or new_stop > position["trailing_stop"]
            ):
                return {"new_stop": new_stop}
        else:
            new_stop = current_price + (atr * current_multiplier)
            if (
                position.get("trailing_stop") is None
                or new_stop < position["trailing_stop"]
            ):
                return {"new_stop": new_stop}
        return None

    def _get_current_rr_ratio(
        self, position: Dict[str, Any], current_price: float
    ) -> float:
        risk = abs(
            position["entry_price"] - position.get("stop_loss", position["entry_price"])
        )
        if risk == 0:
            risk = position.get("atr", 0.001)
        reward = (
            current_price - position["entry_price"]
            if position["position_type"] == "LONG"
            else position["entry_price"] - current_price
        )
        return reward / risk

    def _check_time_adjustments(
        self, position: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        entry_time = position.get("entry_time")
        if not entry_time:
            return None
        timeframe = position.get("timeframe", "1H")
        settings = self.time_stops.get(timeframe, self.time_stops["1H"])
        current_duration = (
            datetime.now(timezone("Asia/Bangkok")) - entry_time
        ).total_seconds() / 3600
        if current_duration > settings["max_duration"]:
            return {"action": "tighten_stop", "multiplier": settings["stop_adjustment"]}
        return None


# TradingConfig remains as before
class TradingConfig:
    def __init__(self):
        self.atr_multipliers = {
            "FOREX": {"15M": 1.5, "1H": 1.75, "4H": 2.0, "1D": 2.25},
            "CRYPTO": {"15M": 2.0, "1H": 2.25, "4H": 2.5, "1D": 2.75},
            "XAU_USD": {"15M": 1.75, "1H": 2.0, "4H": 2.25, "1D": 2.5},
        }
        self.take_profit_levels = {
            "15M": {"first_exit": 0.5, "second_exit": 0.25, "runner": 0.25},
            "1H": {"first_exit": 0.4, "second_exit": 0.3, "runner": 0.3},
            "4H": {"first_exit": 0.33, "second_exit": 0.33, "runner": 0.34},
            "1D": {"first_exit": 0.33, "second_exit": 0.33, "runner": 0.34},
        }
        self.market_conditions = {
            "volatility_adjustments": {"high": 0.75, "low": 1.25, "normal": 1.0}
        }

    def update_atr_multipliers(
        self, instrument: str, timeframe: str, new_multiplier: float
    ):
        instrument_type = "FOREX"
        if any(crypto in instrument for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            instrument_type = "CRYPTO"
        elif "XAU" in instrument:
            instrument_type = "XAU_USD"
        if (
            instrument_type in self.atr_multipliers
            and timeframe in self.atr_multipliers[instrument_type]
        ):
            if 1.0 <= new_multiplier <= 4.0:
                self.atr_multipliers[instrument_type][timeframe] = new_multiplier
            else:
                logger.warning(
                    f"Invalid ATR multiplier: {new_multiplier}. Must be between 1.0 and 4.0."
                )

    def update_take_profit_levels(self, timeframe: str, new_levels: Dict[str, float]):
        if timeframe in self.take_profit_levels:
            total = sum(new_levels.values())
            if abs(total - 1.0) < 0.01:
                self.take_profit_levels[timeframe] = new_levels
            else:
                logger.warning(
                    f"Invalid take-profit levels for {timeframe}. Sum must be 1.0."
                )


async def get_account_balance(account_id: str) -> float:
    try:
        session = await get_session()
        async with session.get(
            f"{config.oanda_api_url}/accounts/{account_id}/summary"
        ) as resp:
            data = await resp.json()
            return float(data["account"]["balance"])
    except Exception as e:
        logger.error(f"Error fetching account balance: {str(e)}")
        raise


async def get_account_summary() -> Tuple[bool, Dict[str, Any]]:
    try:
        session = await get_session()
        async with session.get(
            f"{config.oanda_api_url}/accounts/{config.oanda_account}/summary"
        ) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                logger.error(f"Account summary fetch failed: {error_text}")
                return False, {"error": error_text}
            data = await resp.json()
            return True, data.get("account", {})
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        return False, {"error": str(e)}


async def calculate_trade_size(
    instrument: str, risk_percentage: float, balance: float
) -> Tuple[float, int]:
    if risk_percentage <= 0 or risk_percentage > 100:
        raise ValueError("Invalid percentage value")
    normalized_instrument = standardize_symbol(instrument)
    CRYPTO_MIN_SIZES = {
        "BTC": 0.0001,
        "ETH": 0.002,
        "LTC": 0.05,
        "BCH": 0.02,
        "PAXG": 0.002,
        "LINK": 0.4,
        "UNI": 0.6,
        "AAVE": 0.04,
    }
    CRYPTO_MAX_SIZES = {
        "BTC": 10,
        "ETH": 135,
        "LTC": 3759,
        "BCH": 1342,
        "PAXG": 211,
        "LINK": 33277,
        "UNI": 51480,
        "AAVE": 2577,
    }
    CRYPTO_TICK_SIZES = {
        "BTC": 0.25,
        "ETH": 0.05,
        "LTC": 0.01,
        "BCH": 0.05,
        "PAXG": 0.01,
        "LINK": 0.01,
        "UNI": 0.01,
        "AAVE": 0.01,
    }
    try:
        equity_percentage = risk_percentage / 100
        equity_amount = balance * equity_percentage
        leverage = INSTRUMENT_LEVERAGES.get(normalized_instrument, 20)
        position_value = equity_amount * leverage
        crypto_symbol = next(
            (sym for sym in CRYPTO_MIN_SIZES.keys() if sym in normalized_instrument),
            None,
        )
        if "XAU" in normalized_instrument:
            precision = 2
            min_size = 0.2
            tick_size = 0.01
            price = await get_current_price(normalized_instrument, "BUY")
            trade_size = position_value / price
            max_size = float("inf")
        elif crypto_symbol:
            tick_size = CRYPTO_TICK_SIZES.get(crypto_symbol, 0.01)
            precision = (
                len(str(tick_size).split(".")[-1]) if "." in str(tick_size) else 0
            )
            min_size = CRYPTO_MIN_SIZES.get(crypto_symbol, 0.0001)
            max_size = CRYPTO_MAX_SIZES.get(crypto_symbol, float("inf"))
            price = await get_current_price(normalized_instrument, "BUY")
            trade_size = position_value / price
        else:
            precision = 0
            min_size = 1200
            max_size = float("inf")
            tick_size = 1
            trade_size = position_value
        trade_size = max(min_size, min(max_size, trade_size))
        if tick_size > 0:
            trade_size = round(trade_size / tick_size) * tick_size
            trade_size = (
                round(trade_size, precision)
                if precision > 0
                else int(round(trade_size))
            )
        logger.info(
            f"Using {risk_percentage}% of equity with {leverage}:1 leverage. Calculated trade size: {trade_size} for {normalized_instrument} (original: {instrument}), equity: ${balance}, min_size: {min_size}, max_size: {max_size}, tick_size: {tick_size}"
        )
        return trade_size, precision
    except Exception as e:
        logger.error(f"Error calculating trade size: {str(e)}")
        raise


@handle_async_errors
async def get_open_positions(account_id: str = None) -> Tuple[bool, Dict[str, Any]]:
    try:
        if account_id is None:
            account_id = config.oanda_account
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/positions"
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to get positions: {error_text}")
                return False, {"error": error_text}
            data = await response.json()
            return True, data
    except Exception as e:
        logger.error(f"Error fetching open positions: {str(e)}")
        return False, {"error": str(e)}


@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    request_id = str(uuid.uuid4())
    instrument = standardize_symbol(alert_data["symbol"])
    logger.info(
        f"[{request_id}] Executing trade for {instrument} - Action: {alert_data['action']}"
    )
    try:
        balance = await get_account_balance(
            alert_data.get("account", config.oanda_account)
        )
        units, precision = await calculate_trade_size(
            instrument, alert_data["percentage"], balance
        )
        if alert_data["action"].upper() == "SELL":
            units = -abs(units)
        current_price = await get_current_price(instrument, alert_data["action"])
        atr = await get_atr(instrument, alert_data["timeframe"])
        instrument_type = get_instrument_type(instrument)
        atr_multiplier = get_atr_multiplier(instrument_type, alert_data["timeframe"])
        price_precision = 3 if "JPY" in instrument else 5
        if alert_data["action"].upper() == "BUY":
            stop_loss = round(current_price - (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price + (atr * atr_multiplier), price_precision),
                round(current_price + (atr * atr_multiplier * 2), price_precision),
                round(current_price + (atr * atr_multiplier * 3), price_precision),
            ]
        else:
            stop_loss = round(current_price + (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price - (atr * atr_multiplier), price_precision),
                round(current_price - (atr * atr_multiplier * 2), price_precision),
                round(current_price - (atr * atr_multiplier * 3), price_precision),
            ]
        order_data = {
            "order": {
                "type": alert_data["orderType"],
                "instrument": instrument,
                "units": str(units),
                "timeInForce": alert_data["timeInForce"],
                "positionFill": "DEFAULT",
                "stopLossOnFill": {
                    "price": str(stop_loss),
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK",
                },
                "takeProfitOnFill": {
                    "price": str(take_profits[0]),
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK",
                },
            }
        }
        if alert_data.get("use_trailing_stop", True):
            trailing_distance = round(atr * atr_multiplier, price_precision)
            order_data["order"]["trailingStopLossOnFill"] = {
                "distance": str(trailing_distance),
                "timeInForce": "GTC",
                "triggerMode": "TOP_OF_BOOK",
            }
        logger.info(f"[{request_id}] Order data: {json.dumps(order_data)}")
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(
                    url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT
                ) as response:
                    response_text = await response.text()
                    logger.info(
                        f"[{request_id}] Response status: {response.status}, Response: {response_text}"
                    )
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(
                            f"[{request_id}] Trade executed successfully with stops: {result}"
                        )
                        return True, result
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(
                            f"[{request_id}] OANDA error: {error_code} - {error_message}"
                        )
                    except:
                        pass
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)
                    elif "MARKET_HALTED" in response_text:
                        return False, {"error": "Market is halted"}
                    else:
                        delay = config.base_delay * (2**retries)
                        await asyncio.sleep(delay)
                    logger.warning(
                        f"[{request_id}] Retry {retries + 1}/{config.max_retries}"
                    )
                    retries += 1
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2**retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        return False, {"error": "Maximum retries exceeded"}
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}")
        return False, {"error": str(e)}


@handle_async_errors
async def close_position(
    alert_data: Dict[str, Any], position_tracker=None
) -> Tuple[bool, Dict[str, Any]]:
    request_id = str(uuid.uuid4())
    try:
        instrument = standardize_symbol(alert_data["symbol"])
        account_id = alert_data.get("account", config.oanda_account)
        success, position_data = await get_open_positions(account_id)
        if not success:
            return False, position_data
        position = next(
            (
                p
                for p in position_data.get("positions", [])
                if p["instrument"] == instrument
            ),
            None,
        )
        if not position:
            logger.warning(f"[{request_id}] No position found for {instrument}")
            return False, {"error": f"No open position for {instrument}"}
        long_units = float(position["long"].get("units", "0"))
        short_units = float(position["short"].get("units", "0"))
        close_data = {
            "longUnits": "ALL" if long_units > 0 else "NONE",
            "shortUnits": "ALL" if short_units < 0 else "NONE",
        }
        session = await get_session()
        url = (
            f"{config.oanda_api_url}/accounts/{account_id}/positions/{instrument}/close"
        )
        async with session.put(
            url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT
        ) as response:
            result = await response.json()
            if response.status == 200:
                logger.info(f"[{request_id}] Position closed successfully: {result}")
                pnl = 0.0
                try:
                    if (
                        "longOrderFillTransaction" in result
                        and result["longOrderFillTransaction"]
                    ):
                        pnl += float(result["longOrderFillTransaction"].get("pl", 0))
                    if (
                        "shortOrderFillTransaction" in result
                        and result["shortOrderFillTransaction"]
                    ):
                        pnl += float(result["shortOrderFillTransaction"].get("pl", 0))
                    logger.info(f"[{request_id}] Position P&L: {pnl}")
                    if position_tracker and pnl != 0:
                        await position_tracker.record_trade_pnl(pnl)
                except Exception as e:
                    logger.error(f"[{request_id}] Error calculating P&L: {str(e)}")
                return True, result
            else:
                logger.error(f"[{request_id}] Failed to close position: {result}")
                return False, result
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}")
        return False, {"error": str(e)}


@handle_async_errors
async def close_partial_position(
    alert_data: Dict[str, Any], percentage: float, position_tracker=None
) -> Tuple[bool, Dict[str, Any]]:
    request_id = str(uuid.uuid4())
    try:
        instrument = standardize_symbol(alert_data["symbol"])
        account_id = alert_data.get("account", config.oanda_account)
        success, position_data = await get_open_positions(account_id)
        if not success:
            return False, position_data
        position = next(
            (
                p
                for p in position_data.get("positions", [])
                if p["instrument"] == instrument
            ),
            None,
        )
        if not position:
            logger.warning(f"[{request_id}] No position found for {instrument}")
            return False, {"error": f"No open position for {instrument}"}
        long_units = float(position["long"].get("units", "0"))
        short_units = float(position["short"].get("units", "0"))
        if long_units > 0:
            units_to_close = int(long_units * percentage / 100)
            close_data = {"longUnits": str(units_to_close)}
        elif short_units < 0:
            units_to_close = int(abs(short_units) * percentage / 100)
            close_data = {"shortUnits": str(units_to_close)}
        else:
            return False, {"error": "No units to close"}
        session = await get_session()
        url = (
            f"{config.oanda_api_url}/accounts/{account_id}/positions/{instrument}/close"
        )
        async with session.put(
            url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT
        ) as response:
            result = await response.json()
            if response.status == 200:
                logger.info(
                    f"[{request_id}] Position partially closed successfully: {result}"
                )
                pnl = 0.0
                try:
                    if (
                        "longOrderFillTransaction" in result
                        and result["longOrderFillTransaction"]
                    ):
                        pnl += float(result["longOrderFillTransaction"].get("pl", 0))
                    if (
                        "shortOrderFillTransaction" in result
                        and result["shortOrderFillTransaction"]
                    ):
                        pnl += float(result["shortOrderFillTransaction"].get("pl", 0))
                    logger.info(f"[{request_id}] Partial position P&L: {pnl}")
                    if position_tracker and pnl != 0:
                        await position_tracker.record_trade_pnl(pnl)
                except Exception as e:
                    logger.error(f"[{request_id}] Error calculating P&L: {str(e)}")
                return True, result
            else:
                logger.error(
                    f"[{request_id}] Failed to close partial position: {result}"
                )
                return False, result
    except Exception as e:
        logger.error(f"[{request_id}] Error closing partial position: {str(e)}")
        return False, {"error": str(e)}


class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._initialized = False
        self.daily_pnl = 0.0
        self.pnl_reset_date = datetime.now().date()
        self._price_monitor_task = None
        self.bar_times: Dict[str, List[datetime]] = {}

    async def record_position(
        self,
        symbol: str,
        action: str,
        timeframe: str,
        entry_price: float,
        units: float,
        account_balance: float,
        atr: float,
    ) -> bool:
        try:
            async with self._lock:
                position_type = "LONG" if action.upper() == "BUY" else "SHORT"
                current_time = datetime.now(timezone("Asia/Bangkok"))
                instrument_type = self._get_instrument_type(symbol)
                position_data = {
                    "entry_time": current_time,
                    "position_type": position_type,
                    "timeframe": timeframe,
                    "entry_price": entry_price,
                    "last_update": current_time,
                    "bars_held": 0,
                    "units": units,
                    "current_units": units,
                    "atr": atr,
                    "instrument_type": instrument_type,
                    "stop_loss": None,
                    "take_profits": [],
                    "trailing_stop": None,
                    "exit_levels_hit": [],
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "current_price": entry_price,
                    "market_regime": "UNKNOWN",
                    "volatility_state": "normal",
                    "volatility_ratio": 1.0,
                    "max_loss": self._calculate_position_max_loss(
                        entry_price, units, account_balance
                    ),
                    "current_loss": 0.0,
                    "correlation_factor": 1.0,
                    "nearest_support": None,
                    "nearest_resistance": None,
                }
                self.positions[symbol] = position_data
                self.bar_times.setdefault(symbol, []).append(current_time)
                logger.info(
                    f"Recorded comprehensive position for {symbol}: {position_data}"
                )
                return True
        except Exception as e:
            logger.error(f"Error recording position for {symbol}: {str(e)}")
            return False

    def _calculate_position_max_loss(
        self, entry_price: float, units: float, account_balance: float
    ) -> float:
        position_value = abs(entry_price * units)
        risk_percentage = min(0.02, position_value / account_balance)
        return position_value * risk_percentage

    def _get_instrument_type(self, symbol: str) -> str:
        normalized_symbol = standardize_symbol(symbol)
        if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            return "CRYPTO"
        elif "XAU" in normalized_symbol:
            return "XAU_USD"
        else:
            return "FOREX"

    async def update_position_price(
        self, symbol: str, current_price: float
    ) -> Dict[str, Any]:
        changes = {}
        async with self._lock:
            if symbol not in self.positions:
                return changes
            position = self.positions[symbol]
            old_price = position.get("current_price")
            position["current_price"] = current_price
            position["last_update"] = datetime.now(timezone("Asia/Bangkok"))
            entry_price = position["entry_price"]
            units = position["current_units"]
            if position["position_type"] == "LONG":
                unrealized_pnl = (current_price - entry_price) * units
            else:
                unrealized_pnl = (entry_price - current_price) * units
            position["unrealized_pnl"] = unrealized_pnl
            if position["position_type"] == "LONG":
                current_loss = max(0, (entry_price - current_price) * units)
            else:
                current_loss = max(0, (current_price - entry_price) * units)
            position["current_loss"] = current_loss
            changes = {
                "price_changed": old_price != current_price,
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "current_loss": current_loss,
            }
            return changes

    async def update_risk_parameters(
        self,
        symbol: str,
        stop_loss: float,
        take_profits: List[float],
        trailing_stop: Optional[float] = None,
    ) -> bool:
        async with self._lock:
            if symbol not in self.positions:
                return False
            position = self.positions[symbol]
            position["stop_loss"] = stop_loss
            position["take_profits"] = take_profits
            if trailing_stop is not None:
                position["trailing_stop"] = trailing_stop
            return True

    async def update_market_condition(
        self, symbol: str, regime: str, volatility_state: str, volatility_ratio: float
    ) -> bool:
        async with self._lock:
            if symbol not in self.positions:
                return False
            position = self.positions[symbol]
            position["market_regime"] = regime
            position["volatility_state"] = volatility_state
            position["volatility_ratio"] = volatility_ratio
            return True

    async def update_support_resistance(
        self, symbol: str, support: Optional[float], resistance: Optional[float]
    ) -> bool:
        async with self._lock:
            if symbol not in self.positions:
                return False
            position = self.positions[symbol]
            position["nearest_support"] = support
            position["nearest_resistance"] = resistance
            return True

    async def record_exit_level_hit(self, symbol: str, level_index: int) -> bool:
        async with self._lock:
            if symbol not in self.positions:
                return False
            position = self.positions[symbol]
            if level_index not in position["exit_levels_hit"]:
                position["exit_levels_hit"].append(level_index)
            return True

    async def update_partial_close(
        self, symbol: str, closed_units: float, realized_pnl: float
    ) -> bool:
        async with self._lock:
            if symbol not in self.positions:
                return False
            position = self.positions[symbol]
            position["current_units"] -= closed_units
            position["realized_pnl"] += realized_pnl
            return True

    async def get_risk_view(self, symbol: str) -> Dict[str, Any]:
        async with self._lock:
            if symbol not in self.positions:
                return {}
            position = self.positions[symbol]
            return {
                "entry_price": position["entry_price"],
                "position_type": position["position_type"],
                "units": position["units"],
                "current_units": position["current_units"],
                "stop_loss": position["stop_loss"],
                "take_profits": position["take_profits"],
                "trailing_stop": position["trailing_stop"],
                "exit_levels_hit": position["exit_levels_hit"],
                "atr": position["atr"],
                "timeframe": position["timeframe"],
                "current_price": position["current_price"],
                "instrument_type": position["instrument_type"],
                "max_loss": position["max_loss"],
                "current_loss": position["current_loss"],
            }

    async def get_exit_view(self, symbol: str) -> Dict[str, Any]:
        async with self._lock:
            if symbol not in self.positions:
                return {}
            position = self.positions[symbol]
            return {
                "entry_price": position["entry_price"],
                "position_type": position["position_type"],
                "current_price": position["current_price"],
                "stop_loss": position["stop_loss"],
                "take_profits": position["take_profits"],
                "trailing_stop": position["trailing_stop"],
                "exit_levels_hit": position["exit_levels_hit"],
                "market_regime": position["market_regime"],
                "volatility_state": position["volatility_state"],
                "nearest_support": position["nearest_support"],
                "nearest_resistance": position["nearest_resistance"],
            }

    async def get_analytics_view(self, symbol: str) -> Dict[str, Any]:
        async with self._lock:
            if symbol not in self.positions:
                return {}
            position = self.positions[symbol]
            entry_time = position["entry_time"]
            now = datetime.now(timezone("Asia/Bangkok"))
            duration = (now - entry_time).total_seconds() / 3600
            return {
                "entry_price": position["entry_price"],
                "current_price": position["current_price"],
                "units": position["units"],
                "current_units": position["current_units"],
                "entry_time": entry_time,
                "duration_hours": round(duration, 2),
                "unrealized_pnl": position["unrealized_pnl"],
                "realized_pnl": position["realized_pnl"],
                "market_regime": position["market_regime"],
                "volatility_state": position["volatility_state"],
            }

    async def start(self):
        if not self._initialized:
            async with self._lock:
                if not self._initialized:
                    self._running = True
                    self.reconciliation_task = asyncio.create_task(
                        self.reconcile_positions()
                    )
                    self._initialized = True
                    logger.info("Position tracker started")

    async def stop(self):
        self._running = False
        if hasattr(self, "reconciliation_task"):
            self.reconciliation_task.cancel()
            try:
                await self.reconciliation_task
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
            except Exception as e:
                logger.error(f"Error stopping reconciliation task: {str(e)}")
        logger.info("Position tracker stopped")

    async def reconcile_positions(self):
        while self._running:
            try:
                await asyncio.sleep(900)
                logger.info("Starting position reconciliation")
                async with self._lock:
                    async with asyncio.timeout(60):
                        success, positions_data = await get_open_positions()
                    if not success:
                        logger.error("Failed to fetch positions for reconciliation")
                        continue
                    oanda_positions = {
                        p["instrument"] for p in positions_data.get("positions", [])
                    }
                    for symbol in list(self.positions.keys()):
                        try:
                            if symbol not in oanda_positions:
                                old_data = self.positions.pop(symbol, None)
                                logger.warning(
                                    f"Removing stale position for {symbol}. Old data: {old_data}"
                                )
                        except Exception as e:
                            logger.error(
                                f"Error reconciling position for {symbol}: {str(e)}"
                            )
                    logger.info(
                        f"Reconciliation complete. Active positions: {list(self.positions.keys())}"
                    )
            except asyncio.TimeoutError:
                logger.error(
                    "Position reconciliation timed out, will retry in next cycle"
                )
                continue
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}")
                await asyncio.sleep(60)

    @handle_async_errors
    async def record_position(
        self,
        symbol: str,
        action: str,
        timeframe: str,
        entry_price: float,
        units: float,
        account_balance: float,
        atr: float,
    ) -> bool:
        try:
            async with self._lock:
                position_type = "LONG" if action.upper() == "BUY" else "SHORT"
                current_time = datetime.now(timezone("Asia/Bangkok"))
                instrument_type = self._get_instrument_type(symbol)
                position_data = {
                    "entry_time": current_time,
                    "position_type": position_type,
                    "timeframe": timeframe,
                    "entry_price": entry_price,
                    "last_update": current_time,
                    "bars_held": 0,
                    "units": units,
                    "current_units": units,
                    "atr": atr,
                    "instrument_type": instrument_type,
                    "stop_loss": None,
                    "take_profits": [],
                    "trailing_stop": None,
                    "exit_levels_hit": [],
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "current_price": entry_price,
                    "market_regime": "UNKNOWN",
                    "volatility_state": "normal",
                    "volatility_ratio": 1.0,
                    "max_loss": self._calculate_position_max_loss(
                        entry_price, units, account_balance
                    ),
                    "current_loss": 0.0,
                    "correlation_factor": 1.0,
                    "nearest_support": None,
                    "nearest_resistance": None,
                }
                self.positions[symbol] = position_data
                self.bar_times.setdefault(symbol, []).append(current_time)
                logger.info(
                    f"Recorded comprehensive position for {symbol}: {position_data}"
                )
                return True
        except Exception as e:
            logger.error(f"Error recording position for {symbol}: {str(e)}")
            return False

    @handle_async_errors
    async def clear_position(self, symbol: str) -> bool:
        try:
            async with self._lock:
                if symbol in self.positions:
                    position_data = self.positions.pop(symbol)
                    self.bar_times.pop(symbol, None)
                    logger.info(f"Cleared position for {symbol}: {position_data}")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error clearing position for {symbol}: {str(e)}")
            return False

    async def get_position_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            return self.positions.get(symbol)

    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return self.positions.copy()

    @handle_async_errors
    async def record_trade_pnl(self, pnl: float) -> None:
        async with self._lock:
            current_date = datetime.now().date()
            if current_date != self.pnl_reset_date:
                logger.info(
                    f"Resetting daily P&L (was {self.daily_pnl}) for new day: {current_date}"
                )
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            self.daily_pnl += pnl
            logger.info(f"Updated daily P&L: {self.daily_pnl}")

    async def get_daily_pnl(self) -> float:
        async with self._lock:
            current_date = datetime.now().date()
            if current_date != self.pnl_reset_date:
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            return self.daily_pnl

    async def check_max_daily_loss(self, account_balance: float) -> Tuple[bool, float]:
        daily_pnl = await self.get_daily_pnl()
        loss_percentage = abs(min(0, daily_pnl)) / account_balance
        if loss_percentage > MAX_DAILY_LOSS * 0.5:
            logger.warning(
                f"Daily loss at {loss_percentage:.2%} of account (reference limit: {MAX_DAILY_LOSS:.2%})"
            )
        return True, loss_percentage

    async def update_position_exits(self, symbol: str, current_price: float) -> bool:
        try:
            async with self._lock:
                if symbol not in self.positions:
                    return False
                position = self.positions[symbol]
                # This is a placeholder for integration with risk manager adjustments.
                return False
        except Exception as e:
            logger.error(f"Error updating position exits for {symbol}: {str(e)}")
            return False

    async def get_position_entry_price(self, symbol: str) -> Optional[float]:
        async with self._lock:
            position = self.positions.get(symbol)
            return position.get("entry_price") if position else None

    async def get_position_type(self, symbol: str) -> Optional[str]:
        async with self._lock:
            position = self.positions.get(symbol)
            return position.get("position_type") if position else None

    async def get_position_timeframe(self, symbol: str) -> Optional[str]:
        async with self._lock:
            position = self.positions.get(symbol)
            return position.get("timeframe") if position else None

    async def get_position_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            position = self.positions.get(symbol)
            if not position:
                return None
            return {
                "entry_price": position.get("entry_price"),
                "position_type": position.get("position_type"),
                "timeframe": position.get("timeframe"),
                "entry_time": position.get("entry_time"),
                "bars_held": position.get("bars_held", 0),
                "last_update": position.get("last_update"),
                "daily_pnl": self.daily_pnl,
            }


# Alert Handler
class AlertHandler:
    def __init__(self):
        self.position_tracker = PositionTracker()
        self.risk_manager = EnhancedRiskManager(self.position_tracker)
        self.volatility_monitor = VolatilityMonitor()
        self.market_structure = MarketStructureAnalyzer()
        self.position_sizing = PositionSizingManager()
        self.config = TradingConfig()
        # For backward compatibility, other managers (loss_manager, risk_analytics, dynamic_exit_manager)
        # can be set here if needed. For now, we assume all use the centralized tracker.
        self._lock = asyncio.Lock()
        self._initialized = False
        self._price_monitor_task = None
        self._running = False
        # Assume error_recovery is also used
        self.error_recovery = ErrorRecoverySystem()

    async def start(self):
        if not self._initialized:
            async with self._lock:
                if not self._initialized:
                    await self.position_tracker.start()
                    self._initialized = True
                    self._running = True
                    self._price_monitor_task = asyncio.create_task(
                        self._monitor_positions()
                    )
                    logger.info("Alert handler initialized with price monitoring")

    async def stop(self):
        try:
            self._running = False
            if self._price_monitor_task:
                self._price_monitor_task.cancel()
                try:
                    await self._price_monitor_task
                except asyncio.CancelledError:
                    logger.info("Price monitoring task cancelled")
                except Exception as e:
                    logger.error(f"Error cancelling position monitoring: {str(e)}")
            await self.position_tracker.stop()
            logger.info("Alert handler stopped")
        except Exception as e:
            logger.error(f"Error stopping alert handler: {str(e)}")

    async def _monitor_positions(self):
        # Using the consolidated tracker and risk manager for adjustments.
        while self._running:
            try:
                positions = await self.position_tracker.get_all_positions()
                for symbol, position in positions.items():
                    try:
                        current_price = await get_current_price(
                            symbol, position["position_type"]
                        )
                        changes = await self.position_tracker.update_position_price(
                            symbol, current_price
                        )
                        # For market regime, use the Lorentzian classifier from risk_manager
                        regime_data = await self.risk_manager._get_current_rr_ratio(
                            position, current_price
                        )  # or similar call
                        # (If needed, call volatility monitor and update market condition accordingly)
                        await self.position_tracker.update_market_condition(
                            symbol, "UNKNOWN", "normal", 1.0
                        )
                        actions = await self.risk_manager.update_position(
                            symbol, current_price
                        )
                        if actions:
                            await self._handle_position_actions(
                                symbol, actions, current_price
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.error(f"Error monitoring position {symbol}: {str(e)}")
                        continue
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                logger.info("Position monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Error in position monitoring: {str(e)}")
                await asyncio.sleep(60)

    async def _handle_position_actions(
        self, symbol: str, actions: Dict[str, Any], current_price: float
    ):
        request_id = str(uuid.uuid4())
        try:
            if (
                actions.get("stop_loss")
                or actions.get("position_limit")
                or actions.get("daily_limit")
                or actions.get("drawdown_limit")
            ):
                logger.info(
                    f"Stop loss or risk limit hit for {symbol} at {current_price}"
                )
                await self._close_position(symbol)
            if "take_profits" in actions:
                tp_actions = actions["take_profits"]
                for level, tp_data in tp_actions.items():
                    logger.info(
                        f"Take profit level {tp_data.get('level', level+1)} hit for {symbol} at {tp_data['price']}"
                    )
                    percentage = tp_data.get("percentage", 50.0)
                    success = await self._close_partial_position(symbol, percentage)
                    if success:
                        # Recording of exit level can be handled via the tracker if needed.
                        logger.info(
                            f"Recorded take profit level {level+1} as hit for {symbol}"
                        )
                        if level == 2:
                            await self._adjust_trailing_stop_after_final_tp(
                                symbol, current_price
                            )
            if "trailing_stop" in actions and isinstance(
                actions["trailing_stop"], dict
            ):
                logger.info(
                    f"Updated trailing stop for {symbol} to {actions['trailing_stop'].get('new_stop')}"
                )
            if "time_adjustment" in actions:
                logger.info(
                    f"Time-based adjustment for {symbol}: {actions['time_adjustment'].get('action')}"
                )
        except Exception as e:
            logger.error(f"Error handling position actions for {symbol}: {str(e)}")
            error_context = {
                "symbol": symbol,
                "current_price": current_price,
                "handler": self,
            }
            await self.error_recovery.handle_error(
                request_id, "_handle_position_actions", e, error_context
            )

    async def _close_position(self, symbol: str):
        try:
            position_info = await self.position_tracker.get_position_info(symbol)
            if not position_info:
                logger.warning(
                    f"Cannot close position for {symbol} - not found in tracker"
                )
                return False
            close_alert = {
                "symbol": symbol,
                "action": "CLOSE",
                "timeframe": position_info["timeframe"],
                "account": config.oanda_account,
            }
            success, result = await close_position(close_alert, self.position_tracker)
            if success:
                await self.position_tracker.clear_position(symbol)
                # Clear from other managers if integrated.
                if "longOrderFillTransaction" in result:
                    await self.risk_manager.update_daily_pnl(
                        float(result["longOrderFillTransaction"].get("pl", 0))
                    )
                if "shortOrderFillTransaction" in result:
                    await self.risk_manager.update_daily_pnl(
                        float(result["shortOrderFillTransaction"].get("pl", 0))
                    )
            return success
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {str(e)}")
            return False

    async def _close_partial_position(self, symbol: str, percentage: float):
        try:
            position_info = await self.position_tracker.get_position_info(symbol)
            if not position_info:
                logger.warning(
                    f"Cannot close partial position for {symbol} - not found in tracker"
                )
                return False
            close_alert = {
                "symbol": symbol,
                "action": "CLOSE",
                "timeframe": position_info["timeframe"],
                "account": config.oanda_account,
            }
            success, result = await close_partial_position(
                close_alert, percentage, self.position_tracker
            )
            if success:
                # Update positions in the tracker if needed.
                if "longOrderFillTransaction" in result:
                    await self.risk_manager.update_daily_pnl(
                        float(result["longOrderFillTransaction"].get("pl", 0))
                    )
                if "shortOrderFillTransaction" in result:
                    await self.risk_manager.update_daily_pnl(
                        float(result["shortOrderFillTransaction"].get("pl", 0))
                    )
                logger.info(
                    f"Partial position close for {symbol} ({percentage}%): Success"
                )
                return True
            else:
                logger.error(
                    f"Failed to close partial position for {symbol}: {result.get('error', 'Unknown error')}"
                )
                return False
        except Exception as e:
            logger.error(f"Error closing partial position for {symbol}: {str(e)}")
            return False

    async def _adjust_trailing_stop_after_final_tp(
        self, symbol: str, current_price: float
    ):
        try:
            position_info = await self.position_tracker.get_position_info(symbol)
            if not position_info:
                return False
            position_type = position_info.get("position_type")
            atr = position_info.get("atr", 0.0025)
            if position_type == "LONG":
                new_stop = current_price - (atr * 0.75)
            else:
                new_stop = current_price + (atr * 0.75)
            account_id = config.oanda_account
            session = await get_session()
            success, positions_data = await get_open_positions(account_id)
            if not success:
                logger.error("Failed to get positions for trailing stop update")
                return False
            trade_id = None
            for pos in positions_data.get("positions", []):
                if pos["instrument"] == symbol:
                    side = "long" if position_type == "LONG" else "short"
                    if pos[side].get("tradeIDs"):
                        trade_id = pos[side]["tradeIDs"][0]
                        break
            if not trade_id:
                logger.warning(
                    f"No trade ID found for {symbol} when updating trailing stop"
                )
                return False
            url = (
                f"{config.oanda_api_url}/accounts/{account_id}/trades/{trade_id}/orders"
            )
            stop_data = {
                "trailingStopLoss": {
                    "distance": str(round(atr * 0.75, 5)),
                    "timeInForce": "GTC",
                }
            }
            async with session.put(
                url, json=stop_data, timeout=HTTP_REQUEST_TIMEOUT
            ) as response:
                if response.status not in (200, 201):
                    error_text = await response.text()
                    logger.error(f"Failed to update trailing stop: {error_text}")
                    return False
                result = await response.json()
                logger.info(f"Updated trailing stop after final take profit: {result}")
                return True
        except Exception as e:
            logger.error(
                f"Error adjusting trailing stop after final TP for {symbol}: {str(e)}"
            )
            return False


# FastAPI Setup & Application Lifespan
alert_handler: Optional[AlertHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing application...")
    global _session, alert_handler
    try:
        await get_session(force_new=True)
        alert_handler = AlertHandler()
        await alert_handler.start()
        logger.info("Services initialized successfully")
        handle_shutdown_signals()
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup_resources()
        logger.info("Shutdown complete")


async def cleanup_resources():
    tasks = []
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    if _session is not None and not _session.closed:
        tasks.append(_session.close())
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def handle_shutdown_signals():
    async def shutdown(sig: signal.Signals):
        logger.info(f"Received exit signal {sig.name}")
        await cleanup_resources()

    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(s))
        )

def create_error_response(
    status_code: int, message: str, request_id: str
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code, content={"error": message, "request_id": request_id}
    )

app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def inject_dependencies(request: Request, call_next):
    request.state.alert_handler = alert_handler
    request.state.session = await get_session()
    return await call_next(request)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    try:
        logger.info(f"[{request_id}] {request.method} {request.url} started")
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.info(
            f"[{request_id}] {request.method} {request.url} completed with status {response.status_code} in {process_time:.4f}s"
        )
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception as e:
        logger.error(
            f"[{request_id}] Error processing request: {str(e)}", exc_info=True
        )
        return create_error_response(
            status_code=500, message="Internal server error", request_id=request_id
        )


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if path in ["/api/trade", "/api/close", "/api/alerts"]:
        client_ip = request.client.host
        if not hasattr(app, "rate_limiters"):
            app.rate_limiters = {}
        if client_ip not in app.rate_limiters:
            app.rate_limiters[client_ip] = {"count": 0, "reset_time": time.time() + 60}
        rate_limiter = app.rate_limiters[client_ip]
        current_time = time.time()
        if current_time > rate_limiter["reset_time"]:
            rate_limiter["count"] = 0
            rate_limiter["reset_time"] = current_time + 60
        rate_limiter["count"] += 1
        if rate_limiter["count"] > config.max_requests_per_minute:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too many requests",
                    "retry_after": int(rate_limiter["reset_time"] - current_time),
                },
            )
    return await call_next(request)


@app.get("/api/health")
async def health_check():
    try:
        session_status = (
            "healthy" if _session and not _session.closed else "unavailable"
        )
        account_status = "unknown"
        if session_status == "healthy":
            try:
                async with asyncio.timeout(5):
                    success, _ = await get_account_summary()
                    account_status = "connected" if success else "disconnected"
            except asyncio.TimeoutError:
                account_status = "timeout"
            except Exception:
                account_status = "error"
        tracker_status = (
            "healthy" if alert_handler and alert_handler._initialized else "unavailable"
        )
        circuit_breaker_status = None
        if alert_handler and hasattr(alert_handler, "error_recovery"):
            circuit_breaker_status = (
                await alert_handler.error_recovery.get_circuit_breaker_status()
            )
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "session": session_status,
                "account": account_status,
                "position_tracker": tracker_status,
            },
            "circuit_breaker": circuit_breaker_status,
            "version": "1.2.0",
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=500, content={"status": "unhealthy", "error": str(e)}
        )


@app.get("/api/circuit-breaker/status")
async def circuit_breaker_status():
    if not alert_handler or not hasattr(alert_handler, "error_recovery"):
        return JSONResponse(status_code=503, content={"error": "Service unavailable"})
    status = await alert_handler.error_recovery.get_circuit_breaker_status()
    return {"circuit_breaker": status, "timestamp": datetime.now().isoformat()}


@app.post("/api/circuit-breaker/reset")
async def reset_circuit_breaker():
    if not alert_handler or not hasattr(alert_handler, "error_recovery"):
        return JSONResponse(status_code=503, content={"error": "Service unavailable"})
    success = await alert_handler.error_recovery.reset_circuit_breaker()
    return {
        "success": success,
        "message": "Circuit breaker reset successfully"
        if success
        else "Failed to reset circuit breaker",
        "circuit_breaker": await alert_handler.error_recovery.get_circuit_breaker_status(),
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/account")
async def get_account_info():
    try:
        success, account_info = await get_account_summary()
        if not success:
            return JSONResponse(
                status_code=400, content={"error": "Failed to get account information"}
            )
        margin_rate = float(account_info.get("marginRate", "0"))
        margin_available = float(account_info.get("marginAvailable", "0"))
        margin_used = float(account_info.get("marginUsed", "0"))
        balance = float(account_info.get("balance", "0"))
        margin_utilization = (margin_used / balance) * 100 if balance > 0 else 0
        daily_pnl = 0
        if alert_handler:
            daily_pnl = await alert_handler.position_tracker.get_daily_pnl()
        return {
            "account_id": account_info.get("id"),
            "balance": balance,
            "currency": account_info.get("currency"),
            "margin_available": margin_available,
            "margin_used": margin_used,
            "margin_rate": margin_rate,
            "margin_utilization": round(margin_utilization, 2),
            "open_position_count": len(account_info.get("positions", [])),
            "daily_pnl": daily_pnl,
            "last_updated": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error getting account info: {str(e)}")
        return JSONResponse(
            status_code=500, content={"error": f"Internal server error: {str(e)}"}
        )


@app.get("/api/positions")
async def get_positions_info():
    try:
        success, oanda_positions = await get_open_positions()
        tracked_positions = {}
        if alert_handler and alert_handler.position_tracker:
            tracked_positions = await alert_handler.position_tracker.get_all_positions()
        positions_data = {}
        if success:
            for pos in oanda_positions.get("positions", []):
                symbol = pos["instrument"]
                long_units = float(pos.get("long", {}).get("units", 0))
                short_units = float(pos.get("short", {}).get("units", 0))
                direction = "LONG" if long_units > 0 else "SHORT"
                units = long_units if direction == "LONG" else abs(short_units)
                current_price = await get_current_price(symbol, direction)
                tracked_data = tracked_positions.get(symbol, {})
                risk_data = {}
                if (
                    alert_handler
                    and alert_handler.risk_manager
                    and symbol in alert_handler.risk_manager.position_tracker.positions
                ):
                    risk_data = {
                        "stop_loss": tracked_data.get("stop_loss"),
                        "take_profits": tracked_data.get("take_profits", {}),
                        "trailing_stop": tracked_data.get("trailing_stop"),
                    }
                unrealized_pl = float(
                    pos.get("long" if direction == "LONG" else "short", {}).get(
                        "unrealizedPL", 0
                    )
                )
                entry_price = tracked_data.get("entry_price") or float(
                    pos.get("long" if direction == "LONG" else "short", {}).get(
                        "averagePrice", 0
                    )
                )
                entry_time = tracked_data.get("entry_time")
                duration = None
                if entry_time:
                    now = datetime.now(timezone("Asia/Bangkok"))
                    duration = (now - entry_time).total_seconds() / 3600
                positions_data[symbol] = {
                    "symbol": symbol,
                    "direction": direction,
                    "units": units,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pl": unrealized_pl,
                    "timeframe": tracked_data.get("timeframe", "Unknown"),
                    "entry_time": entry_time.isoformat() if entry_time else None,
                    "duration_hours": round(duration, 2) if duration else None,
                    "risk_data": risk_data,
                }
        return {
            "positions": list(positions_data.values()),
            "count": len(positions_data),
            "tracking_available": alert_handler is not None
            and alert_handler._initialized,
            "last_updated": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error getting positions info: {str(e)}")
        return JSONResponse(
            status_code=500, content={"error": f"Internal server error: {str(e)}"}
        )


@app.get("/api/market/{instrument}")
async def get_market_info(instrument: str, timeframe: str = "H1"):
    try:
        instrument = standardize_symbol(instrument)
        tradeable, reason = is_instrument_tradeable(instrument)
        buy_price = await get_current_price(instrument, "BUY")
        sell_price = await get_current_price(instrument, "SELL")
        market_condition = "NORMAL"
        if alert_handler and alert_handler.volatility_monitor:
            await alert_handler.volatility_monitor.update_volatility(
                instrument, 0.001, timeframe
            )
            market_condition = (
                await alert_handler.volatility_monitor.get_market_condition(instrument)
            )
        structure_data = {}
        if alert_handler and alert_handler.market_structure:
            try:
                structure = (
                    await alert_handler.market_structure.analyze_market_structure(
                        instrument, timeframe, buy_price, buy_price, buy_price
                    )
                )
                structure_data = {
                    "nearest_support": structure.get("nearest_support"),
                    "nearest_resistance": structure.get("nearest_resistance"),
                    "support_levels": structure.get("support_levels", []),
                    "resistance_levels": structure.get("resistance_levels", []),
                }
            except Exception as e:
                logger.warning(f"Error getting market structure: {str(e)}")
        current_time = datetime.now(timezone("Asia/Bangkok"))
        return {
            "instrument": instrument,
            "timestamp": current_time.isoformat(),
            "tradeable": tradeable,
            "reason": reason if not tradeable else None,
            "prices": {
                "buy": buy_price,
                "sell": sell_price,
                "spread": round(abs(buy_price - sell_price), 5),
            },
            "market_condition": market_condition,
            "market_structure": structure_data,
            "current_session": get_current_market_session(current_time),
        }
    except Exception as e:
        logger.error(f"Error getting market info: {str(e)}")
        return JSONResponse(
            status_code=500, content={"error": f"Internal server error: {str(e)}"}
        )


@app.post("/api/alerts")
async def handle_alert(
    alert_data: AlertData, background_tasks: BackgroundTasks, request: Request
):
    request_id = str(uuid.uuid4())
    try:
        alert_dict = alert_data.dict()
        logger.info(
            f"[{request_id}] Received alert: {json.dumps(alert_dict, indent=2)}"
        )
        if not alert_handler:
            logger.error(f"[{request_id}] Alert handler not initialized")
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable", "request_id": request_id},
            )
        background_tasks.add_task(alert_handler.process_alert, alert_dict)
        return {
            "message": "Alert received and processing started",
            "request_id": request_id,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"[{request_id}] Error processing alert: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Internal server error: {str(e)}",
                "request_id": request_id,
            },
        )


@app.post("/api/trade")
async def execute_trade_endpoint(
    alert_data: AlertData, background_tasks: BackgroundTasks, request: Request
):
    request_id = str(uuid.uuid4())
    try:
        if (
            alert_handler
            and hasattr(alert_handler, "error_recovery")
            and await alert_handler.error_recovery.circuit_breaker.is_open()
        ):
            logger.warning(
                f"[{request_id}] Circuit breaker is open, rejecting trade request"
            )
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "message": "Trading temporarily disabled by circuit breaker",
                    "request_id": request_id,
                    "circuit_breaker": await alert_handler.error_recovery.get_circuit_breaker_status(),
                },
            )
        alert_dict = alert_data.dict()
        logger.info(f"[{request_id}] Trade request: {json.dumps(alert_dict, indent=2)}")
        success, result = await execute_trade(alert_dict)
        if success:
            if alert_handler and alert_handler.position_tracker:
                background_tasks.add_task(
                    alert_handler.position_tracker.record_position,
                    alert_dict["symbol"],
                    alert_dict["action"],
                    alert_dict["timeframe"],
                    float(result.get("orderFillTransaction", {}).get("price", 0)),
                )
            return {
                "success": True,
                "message": "Trade executed successfully",
                "transaction_id": result.get("orderFillTransaction", {}).get("id"),
                "request_id": request_id,
                "details": result,
            }
        else:
            if alert_handler and hasattr(alert_handler, "error_recovery"):
                await alert_handler.error_recovery.circuit_breaker.record_error()
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Trade execution failed",
                    "request_id": request_id,
                    "error": result.get("error", "Unknown error"),
                },
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}", exc_info=True)
        if alert_handler and hasattr(alert_handler, "error_recovery"):
            error_context = {
                "func": execute_trade,
                "args": [alert_data.dict()],
                "handler": alert_handler,
            }
            await alert_handler.error_recovery.handle_error(
                request_id, "execute_trade_endpoint", e, error_context
            )
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Internal server error: {str(e)}",
                "request_id": request_id,
            },
        )


@app.post("/api/close")
async def close_position_endpoint(close_data: Dict[str, Any], request: Request):
    request_id = str(uuid.uuid4())
    try:
        logger.info(
            f"[{request_id}] Close position request: {json.dumps(close_data, indent=2)}"
        )
        success, result = await close_position(close_data)
        if success:
            if alert_handler and alert_handler.position_tracker:
                await alert_handler.position_tracker.clear_position(
                    close_data["symbol"]
                )
                if alert_handler.risk_manager:
                    await alert_handler.risk_manager.clear_position(
                        close_data["symbol"]
                    )
            return {
                "success": True,
                "message": "Position closed successfully",
                "transaction_id": result.get("longOrderFillTransaction", {}).get("id")
                or result.get("shortOrderFillTransaction", {}).get("id"),
                "request_id": request_id,
            }
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Failed to close position",
                    "request_id": request_id,
                    "error": result.get("error", "Unknown error"),
                },
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Internal server error: {str(e)}",
                "request_id": request_id,
            },
        )


@app.post("/api/config")
async def update_config_endpoint(config_data: Dict[str, Any], request: Request):
    try:
        if not alert_handler:
            return JSONResponse(
                status_code=503, content={"error": "Service unavailable"}
            )
        success = await alert_handler.update_config(config_data)
        if success:
            return {"success": True, "message": "Configuration updated successfully"}
        else:
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "Failed to update configuration"},
            )
    except Exception as e:
        logger.error(f"Error updating configuration: {str(e)}")
        return JSONResponse(
            status_code=500, content={"error": f"Internal server error: {str(e)}"}
        )


@app.post("/tradingview")
async def tradingview_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Process TradingView webhook alerts with detailed logging"""
    request_id = str(uuid.uuid4())
    
    try:
        # Log raw request details
        logger.info(f"[{request_id}] RAW TRADINGVIEW WEBHOOK RECEIVED")
        logger.info(f"[{request_id}] Headers: {dict(request.headers)}")
        
        # Get the raw body before parsing as JSON
        body = await request.body()
        logger.info(f"[{request_id}] Raw body: {body.decode('utf-8')}")
        
        try:
            # Try to parse the JSON payload
            payload = await request.json()
            logger.info(f"[{request_id}] Parsed TradingView webhook: {json.dumps(payload, indent=2)}")
            
            # Map TradingView fields to your AlertData model
            alert_data = {
                "symbol": payload.get("symbol", ""),
                "action": payload.get("action", ""),
                "timeframe": payload.get("timeframe", "15M"),
                "orderType": payload.get("orderType", "MARKET"),
                "timeInForce": payload.get("timeInForce", "FOK"),
                "percentage": float(payload.get("percentage", 15.0)),
                "account": payload.get("account", config.oanda_account),
                "comment": payload.get("comment", "")
            }
            
            logger.info(f"[{request_id}] Mapped alert data: {json.dumps(alert_data, indent=2)}")
            
            # Process alert in the background
            if alert_handler:
                background_tasks.add_task(
                    alert_handler.process_alert,
                    alert_data
                )
                
                logger.info(f"[{request_id}] Alert processing task created successfully")
                
                return {
                    "message": "TradingView alert received and processing started",
                    "request_id": request_id,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                logger.error(f"[{request_id}] Alert handler not initialized")
                return JSONResponse(
                    status_code=503,
                    content={"error": "Service unavailable", "request_id": request_id}
                )
        except json.JSONDecodeError as e:
            logger.error(f"[{request_id}] Invalid JSON: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={"error": f"Invalid JSON: {str(e)}", "request_id": request_id}
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error processing TradingView webhook: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )


def start():
    import uvicorn

    setup_logging()
    logger.info(f"Starting application in {config.environment} mode")
    host = config.host
    port = config.port
    logger.info(f"Server starting at {host}:{port}")
    uvicorn.run(
        "app:app", host=host, port=port, reload=config.environment == "development"
    )


if __name__ == "__main__":
    start()
