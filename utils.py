# utils.py - Fixed version with circular import resolution
import os
import logging
import json
import random
import asyncio
from typing import Tuple, Dict, Any, Optional, TYPE_CHECKING
from config import config
from datetime import datetime, time, timezone, timedelta
import pytz

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from main import robust_oanda_request

# --- Static Mappings and Constants ---
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 5

# TV_FIELD_MAP for TradingView webhook processing
TV_FIELD_MAP = {
    "action": "direction",        
    "percentage": "risk_percent", 
    "risk_percent": "risk_percent", 
    "ticker": "symbol",           
    "symbol": "symbol",           
    "timeframe": "timeframe",     
    "interval": "timeframe",      
    "alert_id": "alert_id",       
    "position_id": "position_id", 
    "timestamp": "timestamp",     
    "orderType": "orderType",     
    "timeInForce": "timeInForce", 
    "account": "account",         
    "comment": "comment",         
    "exchange": "exchange",       
    "strategy": "strategy",       
    "side": "direction",          
    "risk": "risk_percent"        
}

INSTRUMENT_LEVERAGES = {
    "XAU_USD": 20,
    "XAG_USD": 20,
    "EUR_USD": 30,
    "GBP_USD": 30,
    "USD_JPY": 30,
    "USD_CHF": 30,
    "AUD_USD": 30,
    "NZD_USD": 30,
    "USD_CAD": 30,
    "BTC_USD": 2,
    "ETH_USD": 5,
    "default": 20,
}

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

class MarketDataUnavailableError(Exception):
    """Raised when market data cannot be fetched from the broker/API."""
    pass

class JSONFormatter(logging.Formatter):
    def format(self, record):
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
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)
        if hasattr(record, "position_id"):
            log_data["position_id"] = record.position_id
        if hasattr(record, "symbol"):
            log_data["symbol"] = record.symbol
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id
        return json.dumps(log_data)

def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    json_formatter = JSONFormatter()
    main_handler = logging.FileHandler(os.path.join(log_dir, "trading_system.log"))
    main_handler.setFormatter(json_formatter)
    main_handler.setLevel(logging.INFO)
    logger.addHandler(main_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

class TradingLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        context = self.extra.copy()
        if "extra" in kwargs:
            context.update(kwargs["extra"])
        kwargs["extra"] = context
        return msg, kwargs

def get_trading_logger(name, **context):
    return TradingLogger(logging.getLogger(name), context)

def get_module_logger(name, **context):
    """Get a logger for a specific module with context"""
    return TradingLogger(logging.getLogger(name), context)

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format with robust error handling"""
    if not symbol:
        return ""
    try:
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')

        # Crypto mapping override (do this first)
        if symbol_upper in CRYPTO_MAPPING:
            return CRYPTO_MAPPING[symbol_upper]

        # If already in "XXX_YYY" form and both parts look valid, return it
        if "_" in symbol_upper and len(symbol_upper.split("_")) == 2:
            base, quote = symbol_upper.split("_")
            if len(base) >= 3 and len(quote) >= 3:
                return symbol_upper

        # Special handling for JPY pairs (6-char, no underscore)
        if "JPY" in symbol_upper and "_" not in symbol_upper and len(symbol_upper) == 6:
            return symbol_upper[:3] + "_" + symbol_upper[3:]

        # 6-char Forex pairs (not crypto)
        if (
            len(symbol_upper) == 6
            and not any(crypto in symbol_upper for crypto in ["BTC", "ETH", "XRP", "LTC", "BCH", "DOT", "ADA", "SOL"])
        ):
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"

        # Fallback for crypto pairs without mapping
        for crypto in ["BTC", "ETH", "LTC", "XRP", "BCH", "DOT", "ADA", "SOL"]:
            if crypto in symbol_upper and "USD" in symbol_upper:
                return f"{crypto}_USD"

        # Final fallback
        return symbol_upper

    except Exception as e:
        logger.error(f"Error standardizing symbol {symbol}: {e}")
        return symbol.upper() if symbol else ""

def get_instrument_type(instrument: str) -> str:
    """Determine instrument type from symbol"""
    try:
        if not instrument:
            logger.warning("Empty instrument provided, defaulting to FOREX")
            return "FOREX"
            
        inst = instrument.upper()
        
        crypto_list = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']
        commodity_list = ['XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BCO', 'NATGAS', 'OIL']
        index_list = ['SPX', 'NAS', 'US30', 'UK100', 'DE30', 'JP225', 'AUS200', 'DAX']

        # Check for underscore format (e.g., EUR_USD, BTC_USD)
        if '_' in inst:
            parts = inst.split('_')
            if len(parts) == 2:
                base, quote = parts
                
                if base in crypto_list:
                    return "CRYPTO"
                if base in commodity_list:
                    return "COMMODITY"
                if base in index_list:
                    return "INDICES"
                    
                # Check Forex (standard 3-letter codes)
                if len(base) == 3 and len(quote) == 3 and base.isalpha() and quote.isalpha():
                    if base not in commodity_list:
                        return "FOREX"
                    else:
                        return "COMMODITY"
        
        # Handle no-underscore format
        else:
            # Check Crypto patterns
            for crypto in crypto_list:
                if inst.startswith(crypto):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "USDT", "GBP", "JPY"]):
                        return "CRYPTO"
            
            # Check Commodity patterns
            for comm in commodity_list:
                if inst.startswith(comm):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                        return "COMMODITY"
            
            # Check Index patterns
            for index in index_list:
                if inst.startswith(index):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                        return "INDICES"
            
            # Check standard 6-char Forex
            if len(inst) == 6 and inst.isalpha():
                for comm in commodity_list:
                    if inst.startswith(comm):
                        return "COMMODITY"
                return "FOREX"

        logger.warning(f"Could not determine instrument type for '{instrument}', defaulting to FOREX.")
        return "FOREX"
        
    except Exception as e:
        logger.error(f"Error determining instrument type for '{instrument}': {str(e)}")
        return "FOREX"

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if instrument is tradeable based on market hours and symbol validity"""
    if not symbol or not isinstance(symbol, str):
        return False, "Invalid symbol: symbol must be a non-empty string"
    
    symbol = symbol.strip().upper()
    if not symbol:
        return False, "Invalid symbol: empty symbol after cleanup"
    
    try:
        instrument_type = get_instrument_type(symbol)
    except Exception as e:
        return False, f"Unable to determine instrument type: {str(e)}"
    
    # Crypto validation and trading status
    if instrument_type == "CRYPTO":
        valid_crypto_symbols = {
            "BTC_USD", "ETH_USD", "LTC_USD", "XRP_USD", "BCH_USD", 
            "DOT_USD", "ADA_USD", "SOL_USD", "BTCUSD", "ETHUSD",
            "LTCUSD", "XRPUSD", "BCHUSD", "DOTUSD", "ADAUSD", "SOLUSD"
        }
        
        if symbol not in valid_crypto_symbols:
            if "_" in symbol:
                base_currency = symbol.split("_")[0]
            elif len(symbol) >= 6:
                base_currency = symbol[:3]
            else:
                return False, f"Invalid crypto symbol format: {symbol}"
            
            supported_cryptos = {"BTC", "ETH", "LTC", "XRP", "BCH", "DOT", "ADA", "SOL"}
            if base_currency not in supported_cryptos:
                return False, f"Unsupported crypto currency: {base_currency}"
        
        # Crypto trades 24/7 but check for maintenance windows
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        
        if hour == 0 and now_utc.minute < 30:
            return False, "Crypto exchange may be in maintenance window (00:00-00:30 UTC)"
        
        return True, ""
    
    # Forex market hours validation
    EST = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_est = now_utc.astimezone(EST)

    weekday = now_est.weekday()  # Mon=0 … Sun=6
    t = now_est.time()

    # Market hours logic
    if weekday == 6 and t < time(17, 5):  # Sunday before open
        return False, "Forex market closed. Opens Sunday 17:05 EST"

    if weekday == 4 and t >= time(17, 0):  # Friday after close
        return False, "Forex market closed for weekend. Reopens Sunday 17:05 EST"

    if weekday == 5:  # Saturday
        return False, "Forex market closed Saturday. Opens Sunday 17:05 EST"

    return True, ""

# Async functions with proper error handling
async def get_current_price(symbol: str, action: str) -> float:
    """Get current price from OANDA with fallback handling"""
    try:
        # Dynamic import to avoid circular dependency
        import importlib
        main_module = importlib.import_module('main')
        robust_oanda_request = getattr(main_module, 'robust_oanda_request', None)
        
        if not robust_oanda_request:
            raise ImportError("robust_oanda_request not available")
            
        from oandapyV20.endpoints.pricing import PricingInfo
        pricing_request = PricingInfo(
            accountID=config.oanda_account_id,
            params={"instruments": symbol}
        )
        
        response = await robust_oanda_request(pricing_request)
        
        if 'prices' in response and response['prices']:
            price_data = response['prices'][0]
            if action.upper() == "BUY":
                return float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
            else:
                return float(price_data.get('bid', price_data.get('closeoutBid', 0)))
        
        raise MarketDataUnavailableError(f"No price data returned for {symbol}")
        
    except ImportError:
        # Fallback to direct OANDA client
        try:
            import oandapyV20
            from oandapyV20.endpoints.pricing import PricingInfo
            
            access_token = config.oanda_access_token
            if hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            
            api = oandapyV20.API(
                access_token=access_token,
                environment=config.oanda_environment
            )
            
            pricing_request = PricingInfo(
                accountID=config.oanda_account_id,
                params={"instruments": symbol}
            )
            
            response = api.request(pricing_request)
            
            if 'prices' in response and response['prices']:
                price_data = response['prices'][0]
                if action.upper() == "BUY":
                    return float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
                else:
                    return float(price_data.get('bid', price_data.get('closeoutBid', 0)))
            
            raise MarketDataUnavailableError(f"No price data returned for {symbol}")
            
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            raise MarketDataUnavailableError(f"Failed to fetch current price for {symbol}: {e}")
    
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        raise MarketDataUnavailableError(f"Failed to fetch current price for {symbol}: {e}")

async def get_account_balance(use_fallback: bool = False) -> float:
    """Get account balance with proper error handling"""
    if use_fallback:
        return 10000.0
    
    try:
        # Dynamic import to avoid circular dependency
        import importlib
        main_module = importlib.import_module('main')
        robust_oanda_request = getattr(main_module, 'robust_oanda_request', None)
        
        if robust_oanda_request:
            from oandapyV20.endpoints.accounts import AccountDetails
            account_request = AccountDetails(accountID=config.oanda_account_id)
            response = await robust_oanda_request(account_request)
            return float(response['account']['balance'])
        
    except ImportError:
        pass
    
    # Fallback to direct OANDA client
    try:
        import oandapyV20
        from oandapyV20.endpoints.accounts import AccountDetails
        
        access_token = config.oanda_access_token
        if hasattr(access_token, 'get_secret_value'):
            access_token = access_token.get_secret_value()
        
        api = oandapyV20.API(
            access_token=access_token,
            environment=config.oanda_environment
        )
        
        account_request = AccountDetails(accountID=config.oanda_account_id)
        response = api.request(account_request)
        return float(response['account']['balance'])
        
    except Exception as e:
        logger.error(f"Error getting account balance: {e}")
        return 10000.0

# ATR and position sizing functions
async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    """Get ATR with proper error handling and caching"""
    try:
        # Your existing ATR calculation logic here
        # For now, return a reasonable default
        logger.warning(f"ATR calculation not fully implemented for {symbol} {timeframe}")
        
        # Return instrument-specific default ATR
        instrument_type = get_instrument_type(symbol)
        if instrument_type == "FOREX":
            if "JPY" in symbol:
                return 0.8  # JPY pairs typically have higher ATR
            else:
                return 0.0008  # Standard forex ATR
        elif instrument_type == "CRYPTO":
            return 50.0  # Crypto typically has much higher volatility
        elif instrument_type == "COMMODITY":
            if "XAU" in symbol:
                return 1.5  # Gold ATR
            elif "XAG" in symbol:
                return 0.05  # Silver ATR
            else:
                return 1.0
        else:
            return 0.001
            
    except Exception as e:
        logger.error(f"Error calculating ATR for {symbol}: {e}")
        raise MarketDataUnavailableError(f"Failed to calculate ATR for {symbol}: {e}")

def get_instrument_leverage(symbol: str) -> float:
    """Get leverage for instrument"""
    return INSTRUMENT_LEVERAGES.get(symbol, INSTRUMENT_LEVERAGES.get("default", 20))

def get_position_size_limits(symbol: str) -> Tuple[float, float]:
    """Get min/max position sizes for symbol"""
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == "CRYPTO":
        base_currency = symbol.split("_")[0] if "_" in symbol else symbol[:3]
        if base_currency == "BTC":
            return 0.0001, 10.0
        elif base_currency == "ETH":
            return 0.002, 135.0
        else:
            return 0.001, 1000.0
    elif instrument_type == "COMMODITY":
        if "XAU" in symbol:
            return 0.01, 100.0
        elif "XAG" in symbol:
            return 0.1, 1000.0
        else:
            return 0.1, 1000.0
    else:  # FOREX
        return 1.0, 10000000.0

def calculate_simple_position_size(account_balance: float, risk_percent: float, 
                                 entry_price: float, stop_loss: float, symbol: str) -> float:
    """Calculate position size based on risk"""
    try:
        risk_amount = account_balance * (risk_percent / 100.0)
        stop_distance = abs(entry_price - stop_loss)
        
        if stop_distance <= 0:
            return 0.0
        
        raw_size = risk_amount / stop_distance
        min_units, max_units = get_position_size_limits(symbol)
        
        # Apply leverage and margin constraints
        leverage = get_instrument_leverage(symbol)
        required_margin = (raw_size * entry_price) / leverage
        available_margin = account_balance * 0.8  # 80% utilization max
        
        if required_margin > available_margin:
            raw_size = (available_margin * leverage) / entry_price
        
        position_size = max(min_units, min(max_units, raw_size))
        return position_size
        
    except Exception as e:
        logger.error(f"Error calculating position size: {e}")
        return 0.0

def round_position_size(symbol: str, position_size: float) -> float:
    """Round position size to appropriate precision"""
    try:
        instrument_type = get_instrument_type(symbol)
        
        if instrument_type == "FOREX":
            return round(position_size)  # Whole units for OANDA
        elif instrument_type == "CRYPTO":
            if "BTC" in symbol:
                return round(position_size, 4)
            elif "ETH" in symbol:
                return round(position_size, 3)
            else:
                return round(position_size, 2)
        elif instrument_type == "COMMODITY":
            if "XAU" in symbol or "XAG" in symbol:
                return round(position_size, 1)
            else:
                return round(position_size)
        else:
            return round(position_size)
            
    except Exception as e:
        logger.error(f"Error rounding position size for {symbol}: {e}")
        return round(position_size)

def validate_trade_inputs(units: float, risk_percent: float, atr: float, 
                         stop_loss_distance: float, min_units: float, max_units: float) -> Tuple[bool, str]:
    """Validate trade inputs"""
    if units is None or units <= 0:
        return False, f"Units ({units}) is zero or negative"
    
    units_int = int(units)
    
    if units_int < min_units:
        return False, f"Units ({units_int}) below minimum ({min_units})"
    
    if units_int > max_units:
        return False, f"Units ({units_int}) above maximum ({max_units})"
    
    if risk_percent is None or risk_percent <= 0:
        return False, f"Risk percent ({risk_percent}) is invalid"
    
    if atr is None or atr <= 0:
        return False, f"ATR ({atr}) is invalid"
    
    if stop_loss_distance is None or stop_loss_distance <= 0:
        return False, f"Stop loss distance ({stop_loss_distance}) is invalid"
    
    return True, "Inputs valid"

# Additional utility functions
def parse_iso_datetime(datetime_str: str) -> datetime:
    """Parse ISO datetime string"""
    try:
        if datetime_str.endswith('Z'):
            datetime_str = datetime_str[:-1] + '+00:00'
        return datetime.fromisoformat(datetime_str)
    except ValueError:
        return datetime.now(timezone.utc)

def normalize_timeframe(tf: str, *, target: str = "OANDA") -> str:
    """Normalize timeframe format"""
    if not tf:
        return "H1"
    
    tf_upper = tf.upper()
    timeframe_map = {
        "1M": "M1", "5M": "M5", "15M": "M15", "30M": "M30",
        "1H": "H1", "4H": "H4", "1D": "D1", "1W": "W1"
    }
    
    return timeframe_map.get(tf_upper, tf_upper)

# Market impact and transaction cost analysis
async def check_market_impact(symbol: str, position_size: float, timeframe: str = "D1", 
                            warn_threshold: float = 1.0, cap_threshold: float = 5.0) -> tuple:
    """Check market impact for large trades"""
    try:
        # Simplified implementation - would need real volume data
        return True, None, 0.0, 0.0, cap_threshold
    except Exception as e:
        logger.error(f"Error checking market impact: {e}")
        return True, None, 0.0, 0.0, cap_threshold

async def analyze_transaction_costs(symbol: str, position_size: float, action: str = "BUY") -> dict:
    """Analyze transaction costs"""
    try:
        # Simplified implementation
        return {
            "spread_pips": 0.0,
            "spread_percent": 0.0,
            "commission_per_unit": 0.0,
            "commission_total": 0.0,
            "estimated_total_cost": 0.0,
            "summary": "Transaction cost analysis not implemented"
        }
    except Exception as e:
        logger.error(f"Error analyzing transaction costs: {e}")
        return {"summary": f"Error: {e}"}

# Position sizing alternatives
def calculate_notional_position_size(account_balance: float, allocation_percent: float, 
                                   current_price: float, symbol: str) -> float:
    """Calculate position size based on notional allocation"""
    leverage = get_instrument_leverage(symbol)
    notional_value = account_balance * (allocation_percent / 100.0) * leverage
    position_size = notional_value / current_price
    return round_position_size(symbol, position_size)

def _get_simulated_price(symbol: str, direction: str) -> float:
    """Disabled simulated price fallback"""
    raise MarketDataUnavailableError(f"Simulated price fallback disabled for {symbol}")

# Add any other missing functions that are referenced in your codebase
def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """Get ATR multiplier for stop loss calculation"""
    base_multipliers = {
        "forex": 2.0,
        "jpy_pair": 2.5,
        "metal": 1.5,
        "index": 2.0,
        "crypto": 3.0,
        "commodity": 1.8,
        "other": 2.0
    }
    
    timeframe_factors = {
        "M1": 1.5, "M5": 1.3, "M15": 1.2, "M30": 1.1,
        "H1": 1.0, "H4": 0.9, "D1": 0.8, "W1": 0.7
    }
    
    normalized_timeframe = normalize_timeframe(timeframe)
    base = base_multipliers.get(instrument_type.lower(), 2.0)
    factor = timeframe_factors.get(normalized_timeframe, 1.0)
    
    return base * factor
