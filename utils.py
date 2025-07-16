#
# file: utils.py
#
"""
INSTITUTIONAL TRADING UTILITIES
Enhanced position sizing, price handling, and risk management utilities
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
import json
import math
import random

# FIX: Import the settings object to get configuration values
from config import settings

logger = logging.getLogger(__name__)


# ===== CONSTANTS & EXCEPTIONS =====
# FIX: Define the missing RETRY_DELAY constant.
# We'll pull this from the central config for consistency.
RETRY_DELAY = settings.oanda.retry_delay

class MarketDataUnavailableError(Exception):
    """Custom exception for when market data cannot be fetched."""
    pass


# ... (The rest of the file remains exactly as you corrected it in the previous step)
# The functions _get_simulated_price, get_instrument_leverage, etc., are all correct.


# ===== EXPORT UTILITIES =====
# FIX: Update __all__ to export the new constant.
__all__ = [
    'get_current_price',
    'format_symbol_for_oanda', 
    'calculate_position_size',
    'get_instrument_settings',
    'parse_iso_datetime',
    'format_datetime_iso',
    'is_market_hours',
    'validate_trading_request',
    'safe_json_parse',
    'PerformanceTimer',
    'log_trade_metrics',
    '_get_simulated_price',
    'get_instrument_leverage',
    'round_position_size',
    'get_position_size_limits',
    'validate_trade_inputs',
    'MarketDataUnavailableError',
    'RETRY_DELAY' # Add the new constant here
]

# Note: The full body of all other functions in utils.py should be included here as before.
# For brevity, they are omitted from this code block display.
