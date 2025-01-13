# trading_bot.py

import os
import requests
import json
import uuid
import aiohttp
import asyncio
from flask import Flask, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
import time
import math
from datetime import datetime, timedelta
from pytz import timezone

# Constants
SPREAD_THRESHOLD_FOREX = 0.001  # 0.1% for forex
SPREAD_THRESHOLD_CRYPTO = 0.008  # 0.8% for crypto
MAX_RETRIES = 3
BASE_DELAY = 1.0  # Base delay in seconds

# Default settings
DEFAULT_FOREX_PRECISION = 5
DEFAULT_CRYPTO_PRECISION = 2
DEFAULT_MIN_ORDER_SIZE = 1000

# Environment variables
OANDA_API_TOKEN = os.getenv('OANDA_API_TOKEN')
OANDA_API_URL = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Setup logging
os.makedirs('/opt/render/project/src/logs', exist_ok=True)
log_file = os.path.join('/opt/render/project/src/logs', 'trading_bot.log')
max_bytes = 10 * 1024 * 1024  # 10MB
backup_count = 1  # Only keep 1 backup for 24 hours of logs

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Instrument configurations
INSTRUMENT_LEVERAGES = {
    # Forex
    "USD_CHF": 20, "SGD_CHF": 20, "CAD_HKD": 10, "USD_JPY": 20, "EUR_TRY": 4,
    "AUD_HKD": 10, "USD_CNH": 20, "AUD_JPY": 20, "USD_TRY": 4,  "GBP_JPY": 20,
    "CHF_ZAR": 20, "USD_NOK": 20, "USD_HKD": 10, "USD_DKK": 20, "GBP_NZD": 20,
    "EUR_CAD": 20, "EUR_HKD": 10, "EUR_ZAR": 20, "AUD_USD": 20, "EUR_JPY": 20,
    "NZD_SGD": 20, "GBP_PLN": 20, "EUR_DKK": 10, "EUR_SEK": 20, "USD_SGD": 20,
    "CHF_JPY": 20, "NZD_CAD": 20, "GBP_CAD": 20, "GBP_ZAR": 20, "EUR_PLN": 20,
    "CHF_HKD": 10, "GBP_AUD": 20, "USD_PLN": 20, "EUR_USD": 20, "NZD_HKD": 10,
    "USD_MXN": 20, "GBP_USD": 20, "HKD_JPY": 10, "SGD_JPY": 20, "CAD_SGD": 20,
    "USD_CZK": 20, "NZD_USD": 20, "GBP_HKD": 10, "AUD_CHF": 20, "AUD_NZD": 20,
    "EUR_AUD": 20, "USD_SEK": 20, "GBP_SGD": 20, "CAD_JPY": 20, "ZAR_JPY": 20,
    "USD_HUF": 20, "USD_CAD": 20, "AUD_SGD": 20, "EUR_HUF": 20, "NZD_CHF": 20,
    "EUR_CZK": 20, "USD_ZAR": 20, "EUR_SGD": 20, "EUR_CHF": 20, "EUR_NZD": 20,
    "EUR_GBP": 20, "CAD_CHF": 20, "EUR_NOK": 20, "AUD_CAD": 20, "NZD_JPY": 20,
    "TRY_JPY": 4,  "GBP_CHF": 20, "USD_THB": 20,

    # Bonds
    "UK10Y_GILT": 5, "US5Y_TNOTE": 5, "US_TBOND": 5, "US10Y_TNOTE": 5,
    "BUND": 5, "US2Y_TNOTE": 5,

    # Metals
    "XAU_USD": 5, "XAG_USD": 5,

    # Indices
    "US_SPX_500": 20, "US_NAS_100": 20, "US_WALL_ST_30": 20,
    "UK_100": 20, "EUROPE_50": 20, "FRANCE_40": 20, "GERMANY_30": 20,
    "AUSTRALIA_200": 20, "US_RUSS_2000": 20, "SWITZERLAND_20": 5,
    "SPAIN_35": 5, "NETHERLANDS_25": 5,

    # Commodity
    "SOYBEANS": 5, "COPPER": 5, "BRENT_CRUDE_OIL": 5, "PLATINUM": 5,
    "CORN": 5, "NATURAL_GAS": 5, "SUGAR": 5, "PALLADIUM": 5,
    "WHEAT": 5, "WTI_CRUDE_OIL": 5,

    # Crypto
    "BTC_USD": 2, "ETH_USD": 2, "LTC_USD": 2, "XRP_USD": 2, "BCH_USD": 2
}

INSTRUMENT_PRECISION = {
    # Major Forex
    "EUR_USD": 5, "GBP_USD": 5, "USD_JPY": 3, "USD_CHF": 5, 
    "USD_CAD": 5, "AUD_USD": 5, "NZD_USD": 5,
    # Cross Rates  
    "EUR_GBP": 5, "EUR_JPY": 3, "GBP_JPY": 3, "EUR_CHF": 5,
    "GBP_CHF": 5, "EUR_CAD": 5, "GBP_CAD": 5, "CAD_CHF": 5,
    "AUD_CAD": 5, "NZD_CAD": 5,
    # Crypto
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 4, "LTC_USD": 2
}

MIN_ORDER_SIZES = {
    # Major Forex
    "EUR_USD": 1000, "GBP_USD": 1000, "USD_JPY": 1000, "USD_CHF": 1000,
    "USD_CAD": 1000, "AUD_USD": 1000, "NZD_USD": 1000,
    # Cross Rates
    "EUR_GBP": 1000, "EUR_JPY": 1000, "GBP_JPY": 1000, "EUR_CHF": 1000,
    "GBP_CHF": 1000, "EUR_CAD": 1000, "GBP_CAD": 1000, "CAD_CHF": 1000,
    "AUD_CAD": 1000, "NZD_CAD": 1000,
    # Crypto
    "BTC_USD": 0.25, "ETH_USD": 4, "XRP_USD": 200, "LTC_USD": 4
}

def get_instrument_leverage(instrument: str) -> float:
    """Return the leverage for a given instrument"""
    return INSTRUMENT_LEVERAGES.get(instrument, 20)

def is_market_open():
    """Check if market is open based on Bangkok time"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    hour = current_time.hour

    if (wday == 5 and hour >= 5) or (wday == 6) or (wday == 0 and hour < 5):
        return False, "Weekend market closure"
    return True, "Regular trading hours"

def calculate_next_market_open():
    """Calculate next market open time in Bangkok timezone"""
    current = datetime.now(timezone('Asia/Bangkok'))
    if current.weekday() == 5:
        days_to_add = 1
    elif current.weekday() == 6 and current.hour < 4:
        days_to_add = 0
    else:
        days_to_add = 7 - current.weekday()

    next_open = current + timedelta(days=days_to_add)
    next_open = next_open.replace(hour=4, minute=0, second=0, microsecond=0)
    return next_open

def check_spread_warning(pricing_data, instrument):
    """Check spreads with different thresholds for forex and crypto"""
    if not pricing_data.get('prices'):
        return False, 0

    price = pricing_data['prices'][0]
    try:
        bid = float(price['bids'][0]['price'])
        ask = float(price['asks'][0]['price'])
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error parsing price data: {e}")
        return False, 0

    spread = ask - bid
    spread_percentage = (spread / bid)

    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    threshold = SPREAD_THRESHOLD_CRYPTO if is_crypto else SPREAD_THRESHOLD_FOREX

    if spread_percentage > threshold:
        logger.warning(f"Wide spread detected for {instrument}: {spread:.5f} ({spread_percentage*100:.2f}%)")
        return True, spread

    return False, 0

def get_instrument_price(instrument, account_id):
    """Fetch current pricing data from OANDA"""
    if not all([OANDA_API_TOKEN, OANDA_API_URL]):
        error_msg = "Missing required OANDA configuration"
        logger.error(error_msg)
        return False, error_msg

    try:
        headers = {
            "Authorization": f"Bearer {OANDA_API_TOKEN}",
            "Content-Type": "application/json"
        }
        url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/pricing?instruments={instrument}"

        logger.info(f"Fetching instrument price from OANDA: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        pricing_data = response.json()
        if not pricing_data.get('prices'):
            error_msg = f"No pricing data returned for {instrument}"
            logger.warning(error_msg)
            return False, error_msg

        return True, pricing_data

    except Exception as e:
        error_msg = f"Error fetching instrument price: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def check_market_status(instrument, account_id):
    """Check market status with trading hours and spread monitoring"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    logger.info(f"Checking market status at {current_time.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")

    is_open, reason = is_market_open()
    if not is_open:
        next_open = calculate_next_market_open()
        logger.info(f"Market is closed: {reason}. Next opening at {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
        return False, f"Market closed: {reason}. Opens {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time"

    price_success, pricing_data = get_instrument_price(instrument, account_id)
    if not price_success:
        return False, pricing_data

    has_wide_spread, spread = check_spread_warning(pricing_data, instrument)
    if has_wide_spread:
        return False, f"Spread too wide: {spread:.5f}"

    return True, pricing_data

class AlertHandler:
    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY):
        self.logger = logging.getLogger('alert_handler')
        self.max_retries = max_retries
        self.base_delay = base_delay

    async def process_alert(self, alert_data: dict) -> bool:
        """Process an alert with retries and discard if failed"""
        alert_id = alert_data.get('id', str(uuid.uuid4()))
        
        for attempt in range(self.max_retries):
            try:
                instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
                is_tradeable, status_message = check_market_status(
                    instrument,
                    alert_data.get('account', OANDA_ACCOUNT_ID)
                )

                if not is_tradeable:
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        self.logger.warning(
                            f"Market not tradeable for alert {alert_id}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{self.max_retries}): {status_message}"
                        )
                        await asyncio.sleep(delay)
                        continue
                    return False

                success, trade_result = execute_trade(alert_data)
                if success:
                    if attempt > 0:
                        self.logger.info(f"Alert {alert_id} succeeded on attempt {attempt + 1}")
                    return True
                
                if attempt < self.max_retries - 1:
                    delay = self.base_delay * (2 ** attempt)
                    self.logger.warning(
                        f"Alert {alert_id} failed, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(delay)
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Alert {alert_id} failed permanently: {str(e)}")
                    return False
                
                self.logger.error(f"Error processing alert {alert_id}: {str(e)}")
        
        self.logger.info(f"Alert {alert_id} discarded after {self.max_retries} failed attempts")
        return False

def execute_trade(alert_data):
    """Execute trade with OANDA"""
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    BASE_POSITION = 100000
    
    # Get the leverage and determine precision
    leverage = get_instrument_leverage(instrument)
    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    precision = INSTRUMENT_PRECISION.get(
        instrument,
        DEFAULT_CRYPTO_PRECISION if is_crypto else DEFAULT_FOREX_PRECISION
    )
    min_size = MIN_ORDER_SIZES.get(instrument, DEFAULT_MIN_ORDER_SIZE)
    
    # Calculate trade size
    trade_size = BASE_POSITION * float(alert_data['percentage']) * leverage
    
    price_success, price_data = get_instrument_price(instrument, alert_data['account'])
    if not price_success:
        return False, price_data
    
    price_info = price_data['prices'][0]
    is_sell = alert_data['action'] == 'SELL'
    price = float(price_info['bids'][0]['price']) if is_sell else float(price_info['asks'][0]['price'])
    
    units = round(trade_size / price, precision)
    
    # Enforce minimum order size
    if abs(units) < min_size:
        logger.warning(f"Order size {abs(units)} below minimum {min_size} for {instrument}")
        units = min_size if not is_sell else -min_size
    elif is_sell:
        units = -abs(units)
    
    order_data = {
        "order": {
            "type": alert_data['orderType'],
            "instrument": instrument,
            "units": str(units),
            "timeInForce": alert_data['timeInForce'],
            "positionFill": "DEFAULT"
        }
    }
    
    logger.info(f"Trade details: {instrument}, {'SELL' if is_sell else 'BUY'}, "
                f"Price={price}, Units={units}, Size=${trade_size}, "
                f"Precision={precision}")
                
    url = f"{OANDA_API_URL}/accounts/{alert_data['account']}/orders"
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        resp = requests.post(url, headers=headers, json=order_data, timeout=10)
        resp.raise_for_status()
        order_response = resp.json()
        logger.info(f"Trade executed: {order_response}")
        return True, order_response
    except Exception as e:
        error_msg = f"Trade execution failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

@app.route('/tradingview', methods=['POST'])
async def tradingview_webhook():
    logger.info("Webhook endpoint hit - beginning request processing")
    logger.info(f"Request headers: {dict(request.headers)}")
    
    try:
        raw_data = request.get_data()
        logger.info(f"Raw request data: {raw_data}")
        
        alert_data = request.get_json()
        logger.info(f"Parsed alert data: {alert_data}")
        
        if not alert_data:
            logger.error("No alert data received")
            return jsonify({"error": "No data received"}), 400

        handler = AlertHandler()
        success = await handler.process_alert(alert_data)
        
        if success:
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Alert processing failed after all retries"}), 503

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/tradingview', methods=['GET'])
def tradingview_test():
    logger.info("Test endpoint hit")
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat(),
        "message": "Tradingview endpoint is accessible",
        "config": {
            "api_url_set": bool(OANDA_API_URL),
            "api_token_set": bool(OANDA_API_TOKEN),
            "account_id_set": bool(OANDA_ACCOUNT_ID)
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    
    return jsonify({
        "status": "healthy",
        "timestamp": current_time.isoformat(),
        "config": {
            "api_url_set": bool(OANDA_API_URL),
            "api_token_set": bool(OANDA_API_TOKEN),
            "account_id_set": bool(OANDA_ACCOUNT_ID),
            "spread_thresholds": {
                "forex": SPREAD_THRESHOLD_FOREX,
                "crypto": SPREAD_THRESHOLD_CRYPTO
            }
        }
    })

if __name__ == '__main__':
    try:
        startup_time = datetime.now(timezone('Asia/Bangkok'))
        logger.info(f"Starting server at {startup_time.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")

        port = int(os.environ.get("PORT", 5000))
        logger.info(f"Starting server on port {port}")
        
        app.run(
            debug=DEBUG_MODE,
            host='0.0.0.0',
            port=port
        )
    except Exception as e:
        logger.error(f"Server startup failed: {str(e)}")
        raise
