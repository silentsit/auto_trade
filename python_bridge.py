# trading_bot.py

import os
import requests
import json
from flask import Flask, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
import time
import math
from datetime import datetime, timedelta
from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler
from collections import deque

# Constants
SPREAD_THRESHOLD_FOREX = 0.001  # 0.1% for forex
SPREAD_THRESHOLD_CRYPTO = 0.008  # 0.8% for crypto
MAX_QUEUE_SIZE = 1000
MAX_RETRIES = 3
RETRY_INTERVALS = [60, 300, 1500]  # 1 min, 5 mins, 25 mins

# Note: OANDA uses <BASE>_<QUOTE> format in the API, e.g. "EUR_USD", "USD_JPY".
#       For crypto, we match the code you already use: BTC_USD, ETH_USD, etc.
INSTRUMENT_LEVERAGES = {
    # ======================
    #          Forex
    # ======================
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

    # ======================
    #         Bonds
    # ======================
    # (OANDA typically uses their own naming for bonds; you can map them if needed)
    # Example placeholders (depends on actual OANDA symbols):
    "UK10Y_GILT": 5, "US5Y_TNOTE": 5, "US_TBOND": 5, "US10Y_TNOTE": 5,
    "BUND": 5, "US2Y_TNOTE": 5,

    # ======================
    #         Metals
    # ======================
    # (Again, real instrument names might vary on OANDA; adjust accordingly)
    "XAU_USD": 5,  # Gold
    "XAG_USD": 5,  # Silver
    # If you have more specific pairs like Gold/JPY, Gold/HKD, you'll need to map them as well.
    # For instance:
    # "XAU_HKD": 5, "XAU_JPY": 5, "XAU_CHF": 5, etc.

    # ======================
    #         Indices
    # ======================
    # Examples based on your list; match OANDA symbol naming if needed:
    "US_SPX_500": 20,
    "US_NAS_100": 20,
    "US_WALL_ST_30": 20,
    "UK_100": 20,
    "EUROPE_50": 20,
    "FRANCE_40": 20,
    "GERMANY_30": 20,
    "AUSTRALIA_200": 20,
    "US_RUSS_2000": 20,
    # Some are 5:1, e.g. "Switzerland 20" or "Spain 35"
    "SWITZERLAND_20": 5,
    "SPAIN_35": 5,
    "NETHERLANDS_25": 5,
    # And so on...

    # ======================
    #       Commodity
    # ======================
    # Adjust if you have exact OANDA names
    "SOYBEANS": 5,
    "COPPER": 5,
    "BRENT_CRUDE_OIL": 5,
    "PLATINUM": 5,
    "CORN": 5,
    "NATURAL_GAS": 5,
    "SUGAR": 5,
    "PALLADIUM": 5,
    "WHEAT": 5,
    "WTI_CRUDE_OIL": 5,

    # ======================
    #        Crypto
    # ======================
    # These should match your existing "BTC_USD", "ETH_USD", "LTC_USD", "XRP_USD" usage
    "BTC_USD": 2,
    "ETH_USD": 2,
    "LTC_USD": 2,
    "XRP_USD": 2,
    # If you want to trade Bitcoin Cash as "BCH_USD":
    "BCH_USD": 2,
}

def get_instrument_leverage(instrument: str) -> float:
    """
    Return the leverage for a given instrument. Defaults to 20:1 
    if instrument is not found in the dictionary above.
    """
    return INSTRUMENT_LEVERAGES.get(instrument, 20)

# Environment variables with defaults
OANDA_API_TOKEN = os.getenv('OANDA_API_TOKEN')
OANDA_API_URL = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Create necessary directories
os.makedirs('/opt/render/project/src/logs', exist_ok=True)

# Configure logging with 24-hour retention
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

# Initialize failed alerts queue
failed_alerts_queue = deque(maxlen=MAX_QUEUE_SIZE)

class RetryableAlert:
    def __init__(self, alert_data):
        self.alert_data = alert_data
        self.retry_count = 0
        self.timestamp = time.time()
        self.next_retry = self.timestamp
        self.created_at = datetime.now(timezone('Asia/Bangkok'))

    def should_retry(self):
        if self.retry_count >= MAX_RETRIES:
            return False
        if time.time() < self.next_retry:
            return False
        if time.time() - self.timestamp > 86400:  # 24 hours expiry
            return False
        return True

    def schedule_next_retry(self):
        if self.retry_count >= len(RETRY_INTERVALS):
            return None
        self.retry_count += 1
        delay = RETRY_INTERVALS[self.retry_count - 1]
        self.next_retry = time.time() + delay
        return self.next_retry

    def get_next_retry_time(self):
        if not self.should_retry():
            return None
        return datetime.fromtimestamp(self.next_retry).astimezone(timezone('Asia/Bangkok'))

# Core market functions

def is_market_open():
    """
    Determines if the market is open based on OANDA's 
    typical Fri 5 PM NY close and Sun 5 PM NY open, 
    converted approximately to Bangkok time (UTC+7).

    Closed:
      - Saturday from 5 AM onward,
      - All Sunday,
      - Monday before 5 AM.
    Open:
      - Monday 5 AM through Saturday 5 AM (24 hours each day).
    """
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()  # Monday=0, Tuesday=1, ... Sunday=6
    hour = current_time.hour

    # If it's Saturday (weekday=5) and hour >= 5  => closed
    # Or if it's Sunday (weekday=6) => closed
    # Or if it's Monday (weekday=0) and hour < 5  => still closed
    if (wday == 5 and hour >= 5) or (wday == 6) or (wday == 0 and hour < 5):
        return False, "Weekend market closure"

    # Otherwise => open
    return True, "Regular trading hours"


def calculate_next_market_open():
    """Calculate when market will next open (Bangkok time)"""
    current = datetime.now(timezone('Asia/Bangkok'))
    if current.weekday() == 5:  # Saturday
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

    # Determine if it's a crypto pair
    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    threshold = SPREAD_THRESHOLD_CRYPTO if is_crypto else SPREAD_THRESHOLD_FOREX

    if spread_percentage > threshold:
        logger.warning(f"Wide spread detected for {instrument}: {spread:.5f} ({spread_percentage*100:.2f}%)")
        return True, spread

    return False, 0

def get_instrument_price(instrument, account_id):
    """
    Fetch current pricing data from OANDA for the given instrument/account.
    Returns (True, pricing_data) on success, or (False, error_message) on failure.
    """
    # Ensure required configuration is present
    if not all([OANDA_API_TOKEN, OANDA_API_URL]):
        error_msg = "Missing required OANDA configuration"
        logger.error(error_msg)
        return False, error_msg

    try:
        headers = {
            "Authorization": f"Bearer {OANDA_API_TOKEN}",
            "Content-Type": "application/json"
        }
        base_url = OANDA_API_URL.rstrip('/')
        url = f"{base_url}/accounts/{account_id}/pricing?instruments={instrument}"

        logger.info(f"Fetching instrument price from OANDA: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raises HTTPError if status != 200

        pricing_data = response.json()

        if not pricing_data.get('prices'):
            error_msg = f"No pricing data returned for {instrument}"
            logger.warning(error_msg)
            return False, error_msg

        # If we get here, we have valid pricing data
        return True, pricing_data

    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching instrument price: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error fetching instrument price: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def check_market_status(instrument, account_id):
    """
    Enhanced market status check with trading hours and spread monitoring.
    1) Check if market is open (via is_market_open())
    2) Call get_instrument_price() for pricing
    3) Check for wide spreads (via check_spread_warning())
    4) Return (True, pricing_data) or (False, reason)
    """
    current_time = datetime.now(timezone('Asia/Bangkok'))
    logger.info(f"Checking market status at {current_time.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")

    # Validate required configuration
    if not all([OANDA_API_TOKEN, OANDA_API_URL]):
        error_msg = "Missing required OANDA configuration"
        logger.error(error_msg)
        return False, error_msg

    # (1) Check if the market is open
    is_open, reason = is_market_open()
    if not is_open:
        next_open = calculate_next_market_open()
        logger.info(f"Market is closed: {reason}. Next opening at {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
        return False, f"Market closed: {reason}. Opens {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time"

    # (2) Call get_instrument_price to fetch current quotes
    price_success, pricing_data = get_instrument_price(instrument, account_id)
    if not price_success:
        return False, pricing_data  # e.g. "No pricing data returned" or "Error fetching instrument price: ..."

    # (3) Check for wide spreads
    has_wide_spread, spread = check_spread_warning(pricing_data, instrument)
    if has_wide_spread:
        # Decide if you just log the warning or block trading entirely
        logger.warning(f"Wide spread warning for {instrument}: {spread}")
        # If you want to block trading when spreads are too wide, do this:
        return False, f"Spread too wide: {spread:.5f}"

    # If all is good, return True + the pricing data
    return True, pricing_data

# Instrument Settings
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

def execute_trade(alert_data):
    """Execute trade with OANDA"""
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    BASE_POSITION = 100000
    
    # Default values
    DEFAULT_FOREX_PRECISION = 5
    DEFAULT_CRYPTO_PRECISION = 2
    DEFAULT_MIN_ORDER_SIZE = 1000
    
    # Determine precision and min order size
    precision = INSTRUMENT_PRECISION.get(instrument)
    min_size = MIN_ORDER_SIZES.get(instrument)
    
    if precision is None:
        is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
        precision = DEFAULT_CRYPTO_PRECISION if is_crypto else DEFAULT_FOREX_PRECISION
        min_size = DEFAULT_MIN_ORDER_SIZE
        logger.warning(f"Using default settings for {instrument}: Precision={precision}, Min Size={min_size}")
    
    # Get the leverage for the instrument
    leverage = get_instrument_leverage(instrument)
    
    # Calculate trade size using the dynamic leverage
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
        units = min_size
    
    # Make units negative for sell orders
    if is_sell:
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

# Webhook handling and retry logic

def store_failed_alert(alert_data):
    """Store failed alert in memory queue"""
    retryable_alert = RetryableAlert(alert_data)
    failed_alerts_queue.append(retryable_alert)
    logger.info(f"Stored failed alert for retry: {alert_data}")

def process_single_alert(alert):
    """Process a single alert with proper error handling"""
    try:
        symbol = alert.alert_data.get('symbol')
        if not symbol:
            logger.error("No symbol in alert data")
            return False, "No symbol provided"

        instrument = symbol[:3] + "_" + symbol[3:] if len(symbol) == 6 else symbol

        is_tradeable, status_message = check_market_status(
            instrument,
            alert.alert_data.get('account', OANDA_ACCOUNT_ID)
        )

        if not is_tradeable:
            next_retry = alert.schedule_next_retry()
            if next_retry:
                logger.warning(f"Market not tradeable: {status_message}. Next retry at {datetime.fromtimestamp(next_retry)}")
            return False, status_message

        with app.test_request_context(json=alert.alert_data):
            response = tradingview_webhook()
            if response[1] != 200:
                raise Exception(f"Webhook returned status {response[1]}")
            
        logger.info(f"Successfully processed alert: {alert.alert_data}")
        return True, "Success"

    except Exception as e:
        error_msg = f"Failed to process alert: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def retry_failed_alerts():
    """Process failed alerts with exponential backoff"""
    if not failed_alerts_queue:
        return

    is_open, reason = is_market_open()
    if not is_open:
        next_open = calculate_next_market_open()
        logger.info(f"Skipping retry - {reason}. Next market open: {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
        return

    alerts_to_retry = len(failed_alerts_queue)
    logger.info(f"Processing {alerts_to_retry} failed alerts")

    for _ in range(alerts_to_retry):
        if not failed_alerts_queue:
            break
            
        alert = failed_alerts_queue.popleft()
        
        if not alert.should_retry():
            logger.info(f"Alert exceeded retry limit or expired: {alert.alert_data}")
            continue

        success, message = process_single_alert(alert)
        if not success and alert.should_retry():
            failed_alerts_queue.append(alert)
            next_retry = alert.get_next_retry_time()
            if next_retry:
                logger.info(f"Scheduled retry #{alert.retry_count} at {next_retry}")

@app.route('/tradingview', methods=['POST'])
def tradingview_webhook():
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

        symbol = alert_data.get('symbol')
        logger.info(f"Extracted symbol: {symbol}")
        
        if not symbol:
            logger.error("No symbol in alert data")
            return jsonify({"error": "No symbol provided"}), 400

        instrument = symbol[:3] + "_" + symbol[3:] if len(symbol) == 6 else symbol
        logger.info(f"Formatted instrument: {instrument}")
        
        is_tradeable, status_message = check_market_status(
            instrument,
            alert_data.get('account', OANDA_ACCOUNT_ID)
        )
        logger.info(f"Market status check result - tradeable: {is_tradeable}, message: {status_message}")

        if not is_tradeable:
            logger.warning(f"Market not tradeable, storing for retry. Status: {status_message}")
            store_failed_alert(alert_data)
            return jsonify({"error": status_message}), 503

        logger.info(f"Processing trading alert for {instrument}")
        
        success, trade_result = execute_trade(alert_data)
        if not success:
            return jsonify({"error": trade_result}), 503
        return jsonify({"status": "success", "trade": trade_result}), 200

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
    """Enhanced health check endpoint with detailed status"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    scheduler_status = "running" if scheduler.running else "stopped"
    
    # Get queue information
    queue_info = {
        "size": len(failed_alerts_queue),
        "max_size": MAX_QUEUE_SIZE,
    }
    
    # Add information about retry schedules if queue not empty
    if failed_alerts_queue:
        next_retries = [
            {
                "retry_count": alert.retry_count,
                "next_retry": alert.get_next_retry_time().isoformat() if alert.get_next_retry_time() else None,
                "created_at": alert.created_at.isoformat()
            }
            for alert in failed_alerts_queue
        ]
        queue_info["pending_retries"] = next_retries

    scheduler_jobs = scheduler.get_jobs()
    next_run = scheduler_jobs[0].next_run_time if scheduler_jobs else None
    
    return jsonify({
        "status": "healthy",
        "timestamp": current_time.isoformat(),
        "scheduler": {
            "status": scheduler_status,
            "jobs": len(scheduler_jobs),
            "next_run": next_run.isoformat() if next_run else None
        },
        "queue": queue_info,
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

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(retry_failed_alerts, 'interval', minutes=5)
scheduler.start()

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
    finally:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
