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
    """Check if it's during OANDA's trading hours (Bangkok time)"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    current_weekday = current_time.weekday()

    if current_weekday == 5 or (current_weekday == 6 and current_time.hour < 4):
        return False, "Weekend market closure"

    if 0 <= current_weekday <= 4:
        return True, "Regular trading hours"

    if current_weekday == 6 and current_time.hour >= 4:
        return True, "Market open after weekend"

    return False, "Outside trading hours"

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

def check_market_status(instrument, account_id):
    """Enhanced market status check with trading hours and spread monitoring"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    logger.info(f"Checking market status at {current_time.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")

    # Validate required configuration
    if not all([OANDA_API_TOKEN, OANDA_API_URL]):
        error_msg = "Missing required OANDA configuration"
        logger.error(error_msg)
        return False, error_msg

    is_open, reason = is_market_open()
    if not is_open:
        next_open = calculate_next_market_open()
        logger.info(f"Market is closed: {reason}. Next opening at {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
        return False, f"Market closed: {reason}. Opens {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time"

    try:
        headers = {
            "Authorization": f"Bearer {OANDA_API_TOKEN}",
            "Content-Type": "application/json"
        }
        
        base_url = OANDA_API_URL.rstrip('/')
        url = f"{base_url}/accounts/{account_id}/pricing?instruments={instrument}"
        
        logger.info(f"Requesting OANDA API: {url}")

        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        pricing_data = resp.json()

        if not pricing_data.get('prices'):
            logger.warning(f"No pricing data available for {instrument}")
            return False, "No pricing data available"

        has_wide_spread, spread = check_spread_warning(pricing_data, instrument)
        if has_wide_spread:
            logger.warning(f"Wide spread warning for {instrument}: {spread}")

        price_data = pricing_data['prices'][0]
        tradeable = price_data.get('tradeable', False)
        status = price_data.get('status', 'unknown')

        logger.info(f"Market status for {instrument}: tradeable={tradeable}, status={status}")

        if not tradeable:
            return False, f"Market is not tradeable. Status: {status}"

        return True, "Market is available for trading"

    except requests.exceptions.RequestException as e:
        error_msg = f"Error checking market status: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error checking market status: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

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
    LEVERAGE = 20
    
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
    
    trade_size = BASE_POSITION * float(alert_data['percentage']) * LEVERAGE
    
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
