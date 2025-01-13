# Part 1: Core Functions and Setup

import os
import requests
import json
from flask import Flask, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime, timedelta
from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler

# Environment variables with defaults
OANDA_API_TOKEN = os.getenv('OANDA_API_TOKEN')
OANDA_API_URL = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')  # Default to live API URL
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Create necessary directories at startup
os.makedirs('/opt/render/project/src/alerts', exist_ok=True)
os.makedirs('/opt/render/project/src/logs', exist_ok=True)

# Configure logging
log_file = os.path.join('/opt/render/project/src/logs', 'trading_bot.log')
max_bytes = 10 * 1024 * 1024  # 10MB
backup_count = 5  # Keep 5 backup files

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

# Validate required environment variables
if not all([OANDA_API_TOKEN, OANDA_ACCOUNT_ID]):
    logger.error("Missing required environment variables. Please set OANDA_API_TOKEN and OANDA_ACCOUNT_ID")

app = Flask(__name__)

def is_market_open():
    """Check if it's during OANDA's crypto CFD trading hours (Bangkok time)"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday

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

def check_spread_warning(pricing_data):
    """Check if spreads are widening near market close"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    is_near_close = (
        current_time.weekday() == 4 and  # Friday
        2 <= current_time.hour <= 4  # 2 AM to 4 AM Bangkok time
    )

    if is_near_close and pricing_data.get('prices'):
        price = pricing_data['prices'][0]
        bid = float(price['bids'][0]['price'])
        ask = float(price['asks'][0]['price'])
        spread = ask - bid

        if spread > (bid * 0.001):  # More than 0.1% spread
            logger.warning(f"Wide spread detected near market close: {spread:.5f} ({(spread/bid)*100:.2f}%)")
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

    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Ensure URL is properly formatted
    base_url = OANDA_API_URL.rstrip('/')
    url = f"{base_url}/accounts/{account_id}/pricing?instruments={instrument}"
    
    logger.info(f"Requesting OANDA API: {url}")  # Log the complete URL (exclude token)

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        pricing_data = resp.json()

        if not pricing_data.get('prices'):
            logger.warning(f"No pricing data available for {instrument}")
            return False, "No pricing data available"

        has_wide_spread, spread = check_spread_warning(pricing_data)
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

# Part 2: Webhook and Retry Logic

@app.route('/tradingview', methods=['POST'])
def tradingview_webhook():
    logger.info("Webhook endpoint hit - beginning request processing")
    logger.info(f"Request headers: {dict(request.headers)}")
    
    try:
        # Log raw request data
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
        # Your trading logic here

        return jsonify({"status": "success", "message": f"Processed alert for {instrument}"}), 200

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

def store_failed_alert(alert_data):
    """Store failed alert for retry"""
    filepath = os.path.join('/opt/render/project/src/alerts', 'failed_alerts.json')
    alert = {
        'timestamp': time.time(),
        'retry_count': 0,
        'alert_data': alert_data
    }
    
    try:
        with open(filepath, 'a') as f:
            f.write(json.dumps(alert) + '\n')
        logger.info(f"Stored failed alert for retry: {alert}")
    except Exception as e:
        logger.error(f"Error storing failed alert: {e}")

def retry_failed_alerts():
    """Retry failed alerts only once"""
    is_open, reason = is_market_open()
    if not is_open:
        next_open = calculate_next_market_open()
        logger.info(f"Skipping retry - {reason}. Next market open: {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
        return

    filepath = os.path.join('/opt/render/project/src/alerts', 'failed_alerts.json')
    if not os.path.exists(filepath):
        return

    try:
        with open(filepath, 'r') as f:
            alerts = [json.loads(line) for line in f if line.strip()]

        processed_alerts = []
        remaining_alerts = []

        for alert in alerts:
            if time.time() - alert['timestamp'] > 86400:  # Remove alerts older than 24 hours
                logger.info(f"Alert expired, removing: {alert}")
                continue

            if alert['retry_count'] > 0:  # Skip if already retried once
                logger.info(f"Alert already retried once, removing: {alert}")
                continue

            symbol = alert['alert_data'].get('symbol')
            instrument = symbol[:3] + "_" + symbol[3:] if len(symbol) == 6 else symbol

            is_tradeable, status_message = check_market_status(
                instrument,
                alert['alert_data'].get('account', OANDA_ACCOUNT_ID)
            )

            if not is_tradeable:
                logger.warning(f"Market not tradeable: {status_message}")
                continue

            try:
                with app.test_request_context(json=alert['alert_data']):
                    response = tradingview_webhook()
                    if response[1] == 200:
                        processed_alerts.append(alert)
                        logger.info(f"Successfully processed stored alert: {alert}")
                        continue
            except Exception as e:
                logger.error(f"Failed to process stored alert: {e}")

            alert['retry_count'] += 1
            remaining_alerts.append(alert)

        # Write remaining alerts back to file
        with open(filepath, 'w') as f:
            for alert in remaining_alerts:
                f.write(json.dumps(alert) + '\n')

    except Exception as e:
        logger.error(f"Error processing retry file: {e}")

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
