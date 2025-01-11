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

# Create necessary directories at startup
os.makedirs('/opt/render/project/src/alerts', exist_ok=True)
os.makedirs('/opt/render/project/src/logs', exist_ok=True)

# Add log rotation configuration
log_file = os.path.join('/opt/render/project/src/logs', 'trading_bot.log')
max_bytes = 10 * 1024 * 1024  # 10MB
backup_count = 5  # Keep 5 backup files

# Configure logging
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

app = Flask(__name__)

# Functions

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

    is_open, reason = is_market_open()
    if not is_open:
        next_open = calculate_next_market_open()
        logger.info(f"Market is closed: {reason}. Next opening at {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time")
        return False, f"Market closed: {reason}. Opens {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time"

    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{account_id}/pricing?instruments={instrument}"

    try:
        resp = retry_request(requests.get, url=url, headers=headers, timeout=10)
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

    except Exception as e:
        logger.error(f"Error checking market status: {str(e)}")
        return False, f"Error checking market status: {str(e)}"

def retry_failed_alerts():
    """Retry failed alerts with market hours check"""
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
            if time.time() - alert['timestamp'] > 86400:
                logger.info(f"Alert expired, removing: {alert}")
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

            if alert['retry_count'] < 5:
                alert['retry_count'] += 1
                remaining_alerts.append(alert)

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
        app.run(
            debug=DEBUG_MODE,
            host='0.0.0.0',
            port=port,
            ssl_context=ssl_context
        )
    finally:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
