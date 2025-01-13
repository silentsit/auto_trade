# trading_bot.py

import os
import requests
import json
from flask import Flask, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
import time
import math
import asyncio
import aiohttp
from datetime import datetime, timedelta
from pytz import timezone
import uuid


# Constants
SPREAD_THRESHOLD_FOREX = 0.001  # 0.1% for forex
SPREAD_THRESHOLD_CRYPTO = 0.008  # 0.8% for crypto
MAX_RETRIES = 3
BASE_DELAY = 1.0  # Base delay in seconds

# All your existing INSTRUMENT_LEVERAGES dictionary here...
# (I notice it's quite long, so I'm assuming it stays the same)

# Environment variables with defaults
OANDA_API_TOKEN = os.getenv('OANDA_API_TOKEN')
OANDA_API_URL = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Create necessary directories and setup logging
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
                # Check market status first
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

                # Execute the trade
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

# Your existing market functions (is_market_open, calculate_next_market_open, 
# check_spread_warning, get_instrument_price, check_market_status) remain the same...

# Your existing INSTRUMENT_PRECISION and MIN_ORDER_SIZES dictionaries remain the same...

def execute_trade(alert_data):
    """Execute trade with OANDA"""
    # Your existing execute_trade function remains the same...

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

        # Initialize alert handler
        handler = AlertHandler()
        
        # Process the alert with retries
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
    # Your existing tradingview_test function remains the same...

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
