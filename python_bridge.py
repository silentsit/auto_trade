import os
import requests
import json
from flask import Flask, request, jsonify
import logging
import time
import math
from apscheduler.schedulers.background import BackgroundScheduler

# Create necessary directories at startup
os.makedirs('/opt/render/project/src/alerts', exist_ok=True)
os.makedirs('/opt/render/project/src/logs', exist_ok=True)

app = Flask(__name__)

# --- Configuration ---
OANDA_API_TOKEN = os.environ.get("OANDA_API_TOKEN", "YOUR_OANDA_TOKEN_HERE")
OANDA_ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID")
OANDA_ENVIRONMENT = os.environ.get("OANDA_ENVIRONMENT", "practice")  # Default to practice
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
DEFAULT_LEVERAGE = 10  # Default leverage

# Updated precision settings and minimum sizes with additional instruments
INSTRUMENT_PRECISION = {
    "BTC_USD": 2,  # BTC/USD allows 2 decimal places for order size
    "ETH_USD": 2,  # ETH/USD allows 2 decimal places for order size
    "XAU_USD": 2,
    "EUR_USD": 4,
    "USD_JPY": 2,
    "GBP_USD": 4,
    "AUD_USD": 4,
    "NZD_USD": 4,
    "CAD_CHF": 4,
}

MIN_ORDER_SIZES = {
    "BTC_USD": 0.25,  # Minimum 0.25 units for BTC
    "ETH_USD": 1.0,   # Minimum 1.0 units for ETH
    "XAU_USD": 0.01,
    "EUR_USD": 1000,
    "USD_JPY": 1000,
    "GBP_USD": 1000,
    "AUD_USD": 1000,
    "NZD_USD": 1000,
    "CAD_CHF": 1000,
} 

if not OANDA_ACCOUNT_ID:
    raise ValueError("OANDA_ACCOUNT_ID must be set as an environment variable.")

if OANDA_ENVIRONMENT == "practice":
    OANDA_API_URL = "https://api-fxpractice.oanda.com/v3"
elif OANDA_ENVIRONMENT == "live":
    OANDA_API_URL = "https://api-fxtrade.oanda.com/v3"
else:
    raise ValueError(
        f"Invalid OANDA_ENVIRONMENT: {OANDA_ENVIRONMENT}. Must be 'practice' or 'live'."
    )

# --- Enhanced Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join('/opt/render/project/src/logs', 'trading_bot.log'))
    ]
)
logger = logging.getLogger(__name__)

def check_market_status(instrument, account_id):
    """Check if the market is currently tradeable"""
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
            
        price_data = pricing_data['prices'][0]
        tradeable = price_data.get('tradeable', False)
        status = price_data.get('status', 'unknown')
        trading_status = price_data.get('tradingStatus', 'unknown')
        
        logger.info(f"Market status for {instrument}: tradeable={tradeable}, status={status}, tradingStatus={trading_status}")
        
        if not tradeable or trading_status != 'TRADEABLE':
            return False, f"Market is not tradeable. Status: {status}, Trading Status: {trading_status}"
            
        return True, "Market is available for trading"
        
    except Exception as e:
        logger.error(f"Error checking market status: {str(e)}")
        return False, f"Error checking market status: {str(e)}"

def store_failed_alert(alert_data, reason):
    """Store failed alerts in a file for later retry"""
    failed_alert = {
        "timestamp": time.time(),
        "alert_data": alert_data,
        "reason": reason,
        "retry_count": 0
    }
    
    try:
        filepath = os.path.join('/opt/render/project/src/alerts', 'failed_alerts.json')
        
        # Store in a JSON file
        with open(filepath, 'a+') as f:
            f.write(json.dumps(failed_alert) + '\n')
        logger.info(f"Stored failed alert for later retry: {alert_data.get('symbol')}")
    except Exception as e:
        logger.error(f"Failed to store alert: {e}")

def retry_failed_alerts():
    """Retry failed alerts periodically"""
    filepath = os.path.join('/opt/render/project/src/alerts', 'failed_alerts.json')
    if not os.path.exists(filepath):
        return
    
    try:
        with open(filepath, 'r') as f:
            alerts = [json.loads(line) for line in f if line.strip()]
            
        processed_alerts = []
        remaining_alerts = []
        
        for alert in alerts:
            # Check if alert is older than 24 hours
            if time.time() - alert['timestamp'] > 86400:  # 24 hours
                logger.info(f"Alert expired, removing: {alert}")
                continue
                
            # Check if market is now available
            symbol = alert['alert_data'].get('symbol')
            if len(symbol) == 6:
                instrument = symbol[:3] + "_" + symbol[3:]
            else:
                instrument = symbol
                
            is_tradeable, _ = check_market_status(
                instrument, 
                alert['alert_data'].get('account', OANDA_ACCOUNT_ID)
            )
            
            if is_tradeable:
                # Process the alert
                try:
                    with app.test_request_context(json=alert['alert_data']):
                        response = tradingview_webhook()
                        if response[1] == 200:  # Check the status code
                            processed_alerts.append(alert)
                            logger.info(f"Successfully processed stored alert: {alert}")
                            continue
                except Exception as e:
                    logger.error(f"Failed to process stored alert: {e}")
            
            # If reached here, alert needs to be retried
            if alert['retry_count'] < 5:  # Max 5 retries
                alert['retry_count'] += 1
                remaining_alerts.append(alert)
        
        # Write remaining alerts back to file
        with open(filepath, 'w') as f:
            for alert in remaining_alerts:
                f.write(json.dumps(alert) + '\n')
                
    except Exception as e:
        logger.error(f"Error processing retry file: {e}")

[Rest of your existing functions remain the same: retry_request, get_oanda_account_summary, get_exchange_rate, calculate_units, place_oanda_trade, close_oanda_positions, error_response]

@app.route('/')
def index():
    return f"Hello! Your Oanda webhook server is running for the {OANDA_ENVIRONMENT} environment."

@app.route('/tradingview', methods=['POST'])
def tradingview_webhook():
    """
    Main endpoint for TradingView to POST its alerts.
    """
    # Log raw request data
    logger.info(f"Raw request data: {request.get_data()}")
    
    if not DEBUG_MODE and request.scheme != "https":
        return error_response("HTTPS is required in production.", 403)
    
    try:
        data = request.get_json()
        if not data:
            return error_response("No JSON payload received.", 400)
        
        # Enhanced logging for debugging
        logger.info(f"Received alert: {data}")
        logger.info(f"Symbol received: {data.get('symbol')}")
        logger.info(f"Raw symbol format check: len={len(data.get('symbol', ''))} contains_underscore={'_' in data.get('symbol', '')}")

        action = data.get("action", "").upper()
        symbol = data.get("symbol")
        account_id = data.get("account", OANDA_ACCOUNT_ID)
        percentage_str = data.get("percentage")
        order_type = data.get("orderType", "MARKET").upper()
        close_type = data.get("closeType", "ALL").upper()
        leverage = float(data.get("leverage", DEFAULT_LEVERAGE))

        # Basic validation
        if not action or action not in ["BUY", "SELL", "CLOSE"]:
            return error_response(f"Invalid 'action': {action}. Must be BUY, SELL, or CLOSE.", 400)
        if not symbol:
            return error_response("'symbol' field is missing.", 400)
        
        # Modified symbol format validation
        if len(symbol) == 6:
            # Convert 6-character format to Oanda format
            instrument = symbol[:3] + "_" + symbol[3:]
            logger.info(f"Converted 6-char symbol {symbol} to instrument format: {instrument}")
        elif "_" in symbol:
            instrument = symbol
            logger.info(f"Using provided instrument format: {instrument}")
        else:
            return error_response(f"Invalid symbol format: {symbol}. Expected 6 characters (e.g., ETHUSD) or instrument format (e.g., ETH_USD).", 400)

        logger.info(f"Final instrument format: {instrument}")

       if action in ["BUY", "SELL"]:
            # Add this market status check
            is_tradeable, status_message = check_market_status(instrument, account_id)
            if not is_tradeable:
                logger.warning(f"Market status check failed for {instrument}: {status_message}")
                return jsonify({
                    "status": "warning",
                    "message": f"Market is currently unavailable: {status_message}",
                    "instrument": instrument,
                    "action": action
                }), 202

            # Your existing BUY/SELL code continues here...
            [Your existing BUY/SELL code here]

        elif action == "CLOSE":
            # Your existing CLOSE logic remains the same
            [Your existing CLOSE code here]

    except Exception as e:
        logger.error(f"Unexpected error processing webhook: {e}", exc_info=True)
        return error_response("Internal server error.", 500)

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(retry_failed_alerts, 'interval', minutes=5)
scheduler.start()

if __name__ == '__main__':
    try:
        port = int(os.environ.get("PORT", 5000))
        app.run(
            debug=DEBUG_MODE,
            host='0.0.0.0',
            port=port,
            ssl_context=ssl_context
        )
    finally:
        scheduler.shutdown()
