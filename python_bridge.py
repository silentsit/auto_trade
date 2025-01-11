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
OANDA_ENVIRONMENT = os.environ.get("OANDA_ENVIRONMENT", "practice")  
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
DEFAULT_LEVERAGE = 10

INSTRUMENT_PRECISION = {
    "BTC_USD": 2,
    "ETH_USD": 2,
    "XAU_USD": 2,
    "EUR_USD": 4,
    "USD_JPY": 2,
    "GBP_USD": 4,
    "AUD_USD": 4,
    "NZD_USD": 4,
    "CAD_CHF": 4,
}

MIN_ORDER_SIZES = {
    "BTC_USD": 0.25,
    "ETH_USD": 1.0,
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
    raise ValueError(f"Invalid OANDA_ENVIRONMENT: {OANDA_ENVIRONMENT}. Must be 'practice' or 'live'.")

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
        
        logger.info(f"Market status for {instrument}: tradeable={tradeable}, status={status}")
        
        if not tradeable:
            return False, f"Market is not tradeable. Status: {status}"
            
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
        with open(filepath, 'a+') as f:
            f.write(json.dumps(failed_alert) + '\n')
        logger.info(f"Stored failed alert for retry: {alert_data.get('symbol')}")
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
            if time.time() - alert['timestamp'] > 86400:  # 24 hours
                logger.info(f"Alert expired, removing: {alert}")
                continue
                
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

def retry_request(func, retries=3, backoff=2, *args, **kwargs):
    """Helper function to retry failed requests with exponential backoff."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                sleep_time = backoff ** attempt
                logger.warning(f"Request failed: {e}, retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                raise e

def get_oanda_account_summary(account_id):
    """Retrieves account summary from Oanda."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{account_id}/summary"
    try:
        resp = retry_request(requests.get, url=url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting account summary: {e}")
        raise

def get_exchange_rate(instrument, currency, account_id):
    """Retrieves the exchange rate for the given instrument."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{account_id}/pricing?instruments={instrument}"

    try:
        resp = retry_request(requests.get, url=url, headers=headers, timeout=10)
        resp.raise_for_status()
        pricing_data = resp.json()
        logger.info(f"Pricing data for {instrument}: {pricing_data}")

        if pricing_data['prices']:
            bid = float(pricing_data['prices'][0]['bids'][0]['price'])
            ask = float(pricing_data['prices'][0]['asks'][0]['price'])
            exchange_rate = (bid + ask) / 2
            return exchange_rate
        else:
            raise ValueError("Could not retrieve price for the specified instrument.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting exchange rate for {instrument}: {e}")
        raise
    except (KeyError, ValueError, IndexError) as e:
        logger.error(f"Error parsing exchange rate response: {e}")
        raise

def calculate_units(account_balance, percentage, exchange_rate, action, instrument):
    """Calculates the number of units based on account balance, percentage, and exchange rate."""
    amount_to_trade = account_balance * percentage
    logger.info(f"Amount to trade: {amount_to_trade}")

    units = amount_to_trade / exchange_rate
    precision = INSTRUMENT_PRECISION.get(instrument, 4)
    units = round(units, precision)
    
    logger.info(f"Units after rounding for {instrument}: {units}")
    return units if action == "BUY" else -units

def place_oanda_trade(instrument, units, order_type, account_id):
    """Places an order on Oanda."""
    precision = INSTRUMENT_PRECISION.get(instrument, 4)
    units = round(units, precision)
    units_str = f"{units:.{precision}f}"
    
    data = {
        "order": {
            "instrument": instrument,
            "units": units_str,
            "type": order_type,
            "timeInForce": "FOK",
            "positionFill": "DEFAULT"
        }
    }
    
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{account_id}/orders"
    logger.info(f"Placing order with data: {data}")

    try:
        resp = retry_request(requests.post, url=url, headers=headers, json=data, timeout=10)
        resp.raise_for_status()
        return resp.status_code, resp.text, None
    except requests.exceptions.RequestException as e:
        try:
            error_message = resp.json().get("errorMessage", str(e))
        except ValueError:
            error_message = resp.text or str(e)
        return getattr(resp, 'status_code', 500), getattr(resp, 'text', ''), f"Request to Oanda failed: {error_message}"

def close_oanda_positions(instrument, account_id, close_type="ALL"):
    """Closes open positions for the given instrument."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }

    long_status_code, short_status_code = -1, -1
    long_resp_text, short_resp_text = "", ""

    if close_type in ["ALL", "LONG"]:
        long_url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
        long_data = {"longUnits": "ALL"}
        try:
            long_resp = retry_request(requests.put, url=long_url, headers=headers, json=long_data, timeout=10)
            long_resp.raise_for_status()
            long_status_code = long_resp.status_code
            long_resp_text = long_resp.text
        except requests.exceptions.RequestException as e:
            return None, None, f"Request to close long positions on Oanda failed: {e}"

    if close_type in ["ALL", "SHORT"]:
        short_url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
        short_data = {"shortUnits": "ALL"}
        try:
            short_resp = retry_request(requests.put, url=short_url, headers=headers, json=short_data, timeout=10)
            short_resp.raise_for_status()
            short_status_code = short_resp.status_code
            short_resp_text = short_resp.text
        except requests.exceptions.RequestException as e:
            return None, None, f"Request to close short positions on Oanda failed: {e}"

    if long_status_code >= 400 or short_status_code >= 400:
        return (
            max(long_status_code, short_status_code),
            f"Long: {long_resp_text}, Short: {short_resp_text}",
            "Error closing positions. See response text for details.",
        )

    return (
        max(long_status_code, short_status_code),
        f"Long: {long_resp_text}, Short: {short_resp_text}",
        None,
    )

def error_response(message, http_status):
    """Helper to return a JSON error with message and HTTP status code."""
    logger.error(f"Error: {message} (HTTP {http_status})")
    return jsonify({
        "status": "error",
        "message": message
    }), http_status

@app.route('/')
def index():
    return f"Hello! Your Oanda webhook server is running for the {OANDA_ENVIRONMENT} environment."

@app.route('/tradingview', methods=['POST'])
def tradingview_webhook():
    """Main endpoint for TradingView to POST its alerts."""
    logger.info(f"Raw request data: {request.get_data()}")
    
    if not DEBUG_MODE and request.scheme != "https":
        return error_response("HTTPS is required in production.", 403)
    
    try:
        data = request.get_json()
        if not data:
            return error_response("No JSON payload received.", 400)
        
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

        if not action or action not in ["BUY", "SELL", "CLOSE"]:
            return error_response(f"Invalid 'action': {action}. Must be BUY, SELL, or CLOSE.", 400)
        if not symbol:
            return error_response("'symbol' field is missing.", 400)
        
        if len(symbol) == 6:
            instrument = symbol[:3] + "_" + symbol[3:]
            logger.info(f"Converted 6-char symbol {symbol} to instrument format: {instrument}")
        elif "_" in symbol:
            instrument = symbol
            logger.info(f"Using provided instrument format: {instrument}")
        else:
            return error_response(f"Invalid symbol format: {symbol}. Expected 6 characters (e.g., ETHUSD) or instrument format (e.g., ETH_USD).", 400)

        logger.info(f"Final instrument format: {instrument}")

        if action in ["BUY", "SELL"]:
            # Check market status before proceeding
            is_tradeable, status_message = check_market_status(instrument, account_id)
            if not is_tradeable:
                logger.warning(f"Market status check failed for {instrument}: {status_message}")
                store_failed_alert(request.get_json(), status_message)
                return jsonify({
                    "status": "warning",
                    "message": f"Market is currently unavailable: {status_message}. Alert has been stored for retry.",
                    "instrument": instrument,
                    "action": action
                }), 202

            if not percentage_str:
                return error_response("'percentage' field is missing.", 400)
            try:
                percentage = float(percentage_str)
                if not 0 < percentage <= 1:
                    raise ValueError
            except ValueError:
                return error_response(f"Invalid 'percentage' value: {percentage_str}. Must be between 0 and 1.", 400)

            try:
                account_info = get_oanda_account_summary(account_id)
                nav = float(account_info['account']['NAV'])
                currency = account_info['account']['currency']
                exchange_rate = get_exchange_rate(instrument, currency, account_id)
                logger.info(f"Account NAV: {nav} {currency}, Exchange Rate: {exchange_rate}")
            except Exception as e:
                logger.error(f"Failed to retrieve account or pricing information: {e}", exc_info=True)
                return error_response(f"Failed to retrieve account or pricing information: {e}", 500)

            try:
                units = calculate_units(nav, percentage, exchange_rate, action, instrument)
                logger.info(f"Calculated Units: {units}")
                
                min_size = MIN_ORDER_SIZES.get(instrument, 1000)
                if abs(units) < min_size:
                    return error_response(f"Calculated units ({units}) is below minimum size ({min_size}) for {instrument}", 400)
                
            except Exception as e:
                logger.error(f"Error calculating units: {e}", exc_info=True)
                return error_response(f"Error calculating units: {e}", 500)

            status_code, resp_text, error_msg = place_oanda_trade(instrument, units, order_type, account_id)
            if error_msg:
                return error_response(f"Failed to place order on Oanda: {error_msg}", 400 if status_code < 500 else 500)

            logger.info(f"Oanda response: {status_code} - {resp_text}")
            return jsonify({
                "status": "ok",
                "message": f"Order placed successfully: {action} {units} units of {symbol}",
                "oanda_response_code": status_code,
                "oanda_response": resp_text
            }), 200

        elif action == "CLOSE":
            status_code, resp_text, error_msg = close_oanda_positions(instrument, account_id, close_type)
            if error_msg:
                return error_response(f"Failed to close position on Oanda: {error_msg}", 500)

            logger.info(f"Oanda response: {status_code} - {resp_text}")
            return jsonify({
                "status": "ok",
                "message": f"Successfully closed {close_type} positions for {symbol}",
                "oanda_response_code": status_code,
                "oanda_response": resp_text
            }), 200

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
