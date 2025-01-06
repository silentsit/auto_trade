import os
import requests
from flask import Flask, request, jsonify
import logging
import time

app = Flask(__name__)

# --- Configuration ---
OANDA_API_TOKEN = os.environ.get("OANDA_API_TOKEN", "YOUR_OANDA_TOKEN_HERE")
OANDA_ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID")
OANDA_ENVIRONMENT = os.environ.get("OANDA_ENVIRONMENT", "practice")  # Default to practice
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
MAX_UNITS = 5  # Maximum allowed units (BTCUSD instrument specification)
DEFAULT_LEVERAGE = 10  # Default leverage
INSTRUMENT_PRECISION = {
    "BTC_USD": 3,  # Correct precision for BTC_USD
    "XAU_USD": 2,
    "EUR_USD": 4,
    # Add other instruments as needed
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

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Routes ---
@app.route('/')
def index():
    return f"Hello! Your Oanda webhook server is running for the {OANDA_ENVIRONMENT} environment."

@app.route('/tradingview', methods=['POST'])
def tradingview_webhook():
    """
    Main endpoint for TradingView to POST its alerts.
    """
    if not DEBUG_MODE and request.scheme != "https":
        return error_response("HTTPS is required in production.", 403)
    try:
        data = request.get_json()
        if not data:
            return error_response("No JSON payload received.", 400)
        logger.info(f"Received alert: {data}")

        action = data.get("action", "").upper()
        symbol = data.get("symbol")
        account_id = data.get("account", OANDA_ACCOUNT_ID)
        percentage_str = data.get("percentage")
        order_type = data.get("orderType", "MARKET").upper()
        leverage = float(data.get("leverage", DEFAULT_LEVERAGE))

        # Basic validation
        if not action or action not in ["BUY", "SELL"]:
            return error_response(f"Invalid 'action': {action}. Must be BUY or SELL.", 400)
        if not symbol:
            return error_response("'symbol' field is missing.", 400)

        # Convert symbol to Oanda instrument format
        instrument = symbol[:3] + "_" + symbol[3:] if len(symbol) == 6 else symbol
        logger.info(f"Instrument: {instrument}")

        # Further validation for BUY/SELL actions
        if not percentage_str:
            return error_response("'percentage' field is missing.", 400)
        try:
            percentage = float(percentage_str)
            if not 0 < percentage <= 1:
                raise ValueError
        except ValueError:
            return error_response(f"Invalid 'percentage' value: {percentage_str}. Must be between 0 and 1.", 400)

        # Get account and pricing info
        try:
            account_info = get_oanda_account_summary(account_id)
            nav = float(account_info['account']['NAV'])
            currency = account_info['account']['currency']
            exchange_rate = get_exchange_rate(instrument, currency, account_id)
        except Exception as e:
            return error_response(f"Failed to retrieve account or pricing information: {e}", 500)

        logger.info(f"Account NAV: {nav} {currency}, Exchange Rate: {exchange_rate}")

        # Calculate units
        try:
            units = calculate_units(nav, percentage, exchange_rate, action, instrument, leverage)
            if abs(units) > MAX_UNITS:
                return error_response(f"Calculated units ({units}) exceed maximum allowed ({MAX_UNITS}).", 400)
            logger.info(f"Calculated Units: {units}")
        except Exception as e:
            return error_response(f"Error calculating units: {e}", 500)

        # Place order
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

    except Exception as e:
        logger.error(f"Unexpected error processing webhook: {e}", exc_info=True)
        return error_response("Internal server error.", 500)

# --- Helper Functions ---

def get_oanda_account_summary(account_id):
    """Retrieves account summary from Oanda."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{account_id}/summary"
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting account summary: {e}")
        raise

def get_exchange_rate(instrument, currency, account_id):
    """Retrieves the exchange rate for the given instrument and currency from Oanda."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{account_id}/pricing?instruments={instrument}"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        pricing_data = resp.json()
        logger.info(f"Pricing data for {instrument}: {pricing_data}")

        if pricing_data['prices']:
            bid = float(pricing_data['prices'][0]['bids'][0]['price'])
            ask = float(pricing_data['prices'][0]['asks'][0]['price'])
            exchange_rate = (bid + ask) / 2
            return round(exchange_rate, 1)  # Round exchange rate to 1 decimal place
        else:
            raise ValueError("Could not retrieve price for the specified instrument.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting exchange rate for {instrument}: {e}")
        raise

def calculate_units(account_balance, percentage, exchange_rate, action, instrument, leverage):
    """
    Calculates the number of units based on account balance, percentage, exchange rate, and instrument type.

    Args:
        account_balance (float): The current account balance (NAV).
        percentage (float): The percentage of the account to trade (e.g., 0.25 for 25%).
        exchange_rate (float): The exchange rate of the instrument.
        action (str): The action being taken ('BUY' or 'SELL').
        instrument (str): The instrument being traded (e.g., 'EUR_USD', 'BTC_USD').
        leverage (float): The leverage to use for the trade.

    Returns:
        float: The number of units to trade, rounded to the appropriate precision.
    """
    amount_to_trade = account_balance * percentage
    logger.info(f"Amount to trade: {amount_to_trade}")

    # Validate exchange rate
    if exchange_rate == 0:
        raise ValueError(f"Exchange rate for {instrument} is zero, cannot calculate units.")

    precision = INSTRUMENT_PRECISION.get(instrument, 4)  # Default to 4 decimals if not found
    units = (amount_to_trade * leverage) / exchange_rate
    units = round(units, precision)
    logger.info(f"Units after rounding for {instrument}: {units}")

    return units if action == "BUY" else -units

def place_oanda_trade(instrument, units, order_type, account_id):
    """Places an order on Oanda."""
    data = {
        "order": {
            "instrument": instrument,
            "units": str(units),
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
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        resp.raise_for_status()
        return resp.status_code, resp.text, None
    except requests.exceptions.RequestException as e:
        try:
            error_message = resp.json().get("errorMessage", str(e))
        except ValueError:
            error_message = resp.text or str(e)
        return resp.status_code, resp.text, f"Request to Oanda failed: {error_message}"

def error_response(message, http_status):
    """Helper to return a JSON error with message and HTTP status code."""
    logger.error(f"Error: {message} (HTTP {http_status})")
    return jsonify({
        "status": "error",
        "message": message
    }), http_status

if __name__ == '__main__':
    app.run(debug=DEBUG_MODE, host='0.0.0.0', port=5000)
