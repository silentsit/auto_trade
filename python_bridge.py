import os
import requests
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# --- Configuration ---
OANDA_API_TOKEN = os.environ.get("OANDA_API_TOKEN", "YOUR_OANDA_TOKEN_HERE")
OANDA_ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID", "YOUR_OANDA_ACCOUNT_ID")
OANDA_ENVIRONMENT = os.environ.get("OANDA_ENVIRONMENT", "practice")  # Default to practice

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
    Handles:
      1. JSON validation
      2. Parsing 'action', 'symbol', 'percentage' (and optionally 'orderType', 'price')
      3. Placing an order via Oanda's REST API
      4. Logging
    """

    # 1. Check JSON
    try:
        data = request.get_json()
    except Exception as e:
        return error_response(f"Invalid JSON payload received: {e}", 400)

    if not data:
        return error_response("No JSON payload received.", 400)

    logger.info(f"Received alert: {data}")

    # 2. Extract data from JSON (with defaults and validation)
    action = data.get("action", "").upper()
    symbol = data.get("symbol")  # "{{ticker}}" from TradingView
    percentage_str = data.get("percentage")
    order_type = data.get("orderType", "MARKET").upper()
    #price = data.get("price") # Not used for MARKET orders

    # Essential validation
    if not action:
        return error_response("'action' field is missing or empty.", 400)
    if not symbol:
        return error_response("'symbol' field is missing.", 400)
    if not percentage_str:
        return error_response("'percentage' field is missing.", 400)
    if action not in ["BUY", "SELL"]:
        return error_response(f"Unrecognized action: {action}. Must be BUY or SELL.", 400)
    if order_type not in ["MARKET", "LIMIT"]: # Add more types if needed
        return error_response(f"Unsupported orderType: {order_type}. Must be MARKET or LIMIT.", 400)

    # Convert percentage to a number
    try:
        percentage = float(percentage_str)
        if not 0 < percentage <= 1:
            raise ValueError
    except ValueError:
        return error_response(f"Invalid 'percentage' value: {percentage_str}. Must be a number between 0 and 1.", 400)

    # 3. Get Oanda Account Balance and calculate units
    try:
        account_info = get_oanda_account_summary()
        account_balance = float(account_info['account']['balance'])
        currency = account_info['account']['currency']
        nav = float(account_info['account']['NAV'])
    except Exception as e:
        return error_response(f"Failed to retrieve account information: {e}", 500)

    logger.info(f"Account Balance: {account_balance} {currency}, NAV: {nav} {currency}")

    # Oanda API expects INSTRUMENT in the format: USD_JPY
    instrument = "_".join(symbol.split("USD")[::-1])

    try:
        exchange_rate = get_exchange_rate(instrument, currency)
    except Exception as e:
        return error_response(f"Failed to retrieve exchange rate for {instrument}: {e}", 500)

    units = calculate_units(nav, percentage, exchange_rate, action)

    logger.info(f"Calculated Units: {units}")

    # 4. Place Oanda Order
    status_code, resp_text, error_msg = place_oanda_trade(
        instrument, units, order_type
    )

    if error_msg:
        return error_response(f"Failed to place order on Oanda: {error_msg}", 500)

    logger.info(f"Oanda response: {status_code} - {resp_text}")

    return jsonify({
        "status": "ok",
        "message": f"Order placed successfully: {action} {units} units of {symbol}",
        "oanda_response_code": status_code,
        "oanda_response": resp_text
    }), 200

# --- Helper Functions ---
def get_oanda_account_summary():
    """Retrieves account summary from Oanda."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}/summary"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()  # Raise an exception for bad status codes
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting account summary: {e}")
        raise

def get_exchange_rate(instrument, currency):
    """Retrieves the exchange rate for the given instrument and currency from Oanda."""
    headers = {
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Content-Type": "application/json"
    }

    url = f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}/pricing?instruments={instrument}"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        pricing_data = resp.json()

        # Extract the bid and ask prices
        if pricing_data['prices']:
            bid = float(pricing_data['prices'][0]['bids'][0]['price'])
            ask = float(pricing_data['prices'][0]['asks'][0]['price'])
            # Calculate the average price
            exchange_rate = (bid + ask) / 2
        else:
            raise ValueError("Could not retrieve price for the specified instrument.")

        logger.info(f"Exchange Rate for {instrument}: {exchange_rate}")
        return exchange_rate

    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting exchange rate for {instrument}: {e}")
        raise
    except (KeyError, ValueError, IndexError) as e:
        logger.error(f"Error parsing exchange rate response: {e}")
        raise

def calculate_units(account_balance, percentage, exchange_rate, action):
    """Calculates the number of units based on account balance and percentage."""
    amount_to_trade = account_balance * percentage
    units = int(amount_to_trade * exchange_rate)
    return units if action == "BUY" else -units

def place_oanda_trade(symbol, units, order_type):
    """
    Places an order on Oanda.

    Args:
        symbol (str): The trading symbol (e.g., "EUR_USD").
        units (str): The number of units to trade.
        order_type (str): The type of order ('MARKET' or 'LIMIT').

    Returns:
        tuple: (status_code, resp_text, error_msg)
               - status_code: HTTP status code from Oanda's response.
               - resp_text: The raw response text from Oanda.
               - error_msg: An error message if something went wrong (None if successful).
    """

    data = {
        "order": {
            "instrument": symbol,
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

    url = f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}/orders"

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        resp.raise_for_status()
        return resp.status_code, resp.text, None  # Success

    except requests.exceptions.RequestException as e:
        return None, None, f"Request to Oanda failed: {e}"

def error_response(message, http_status):
    """Helper to return a JSON error with message and HTTP status code."""
    logger.error(f"Error: {message} (HTTP {http_status})")
    return jsonify({
        "status": "error",
        "message": message
    }), http_status

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
