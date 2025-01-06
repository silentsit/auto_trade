import os
import requests
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# --- Configuration ---
OANDA_API_TOKEN = os.environ.get("OANDA_API_TOKEN", "YOUR_OANDA_TOKEN_HERE")
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
    """
    try:
        data = request.get_json()
        if not data:
            return error_response("No JSON payload received.", 400)
        logger.info(f"Received alert: {data}")

        action = data.get("action", "").upper()
        symbol = data.get("symbol")
        account_id = data.get("account", OANDA_ACCOUNT_ID) # Default to main account if not provided
        percentage_str = data.get("percentage")
        order_type = data.get("orderType", "MARKET").upper()
        close_type = data.get("closeType", "ALL").upper()

        # Basic validation
        if not action or action not in ["BUY", "SELL", "CLOSE"]:
            return error_response(f"Invalid 'action': {action}. Must be BUY, SELL, or CLOSE.", 400)
        if not symbol:
            return error_response("'symbol' field is missing.", 400)

        # Convert symbol to Oanda instrument format
        instrument = symbol[:3] + "_" + symbol[3:] if len(symbol) == 6 else symbol
        logger.info(f"Instrument: {instrument}")

        if action in ["BUY", "SELL"]:
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
                units = calculate_units(nav, percentage, exchange_rate, action, instrument)
                logger.info(f"Calculated Units: {units}")
                if units == 0:
                    logger.info("Skipping order placement as calculated units is zero.")
                    return jsonify({
                        "status": "ok",
                        "message": "Order skipped. Calculated units is zero."
                    }), 200
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

        elif action == "CLOSE":
            # Validate close type
            if close_type not in ["ALL", "LONG", "SHORT"]:
                return error_response(f"Invalid closeType: {close_type}. Must be ALL, LONG, or SHORT.", 400)

            # Close positions
            status_code, resp_text, error_msg = close_oanda_positions(instrument, close_type, account_id)
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

# --- Helper Functions ---

def get_oanda_account_summary(account_id=OANDA_ACCOUNT_ID):
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

def get_exchange_rate(instrument, currency, account_id=OANDA_ACCOUNT_ID):
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

    if instrument == "BTC_USD":
        # Correctly calculate units for BTC/USD
        units = amount_to_trade / exchange_rate
        units = round(units, 4) # Adjust precision as needed for your strategy
    else:
        # For other instruments, keep the existing logic (or adjust as needed)
        units = int(amount_to_trade * exchange_rate)

    # Ensure units are positive for both BUY and SELL actions
    if units < 0:
        units = -units

    return units if action == "BUY" else -units

def place_oanda_trade(instrument, units, order_type, account_id=OANDA_ACCOUNT_ID):
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

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        resp.raise_for_status()
        return resp.status_code, resp.text, None
    except requests.exceptions.RequestException as e:
        try:
            error_message = resp.json().get("errorMessage", str(e))
        except:
            error_message = str(e)
        return resp.status_code, resp.text, f"Request to Oanda failed: {error_message}"

def close_oanda_positions(instrument, close_type="ALL", account_id=OANDA_ACCOUNT_ID):
    """Closes open positions for the given instrument on Oanda."""
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
            long_resp = requests.put(long_url, headers=headers, json=long_data, timeout=10)
            long_resp.raise_for_status()
            long_status_code = long_resp.status_code
            long_resp_text = long_resp.text
        except requests.exceptions.RequestException as e:
            return None, None, f"Request to close long positions on Oanda failed: {e}"

    if close_type in ["ALL", "SHORT"]:
        short_url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
        short_data = {"shortUnits": "ALL"}
        try:
            short_resp = requests.put(short_url, headers=headers, json=short_data, timeout=10)
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
