#
# app.py
#
# A Flask app to receive TradingView alerts via webhook
# and place trades on Oanda, with basic error handling (no secret key).
#
# Usage:
#   1. Put this file in your GitHub repo along with a `requirements.txt`.
#   2. Deploy to a hosting service (Render/Railway/etc.).
#   3. Set these environment variables:
#       OANDA_API_TOKEN: Your Oanda personal access token
#       OANDA_ACCOUNT_ID: Your Oanda account ID
#       OANDA_API_URL: (optional) default is "https://api-fxpractice.oanda.com/v3"
#   4. In TradingView alert settings:
#       Webhook URL: https://<YOUR_APP_URL>/tradingview
#       Message (JSON):
#         {
#           "action": "BUY",
#           "symbol": "EUR_USD",
#           "units": "1000"
#         }
#

import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Environment variables for Oanda config
OANDA_API_TOKEN   = os.environ.get("OANDA_API_TOKEN", "YOUR_OANDA_TOKEN_HERE")
OANDA_ACCOUNT_ID  = os.environ.get("OANDA_ACCOUNT_ID",  "YOUR_OANDA_ACCOUNT_ID")
OANDA_API_URL     = os.environ.get("OANDA_API_URL",     "https://api-fxpractice.oanda.com/v3")

@app.route('/')
def index():
    return "Hello! Your Oanda webhook server is running with error handling (no secret)."

@app.route('/tradingview', methods=['POST'])
def tradingview_webhook():
    """
    Main endpoint for TradingView to POST its alerts.
    We do:
      1. Check if the payload is valid JSON.
      2. Parse 'action', 'symbol', 'units' from the JSON.
      3. Place an order via Oanda's REST API.
    """

    # 1. Check JSON
    data = request.json
    if not data:
        return error_response("No JSON payload received.", 400)

    # 2. Extract action, symbol, units
    action = data.get("action", "").upper()
    symbol = data.get("symbol", "EUR_USD")  # fallback to "EUR_USD" if not provided
    units_str = data.get("units", "1000")   # default to "1000" if not provided

    # Basic validation
    if action not in ["BUY", "SELL"]:
        return error_response(f"Unrecognized action: {action}. Must be BUY or SELL.", 400)

    # Convert units to negative if SELL (and not already negative)
    if action == "SELL" and not units_str.startswith("-"):
        units_str = "-" + units_str

    print(f"Received alert: {data}")
    print(f"Placing order: {action} {units_str} on {symbol}")

    # 3. Attempt to place the Oanda trade
    status_code, resp_text, error_msg = place_oanda_trade(symbol, units_str)
    if error_msg:
        # If there's an internal error, we handle it here
        return error_response(f"Failed to place order on Oanda: {error_msg}", 500)

    # If everything went well:
    return jsonify({
        "status": "ok",
        "oanda_response_code": status_code,
        "oanda_response": resp_text
    }), 200


def place_oanda_trade(symbol, units_str):
    """
    Place a MARKET order on Oanda for 'units_str' (e.g. "1000" or "-1000").
    Returns: (status_code, resp_text, error_msg)
      - If error_msg is not None, something internal went wrong.
    """

    data = {
        "order": {
            "instrument": symbol,
            "units": units_str,        # e.g. "1000" for buy or "-1000" for sell
            "type": "MARKET",
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
    except requests.exceptions.RequestException as e:
        # This could happen if there's a network error, etc.
        return None, None, f"Request to Oanda failed: {str(e)}"

    status_code = resp.status_code
    resp_text   = resp.text

    # If Oanda responded with an error status code (not 2xx), note it
    if status_code < 200 or status_code >= 300:
        return status_code, resp_text, f"Oanda responded with status {status_code}"

    # Otherwise, success
    return status_code, resp_text, None


def error_response(message, http_status):
    """Helper to return a JSON error with a given message and HTTP status code."""
    return jsonify({
        "status": "error",
        "message": message
    }), http_status


if __name__ == '__main__':
    # For local testing only. 
    # In production, your hosting might run 'gunicorn app:app' or similar.
    app.run(debug=True, host='0.0.0.0', port=5000)
