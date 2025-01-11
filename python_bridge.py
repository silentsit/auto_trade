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

[Your existing configuration code remains exactly the same until the logging setup]

# Modified logging setup to use the correct path
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join('/opt/render/project/src/logs', 'trading_bot.log'))
    ]
)
logger = logging.getLogger(__name__)

# Add these new functions before your routes
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
                    # Create a mock request context
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

# Modify your existing tradingview_webhook function
[Keep your existing function header and initial validation]

# Add this block after instrument validation but before processing BUY/SELL:
        if action in ["BUY", "SELL"]:
            # Check market status before proceeding
            is_tradeable, status_message = check_market_status(instrument, account_id)
            if not is_tradeable:
                logger.warning(f"Market status check failed for {instrument}: {status_message}")
                # Store the alert for retry
                store_failed_alert(request.get_json(), status_message)
                return jsonify({
                    "status": "warning",
                    "message": f"Market is currently unavailable: {status_message}. Alert has been stored for retry.",
                    "instrument": instrument,
                    "action": action
                }), 202

[Rest of your existing code remains the same until the end]

# Add this before your if __name__ == '__main__': block
# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(retry_failed_alerts, 'interval', minutes=5)
scheduler.start()

# Modify your if __name__ == '__main__': block
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
