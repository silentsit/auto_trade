import asyncio
import aiohttp
import json
from datetime import datetime

# Change this to your local or deployed bot URL
BOT_URL = "http://localhost:8000/tradingview"  # or your deployed URL

def make_alert(accounts):
    return {
        "symbol": "EUR_USD",
        "action": "BUY",
        "alert_id": f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "position_id": f"EUR_USD_15_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
        "exchange": "OANDA",
        "accounts": accounts,
        "percentage": 10,
        "risk_percent": 10,
        "orderType": "MARKET",
        "timeInForce": "FOK",
        "comment": "Forwarding Test",
        "strategy": "Lorentzian_Classification",
        "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z",
        "timeframe": "15"
    }

async def test_alert(alert, description):
    print(f"\n=== {description} ===")
    print(f"POSTing: {json.dumps(alert, indent=2)}")
    async with aiohttp.ClientSession() as session:
        async with session.post(BOT_URL, json=alert) as resp:
            result = await resp.json()
            print(f"Response ({resp.status}): {json.dumps(result, indent=2)}")

async def main():
    # 1. 011 only
    alert_011 = make_alert(["101-003-26651494-011"])
    await test_alert(alert_011, "Account 011 only (should process locally, not forward)")

    # 2. 006 only
    alert_006 = make_alert(["101-003-26651494-006"])
    await test_alert(alert_006, "Account 006 only (should forward, not process locally)")

    # 3. Both 011 and 006
    alert_both = make_alert(["101-003-26651494-011", "101-003-26651494-006"])
    await test_alert(alert_both, "Both 011 and 006 (should process 011 locally, forward for 006)")

if __name__ == "__main__":
    asyncio.run(main()) 