# FX Trading Bot

An advanced algorithmic trading bot for forex, gold, and cryptocurrency trading with the OANDA API.

## Features

- Automated position management with dynamic risk controls
- Advanced stop loss and take profit strategies
- Position reconciliation and monitoring
- Market structure analysis
- Volatility-based position sizing
- API endpoints for manual and automated trading

## Setup

1. Clone this repository to your local machine
2. Create a `.env` file with the following environment variables:

```
OANDA_ACCOUNT_ID=your_oanda_account_id
OANDA_API_TOKEN=your_oanda_api_token
OANDA_API_URL=https://api-fxtrade.oanda.com/v3  # or practice API
OANDA_ENVIRONMENT=practice  # or 'live'
```

3. Install the required packages:

```
pip install -r requirements.txt
```

## Running the Application

Run the application using:

```
python python_bridge.py
```

Or for deployment on Render:

```
uvicorn python_bridge:app --host 0.0.0.0 --port 10000
```

## API Endpoints

- `/api/health` - Check server health
- `/api/account` - Get account information
- `/api/positions` - Get current positions
- `/api/market/{instrument}` - Get market data for an instrument
- `/api/alerts` - Process a trading alert (POST)
- `/api/trade` - Execute a trade (POST)
- `/api/close` - Close a position (POST)
- `/tradingview` - Webhook for TradingView alerts (POST)

## Troubleshooting

If you see errors like:
- `name 'get_open_positions' is not defined` - This has been fixed in the latest version
- `'float' object is not subscriptable'` - This has been fixed with improved error handling

## License

MIT License 