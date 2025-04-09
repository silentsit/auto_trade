# Python Trading Bridge

A high-performance bridge for connecting trading signals from platforms like TradingView to various brokers and trading platforms.

## Features

- FastAPI-based REST API for handling webhook alerts
- Comprehensive risk management system
- Position management with tracking
- Market analysis tools
- Support for various brokers (extensible)
- Redis-based data persistence (optional)
- Docker and docker-compose support for easy deployment

## Requirements

- Python 3.9+
- Docker and docker-compose (for containerized deployment)
- Redis (optional, for data persistence)

## Quick Start

### Using Docker (Recommended)

1. Clone this repository
2. Copy `.env.example` to `.env` and fill in your API credentials
3. Run with docker-compose:
   ```
   docker-compose up -d
   ```

### Manual Setup

1. Clone this repository
2. Create a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in your API credentials
5. Run the application:
   ```
   python fx_main.py
   ```

## Environment Variables

Create a `.env` file with the following variables:

```
# API Settings
HOST=0.0.0.0
PORT=10000
ENVIRONMENT=production  # production, development
ALLOWED_ORIGINS=*

# OANDA API Settings (if using OANDA)
OANDA_ACCOUNT_ID=your_account_id
OANDA_API_TOKEN=your_api_token
OANDA_API_URL=https://api-fxtrade.oanda.com/v3
OANDA_ENVIRONMENT=practice  # practice, live

# Redis Settings (optional)
REDIS_URL=redis://redis:6379/0

# Risk Management Settings
DEFAULT_RISK_PERCENTAGE=2.0
MAX_RISK_PERCENTAGE=5.0
MAX_DAILY_LOSS=20.0
MAX_PORTFOLIO_HEAT=15.0
```

## API Endpoints

### Health Check
- `GET /health`: Check API status

### Account Information
- `GET /api/account`: Get account information

### Positions
- `GET /api/positions`: Get open positions information

### Trading
- `POST /api/trade`: Execute a trade
- `POST /api/close`: Close a position
- `POST /api/calculate-size`: Calculate position size based on risk

### TradingView Webhook
- `POST /tradingview`: Receive and process TradingView alerts

## TradingView Alert Format

Set up your TradingView alerts with the following JSON format:

```json
{
  "symbol": "EURUSD",
  "action": "BUY",
  "risk": 1.0,
  "takeProfit": 1.1050,
  "stopLoss": 1.0950
}
```

## Architecture

This application follows a modular architecture with the following components:

1. **Error Handling**: Custom exceptions and error handling decorators
2. **Logging**: Structured JSON logging for better analysis
3. **Data Management**: Redis-based persistence with fallback to in-memory storage
4. **Position Management**: Track and manage open positions
5. **Market Analysis**: Price data handling and market analysis tools
6. **Risk Management**: Comprehensive risk management system
7. **Trade Execution**: Functions for executing trades with brokers
8. **API Layer**: FastAPI endpoints for external communication

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
