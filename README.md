# FX Trading Bot

A comprehensive trading system with dynamic configuration management, robust position tracking, and sophisticated risk management.

## Features

- Multi-stage take profit management
- Advanced loss management with dynamic stop loss adjustment
- Market structure analysis for trend detection
- Dynamic exit strategies
- Position tracking and risk management
- Circuit breaker for error handling
- Volatility monitoring
- Correlation analysis
- Time-based exit management

## Prerequisites

- Docker and Docker Compose
- API access to your preferred broker (Oanda configured by default)

## Quick Start

1. Clone this repository
2. Copy the `.env.example` file to `.env` and fill in your API credentials
3. Build and start the container:

```bash
docker-compose up -d
```

4. The API will be available at http://localhost:8000

## Configuration

Configure the trading bot by editing the `.env` file before starting the container. Key parameters include:

- `OANDA_ACCOUNT_ID` and `OANDA_API_TOKEN`: Your broker API credentials
- `MAX_DAILY_LOSS`: Maximum percentage of account that can be lost in a day
- `DEFAULT_RISK_PERCENTAGE`: Default risk per trade
- `ENABLE_*` settings: Toggle various advanced features

## API Endpoints

### Trading Endpoints

- GET `/health`: Check if the service is running
- GET `/api/account`: Get account information
- GET `/api/positions`: Get current positions
- POST `/api/trade`: Execute a trade
- POST `/api/close`: Close a position

### Analysis Endpoints

- GET `/api/market_info/{symbol}`: Get market information
- POST `/api/analysis/trade_timing`: Evaluate trade timing
- GET `/api/correlation/matrix`: Get correlation matrix

### Risk Management Endpoints

- POST `/api/risk/position_size`: Calculate optimal position size
- GET `/api/risk/portfolio_heat`: Get current portfolio heat

### Take Profit Management

- POST `/api/take_profits`: Set take profit levels
- GET `/api/take_profits/{position_id}`: Get take profit status

### Exit Management

- POST `/api/exits`: Initialize exit strategy for a position
- GET `/api/exits/{position_id}`: Get exit status

## Backtest Visualization

The FX Trading Bridge now includes a powerful backtest visualization dashboard built with Plotly and Dash. This dashboard provides interactive charts and performance metrics to analyze your trading strategies.

### Features:

- **Equity Curve Visualization**: Track account balance over time with drawdown analysis
- **Performance Metrics Dashboard**: View key metrics like total return, win rate, Sharpe ratio
- **Trade Distribution Analysis**: Analyze profit/loss distribution and cumulative PnL
- **Trade List**: Detailed table of all trades with entry/exit information

### Using the Dashboard:

1. Run a backtest using the API:
   ```
   POST /api/backtest/run
   ```

2. Launch the dashboard:
   ```
   GET /dashboard
   ```

3. Access the dashboard in your browser at http://localhost:8050 (or your server's address)

4. Select a backtest from the dropdown to visualize results

### Sample Data:

If no backtest results are available, the system will generate sample data to demonstrate the dashboard functionality.

## Deployment

For production use, consider setting up:

1. Persistent volumes for data and logs
2. Proper security measures (JWT authentication, HTTPS)
3. Monitoring via Prometheus (configure `ENABLE_PROMETHEUS=true`)

```bash
# Example production deployment
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## Development

To run in development mode:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
python enhanced_trading.py
```

## License

Proprietary and confidential 