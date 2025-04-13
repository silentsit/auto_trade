#!/bin/bash

# Initialize environment variables
echo "Initializing application..."

# Create necessary directories if they don't exist
mkdir -p data/backtest_results
mkdir -p historical_data
mkdir -p reports

# Check if .env file exists and load it
if [ -f .env ]; then
    echo "Loading environment variables from .env file"
    set -a
    source .env
    set +a
elif [ -f .env.render ]; then
    echo "Loading environment variables from .env.render file"
    set -a
    source .env.render
    set +a
fi

# Start the application using uvicorn
if [ -z "$PORT" ]; then
    PORT=8000
    echo "PORT not set, defaulting to $PORT"
fi

echo "Starting application on port $PORT..."
uvicorn enhanced_trading:app --host 0.0.0.0 --port $PORT 