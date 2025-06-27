@echo off
echo 🚀 Starting Trading Bot...
echo.

cd /d "C:\Users\daryl\Downloads\auto_trade-main"

echo 📦 Installing/updating dependencies...
pip install fastapi uvicorn pydantic pydantic-settings oandapyV20 aiohttp asyncpg python-multipart requests PyJWT --upgrade --quiet

echo.
echo 🎯 Starting the trading bot on port 8000...
echo Press Ctrl+C to stop the bot
echo API will be available at: http://localhost:8000/api/docs
echo.

python main.py

pause 