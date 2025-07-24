# Trading Bot Startup Script for PowerShell
# This script handles dependencies and starts the bot correctly

Write-Host "🚀 Starting Trading Bot..." -ForegroundColor Green

# Check if we're in the right directory
if (-not (Test-Path "main.py")) {
    Write-Host "❌ main.py not found. Make sure you're in the correct directory." -ForegroundColor Red
    Write-Host "Current directory: $(Get-Location)" -ForegroundColor Yellow
    Write-Host "Expected directory: C:\Users\daryl\Downloads\auto_trade-main" -ForegroundColor Yellow
    exit 1
}

# Install missing dependencies if needed
Write-Host "📦 Checking dependencies..." -ForegroundColor Yellow
try {
    python -c "import fastapi, uvicorn, oandapyV20, requests, aiohttp" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "📥 Installing missing dependencies..." -ForegroundColor Yellow
        pip install fastapi uvicorn pydantic pydantic-settings oandapyV20 aiohttp asyncpg python-multipart requests PyJWT --upgrade
    } else {
        Write-Host "✅ All dependencies are installed." -ForegroundColor Green
    }
} catch {
    Write-Host "📥 Installing dependencies..." -ForegroundColor Yellow
    pip install fastapi uvicorn pydantic pydantic-settings oandapyV20 aiohttp asyncpg python-multipart requests PyJWT --upgrade
}

# Start the bot
Write-Host "🎯 Starting the trading bot on port 8000..." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop the bot" -ForegroundColor Yellow
Write-Host "API will be available at: http://localhost:8000/api/docs" -ForegroundColor Cyan
Write-Host "" 

try {
    python main.py
} catch {
    Write-Host "❌ Error starting bot: $_" -ForegroundColor Red
    Write-Host "Check the logs above for details." -ForegroundColor Yellow
} 