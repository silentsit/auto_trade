from fastapi import APIRouter, Request, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os

# Import modular components
from alert_handler import EnhancedAlertHandler
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from volatility_monitor import VolatilityMonitor
from regime_classifier import LorentzianDistanceClassifier
from database import PostgresDatabaseManager
from backup import BackupManager
from error_recovery import ErrorRecoverySystem
from notification import NotificationSystem
from system_monitor import SystemMonitor

# Instantiate components (or import from main if shared)
alert_handler = EnhancedAlertHandler()
tracker = PositionTracker()
risk_manager = EnhancedRiskManager()
vol_monitor = VolatilityMonitor()
regime_classifier = LorentzianDistanceClassifier()
db_manager = PostgresDatabaseManager()
backup_manager = BackupManager(db_manager=db_manager)
error_recovery = ErrorRecoverySystem()
notification_system = NotificationSystem()
system_monitor = SystemMonitor()

router = APIRouter()

# --- Simple API Key Auth Dependency ---
API_KEY = os.environ.get("API_KEY", "changeme")  # Set this in your environment!

def api_key_auth(request: Request):
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key.")

# --- Pydantic Models ---
class TradeRequest(BaseModel):
    symbol: str
    action: str  # "BUY" or "SELL"
    percentage: float
    timeframe: str = "1H"
    # Add more fields as needed

# --- Endpoints ---

@router.get("/api/status", tags=["system"])
async def get_status():
    # Example: return system/component status
    return await system_monitor.get_system_status()

@router.post("/api/trade", tags=["trading"])
async def execute_trade_endpoint(trade: TradeRequest, request: Request, auth=Depends(api_key_auth)):
    result = await alert_handler.process_alert(trade.dict())
    return {"status": "ok", "result": result}

@router.get("/api/positions", tags=["positions"])
async def get_positions(status: Optional[str] = None, symbol: Optional[str] = None, limit: int = 100, auth=Depends(api_key_auth)):
    if status == "open":
        positions = await tracker.get_open_positions()
    elif status == "closed":
        positions = await tracker.get_closed_positions(limit=limit)
    else:
        positions = await tracker.get_all_positions()
    return {"positions": positions}

@router.get("/api/positions/{position_id}", tags=["positions"])
async def get_position(position_id: str, auth=Depends(api_key_auth)):
    position = await tracker.get_position_info(position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    return position

@router.post("/api/positions/{position_id}/close", tags=["positions"])
async def close_position(position_id: str, request: Request, auth=Depends(api_key_auth)):
    data = await request.json()
    exit_price = data.get("exit_price")
    reason = data.get("reason", "manual")
    result = await tracker.close_position(position_id, exit_price, reason)
    return {"status": "ok", "result": result}

@router.get("/api/risk/metrics", tags=["risk"])
async def get_risk_metrics(auth=Depends(api_key_auth)):
    metrics = await risk_manager.get_risk_metrics()
    return metrics

@router.get("/api/market/regime/{symbol}", tags=["market"])
async def get_market_regime(symbol: str, auth=Depends(api_key_auth)):
    regime = regime_classifier.get_dominant_regime(symbol)
    return {"symbol": symbol, "regime": regime}

@router.get("/api/market/volatility/{symbol}", tags=["market"])
async def get_volatility_state(symbol: str, auth=Depends(api_key_auth)):
    state = vol_monitor.get_volatility_state(symbol)
    return {"symbol": symbol, "volatility_state": state}

@router.get("/api/database/test", tags=["system"])
async def test_database_connection(auth=Depends(api_key_auth)):
    try:
        await db_manager.initialize()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@router.post("/api/admin/cleanup-positions", tags=["admin"])
async def cleanup_positions(auth=Depends(api_key_auth)):
    result = await tracker.clean_up_duplicate_positions()
    return {"status": "ok", "result": result}

@router.post("/tradingview", tags=["webhook"])
async def tradingview_webhook(request: Request, auth=Depends(api_key_auth)):
    data = await request.json()
    result = await alert_handler.process_alert(data)
    return {"status": "ok", "result": result}

# Add more endpoints as needed, following this pattern. 