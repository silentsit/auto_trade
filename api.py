from fastapi import APIRouter, Request, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os

from fastapi import APIRouter, Request, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os

# Global references that will be set by main.py
alert_handler = None
tracker = None
risk_manager = None
vol_monitor = None
regime_classifier = None
db_manager = None
backup_manager = None
error_recovery = None
notification_system = None
system_monitor = None

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
    try:
        components = get_components()
        if components['alert_handler'] and hasattr(components['alert_handler'], 'system_monitor'):
            return await components['alert_handler'].system_monitor.get_system_status()
        else:
            return {"status": "ok", "message": "Basic status check"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/api/trade", tags=["trading"])
async def execute_trade_endpoint(trade: TradeRequest, request: Request, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        result = await handler.process_alert(trade.dict())
        return {"status": "ok", "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/positions", tags=["positions"])
async def get_positions(status: Optional[str] = None, symbol: Optional[str] = None, limit: int = 100, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Position tracker not available")
            
        if status == "open":
            positions = await handler.position_tracker.get_open_positions()
        elif status == "closed":
            positions = await handler.position_tracker.get_closed_positions(limit=limit)
        else:
            positions = await handler.position_tracker.get_all_positions()
        return {"positions": positions}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/positions/{position_id}", tags=["positions"])
async def get_position(position_id: str, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Position tracker not available")
            
        position = await handler.position_tracker.get_position_info(position_id)
        if not position:
            raise HTTPException(status_code=404, detail="Position not found")
        return position
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/positions/{position_id}/close", tags=["positions"])
async def close_position(position_id: str, request: Request, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Position tracker not available")
            
        data = await request.json()
        exit_price = data.get("exit_price")
        reason = data.get("reason", "manual")
        result = await handler.position_tracker.close_position(position_id, exit_price, reason)
        return {"status": "ok", "result": result.position_data if result.success else result.error}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/risk/metrics", tags=["risk"])
async def get_risk_metrics(auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.risk_manager:
            raise HTTPException(status_code=503, detail="Risk manager not available")
            
        metrics = await handler.risk_manager.get_risk_metrics()
        return metrics
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/market/regime/{symbol}", tags=["market"])
async def get_market_regime(symbol: str, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.regime_classifier:
            return {"symbol": symbol, "regime": "unknown", "message": "Regime classifier not available"}
            
        regime_data = handler.regime_classifier.get_regime_data(symbol)
        regime = regime_data.get("regime", "unknown")
        return {"symbol": symbol, "regime": regime, "data": regime_data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/market/volatility/{symbol}", tags=["market"])
async def get_volatility_state(symbol: str, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.volatility_monitor:
            return {"symbol": symbol, "volatility_state": "unknown", "message": "Volatility monitor not available"}
            
        state = handler.volatility_monitor.get_volatility_state(symbol)
        return {"symbol": symbol, "volatility_state": state}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/database/test", tags=["system"])
async def test_database_connection(auth=Depends(api_key_auth)):
    try:
        components = get_components()
        if not components['db_manager']:
            raise HTTPException(status_code=503, detail="Database manager not available")
            
        # Test basic connection
        await components['db_manager'].initialize()
        return {"status": "ok", "message": "Database connection successful"}
    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@router.post("/api/admin/cleanup-positions", tags=["admin"])
async def cleanup_positions(auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        if not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Position tracker not available")
            
        await handler.position_tracker.clean_up_duplicate_positions()
        return {"status": "ok", "message": "Position cleanup completed"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tradingview", tags=["webhook"])
async def tradingview_webhook(request: Request, auth=Depends(api_key_auth)):
    try:
        handler = get_alert_handler()
        data = await request.json()
        result = await handler.process_alert(data)
        return {"status": "ok", "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add more endpoints as needed, following this pattern. 
