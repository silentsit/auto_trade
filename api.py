from fastapi import APIRouter, Request, HTTPException, status
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os
from utils import logger

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

# --- Helper Functions ---
def get_alert_handler():
    """Get the alert handler instance"""
    global alert_handler
    if alert_handler is None:
        # Try to import from main
        try:
            from main import alert_handler as main_alert_handler
            alert_handler = main_alert_handler
        except ImportError:
            print("Could not import alert_handler from main")
            return None
    return alert_handler

def get_components():
    """Get all component instances"""
    components = {}
    global alert_handler, tracker, risk_manager, vol_monitor, regime_classifier
    global db_manager, backup_manager, error_recovery, notification_system, system_monitor
    
    # Try to get from globals first
    components['alert_handler'] = alert_handler
    components['tracker'] = tracker
    components['risk_manager'] = risk_manager
    components['vol_monitor'] = vol_monitor
    components['regime_classifier'] = regime_classifier
    components['db_manager'] = db_manager
    components['backup_manager'] = backup_manager
    components['error_recovery'] = error_recovery
    components['notification_system'] = notification_system
    components['system_monitor'] = system_monitor
    
    # If not available, try to import from main
    if not components['alert_handler']:
        try:
            import main
            components['alert_handler'] = main.alert_handler
            components['db_manager'] = main.db_manager
            components['backup_manager'] = main.backup_manager
            components['error_recovery'] = main.error_recovery
        except (ImportError, AttributeError):
            print("Could not import components from main module")
    
    return components

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
async def execute_trade_endpoint(trade: TradeRequest, request: Request):
    try:
        handler = get_alert_handler()
        result = await handler.process_alert(trade.dict())
        return {"status": "ok", "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/positions", tags=["positions"])
async def get_positions(status: Optional[str] = None, symbol: Optional[str] = None, limit: int = 100):
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
async def get_position(position_id: str):
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
async def close_position(position_id: str, request: Request):
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
async def get_risk_metrics():
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
async def get_market_regime(symbol: str):
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
async def get_volatility_state(symbol: str):
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
async def test_database_connection():
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
async def cleanup_positions():
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
async def tradingview_webhook(request: Request):
    """TradingView webhook endpoint - NO AUTHENTICATION"""
    try:
        # Log the incoming request
        client_ip = request.client.host if request.client else "unknown"
        logger.info(f"=== WEBHOOK RECEIVED FROM {client_ip} ===")
        
        # Get the raw body for debugging
        body = await request.body()
        logger.info(f"Raw webhook body: {body.decode('utf-8')[:500]}...")
        
        # Get the JSON data
        data = await request.json()
        logger.info(f"=== PARSED WEBHOOK DATA ===")
        logger.info(f"Keys received: {list(data.keys())}")
        for key, value in data.items():
            logger.info(f"  {key}: {value}")
        
        handler = get_alert_handler()
        if not handler:
            logger.error("Alert handler not available")
            raise HTTPException(status_code=503, detail="Alert handler not available")
            
        # Process the alert
        logger.info("=== PROCESSING ALERT ===")
        result = await handler.process_alert(data)
        logger.info(f"=== ALERT RESULT ===")
        logger.info(f"Result: {result}")
        
        return {"status": "ok", "result": result}
        
    except Exception as e:
        logger.error(f"=== WEBHOOK ERROR ===")
        logger.error(f"Error in TradingView webhook: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
