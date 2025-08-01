"""
INSTITUTIONAL TRADING BOT API
FastAPI endpoints with robust error handling and validation
"""

from fastapi import APIRouter, Request, HTTPException, status, Body
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os
import json
import logging
import hmac
import hashlib
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Set
import jwt
from fastapi import HTTPException, Header, Depends
from pydantic import BaseModel

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Router setup
router = APIRouter()

# Global alert handler reference
_alert_handler = None

def set_alert_handler(handler):
    """Set the global alert handler reference"""
    global _alert_handler
    _alert_handler = handler
    logger.info("✅ Alert handler reference set in API module")

def get_alert_handler():
    """Get the global alert handler with validation"""
    global _alert_handler
    if _alert_handler is None:
        logger.error("❌ Alert handler not set - system not properly initialized")
        return None
    return _alert_handler

# Pydantic models for request validation
class TradeRequest(BaseModel):
    symbol: str
    action: str  # BUY, SELL, CLOSE
    risk_percent: Optional[float] = 1.0
    timeframe: Optional[str] = "15"
    comment: Optional[str] = ""
    account: Optional[str] = None
    accounts: Optional[list] = None

class PositionResponse(BaseModel):
    position_id: str
    symbol: str
    side: str
    size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    created_at: datetime

class SystemStatusResponse(BaseModel):
    status: str
    uptime: str
    active_positions: int
    total_trades: int
    system_health: str

# === HEALTH CHECK ENDPOINTS ===

@router.get("/health", tags=["system"])
async def health_check():
    """System health check endpoint"""
    try:
        handler = get_alert_handler()
        
        # Basic health check
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0"
        }
        
        # Enhanced health check if handler is available
        if handler:
            health_status.update({
                "alert_handler": "initialized",
                "position_tracker": "available" if handler.position_tracker else "not_available",
                "oanda_service": "available" if hasattr(handler, 'oanda_service') else "not_available"
            })
        else:
            health_status.update({
                "alert_handler": "not_initialized",
                "warning": "System components not ready"
            })
            
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.get("/api/status", tags=["system"])
async def get_system_status():
    """Get comprehensive system status"""
    try:
        handler = get_alert_handler()
        
        if not handler:
            return {
                "status": "error",
                "message": "Alert handler not initialized",
                "system_ready": False
            }
            
        # Check if position tracker is available
        position_tracker_status = "available" if handler.position_tracker else "not_available"
        
        status_data = {
            "status": "online",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "system_ready": bool(handler.position_tracker),
            "components": {
                "alert_handler": "initialized",
                "position_tracker": position_tracker_status,
                "oanda_service": "available" if hasattr(handler, 'oanda_service') else "not_available"
            }
        }
        
        # Add position count if tracker available
        if handler.position_tracker:
            try:
                # Safe position count retrieval
                positions = await handler.position_tracker.get_all_positions()
                status_data["active_positions"] = len(positions) if positions else 0
            except Exception as e:
                logger.warning(f"Could not get position count: {e}")
                status_data["active_positions"] = "unknown"
        else:
            status_data["active_positions"] = 0
            status_data["warning"] = "Position tracker not available - system initializing"
            
        return status_data
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")

# === POSITION MANAGEMENT ENDPOINTS ===

@router.get("/api/positions", tags=["positions"])
async def get_positions(status: Optional[str] = None, symbol: Optional[str] = None, limit: int = 100):
    try:
        handler = get_alert_handler()
        
        # FIX: Add robust null checks for position_tracker
        if not handler:
            raise HTTPException(status_code=503, detail="Alert handler not initialized")
            
        if not handler.position_tracker:
            logger.error("Position tracker is None - handler may not be properly started")
            raise HTTPException(status_code=503, detail="Position tracker not available - system initializing")
            
        if not handler._started:
            logger.error("Alert handler not started - call start() method first")
            raise HTTPException(status_code=503, detail="System not started - please wait for initialization")
            
        # Safe position retrieval
        try:
            positions = await handler.position_tracker.get_all_positions()
            
            # Filter positions based on parameters
            filtered_positions = []
            for pos in positions or []:
                if status and pos.get('status') != status:
                    continue
                if symbol and pos.get('symbol') != symbol:
                    continue
                filtered_positions.append(pos)
                
            # Limit results
            limited_positions = filtered_positions[:limit]
            
            return {
                "positions": limited_positions,
                "total": len(filtered_positions),
                "limit": limit,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error retrieving positions: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to retrieve positions: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_positions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/api/positions/{position_id}", tags=["positions"])
async def get_position(position_id: str):
    """Get specific position by ID"""
    try:
        handler = get_alert_handler()
        
        if not handler or not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Position tracker not available")
            
        position = await handler.position_tracker.get_position_info(position_id)
        
        if not position:
            raise HTTPException(status_code=404, detail="Position not found")
            
        return {"position": position}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting position {position_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# === TRADING ENDPOINTS ===

@router.post("/tradingview", tags=["webhook"])
async def tradingview_webhook(request: Request):
    """TradingView webhook endpoint with enhanced error handling"""
    try:
        # Log the incoming request
        client_ip = request.client.host if request.client else "unknown"
        logger.info(f"=== WEBHOOK RECEIVED FROM {client_ip} ===")
        
        # Get the raw body for debugging
        body = await request.body()
        
        # FIX: Enhanced JSON parsing with robust extraction from mixed content
        try:
            # Try to decode as JSON
            if body:
                body_str = body.decode('utf-8')
                logger.info(f"Raw webhook body: {body_str[:500]}{'...' if len(body_str) > 500 else ''}")
                
                # First try direct JSON parsing
                try:
                    data = json.loads(body_str)
                except json.JSONDecodeError:
                    # If direct parsing fails, try to extract JSON from mixed content
                    logger.warning("Direct JSON parsing failed, attempting to extract JSON from mixed content")
                    
                    # Look for JSON object boundaries
                    json_start = body_str.find('{')
                    json_end = body_str.rfind('}')
                    
                    if json_start != -1 and json_end != -1 and json_end > json_start:
                        # Extract the JSON portion
                        json_portion = body_str[json_start:json_end + 1]
                        logger.info(f"Extracted JSON portion: {json_portion}")
                        
                        try:
                            data = json.loads(json_portion)
                            logger.info("✅ Successfully parsed JSON from mixed content")
                        except json.JSONDecodeError as e2:
                            logger.error(f"Failed to parse extracted JSON: {e2}")
                            raise e2
                    else:
                        logger.error("No valid JSON object found in body")
                        raise json.JSONDecodeError("No JSON object boundaries found", body_str, 0)
            else:
                logger.error("Empty request body received")
                raise HTTPException(status_code=400, detail="Empty request body")
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            logger.error(f"Raw body: {body.decode('utf-8', errors='replace')}")
            
            # Return error but don't crash the system
            return {
                "status": "error",
                "message": f"Invalid JSON format: {str(e)}",
                "received_data": body.decode('utf-8', errors='replace')[:200]
            }
        except UnicodeDecodeError as e:
            logger.error(f"Unicode decode error: {e}")
            return {
                "status": "error", 
                "message": "Invalid character encoding in request body"
            }
        
        logger.info(f"=== PARSED WEBHOOK DATA ===")
        logger.info(f"Keys received: {list(data.keys())}")
        for key, value in data.items():
            logger.info(f"  {key}: {value}")
        
        # Get alert handler with validation
        handler = get_alert_handler()
        if not handler:
            logger.error("Alert handler not available")
            return {
                "status": "error",
                "message": "System not ready - alert handler not initialized"
            }
            
        # FIX: Check if handler is properly started before processing
        if not handler._started:
            logger.error("Alert handler not started")
            return {
                "status": "error", 
                "message": "System initializing - please retry in a few moments"
            }
            
        logger.info("=== PROCESSING ALERT ON SECONDARY BOT ===")
        
        # Process the alert with enhanced error handling
        try:
            result = await handler.process_alert(data)
            logger.info("=== SECONDARY BOT ALERT RESULT ===")
            logger.info(f"Result: {result}")
            return result
            
        except AttributeError as e:
            if "'NoneType' object has no attribute" in str(e):
                logger.error(f"Component not initialized: {e}")
                return {
                    "status": "error",
                    "message": f"System component not ready: {str(e)}",
                    "alert_id": data.get('alert_id', 'unknown')
                }
            else:
                raise
        except Exception as e:
            logger.error(f"Error processing alert: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Internal error processing alert: {str(e)}",
                "alert_id": data.get('alert_id', 'unknown')
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook endpoint error: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Webhook processing failed: {str(e)}"
        }

@router.post("/api/trade", tags=["trading"])
async def execute_trade(trade_request: TradeRequest):
    """Manual trade execution endpoint"""
    try:
        handler = get_alert_handler()
        
        if not handler or not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Trading system not available")
            
        # Convert trade request to alert format
        alert_data = {
            "symbol": trade_request.symbol,
            "action": trade_request.action,
            "risk_percent": trade_request.risk_percent,
            "timeframe": trade_request.timeframe,
            "comment": trade_request.comment,
            "account": trade_request.account,
            "accounts": trade_request.accounts
        }
        
        result = await handler.process_alert(alert_data)
        return result
        
    except Exception as e:
        logger.error(f"Manual trade execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# === EMERGENCY ENDPOINTS ===

@router.post("/api/emergency/close-all", tags=["emergency"])
async def emergency_close_all():
    """Emergency close all positions"""
    try:
        handler = get_alert_handler()
        
        if not handler or not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Trading system not available")
            
        # Get all open positions
        positions = await handler.position_tracker.get_all_positions()
        
        results = []
        for position in positions or []:
            try:
                # Close each position
                result = await handler.close_position(position['position_id'])
                results.append({
                    "position_id": position['position_id'],
                    "symbol": position.get('symbol'),
                    "result": result
                })
            except Exception as e:
                results.append({
                    "position_id": position['position_id'],
                    "error": str(e)
                })
                
        return {
            "status": "completed",
            "message": f"Emergency close attempted for {len(positions or [])} positions",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Emergency close all failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# === UTILITY FUNCTIONS ===

def set_api_components():
    """Set API component references - called from main.py"""
    # This function is called from main.py after all components are initialized
    logger.info("✅ API components configured")

@router.get("/debug/open-positions", tags=["debug"])
async def debug_open_positions():
    """Debug endpoint to check open positions"""
    try:
        handler = get_alert_handler()
        if not handler or not handler.position_tracker:
            return {"status": "error", "message": "Position tracker not available"}
        
        open_positions = await handler.position_tracker.get_open_positions()
        
        # Format for easy reading
        formatted_positions = []
        for symbol, positions in open_positions.items():
            for pos_id, pos_data in positions.items():
                formatted_positions.append({
                    "symbol": symbol,
                    "position_id": pos_id,
                    "action": pos_data.get("action"),
                    "size": pos_data.get("size"),
                    "entry_price": pos_data.get("entry_price"),
                    "open_time": pos_data.get("open_time"),
                    "timeframe": pos_data.get("timeframe"),
                    "stop_loss": pos_data.get("stop_loss"),
                    "take_profit": pos_data.get("take_profit"),
                    "pnl": pos_data.get("pnl", 0),
                    "metadata": pos_data.get("metadata", {})
                })
        
        return {
            "status": "success",
            "open_positions_count": len(formatted_positions),
            "positions": formatted_positions
        }
    except Exception as e:
        logger.error(f"Error in debug_open_positions: {e}")
        return {"status": "error", "message": str(e)}

@router.post("/debug/test-close", tags=["debug"])
async def test_close_signal(symbol: str, position_id: Optional[str] = None):
    """Test endpoint to manually trigger a close signal"""
    try:
        handler = get_alert_handler()
        if not handler:
            return {"status": "error", "message": "Alert handler not available"}
        
        # Create a test close signal
        test_alert = {
            "symbol": symbol,
            "action": "CLOSE",
            "timeframe": "15",
            "comment": "Manual close test",
            "position_id": position_id
        }
        
        logger.info(f"🧪 Testing close signal for {symbol} (position_id: {position_id})")
        result = await handler.process_alert(test_alert)
        
        return {
            "status": "success",
            "test_result": result,
            "message": f"Close signal test completed for {symbol}"
        }
    except Exception as e:
        logger.error(f"Error in test_close_signal: {e}")
        return {"status": "error", "message": str(e)}

# Export router for FastAPI app
__all__ = ["router", "set_alert_handler", "set_api_components"]
