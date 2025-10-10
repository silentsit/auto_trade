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
                "position_tracker": "available" if hasattr(handler, 'position_tracker') and handler.position_tracker else "not_available",
                "oanda_service": "available" if hasattr(handler, 'oanda_service') and handler.oanda_service else "not_available"
            })
        else:
            health_status.update({
                "alert_handler": "not_initialized",
                "warning": "System components not ready"
            })
            
        return health_status

@router.get("/api/duplicate-stats", tags=["monitoring"])
async def get_duplicate_detection_stats():
    """
    Get real-time duplicate alert detection statistics.
    
    INSTITUTIONAL MONITORING: Track duplicate detection to ensure idempotency
    controls are working and prevent double execution.
    """
    try:
        handler = get_alert_handler()
        if not handler:
            raise HTTPException(
                status_code=503,
                detail="Alert handler not initialized"
            )
        
        stats = handler.get_duplicate_stats()
        
        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duplicate_detection_stats": stats,
            "health": {
                "duplicate_rate_acceptable": stats["duplicate_block_rate"] < 10.0,  # Alert if >10% duplicates
                "active_alerts_count": stats["active_alerts_count"]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting duplicate stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get duplicate stats: {str(e)}"
        )

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
        position_tracker_status = "available" if hasattr(handler, 'position_tracker') and handler.position_tracker else "not_available"
        
        status_data = {
            "status": "online",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "system_ready": bool(hasattr(handler, 'position_tracker') and handler.position_tracker),
            "components": {
                "alert_handler": "initialized",
                "position_tracker": position_tracker_status,
                "oanda_service": "available" if hasattr(handler, 'oanda_service') and handler.oanda_service else "not_available"
            }
        }
        
        # Add position count if tracker available
        if hasattr(handler, 'position_tracker') and handler.position_tracker:
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

@router.get("/api/status/components", tags=["system"])
async def get_component_status():
    """Get detailed component initialization status"""
    try:
        from main import C
        
        # Check each component
        components = {
            "storage": {
                "initialized": C.storage is not None,
                "type": "critical" if C.storage else "missing"
            },
            "oanda_service": {
                "initialized": C.oanda is not None,
                "type": "critical" if C.oanda else "missing"
            },
            "risk_manager": {
                "initialized": C.risk is not None,
                "type": "critical" if C.risk else "missing"
            },
            "position_tracker": {
                "initialized": C.tracker is not None,
                "type": "critical" if C.tracker else "missing"
            },
            "unified_analysis": {
                "initialized": C.analysis is not None,
                "type": "optional" if C.analysis else "missing"
            },
            "unified_exit_manager": {
                "initialized": C.exit_mgr is not None,
                "type": "optional" if C.exit_mgr else "missing"
            },
            "alert_handler": {
                "initialized": C.alerts is not None,
                "type": "critical" if C.alerts else "missing"
            },
            "health_monitor": {
                "initialized": C.monitor is not None,
                "type": "optional" if C.monitor else "missing"
            }
        }
        
        # Calculate system health
        critical_components = ["oanda_service", "risk_manager", "position_tracker", "alert_handler"]
        critical_ready = all(components[comp]["initialized"] for comp in critical_components)
        
        # Get additional health info if available
        health_info = {}
        if C.oanda and hasattr(C.oanda, 'get_connection_status'):
            try:
                health_info["oanda_connection"] = await C.oanda.get_connection_status()
            except:
                pass
        
        # Check if alert handler is in degraded mode
        alert_handler_status = {}
        if C.alerts and hasattr(C.alerts, 'get_status'):
            try:
                alert_handler_status = C.alerts.get_status()
            except Exception as e:
                alert_handler_status = {"error": str(e)}
        
        # Check if OANDA is operational (not in maintenance)
        oanda_operational = True
        if C.oanda and hasattr(C.oanda, 'is_operational'):
            oanda_operational = C.oanda.is_operational()
        elif C.oanda and hasattr(C.oanda, 'can_trade'):
            oanda_operational = C.oanda.can_trade()
        
        return {
            "system_operational": critical_ready,
            "oanda_operational": oanda_operational,
            "degraded_mode": not oanda_operational,
            "can_trade": oanda_operational,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "components": components,
            "health_info": health_info,
            "alert_handler_status": alert_handler_status,
            "critical_components_ready": critical_ready,
            "total_components": len(components),
            "initialized_components": sum(1 for comp in components.values() if comp["initialized"])
        }
        
    except Exception as e:
        logger.error(f"Error getting component status: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

# === POSITION MANAGEMENT ENDPOINTS ===

@router.get("/api/positions", tags=["positions"])
async def get_positions(status: Optional[str] = None, symbol: Optional[str] = None, limit: int = 100):
    try:
        handler = get_alert_handler()
        
        # FIX: Add robust null checks for position_tracker
        if not handler:
            raise HTTPException(status_code=503, detail="Alert handler not initialized")
            
        if not hasattr(handler, 'position_tracker') or not handler.position_tracker:
            logger.error("Position tracker is None - handler may not be properly started")
            raise HTTPException(status_code=503, detail="Position tracker not available - system initializing")
            
        if not hasattr(handler, '_started') or not handler._started:
            logger.error("Alert handler not started or missing _started attribute - call start() method first")
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
        
        if not handler or not hasattr(handler, 'position_tracker') or not handler.position_tracker:
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
        
        # Debug: Check if we're using shim vs real handler
        handler_type = type(handler).__name__
        is_shim = hasattr(handler, 'get_status') and handler.get_status().get('shim_mode', False)
        logger.info(f"🔍 Using handler type: {handler_type} (shim_mode: {is_shim})")
            
        # CRITICAL FIX: Enhanced started check with detailed diagnostics
        if not hasattr(handler, '_started'):
            logger.error("CRITICAL: Alert handler missing _started attribute completely")
            return {
                "status": "error", 
                "message": "Alert handler _started attribute missing - system initialization error"
            }
        
        if not handler._started:
            # Get detailed status for debugging
            status_info = "Unknown"
            if hasattr(handler, 'get_status'):
                try:
                    status_info = handler.get_status()
                except Exception as e:
                    logger.error(f"Error getting handler status: {e}")
            
            logger.error(f"Alert handler not started (_started={handler._started}). Status: {status_info}")
            return {
                "status": "error", 
                "message": f"System not ready - alert handler not started (_started={handler._started})",
                "debug_info": status_info
            }
        
        logger.debug(f"✅ API: Alert handler started check passed (_started={handler._started})")
        
        # Check if system is in degraded mode
        if hasattr(handler, 'degraded_mode') and handler.degraded_mode:
            logger.warning("🚨 Processing alert in DEGRADED MODE - OANDA service unavailable")
            # Still process the alert but queue it for later
            result = await handler.handle_alert(data)
            return {
                "status": "queued",
                "message": "Alert queued for processing when OANDA service is restored",
                "degraded_mode": True,
                "alert_id": data.get('alert_id', 'unknown'),
                "result": result
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
        
        if not handler or not hasattr(handler, 'position_tracker') or not handler.position_tracker:
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
        
        if not handler or not hasattr(handler, 'position_tracker') or not handler.position_tracker:
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
        if not handler or not hasattr(handler, 'position_tracker') or not handler.position_tracker:
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

@router.get("/correlation/status", tags=["monitoring"])
async def get_correlation_status():
    """Get dynamic correlation system status and health"""
    try:
        handler = get_alert_handler()
        if not handler or not handler.risk_manager:
            return {"status": "error", "message": "Risk manager not available"}
        
        correlation_manager = handler.risk_manager.correlation_manager
        
        # Get price data status
        price_status = await correlation_manager.get_price_data_status()
        
        # Get current positions for correlation analysis
        positions = await handler.position_tracker.get_all_positions()
        portfolio_metrics = await correlation_manager.get_portfolio_correlation_metrics(positions)
        
        return {
            "status": "success",
            "price_data_status": price_status,
            "portfolio_correlation_metrics": portfolio_metrics,
            "dynamic_correlation_enabled": True,
            "correlation_thresholds": {
                "high": "≥70%",
                "medium": "60-70%", 
                "low": "<60%"
            }
        }
    except Exception as e:
        logger.error(f"Error getting correlation status: {e}")
        return {"status": "error", "message": str(e)}

@router.get("/correlation/matrix", tags=["monitoring"])
async def get_correlation_matrix():
    """Get current correlation matrix for all tracked symbols"""
    try:
        handler = get_alert_handler()
        if not handler or not handler.risk_manager:
            return {"status": "error", "message": "Risk manager not available"}
        
        correlation_manager = handler.risk_manager.correlation_manager
        
        # Get all symbols with price data
        symbols = list(correlation_manager.price_history.keys())
        
        if len(symbols) < 2:
            return {
                "status": "success",
                "message": "Insufficient symbols for correlation matrix",
                "symbols": symbols
            }
        
        # Calculate correlation matrix
        correlation_matrix = await correlation_manager.get_correlation_matrix(symbols)
        
        return {
            "status": "success",
            "correlation_matrix": correlation_matrix,
            "symbols": symbols,
            "matrix_size": f"{len(symbols)}x{len(symbols)}",
            "total_pairs": len(symbols) * (len(symbols) - 1) // 2
        }
    except Exception as e:
        logger.error(f"Error getting correlation matrix: {e}")
        return {"status": "error", "message": str(e)}

@router.post("/correlation/update", tags=["monitoring"])
async def force_correlation_update():
    """Force immediate recalculation of all correlations"""
    try:
        handler = get_alert_handler()
        if not handler or not handler.risk_manager:
            return {"status": "error", "message": "Risk manager not available"}
        
        correlation_manager = handler.risk_manager.correlation_manager
        
        logger.info("🔄 Manual correlation update triggered via API")
        await correlation_manager.update_all_correlations(force=True)
        
        return {
            "status": "success",
            "message": "Correlation update completed",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error forcing correlation update: {e}")
        return {"status": "error", "message": str(e)}

# === MARKET HOURS AND OANDA RECONNECTION ENDPOINTS ===

@router.post("/api/force-reconnect", tags=["system"])
async def force_oanda_reconnect():
    """Force OANDA service reconnection"""
    try:
        from market_hours_fix import force_oanda_reconnection
        success = await force_oanda_reconnection()
        
        if success:
            return {
                "status": "success",
                "message": "OANDA reconnection successful",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            return {
                "status": "error",
                "message": "OANDA reconnection failed",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Force reconnect failed: {e}")
        return {
            "status": "error",
            "message": f"Force reconnect failed: {str(e)}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.get("/api/market-status", tags=["system"])
async def get_market_status():
    """Get current market and OANDA status"""
    try:
        from market_hours_fix import get_market_status
        status = await get_market_status()
        return status
    except Exception as e:
        logger.error(f"Error getting market status: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.post("/api/process-queued-alerts", tags=["system"])
async def process_queued_alerts():
    """Process any queued alerts from degraded mode"""
    try:
        handler = get_alert_handler()
        
        if not handler:
            return {
                "status": "error",
                "message": "Alert handler not available"
            }
        
        # Check if we have queued alerts
        if hasattr(handler, 'queued_alerts') and handler.queued_alerts:
            logger.info(f"Processing {len(handler.queued_alerts)} queued alerts...")
            
            # Process each queued alert
            processed_count = 0
            for alert in handler.queued_alerts[:]:  # Copy to avoid modification during iteration
                try:
                    # Process the alert normally
                    result = await handler.process_alert(alert)
                    if result.get('status') == 'success':
                        processed_count += 1
                        handler.queued_alerts.remove(alert)
                except Exception as e:
                    logger.error(f"Failed to process queued alert: {e}")
            
            return {
                "status": "success",
                "message": f"Processed {processed_count} queued alerts",
                "processed_count": processed_count,
                "remaining_queued": len(handler.queued_alerts)
            }
        else:
            return {
                "status": "success",
                "message": "No queued alerts to process",
                "processed_count": 0
            }
            
    except Exception as e:
        logger.error(f"Error processing queued alerts: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

# Export router for FastAPI app
__all__ = ["router", "set_alert_handler", "set_api_components"]
