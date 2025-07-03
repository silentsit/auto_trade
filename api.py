from fastapi import APIRouter, Request, HTTPException, status, Body
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os
from utils import logger
import hmac
import hashlib
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Set
import jwt
from fastapi import HTTPException, Header, Depends
from pydantic import BaseModel

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
exit_monitor = None

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
        
        # *** NEW: Clear position from risk manager for opposing trade prevention ***
        if handler.risk_manager and result and result.success:
            try:
                await handler.risk_manager.clear_position(position_id)
                logger.info(f"Position {position_id} cleared from risk manager via API")
            except Exception as e:
                logger.error(f"Failed to clear position {position_id} from risk manager via API: {str(e)}")
        
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

@router.get("/api/risk/correlation", tags=["risk"])
async def get_correlation_metrics():
    """Get portfolio correlation metrics and currency exposure"""
    try:
        handler = get_alert_handler()
        if not handler.risk_manager:
            raise HTTPException(status_code=503, detail="Risk manager not available")
            
        correlation_metrics = await handler.risk_manager.get_correlation_metrics()
        return correlation_metrics
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/risk/correlation/{symbol}", tags=["risk"])
async def get_symbol_correlations(symbol: str, min_correlation: float = 0.5):
    """Get instruments correlated with the specified symbol"""
    try:
        handler = get_alert_handler()
        if not handler.risk_manager:
            raise HTTPException(status_code=503, detail="Risk manager not available")
            
        correlations = await handler.risk_manager.get_correlated_instruments(symbol, min_correlation)
        return {
            "symbol": symbol,
            "min_correlation": min_correlation,
            "correlated_instruments": [
                {"instrument": inst, "correlation": corr} for inst, corr in correlations
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/risk/correlation/breaches", tags=["risk"])
async def get_correlation_breaches():
    """Check for correlation limit breaches in current portfolio"""
    try:
        handler = get_alert_handler()
        if not handler.risk_manager:
            raise HTTPException(status_code=503, detail="Risk manager not available")
            
        breaches = await handler.risk_manager.check_correlation_breach()
        return {
            "breaches": breaches,
            "breach_count": len(breaches),
            "has_breaches": len(breaches) > 0
        }
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

# === DEBUG ENDPOINTS ===
@router.get("/debug/oanda-test", tags=["debug"])
async def debug_oanda_test():
    """Test OANDA connection and price retrieval."""
    handler = get_alert_handler()
    if not handler:
        return {"status": "error", "error": "Alert handler not available"}
    try:
        price = await handler.get_current_price("EUR_USD", "BUY")
        return {"status": "ok", "price": price}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.post("/debug/trade-path", tags=["debug"])
async def debug_trade_path(request: Request):
    """Test the full trade execution path with a sample payload."""
    handler = get_alert_handler()
    if not handler:
        return {"status": "error", "error": "Alert handler not available"}
    try:
        data = await request.json()
        result = await handler.process_alert(data)
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.post("/debug/force-close/{symbol}", tags=["debug"])
async def force_close_position(symbol: str):
    """Force close any open position for a symbol directly via OANDA"""
    try:
        from main import _close_position
        result = await _close_position(symbol)
        return {"status": "success", "result": result}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.post("/debug/test-close-signal", tags=["debug"])
async def test_close_signal(request: Request):
    """Test close signal processing with various TradingView formats"""
    handler = get_alert_handler()
    if not handler:
        return {"status": "error", "error": "Alert handler not available"}
    
    try:
        data = await request.json()
        
        # Log the original signal format
        logger.info(f"[DEBUG] Testing close signal with data: {data}")
        
        # If no data provided, test with common close signal formats
        if not data:
            test_formats = [
                {"message": "CLOSE_POSITION", "symbol": "EUR_USD"},
                {"action": "CLOSE", "symbol": "EUR_USD"},
                {"direction": "CLOSE", "symbol": "EUR_USD"},
                {"alertcondition": "Close Position Signal", "symbol": "EUR_USD"},
                {"side": "EXIT", "symbol": "EUR_USD"}
            ]
            
            results = []
            for i, test_data in enumerate(test_formats):
                try:
                    result = await handler.process_alert(test_data.copy())
                    results.append({
                        "test_case": i + 1,
                        "input": test_data,
                        "result": result,
                        "status": "processed"
                    })
                except Exception as e:
                    results.append({
                        "test_case": i + 1,
                        "input": test_data,
                        "error": str(e),
                        "status": "failed"
                    })
            
            return {"status": "ok", "test_results": results}
        else:
            # Test with provided data
            result = await handler.process_alert(data)
            return {"status": "ok", "input": data, "result": result}
            
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.post("/debug/cleanup-stale-positions", tags=["debug"])
async def cleanup_stale_positions():
    """Clean up stale positions that are causing reconciliation issues"""
    try:
        handler = get_alert_handler()
        if not handler or not handler.position_tracker:
            return {"status": "error", "error": "Handler or position tracker not available"}
        
        # Get all positions
        all_positions = await handler.position_tracker.get_all_positions()
        
        stale_positions = []
        cleaned_count = 0
        
        for symbol, positions in all_positions.items():
            for position_id, position_data in positions.items():
                # Check if position is older than 7 days and still marked as open
                if position_data.get("status") == "open":
                    from utils import parse_iso_datetime
                    from datetime import datetime, timezone, timedelta
                    
                    try:
                        open_time_str = position_data.get("open_time")
                        if open_time_str:
                            open_time = parse_iso_datetime(open_time_str)
                            age_days = (datetime.now(timezone.utc) - open_time).days
                            
                            if age_days > 7:  # Older than 7 days
                                stale_positions.append({
                                    "position_id": position_id,
                                    "symbol": symbol,
                                    "age_days": age_days,
                                    "open_time": open_time_str
                                })
                                
                                # Force close the stale position
                                current_price = await handler.get_current_price(symbol, "SELL" if position_data.get("action") == "BUY" else "BUY")
                                result = await handler.position_tracker.close_position(
                                    position_id, 
                                    current_price, 
                                    reason="debug_cleanup_stale"
                                )
                                
                                if result and hasattr(result, 'success') and result.success:
                                    cleaned_count += 1
                                    
                    except Exception as e:
                        logger.error(f"Error processing position {position_id}: {str(e)}")
        
        return {
            "status": "success",
            "stale_positions_found": len(stale_positions),
            "positions_cleaned": cleaned_count,
            "details": stale_positions
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.get("/debug/positions", tags=["debug"])
async def debug_positions():
    """Debug position tracking vs OANDA"""
    handler = get_alert_handler()
    if not handler:
        return {"error": "Alert handler not available"}
    
    try:
        # Check internal tracking
        internal_positions = await handler.get_open_positions()
        
        # Check OANDA directly
        from oandapyV20.endpoints.positions import OpenPositions
        from main import robust_oanda_request
        request = OpenPositions(accountID=config.oanda_account_id)
        response = await robust_oanda_request(request)
        
        oanda_positions = []
        if 'positions' in response:
            for pos in response['positions']:
                long_units = float(pos['long']['units'])
                short_units = float(pos['short']['units'])
                if long_units != 0 or short_units != 0:
                    oanda_positions.append({
                        'instrument': pos['instrument'],
                        'long_units': long_units,
                        'short_units': short_units
                    })
        
        return {
            "internal_tracking": internal_positions,
            "oanda_actual": oanda_positions,
            "mismatch": len(internal_positions) != len(oanda_positions)
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/weekend-positions", tags=["positions"])
async def get_weekend_positions():
    try:
        handler = get_alert_handler()
        if not handler.position_tracker:
            raise HTTPException(status_code=503, detail="Position tracker not available")
            
        weekend_summary = await handler.position_tracker.get_weekend_positions_summary()
        return {"status": "ok", "data": weekend_summary}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/weekend-positions/config", tags=["admin"])
async def get_weekend_position_config():
    try:
        return {
            "status": "ok",
            "config": {
                "weekend_position_max_age_hours": config.weekend_position_max_age_hours,
                "enable_weekend_position_limits": config.enable_weekend_position_limits,
                "weekend_position_check_interval": config.weekend_position_check_interval,
                "weekend_auto_close_buffer_hours": config.weekend_auto_close_buffer_hours
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/weekend-positions/close-all", tags=["admin"])
async def close_all_weekend_positions():
    try:
        handler = get_alert_handler()
        if not handler.position_tracker or not handler.health_checker:
            raise HTTPException(status_code=503, detail="Required services not available")
            
        # Get weekend positions
        weekend_summary = await handler.position_tracker.get_weekend_positions_summary()
        weekend_positions = weekend_summary.get('positions', [])
        
        closed_positions = []
        failed_positions = []
        
        for pos_info in weekend_positions:
            try:
                position_id = pos_info['position_id']
                symbol = pos_info['symbol']
                action = pos_info['action']
                
                # Get current price
                current_price = await handler.get_current_price(symbol, action)
                
                # Close position
                result = await handler.position_tracker.close_position(
                    position_id, 
                    current_price, 
                    reason="manual_weekend_closure"
                )
                
                if result and result.success:
                    # *** NEW: Clear position from risk manager for opposing trade prevention ***
                    if handler.risk_manager:
                        try:
                            await handler.risk_manager.clear_position(position_id)
                            logger.info(f"Weekend position {position_id} cleared from risk manager")
                        except Exception as e:
                            logger.error(f"Failed to clear weekend position {position_id} from risk manager: {str(e)}")
                    
                    closed_positions.append({
                        'position_id': position_id,
                        'symbol': symbol,
                        'weekend_age_hours': pos_info['weekend_age_hours']
                    })
                else:
                    failed_positions.append({
                        'position_id': position_id,
                        'symbol': symbol,
                        'error': result.error if result else 'Unknown error'
                    })
                    
            except Exception as e:
                failed_positions.append({
                    'position_id': pos_info.get('position_id', 'unknown'),
                    'symbol': pos_info.get('symbol', 'unknown'),
                    'error': str(e)
                })
        
        return {
            "status": "ok",
            "closed_positions": closed_positions,
            "failed_positions": failed_positions,
            "summary": {
                "total_weekend_positions": len(weekend_positions),
                "successfully_closed": len(closed_positions),
                "failed_to_close": len(failed_positions)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/exit-monitor", tags=["monitoring"])
async def get_exit_monitoring_report():
    """Get comprehensive exit signal monitoring report"""
    try:
        # Import the exit monitor
        try:
            from exit_monitor import exit_monitor
        except ImportError:
            raise HTTPException(status_code=503, detail="Exit monitor not available")
        
        if not config.enable_exit_signal_monitoring:
            return {
                "status": "disabled",
                "message": "Exit signal monitoring is disabled",
                "config": {
                    "enable_exit_signal_monitoring": False
                }
            }
        
        report = await exit_monitor.get_monitoring_report()
        
        # Add configuration information
        report["config"] = {
            "exit_signal_timeout_minutes": config.exit_signal_timeout_minutes,
            "max_exit_retries": config.max_exit_retries,
            "enable_emergency_exit_on_timeout": config.enable_emergency_exit_on_timeout,
            "exit_price_tolerance_pips": config.exit_price_tolerance_pips,
            "enable_exit_signal_debugging": config.enable_exit_signal_debugging
        }
        
        return {
            "status": "ok",
            "data": report,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/health", tags=["system"])
async def detailed_health_check():
    """Comprehensive health check including weekend positions"""
    try:
        handler = get_alert_handler()
        if not handler:
            return {"status": "error", "message": "Alert handler not available"}
        
        health_data = {
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "components": {},
            "weekend_positions": {}
        }
        
        # System monitor status
        if handler.system_monitor:
            components_status = await handler.system_monitor.get_all_component_status()
            health_data["components"] = components_status
        
        # Health checker status
        if hasattr(handler, 'health_checker') and handler.health_checker:
            try:
                system_health = await handler.health_checker.check_system_health()
                health_data["system_health"] = system_health
            except Exception as e:
                health_data["system_health"] = {"error": str(e)}
        
        # Weekend position status
        if handler.position_tracker:
            try:
                weekend_summary = await handler.position_tracker.get_weekend_positions_summary()
                health_data["weekend_positions"] = weekend_summary
                
                # Add weekend position alerts to overall status
                weekend_count = weekend_summary.get('weekend_positions_count', 0)
                if weekend_count > 0:
                    health_data["weekend_positions"]["alerts"] = []
                    
                    for pos in weekend_summary.get('positions', []):
                        age_hours = pos.get('weekend_age_hours', 0)
                        remaining_hours = config.weekend_position_max_age_hours - age_hours
                        
                        if remaining_hours <= config.weekend_auto_close_buffer_hours:
                            health_data["weekend_positions"]["alerts"].append({
                                "position_id": pos['position_id'],
                                "symbol": pos['symbol'],
                                "age_hours": age_hours,
                                "remaining_hours": remaining_hours,
                                "alert_level": "critical" if remaining_hours <= 0 else "warning"
                            })
            except Exception as e:
                health_data["weekend_positions"] = {"error": str(e)}
        
        # Determine overall status
        has_errors = False
        if "system_health" in health_data and health_data["system_health"].get("status") != "healthy":
            has_errors = True
        
        if health_data["weekend_positions"].get("alerts"):
            critical_alerts = [a for a in health_data["weekend_positions"]["alerts"] if a["alert_level"] == "critical"]
            if critical_alerts:
                has_errors = True
        
        if has_errors:
            health_data["status"] = "warning"
        
        return health_data
        
    except Exception as e:
        return {"status": "error", "message": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
