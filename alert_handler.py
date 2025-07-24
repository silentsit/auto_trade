# file: alert_handler.py

import asyncio
import logging
import uuid
import time
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import wraps

from oandapyV20.exceptions import V20Error

from config import settings
from oanda_service import OandaService, MarketDataUnavailableError
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from technical_analysis import get_atr
from utils import (
    get_module_logger,
    format_symbol_for_oanda,
    calculate_position_size,
    get_instrument_leverage,
    TV_FIELD_MAP
)
from position_journal import position_journal
from crypto_signal_handler import crypto_handler

logger = get_module_logger(__name__)

def async_retry(max_retries: int = 3, delay: int = 5, backoff: int = 2):
    """A decorator for retrying an async function if it raises MarketDataUnavailableError."""
    def decorator(func: Callable[..., Awaitable[Any]]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            while attempt < max_retries:
                try:
                    return await func(*args, **kwargs)
                except MarketDataUnavailableError as e:
                    attempt += 1
                    if attempt >= max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} attempts. Final error: {e}")
                        return {"status": "error", "message": f"Operation failed after multiple retries: {e}"}
                    
                    logger.warning(
                        f"Attempt {attempt}/{max_retries} failed for {func.__name__}: {e}. "
                        f"Retrying in {current_delay} seconds..."
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

class AlertHandler:
    """
    Orchestrates the processing of trading alerts by coordinating with various services
    like the OANDA service, position tracker, and risk manager.
    """
    def __init__(
        self,
        oanda_service: OandaService,
        position_tracker: PositionTracker,
        db_manager,
        risk_manager: EnhancedRiskManager
    ):
        """Initializes the AlertHandler with all required components."""
        self.oanda_service = oanda_service 
        self.position_tracker = position_tracker
        self.risk_manager = risk_manager
        self.db_manager = db_manager
        self._lock = asyncio.Lock()
        self._started = False
        # INSTITUTIONAL FIX: Add idempotency controls
        self.active_alerts = set()  # Track active alert IDs
        self.alert_timeout = 300  # 5 minutes timeout for alert tracking
        logger.info("✅ AlertHandler initialized with all components.")

    async def start(self):
        """Starts the alert handler."""
        self._started = True
        logger.info("✅ AlertHandler started and ready to process alerts.")

    async def stop(self):
        """Stops the alert handler."""
        self._started = False
        logger.info("🛑 AlertHandler stopped.")

    def _standardize_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardizes the incoming alert data."""
        standardized_data = alert_data.copy()
        for tv_field, expected_field in TV_FIELD_MAP.items():
            if tv_field in standardized_data:
                standardized_data[expected_field] = standardized_data.pop(tv_field)
        
        if 'symbol' in standardized_data:
            # First check if it's crypto and format appropriately
            from crypto_signal_handler import crypto_handler
            if crypto_handler.is_crypto_signal(standardized_data['symbol']):
                from utils import format_crypto_symbol_for_oanda
                standardized_data['symbol'] = format_crypto_symbol_for_oanda(standardized_data['symbol'])
                logger.info(f"Crypto symbol detected and formatted: {standardized_data['symbol']}")
            else:
                standardized_data['symbol'] = format_symbol_for_oanda(standardized_data['symbol'])
            
        if 'action' in standardized_data:
            standardized_data['action'] = standardized_data['action'].upper()

        return standardized_data

    def _generate_alert_id(self, alert_data: Dict[str, Any]) -> str:
        """Generate a unique alert ID based on key parameters to prevent duplicates."""
        symbol = alert_data.get('symbol', '')
        action = alert_data.get('action', '')
        timeframe = alert_data.get('timeframe', '')
        # Use timestamp rounded to nearest minute to group alerts within same minute
        timestamp = int(time.time() // 60) * 60
        return f"{symbol}_{action}_{timeframe}_{timestamp}"

    async def _cleanup_expired_alerts(self):
        """Remove expired alerts from tracking set."""
        current_time = time.time()
        expired_alerts = set()
        for alert_id in self.active_alerts:
            # Extract timestamp from alert_id and check if expired
            try:
                timestamp_str = alert_id.split('_')[-1]
                alert_timestamp = int(timestamp_str)
                if current_time - alert_timestamp > self.alert_timeout:
                    expired_alerts.add(alert_id)
            except (ValueError, IndexError):
                # If we can't parse the timestamp, remove it anyway
                expired_alerts.add(alert_id)
        
        for alert_id in expired_alerts:
            self.active_alerts.discard(alert_id)
            logger.debug(f"Cleaned up expired alert: {alert_id}")
        
        # INSTITUTIONAL FIX: Clean up recent positions tracking
        await self._cleanup_recent_positions()

    async def _cleanup_recent_positions(self):
        """Remove expired recent positions from tracking."""
        if not hasattr(self, '_recent_positions'):
            return
            
        current_time = time.time()
        expired_positions = []
        
        for position_key, last_time in self._recent_positions.items():
            if current_time - last_time > 300:  # 5 minutes timeout
                expired_positions.append(position_key)
        
        for position_key in expired_positions:
            del self._recent_positions[position_key]
            logger.debug(f"Cleaned up expired recent position: {position_key}")

    @async_retry(max_retries=3, delay=5, backoff=2)
    async def process_alert(self, raw_alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for processing an alert with idempotency controls."""
        # Generate unique alert ID for duplicate detection
        alert_id = self._generate_alert_id(raw_alert_data)
        logger.info(f"--- Processing Alert ID: {alert_id} ---")
        
        # Clean up expired alerts
        await self._cleanup_expired_alerts()
        
        # INSTITUTIONAL FIX: Check for duplicate alerts
        if alert_id in self.active_alerts:
            logger.warning(f"Duplicate alert ignored: {alert_id}")
            return {
                "status": "ignored", 
                "message": "Duplicate alert detected", 
                "alert_id": alert_id
            }
        
        # Add to active alerts set
        self.active_alerts.add(alert_id)
        
        alert = self._standardize_alert(raw_alert_data)
        symbol = alert.get("symbol")
        
        if not self._started:
            logger.error("Cannot process alert: Handler is not started.")
            # Remove from active alerts if handler not started
            self.active_alerts.discard(alert_id)
            return {"status": "error", "message": "Handler not started"}

        try:
            async with self._lock:
                action = alert.get("action")
                
                if action in ["BUY", "SELL"]:
                    result = await self._handle_open_position(alert, alert_id)
                elif action == "CLOSE":
                    result = await self._handle_close_position(alert, alert_id)
                else:
                    logger.warning(f"Invalid action '{action}' in alert.")
                    result = {"status": "error", "message": f"Invalid action: {action}"}
                
                # Remove from active alerts on completion
                self.active_alerts.discard(alert_id)
                return result
                
        except Exception as e:
            # Remove from active alerts on error
            self.active_alerts.discard(alert_id)
            logger.error(f"Error processing alert {alert_id}: {e}", exc_info=True)
            return {"status": "error", "message": f"Processing error: {str(e)}"}

    async def _handle_open_position(self, alert: Dict[str, Any], alert_id: str) -> Dict[str, Any]:
        """Handles the logic for opening a new position."""
        symbol = alert.get("symbol")
        action = alert.get("action")
        # Robustly extract risk_percent from alert, supporting both 'risk_percent' and 'percentage' keys, and fallback to config
        risk_percent_raw = alert.get("risk_percent", alert.get("percentage", getattr(settings, 'max_risk_percentage', 20.0)))
        try:
            risk_percent = float(risk_percent_raw)
        except Exception as e:
            logger.error(f"Could not convert risk_percent value '{risk_percent_raw}' to float: {e}. Using default 5.0.")
            risk_percent = 5.0
        logger.info(f"[ALERT HANDLER] Using risk_percent={risk_percent}")
        atr = None  # Ensure atr is always defined
        
        # INSTITUTIONAL FIX: Position-level idempotency check
        position_key = f"{symbol}_{action}"
        current_time = time.time()
        
        # Check if we recently opened a position for this symbol/action
        if hasattr(self, '_recent_positions'):
            if position_key in self._recent_positions:
                last_time = self._recent_positions[position_key]
                if current_time - last_time < 60:  # 1 minute cooldown
                    logger.warning(f"Recent position detected for {position_key}, skipping duplicate trade")
                    return {
                        "status": "ignored", 
                        "message": f"Recent {action} position exists for {symbol}", 
                        "alert_id": alert_id
                    }
        else:
            self._recent_positions = {}
        
        try:
            is_allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage=risk_percent / 100.0, symbol=symbol)
            if not is_allowed:
                logger.warning(f"Trade rejected by Risk Manager: {reason}")
                return {"status": "rejected", "reason": reason, "alert_id": alert_id}

            account_balance = await self.oanda_service.get_account_balance()
            entry_price = await self.oanda_service.get_current_price(symbol, action)
            df = await self.oanda_service.get_historical_data(symbol, count=50, granularity="H1")
            
            if df is None or df.empty or account_balance is None or entry_price is None:
                logger.error("Failed to get required market data for trade.")
                raise MarketDataUnavailableError("Failed to fetch market data (price, balance, or history).")

            try:
                atr = get_atr(df)
                if not atr or not atr > 0:
                    logger.error(f"Invalid ATR value ({atr}) for {symbol}.")
                    raise MarketDataUnavailableError(f"Invalid ATR ({atr}) calculated for {symbol}.")
            except Exception as e:
                logger.error(f"Failed to calculate ATR: {e}")
                raise MarketDataUnavailableError("Failed to calculate ATR.")

            # INSTITUTIONAL FIX: Calculate stop loss first for consistent risk management
            # Use configurable ATR multiplier for better margin utilization
            atr_multiplier = getattr(settings.trading, 'atr_stop_loss_multiplier', 1.5)
            stop_loss_price = entry_price - (atr * atr_multiplier) if action == "BUY" else entry_price + (atr * atr_multiplier)
            
            leverage = get_instrument_leverage(symbol)
            # Pass actual stop loss to position sizing for accurate risk calculation
            position_size, sizing_info = await calculate_position_size(
                symbol, entry_price, risk_percent, account_balance, leverage, 
                stop_loss_price=stop_loss_price, timeframe="H1"
            )
            
            trade_payload = {
                "symbol": symbol, "action": action, "units": position_size, "stop_loss": stop_loss_price
            }
            success, result = await self.oanda_service.execute_trade(trade_payload)

            if success:
                # INSTITUTIONAL FIX: Record recent position to prevent duplicates
                self._recent_positions[position_key] = current_time
                
                position_id = f"{symbol}_{int(time.time())}"
                await self.position_tracker.record_position(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], stop_loss=stop_loss_price,
                    metadata={"alert_id": alert_id, "sizing_info": sizing_info, "transaction_id": result['transaction_id']}
                )
                await position_journal.record_entry(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], strategy=alert.get("strategy", "N/A"),
                    stop_loss=stop_loss_price
                )
                logger.info(f"✅ Successfully opened position {position_id} for {symbol}.")
                return {"status": "success", "position_id": position_id, "result": result}
            else:
                logger.error(f"Failed to execute trade: {result.get('error')}")
                return {"status": "error", "message": "Trade execution failed", "details": result}

        except MarketDataUnavailableError as e:
            logger.error(f"Market data unavailable for {symbol}, cannot open position. Error: {e}")
            raise  # Re-raise to allow the retry decorator to catch it
        except V20Error as e:
            logger.error(f"OANDA API error while opening position for {symbol}: {e}")
            return {"status": "error", "message": f"OANDA API Error: {e}"}
        except Exception as e:
            logger.error(f"Unhandled exception in _handle_open_position: {e}", exc_info=True)
            return {"status": "error", "message": "An internal error occurred."}

    async def _handle_close_position(self, alert: Dict[str, Any], alert_id: str) -> Dict[str, Any]:
        """Handles the logic for closing an existing position with profit ride override."""
        symbol = alert.get("symbol")
        
        try:
            position = await self.position_tracker.get_position_by_symbol(symbol)
            if not position:
                logger.warning(f"Received CLOSE signal for {symbol}, but no open position found.")
                return {"status": "ignored", "reason": "No open position found"}

            position_id = position['position_id']
            action_to_close = "SELL" if position['action'] == "BUY" else "BUY"

            # Get current market price for override evaluation
            current_price = await self.oanda_service.get_current_price(symbol, action_to_close)
            if not current_price:
                logger.error(f"Could not get current price for {symbol}.")
                raise MarketDataUnavailableError(f"Could not get current price for {symbol}")

            # ===== PROFIT RIDE OVERRIDE LOGIC =====
            # Step 2: Determine whether to execute close or override based on conditions
            try:
                from services_x.profit_ride_override import ProfitRideOverride, OverrideDecision
                from regime_classifier import LorentzianDistanceClassifier
                from volatility_monitor import VolatilityMonitor
                
                # Initialize override components (you may want to make these class attributes)
                regime_classifier = LorentzianDistanceClassifier()
                volatility_monitor = VolatilityMonitor()
                override_manager = ProfitRideOverride(regime_classifier, volatility_monitor)
                
                # Create position object for override evaluation
                from dataclasses import dataclass
                @dataclass
                class PositionForOverride:
                    symbol: str
                    action: str
                    entry_price: float
                    size: float
                    timeframe: str
                    stop_loss: float = None
                    metadata: dict = None
                    
                    def __post_init__(self):
                        if self.metadata is None:
                            self.metadata = {}
                
                # Convert position to override-compatible format
                position_obj = PositionForOverride(
                    symbol=symbol,
                    action=position['action'],
                    entry_price=position['entry_price'],
                    size=position['size'],
                    timeframe=alert.get('timeframe', '15'),  # Default to 15m if not provided
                    stop_loss=position.get('stop_loss'),
                    metadata=position.get('metadata', {})
                )
                
                # Calculate current account balance for drawdown check
                account_balance = await self.oanda_service.get_account_balance()
                
                # Evaluate override conditions
                override_decision = await override_manager.should_override(
                    position_obj, 
                    current_price, 
                    drawdown=0.0  # You may want to calculate actual drawdown
                )
                
                # Step 3: If conditions are met, let it continue running
                if override_decision.ignore_close:
                    logger.info(f"🚀 PROFIT RIDE OVERRIDE ACTIVE for {symbol}: {override_decision.reason}")
                    
                    # === INSTITUTIONAL TIERED TP LOGIC ON OVERRIDE ===
                    # 1. Get latest ATR
                    timeframe = position_obj.timeframe
                    df = await self.oanda_service.get_historical_data(symbol, count=50, granularity=f"{timeframe.upper()}")
                    atr = get_atr(df)
                    current_price = float(current_price)
                    
                    # 2. Determine RR ratio
                    if timeframe in ["15", "15m", "M15"]:
                        rr_ratio = 1.5
                    else:
                        rr_ratio = 2.0
                    
                    # === STORE ORIGINAL TRIGGER DATA FOR MIGRATION ===
                    import datetime
                    position['metadata'] = position.get('metadata', {})
                    position['metadata']['profit_ride_override_fired'] = True
                    position['metadata']['override_trigger_data'] = {
                        'trigger_price': current_price,
                        'trigger_atr': atr,
                        'trigger_timestamp': datetime.datetime.utcnow().isoformat(),
                        'trigger_rr_ratio': rr_ratio,
                        'trigger_timeframe': timeframe
                    }
                    
                    # === TIERED TP IMPLEMENTATION ===
                    if override_decision.tiered_tp_levels:
                        # Store tiered TP levels in metadata
                        tiered_tp_data = []
                        for level in override_decision.tiered_tp_levels:
                            tiered_tp_data.append({
                                'level': level.level,
                                'atr_multiple': level.atr_multiple,
                                'percentage': level.percentage,
                                'price': level.price,
                                'units': level.units,
                                'triggered': level.triggered,
                                'closed_at': level.closed_at.isoformat() if level.closed_at else None
                            })
                        
                        position['metadata']['tiered_tp_levels'] = tiered_tp_data
                        
                        logger.info(f"🎯 TIERED TP SETUP for {symbol}:")
                        for level in override_decision.tiered_tp_levels:
                            logger.info(f"   Level {level.level}: {level.units} units at {level.price:.5f} ({level.percentage*100:.0f}%)")
                    
                    # 3. Calculate new SL (keep original TP logic for now, tiered TP will be handled separately)
                    if position_obj.action == "BUY":
                        new_sl = current_price - (atr * override_decision.sl_atr_multiple)
                        # Set a far TP as backup (tiered TP will handle actual exits)
                        new_tp = current_price + (atr * 10.0)  # Very far TP as backup
                    else:
                        new_sl = current_price + (atr * override_decision.sl_atr_multiple)
                        # Set a far TP as backup (tiered TP will handle actual exits)
                        new_tp = current_price - (atr * 10.0)  # Very far TP as backup
                    
                    # 4. Update OANDA SL/TP (requires trade_id from metadata)
                    trade_id = position['metadata'].get('transaction_id')
                    if trade_id:
                        await self.oanda_service.modify_position(trade_id, stop_loss=new_sl, take_profit=new_tp)
                        logger.info(f"Updated SL/TP for trade {trade_id}: SL={new_sl}, TP={new_tp}")
                    else:
                        logger.warning(f"No trade_id found in position metadata for {symbol}, cannot update SL/TP on OANDA.")
                    
                    # 5. Update tracker
                    await self.position_tracker.update_position(position_id, stop_loss=new_sl, take_profit=new_tp, metadata=position['metadata'])
                    
                    return {
                        "status": "overridden", 
                        "position_id": position_id,
                        "reason": override_decision.reason,
                        "message": f"Close signal overridden - position continues running with tiered TP strategy. SL/TP updated.",
                        "tiered_tp_levels": len(override_decision.tiered_tp_levels) if override_decision.tiered_tp_levels else 0
                    }
                
                # Step 4: If conditions not met, execute close
                logger.info(f"📉 Override conditions not met for {symbol}: {override_decision.reason}")
                
            except Exception as override_error:
                logger.warning(f"Override evaluation failed for {symbol}: {override_error}. Proceeding with normal close.")
                # Continue to normal close if override logic fails
            
            # ===== NORMAL CLOSE EXECUTION =====
            exit_price = current_price  # We already have the current price
            
            close_payload = {"symbol": symbol, "action": action_to_close, "units": position['size']}
            success, result = await self.oanda_service.execute_trade(close_payload)

            if success:
                close_result = await self.position_tracker.close_position(position_id, exit_price, "Signal")
                await position_journal.record_exit(
                    position_id=position_id, exit_price=exit_price,
                    exit_reason="Signal", pnl=close_result.position_data.get('pnl', 0)
                )
                logger.info(f"✅ Successfully closed position {position_id} for {symbol}.")
                return {"status": "success", "position_id": position_id, "result": result}
            else:
                logger.error(f"Failed to close trade: {result.get('error')}")
                return {"status": "error", "message": "Close trade execution failed", "details": result}

        except MarketDataUnavailableError as e:
            logger.error(f"Market data unavailable for {symbol}, cannot close position. Error: {e}")
            return {"status": "error", "message": "An internal error occurred."}
        except Exception as e:
            logger.error(f"Unhandled exception in _handle_close_position: {e}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "message": "An internal error occurred."}