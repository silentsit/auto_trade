# file: alert_handler.py

import asyncio
import logging
import uuid
import time
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import wraps
from utils import get_atr_multiplier, round_price, enforce_min_distance
from oandapyV20.exceptions import V20Error


from config import settings
from oanda_service import OandaService, MarketDataUnavailableError
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from utils import (
    get_module_logger,
    format_symbol_for_oanda,
    format_crypto_symbol_for_oanda,
    calculate_position_size,
    get_instrument_leverage,
    TV_FIELD_MAP,
    get_instrument_type,
    get_asset_class,
    get_position_size_limits,
    round_position_size,
    MetricsUtils,
    get_atr_multiplier,
    get_atr
)
from unified_storage import UnifiedStorage
from unified_analysis import UnifiedMarketAnalyzer

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
        risk_manager: EnhancedRiskManager,
        unified_exit_manager=None
    ):
        """Initializes the AlertHandler with all required components."""
        self.oanda_service = oanda_service 
        self.position_tracker = position_tracker
        self.risk_manager = risk_manager
        self.db_manager = db_manager
        self.unified_exit_manager = unified_exit_manager
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
            # Check if it's crypto and format appropriately
            symbol_upper = standardized_data['symbol'].upper()
            crypto_symbols = ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'ADA', 'DOT', 'LINK']
            is_crypto = any(crypto in symbol_upper for crypto in crypto_symbols)
            
            if is_crypto:
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
        """Handles the logic for opening a new position with integrated safety and risk validation."""
        symbol = alert.get("symbol")
        action = alert.get("action")
        risk_percent_raw = alert.get("risk_percent", alert.get("percentage", getattr(settings, 'max_risk_percentage', 20.0)))
        try:
            risk_percent = float(risk_percent_raw)
        except Exception as e:
            logger.error(f"Could not convert risk_percent value '{risk_percent_raw}' to float: {e}. Using default 5.0.")
            risk_percent = 5.0
        logger.info(f"[ALERT HANDLER] Using risk_percent={risk_percent}")
        atr = None
        position_key = f"{symbol}_{action}"
        current_time = time.time()
        
        # FIX: Ensure _recent_positions is always initialized
        if not hasattr(self, '_recent_positions'):
            self._recent_positions = {}
            
        if position_key in self._recent_positions:
            last_time = self._recent_positions[position_key]
            time_diff = current_time - last_time
            logger.info(f"🔍 Duplicate check for {position_key}: {time_diff:.1f}s since last trade")
            if time_diff < 60:
                logger.warning(f"❌ Recent position detected for {position_key}, skipping duplicate trade (last trade was {time_diff:.1f}s ago)")
                return {
                    "status": "ignored",
                    "message": f"Recent {action} position exists for {symbol}",
                    "alert_id": alert_id
                }
            else:
                logger.info(f"✅ Duplicate check passed for {position_key} (last trade was {time_diff:.1f}s ago)")
        else:
            logger.info(f"✅ No recent trades found for {position_key}")
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
                # Calculate ATR from the dataframe
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())
                true_range = np.maximum(high_low, np.maximum(high_close, low_close))
                atr = true_range.rolling(window=14).mean().iloc[-1]
                
                if not atr or not atr > 0:
                    logger.error(f"Invalid ATR value ({atr}) for {symbol}.")
                    raise MarketDataUnavailableError(f"Invalid ATR ({atr}) calculated for {symbol}.")
            except Exception as e:
                logger.error(f"Failed to calculate ATR: {e}")
                raise MarketDataUnavailableError("Failed to calculate ATR.")
            leverage = get_instrument_leverage(symbol)
            timeframe = alert.get("timeframe", "H1")
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            # --- ENHANCED STOP LOSS VALIDATION ---
            min_stop_percent = 0.0010  # 10 pips default
            if action == "BUY":
                stop_loss_price = entry_price - (atr * atr_multiplier)
                stop_distance = entry_price - stop_loss_price
                if stop_loss_price >= entry_price or (stop_distance / entry_price) < min_stop_percent:
                    stop_loss_price = entry_price - (entry_price * min_stop_percent)
                    logger.warning(f"Adjusted stop loss for BUY: {stop_loss_price}")
            else:
                stop_loss_price = entry_price + (atr * atr_multiplier)
                stop_distance = stop_loss_price - entry_price
                if stop_loss_price <= entry_price or (stop_distance / entry_price) < min_stop_percent:
                    stop_loss_price = entry_price + (entry_price * min_stop_percent)
                    logger.warning(f"Adjusted stop loss for SELL: {stop_loss_price}")
            logger.info(f"🎯 Entry SL for {symbol}: SL={stop_loss_price:.5f} (ATR={atr:.5f}, multiplier={atr_multiplier})")
            
            # --- POSITION SIZE CALCULATION ---
            # Use the improved ATR-based position sizing from Backtest-Adapter-3.0
            try:
                # Get account balance for position sizing
                account_info = await self.oanda_service.get_account_info()
                account_balance = float(account_info.get('balance', 0))
                actual_leverage = float(account_info.get('leverage', 50.0))  # Get actual account leverage
                
                if account_balance <= 0:
                    logger.error(f"❌ Invalid account balance: ${account_balance}")
                    return {"status": "error", "message": "Invalid account balance"}
                
                logger.info(f"🎯 Using actual account leverage: {actual_leverage:.1f}:1 for position sizing")
                
                # Use the improved ATR-based position sizing
                position_size, sizing_info = await calculate_position_size(
                    symbol=symbol,
                    entry_price=entry_price,
                    risk_percent=risk_percent,
                    account_balance=account_balance,
                    leverage=actual_leverage,  # ✅ Use actual account leverage instead of hardcoded 50.0
                    max_position_value=100000.0,  # Default max position value
                    stop_loss_price=stop_loss_price,  # Use calculated ATR-based stop loss
                    timeframe=alert.get("timeframe", "H1")
                )
                
                if position_size <= 0:
                    error_msg = sizing_info.get("error", "Unknown error in position sizing")
                    logger.error(f"❌ Position sizing failed for {symbol}: {error_msg}")
                    return {"status": "error", "message": f"Position sizing failed: {error_msg}"}
                
                logger.info(f"[ATR-BASED SIZING] {symbol}: Final size={position_size}, Risk=${sizing_info.get('actual_risk', 0):.2f}, Method={sizing_info.get('method', 'Unknown')}")
                
            except Exception as e:
                logger.error(f"❌ Error in ATR-based position sizing for {symbol}: {e}")
                # Fallback to simple percentage-based sizing
                account_info = await self.oanda_service.get_account_info()
                account_balance = float(account_info.get('balance', 0))
                target_position_value = account_balance * (risk_percent / 100.0)
                position_size = target_position_value / entry_price
                
                # Apply basic limits
                min_units, max_units = get_position_size_limits(symbol)
                position_size = max(min_units, min(max_units, position_size))
                position_size = round_position_size(symbol, position_size)
                
                logger.warning(f"[FALLBACK SIZING] {symbol}: Using fallback method, size={position_size}")
            
            # --- FINAL TRADE PAYLOAD ---
            # OANDA requires negative units for SELL positions
            final_units = position_size if action == "BUY" else -abs(position_size)
            
            trade_payload = {
                "symbol": symbol,
                "action": action,
                "units": final_units,
                "stop_loss": stop_loss_price
            }
            success, result = await self.oanda_service.execute_trade(trade_payload)
            if success:
                self._recent_positions[position_key] = current_time
                logger.info(f"📝 Recorded recent position for {position_key} at {current_time}")
                if alert_id:
                    position_id = alert_id
                    logger.info(f"🎯 Using TradingView-generated position_id: {position_id}")
                else:
                    position_id = f"{symbol}_{int(time.time())}"
                    logger.warning(f"⚠️ No TradingView position_id provided, using fallback: {position_id}")
                await self.position_tracker.record_position(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], stop_loss=stop_loss_price, take_profit=None,
                    metadata={"alert_id": alert_id, "transaction_id": result['transaction_id']}
                )
                # Record entry in unified storage
                await self.db_manager.record_position_entry(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], strategy=alert.get("strategy", "N/A"),
                    stop_loss=stop_loss_price, take_profit=None
                )
                
                # --- INITIALIZE EXIT STRATEGY ---
                try:
                    # Use the unified exit manager instance
                    if self.unified_exit_manager:
                        # Create position data for exit strategy initialization
                        position_data = {
                            'position_id': position_id,
                            'symbol': symbol,
                            'action': action,
                            'entry_price': result['fill_price'],
                            'entry_time': datetime.now(timezone.utc),
                            'timeframe': alert.get("timeframe", "N/A"),
                            'units': result['units']
                        }
                        
                        # Initialize exit strategy for the position
                        exit_strategy = await self.unified_exit_manager.add_position(position_data)
                        if exit_strategy:
                            logger.info(f"🎯 Exit strategy initialized for {symbol}: SL={exit_strategy.stop_loss_price:.5f}, TP={exit_strategy.take_profit_price:.5f}")
                        else:
                            logger.warning(f"⚠️ Failed to initialize exit strategy for {symbol}")
                    else:
                        logger.warning(f"⚠️ Unified exit manager not available for {symbol}")
                        
                except Exception as e:
                    logger.error(f"❌ Error initializing exit strategy for {symbol}: {e}")
                    # Continue with position creation even if exit strategy fails
                
                logger.info(f"✅ Successfully opened position {position_id} for {symbol}.")
                return {"status": "success", "position_id": position_id, "result": result}
            else:
                logger.error(f"Failed to execute trade: {result.get('error')}")
                return {"status": "error", "message": "Trade execution failed", "details": result}
        except MarketDataUnavailableError as e:
            logger.error(f"Market data unavailable for {symbol}, cannot open position. Error: {e}")
            raise
        except V20Error as e:
            logger.error(f"OANDA API error while opening position for {symbol}: {e}")
            return {"status": "error", "message": f"OANDA API Error: {e}"}
        except Exception as e:
            logger.error(f"Unhandled exception in _handle_open_position: {e}", exc_info=True)
            return {"status": "error", "message": "An internal error occurred."}

    async def _handle_close_position(self, alert: Dict[str, Any], alert_id: str) -> Dict[str, Any]:
        """
        Handles the logic for closing an existing position using the new unified exit manager flow.
        
        NEW FLOW:
        1. Check if position still exists
        2. If exists, use unified exit manager to handle close signal
        3. If not, acknowledge it already exited
        """
        symbol = alert.get("symbol")
        position_id = alert.get("position_id")
        timeframe = alert.get("timeframe")
        logger.info(f"🎯 Processing CLOSE signal for {symbol} (position_id: {position_id})")
        
        # --- POSITION IDENTIFICATION ---
        position = None
        target_position_id = None
        if position_id:
            position = await self.position_tracker.get_position_info(position_id)
            if position:
                target_position_id = position_id
                logger.info(f"✅ Found position by ID: {position_id}")
        
        if not position:
            logger.info(f"🔍 Searching for open positions by symbol/timeframe: {symbol}/{timeframe}")
            open_positions = await self.position_tracker.get_positions_by_symbol(symbol, status="open")
            matching_positions = [pos for pos in open_positions if str(pos.get("timeframe")) == str(timeframe)]
            if matching_positions:
                matching_positions.sort(key=lambda x: x.get('open_time', ''), reverse=True)
                position = matching_positions[0]
                target_position_id = position.get('position_id')
                logger.info(f"✅ Found position by symbol/timeframe: {target_position_id}")
        
        if not position:
            logger.info(f"🔍 Searching for any open position for {symbol}")
            open_positions = await self.position_tracker.get_positions_by_symbol(symbol, status="open")
            if open_positions:
                open_positions.sort(key=lambda x: x.get('open_time', ''), reverse=True)
                position = open_positions[0]
                target_position_id = position.get('position_id')
                logger.info(f"✅ Found position by symbol only: {target_position_id}")
        
        # --- POSITION STATUS CHECK ---
        if not position:
            logger.info(f"📝 Close signal acknowledged for {symbol}: Position already exited")
            return {
                "status": "acknowledged",
                "message": f"Close signal acknowledged for {symbol} - Position already exited",
                "details": {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "position_id": position_id,
                    "available_positions": len(await self.position_tracker.get_positions_by_symbol(symbol, status="open"))
                }
            }
        
        # --- UNIFIED EXIT MANAGER INTEGRATION ---
        try:
            # Use the unified exit manager instance
            if not self.unified_exit_manager:
                logger.error("❌ Unified exit manager not available")
                return {
                    "status": "error",
                    "message": "Unified exit manager not available"
                }
            
            # Use unified exit manager to handle the close signal
            # This will either execute the close immediately or activate profit override
            result = await self.unified_exit_manager.handle_close_signal(
                position_id=target_position_id,
                reason=f"Close signal for {symbol}"
            )
            
            if result:
                logger.info(f"✅ Close signal handled successfully by unified exit manager for {symbol}")
                return {
                    "status": "success",
                    "message": "Close signal processed by unified exit manager",
                    "position_id": target_position_id,
                    "symbol": symbol
                }
            else:
                logger.error(f"❌ Unified exit manager failed to handle close signal for {symbol}")
                return {
                    "status": "error",
                    "message": "Unified exit manager failed to process close signal"
                }
                
        except Exception as e:
            logger.error(f"❌ Error in unified exit manager integration: {e}")
            # Fallback to manual close if unified exit manager fails
            return await self._manual_close_fallback(position, target_position_id, symbol)
    
    async def _manual_close_fallback(self, position: Dict, target_position_id: str, symbol: str) -> Dict[str, Any]:
        """
        Fallback manual close method if unified exit manager fails
        """
        try:
            current_price = await self.oanda_service.get_current_price(symbol, "SELL" if position['action'] == "BUY" else "BUY")
            
            if current_price is None or current_price <= 0:
                logger.error(f"❌ Failed to get current price for {symbol}")
                return {
                    "status": "error",
                    "message": f"Failed to get current price for {symbol}"
                }
            
            # Execute manual close
            action_to_close = "SELL" if position['action'] == "BUY" else "BUY"
            position_size = position['size']
            
            logger.info(f"📉 Executing manual close fallback for {symbol}: {action_to_close} {position_size} units")
            
            close_payload = {
                "symbol": symbol,
                "action": action_to_close,
                "units": position_size
            }
            
            success, result = await self.oanda_service.execute_trade(close_payload)
            if success:
                close_result = await self.position_tracker.close_position(target_position_id, current_price, "Manual Fallback")
                # Record exit in unified storage
                await self.db_manager.record_position_exit(
                    position_id=target_position_id,
                    exit_price=current_price,
                    exit_reason="Manual Fallback",
                    pnl=close_result.position_data.get('pnl', 0) if close_result.position_data else 0
                )
                
                logger.info(f"✅ Successfully closed position {target_position_id} for {symbol} (manual fallback)")
                
                return {
                    "status": "success",
                    "position_id": target_position_id,
                    "exit_price": current_price,
                    "pnl": close_result.position_data.get('pnl', 0) if close_result.position_data else 0,
                    "method": "manual_fallback"
                }
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"❌ Manual close fallback failed: {error_msg}")
                return {
                    "status": "error",
                    "message": f"Manual close fallback failed: {error_msg}",
                    "details": result
                }
                
        except Exception as e:
            logger.error(f"❌ Exception in manual close fallback: {e}")
            return {
                "status": "error",
                "message": f"Exception in manual close fallback: {str(e)}"
            }

    def _extract_close_direction(self, alert: Dict[str, Any]) -> Optional[str]:
        """Extract the target direction for closing from alert data"""
        # Priority 1: Explicit close direction fields
        direction_fields = ['close_direction', 'original_direction', 'position_direction', 'trade_direction', 'side', 'action_to_close']
        for field in direction_fields:
            if field in alert:
                direction = alert[field].upper()
                if direction in ['BUY', 'LONG']:
                    return 'BUY'
                elif direction in ['SELL', 'SHORT']:
                    return 'SELL'
        
        # Priority 2: Comment field analysis
        comment = alert.get('comment', '').upper()
        if any(phrase in comment for phrase in ['CLOSE LONG', 'LONG EXIT', 'EXIT LONG', 'CLOSE BUY']):
            return 'BUY'
        elif any(phrase in comment for phrase in ['CLOSE SHORT', 'SHORT EXIT', 'EXIT SHORT', 'CLOSE SELL']):
            return 'SELL'
        
        # Priority 3: Action field analysis
        action = alert.get('action', '').upper()
        if action in ['CLOSE_LONG', 'CLOSE_BUY']:
            return 'BUY'
        elif action in ['CLOSE_SHORT', 'CLOSE_SELL']:
            return 'SELL'
        
        # No specific direction found
        return None

