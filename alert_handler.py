# file: alert_handler.py

import asyncio
import logging
import uuid
import time
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import wraps
from utils import get_atr_multiplier, round_price, enforce_min_distance, standardize_symbol
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
    get_atr,  # This is the fallback ATR function
    get_pip_value
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
            if crypto_handler.is_crypto_signal(standardized_data['symbol']):
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
        symbol = standardize_symbol(alert.get("symbol"))
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
            # Try to get historical data for ATR calculation (not required for trade execution)
            df = None
            try:
                df = await self.oanda_service.get_historical_data(symbol, count=50, granularity="H1")
                if df is not None and not df.empty:
                    logger.info(f"✅ Historical data available for {symbol}")
                else:
                    logger.warning(f"⚠️ Historical data not available for {symbol}, will use default ATR")
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch historical data for {symbol}: {e}, will use default ATR")
            
            # Check essential data only (balance and price)
            if account_balance is None or entry_price is None:
                logger.error("Failed to get required market data for trade.")
                raise MarketDataUnavailableError("Failed to fetch market data (price or balance).")
            try:
                # Try to get ATR from historical data first
                atr = None
                if df is not None and not df.empty:
                    from technical_analysis import get_atr as get_atr_from_df
                    atr = get_atr_from_df(df)
                
                # If historical ATR fails, use fallback default ATR values
                if not atr or atr <= 0:
                    logger.warning(f"Historical ATR calculation failed for {symbol}, using default ATR")
                    atr = get_atr(symbol)  # This is the fallback function from utils.py
                
                if not atr or atr <= 0:
                    logger.error(f"Both historical and default ATR failed for {symbol}")
                    raise MarketDataUnavailableError(f"Could not determine ATR for {symbol}")
                    
                logger.info(f"✅ Using ATR {atr:.5f} for {symbol}")
                
            except Exception as e:
                logger.error(f"Failed to calculate ATR: {e}")
                # Final fallback - use default ATR
                logger.warning(f"Using emergency fallback ATR for {symbol}")
                atr = get_atr(symbol)
                if not atr or atr <= 0:
                    raise MarketDataUnavailableError("Failed to calculate ATR.")
                logger.info(f"✅ Emergency fallback ATR {atr:.5f} for {symbol}")
            leverage = get_instrument_leverage(symbol)
            timeframe = alert.get("timeframe", "H1")
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = self._get_atr_multiplier(instrument_type, timeframe)
            # --- ENHANCED STOP LOSS VALIDATION ---
            min_stop_percent = 0.0100  # 100 pips minimum - much wider stops for forex
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
                
                # Calculate stop loss in pips for position sizing
                if action == "BUY":
                    stop_loss_pips = (entry_price - stop_loss_price) / get_pip_value(symbol)
                else:
                    stop_loss_pips = (stop_loss_price - entry_price) / get_pip_value(symbol)
                
                # Use the ATR-based position sizing
                position_size = calculate_position_size(
                    account_balance=account_balance,
                    risk_percent=risk_percent,
                    stop_loss_pips=stop_loss_pips,
                    symbol=symbol
                )
                
                # Create sizing info for logging
                sizing_info = {
                    "method": "ATR-based",
                    "actual_risk": account_balance * (risk_percent / 100),
                    "stop_loss_pips": stop_loss_pips
                }
                
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
                await position_journal.record_entry(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], strategy=alert.get("strategy", "N/A"),
                    stop_loss=stop_loss_price, take_profit=None
                )
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
        """Handles the logic for closing an existing position with enhanced safety and override logic."""
        symbol = standardize_symbol(alert.get("symbol"))
        position_id = alert.get("position_id")
        timeframe = alert.get("timeframe")
        logger.info(f"🎯 Processing CLOSE signal for {symbol} (position_id: {position_id})")
        # --- POSITION IDENTIFICATION (unchanged) ---
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
        if not position:
            # FALLBACK: Try to close directly via OANDA if no position found in database
            logger.warning(f"❌ No open position found in database for {symbol}. Trying direct OANDA close...")
            try:
                # Get current price for PnL calculation
                current_price = await self.oanda_service.get_current_price(symbol, "SELL")
                
                # Try to close directly via OANDA
                success, result = await self.oanda_service.close_position(symbol, 1000)  # Use default units
                
                if success:
                    logger.info(f"✅ Successfully closed position directly via OANDA for {symbol}")
                    return {
                        "status": "success",
                        "position_id": f"OANDA_DIRECT_{symbol}",
                        "exit_price": float(result.get('price', current_price)),
                        "pnl": 0,  # Can't calculate PnL without entry price
                        "message": "Position closed directly via OANDA (not in database)"
                    }
                else:
                    logger.error(f"❌ Direct OANDA close failed for {symbol}: {result.get('error', 'Unknown error')}")
                    return {
                        "status": "error",
                        "message": f"Direct OANDA close failed: {result.get('error', 'Unknown error')}",
                        "details": {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "position_id": position_id,
                            "available_positions": len(await self.position_tracker.get_positions_by_symbol(symbol, status="open"))
                        }
                    }
            except Exception as e:
                logger.error(f"❌ Direct OANDA close attempt failed: {e}")
                return {
                    "status": "error",
                    "message": f"No open position found for {symbol} and direct OANDA close failed: {str(e)}",
                    "details": {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "position_id": position_id,
                        "available_positions": len(await self.position_tracker.get_positions_by_symbol(symbol, status="open"))
                    }
                }
        # --- ENHANCED CLOSE LOGIC: FORCE CLOSE & PROFIT RIDE OVERRIDE ---
        logger.info(f"🎯 [ALERT DEBUG] Processing CLOSE signal for {symbol} (position_id: {target_position_id})")
        logger.info(f"🎯 [ALERT DEBUG] Found position data: {position}")
        
        # FIX: Properly handle get_current_price exceptions and None returns
        try:
            current_price = await self.oanda_service.get_current_price(symbol, "SELL" if position['action'] == "BUY" else "BUY")
            logger.info(f"🎯 [ALERT DEBUG] Got current price for {symbol}: {current_price}")
        except Exception as e:
            logger.error(f"❌ Exception getting current price for {symbol}: {e}")
            current_price = None
        
        # FIX: Add safety check for current_price
        if current_price is None or current_price <= 0:
            logger.error(f"❌ Failed to get valid current price for {symbol}: {current_price}")
            return {
                "status": "error",
                "message": f"Failed to get valid current price for {symbol}: {current_price}"
            }
        # Force close if position is too old or drawdown too high
        from datetime import datetime, timezone
        open_time = position.get("open_time")
        max_hold_days = 5
        should_force_close = False
        if open_time:
            if isinstance(open_time, str):
                try:
                    open_time_dt = datetime.fromisoformat(open_time)
                except Exception:
                    open_time_dt = None
            elif isinstance(open_time, datetime):
                open_time_dt = open_time
            else:
                open_time_dt = None
            if open_time_dt:
                now = datetime.now(timezone.utc)
                hold_days = (now - open_time_dt).days
                if hold_days > max_hold_days:
                    should_force_close = True
                    logger.warning(f"Force closing {target_position_id}: hold time {hold_days}d > {max_hold_days}d")
        # Drawdown check (example: 15% max)
        entry_price = float(position.get("entry_price", 0))
        size = float(position.get("size", 0))
        
        # FIX: Add safety checks for None values
        if entry_price <= 0 or size <= 0:
            logger.error(f"❌ Invalid position data for {target_position_id}: entry_price={entry_price}, size={size}")
            return {
                "status": "error",
                "message": f"Invalid position data: entry_price={entry_price}, size={size}"
            }
        
        # Additional safety check for current_price before calculations
        if current_price is None or current_price <= 0:
            logger.error(f"❌ Current price is invalid for PnL calculation: {current_price}")
            return {
                "status": "error", 
                "message": f"Failed to get valid current price for {symbol}: {current_price}"
            }
        
        pnl = (current_price - entry_price) * size if position['action'] == "BUY" else (entry_price - current_price) * size
        drawdown = abs(min(0, pnl)) / (entry_price * size) if entry_price and size else 0
        if drawdown > 0.15:
            should_force_close = True
            logger.warning(f"Force closing {target_position_id}: drawdown {drawdown:.2%} > 15%")
        # Profit ride override (example logic)
        override_fired = False
        if not should_force_close:
            # Only override if position is profitable and market is trending (simplified)
            # FIX: Handle None stop_loss values properly
            stop_loss = position.get("stop_loss")
            if stop_loss is None:
                stop_loss = entry_price  # Use entry price as fallback
                logger.warning(f"⚠️ No stop_loss found for position {target_position_id}, using entry_price as fallback")
            
            if pnl > abs(entry_price - stop_loss) * size and drawdown < 0.05:
                # Activate trailing stop system (NO take profit level)
                atr = get_atr(symbol)  # Use fallback ATR from utils.py
                if position['action'] == "BUY":
                    new_sl = current_price - (atr * 2.0)  # Initial trailing stop
                else:
                    new_sl = current_price + (atr * 2.0)  # Initial trailing stop
                
                # Only set trailing stop loss, NO take profit
                await self.oanda_service.modify_position(target_position_id, stop_loss=new_sl, take_profit=None)
                await self.position_tracker.update_position(target_position_id, stop_loss=new_sl, take_profit=None)
                
                # Mark position for trailing stop system
                await self.position_tracker.update_position(target_position_id, metadata={
                    'profit_ride_override_fired': True,
                    'trailing_stop_active': True,
                    'trailing_stop_price': new_sl,
                    'initial_trail_distance': atr * 2.0
                })
                
                logger.info(f"Profit ride override: activated trailing stop at {new_sl} (NO take profit)")
                override_fired = True
        if override_fired:
            return {
                "status": "overridden",
                "message": "Close signal ignored due to profit ride override",
                "position_id": target_position_id
            }
        # --- NORMAL CLOSE EXECUTION (FIXED) ---
        try:
            position_size = position['size']
            logger.info(f"📉 Executing close for {symbol}: {position_size} units")
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    logger.info(f"🔄 Close attempt {attempt + 1}/{max_retries}: closing {position_size} units of {symbol}")
                    success, result = await self.oanda_service.close_position(symbol, position_size)
                    if success:
                        # Use the actual close price from OANDA
                        actual_close_price = float(result.get('price', current_price))
                        close_result = await self.position_tracker.close_position(target_position_id, actual_close_price, "Signal")
                        await position_journal.record_exit(
                            position_id=target_position_id,
                            exit_price=actual_close_price,
                            exit_reason="Signal",
                            pnl=close_result.position_data.get('pnl', 0) if close_result.position_data else 0
                        )
                        logger.info(f"✅ Successfully closed position {target_position_id} for {symbol} at {actual_close_price}")
                        # --- INSTITUTIONAL PnL/DD LOGGING ---
                        try:
                            starting_equity = await self.db_manager.get_initial_account_balance()
                            final_equity = await self.oanda_service.get_account_balance()
                            max_drawdown_absolute = await self.db_manager.get_max_drawdown_absolute()
                            pnl_dd_ratio = MetricsUtils.calculate_pnl_dd_ratio(
                                starting_equity, final_equity, max_drawdown_absolute
                            )
                            logger.info(f"📊 PnL/DD Ratio: {pnl_dd_ratio:.4f} (Total Return: {((final_equity-starting_equity)/starting_equity*100):.2f}%)")
                        except Exception as e:
                            logger.warning(f"Could not calculate PnL/DD ratio: {e}")
                        # --- END INSTITUTIONAL PnL/DD LOGGING ---
                        return {
                            "status": "success",
                            "position_id": target_position_id,
                            "exit_price": actual_close_price,
                            "pnl": close_result.position_data.get('pnl', 0) if close_result.position_data else 0
                        }
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        logger.warning(f"⚠️ Close attempt {attempt + 1} failed: {error_msg}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                        else:
                            logger.error(f"❌ All close attempts failed for {symbol}")
                            return {
                                "status": "error",
                                "message": f"Failed to close position after {max_retries} attempts",
                                "details": result
                            }
                except Exception as e:
                    logger.error(f"❌ Exception during close attempt {attempt + 1}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                    else:
                        return {
                            "status": "error",
                            "message": f"Exception during close execution: {str(e)}"
                        }
        except Exception as e:
            logger.error(f"❌ Unhandled exception in close execution: {e}")
            return {
                "status": "error",
                "message": f"Unhandled exception: {str(e)}"
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

    def _get_atr_multiplier(self, instrument_type, timeframe):
        """
        Calculate ATR-based stop loss multiplier for different instrument types and timeframes.
        
        Based on institutional risk management practices:
        - Higher volatility assets (crypto) = lower multiplier
        - Lower volatility assets (majors) = higher multiplier  
        - Shorter timeframes = tighter stops
        - Longer timeframes = wider stops
        
        Args:
            instrument_type (str): 'major', 'minor', 'exotic', 'crypto', 'commodity', 'index'
            timeframe (str): '5', '15', '30', '60', '240', 'D'
            
        Returns:
            float: ATR multiplier for stop loss calculation
        """
        
        # Base multipliers by instrument type (institutional standards) - INCREASED for wider stops
        base_multipliers = {
            'major': 3.0,      # EURUSD, GBPUSD, USDJPY, USDCHF - wider stops for better breathing room
            'minor': 3.5,      # EURJPY, GBPJPY, AUDCAD - even wider for cross pairs
            'exotic': 4.0,     # USDTRY, USDZAR - very wide for high volatility
            'crypto': 4.0,     # BTCUSD, ETHUSD - much wider stops for crypto volatility
            'commodity': 3.5,  # XAUUSD, XAGUSD, Oil - wider for commodity volatility
            'index': 3.0,      # SPX500, NAS100 - wider for index volatility
            'default': 3.0     # Fallback for unknown types
        }
        
        # Timeframe adjustments (shorter = tighter, longer = wider) - INCREASED for wider stops
        timeframe_multipliers = {
            '1': 1.0,    # 1min - wider baseline
            '5': 1.2,    # 5min - wider  
            '15': 1.4,   # 15min - wider baseline
            '30': 1.6,   # 30min - wider
            '60': 1.8,   # 1H - much wider
            '240': 2.0,  # 4H - very wide
            'D': 2.5,    # Daily - widest
            'default': 1.4
        }
        
        # Determine instrument type from common patterns
        def classify_instrument(symbol):
            symbol = symbol.upper().replace('_', '')
            
            # Crypto patterns
            if any(crypto in symbol for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'ADA']):
                return 'crypto'
            
            # Major FX pairs
            majors = ['EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD']
            if symbol in majors:
                return 'major'
            
            # JPY crosses (typically more volatile)
            if 'JPY' in symbol and symbol not in ['USDJPY']:
                return 'minor'
            
            # Commodities
            if any(comm in symbol for comm in ['XAU', 'XAG', 'OIL', 'GOLD', 'SILVER']):
                return 'commodity'
            
            # Indices  
            if any(idx in symbol for idx in ['SPX', 'NAS', 'DAX', 'FTSE', 'NIKKEI']):
                return 'index'
            
            # Default to minor for other FX pairs
            if len(symbol) == 6 and symbol[:3] != symbol[3:]:
                return 'minor'
            
            return 'default'
        
        # Get the appropriate multipliers
        if isinstance(instrument_type, str) and instrument_type.lower() in base_multipliers:
            base_mult = base_multipliers[instrument_type.lower()]
        else:
            # Auto-classify if not provided or invalid
            classified_type = classify_instrument(str(instrument_type))
            base_mult = base_multipliers.get(classified_type, base_multipliers['default'])
        
        timeframe_str = str(timeframe)
        time_mult = timeframe_multipliers.get(timeframe_str, timeframe_multipliers['default'])
        
        # Calculate final multiplier
        final_multiplier = base_mult * time_mult
        
        # Institutional bounds - never go below 1.0 or above 4.0
        final_multiplier = max(1.0, min(4.0, final_multiplier))
        
        return round(final_multiplier, 2)