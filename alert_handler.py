# file: alert_handler.py

import asyncio
import logging
import uuid
import time
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import wraps
from utils import get_atr_multiplier, round_price, enforce_min_distance
from oandapyV20.exceptions import V20Error


from config import settings
from oanda_service import OandaService, MarketDataUnavailableError
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from technical_analysis import get_atr
from utils import (
    get_module_logger,
    format_symbol_for_oanda,
    format_crypto_symbol_for_oanda,
    calculate_position_size,
    get_instrument_leverage,
    TV_FIELD_MAP,
    get_instrument_type
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
            
            leverage = get_instrument_leverage(symbol)
            
            # === CALCULATE SL/TP UPON ENTRY ===
            timeframe = alert.get("timeframe", "H1")
            instrument_type = get_instrument_type(symbol)
            atr_multiplier = get_atr_multiplier(instrument_type, timeframe)
            
            # Calculate SL only (like the working past version)
            if action == "BUY":
                stop_loss_price = entry_price - (atr * atr_multiplier)
            else:  # SELL
                stop_loss_price = entry_price + (atr * atr_multiplier)
            
            logger.info(f"🎯 Entry SL for {symbol}: SL={stop_loss_price:.5f} (ATR={atr:.5f}, multiplier={atr_multiplier})")
            
            # Calculate position size with stop loss for proper risk management
            position_size, sizing_info = await calculate_position_size(
                symbol, entry_price, risk_percent, account_balance, leverage, 
                stop_loss_price=stop_loss_price, timeframe=timeframe
            )
            
            # SIMPLE TRADE PAYLOAD - NO COMPLEX VALIDATION (like working past version)
            trade_payload = {
                "symbol": symbol, 
                "action": action, 
                "units": position_size,
                "stop_loss": stop_loss_price
                # NO take_profit - this prevents TAKE_PROFIT_ON_FILL_LOSS errors
            }
            success, result = await self.oanda_service.execute_trade(trade_payload)

            if success:
                # INSTITUTIONAL FIX: Record recent position to prevent duplicates
                self._recent_positions[position_key] = current_time
                
                # === POSITION ID FROM TRADINGVIEW ===
                # TradingView generates and provides the position_id/alert_id
                if alert_id:
                    position_id = alert_id  # Use TradingView's generated ID
                    logger.info(f"🎯 Using TradingView-generated position_id: {position_id}")
                else:
                    # Fallback for backward compatibility
                    position_id = f"{symbol}_{int(time.time())}"
                    logger.warning(f"⚠️ No TradingView position_id provided, using fallback: {position_id}")
                await self.position_tracker.record_position(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], stop_loss=stop_loss_price, take_profit=None, # No take_profit in payload
                    metadata={"alert_id": alert_id, "sizing_info": sizing_info, "transaction_id": result['transaction_id']}
                )
                await position_journal.record_entry(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], strategy=alert.get("strategy", "N/A"),
                    stop_loss=stop_loss_price, take_profit=None # No take_profit in journal
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
        """Handles the logic for closing an existing position with simplified logic."""
        symbol = alert.get("symbol")
        position_id = alert.get("position_id")
        timeframe = alert.get("timeframe")
        
        logger.info(f"🎯 Processing CLOSE signal for {symbol} (position_id: {position_id})")
        
        # === SIMPLIFIED POSITION IDENTIFICATION ===
        position = None
        target_position_id = None
        
        # Strategy 1: Try position_id if provided
        if position_id:
            position = await self.position_tracker.get_position_info(position_id)
            if position:
                target_position_id = position_id
                logger.info(f"✅ Found position by ID: {position_id}")
        
        # Strategy 2: Fallback to symbol/timeframe matching
        if not position:
            logger.info(f"🔍 Searching for open positions by symbol/timeframe: {symbol}/{timeframe}")
            open_positions = await self.position_tracker.get_positions_by_symbol(symbol, status="open")
            
            # Find most recent position matching timeframe
            matching_positions = []
            for pos in open_positions:
                if str(pos.get("timeframe")) == str(timeframe):
                    matching_positions.append(pos)
            
            if matching_positions:
                # Sort by open time (most recent first)
                matching_positions.sort(key=lambda x: x.get('open_time', ''), reverse=True)
                position = matching_positions[0]
                target_position_id = position.get('position_id')
                logger.info(f"✅ Found position by symbol/timeframe: {target_position_id}")
        
        # Strategy 3: Last resort - any open position for symbol
        if not position:
            logger.info(f"🔍 Searching for any open position for {symbol}")
            open_positions = await self.position_tracker.get_positions_by_symbol(symbol, status="open")
            
            if open_positions:
                # Get the most recent position
                open_positions.sort(key=lambda x: x.get('open_time', ''), reverse=True)
                position = open_positions[0]
                target_position_id = position.get('position_id')
                logger.info(f"✅ Found position by symbol only: {target_position_id}")
        
        # If no position found, return error
        if not position:
            logger.warning(f"❌ No open position found for {symbol}. Close signal ignored.")
            return {
                "status": "error", 
                "message": f"No open position found for {symbol}",
                "details": {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "position_id": position_id,
                    "available_positions": len(await self.position_tracker.get_positions_by_symbol(symbol, status="open"))
                }
            }
        
        # === SIMPLIFIED CLOSE EXECUTION ===
        try:
            # Determine close action
            action_to_close = "SELL" if position['action'] == "BUY" else "BUY"
            position_size = position['size']
            
            logger.info(f"📉 Executing close for {symbol}: {action_to_close} {position_size} units")
            
            # Get current price for close
            current_price = await self.oanda_service.get_current_price(symbol, action_to_close)
            if not current_price:
                logger.error(f"❌ Could not get current price for {symbol}")
                return {"status": "error", "message": f"Could not get current price for {symbol}"}
            
            # === RETRY MECHANISM FOR OANDA CLOSE ===
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    close_payload = {
                        "symbol": symbol, 
                        "action": action_to_close, 
                        "units": position_size
                    }
                    
                    logger.info(f"🔄 Close attempt {attempt + 1}/{max_retries}: {action_to_close} {position_size} units at {current_price}")
                    success, result = await self.oanda_service.execute_trade(close_payload)
                    
                    if success:
                        # Update position tracker
                        close_result = await self.position_tracker.close_position(target_position_id, current_price, "Signal")
                        
                        # Record exit in journal
                        await position_journal.record_exit(
                            position_id=target_position_id, 
                            exit_price=current_price,
                            exit_reason="Signal", 
                            pnl=close_result.position_data.get('pnl', 0) if close_result.position_data else 0
                        )
                        
                        logger.info(f"✅ Successfully closed position {target_position_id} for {symbol}")
                        return {
                            "status": "success", 
                            "position_id": target_position_id, 
                            "exit_price": current_price,
                            "pnl": close_result.position_data.get('pnl', 0) if close_result.position_data else 0
                        }
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        logger.warning(f"⚠️ Close attempt {attempt + 1} failed: {error_msg}")
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)  # Wait before retry
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

    def get_atr_multiplier(instrument_type, timeframe):
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
        
        # Base multipliers by instrument type (institutional standards)
        base_multipliers = {
            'major': 2.0,      # EURUSD, GBPUSD, USDJPY, USDCHF - tight spreads, good liquidity
            'minor': 2.5,      # EURJPY, GBPJPY, AUDCAD - wider spreads  
            'exotic': 3.0,     # USDTRY, USDZAR - high volatility, wide spreads
            'crypto': 1.5,     # BTCUSD, ETHUSD - high volatility, need tighter stops
            'commodity': 2.2,  # XAUUSD, XAGUSD, Oil
            'index': 1.8,      # SPX500, NAS100 - trending instruments
            'default': 2.0     # Fallback for unknown types
        }
        
        # Timeframe adjustments (shorter = tighter, longer = wider)
        timeframe_multipliers = {
            '1': 0.8,    # 1min - very tight
            '5': 0.9,    # 5min - tight  
            '15': 1.0,   # 15min - baseline
            '30': 1.1,   # 30min - slightly wider
            '60': 1.2,   # 1H - wider
            '240': 1.4,  # 4H - much wider
            'D': 1.6,    # Daily - widest
            'default': 1.0
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