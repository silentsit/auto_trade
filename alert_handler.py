# file: alert_handler.py

import asyncio
import logging
import uuid
import time
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import wraps
from utils import round_price, enforce_min_distance, standardize_symbol
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
    get_atr,  # This is the fallback ATR function
    get_pip_value
)
from position_journal import position_journal
# Removed dependency on crypto_signal_handler (module not present). Use utils classification instead.
from ml_integration import enhance_tradingview_signal, ml_meta_filter

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
        
        # MONITORING: Track duplicate detection statistics
        self.duplicate_stats = {
            "total_alerts_received": 0,
            "duplicates_blocked": 0,
            "alerts_processed": 0,
            "last_duplicate_timestamp": None,
            "duplicate_symbols": {}  # Track which symbols have duplicates
        }
        logger.info("✅ AlertHandler initialized with all components.")
        # Preflight queue when OANDA connectivity is degraded
        self.queued_alerts = []

    async def start(self):
        """Starts the alert handler."""
        self._started = True
        logger.info("✅ AlertHandler started and ready to process alerts.")
    
    async def retry_queued_alerts(self) -> Dict[str, Any]:
        """
        Retry processing of queued alerts when OANDA connectivity recovers.
        Called periodically by background task.
        """
        if not self.queued_alerts:
            return {"status": "ok", "queued": 0, "processed": 0}
        
        # Check if OANDA is now tradeable
        can_trade = getattr(self.oanda_service, 'can_trade', lambda: False)()
        if not can_trade:
            return {
                "status": "waiting", 
                "queued": len(self.queued_alerts),
                "message": "OANDA still degraded, waiting for recovery"
            }
        
        # OANDA is healthy - process queued alerts
        logger.info(f"🔄 OANDA connectivity restored. Retrying {len(self.queued_alerts)} queued alerts...")
        
        processed = 0
        failed = 0
        alerts_to_retry = self.queued_alerts.copy()
        self.queued_alerts.clear()
        
        for queued_alert in alerts_to_retry:
            try:
                # Re-process the alert through the normal flow
                # Note: We skip the duplicate check since these were already validated
                result = await self._process_alert_internal(queued_alert)
                if result.get("status") == "success":
                    processed += 1
                    logger.info(f"✅ Queued alert processed successfully: {queued_alert.get('symbol')}")
                else:
                    failed += 1
                    # Re-queue if still having issues
                    if result.get("status") == "queued":
                        self.queued_alerts.append(queued_alert)
                        logger.warning(f"⚠️ Alert re-queued: {queued_alert.get('symbol')}")
            except Exception as e:
                failed += 1
                logger.error(f"❌ Error processing queued alert for {queued_alert.get('symbol')}: {e}")
                # Don't re-queue on hard errors to prevent infinite loops
        
        result = {
            "status": "completed",
            "processed": processed,
            "failed": failed,
            "still_queued": len(self.queued_alerts)
        }
        
        if processed > 0:
            logger.info(f"🎯 Queued alert retry complete: {processed} processed, {failed} failed, {len(self.queued_alerts)} still queued")
        
        return result
    
    async def _process_alert_internal(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method to process an alert without duplicate checking.
        Used by retry_queued_alerts to reprocess queued alerts.
        """
        try:
            action = alert.get("action")
            
            # Re-check OANDA health before processing
            if hasattr(self.oanda_service, 'can_trade') and not self.oanda_service.can_trade():
                return {
                    "status": "queued",
                    "message": "OANDA connectivity degraded during retry"
                }
            
            if action in ["BUY", "SELL"]:
                # Generate a new alert_id for the retry
                alert_id = f"retry_{int(time.time() * 1000)}_{alert.get('symbol', 'UNKNOWN')}"
                result = await self._handle_open_position(alert, alert_id)
            elif action == "CLOSE":
                alert_id = f"retry_{int(time.time() * 1000)}_{alert.get('symbol', 'UNKNOWN')}"
                result = await self._handle_close_position(alert, alert_id)
            else:
                return {"status": "error", "message": f"Unknown action: {action}"}
            
            return result
        except Exception as e:
            logger.error(f"Error in _process_alert_internal: {e}")
            return {"status": "error", "message": str(e)}

    def get_status(self) -> Dict[str, Any]:
        """Lightweight status for API diagnostics and degraded mode reporting."""
        oanda_status = {}
        try:
            if hasattr(self.oanda_service, 'connection_state'):
                ocs = self.oanda_service.connection_state
                oanda_status = getattr(ocs, 'get_status', lambda: {} )() or {}
        except Exception:
            oanda_status = {"state": "unknown"}
        return {
            "shim_mode": False,
            "started": self._started,
            "queued_alerts": len(getattr(self, 'queued_alerts', [])),
            "oanda_state": oanda_status.get("state"),
            "oanda_can_trade": getattr(self.oanda_service, 'can_trade', lambda: False)(),
        }

    async def stop(self):
        """Stops the alert handler."""
        self._started = False
        logger.info("🛑 AlertHandler stopped.")
        
        # Log final duplicate detection statistics
        logger.info(f"📊 DUPLICATE DETECTION STATS:")
        logger.info(f"   Total Alerts Received: {self.duplicate_stats['total_alerts_received']}")
        logger.info(f"   Alerts Processed: {self.duplicate_stats['alerts_processed']}")
        logger.info(f"   Duplicates Blocked: {self.duplicate_stats['duplicates_blocked']}")
        if self.duplicate_stats['duplicates_blocked'] > 0:
            block_rate = (self.duplicate_stats['duplicates_blocked'] / self.duplicate_stats['total_alerts_received']) * 100
            logger.info(f"   Duplicate Block Rate: {block_rate:.2f}%")
            logger.info(f"   Symbols with Duplicates: {self.duplicate_stats['duplicate_symbols']}")
    
    def get_duplicate_stats(self) -> Dict[str, Any]:
        """Get current duplicate detection statistics for monitoring."""
        return {
            **self.duplicate_stats,
            "active_alerts_count": len(self.active_alerts),
            "duplicate_block_rate": (
                (self.duplicate_stats['duplicates_blocked'] / self.duplicate_stats['total_alerts_received'] * 100)
                if self.duplicate_stats['total_alerts_received'] > 0 else 0
            )
        }

    def _standardize_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardizes the incoming alert data."""
        standardized_data = alert_data.copy()
        for tv_field, expected_field in TV_FIELD_MAP.items():
            if tv_field in standardized_data:
                standardized_data[expected_field] = standardized_data.pop(tv_field)
        
        if 'symbol' in standardized_data:
            # First check if it's crypto and format appropriately (no external crypto handler required)
            raw_symbol = standardized_data['symbol']
            # Standardize format for detection
            standardized = format_symbol_for_oanda(raw_symbol)
            if get_instrument_type(standardized) == 'crypto':
                standardized_data['symbol'] = format_crypto_symbol_for_oanda(standardized)
                logger.info(f"Crypto symbol detected and formatted: {standardized_data['symbol']}")
            else:
                standardized_data['symbol'] = standardized
            
        if 'action' in standardized_data:
            standardized_data['action'] = standardized_data['action'].upper()

        return standardized_data

    def _generate_alert_id(self, alert_data: Dict[str, Any]) -> str:
        """
        Generate unique alert ID with sub-second precision to prevent duplicates.
        
        INSTITUTIONAL FIX: Use full timestamp precision (not rounded) plus hash of alert data
        to ensure true uniqueness even for rapid-fire alerts within same second.
        """
        symbol = alert_data.get('symbol', '')
        action = alert_data.get('action', '')
        timeframe = alert_data.get('timeframe', '')
        
        # Use full timestamp with millisecond precision
        timestamp_ms = int(time.time() * 1000)
        
        # Create deterministic hash of alert content for idempotency
        import hashlib
        alert_content = f"{symbol}_{action}_{timeframe}_{alert_data.get('entry_price', '')}_{alert_data.get('risk_percent', '')}"
        content_hash = hashlib.md5(alert_content.encode()).hexdigest()[:8]
        
        return f"{symbol}_{action}_{timeframe}_{timestamp_ms}_{content_hash}"

    async def _cleanup_expired_alerts(self):
        """
        Remove expired alerts from tracking set.
        
        INSTITUTIONAL FIX: Extract timestamp from millisecond-precision alert IDs
        and clean up alerts older than timeout threshold.
        """
        current_time_ms = time.time() * 1000
        expired_alerts = set()
        
        for alert_id in self.active_alerts:
            # Extract millisecond timestamp from alert_id
            # Format: symbol_action_timeframe_timestamp_ms_hash
            try:
                parts = alert_id.split('_')
                if len(parts) >= 5:
                    # Second-to-last element is timestamp_ms
                    timestamp_ms = int(parts[-2])
                    if (current_time_ms - timestamp_ms) / 1000 > self.alert_timeout:
                        expired_alerts.add(alert_id)
            except (ValueError, IndexError) as e:
                # If we can't parse the timestamp, keep it for safety
                # Only remove alerts we're certain have expired
                logger.warning(f"Unable to parse alert_id timestamp: {alert_id}, error: {e}")
        
        for alert_id in expired_alerts:
            self.active_alerts.discard(alert_id)
            logger.debug(f"🧹 Cleaned up expired alert: {alert_id}")
        
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
        """
        Main entry point for processing alerts with military-grade idempotency.
        
        CRITICAL: Prevents duplicate execution that causes double position entry,
        excessive transaction costs, and unintended leverage exposure.
        """
        # Acquire lock BEFORE generating alert ID to prevent race conditions
        async with self._lock:
            # MONITORING: Track total alerts received
            self.duplicate_stats["total_alerts_received"] += 1
            
            # Generate unique alert ID for duplicate detection
            alert_id = self._generate_alert_id(raw_alert_data)
            logger.info(f"--- Processing Alert ID: {alert_id} ---")
            
            # Clean up expired alerts
            await self._cleanup_expired_alerts()
            
            # INSTITUTIONAL FIX: Check for duplicate alerts within critical section
            if alert_id in self.active_alerts:
                # MONITORING: Track duplicate detection
                self.duplicate_stats["duplicates_blocked"] += 1
                self.duplicate_stats["last_duplicate_timestamp"] = time.time()
                
                symbol = raw_alert_data.get("symbol", "UNKNOWN")
                if symbol not in self.duplicate_stats["duplicate_symbols"]:
                    self.duplicate_stats["duplicate_symbols"][symbol] = 0
                self.duplicate_stats["duplicate_symbols"][symbol] += 1
                
                logger.warning(f"❌ DUPLICATE ALERT BLOCKED: {alert_id} (Total blocked: {self.duplicate_stats['duplicates_blocked']})")
                return {
                    "status": "ignored", 
                    "message": "Duplicate alert detected - execution blocked", 
                    "alert_id": alert_id,
                    "duplicate_stats": self.duplicate_stats
                }
            
            # Add to active alerts set within critical section to prevent race
            self.active_alerts.add(alert_id)
            self.duplicate_stats["alerts_processed"] += 1
            logger.info(f"✅ Alert registered for execution: {alert_id} (Total processed: {self.duplicate_stats['alerts_processed']})")
            
            # Standardize alert data
            alert = self._standardize_alert(raw_alert_data)
            symbol = alert.get("symbol")
            
            if not self._started:
                logger.error("Cannot process alert: Handler is not started.")
                # Remove from active alerts if handler not started
                self.active_alerts.discard(alert_id)
                return {"status": "error", "message": "Handler not started"}

        # Release lock before processing - we've already checked for duplicates
        try:
            action = alert.get("action")

            # OANDA preflight health gate: queue alerts if connectivity is degraded
            try:
                if hasattr(self.oanda_service, 'can_trade') and not self.oanda_service.can_trade():
                    self.queued_alerts.append(alert)
                    logger.warning("🚧 OANDA connectivity degraded; alert queued for later processing")
                    # Provide actionable diagnostics
                    conn = {}
                    if hasattr(self.oanda_service, 'get_connection_status'):
                        try:
                            conn = await self.oanda_service.get_connection_status()
                        except Exception:
                            conn = {"error": "status_unavailable"}
                    return {
                        "status": "queued",
                        "message": "OANDA connectivity degraded; alert queued",
                        "alert_id": alert_id,
                        "connection": conn
                    }
            except Exception as e:
                logger.warning(f"Preflight health gate failed open: {e}")
            
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
            logger.error(f"❌ CRITICAL ERROR processing alert {alert_id}: {e}", exc_info=True)
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
                # Update duplicate detection stats
                self.duplicate_stats["duplicates_blocked"] += 1
                self.duplicate_stats["last_duplicate_timestamp"] = datetime.now(timezone.utc).isoformat()
                self.duplicate_stats["duplicate_symbols"][symbol] = self.duplicate_stats["duplicate_symbols"].get(symbol, 0) + 1
                return {
                    "status": "ignored",
                    "message": f"Recent {action} position exists for {symbol}",
                    "alert_id": alert_id
                }
            else:
                logger.info(f"✅ Duplicate check passed for {position_key} (last trade was {time_diff:.1f}s ago)")
        else:
            logger.info(f"✅ No recent trades found for {position_key}")
        
        # CRITICAL FIX: Record position IMMEDIATELY to prevent duplicate webhooks from racing through
        # This must happen BEFORE any async operations that could allow concurrent webhooks to pass the duplicate check
        self._recent_positions[position_key] = current_time
        logger.info(f"🔒 Locked {position_key} to prevent duplicates (valid for 60s)")
        
        try:
            # ML META-FILTER: Validate signal quality before proceeding
            logger.info("🤖 Running ML meta-filter on signal...")
            
            # Build market context for ML filter
            ml_context = await self._build_ml_context(symbol, action)
            
            # Enhance signal with ML confidence
            ml_result = await enhance_tradingview_signal(alert, ml_context)
            
            ml_approved = ml_result.get("approved", True)
            ml_confidence = ml_result.get("confidence", 0.5)
            ml_reason = ml_result.get("reason", "No reason provided")
            
            logger.info(f"🤖 ML Filter: approved={ml_approved}, confidence={ml_confidence:.2f}, reason={ml_reason}")
            
            if not ml_approved:
                logger.warning(f"❌ ML Meta-Filter rejected signal: {ml_reason}")
                return {
                    "status": "ml_rejected",
                    "reason": ml_reason,
                    "confidence": ml_confidence,
                    "alert_id": alert_id
                }
            
            logger.info(f"✅ ML Meta-Filter approved signal with confidence {ml_confidence:.2f}")
            
            # Continue with risk manager validation
            is_allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage=risk_percent / 100.0, symbol=symbol, action=action)
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
            # --- INSTITUTIONAL STOP LOSS VALIDATION ---
            # Minimum stop distance based on instrument type
            if instrument_type == 'crypto':
                min_stop_percent = 0.0150  # 1.5% minimum for crypto (high volatility)
                min_stop_pips = 50  # 50 pips absolute minimum for OANDA crypto
            elif instrument_type == 'commodity':
                min_stop_percent = 0.0120  # 1.2% for gold, oil, etc.
                min_stop_pips = 30
            else:  # Forex
                min_stop_percent = 0.0100  # 1.0% for forex pairs
                min_stop_pips = 20

            # 15m-specific floor widening (safer in live conditions)
            timeframe_str = str(timeframe).upper()
            if timeframe_str in ('15', '15M', 'M15'):
                if instrument_type == 'crypto':
                    # Raise crypto floor from 1.5% -> 2.0%
                    min_stop_percent = max(min_stop_percent, 0.0200)
                elif instrument_type == 'commodity':
                    # Leave commodity unchanged for now
                    pass
                else:
                    # Raise FX floor from 1.0% -> 1.2%
                    min_stop_percent = max(min_stop_percent, 0.0120)
            
            if action == "BUY":
                # Calculate ATR-based stop
                stop_loss_price = entry_price - (atr * atr_multiplier)
                stop_distance = entry_price - stop_loss_price
                stop_distance_percent = stop_distance / entry_price
                
                # Enforce minimums
                if stop_loss_price >= entry_price:
                    logger.error(f"❌ INVALID STOP: Stop {stop_loss_price} >= Entry {entry_price} for BUY")
                    stop_loss_price = entry_price - (entry_price * min_stop_percent)
                    logger.warning(f"⚠️ Corrected to minimum stop: {stop_loss_price}")
                elif stop_distance_percent < min_stop_percent:
                    logger.warning(f"⚠️ Stop too tight: {stop_distance_percent:.4%} < {min_stop_percent:.4%}")
                    stop_loss_price = entry_price - (entry_price * min_stop_percent)
                    logger.warning(f"⚠️ Adjusted stop loss for BUY: {stop_loss_price:.5f}")
            else:  # SELL
                # Calculate ATR-based stop
                stop_loss_price = entry_price + (atr * atr_multiplier)
                stop_distance = stop_loss_price - entry_price
                stop_distance_percent = stop_distance / entry_price
                
                # Enforce minimums
                if stop_loss_price <= entry_price:
                    logger.error(f"❌ INVALID STOP: Stop {stop_loss_price} <= Entry {entry_price} for SELL")
                    stop_loss_price = entry_price + (entry_price * min_stop_percent)
                    logger.warning(f"⚠️ Corrected to minimum stop: {stop_loss_price}")
                elif stop_distance_percent < min_stop_percent:
                    logger.warning(f"⚠️ Stop too tight: {stop_distance_percent:.4%} < {min_stop_percent:.4%}")
                    stop_loss_price = entry_price + (entry_price * min_stop_percent)
                    logger.warning(f"⚠️ Adjusted stop loss for SELL: {stop_loss_price:.5f}")
            
            logger.info(f"🎯 Entry SL for {symbol}: SL={stop_loss_price:.5f} (ATR={atr:.5f}, multiplier={atr_multiplier}, distance={stop_distance_percent:.4%})")
            
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
                pip_size = 0.01 if 'JPY' in symbol else 0.0001
                if action == "BUY":
                    stop_loss_pips = (entry_price - stop_loss_price) / pip_size
                else:
                    stop_loss_pips = (stop_loss_price - entry_price) / pip_size
                
                # Use the ATR-based position sizing
                position_size = calculate_position_size(
                    account_balance=account_balance,
                    risk_percent=risk_percent,
                    stop_loss_pips=stop_loss_pips,
                    symbol=symbol,
                    current_price=entry_price
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
            
            # DIAGNOSTIC: Log position size before trade execution
            logger.info(f"🔍 PRE-TRADE DEBUG: symbol={symbol}, position_size={position_size}, final_units={final_units}, stop_loss={stop_loss_price:.5f}")
            
            trade_payload = {
                "symbol": symbol,
                "action": action,
                "units": final_units,
                "stop_loss": stop_loss_price
            }
            logger.info(f"🔍 TRADE PAYLOAD: {trade_payload}")
            # Capture pre-trade snapshot for implementation shortfall
            requested_price = entry_price
            t0 = time.time()
            success, result = await self.oanda_service.execute_trade(trade_payload)
            if success:
                latency_ms = int((time.time() - t0) * 1000)
                # Position already recorded upfront to prevent duplicates - no need to record again
                if alert_id:
                    position_id = alert_id
                    logger.info(f"🎯 Using TradingView-generated position_id: {position_id}")
                else:
                    position_id = f"{symbol}_{int(time.time())}"
                    logger.warning(f"⚠️ No TradingView position_id provided, using fallback: {position_id}")
                # INSTITUTIONAL FIX: Check for duplicate position before recording
                existing_position = await self.position_tracker.get_position_info(position_id)
                if existing_position and existing_position.get('status') == 'open':
                    logger.error(f"🚨 DUPLICATE POSITION DETECTED: {position_id} already exists in tracker")
                    # Close the duplicate OANDA position immediately
                    try:
                        await self.oanda_service.close_position(symbol, result['units'])
                        logger.warning(f"⚠️ Closed duplicate OANDA position for {symbol}")
                    except Exception as close_error:
                        logger.error(f"❌ Failed to close duplicate position: {close_error}")
                    
                    return {
                        "status": "error",
                        "message": f"Duplicate position detected and closed: {position_id}",
                        "alert_id": alert_id
                    }
                
                # Compute slippage (implementation shortfall): actual fill vs requested
                actual_fill_price = float(result['fill_price']) if 'fill_price' in result else requested_price
                slippage = float(actual_fill_price) - float(requested_price)
                await self.position_tracker.record_position(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], stop_loss=stop_loss_price, take_profit=None,
                    metadata={"alert_id": alert_id, "transaction_id": result['transaction_id']}
                )
                # Register risk immediately
                try:
                    await self.risk_manager.register_position(
                        position_id=position_id,
                        symbol=symbol,
                        action=action,
                        size=float(result['units']),
                        entry_price=float(result['fill_price']),
                        account_risk=risk_percent / 100.0,
                        stop_loss=stop_loss_price,
                        timeframe=str(alert.get("timeframe", "H1"))
                    )
                except Exception as e:
                    logger.warning(f"Risk registration failed for {position_id}: {e}")
                await position_journal.record_entry(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], strategy=alert.get("strategy", "N/A"),
                    stop_loss=stop_loss_price, take_profit=None,
                    execution_time=latency_ms / 1000.0, slippage=slippage,
                    market_regime="unknown", volatility_state="normal"
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
                # Get current price for PnL calculation with proper error handling
                try:
                    current_price = await self.oanda_service.get_current_price(symbol, "SELL")
                    logger.info(f"🎯 [FALLBACK DEBUG] Got current price for {symbol}: {current_price}")
                except Exception as e:
                    logger.error(f"❌ Exception getting current price in fallback for {symbol}: {e}")
                    current_price = None
                
                # Safety check for current_price in fallback
                if current_price is None or current_price <= 0:
                    logger.error(f"❌ Invalid current price in fallback for {symbol}: {current_price}")
                    current_price = 1.0  # Use fallback price to prevent TypeError
                
                # Try to close directly via OANDA
                success, result = await self.oanda_service.close_position(symbol, 1000)  # Use default units
                
                if success:
                    logger.info(f"✅ Successfully closed position directly via OANDA for {symbol}")
                    # Use actual close price from result, fallback to current_price
                    actual_close_price = result.get('price', current_price)
                    exit_price = float(actual_close_price) if actual_close_price is not None else current_price
                    return {
                        "status": "success",
                        "position_id": f"OANDA_DIRECT_{symbol}",
                        "exit_price": exit_price,
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
                        # Use the actual close price from OANDA with safety checks
                        close_price_raw = result.get('price', current_price)
                        if close_price_raw is None:
                            logger.warning(f"⚠️ No close price in OANDA result, using current_price: {current_price}")
                            close_price_raw = current_price
                        try:
                            actual_close_price = float(close_price_raw)
                        except (TypeError, ValueError) as e:
                            logger.error(f"❌ Invalid close price data: {close_price_raw}, using current_price: {current_price}")
                            actual_close_price = float(current_price)
                        
                        logger.info(f"✅ Using close price: {actual_close_price}")
                        close_result = await self.position_tracker.close_position(target_position_id, actual_close_price, "Signal")
                        # Clear from risk manager
                        try:
                            await self.risk_manager.clear_position(target_position_id)
                        except Exception as e:
                            logger.warning(f"Risk clear failed for {target_position_id}: {e}")
                        await position_journal.record_exit(
                            position_id=target_position_id,
                            exit_price=actual_close_price,
                            exit_reason="Signal",
                            pnl=close_result.position_data.get('pnl', 0) if close_result.position_data else 0
                        )
                        logger.info(f"✅ Successfully closed position {target_position_id} for {symbol} at {actual_close_price}")
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

    async def _execute_close_internal(self, symbol: str, position_id: str, size: float, reason: str) -> Dict[str, Any]:
        """
        Internal method for direct position closure without override checks
        Used by emergency systems like orphaned trade monitor
        
        Args:
            symbol: Trading symbol (e.g., 'BTC_USD')
            position_id: Position identifier
            size: Position size in units
            reason: Closure reason for tracking
            
        Returns:
            Dict with status, exit_price, pnl
        """
        try:
            logger.info(f"🚨 INTERNAL CLOSE: {symbol} position {position_id} (reason: {reason})")
            
            # Get current price for PnL calculation
            position_data = await self.position_tracker.get_position_info(position_id)
            if not position_data:
                return {"status": "error", "message": f"Position {position_id} not found"}
                
            action = position_data['action']
            current_price = await self.oanda_service.get_current_price(symbol, action)
            
            if not current_price:
                logger.error(f"❌ Could not get current price for {symbol}")
                return {"status": "error", "message": "Price unavailable"}
                
            # Execute close on OANDA
            success, result = await self.oanda_service.close_position(symbol, size)
            
            if not success:
                logger.error(f"❌ OANDA close failed: {result}")
                return {"status": "error", "message": "OANDA close failed", "details": result}
                
            # Extract exit price
            exit_price = result.get('price', current_price)
            if exit_price is None:
                exit_price = current_price
                
            # Close in tracker
            await self.position_tracker.close_position(
                position_id=position_id,
                exit_price=exit_price,
                reason=reason
            )
            
            # Calculate PnL
            entry_price = position_data['entry_price']
            if action == 'BUY':
                pnl = (exit_price - entry_price) * size
            else:
                pnl = (entry_price - exit_price) * size
                
            # Update risk manager
            if hasattr(self.risk_manager, 'clear_position'):
                await self.risk_manager.clear_position(position_id)
            
            logger.info(f"✅ INTERNAL CLOSE SUCCESS: {symbol} at {exit_price} (PnL: ${pnl:.2f})")
            
            return {
                "status": "success",
                "position_id": position_id,
                "exit_price": exit_price,
                "pnl": pnl
            }
            
        except Exception as e:
            logger.error(f"❌ Internal close failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

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

    async def _build_ml_context(self, symbol: str, action: str) -> Dict[str, Any]:
        """
        Build market context for ML meta-filter.
        Gathers real-time and historical metrics for signal quality assessment.
        """
        context = {}
        
        try:
            # Get recent performance from position journal
            recent_stats = position_journal.get_statistics()
            context["recent_win_rate"] = recent_stats.get("win_rate", 0.5)
            context["avg_recent_pnl"] = recent_stats.get("avg_pnl", 0.0)
            context["consecutive_losses"] = recent_stats.get("consecutive_losses", 0)
            
            # Get portfolio heat from position tracker
            open_positions = await self.position_tracker.get_all_positions()
            context["portfolio_heat"] = min(len(open_positions) / 10.0, 1.0)  # Normalize to [0, 1]
            
            # Get volatility metrics from OANDA
            try:
                df = await self.oanda_service.get_historical_data(symbol, count=50, granularity="H1")
                if df is not None and not df.empty:
                    from technical_analysis import get_atr as get_atr_from_df
                    current_atr = get_atr_from_df(df)
                    
                    # Calculate historical ATR average
                    df_100 = await self.oanda_service.get_historical_data(symbol, count=100, granularity="H1")
                    if df_100 is not None and not df_100.empty:
                        hist_atr = get_atr_from_df(df_100)
                        context["current_atr"] = current_atr or 0.0
                        context["avg_atr"] = hist_atr or 1.0
                        context["volatility_percentile"] = min(current_atr / hist_atr, 2.0) / 2.0 if hist_atr > 0 else 0.5
                    else:
                        context["current_atr"] = current_atr or 0.0
                        context["avg_atr"] = 1.0
                        context["volatility_percentile"] = 0.5
                else:
                    context["current_atr"] = 0.0
                    context["avg_atr"] = 1.0
                    context["volatility_percentile"] = 0.5
            except Exception as e:
                logger.warning(f"Could not fetch volatility metrics: {e}")
                context["current_atr"] = 0.0
                context["avg_atr"] = 1.0
                context["volatility_percentile"] = 0.5
            
            # Get spread from OANDA pricing
            try:
                pricing_info = await self.oanda_service._get_pricing_info(symbol)
                if pricing_info:
                    bid = float(pricing_info.get("bids", [{}])[0].get("price", 0))
                    ask = float(pricing_info.get("asks", [{}])[0].get("price", 0))
                    mid = (bid + ask) / 2.0
                    spread_bps = ((ask - bid) / mid * 10000) if mid > 0 else 2.0
                    context["spread_bps"] = spread_bps
                else:
                    context["spread_bps"] = 2.0
            except Exception as e:
                logger.warning(f"Could not fetch spread: {e}")
                context["spread_bps"] = 2.0
            
            # Get correlation risk from risk manager
            try:
                if hasattr(self.risk_manager, 'correlation_manager'):
                    corr_mgr = self.risk_manager.correlation_manager
                    if corr_mgr and hasattr(corr_mgr, 'get_highest_correlation'):
                        context["correlation_risk"] = abs(corr_mgr.get_highest_correlation(symbol))
                    else:
                        context["correlation_risk"] = 0.0
                else:
                    context["correlation_risk"] = 0.0
            except Exception as e:
                logger.warning(f"Could not fetch correlation risk: {e}")
                context["correlation_risk"] = 0.0
            
            # Regime and trend (simplified - can be enhanced)
            context["regime_score"] = 0.0  # Neutral by default
            context["trend_alignment"] = 0.5  # Neutral by default
            
            logger.debug(f"ML context built: {context}")
            
        except Exception as e:
            logger.error(f"Error building ML context: {e}", exc_info=True)
            # Return safe defaults
            context = {
                "recent_win_rate": 0.5,
                "avg_recent_pnl": 0.0,
                "consecutive_losses": 0,
                "portfolio_heat": 0.0,
                "current_atr": 0.0,
                "avg_atr": 1.0,
                "volatility_percentile": 0.5,
                "spread_bps": 2.0,
                "correlation_risk": 0.0,
                "regime_score": 0.0,
                "trend_alignment": 0.5
            }
        
        return context
    
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
        
        # Institutional bounds with instrument-specific caps
        # Crypto needs wider stops due to volatility
        if isinstance(instrument_type, str) and 'crypto' in instrument_type.lower():
            max_mult = 6.0  # Allow up to 6x ATR for crypto
        elif isinstance(instrument_type, str):
            classified_type = classify_instrument(str(instrument_type))
            max_mult = 6.0 if classified_type == 'crypto' else 4.0
        else:
            max_mult = 4.0
        
        # Apply bounds: minimum 1.0, maximum based on instrument type
        final_multiplier = max(1.0, min(max_mult, final_multiplier))
        
        return round(final_multiplier, 2)