import configparser
import os
import json
import logging
import asyncio
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List
from oandapyV20 import V20Error
from oandapyV20.endpoints.positions import PositionClose
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo
import oandapyV20
from pydantic import SecretStr
from config import config
import error_recovery
from utils import (
    logger, get_module_logger, normalize_timeframe, standardize_symbol, 
    is_instrument_tradeable, get_atr, get_instrument_type, 
    get_atr_multiplier, get_trading_logger, parse_iso_datetime,
    _get_simulated_price, validate_trade_inputs, calculate_position_risk_amount, TV_FIELD_MAP, MarketDataUnavailableError, calculate_simple_position_size, get_position_size_limits
)
# from dynamic_exit_manager import HybridExitManager  # (restored, commented out)

class EnhancedAlertHandler:
    def __init__(self, db_manager=None):
        """Initialize alert handler with proper defaults"""
        # Initialize all attributes to None first
        self.position_tracker = None
        self.risk_manager = None
        self.volatility_monitor = None
        self.market_structure = None
        self.regime_classifier = None
        self.position_journal = None
        self.notification_system = None
        self.system_monitor = None
        
        # Set the db_manager
        self.db_manager = db_manager
        
        # Other initialization code...
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        
        # Configuration flags
        self.enable_reconciliation = getattr(config, 'enable_broker_reconciliation', True)
        self.enable_close_overrides = True
        
        # Initialize OANDA client
        self._init_oanda_client()
        
        # self.hybrid_exit_manager = HybridExitManager()  # (restored, commented out)
        
        logger.info("EnhancedAlertHandler initialized with default values")

    def _init_oanda_client(self):
        """Initialize OANDA client"""
        try:
            access_token = config.oanda_access_token
            if isinstance(access_token, object) and hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            
            self.oanda = oandapyV20.API(
                access_token=access_token,
                environment=config.oanda_environment
            )
            logger.info(f"OANDA client initialized in alert handler")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA client: {e}")
            self.oanda = None

    def validate_trade_inputs(
        units: float,
        risk_percent: float,
        atr: float,
        stop_loss_distance: float,
        min_units: float,
        max_units: float
    ) -> tuple[bool, str]:
        """
        Validate trade inputs before execution
        Returns: (is_valid: bool, reason: str)
        """
        # Check units range
        if units <= 0:
            return False, f"Units must be positive (got {units})"
        
        if units < min_units:
            return False, f"Units {units} below minimum {min_units}"
        
        if units > max_units:
            return False, f"Units {units} exceeds maximum {max_units}"
        
        # Check risk percentage
        if risk_percent <= 0.5:  # Minimum 0.5% risk
            return False, f"Risk {risk_percent}% below minimum 0.5%"
        
        if risk_percent > config.max_risk_percentage:
            return False, f"Risk {risk_percent}% exceeds maximum {config.max_risk_percentage}%"
        
        # Check ATR validity
        if atr <= 0.00001:  # Very small ATR threshold
            return False, f"ATR {atr} below minimum threshold 0.00001"
        
        # Check stop loss distance
        if stop_loss_distance <= 0:
            return False, f"Stop loss distance {stop_loss_distance} must be positive"
        
        if stop_loss_distance < 0.0001:  # Minimum 1 pip for major pairs
            return False, f"Stop loss distance {stop_loss_distance} below minimum 0.0001"
        
        # All validations passed
        return True, "Trade inputs validated successfully"

    async def robust_oanda_request(self, request, max_retries: int = 3, initial_delay: float = 1.0):
        """Make robust OANDA API request with retries"""
        if not self.oanda:
            self._init_oanda_client()
            if not self.oanda:
                raise Exception("OANDA client not initialized")
        
        for attempt in range(max_retries):
            try:
                response = self.oanda.request(request)
                return response
            except Exception as e:
                if attempt == max_retries - 1:
                    raise error_recovery.BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")
                await asyncio.sleep(initial_delay * (2 ** attempt))
                logger.warning(f"OANDA request attempt {attempt + 1} failed, retrying: {e}")


    async def get_current_price(self, symbol: str, action: str) -> float:
        """Get current price for symbol"""
        try:
            pricing_request = PricingInfo(
                accountID=config.oanda_account_id,
                params={"instruments": symbol}
            )
            response = await self.robust_oanda_request(pricing_request)
            
            if 'prices' in response and response['prices']:
                price_data = response['prices'][0]
                if action.upper() == "BUY":
                    return float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
                else:
                    return float(price_data.get('bid', price_data.get('closeoutBid', 0)))
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            
        # Fallback to simulated price
        return _get_simulated_price(symbol, action)

    async def get_account_balance(self, use_fallback: bool = False) -> float:
        """Get account balance from OANDA or return fallback"""
        if use_fallback:
            return 10000.0  # Fallback balance for startup
        
        try:
            account_request = AccountDetails(accountID=config.oanda_account_id)
            response = await self.robust_oanda_request(account_request)
            return float(response['account']['balance'])
        except Exception as e:
            logger.error(f"Error getting account balance: {e}")
            return 10000.0  # Fallback


    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        """Execute trade with OANDA"""
        try:
            symbol = payload.get("symbol")
            action = payload.get("action")
            risk_percent = payload.get("risk_percent", 1.0)
            # Pre-trade checks
            if not symbol or not action:
                logger.error(f"Trade execution aborted: Missing symbol or action in payload: {payload}")
                return False, {"error": "Missing symbol or action in trade payload"}
            # Get account balance
            account_balance = await self.get_account_balance()
            # Get current price
            try:
            current_price = await self.get_current_price(symbol, action)
            except MarketDataUnavailableError as e:
                logger.error(f"Trade execution aborted: {e}")
                return False, {"error": str(e)}
            # Get stop loss (assume it's provided in payload or calculate using ATR if not)
            stop_loss = payload.get("stop_loss")
            if stop_loss is None:
                try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"))
                except MarketDataUnavailableError as e:
                    logger.error(f"Trade execution aborted: {e}")
                    return False, {"error": str(e)}
                stop_loss = current_price - (atr * config.atr_stop_loss_multiplier) if action.upper() == "BUY" else current_price + (atr * config.atr_stop_loss_multiplier)
            else:
                atr = None  # Will be set below if needed
            stop_distance = abs(current_price - stop_loss)
            if stop_distance <= 0:
                logger.error(f"Trade execution aborted: Invalid stop loss distance: {stop_distance}")
                return False, {"error": "Invalid stop loss distance"}
            # Use universal position sizing
            position_size = calculate_simple_position_size(
                account_balance=account_balance,
                risk_percent=risk_percent,
                entry_price=current_price,
                stop_loss=stop_loss,
                symbol=symbol
            )
            if position_size <= 0:
                logger.error(f"Trade execution aborted: Calculated position size is zero or negative")
                return False, {"error": "Calculated position size is zero or negative"}
            min_units, max_units = get_position_size_limits(symbol)
            # Get ATR for validation (reuse if already calculated above)
            if atr is None:
                try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"))
                except MarketDataUnavailableError as e:
                    logger.error(f"Trade execution aborted: {e}")
                    return False, {"error": str(e)}
            is_valid, validation_reason = validate_trade_inputs(
                units=position_size,
                risk_percent=risk_percent,
                atr=atr,
                stop_loss_distance=stop_distance,
                min_units=min_units,
                max_units=max_units
            )
            if not is_valid:
                logger.error(f"Trade validation failed for {symbol}: {validation_reason}")
                return False, {"error": f"Trade validation failed: {validation_reason}"}
            logger.info(f"Trade validation passed for {symbol}: {validation_reason}")
            # Create OANDA order
            from oandapyV20.endpoints.orders import OrderCreate
            from config import config
            order_data = {
                "order": {
                    "type": "MARKET",
                    "instrument": symbol,
                    "units": str(position_size) if action.upper() == "BUY" else str(-position_size),
                    "timeInForce": "FOK"
                }
            }
            order_request = OrderCreate(
                accountID=config.oanda_account_id,
                data=order_data
            )
            response = await self.robust_oanda_request(order_request)
            if 'orderFillTransaction' in response:
                fill_info = response['orderFillTransaction']
                logger.info(
                    f"Trade execution for {symbol}: "
                    f"Account Balance=${account_balance:.2f}, "
                    f"Risk%={risk_percent:.2f}, "
                    f"Entry={current_price}, Stop={stop_loss}, "
                    f"Position Size={position_size}"
                )
                return True, {
                    "success": True,
                    "fill_price": float(fill_info.get('price', current_price)),
                    "units": abs(float(fill_info.get('units', position_size))),
                    "transaction_id": fill_info.get('id'),
                    "symbol": symbol,
                    "action": action
                }
            else:
                logger.error(f"Order not filled for {symbol}: {response}")
                return False, {"error": "Order not filled", "response": response}
        except Exception as e:
            logger.error(f"Error executing trade: {e}")
            return False, {"error": str(e)}

    async def start(self):
        """Initialize & start all components, including optional broker reconciliation."""
        if self._running:
            logger.info("EnhancedAlertHandler.start() called, but already running.")
            return True

        logger.info("Attempting to start EnhancedAlertHandler and its components...")
        startup_errors = []
        
        try:
            # Import components here to avoid circular imports
            from tracker import PositionTracker
            from risk_manager import EnhancedRiskManager
            from volatility_monitor import VolatilityMonitor
            from regime_classifier import LorentzianDistanceClassifier
            from position_journal import PositionJournal
            from notification import NotificationSystem
            from system_monitor import SystemMonitor
            
            # 1) System Monitor
            self.system_monitor = SystemMonitor()
            await self.system_monitor.register_component("alert_handler", "initializing")

            # 2) DB Manager check
            if not self.db_manager:
                logger.critical("db_manager is not initialized. Cannot proceed with startup.")
                await self.system_monitor.update_component_status(
                    "alert_handler", "error", "db_manager not initialized"
                )
                return False

            # 3) Core components registration
            self.position_tracker = PositionTracker(db_manager=self.db_manager)
            await self.system_monitor.register_component("position_tracker", "initializing")

            self.risk_manager = EnhancedRiskManager()
            await self.system_monitor.register_component("risk_manager", "initializing")

            self.volatility_monitor = VolatilityMonitor()
            await self.system_monitor.register_component("volatility_monitor", "initializing")

            self.regime_classifier = LorentzianDistanceClassifier()
            await self.system_monitor.register_component("regime_classifier", "initializing")

            self.position_journal = PositionJournal()
            await self.system_monitor.register_component("position_journal", "initializing")

            self.notification_system = NotificationSystem()
            await self.system_monitor.register_component("notification_system", "initializing")

            # 4) Configure notification channels
            if hasattr(config, 'slack_webhook_url') and config.slack_webhook_url:
                slack_url = (
                    config.slack_webhook_url.get_secret_value()
                    if isinstance(config.slack_webhook_url, SecretStr)
                    else config.slack_webhook_url
                )
                if slack_url:
                    await self.notification_system.configure_channel("slack", {"webhook_url": slack_url})

            if (hasattr(config, 'telegram_bot_token') and config.telegram_bot_token and 
                hasattr(config, 'telegram_chat_id') and config.telegram_chat_id):
                telegram_token = (
                    config.telegram_bot_token.get_secret_value()
                    if isinstance(config.telegram_bot_token, SecretStr)
                    else config.telegram_bot_token
                )
                telegram_chat_id = config.telegram_chat_id
                await self.notification_system.configure_channel(
                    "telegram",
                    {"bot_token": telegram_token, "chat_id": telegram_chat_id}
                )

            await self.notification_system.configure_channel("console", {})
            logger.info("Notification channels configured.")
            await self.system_monitor.update_component_status("notification_system", "ok")

            # 5) Start components with error handling
            try:
                logger.info("Starting PositionTracker...")
                await self.position_tracker.start()
                await self.system_monitor.update_component_status("position_tracker", "ok")
            except Exception as e:
                startup_errors.append(f"PositionTracker failed: {e}")
                await self.system_monitor.update_component_status("position_tracker", "error", str(e))

            try:
                logger.info("Initializing RiskManager...")
                balance = await self.get_account_balance(use_fallback=True)
                await self.risk_manager.initialize(balance)
                await self.system_monitor.update_component_status("risk_manager", "ok")
            except Exception as e:
                startup_errors.append(f"RiskManager failed: {e}")
                await self.system_monitor.update_component_status("risk_manager", "error", str(e))
                # Initialize with fallback balance
                try:
                    await self.risk_manager.initialize(10000.0)
                    logger.warning("RiskManager initialized with fallback balance")
                except Exception as fallback_error:
                    logger.error(f"RiskManager fallback initialization failed: {fallback_error}")

            # Mark monitors OK (no explicit .start())
            await self.system_monitor.update_component_status("volatility_monitor", "ok")
            await self.system_monitor.update_component_status("regime_classifier", "ok")

            # HybridExitManager startup logic (restored, commented out):
            # """
            # try:
            #     logger.info("Starting HybridExitManager…")
            #     await self.hybrid_exit_manager.start()
            #     await self.system_monitor.update_component_status("hybrid_exit_manager", "ok")
            # except Exception as e:
            #     startup_errors.append(f"HybridExitManager failed: {e}")
            #     await self.system_monitor.update_component_status("hybrid_exit_manager", "error", str(e))
            # """

            await self.system_monitor.update_component_status("position_journal", "ok")

            # 6) Broker reconciliation (optional and graceful)
            if self.enable_reconciliation:
                try:
                    logger.info("Performing initial broker reconciliation...")
                    await self.reconcile_positions_with_broker()
                    logger.info("Initial broker reconciliation complete.")
                except Exception as e:
                    startup_errors.append(f"Broker reconciliation failed: {e}")
                    logger.warning(f"Broker reconciliation failed, but system will continue: {e}")
            else:
                logger.info("Broker reconciliation skipped by configuration.")

            # Finalize startup
            self._running = True
            
            # Determine overall startup status
            if len(startup_errors) == 0:
                status_msg = "EnhancedAlertHandler started successfully."
                await self.system_monitor.update_component_status("alert_handler", "ok", status_msg)
                logger.info(status_msg)
            else:
                status_msg = f"EnhancedAlertHandler started with {len(startup_errors)} errors."
                await self.system_monitor.update_component_status("alert_handler", "warning", status_msg)
                logger.warning(status_msg)
                for error in startup_errors:
                    logger.warning(f"Startup error: {error}")

            # Send a startup notification
            try:
                open_count = len(getattr(self.position_tracker, "positions", {})) if self.position_tracker else 0
                notification_msg = f"EnhancedAlertHandler started. Open positions: {open_count}."
                if startup_errors:
                    notification_msg += f" ({len(startup_errors)} startup warnings - check logs)"
                await self.notification_system.send_notification(notification_msg, "info")
            except Exception as e:
                logger.warning(f"Failed to send startup notification: {e}")

            return True  # Return True even with non-critical errors

        except Exception as e:
            logger.error(f"CRITICAL FAILURE during startup: {e}", exc_info=True)
            if self.system_monitor:
                await self.system_monitor.update_component_status(
                    "alert_handler", "error", f"Startup failure: {e}"
                )
            if self.notification_system:
                try:
                    await self.notification_system.send_notification(
                        f"CRITICAL ALERT: startup failed: {e}", "critical"
                    )
                except Exception:
                    logger.error("Failed to send critical startup notification.", exc_info=True)
            return False

    async def _close_position(self, symbol: str):
        """Close a position with validation"""
        try:
            # FIRST: Check if position actually exists in OANDA
            open_positions = await self.get_open_positions()
            # For OANDA, you may want to check broker positions as well if needed
            # Example: positions_data = await self.oanda_api.get_open_positions() if you have such a method
            # For now, use internal tracking

            # Check if there is an open position for the symbol
            symbol_positions = open_positions.get(symbol, {})
            if not symbol_positions:
                logger.warning(f"Position {symbol} doesn't exist in tracker - clearing from tracker")
                await self.position_tracker.clear_position(symbol)
                await self.risk_manager.clear_position(symbol)
                return True
                
            # Position exists, proceed with normal close
            # ... rest of your existing close logic
            
        except Exception as e:
            logger.error(f"Error validating position for {symbol}: {str(e)}")
            return False

    def _resolve_tradingview_symbol(self, alert_data: dict) -> str:
        """Attempt to resolve TradingView template variables for symbol."""
        # Try common fields that TradingView might send
        for key in ["ticker", "symbol", "instrument"]:
            value = alert_data.get(key)
            if value and not ("{{" in value):
                return value
        # Try mapped fields
        for key in ["SYMBOL", "TICKER"]:
            value = alert_data.get(key)
            if value and not ("{{" in value):
                return value
        return None

    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            
            mapped_fields = {}
            for tv_field, expected_field in TV_FIELD_MAP.items():
                if tv_field in alert_data:
                    mapped_fields[expected_field] = alert_data[tv_field]
                    logger.info(f"[FIELD MAPPING] {tv_field}='{alert_data[tv_field]}' → {expected_field}")
            
            # Merge mapped fields back into alert_data
            alert_data.update(mapped_fields)
            
            # Ensure we have required fields
            if "direction" not in alert_data and "action" not in alert_data:
                if "side" in alert_data:
                    alert_data["direction"] = alert_data["side"]
                else:
                    logger.error("No direction/action field found in alert data")
                    return {"status": "error", "message": "Missing direction/action field", "alert_id": str(uuid.uuid4())}

            # Robust symbol handling with template resolution
            if "symbol" in alert_data:
                original_symbol = alert_data["symbol"]
                
                # Check if we received a template variable
                if original_symbol in ["{{ticker}}", "{{symbol}}", "{{instrument}}"]:
                    logger.warning(f"[TEMPLATE] Received template variable '{original_symbol}' - resolving...")
                    
                    # Try to resolve the template
                    resolved_symbol = self._resolve_tradingview_symbol(alert_data)
                    
                    if resolved_symbol:
                        original_symbol = resolved_symbol
                        logger.info(f"[TEMPLATE RESOLVED] {{{{ticker}}}} → {resolved_symbol}")
                    else:
                        logger.error(f"[TEMPLATE ERROR] Could not resolve '{original_symbol}' - no fallback available")
                        return {"status": "error", "message": f"Could not resolve template variable: {original_symbol}", "alert_id": str(uuid.uuid4())}
                
                # Standardize the symbol
                standardized_symbol = standardize_symbol(original_symbol)
                
                if not standardized_symbol:
                    logger.error(f"[SYMBOL ERROR] Failed to standardize symbol: '{original_symbol}'")
                    return {"status": "error", "message": f"Failed to standardize symbol: {original_symbol}", "alert_id": str(uuid.uuid4())}
                
                alert_data["symbol"] = standardized_symbol
                alert_data["instrument"] = standardized_symbol  # OANDA expects 'instrument' field
                
                logger.info(f"[SYMBOL MAPPING] '{original_symbol}' → '{standardized_symbol}'")
            else:
                logger.error(f"[SYMBOL ERROR] No symbol provided in alert_data")
                return {"status": "error", "message": "No symbol provided", "alert_id": str(uuid.uuid4())}
            
            # Debug logging
            logger.info(f"[DEBUG] Final alert_data after mapping: {alert_data}")
            logger.info(f"[DEBUG] Direction: '{alert_data.get('direction')}' (type: {type(alert_data.get('direction'))})")
            
            alert_id_from_data = alert_data.get("id", alert_data.get("request_id"))
            alert_id = alert_id_from_data if alert_id_from_data else str(uuid.uuid4())
            
            logger_instance = get_module_logger(
                __name__, 
                symbol=alert_data.get("symbol", alert_data.get("instrument", "UNKNOWN")),
                request_id=alert_id
            )

            # Ensure alert_data has an 'id' field for consistent error reporting
            if "id" not in alert_data:
                alert_data["id"] = alert_id

            try:
                direction = alert_data.get("direction", "").upper()
                symbol = alert_data.get("instrument") or alert_data.get("symbol")
                if symbol:
                    from utils import standardize_symbol
                    symbol = standardize_symbol(symbol)
                    alert_data["symbol"] = symbol
                    if "instrument" not in alert_data:
                        alert_data["instrument"] = symbol
                risk_percent_log = alert_data.get("risk_percent", 1.0) 
                logger_instance.info(f"[PROCESS ALERT ID: {alert_id}] Symbol='{symbol}', Direction='{direction}', Risk='{risk_percent_log}%'" )
                if alert_id in self.active_alerts:
                    logger_instance.warning(f"Duplicate alert ignored: {alert_id}")
                    return {"status": "ignored", "message": "Duplicate alert", "alert_id": alert_id}
                self.active_alerts.add(alert_id)
                if self.system_monitor:
                    await self.system_monitor.update_component_status("alert_handler", "processing", f"Processing alert for {symbol} {direction} (ID: {alert_id})")
                # Handle CLOSE action
                if direction == "CLOSE":
                    if not symbol:
                        logger_instance.error(f"Symbol not provided for CLOSE action. Alert ID: {alert_id}")
                        return {"status": "error", "message": "Symbol required for CLOSE action", "alert_id": alert_id}
                    
                    standardized = standardize_symbol(symbol)
                    if not standardized:
                        logger_instance.error(f"[ID: {alert_id}] Failed to standardize symbol '{symbol}' for CLOSE")
                        return {"status": "error", "message": f"Cannot close—invalid symbol format: {symbol}", "alert_id": alert_id}
        
                    # === ENHANCED POSITION MATCHING LOGIC ===
                    position_to_close = None
                    close_method = "unknown"
                    
                    # Method 1: Use alert_id to find exact position (PREFERRED)
                    incoming_alert_id = alert_data.get("alert_id")
                    position_id_from_alert = alert_data.get("position_id", incoming_alert_id)
                    
                    if position_id_from_alert and not ("{{" in position_id_from_alert):  # Skip template variables
                        position_data = await self.position_tracker.get_position_info(position_id_from_alert)
                        if position_data and position_data.get("status") == "open":
                            position_to_close = {
                                "position_id": position_id_from_alert,
                                "data": position_data
                            }
                            close_method = "exact_alert_id_match"
                            logger_instance.info(f"[CLOSE] Found exact position match by alert_id: {position_id_from_alert}")
                        else:
                            logger_instance.warning(f"[CLOSE] Alert ID {position_id_from_alert} not found or position closed")
                    else:
                        logger_instance.info(f"[CLOSE] Skipping template variable alert_id: {position_id_from_alert}")
                    
                    # Method 2: Extract direction from alert_id pattern and match by symbol + direction
                    if not position_to_close and incoming_alert_id:
                        original_direction = None
                            if "_LONG_" in incoming_alert_id or "_BUY" in incoming_alert_id:
                                original_direction = "BUY"
                            elif "_SHORT_" in incoming_alert_id or "_SELL" in incoming_alert_id:
                                original_direction = "SELL"
                        
                        if original_direction:
                            open_positions = await self.position_tracker.get_open_positions()
                            symbol_positions = open_positions.get(standardized, {})
                            
                            # Find position with matching direction
                            for pos_id, pos_data in symbol_positions.items():
                                if pos_data.get("action") == original_direction:
                                    position_to_close = {
                                        "position_id": pos_id,
                                        "data": pos_data
                                    }
                                    close_method = "symbol_direction_match"
                                    logger_instance.info(f"[CLOSE] Found position by symbol+direction: {pos_id} ({standardized} {original_direction})")
                                    break
                    
                    # Method 4: Last resort - close most recent position for symbol
                    if not position_to_close:
                        open_positions = await self.position_tracker.get_open_positions()
                        symbol_positions = open_positions.get(standardized, {})
                        
                        if symbol_positions:
                            # Get the most recent position (by open_time)
                            most_recent_pos = None
                            most_recent_time = None
                            
                            for pos_id, pos_data in symbol_positions.items():
                                open_time_str = pos_data.get("open_time")
                                if open_time_str:
                                    try:
                                        from utils import parse_iso_datetime
                                        open_time = parse_iso_datetime(open_time_str)
                                        if most_recent_time is None or open_time > most_recent_time:
                                            most_recent_time = open_time
                                            most_recent_pos = {"position_id": pos_id, "data": pos_data}
                                    except Exception:
                                        continue
                            
                            if most_recent_pos:
                                position_to_close = most_recent_pos
                                close_method = "most_recent_fallback"
                                logger_instance.warning(f"[CLOSE] Using fallback - closing most recent position: {most_recent_pos['position_id']}")
        
                    # Execute the close or return error
                    if not position_to_close:
                        logger_instance.warning(f"No open position found for CLOSE signal (symbol={standardized}, alert_id={incoming_alert_id})")
                        
                        # Log all current open positions for debugging
                        all_open = await self.position_tracker.get_open_positions()
                        logger_instance.info(f"Current open positions: {list(all_open.keys())}")
                        for sym, positions in all_open.items():
                            logger_instance.info(f"  {sym}: {list(positions.keys())}")
                        
                        return {
                            "status": "error", 
                            "message": "No matching open position found to close", 
                            "alert_id": alert_id,
                            "symbol": standardized,
                            "attempted_alert_id": incoming_alert_id,
                            "debug_info": {
                                "open_symbols": list(all_open.keys()),
                                "total_open_positions": sum(len(positions) for positions in all_open.values())
                            }
                        }
        
                    # Check if we should override the close (your existing logic)
                    should_override, reason = await self._should_override_close(
                        position_to_close["position_id"], 
                        position_to_close["data"]
                    )
                    
                    if should_override:
                        logger_instance.info(f"Ignoring CLOSE signal for {position_to_close['position_id']} ({standardized}): {reason}")
                        return {
                            "status": "ignored",
                            "symbol": standardized,
                            "position_id": position_to_close["position_id"],
                            "reason": reason,
                            "alert_id": alert_id,
                            "close_method": close_method
                        }
                    else:
                        # Execute the close
                        exit_price = await self.get_current_price(standardized, position_to_close["data"].get("action"))
                        result = await self.position_tracker.close_position(
                            position_to_close["position_id"], 
                            exit_price, 
                            reason="close_signal"
                        )
                        
                        logger_instance.info(f"Executed CLOSE for {position_to_close['position_id']} ({standardized}) using {close_method}: {result}")
                        return {
                            "status": "closed",
                            "symbol": standardized,
                            "position_id": position_to_close["position_id"],
                            "result": result,
                            "alert_id": alert_id,
                            "close_method": close_method,
                            "exit_price": exit_price
                        }
                # Validate direction for other actions (BUY/SELL)
                if direction not in ["BUY", "SELL"]:
                    logger_instance.warning(f"Unknown or invalid action type: '{direction}' for alert ID: {alert_id}")
                    return {"status": "error", "message": f"Unknown or invalid action type: {direction}", "alert_id": alert_id}
                if not symbol:
                    logger_instance.error(f"Symbol not provided for {direction} action. Alert ID: {alert_id}")
                    return {"status": "error", "message": f"Symbol required for {direction} action", "alert_id": alert_id}
                # --- Execute Trade Logic for BUY/SELL ---
                instrument = alert_data.get("instrument", symbol)
                timeframe = alert_data.get("timeframe", "H1")
                comment = alert_data.get("comment")
                account = alert_data.get("account")
                risk_percent = float(alert_data.get('risk_percent', alert_data.get('risk', alert_data.get('percentage', 1.0))))
                logger_instance.info(f"[ID: {alert_id}] Trade Execution Details: Risk Percent: {risk_percent} (Type: {type(risk_percent)})")
                standardized_instrument = standardize_symbol(instrument)
                if not standardized_instrument:
                    logger_instance.error(f"[ID: {alert_id}] Failed to standardize instrument: '{instrument}'")
                    return {"status": "rejected", "message": f"Failed to standardize instrument: {instrument}", "alert_id": alert_id}
                tradeable, reason = is_instrument_tradeable(standardized_instrument)
                logger_instance.info(f"[ID: {alert_id}] Instrument '{standardized_instrument}' tradeable: {tradeable}, Reason: {reason}")
                if not tradeable:
                    logger_instance.warning(f"[ID: {alert_id}] Market check failed for '{standardized_instrument}': {reason}")
                    return {"status": "rejected", "message": f"Trading not allowed for {standardized_instrument}: {reason}", "alert_id": alert_id}
                payload_for_execute_trade = {
                    "symbol": standardized_instrument,
                    "action": direction, # Should be "BUY" or "SELL" at this point
                    "risk_percent": risk_percent,
                    "timeframe": timeframe,
                    "comment": comment,
                    "account": account,
                    "request_id": alert_id # Using request_id as per your payload structure
                }
                logger_instance.info(f"[ID: {alert_id}] Payload for execute_trade: {json.dumps(payload_for_execute_trade)}")
                
                success, result_dict = await self.execute_trade(payload_for_execute_trade)
                
                # Standardize response from execute_trade if necessary
                if not isinstance(result_dict, dict): # Ensure it's a dict
                    logger_instance.error(f"[ID: {alert_id}] execute_trade returned non-dict: {result_dict}")
                    return {"status": "error", "message": "Trade execution failed with invalid response format.", "alert_id": alert_id}
                if "alert_id" not in result_dict: # Ensure alert_id is in the response
                    result_dict["alert_id"] = alert_id
                
                return result_dict

            except Exception as e: # Catch exceptions from the main logic
                logger_instance.error(f"Error during processing of alert ID {alert_id}: {str(e)}", exc_info=True)
                if hasattr(self, 'error_recovery') and self.error_recovery:
                    # Avoid sending overly large alert_data if it contains huge payloads
                    alert_data_summary = {k: v for k, v in alert_data.items() if isinstance(v, (str, int, float, bool)) or k == "id"}
                    await self.error_recovery.record_error(
                        "alert_processing", 
                        {"error": str(e), "alert_id": alert_id, "alert_data_summary": alert_data_summary}
                    )
                return {
                    "status": "error",
                    "message": f"Internal error processing alert: {str(e)}",
                    "alert_id": alert_id
                }
            finally: # This will always execute after try or except
                self.active_alerts.discard(alert_id)
                if self.system_monitor:
                    await self.system_monitor.update_component_status("alert_handler", "ok", f"Finished processing alert ID {alert_id}")

    async def get_position_by_symbol(self, symbol: str):
        """Returns the open position dict for a symbol, or None."""
        standardized = standardize_symbol(symbol)
        open_positions = await self.get_open_positions()  # or self.open_positions if it's a dict
        return open_positions.get(standardized)
    
    async def get_open_positions(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Get all open positions"""
        if self.position_tracker:
            return await self.position_tracker.get_open_positions()
        return {}

    async def handle_scheduled_tasks(self):
        """Handle scheduled tasks like managing exits, updating prices, etc."""
        logger.info("Starting scheduled tasks handler")
        
        last_run = {
            "update_prices": datetime.now(timezone.utc),
            "check_exits": datetime.now(timezone.utc),
            "daily_reset": datetime.now(timezone.utc),
            "position_cleanup": datetime.now(timezone.utc),
            "database_sync": datetime.now(timezone.utc)
        }
        
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                
                # Update prices every minute
                if (current_time - last_run["update_prices"]).total_seconds() >= 60:
                    await self._update_position_prices()
                    last_run["update_prices"] = current_time
                
                # Daily reset tasks
                if current_time.day != last_run["daily_reset"].day:
                    await self._perform_daily_reset()
                    last_run["daily_reset"] = current_time
                
                # Weekly position cleanup
                if (current_time - last_run["position_cleanup"]).total_seconds() >= 604_800:
                    await self._cleanup_old_positions()
                    last_run["position_cleanup"] = current_time
                
                # Database sync hourly
                if (current_time - last_run["database_sync"]).total_seconds() >= 3600:
                    await self._sync_database()
                    last_run["database_sync"] = current_time
                
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Error in scheduled tasks: {e}")
                logger.error(traceback.format_exc())
                if hasattr(self, 'error_recovery') and self.error_recovery:
                    await self.error_recovery.record_error("scheduled_tasks", {"error": str(e)})
                await asyncio.sleep(60)

    async def stop(self):
        """Clean-up hook called during shutdown."""
        logger.info("Shutting down EnhancedAlertHandler...")
        
        # Signal the scheduled-tasks loop to exit
        self._running = False
        
        # Give any in-flight iteration a moment to finish
        await asyncio.sleep(1)
        
        # Close the Postgres pool if it exists
        if hasattr(self, "db_manager") and self.db_manager:
            await self.db_manager.close()
        
        # Tear down notifications
        if hasattr(self, "notification_system") and self.notification_system:
            await self.notification_system.shutdown()
        
        logger.info("EnhancedAlertHandler shutdown complete.")

    async def _update_position_prices(self):
        """Update all open position prices"""
        if not self.position_tracker:
            return

        try:
            open_positions = await self.position_tracker.get_open_positions()

            updated_prices_symbols = {}
            position_count = 0

            for symbol, positions_data in open_positions.items():
                if not positions_data:
                    continue

                any_position = next(iter(positions_data.values()))
                direction = any_position.get("action")

                try:
                    price = await self.get_current_price(symbol, "SELL" if direction == "BUY" else "BUY")
                    updated_prices_symbols[symbol] = price

                    if self.volatility_monitor:
                        timeframe = any_position.get("timeframe", "H1")
                        atr_value = await get_atr(symbol, timeframe)
                        await self.volatility_monitor.update_volatility(symbol, atr_value, timeframe)

                    if self.regime_classifier:
                        await self.regime_classifier.add_price_data(symbol, price, any_position.get("timeframe", "H1"))

                    for position_id in positions_data:
                        await self.position_tracker.update_position_price(position_id, price)
                        position_count += 1

                except Exception as e:
                    logger.error(f"Error updating price for {symbol}: {str(e)}")

            if position_count > 0:
                logger.debug(f"Updated prices for {position_count} positions across {len(updated_prices_symbols)} symbols")

        except Exception as e:
            logger.error(f"Error updating position prices: {str(e)}")

    async def _perform_daily_reset(self):
        """Perform daily reset tasks"""
        try:
            logger.info("Performing daily reset tasks")

            if self.risk_manager:
                await self.risk_manager.reset_daily_stats()

            if self.notification_system:
                await self.notification_system.send_notification(
                    "Daily reset completed: Risk statistics reset",
                    "info"
                )

        except Exception as e:
            logger.error(f"Error in daily reset: {str(e)}")

    async def _cleanup_old_positions(self):
        """Clean up old closed positions to prevent memory growth"""
        try:
            logger.info("Cleaning up old closed positions and backups.")
            if self.position_tracker:
                # Add method to position tracker if not exists
                if hasattr(self.position_tracker, 'purge_old_closed_positions'):
                    await self.position_tracker.purge_old_closed_positions(max_age_days=30)
            logger.info("Cleanup of old data finished.")
        except Exception as e:
            logger.error(f"Error cleaning up old positions: {str(e)}")

    async def _sync_database(self):
        """Ensure all data is synced with the database"""
        try:
            logger.info("Starting database sync.")
            if self.position_tracker:
                if hasattr(self.position_tracker, 'sync_with_database'):
                    await self.position_tracker.sync_with_database()
                if hasattr(self.position_tracker, 'clean_up_duplicate_positions'):
                    await self.position_tracker.clean_up_duplicate_positions()
            logger.info("Database sync finished.")
        except Exception as e:
            logger.error(f"Error syncing database: {str(e)}")

    async def reconcile_positions_with_broker(self):
        """Simple, robust position reconciliation"""
        try:
            # Get broker positions
                from oandapyV20.endpoints.positions import OpenPositions
            request = OpenPositions(accountID=config.oanda_account_id)
            response = await self.robust_oanda_request(request)
            
            broker_positions = {}
            if 'positions' in response:
                for pos in response['positions']:
                    instrument = standardize_symbol(pos['instrument'])
                    long_units = float(pos['long']['units'])
                    short_units = float(pos['short']['units'])
                    
                    if long_units != 0:
                        broker_positions[f"{instrument}_BUY"] = long_units
                    if short_units != 0:
                        broker_positions[f"{instrument}_SELL"] = abs(short_units)
            
            # Get database positions  
            db_positions = await self.db_manager.get_open_positions()
            db_keys = {f"{p['symbol']}_{p['action']}" for p in db_positions}
            broker_keys = set(broker_positions.keys())
            
            # Close positions that exist in DB but not in broker
            stale_positions = db_keys - broker_keys
            for pos_key in stale_positions:
                symbol, action = pos_key.split('_')
                await self._close_stale_position(symbol, action)
            
            logger.info(f"Reconciliation complete: {len(stale_positions)} stale positions closed")
    
        except Exception as e:
            logger.warning(f"Position reconciliation failed: {e}")

    async def _close_stale_position(self, symbol, action):
        # Placeholder for closing a stale position in the database
        logger.info(f"Closing stale position: {symbol} {action}")
        # Implement actual close logic as needed
