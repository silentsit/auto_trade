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
import oandapyV20
from pydantic import SecretStr
from config import config
import error_recovery
# Remove circular import - these will be imported locally when needed
# from main import _close_position, robust_oanda_request
from utils import (
    logger, get_module_logger, normalize_timeframe, standardize_symbol, 
    is_instrument_tradeable, get_atr, get_instrument_type, 
    get_atr_multiplier, get_trading_logger
)
import configparser
import os
import json
import logging
import asyncio
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
import oandapyV20
import error_recovery
from main import _close_position, robust_oanda_request
from utils import logger, get_module_logger, normalize_timeframe, standardize_symbol, is_instrument_tradeable, get_account_balance, get_current_price, get_atr, get_instrument_type, get_atr_multiplier, parse_iso_datetime
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from volatility_monitor import VolatilityMonitor
from regime_classifier import LorentzianDistanceClassifier
from dynamic_exit_manager import HybridExitManager
from position_journal import PositionJournal
from notification import NotificationSystem
from system_monitor import SystemMonitor

# Import any other required modules or functions

class EnhancedAlertHandler:
    def __init__(self):
        """Initialize alert handler with proper defaults"""
        # Initialize all attributes to None first
        self.position_tracker = None
        self.risk_manager = None
        self.volatility_monitor = None
        self.market_structure = None
        self.regime_classifier = None
        self.dynamic_exit_manager = None
        self.position_journal = None
        self.notification_system = None
        self.system_monitor = None
        
        # Other initialization code...
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        
        # Configuration flags
        self.enable_reconciliation = config.enable_broker_reconciliation if 'config' in globals() else True
        self.enable_close_overrides = True
        
        logger.info("EnhancedAlertHandler initialized with default values")

    async def start(self):
        """Initialize & start all components, including optional broker reconciliation."""
        if self._running:
            logger.info("EnhancedAlertHandler.start() called, but already running.")
            return True

        logger.info("Attempting to start EnhancedAlertHandler and its components...")
        startup_errors = []
        
        try:
            # 1) System Monitor
            self.system_monitor = SystemMonitor()
            await self.system_monitor.register_component("alert_handler", "initializing")

            # 2) DB Manager check
            if not texmanager:
                logger.critical("db_manager is not initialized. Cannot proceed with startup.")
                await self.system_monitor.update_component_status(
                    "alert_handler", "error", "db_manager not initialized"
                )
                return False

            # 3) Core components registration
            self.position_tracker = PositionTracker(db_manager=texmanager)
            await self.system_monitor.register_component("position_tracker", "initializing")

            self.risk_manager = EnhancedRiskManager()
            await self.system_monitor.register_component("risk_manager", "initializing")

            self.volatility_monitor = VolatilityMonitor()
            await self.system_monitor.register_component("volatility_monitor", "initializing")

            self.regime_classifier = LorentzianDistanceClassifier()
            await self.system_monitor.register_component("regime_classifier", "initializing")

            self.dynamic_exit_manager = HybridExitManager(position_tracker=self.position_tracker)
            self.dynamic_exit_manager.lorentzian_classifier = self.regime_classifier
            await self.system_monitor.register_component("dynamic_exit_manager", "initializing")

            self.position_journal = PositionJournal()
            await self.system_monitor.register_component("position_journal", "initializing")

            self.notification_system = NotificationSystem()
            await self.system_monitor.register_component("notification_system", "initializing")

            # 4) Configure notification channels
            if config.slack_webhook_url:
                slack_url = (
                    config.slack_webhook_url.get_secret_value()
                    if isinstance(config.slack_webhook_url, SecretStr)
                    else config.slack_webhook_url
                )
                if slack_url:
                    await self.notification_system.configure_channel("slack", {"webhook_url": slack_url})

            if config.telegram_bot_token and config.telegram_chat_id:
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
                balance = await get_account_balance(use_fallback=True)  # Use fallback during startup
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

            try:
                logger.info("Starting HybridExitManager…")
                await self.dynamic_exit_manager.start()
                await self.system_monitor.update_component_status("dynamic_exit_manager", "ok")
            except Exception as e:
                startup_errors.append(f"HybridExitManager failed: {e}")
                await self.system_monitor.update_component_status("dynamic_exit_manager", "error", str(e))

            await self.system_monitor.update_component_status("position_journal", "ok")

            # 6) Broker reconciliation (optional and graceful)
            if self.enable_reconciliation and hasattr(self, "reconcile_positions_with_broker"):
                try:
                    logger.info("Performing initial broker reconciliation...")
                    await self.reconcile_positions_with_broker()
                    logger.info("Initial broker reconciliation complete.")
                except Exception as e:
                    startup_errors.append(f"Broker reconciliation failed: {e}")
                    logger.warning(f"Broker reconciliation failed, but system will continue: {e}")
            else:
                logger.info("Broker reconciliation skipped by configuration or OANDA unavailable.")

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

    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
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
                # Prefer 'instrument' if available, fallback to 'symbol'
                symbol = alert_data.get("instrument") or alert_data.get("symbol")
                
                # risk_percent is fetched again later more specifically, this is just for an initial log
                risk_percent_log = alert_data.get("risk_percent", 1.0) 

                logger_instance.info(f"[PROCESS ALERT ID: {alert_id}] Symbol='{symbol}', Direction='{direction}', Risk='{risk_percent_log}%'")

                if alert_id in self.active_alerts:
                    logger_instance.warning(f"Duplicate alert ignored: {alert_id}")
                    return {"status": "ignored", "message": "Duplicate alert", "alert_id": alert_id}
                
                self.active_alerts.add(alert_id) # Add after check, if not duplicate

                if self.system_monitor:
                    await self.system_monitor.update_component_status("alert_handler", "processing", f"Processing alert for {symbol} {direction} (ID: {alert_id})")

                # Handle CLOSE action
                if direction == "CLOSE":
                    if not symbol:
                        logger_instance.error(f"Symbol not provided for CLOSE action. Alert ID: {alert_id}")
                        return {"status": "error", "message": "Symbol required for CLOSE action", "alert_id": alert_id}

                    # === MANDATORY: standardize before closing ===
                    standardized = standardize_symbol(symbol)
                    if not standardized:
                        logger_instance.error(f"[ID: {alert_id}] Failed to standardize symbol '{symbol}' for CLOSE")
                        return {"status": "error", "message": f"Cannot close—invalid symbol format: {symbol}", "alert_id": alert_id}

                    result = await self._close_position(standardized)
                    return {
                        "status": "closed",
                        "symbol": standardized,
                        "result": result,
                        "alert_id": alert_id
                    }

                # Validate direction for other actions (BUY/SELL)
                if direction not in ["BUY", "SELL"]:
                    logger_instance.warning(f"Unknown or invalid action type: '{direction}' for alert ID: {alert_id}")
                    return {"status": "error", "message": f"Unknown or invalid action type: {direction}", "alert_id": alert_id}

                if not symbol: # Symbol is required for BUY/SELL
                    logger_instance.error(f"Symbol not provided for {direction} action. Alert ID: {alert_id}")
                    return {"status": "error", "message": f"Symbol required for {direction} action", "alert_id": alert_id}

                # Check for existing open position
                if self.position_tracker:
                    existing_position = await self.position_tracker.get_position_by_symbol(symbol)
                    if existing_position:
                        logger_instance.info(f"Existing position detected for {symbol}. Evaluating override conditions for alert ID: {alert_id}...")
                        should_override, reason = await self._should_override_close(symbol, existing_position)
                        if not should_override:
                            logger_instance.info(f"Skipping {direction} for {symbol}: position already open and not overridden. Reason: {reason}. Alert ID: {alert_id}")
                            return {"status": "skipped", "reason": f"Position already open, not overridden: {reason}", "alert_id": alert_id}
                        logger_instance.info(f"Override triggered for {symbol}: {reason}. Proceeding with new alert processing. Alert ID: {alert_id}")
                        # If override is true, we continue to process the new trade.

                # --- Execute Trade Logic for BUY/SELL ---
                instrument = alert_data.get("instrument", symbol) # Fallback to symbol if instrument specific key is not there
                timeframe = alert_data.get("timeframe", "H1")
                comment = alert_data.get("comment")
                account = alert_data.get("account") # Could be None
                risk_percent = float(alert_data.get('risk_percent', 1.0)) # Ensure float

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
                
                success, result_dict = await execute_trade(payload_for_execute_trade)
                
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
                
                # Check exits every 5 minutes
                if (current_time - last_run["check_exits"]).total_seconds() >= 300:
                    # 1) Get all open positions from your tracker
                    open_positions = await self.position_tracker.get_open_positions()
                    
                    for symbol, positions_for_symbol in open_positions.items():
                        for pid, pos_data in positions_for_symbol.items():
                            # 2) Fetch current live price (bid/ask midpoint) for this symbol/direction
                            try:
                                current_price = await get_current_price(symbol, pos_data["action"])
                            except Exception:
                                # If we fail to fetch price, skip this one and continue
                                continue
                            
                            # 3) Fetch last 6 candles (so we can compute RSI(5) if needed)
                            try:
                                candle_history = await self.position_tracker.get_price_history(
                                    symbol,
                                    timeframe=pos_data["timeframe"],
                                    count=6
                                )
                            except Exception:
                                candle_history = []
                            
                            # 4) Let HybridExitManager decide what to do
                            await self.dynamic_exit_manager.update_exits(pid, current_price, candle_history)
                    
                    last_run["check_exits"] = current_time
                
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
                if 'error_recovery' in globals() and error_recovery:
                    await error_recovery.record_error("scheduled_tasks", {"error": str(e)})
                await asyncio.sleep(60)

    async def stop(self):
        """Clean-up hook called during shutdown."""
        logger.info("Shutting down EnhancedAlertHandler...")
        
        # Signal the scheduled-tasks loop to exit
        self._running = False
        
        # Give any in-flight iteration a moment to finish
        await asyncio.sleep(1)
        
        # Close the Postgres pool if it exists
        if hasattr(self, "postgres_manager"):
            await self.postgres_manager.close()
        
        # Tear down notifications
        if hasattr(self, "notification_system"):
            await self.notification_system.shutdown()
        
        logger.info("EnhancedAlertHandler shutdown complete.")

    async def _process_entry_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an entry alert (BUY or SELL) with comprehensive error handling.
        Now calculates and uses an ATR-based stop_loss, fetches regime data, and
        calls initialize_hybrid_exits() so that the Hybrid exit logic is engaged.
        """
        request_id = str(uuid.uuid4())
        
        try:
            # 1) Basic payload validation
            if not alert_data:
                logger.error(f"[{request_id}] Empty alert data received")
                return {"status": "rejected", "message": "Empty alert data", "alert_id": request_id}
                
            alert_id = alert_data.get("id", request_id)
            symbol = alert_data.get("symbol", "")
            action = alert_data.get("action", "").upper()
            percentage = float(alert_data.get("percentage", 1.0))
            timeframe = alert_data.get("timeframe", "H1")
            comment = alert_data.get("comment", "")
            
            if not symbol:
                logger.error(f"[{request_id}] Missing required field: symbol")
                return {"status": "rejected", "message": "Missing required field: symbol", "alert_id": alert_id}
            if action not in ["BUY", "SELL"]:
                logger.error(f"[{request_id}] Invalid action for entry alert: {action}")
                return {"status": "rejected", "message": f"Invalid action: {action}", "alert_id": alert_id}
            
            standardized_symbol = standardize_symbol(symbol)
            logger.info(f"[{request_id}] Processing entry: {standardized_symbol} {action} ({percentage}%)")
            
            # 2) Account balance & risk check
            try:
                account_balance = await get_account_balance()
                if self.risk_manager:
                    await self.risk_manager.update_account_balance(account_balance)
            except Exception as e:
                logger.error(f"[{request_id}] Error getting account balance: {e}")
                return {"status": "error", "message": f"Error getting balance: {e}", "alert_id": alert_id}
            
            risk_percentage = min(percentage / 100, config.max_risk_percentage / 100)
            if self.risk_manager:
                try:
                    allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage, standardized_symbol)
                    if not allowed:
                        logger.warning(f"[{request_id}] Trade rejected by risk manager: {reason}")
                        return {"status": "rejected", "message": f"Risk check failed: {reason}", "alert_id": alert_id}
                except Exception as e:
                    logger.error(f"[{request_id}] Error in risk check: {e}")
                    return {"status": "error", "message": f"Error in risk check: {e}", "alert_id": alert_id}
            
            # 3) Determine entry price
            try:
                price = alert_data.get("price")
                if price is None:
                    price = await get_current_price(standardized_symbol, action)
                else:
                    price = float(price)
            except Exception as e:
                logger.error(f"[{request_id}] Error fetching price: {e}")
                return {"status": "error", "message": f"Error getting price: {e}", "alert_id": alert_id}
            
            # 4) Fetch ATR & compute ATR-based stop
            try:
                atr_value = await get_atr(standardized_symbol, timeframe)
                if atr_value <= 0:
                    atr_value = 0.0025
                instrument_type = get_instrument_type(standardized_symbol)
                base_atr_mult = get_atr_multiplier(instrument_type, timeframe)
                vol_mult = 1.0
                if self.volatility_monitor:
                    vol_mult = self.volatility_monitor.get_stop_loss_modifier(standardized_symbol)
                actual_atr_mult = base_atr_mult * vol_mult
                if action == "BUY":
                    stop_loss = price - (atr_value * actual_atr_mult)
                else:
                    stop_loss = price + (atr_value * actual_atr_mult)
                stop_loss = float(f"{stop_loss:.5f}")
            except Exception as e:
                logger.error(f"[{request_id}] Error calculating ATR or stop_loss: {e}")
                atr_value = 0.0025
                actual_atr_mult = 1.5
                if action == "BUY":
                    stop_loss = price - (atr_value * actual_atr_mult)
                else:
                    stop_loss = price + (atr_value * actual_atr_mult)
                stop_loss = float(f"{stop_loss:.5f}")
            
            # 5) Calculate position size
            try:
                position_size = (account_balance * (percentage / 100)) / price
            except Exception as e:
                logger.error(f"[{request_id}] Error calculating position size: {e}")
                return {"status": "error", "message": f"Error position size: {e}", "alert_id": alert_id}
            
            # 6) Execute trade with the real stop_loss
            try:
                order_payload = {
                    "symbol": standardized_symbol,
                    "action": action,
                    "units": str(int(position_size)),
                    "type": "MARKET"
                  # "stopLossOnFill": {"price": f"{stop_loss:.5f}"}
                }
                success, trade_result = await execute_trade(order_payload)
                if not success:
                    err_msg = trade_result.get("error", "Unknown error")
                    logger.error(f"[{request_id}] Trade failed: {err_msg}")
                    return {"status": "error", "message": f"Trade failed: {err_msg}", "alert_id": alert_id}
            except Exception as e:
                logger.error(f"[{request_id}] Error executing trade: {e}")
                return {"status": "error", "message": f"Error executing trade: {e}", "alert_id": alert_id}
            
            # 7) Record in PositionTracker
            try:
                if self.position_tracker:
                    metadata = {
                        "alert_id": alert_id,
                        "atr_value": atr_value,
                        "atr_multiplier": base_atr_mult,
                        "volatility_multiplier": vol_mult
                    }
                    for k, v in alert_data.items():
                        if k not in ["id", "symbol", "action", "percentage", "price", "comment", "timeframe"]:
                            metadata[k] = v
                    await self.position_tracker.record_position(
                        position_id=f"{standardized_symbol}_{action}_{uuid.uuid4().hex[:8]}",
                        symbol=standardized_symbol,
                        action=action,
                        timeframe=timeframe,
                        entry_price=price,
                        size=position_size,
                        stop_loss=stop_loss,
                        take_profit=None,
                        metadata=metadata
                    )
                logger.info(f"[{request_id}] Position recorded: {standardized_symbol} {action} @ {price:.5f}")
            except Exception as e:
                logger.error(f"[{request_id}] Error recording position: {e}")
            
            # 8) Register with RiskManager
            try:
                if self.risk_manager:
                    await self.risk_manager.register_position(
                        position_id=position_size,
                        symbol=standardized_symbol,
                        action=action,
                        size=position_size,
                        entry_price=price,
                        stop_loss=stop_loss,
                        account_risk=risk_percentage,
                        timeframe=timeframe
                    )
            except Exception as e:
                logger.error(f"[{request_id}] Error registering with risk manager: {e}")
            
            # 9) Initialize Hybrid Exits (with ATR, stop_loss, and regime)
            try:
                if self.dynamic_exit_manager:
                    regime = "unknown"
                    if self.regime_classifier:
                        regime_data = self.regime_classifier.get_regime_data(standardized_symbol)
                        regime = regime_data.get("regime", "unknown")
                    await self.dynamic_exit_manager.initialize_hybrid_exits(
                        position_id=position_size,
                        symbol=standardized_symbol,
                        entry_price=price,
                        position_direction=action,
                        stop_loss=stop_loss,
                        atr=atr_value,
                        timeframe=timeframe,
                        regime=regime
                    )
                    logger.info(f"[{request_id}] Hybrid exits initialized (regime: {regime})")
            except Exception as e:
                logger.error(f"[{request_id}] Error initializing hybrid exits: {e}")
            
            # 10) Record in PositionJournal
            try:
                if self.position_journal:
                    regime = "unknown"
                    vol_state = "normal"
                    if self.regime_classifier:
                        regime_data = self.regime_classifier.get_regime_data(standardized_symbol)
                        regime = regime_data.get("regime", "unknown")
                    if self.volatility_monitor:
                        vol_data = self.volatility_monitor.get_volatility_state(standardized_symbol)
                        vol_state = vol_data.get("volatility_state", "normal")
                    await self.position_journal.record_entry(
                        position_id=position_size,
                        symbol=standardized_symbol,
                        action=action,
                        timeframe=timeframe,
                        entry_price=price,
                        size=position_size,
                        strategy="hybrid_exit",
                        stop_loss=stop_loss,
                        market_regime=regime,
                        volatility_state=vol_state,
                        metadata=metadata
                    )
            except Exception as e:
                logger.error(f"[{request_id}] Error recording in journal: {e}")
            
            # 11) Send Notification
            try:
                if self.notification_system:
                    await self.notification_system.send_notification(
                        f"New position opened: {action} {standardized_symbol} @ {price:.5f} (SL: {stop_loss:.5f})",
                        "info"
                    )
            except Exception as e:
                logger.error(f"[{request_id}] Error sending notification: {e}")
            
            # 12) Return success
            return {
                "status": "success",
                "message": f"Position opened: {action} {standardized_symbol} @ {price:.5f}",
                "position_id": position_size,
                "symbol": standardized_symbol,
                "action": action,
                "price": price,
                "size": position_size,
                "stop_loss": stop_loss,
                "alert_id": alert_id
            }
                
        except Exception as e:
            logger.error(f"[{request_id}] Unhandled exception in entry alert: {e}", exc_info=True)
            return {"status": "error", "message": f"Internal error: {e}", "alert_id": alert_data.get("id", "unknown")}

    async def _process_exit_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process exit alert with enhanced override logic, timeframe comparison for generic CLOSE,
        and resilient per-position processing.
        """
        alert_id = alert_data.get("id", str(uuid.uuid4()))
        symbol_from_alert = alert_data.get("symbol", "")
        action_from_alert = alert_data.get("action", "").upper()
        signal_timeframe = alert_data.get("timeframe")
    
        logger_instance = get_module_logger(__name__, symbol=symbol_from_alert, request_id=alert_id)
    
        standardized_symbol = standardize_symbol(symbol_from_alert)
        logger_instance(f"Close signal received: Original='{symbol_from_alert}', Standardized='{standardized_symbol}', Signal Action='{action_from_alert}', Signal Timeframe='{signal_timeframe}'")
    
        open_positions_for_symbol_dict = {}
        if self.position_tracker: # type: ignore
            all_open_positions_by_symbol = await self.position_tracker.get_open_positions() # type: ignore
            if standardized_symbol in all_open_positions_by_symbol:
                open_positions_for_symbol_dict = all_open_positions_by_symbol[standardized_symbol]
    
        if not open_positions_for_symbol_dict:
            logger_instance(f"WARNING: No open positions found for {standardized_symbol} to apply {action_from_alert} signal.")
            return {"status": "warning", "message": f"No open positions found for {standardized_symbol} to close.", "alert_id": alert_id}
    
        price_to_close_at = alert_data.get("price")
        if price_to_close_at is None:
            sample_pos_action_for_price_fetch = "BUY"
            for position_id_temp, pos_details_temp in open_positions_for_symbol_dict.items():
                pos_original_action_temp = pos_details_temp.get("action", "").upper()
                if (action_from_alert == "CLOSE" or
                        (action_from_alert == "CLOSE_LONG" and pos_original_action_temp == "BUY") or
                        (action_from_alert == "CLOSE_SHORT" and pos_original_action_temp == "SELL")):
                    sample_pos_action_for_price_fetch = pos_original_action_temp
                    break
    
            price_fetch_direction_for_close = "SELL" if sample_pos_action_for_price_fetch == "BUY" else "BUY"
            try:
                price_to_close_at, _ = await get_price_with_fallbacks(standardized_symbol, price_fetch_direction_for_close)
                logger_instance(f"Fetched current price {price_to_close_at} for closing {standardized_symbol}")
            except ValueError as e:
                logger_instance(f"ERROR: Failed to fetch price for closing {standardized_symbol}: {e}")
                return {"status": "error", "message": f"Failed to fetch price for closing: {e}", "alert_id": alert_id}
        else:
            try:
                price_to_close_at = float(price_to_close_at)
                logger_instance(f"Using provided price {price_to_close_at} for closing {standardized_symbol}")
            except ValueError:
                logger_instance(f"ERROR: Invalid price provided for closing {standardized_symbol}: {alert_data.get('price')}")
                return {"status": "error", "message": f"Invalid price provided: {alert_data.get('price')}", "alert_id": alert_id}
    
        positions_to_attempt_close_ids = []
    
        for position_id, pos_details in open_positions_for_symbol_dict.items():
            pos_original_action = pos_details.get("action", "").upper()
            pos_original_timeframe = pos_details.get("timeframe")
    
            match = False
            if action_from_alert == "CLOSE":
                if signal_timeframe and pos_original_timeframe:
                    if normalize_timeframe(signal_timeframe, target="OANDA") == normalize_timeframe(pos_original_timeframe, target="OANDA"):
                        match = True
                        logger_instance(f"Generic CLOSE for {standardized_symbol} on signal TF '{signal_timeframe}' matches open position {position_id} on TF '{pos_original_timeframe}'.")
                    else:
                        logger_instance(f"Generic CLOSE for {standardized_symbol} on signal TF '{signal_timeframe}' does NOT match open position {position_id} TF '{pos_original_timeframe}'. Skipping.")
                else:
                    match = True
                    logger_instance(f"WARNING: Generic CLOSE for {standardized_symbol}: Signal TF='{signal_timeframe}', Position TF='{pos_original_timeframe}'. Defaulting to close {position_id} due to missing timeframe info for comparison.")
            elif action_from_alert == "CLOSE_LONG" and pos_original_action == "BUY":
                match = True
            elif action_from_alert == "CLOSE_SHORT" and pos_original_action == "SELL":
                match = True
    
            if match:
                positions_to_attempt_close_ids.append(position_id)
    
        if not positions_to_attempt_close_ids:
            logger_instance(f"WARNING: No specific positions identified for closure matching {standardized_symbol} {action_from_alert} (Signal TF: {signal_timeframe}) criteria.")
            return {"status": "warning", "message": f"No specific positions to close for {standardized_symbol} {action_from_alert} (Signal TF: {signal_timeframe})", "alert_id": alert_id}
    
        closed_positions_results_list = []
        overridden_positions_details_list = []
    
        for position_id in positions_to_attempt_close_ids:
            try:  # Main try for processing each position
                position_data_for_this_id = open_positions_for_symbol_dict[position_id]
                should_override, override_reason = await self._should_override_close(position_id, position_data_for_this_id) # type: ignore
    
                if should_override:
                    logger_instance(f"OVERRIDING close signal for {position_id} - Reason: {override_reason}")
                    overridden_positions_details_list.append({
                        "position_id": position_id,
                        "symbol": position_data_for_this_id.get('symbol'),
                        "action": position_data_for_this_id.get('action'),
                        "override_reason": override_reason,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
    
                    if hasattr(self, 'override_stats') and self.override_stats: # type: ignore
                        self.override_stats["total_overrides"] += 1 # type: ignore
                    continue
    
                logger_instance(f"No override for {position_id}. Proceeding to close {position_data_for_this_id.get('action')} position with broker.")
    
                actual_exit_price_for_tracker = price_to_close_at
                exit_reason_for_tracker = f"{action_from_alert.lower()}_signal_broker_failed"
                is_pos_not_exist_error = False
                symbol = position_data_for_this_id.get("symbol", "")
                standardized = standardize_symbol(symbol)
    
                try:  # For broker close operation
                    broker_close_payload = {
                        "symbol": standardized_symbol,  # always use the one standardized at the top
                        "position_id": position_id,
                        "action": position_data_for_this_id.get("action")
                    }
                    success_broker, broker_close_result = await _close_position(broker_close_payload)

    
                    if success_broker:
                        actual_exit_price_for_tracker = broker_close_result.get("actual_exit_price", price_to_close_at) # type: ignore
                        exit_reason_for_tracker = f"{action_from_alert.lower()}_signal"
                    else:
                        error_msg_lower = str(broker_close_result.get("error", "")).lower() # type: ignore
                        details_lower = str(broker_close_result.get("details", "")).lower() # type: ignore
                        is_pos_not_exist_error = (
                            "closeout_position_doesnt_exist" in error_msg_lower or
                            "closeout_position_doesnt_exist" in details_lower or
                            "position requested to be closed out does not exist" in error_msg_lower or
                            "position requested to be closed out does not exist" in details_lower
                        )
                        if not is_pos_not_exist_error:
                            broker_error_message = broker_close_result.get('error', 'Unknown broker error') # type: ignore
                            logger_instance(f"ERROR: Broker close failed for {position_id}: {broker_error_message}")
                            raise error_recovery.TradingSystemError(f"Broker close failed: {broker_error_message}")
                
                    logger_instance(f"Position {position_id} broker interaction completed. Exit price for tracker: {actual_exit_price_for_tracker}")
    
                except error_recovery.TradingSystemError as e_broker_sys_error:
                    logger_instance(f"ERROR: Broker system error for {position_id}: {e_broker_sys_error}, exc_info=True")
                    raise
                
                except Exception as e_broker_close:
                    error_str_lower = str(e_broker_close).lower()
                    if "closeout_position_doesnt_exist" in error_str_lower or \
                       "position requested to be closed out does not exist" in error_str_lower or \
                       (locals().get("is_pos_not_exist_error") and is_pos_not_exist_error):
                        logger_instance(f"Position {position_id} didn't exist on broker side (or error indicates it). Setting for DB reconciliation. Error: {e_broker_close}")
                        is_pos_not_exist_error = True
                        exit_reason_for_tracker = "reconciliation_broker_not_found"
                        actual_exit_price_for_tracker = price_to_close_at
                    else:
                        logger_instance(f"ERROR: Unexpected error during broker close for position {position_id}: {e_broker_close}, exc_info=True")
                        raise
    
                if self.position_tracker: # type: ignore
                    if is_pos_not_exist_error:
                        logger_instance(f"Reconciling {position_id} in tracker as broker reported it doesn't exist or process failed indicating such.")
    
                    close_tracker_result_obj = await self.position_tracker.close_position( # type: ignore
                        position_id=position_id,
                        exit_price=actual_exit_price_for_tracker,
                        reason=exit_reason_for_tracker
                    )
    
                    if close_tracker_result_obj.success and close_tracker_result_obj.position_data:
                        closed_positions_results_list.append(close_tracker_result_obj.position_data)
                        if self.risk_manager: # type: ignore
                            await self.risk_manager.clear_position(position_id) # type: ignore
    
                        if self.position_journal: # type: ignore
                            market_regime = "unknown"
                            volatility_state = "normal"
                            current_symbol_for_context = position_data_for_this_id.get("symbol", standardized_symbol)
                            if self.regime_classifier: # type: ignore
                                regime_data = self.regime_classifier.get_regime_data(current_symbol_for_context) # type: ignore
                                market_regime = regime_data.get("regime", "unknown")
                            if self.volatility_monitor: # type: ignore
                                vol_data = self.volatility_monitor.get_volatility_state(current_symbol_for_context) # type: ignore
                                volatility_state = vol_data.get("volatility_state", "normal")
    
                            await self.position_journal.record_exit( # type: ignore
                                position_id,
                                actual_exit_price_for_tracker,
                                exit_reason_for_tracker,
                                close_tracker_result_obj.position_data.get("pnl", 0.0),
                                market_regime=market_regime,
                                volatility_state=volatility_state
                            )
                    else:
                        tracker_error_message = close_tracker_result_obj.error if close_tracker_result_obj else 'Tracker error object was None'
                        logger_instance(f"ERROR: Failed to close position {position_id} in tracker: {tracker_error_message}")
    
            except Exception as e_position_processing:
                logger_instance(f"ERROR: Error processing position {position_id} for close signal: {str(e_position_processing)}, exc_info=True")
                continue
    
            try:
                if self.notification_system: # type: ignore
                    total_pnl = sum(p.get("pnl", 0.0) for p in closed_positions_results_list if p) # type: ignore
                    level = "info" if total_pnl >= 0 else "warning"
                    price_display = f"{price_to_close_at:.5f}" if isinstance(price_to_close_at, float) else str(price_to_close_at) # type: ignore
        
                if closed_positions_results_list and overridden_positions_details_list: # type: ignore
                    notif_message = (
                        f"Close Signal Results for {standardized_symbol}:\n" # type: ignore
                        f"✅ Closed {len(closed_positions_results_list)} positions @ {price_display} " # type: ignore
                        f"(Net P&L: {total_pnl:.2f})\n"
                        f"🚫 Overridden {len(overridden_positions_details_list)} positions" # type: ignore
                    )
                elif closed_positions_results_list: # type: ignore
                    notif_message = (
                        f"Closed {len(closed_positions_results_list)} positions for {standardized_symbol} " # type: ignore
                        f"@ {price_display} (Net P&L: {total_pnl:.2f})"
                    )
                elif overridden_positions_details_list: # type: ignore
                    notif_message = (
                        f"All {len(overridden_positions_details_list)} matching positions for " # type: ignore
                        f"{standardized_symbol} were overridden" # type: ignore
                    )
                else:
                    notif_message = (
                        f"No positions were ultimately closed or overridden for {standardized_symbol} from signal " # type: ignore
                        f"(Signal TF: {signal_timeframe}). Check logs if positions were expected." # type: ignore
                    )
        
                await self.notification_system.send_notification(notif_message, level) # type: ignore
            except Exception as e_notif:
                logger_instance(f"ERROR: Error sending notification: {str(e_notif)}, exc_info=True") # type: ignore
        
        if closed_positions_results_list or overridden_positions_details_list: # type: ignore
            return {
                "status": "success",
                "message": f"Processed close signal for {standardized_symbol}. Closed: {len(closed_positions_results_list)}, Overridden: {len(overridden_positions_details_list)}.", # type: ignore
                "closed_positions": closed_positions_results_list, # type: ignore
                "overridden_positions": overridden_positions_details_list, # type: ignore
                "total_closed": len(closed_positions_results_list), # type: ignore
                "total_overridden": len(overridden_positions_details_list), # type: ignore
                "symbol": standardized_symbol, # type: ignore
                "price_at_signal": price_to_close_at, # type: ignore
                "alert_id": alert_id # type: ignore
            }
        else:
            logger_instance(f"WARNING: No positions were closed or overridden for {standardized_symbol} {action_from_alert} (Signal TF: {signal_timeframe}) despite processing attempts. Check logs.") # type: ignore
            return {
                "status": "warning",
                "message": f"No positions were ultimately closed or overridden for {standardized_symbol}. Check logs for details.", # type: ignore
                "closed_positions": [],
                "overridden_positions": [],
                "total_closed": 0,
                "total_overridden": 0,
                "alert_id": alert_id # type: ignore
            }

    @staticmethod
    def _standardize_symbol_for_broker(symbol: str) -> str:
        if not symbol:
            return ""
        s = symbol.replace("/", "").replace("-", "")
        if len(s) == 6 and s.isalnum():
            return f"{s[:3]}_{s[3:]}".upper()
        return symbol.upper().replace("/", "_")

    async def _get_current_broker_price(self, symbol: str, side: str) -> float:
        self.logger.info(f"Fetching current price for {symbol} (to inform {side} action) via self.broker_client")
        # Placeholder: Implement using self.broker_client
        # Example: price_data = await self.broker_client.get_market_price(symbol)
        # return price_data.get('ask' if side == 'ASK' else 'bid', 0.0)
        if "EUR_" in symbol: return 1.0850
        if "GBP_" in symbol: return 1.2700
        return 75000.0

    async def _close_position(self, position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        position_id = position_data.get("position_id", "UNKNOWN_ID")
        symbol_from_payload = position_data.get("symbol", "")
        action_from_payload = position_data.get("action", "").upper()
        
        request_id = str(uuid.uuid4())
        log_context_short = f"[BROKER_CLOSE] PosID: {position_id}, Symbol: {symbol_from_payload}, ReqID: {request_id}"

        self.logger.info(f"{log_context_short} - Attempting to close position. Payload: {position_data}")

        if not symbol_from_payload:
            self.logger.error(f"{log_context_short} - Symbol not provided. Cannot close.")
            return False, {"error": "Symbol not provided for broker closure", "position_id": position_id, "request_id": request_id}

        standardized_symbol_for_broker = self._standardize_symbol_for_broker(symbol_from_payload)
        if not standardized_symbol_for_broker:
            self.logger.error(f"{log_context_short} - Failed to standardize symbol '{symbol_from_payload}'.")
            return False, {"error": f"Failed to standardize symbol for broker: {symbol_from_payload}", "position_id": position_id, "request_id": request_id}

        self.logger.debug(f"{log_context_short} - Standardized symbol for OANDA: {standardized_symbol_for_broker}")

        try:
            oanda_account_id = self.config.oanda_account_id

            if action_from_payload == "BUY":
                oanda_close_data = {"longUnits": "ALL"}
            elif action_from_payload == "SELL":
                oanda_close_data = {"shortUnits": "ALL"}
            else:
                self.logger.warning(f"{log_context_short} - Original action not specified or unknown ('{action_from_payload}'). Attempting general 'ALL units' close.")
                oanda_close_data = {"longUnits": "ALL", "shortUnits": "ALL"}

            close_request_oanda = PositionClose(
                accountID=oanda_account_id,
                instrument=standardized_symbol_for_broker,
                data=oanda_close_data
            )
            self.logger.info(f"{log_context_short} - Sending OANDA PositionClose. Instrument: {standardized_symbol_for_broker}, Data: {oanda_close_data}")
            
            broker_response = await self.broker_client.send(close_request_oanda)
            
            self.logger.info(f"{log_context_short} - OANDA PositionClose RAW response: {json.dumps(broker_response)}")

            actual_exit_price = None
            filled_units = 0
            transactions_in_response = []

            if "longOrderFillTransaction" in broker_response:
                tx = broker_response["longOrderFillTransaction"]
                transactions_in_response.append(tx)
                if tx.get("price"): actual_exit_price = float(tx["price"])
                if tx.get("units"): filled_units += abs(float(tx["units"]))
                self.logger.debug(f"{log_context_short} - longOrderFillTransaction. Price: {actual_exit_price}, Units: {tx.get('units')}")

            if "shortOrderFillTransaction" in broker_response:
                tx = broker_response["shortOrderFillTransaction"]
                transactions_in_response.append(tx)
                if tx.get("price"): actual_exit_price = float(tx["price"])
                if tx.get("units"): filled_units += abs(float(tx["units"]))
                self.logger.debug(f"{log_context_short} - shortOrderFillTransaction. Price: {actual_exit_price}, Units: {tx.get('units')}")
            
            if not transactions_in_response and "orderFillTransaction" in broker_response:
                tx = broker_response["orderFillTransaction"]
                transactions_in_response.append(tx)
                if tx.get("price"): actual_exit_price = float(tx["price"])
                if tx.get("units"): filled_units = abs(float(tx["units"]))
                self.logger.debug(f"{log_context_short} - general orderFillTransaction. Price: {actual_exit_price}, Units: {tx.get('units')}")

            if not transactions_in_response and ("longOrderCreateTransaction" in broker_response or "shortOrderCreateTransaction" in broker_response):
                self.logger.warning(f"{log_context_short} - PositionClose created order, but fill not in immediate response. Response: {broker_response}")
                price_fetch_side = "BID" if action_from_payload == "BUY" else ("ASK" if action_from_payload == "SELL" else "BID")
                actual_exit_price = await self._get_current_broker_price(standardized_symbol_for_broker, price_fetch_side)
                self.logger.warning(f"{log_context_short} - Using current fetched price {actual_exit_price} as fallback exit price.")
            
            elif actual_exit_price is None and not transactions_in_response:
                 if "orderCancelTransaction" in broker_response and broker_response["orderCancelTransaction"].get("reason") == "POSITION_CLOSEOUT_FAILED":
                    self.logger.warning(f"{log_context_short} - PositionClose failed, likely no open position. Response: {broker_response}")
                    return False, {
                        "error": "Position closeout failed, potentially no open position.",
                        "position_id": position_id, "request_id": request_id, "broker_response": broker_response
                    }
                 self.logger.error(f"{log_context_short} - Could not determine exit price or confirm closure from OANDA response. Response: {broker_response}")
                 price_fetch_side = "BID" if action_from_payload == "BUY" else ("ASK" if action_from_payload == "SELL" else "BID")
                 actual_exit_price = await self._get_current_broker_price(standardized_symbol_for_broker, price_fetch_side)
                 self.logger.warning(f"{log_context_short} - Critical: Using current fetched price {actual_exit_price} as fallback due to missing fill.")

            self.logger.info(f"{log_context_short} - Position closure processed. Exit Price: {actual_exit_price}, Units affected: {filled_units if filled_units > 0 else 'ALL'}")
            return True, {
                "position_id": position_id,
                "actual_exit_price": actual_exit_price,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "broker_response": broker_response,
                "message": "Position closure processed successfully with broker.",
                "request_id": request_id
            }

        except V20Error as v20_err:
            self.logger.error(f"{log_context_short} - OANDA API error: {v20_err.msg} (Code: {v20_err.code})", exc_info=True)
            if v20_err.code == '404' and "POSITION_NOT_FOUND" in str(v20_err.msg).upper():
                 self.logger.warning(f"{log_context_short} - Attempted to close a position that does not exist on OANDA.")
                 return True, {
                    "position_id": position_id,
                    "message": "Position did not exist or was already closed on OANDA.",
                    "broker_response": {"error_code": v20_err.code, "error_message": v20_err.msg},
                    "request_id": request_id,
                    "actual_exit_price": None
                 }
            return False, {"error": f"OANDA API Error: {v20_err.msg}", "details": str(v20_err), "code": v20_err.code, "position_id": position_id, "request_id": request_id}
        except Exception as e:
            self.logger.error(f"{log_context_short} - General error during OANDA PositionClose: {str(e)}", exc_info=True)
            return False, {"error": str(e), "position_id": position_id, "request_id": request_id}

    async def handle_alert(self, alert_payload: Dict[str, Any]):
        self.logger.info(f"Handling alert: {alert_payload}")
        action = alert_payload.get("action", "").upper()

        if action in ["CLOSE", "CLOSE_LONG", "CLOSE_SHORT"]:
            position_data_for_close = {
                "position_id": alert_payload.get("strategy_order_id", str(uuid.uuid4())),
                "symbol": alert_payload.get("symbol"),
                "action": alert_payload.get("original_trade_action") 
            }
            if not position_data_for_close["symbol"]:
                 self.logger.error("Symbol missing in alert, cannot process close action.")
                 return

            success, result = await self._close_position(position_data_for_close)
            if success:
                self.logger.info(f"Successfully processed close action from alert: {result}")
                if self.db_manager and result.get("actual_exit_price") is not None:
                    # Example: await self.db_manager.update_trade_status(...)
                    pass
            else:
                self.logger.error(f"Failed to process close action from alert: {result}")
        else:
            self.logger.info(f"Alert action '{action}' is not a close action, skipping broker close.")

    async def _should_override_close(self, position_id: str, position_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Simplified override logic for CLOSE signals:
        • Override (i.e. defer the close) if the trade is at least 0.5 × ATR in profit AND the regime is trending.
        • Otherwise, honor the CLOSE signal immediately.
        Returns:
            (True, reason)  if we should defer to HybridExitManager
            (False, reason) if we should honor the CLOSE now
        """
        # 1) Extract basic position info
        symbol    = position_data.get("symbol")
        direction = position_data.get("action", "").upper()   # "BUY" or "SELL"
        entry     = position_data.get("entry_price")
        timeframe = position_data.get("timeframe", "H1")

        # 2) Get current price if not already in position_data
        current_price = position_data.get("current_price")
        if current_price is None:
            try:
                current_price = await get_current_price(symbol, direction)
            except Exception as e:
                # If we cannot fetch a price, do not override
                return False, f"Price fetch failed: {e}"

        # 3) Check if position is in profit
        if direction == "BUY":
            profit_pips = current_price - entry
        else:  # "SELL"
            profit_pips = entry - current_price

        if profit_pips <= 0:
            return False, f"Not in profit (profit_pips={profit_pips:.5f})"

        # 4) Fetch ATR for this symbol/timeframe
        try:
            atr_value = await get_atr(symbol, timeframe)
        except Exception as e:
            return False, f"ATR fetch failed: {e}"

        if atr_value is None or atr_value <= 0:
            return False, f"Invalid ATR ({atr_value})"

        # 5) Compare profit to 0.5 × ATR
        half_atr = 0.5 * atr_value
        if profit_pips < half_atr:
            return False, f"Profit ({profit_pips:.5f}) < 0.5 × ATR ({half_atr:.5f})"

        # 6) Check regime via Lorentzian classifier
        try:
            regime_data = self.lorentzian_classifier.get_regime_data(symbol)
            regime      = regime_data.get("regime", "unknown").lower()
        except Exception as e:
            return False, f"Regime fetch failed: {e}"

        if (direction == "BUY" and regime not in ["trending_up", "momentum_up"]) or \
           (direction == "SELL" and regime not in ["trending_down", "momentum_down"]):
            return False, f"Regime not trending for direction: {regime}"

        # 7) All checks passed → override the CLOSE
        reason = (
            f"Profit ({profit_pips:.5f}) ≥ 0.5 × ATR ({half_atr:.5f}) "
            f"and regime = {regime}"
        )
        return True, reason

    # — Load OANDA Credentials —
    OANDA_ACCESS_TOKEN = os.getenv('OANDA_ACCESS_TOKEN')
    OANDA_ENVIRONMENT = os.getenv('OANDA_ENVIRONMENT', 'practice')
    OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
    
    if not (OANDA_ACCESS_TOKEN and OANDA_ACCOUNT_ID):
        config = configparser.ConfigParser()
        config.read('config.ini')
        try:
            OANDA_ACCESS_TOKEN = OANDA_ACCESS_TOKEN or config.get('oanda', 'access_token')
            OANDA_ENVIRONMENT = OANDA_ENVIRONMENT or config.get('oanda', 'environment')
            OANDA_ACCOUNT_ID = OANDA_ACCOUNT_ID or config.get('oanda', 'account_id')
        except configparser.NoSectionError:
            raise RuntimeError("Missing OANDA credentials: set env vars or config.ini")
    
    oanda = oandapyV20.API(
        access_token=OANDA_ACCESS_TOKEN,
        environment=OANDA_ENVIRONMENT
    )
    
    def _calculate_position_age_hours(self, position_data: Dict[str, Any]) -> float:
        """Calculate position age in hours"""
        try:
            open_time_str = position_data.get('open_time')
            if open_time_str:
                open_time = datetime.fromisoformat(open_time_str.replace('Z', '+00:00'))
                return (datetime.now(timezone.utc) - open_time).total_seconds() / 3600
        except Exception:  # Consider more specific exception handling
            # Or log the error: logger.warning(f"Could not parse open_time: {open_time_str}", exc_info=True)
            pass
        return 0.0
    
    async def _process_update_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an update alert (update stop loss, take profit, etc.)"""
        alert_id = alert_data.get("id", str(uuid.uuid4()))
        symbol = alert_data.get("symbol", "")
        position_id = alert_data.get("position_id")
        stop_loss_input = alert_data.get("stop_loss") # Store original input
        take_profit_input = alert_data.get("take_profit") # Store original input
    
        # Initialize with None, will be set if valid input is provided
        stop_loss = None
        take_profit = None
    
        if position_id:
            if not self.position_tracker: # type: ignore
                return {
                    "status": "error",
                    "message": "Position tracker not available",
                    "alert_id": alert_id
                }
    
            position = await self.position_tracker.get_position_info(position_id) # type: ignore
    
            if not position:
                return {
                    "status": "error",
                    "message": f"Position {position_id} not found",
                    "alert_id": alert_id
                }
    
            if position.get("status") == "closed":
                return {
                    "status": "error",
                    "message": f"Cannot update closed position {position_id}",
                    "alert_id": alert_id
                }
    

            if stop_loss_input is not None:
                try:
                    stop_loss = float(stop_loss_input)
                except ValueError:
                    logger.warning(f"Invalid stop_loss value for position {position_id}: {stop_loss_input}")

            if take_profit_input is not None:
                try:
                    take_profit = float(take_profit_input)
                except ValueError:
                    logger.warning(f"Invalid take_profit value for position {position_id}: {take_profit_input}")
    
            success = await self.position_tracker.update_position( # type: ignore
                position_id=position_id,
                stop_loss=stop_loss, # Pass the processed value
                take_profit=take_profit # Pass the processed value
            )
    
            if not success:
                return {
                    "status": "error",
                    "message": f"Failed to update position {position_id}",
                    "alert_id": alert_id
                }
    
            updated_position = await self.position_tracker.get_position_info(position_id) # type: ignore
    
            if self.position_journal: # type: ignore
                if stop_loss is not None: # Check against processed value
                    await self.position_journal.record_adjustment( # type: ignore
                        position_id=position_id,
                        adjustment_type="stop_loss",
                        old_value=position.get("stop_loss"),
                        new_value=stop_loss,
                        reason="manual_update"
                    )
    
                if take_profit is not None: # Check against processed value
                    await self.position_journal.record_adjustment( # type: ignore
                        position_id=position_id,
                        adjustment_type="take_profit",
                        old_value=position.get("take_profit"),
                        new_value=take_profit,
                        reason="manual_update"
                    )
    
            return {
                "status": "success",
                "message": f"Updated position {position_id}",
                "position": updated_position,
                "alert_id": alert_id
            }
    
        elif symbol:
            open_positions_for_symbol = {} # Renamed for clarity
            if self.position_tracker: # type: ignore
                all_open = await self.position_tracker.get_open_positions() # type: ignore
                if symbol in all_open:
                    open_positions_for_symbol = all_open[symbol]
    
            if not open_positions_for_symbol:
                return {
                    "status": "warning",
                    "message": f"No open positions found for {symbol}",
                    "alert_id": alert_id
                }
    
            if stop_loss_input is not None:
                try:
                    stop_loss = float(stop_loss_input)
                except ValueError:
                    logger.warning(f"Invalid stop_loss value for symbol {symbol}: {stop_loss_input}")
            # else: stop_loss remains None as initialized
    
            if take_profit_input is not None:
                try:
                    take_profit = float(take_profit_input)
                except ValueError:
                    logger.warning(f"Invalid take_profit value for symbol {symbol}: {take_profit_input}")
            # else: take_profit remains None as initialized
    
            updated_positions_list = [] # Renamed for clarity
    
            for pos_id, original_pos_data in open_positions_for_symbol.items():
                success = await self.position_tracker.update_position( # type: ignore
                    position_id=pos_id,
                    stop_loss=stop_loss, # Pass processed value
                    take_profit=take_profit # Pass processed value
                )
    
                if success:
                    updated_pos_info = await self.position_tracker.get_position_info(pos_id) # type: ignore
                    updated_positions_list.append(updated_pos_info)
    
                    if self.position_journal: # type: ignore
                        if stop_loss is not None:
                            await self.position_journal.record_adjustment( # type: ignore
                                position_id=pos_id,
                                adjustment_type="stop_loss",
                                old_value=original_pos_data.get("stop_loss"),
                                new_value=stop_loss,
                                reason="bulk_update"
                            )
    
                        if take_profit is not None:
                            await self.position_journal.record_adjustment( # type: ignore
                                position_id=pos_id,
                                adjustment_type="take_profit",
                                old_value=original_pos_data.get("take_profit"),
                                new_value=take_profit,
                                reason="bulk_update"
                            )
    
            if updated_positions_list:
                return {
                    "status": "success",
                    "message": f"Updated {len(updated_positions_list)} positions for {symbol}",
                    "positions": updated_positions_list,
                    "alert_id": alert_id
                }
            else:
                return {
                    "status": "error", # Or warning if some attempts were made but none succeeded
                    "message": f"Failed to update any positions for {symbol}",
                    "alert_id": alert_id
                }
        else:
            return {
                "status": "error",
                "message": "Either position_id or symbol must be provided",
                "alert_id": alert_id
            }
    
    async def _update_position_prices(self):
        """Update all open position prices"""
        if not self.position_tracker: # type: ignore
            return
    
        try:
            open_positions = await self.position_tracker.get_open_positions() # type: ignore
    
            updated_prices_symbols = {} # Renamed for clarity
            position_count = 0
    
            for symbol, positions_data in open_positions.items(): # Renamed for clarity
                if not positions_data:
                    continue
    
                any_position = next(iter(positions_data.values()))
                direction = any_position.get("action")
    
                try:
                    price = await get_current_price(symbol, "SELL" if direction == "BUY" else "BUY")
                    updated_prices_symbols[symbol] = price
    
                    if self.volatility_monitor: # type: ignore
                        timeframe = any_position.get("timeframe", "H1")
                        atr_value = await get_atr(symbol, timeframe) # type: ignore
                        await self.volatility_monitor.update_volatility(symbol, atr_value, timeframe) # type: ignore
    
                    if self.regime_classifier: # type: ignore
                        await self.regime_classifier.add_price_data(symbol, price, any_position.get("timeframe", "H1")) # type: ignore
    
                    for position_id in positions_data:
                        await self.position_tracker.update_position_price(position_id, price) # type: ignore
                        position_count += 1
    
                except Exception as e:
                    logger.error(f"Error updating price for {symbol}: {str(e)}")
    
            if position_count > 0:
                logger.debug(f"Updated prices for {position_count} positions across {len(updated_prices_symbols)} symbols")
    
        except Exception as e:
            logger.error(f"Error updating position prices: {str(e)}")

    '''
    async def _check_position_exits(self):
        """Check all positions for dynamic exit conditions based on HybridExitManager rules"""
        if not self.position_tracker or not self.dynamic_exit_manager: # type: ignore
            return
    
        try:
            all_open_positions = await self.position_tracker.get_open_positions() # type: ignore
            if not all_open_positions:
                return
    
            positions_checked = 0
            exits_triggered = 0
    
            for symbol, positions_in_symbol in all_open_positions.items(): # Renamed for clarity
                for position_id, position_data in positions_in_symbol.items(): # Renamed for clarity
                    try:
                        if not position_data.get("current_price"):
                            continue
    
                        positions_checked += 1
    
                        if position_id not in self.dynamic_exit_manager.exit_levels: # type: ignore
                            continue
    
                        exit_config = self.dynamic_exit_manager.exit_levels[position_id] # type: ignore
                        current_price = position_data["current_price"]
                        # entry_price = position_data["entry_price"] # Unused in this scope
                        # action = position_data["action"] # Unused in this scope
    
                        if await self._check_take_profit_levels(position_id, position_data, exit_config, current_price): # type: ignore
                            exits_triggered += 1
                            continue
    
                        if await self._check_breakeven_stop(position_id, position_data, exit_config, current_price): # type: ignore
                            # Breakeven stop updates SL, doesn't necessarily exit, so don't increment exits_triggered unless it does.
                            # The _check_breakeven_stop returns False (meaning no exit)
                            pass # No exit, just potential SL update
    
                        if await self._check_time_based_exit(position_id, position_data, exit_config): # type: ignore
                            exits_triggered += 1
                            continue
    
                        if exit_config.get("override_enhanced", False):
                            if await self._check_enhanced_override_exits(position_id, position_data, exit_config, current_price): # type: ignore
                                exits_triggered += 1
                                continue
    
                    except Exception as e:
                        logger.error(f"Error checking exits for position {position_id}: {str(e)}")
                        continue # Continue with the next position
    
            if positions_checked > 0:
                logger.debug(f"Checked dynamic exits for {positions_checked} positions, triggered {exits_triggered} exits")
    
        except Exception as e:
            logger.error(f"Error in dynamic exit checking: {str(e)}")
            '''
    
    async def _check_take_profit_levels(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any], current_price: float) -> bool:
        """Check if any take profit levels are hit. Returns True if an exit occurred."""
        try:
            if "take_profits" not in exit_config:
                return False
    
            tp_config = exit_config["take_profits"]
            levels = tp_config.get("levels", [])
            action = position["action"]
            exit_occurred = False # Flag to track if any exit action was taken
    
            for i, level_config in enumerate(levels): # Renamed 'level' to 'level_config'
                if level_config.get("hit", False):
                    continue
    
                tp_price = level_config.get("price", 0)
                percentage_to_close = level_config.get("percentage", 0) # Renamed for clarity
    
                hit = False
                if action == "BUY":
                    hit = current_price >= tp_price
                else:  # SELL
                    hit = current_price <= tp_price
    
                if hit:
                    logger.info(f"Take profit level {i+1} hit for {position_id} at {current_price}")
                    level_config["hit"] = True # Mark as hit in the config
                    close_success = False
    
                    if percentage_to_close < 100 and percentage_to_close > 0:
                        close_success, _ = await self.position_tracker.close_partial_position( # type: ignore
                            position_id, current_price, percentage_to_close, f"take_profit_level_{i+1}"
                        )
                    elif percentage_to_close >= 100: # Full close
                        close_success = await self._exit_position(position_id, current_price, f"take_profit_level_{i+1}") # type: ignore
                    
                    if close_success:
                        exit_occurred = True # An exit action (partial or full) was successful
                        if percentage_to_close >= 100: # If full close, no more TPs to check for this position
                            return True 
                    # If partial close was successful, loop continues for other TP levels on remaining position
    
            return exit_occurred # True if any partial/full close happened, False otherwise
    
        except Exception as e:
            logger.error(f"Error checking take profit levels for {position_id}: {str(e)}")
            return False
    
    async def _check_breakeven_stop(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any], current_price: float) -> bool:
        """Check if breakeven stop should be activated. Returns False as it only updates SL."""
        try:
            if "breakeven" not in exit_config:
                return False
    
            be_config = exit_config["breakeven"]
            if be_config.get("activated", False): # Already activated
                return False
    
            activation_price_level = be_config.get("activation_level", 0) # Renamed for clarity
            entry_price = position["entry_price"]
            action = position["action"]
    
            activated_now = False # Renamed for clarity
            if action == "BUY":
                activated_now = current_price >= activation_price_level
            else:  # SELL
                activated_now = current_price <= activation_price_level
    
            if activated_now:
                logger.info(f"Breakeven stop activated for {position_id} at {current_price}")
                be_config["activated"] = True # Update the config state
    
                buffer_pips = be_config.get("buffer_pips", 0)
                # Assuming standard pip definition (0.0001 for most FX, adjust if needed for other assets)
                pip_value = 0.0001 # TODO: Make this configurable or symbol-dependent
                
                new_stop_loss = 0
                if action == "BUY":
                    new_stop_loss = entry_price + (buffer_pips * pip_value)
                else: #SELL
                    new_stop_loss = entry_price - (buffer_pips * pip_value)
    
                if self.position_tracker: # type: ignore
                    await self.position_tracker.update_position( # type: ignore
                        position_id,
                        stop_loss=new_stop_loss,
                        # No take_profit change here
                        metadata={"breakeven_activated_at": datetime.now(timezone.utc).isoformat()} # type: ignore
                    )
            return False # Breakeven activation itself doesn't cause an immediate exit by this function
    
        except Exception as e:
            logger.error(f"Error checking breakeven stop for {position_id}: {str(e)}")
            return False
    
    async def _check_time_based_exit(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any]) -> bool:
        """Check if time-based exit should trigger. Returns True if an exit occurred."""
        try:
            if "time_exit" not in exit_config:
                return False
    
            time_config = exit_config["time_exit"]
            exit_time_str = time_config.get("exit_time")
    
            if not exit_time_str:
                return False
    
            exit_time = parse_iso_datetime(exit_time_str) # Use the helper
            current_time = datetime.now(timezone.utc)
    
            if current_time >= exit_time:
                reason = time_config.get("reason", "time_based_exit")
                logger.info(f"Time-based exit triggered for {position_id}")
                current_price = position["current_price"] # Price should be up-to-date
                return await self._exit_position(position_id, current_price, reason) # type: ignore
    
            return False
    
        except Exception as e:
            logger.error(f"Error checking time-based exit for {position_id}: {str(e)}")
            return False
    
    async def _check_enhanced_override_exits(self, position_id: str, position: Dict[str, Any], exit_config: Dict[str, Any], current_price: float) -> bool:
        """Check enhanced exit conditions for overridden positions. Returns True if an exit occurred."""
        try:
            # entry_price = position["entry_price"] # Unused
            action = position["action"]
            pnl_pct = position.get("pnl_percentage", 0.0) # Ensure float for comparison
    
            # Exit if position has reversed by specified percentage (e.g., -1.0%)
            reversal_threshold = exit_config.get("override_reversal_pct", -1.0)
            if pnl_pct <= reversal_threshold:
                logger.info(f"Enhanced override exit: Position {position_id} reversed by {pnl_pct:.2f}% (threshold: {reversal_threshold:.2f}%)")
                return await self._exit_position(position_id, current_price, "override_reversal") # type: ignore
    
            # Check momentum loss if regime classifier is available
            if hasattr(self, 'regime_classifier') and self.regime_classifier: # type: ignore
                regime_data = self.regime_classifier.get_regime_data(position["symbol"]) # type: ignore
                momentum = regime_data.get("momentum", 0.0) # Ensure float
                momentum_reversal_threshold = exit_config.get("override_momentum_reversal_threshold", 0.001)
    
                # Exit if momentum has reversed against the trade direction
                if (action == "BUY" and momentum < -momentum_reversal_threshold) or \
                   (action == "SELL" and momentum > momentum_reversal_threshold):
                    logger.info(f"Enhanced override exit: Momentum reversed for {position_id} (Momentum: {momentum:.4f})")
                    return await self._exit_position(position_id, current_price, "momentum_reversal") # type: ignore
    
            return False
    
        except Exception as e:
            logger.error(f"Error checking enhanced override exits for {position_id}: {str(e)}")
            return False
    
    def _check_stop_loss(self, position: Dict[str, Any], current_price: float, stop_loss: float) -> bool: # Added stop_loss param
        """Check if stop loss is hit.
        NOTE: The original code had a hardcoded 'return False' making logic unreachable.
        This version assumes stop_loss is passed or retrieved correctly.
        """
        # Original line: logger.debug(f"Stop loss check skipped - functionality disabled")
        # Original line: return False # This made the rest unreachable
    
        if stop_loss is None: # No stop loss set for this position
            return False
    
        action = position.get("action", "").upper()
    
        if action == "BUY":
            return current_price <= stop_loss
        elif action == "SELL": # Explicitly SELL
            return current_price >= stop_loss
        return False # Should not happen if action is always BUY or SELL
    
    async def _exit_position(self, position_id: str, exit_price: float, reason: str) -> bool:
        """Exit a position with the given reason"""
        try:
            position = await self.position_tracker.get_position_info(position_id) # type: ignore
            if not position:
                logger.warning(f"Position {position_id} not found for exit (reason: {reason})")
                return False
    
            if position.get("status") == "closed":
                logger.warning(f"Position {position_id} already closed (exit attempt reason: {reason})")
                return False # Or True if considered successful as it's already in desired state
    
            symbol = position.get("symbol", "")
            standardized = standardize_symbol(symbol)
            success_broker, broker_result = await close_position({ # type: ignore
                "symbol": standardized,
                "position_id": position_id,
                "action": position.get("action") # Broker might need original action
                # "price": exit_price # Broker might accept a target price for market/limit close
            })
    
            if not success_broker:
                logger.error(f"Failed to close position {position_id} with broker: {broker_result.get('error', 'Unknown error')}") # type: ignore
                # Decide: should we still try to close in tracker? For now, returning False.
                return False
    
            # If broker close is successful, then update internal state
            tracker_close_result = await self.position_tracker.close_position( # type: ignore
                position_id=position_id,
                exit_price=broker_result.get("actual_exit_price", exit_price), # Use actual price from broker if available
                reason=reason
            )
    
            if not tracker_close_result.success: # type: ignore
                logger.error(f"Failed to close position {position_id} in tracker: {tracker_close_result.error}") # type: ignore
                # This is a state inconsistency: closed at broker but not in tracker. Critical error.
                return False # Or raise an exception
    
            if self.risk_manager: # type: ignore
                await self.risk_manager.clear_position(position_id) # type: ignore
    
            if self.position_journal: # type: ignore
                market_regime = "unknown"
                volatility_state = "normal"
    
                if self.regime_classifier: # type: ignore
                    regime_data = self.regime_classifier.get_regime_data(symbol) # type: ignore
                    market_regime = regime_data.get("regime", "unknown")
    
                if self.volatility_monitor: # type: ignore
                    vol_data = self.volatility_monitor.get_volatility_state(symbol) # type: ignore
                    volatility_state = vol_data.get("volatility_state", "normal")
    
                await self.position_journal.record_exit( # type: ignore
                    position_id=position_id,
                    exit_price=tracker_close_result.position_data.get("exit_price", exit_price), # type: ignore
                    exit_reason=reason,
                    pnl=tracker_close_result.position_data.get("pnl", 0.0), # type: ignore
                    market_regime=market_regime,
                    volatility_state=volatility_state
                )
    
            try:
                if self.notification_system: # type: ignore
                    pnl = tracker_close_result.position_data.get("pnl", 0.0) # type: ignore
                    level = "info"
                    if pnl < 0: # Only change to warning for losses
                        level = "warning"
                    # Ensure exit_price in notification is consistently formatted
                    formatted_exit_price = f"{tracker_close_result.position_data.get('exit_price', exit_price):.5f}"
                    await self.notification_system.send_notification( # type: ignore
                        f"Position {position_id} closed: {symbol} @ {formatted_exit_price} (P&L: {pnl:.2f}, Reason: {reason})",
                        level
                    )
            except Exception as e_notify:
                logger.error(f"Error sending notification for position {position_id} exit: {str(e_notify)}")
    
            logger.info(f"Position {position_id} exited at {tracker_close_result.position_data.get('exit_price', exit_price)} (Reason: {reason})") # type: ignore
            return True
    
        except Exception as e:
            logger.error(f"Error exiting position {position_id}: {str(e)}", exc_info=True)
            return False
    
    async def _perform_daily_reset(self):
        """Perform daily reset tasks"""
        try:
            logger.info("Performing daily reset tasks")
    
            if self.risk_manager: # type: ignore
                await self.risk_manager.reset_daily_stats() # type: ignore
    
            if 'backup_manager' in globals() and backup_manager: # type: ignore
                await backup_manager.create_backup(include_market_data=True, compress=True) # type: ignore
    
            if self.notification_system: # type: ignore
                await self.notification_system.send_notification( # type: ignore
                    "Daily reset completed: Risk statistics reset and backup created",
                    "info"
                )
    
        except Exception as e:
            logger.error(f"Error in daily reset: {str(e)}")
    
    async def _cleanup_old_positions(self):
        """Clean up old closed positions to prevent memory growth"""
        try:
            logger.info("Cleaning up old closed positions and backups.") # Added log
            if self.position_tracker: # type: ignore
                await self.position_tracker.purge_old_closed_positions(max_age_days=30) # type: ignore
    
            if 'backup_manager' in globals() and backup_manager: # type: ignore
                await backup_manager.cleanup_old_backups(max_age_days=60, keep_min=10) # type: ignore
            logger.info("Cleanup of old data finished.") # Added log
        except Exception as e:
            logger.error(f"Error cleaning up old positions/backups: {str(e)}")
    
    async def _sync_database(self):
        """Ensure all data is synced with the database"""
        try:
            logger.info("Starting database sync.") # Added log
            if self.position_tracker: # type: ignore
                await self.position_tracker.sync_with_database() # type: ignore
                await self.position_tracker.clean_up_duplicate_positions() # type: ignore
            logger.info("Database sync finished.") # Added log
        except Exception as e:
            logger.error(f"Error syncing database: {str(e)}")
    
    async def reconcile_positions_with_broker(self):
        """Reconcile positions between database and broker with graceful error handling"""
        try:
            logger.info("Starting position reconciliation with OANDA...")

            # Try to get broker positions with reduced timeout for startup
            try:
                from oandapyV20.endpoints.positions import OpenPositions
                from oandapyV20.endpoints.trades import OpenTrades
                
                r_positions = OpenPositions(accountID=OANDA_ACCOUNT_ID)
                broker_positions_response = await robust_oanda_request(r_positions, max_retries=2, initial_delay=1)

                r_trades = OpenTrades(accountID=OANDA_ACCOUNT_ID)
                broker_trades_response = await robust_oanda_request(r_trades, max_retries=2, initial_delay=1)
                
            except error_recovery.BrokerConnectionError as broker_err:
                logger.warning(f"Broker reconciliation skipped due to connection issues: {broker_err}")
                logger.info("System will continue without initial reconciliation. Reconciliation will be retried later.")
                return  # Continue startup without reconciliation
            
            except Exception as e:
                logger.warning(f"Broker reconciliation failed with unexpected error: {e}")
                logger.info("System will continue without initial reconciliation.")
                return  # Continue startup without reconciliation

            broker_open_details = {}

            if 'trades' in broker_trades_response:
                for trade in broker_trades_response['trades']:
                    instrument_raw = trade.get('instrument')
                    if not instrument_raw:
                        logger.warning(f"Trade {trade.get('id')} missing instrument, skipping.")
                        continue
                    instrument = standardize_symbol(instrument_raw)
                    units_str = trade.get('currentUnits', '0')
                    try:
                        units = float(units_str)
                    except (TypeError, ValueError):
                        logger.warning(f"Invalid units '{units_str}' for trade {trade.get('id')}, skipping.")
                        continue
                    
                    action = "BUY" if units > 0 else "SELL"
                    broker_key = f"{instrument}_{action}"

                    price_str = trade.get('price', '0')
                    try:
                        price = float(price_str)
                    except (TypeError, ValueError):
                        price = 0.0
                        logger.warning(f"Invalid price value '{price_str}' in trade {trade.get('id')}, using {price}.")

                    open_time_str = trade.get('openTime')
                    open_time_iso = datetime.now(timezone.utc).isoformat()
                    if open_time_str:
                        try:
                            open_time_iso = parse_iso_datetime(open_time_str).isoformat()
                        except Exception as time_err:
                            logger.warning(f"Error parsing openTime '{open_time_str}' for trade {trade.get('id')}: {time_err}, using current time.")
                    
                    if broker_key not in broker_open_details or abs(units) > abs(broker_open_details[broker_key].get('units', 0)):
                        broker_open_details[broker_key] = {
                            "broker_trade_id": trade.get('id', ''),
                            "instrument": instrument,
                            "action": action,
                            "entry_price": price,
                            "units": abs(units),
                            "open_time": open_time_iso
                        }

            logger.info(f"Broker open positions (from trades endpoint): {json.dumps(broker_open_details, indent=2)}")

            db_open_positions_data = await self.position_tracker.db_manager.get_open_positions()
            db_open_positions_map = {}

            for p_data in db_open_positions_data:
                symbol_db = p_data.get('symbol', '')
                action_db = p_data.get('action', '')
                if symbol_db and action_db:
                    db_key = f"{symbol_db}_{action_db}"
                    db_open_positions_map[db_key] = p_data

            logger.info(f"Database open positions before reconciliation (symbol_ACTION): {list(db_open_positions_map.keys())}")

            # Phase A: Positions in DB but not on Broker (stale DB entries)
            reconciled_count = 0
            for db_key, db_pos_data in db_open_positions_map.items():
                position_id = db_pos_data.get('position_id')
                if not position_id: 
                    continue

                if db_key not in broker_open_details:
                    logger.warning(f"Position {position_id} ({db_key}) is open in DB but not on OANDA. Attempting to close in DB.")
                    try:
                        full_symbol_for_price = db_pos_data.get('symbol')
                        if not full_symbol_for_price:
                            logger.error(f"Stale position {position_id} in DB has no symbol, cannot fetch price to close.")
                            continue

                        price_fetch_side_for_close = "SELL" if db_pos_data.get('action') == "BUY" else "BUY"
                        
                        try:
                            exit_price = await get_current_price(full_symbol_for_price, price_fetch_side_for_close)
                        except Exception as price_err:
                            logger.warning(f"Could not get price for reconciliation of {position_id}: {price_err}. Using entry price.")
                            exit_price = db_pos_data.get('entry_price', 1.0)

                        if exit_price is not None:
                            close_result_obj = await self.position_tracker.close_position(
                                position_id=position_id,
                                exit_price=exit_price,
                                reason="reconciliation_broker_not_found"
                            )
                            if close_result_obj.success:
                                logger.info(f"Successfully closed stale position {position_id} ({db_key}) in DB.")
                                reconciled_count += 1
                                if self.risk_manager:
                                    await self.risk_manager.clear_position(position_id)
                            else:
                                logger.error(f"Failed to close stale position {position_id} ({db_key}) in DB: {close_result_obj.error}")
                        else:
                            logger.error(f"Cannot close stale position {position_id} ({db_key}): Failed to get current price for {full_symbol_for_price}")
                    except Exception as e_close_stale:
                        logger.error(f"Error during DB closure of stale position {position_id} ({db_key}): {e_close_stale}", exc_info=True)

            logger.info(f"Position reconciliation with OANDA finished. Reconciled {reconciled_count} positions.")

        except Exception as e:
            logger.warning(f"Broker reconciliation failed but system will continue: {str(e)}")
            # Don't re-raise the exception - let the system continue to start
    
    async def _determine_partial_close_percentage(self,
                                                  position_id: str,
                                                  position_data: Dict[str, Any],
                                                  override_reason: str,
                                                  exit_config: Optional[Dict[str, Any]] = None) -> float:
        """Determine what percentage to partially close based on override reason and exit strategy"""
        try:
            pnl_pct = position_data.get('pnl_percentage', 0.0)
            timeframe = position_data.get('timeframe', 'H1')
            # symbol = position_data.get('symbol', '') # Unused in this logic
    
            base_percentage = 0.0
            if "strong_momentum_confirmed" in override_reason:
                if pnl_pct > 2.0:
                    base_percentage = 30.0
                elif pnl_pct > 1.0:
                    base_percentage = 20.0
                else:
                    base_percentage = 0.0 # Let it run if not much profit yet
    
            elif "higher_timeframe_aligned" in override_reason:
                base_percentage = 15.0 if pnl_pct > 1.0 else 0.0
    
            else: # Default partial close for other override reasons
                base_percentage = 25.0 if pnl_pct > 0.5 else 0.0
    
            # Adjust based on timeframe
            if timeframe in ["M1", "M5", "M15"]:
                base_percentage *= 1.2
            elif timeframe in ["H4", "D1"]:
                base_percentage *= 0.8
    
            # Adjust based on exit strategy if available
            if exit_config:
                strategy = exit_config.get("strategy", "standard")
                if strategy == "trend_following":
                    base_percentage *= 0.7
                elif strategy == "mean_reversion":
                    base_percentage *= 1.3
                # elif strategy == "breakout": base_percentage *= 1.0 (no change)
    
            return min(50.0, max(0.0, round(base_percentage, 2))) # Cap and round
    
        except Exception as e:
            logger.error(f"Error determining partial close percentage for {position_id}: {str(e)}")
            return 0.0
    
    async def _activate_dynamic_exit_monitoring(self, position_id: str, position_data: Dict[str, Any]):
        """Activate enhanced dynamic exit monitoring for an overridden position"""
        try:
            if not self.dynamic_exit_manager: # type: ignore
                logger.warning(f"Dynamic exit manager not available for position {position_id}")
                return
    
            symbol = position_data.get('symbol', '')
            entry_price = position_data.get('entry_price', 0.0) # Ensure float
            action = position_data.get('action', '')
            timeframe = position_data.get('timeframe', 'H1')
    
            if not all([symbol, entry_price, action, timeframe]): # Basic validation
                logger.error(f"Cannot activate dynamic exits for {position_id}: missing essential data (symbol, entry_price, action, or timeframe).")
                return
    
            # Check if position already has exit configuration, if not, initialize
            if position_id not in self.dynamic_exit_manager.exit_levels: # type: ignore
                logger.info(f"Initializing dynamic exits for overridden position {position_id}")
                await self.dynamic_exit_manager.initialize_exits( # type: ignore
                    position_id=position_id,
                    symbol=symbol,
                    entry_price=entry_price,
                    position_direction=action,
                    stop_loss=None,  # Overridden positions might not use a traditional SL initially
                    timeframe=timeframe,
                    strategy_type="override_enhanced" # Specific strategy type
                )
    
            # Mark this position for enhanced monitoring, or update if already exists
            if position_id in self.dynamic_exit_manager.exit_levels: # type: ignore
                self.dynamic_exit_manager.exit_levels[position_id]["override_enhanced"] = True # type: ignore
                self.dynamic_exit_manager.exit_levels[position_id]["override_activated_at"] = datetime.now(timezone.utc).isoformat() # type: ignore
                logger.info(f"Dynamic exit monitoring (enhanced) activated/updated for overridden position {position_id}")
            else:
                logger.error(f"Failed to initialize or find dynamic exit config for {position_id} after attempting initialization.")
    
        except Exception as e:
            logger.error(f"Error activating dynamic exit monitoring for {position_id}: {str(e)}", exc_info=True)

    async def _close_position(self, symbol: str) -> dict:
        """
        Close any open position for a given symbol on OANDA.
        """
        try:
            from oandapyV20.endpoints.positions import PositionClose
    
            request = PositionClose(
                accountID=OANDA_ACCOUNT_ID,  # Use the global constant, not self.config
                instrument=symbol,
                data={"longUnits": "ALL", "shortUnits": "ALL"}
            )
    
            # Use the robust_oanda_request function instead of self.broker_client
            response = await robust_oanda_request(request)
            logger.info(f"[CLOSE] Closed position for {symbol}: {response}")
            return response
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {str(e)}", exc_info=True)
            return {"status": "error", "message": str(e)}

    
    def parse_iso_datetime(datetime_str: str) -> datetime:
        """Parse ISO datetime string to datetime object"""
        try:
            # Handle various ISO formats
            if datetime_str.endswith('Z'):
                datetime_str = datetime_str[:-1] + '+00:00'
            return datetime.fromisoformat(datetime_str)
        except ValueError:
            # Fallback parsing
            import dateutil.parser
            return dateutil.parser.parse(datetime_str)
    
    async def get_account_balance(use_fallback: bool = False) -> float:
        """Get account balance from OANDA or return fallback"""
        if use_fallback:
            return 10000.0  # Fallback balance for startup
        
        try:
            # Import here to avoid circular import
            from main import robust_oanda_request
            from oandapyV20.endpoints.accounts import AccountDetails
            
            account_request = AccountDetails(accountID=config.oanda_account_id)
            response = await robust_oanda_request(account_request)
            return float(response['account']['balance'])
        except Exception as e:
            logger.error(f"Error getting account balance: {e}")
            return 10000.0  # Fallback
    
    async def get_current_price(symbol: str, action: str) -> float:
        """Get current price for symbol"""
        try:
            from main import robust_oanda_request
            from oandapyV20.endpoints.pricing import PricingInfo
            
            pricing_request = PricingInfo(
                accountID=config.oanda_account_id,
                params={"instruments": symbol}
            )
            response = await robust_oanda_request(pricing_request)
            
            if 'prices' in response and response['prices']:
                price_data = response['prices'][0]
                if action.upper() == "BUY":
                    return float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
                else:
                    return float(price_data.get('bid', price_data.get('closeoutBid', 0)))
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            
        # Fallback to simulated price
        from utils import _get_simulated_price
        return _get_simulated_price(symbol, action)
    
    async def get_price_with_fallbacks(symbol: str, direction: str) -> Tuple[float, str]:
        """Get price with fallback mechanisms"""
        try:
            price = await get_current_price(symbol, direction)
            return price, "live"
        except Exception as e:
            logger.warning(f"Failed to get live price for {symbol}: {e}, using fallback")
            from utils import _get_simulated_price
            fallback_price = _get_simulated_price(symbol, direction)
            return fallback_price, "fallback"
    
    async def execute_trade(payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Execute trade with OANDA"""
        try:
            symbol = payload.get("symbol")
            action = payload.get("action")
            risk_percent = payload.get("risk_percent", 1.0)
            
            # Get account balance
            account_balance = await get_account_balance()
            
            # Get current price
            current_price = await get_current_price(symbol, action)
            
            # Calculate position size based on risk
            risk_amount = account_balance * (risk_percent / 100.0)
            position_size = int(risk_amount / current_price)
            
            if position_size <= 0:
                return False, {"error": "Calculated position size is zero or negative"}
            
            # Create OANDA order
            from oandapyV20.endpoints.orders import OrderCreate
            from main import robust_oanda_request
            
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
            
            response = await robust_oanda_request(order_request)
            
            if 'orderFillTransaction' in response:
                fill_info = response['orderFillTransaction']
                return True, {
                    "success": True,
                    "fill_price": float(fill_info.get('price', current_price)),
                    "units": abs(int(fill_info.get('units', position_size))),
                    "transaction_id": fill_info.get('id'),
                    "symbol": symbol,
                    "action": action
                }
            else:
                return False, {"error": "Order not filled", "response": response}
                
        except Exception as e:
            logger.error(f"Error executing trade: {e}")
            return False, {"error": str(e)}
    
    async def robust_oanda_request(request, max_retries: int = 3, initial_delay: float = 1.0):
        """Make robust OANDA API request with retries"""
        try:
            # Import here to avoid circular import
            from main import oanda  # This should be defined in main.py
            
            for attempt in range(max_retries):
                try:
                    response = oanda.request(request)
                    return response
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise error_recovery.BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")
                    await asyncio.sleep(initial_delay * (2 ** attempt))
                    logger.warning(f"OANDA request attempt {attempt + 1} failed, retrying: {e}")
            
        except Exception as e:
            raise error_recovery.BrokerConnectionError(f"OANDA request failed: {e}")
    
    # Also need to fix the _close_position function reference
    async def close_position(position_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Close position - this should be moved from main.py or imported properly"""
        try:
            symbol = position_data.get("symbol")
            if not symbol:
                return False, {"error": "Symbol not provided"}
            
            from oandapyV20.endpoints.positions import PositionClose
            
            close_request = PositionClose(
                accountID=config.oanda_account_id,
                instrument=symbol,
                data={"longUnits": "ALL", "shortUnits": "ALL"}
            )
            
            response = await robust_oanda_request(close_request)
            return True, {"success": True, "response": response}
            
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False, {"error": str(e)}
        
