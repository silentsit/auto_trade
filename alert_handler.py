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
    _get_simulated_price
)

class EnhancedAlertHandler:
    def __init__(self, db_manager=None):
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
            
            # Get account balance
            account_balance = await self.get_account_balance()
            
            # Get current price
            current_price = await self.get_current_price(symbol, action)
            
            # Calculate position size based on risk
            risk_amount = account_balance * (risk_percent / 100.0)
            position_size = int(risk_amount / current_price)
            
            if position_size <= 0:
                return False, {"error": "Calculated position size is zero or negative"}
            
            # Create OANDA order
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
            from dynamic_exit_manager import HybridExitManager
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

            self.dynamic_exit_manager = HybridExitManager(position_tracker=self.position_tracker)
            self.dynamic_exit_manager.lorentzian_classifier = self.regime_classifier
            await self.system_monitor.register_component("dynamic_exit_manager", "initializing")

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

            try:
                logger.info("Starting HybridExitManager…")
                await self.dynamic_exit_manager.start()
                await self.system_monitor.update_component_status("dynamic_exit_manager", "ok")
            except Exception as e:
                startup_errors.append(f"HybridExitManager failed: {e}")
                await self.system_monitor.update_component_status("dynamic_exit_manager", "error", str(e))

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

    async def _close_position(self, symbol: str) -> dict:
        """Close any open position for a given symbol on OANDA."""
        try:
            request = PositionClose(
                accountID=config.oanda_account_id,
                instrument=symbol,
                data={"longUnits": "ALL", "shortUnits": "ALL"}
            )
            
            response = await self.robust_oanda_request(request)
            logger.info(f"[CLOSE] Closed position for {symbol}: {response}")
            return response
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {str(e)}", exc_info=True)
            return {"status": "error", "message": str(e)}

    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            # === FIX: Map TradingView fields to expected format ===
            if "action" in alert_data and "direction" not in alert_data:
                alert_data["direction"] = alert_data["action"]
            
            # Map other common TradingView fields
            field_mappings = {
                "ticker": "symbol",
                "side": "direction", 
                "risk": "risk_percent",
                "tf": "timeframe",
            }
            
            for tv_field, expected_field in field_mappings.items():
                if tv_field in alert_data and expected_field not in alert_data:
                    alert_data[expected_field] = alert_data[tv_field]
                
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

                if symbol:
                    from utils import standardize_symbol
                    symbol = standardize_symbol(symbol)
                    # Update alert_data with standardized symbol
                    alert_data["symbol"] = symbol
                    if "instrument" not in alert_data:
                        alert_data["instrument"] = symbol
                
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
                risk_percent = float(alert_data.get('risk_percent', alert_data.get('risk', alert_data.get('percentage', 1.0)))) # Ensure float

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
                
                # Check exits every 5 minutes
                if (current_time - last_run["check_exits"]).total_seconds() >= 300:
                    # 1) Get all open positions from your tracker
                    open_positions = await self.position_tracker.get_open_positions()
                    
                    for symbol, positions_for_symbol in open_positions.items():
                        for pid, pos_data in positions_for_symbol.items():
                            # 2) Fetch current live price (bid/ask midpoint) for this symbol/direction
                            try:
                                current_price = await self.get_current_price(symbol, pos_data["action"])
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
                current_price = await self.get_current_price(symbol, direction)
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
            if self.regime_classifier:
                regime_data = self.regime_classifier.get_regime_data(symbol)
                regime      = regime_data.get("regime", "unknown").lower()
            else:
                regime = "unknown"
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
        """Reconcile positions between database and broker with graceful error handling"""
        try:
            logger.info("Starting position reconciliation with OANDA...")

            # Try to get broker positions with reduced timeout for startup
            try:
                from oandapyV20.endpoints.positions import OpenPositions
                from oandapyV20.endpoints.trades import OpenTrades
                
                r_positions = OpenPositions(accountID=config.oanda_account_id)
                broker_positions_response = await self.robust_oanda_request(r_positions, max_retries=2, initial_delay=1)

                r_trades = OpenTrades(accountID=config.oanda_account_id)
                broker_trades_response = await self.robust_oanda_request(r_trades, max_retries=2, initial_delay=1)
                
            except error_recovery.BrokerConnectionError as broker_err:
                logger.warning(f"Broker reconciliation skipped due to connection issues: {broker_err}")
                logger.info("System will continue without initial reconciliation. Reconciliation will be retried later.")
                return
            
            except Exception as e:
                logger.warning(f"Broker reconciliation failed with unexpected error: {e}")
                logger.info("System will continue without initial reconciliation.")
                return

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

            if self.position_tracker and self.db_manager:
                db_open_positions_data = await self.db_manager.get_open_positions()
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
                                exit_price = await self.get_current_price(full_symbol_for_price, price_fetch_side_for_close)
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
