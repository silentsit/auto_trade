import configparser
import os
import json
import logging
import asyncio
import traceback
import time
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
import aiohttp

# Local application imports
from config import config
import error_recovery
from utils import (
    get_module_logger, normalize_timeframe, standardize_symbol,
    is_instrument_tradeable, get_atr, get_instrument_type,
    get_atr_multiplier, get_trading_logger, parse_iso_datetime,
    _get_simulated_price, validate_trade_inputs, TV_FIELD_MAP, MarketDataUnavailableError,
    calculate_notional_position_size, round_position_size, get_position_size_limits
)
from exit_monitor import exit_monitor
from position_journal import Position
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from volatility_monitor import VolatilityMonitor
from regime_classifier import LorentzianDistanceClassifier
from notification import NotificationSystem
from system_monitor import SystemMonitor
from health_checker import HealthChecker
from profit_ride_override import ProfitRideOverride, OverrideDecision


logger = logging.getLogger(__name__)

class EnhancedAlertHandler:
    def __init__(self, db_manager=None, oanda_service=None, position_tracker=None):
        """
        Initializes the alert handler with all required components.
        """
        if db_manager is None:
            logger.critical("db_manager must be provided to EnhancedAlertHandler. Initialization aborted.")
        
        # Store injected dependencies
        self.db_manager = db_manager
        self.oanda_service = oanda_service 
        self.position_tracker = position_tracker
        
        # Log successful injection
        if self.position_tracker:
            logger.info("✅ AlertHandler: position_tracker successfully injected")
        else:
            logger.warning("⚠️ AlertHandler: position_tracker is None during initialization")
            
        if db_manager is None:
            raise RuntimeError("db_manager must be provided to EnhancedAlertHandler.")

        # Don't override injected dependencies 
        if not hasattr(self, 'db_manager'):
            self.db_manager = db_manager
            
        # Initialize OANDA client (will be overridden by start() if oanda_service injected)
        self.oanda = None

        # Components to be initialized in start() (don't override injected ones)
        if not hasattr(self, 'position_tracker') or self.position_tracker is None:
            self.position_tracker = None
        self.risk_manager = None
        self.volatility_monitor = None
        self.regime_classifier = None
        self.profit_ride_override = None
        self.notification_system = None
        self.system_monitor = None
        self.health_checker = None
        self.task_handler = None

        # State and configuration
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        self._started = False
        self.enable_reconciliation = getattr(config, 'enable_broker_reconciliation', True)
        self.enable_close_overrides = True
        self.forwarding_url_100k = getattr(config, 'forwarding_url_100k', 'https://auto-trade-100k-demo.onrender.com/tradingview')

        self._init_oanda_client()
        logger.info("EnhancedAlertHandler created. Waiting for start() to initialize components.")


    def _init_oanda_client(self):
        """Initialize OANDA client"""
        try:
            access_token = config.oanda_access_token
            if isinstance(access_token, SecretStr):
                access_token = access_token.get_secret_value()

            self.oanda = oandapyV20.API(
                access_token=access_token,
                environment=config.oanda_environment
            )
            logger.info(f"OANDA client initialized in alert handler")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA client: {e}")
            self.oanda = None

    async def robust_oanda_request(self, request, max_retries: int = 5, initial_delay: float = 3.0):
        """Enhanced OANDA API request with sophisticated retry logic"""
        if not self.oanda:
            self._init_oanda_client()
            if not self.oanda:
                raise error_recovery.BrokerConnectionError("OANDA client is not initialized.")

        def is_connection_error(exception):
            from urllib3.exceptions import ProtocolError
            from http.client import RemoteDisconnected
            from requests.exceptions import ConnectionError, Timeout
            import socket

            connection_errors = (
                ConnectionError, RemoteDisconnected, ProtocolError,
                Timeout, socket.timeout, socket.error, OSError,
                error_recovery.BrokerConnectionError
            )
            if isinstance(exception, connection_errors):
                return True

            error_str = str(exception).lower()
            connection_indicators = [
                'connection aborted', 'remote end closed connection', 'connection reset',
                'timeout', 'network is unreachable', 'connection refused', 'broken pipe',
                'connection timed out'
            ]
            return any(indicator in error_str for indicator in connection_indicators)

        for attempt in range(max_retries):
            try:
                return self.oanda.request(request)
            except Exception as e:
                is_conn_error = is_connection_error(e)
                is_final_attempt = attempt == max_retries - 1

                if is_final_attempt:
                    logger.error(f"OANDA request failed after {max_retries} attempts: {e}")
                    raise error_recovery.BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")

                delay = initial_delay * (2 ** attempt) + (attempt * 0.5) if is_conn_error else initial_delay * (1.5 ** attempt)
                log_level = "warning" if is_conn_error else "info"
                getattr(logger, log_level)(f"OANDA request error attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")

                if is_conn_error and attempt >= 1:
                    logger.info("Reinitializing OANDA client after connection error")
                    self._init_oanda_client()

                await asyncio.sleep(delay)

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
            raise MarketDataUnavailableError(f"No price data in OANDA response for {symbol}")
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            raise MarketDataUnavailableError(f"Could not fetch price for {symbol}: {e}")

    async def get_account_balance(self, account_id: str) -> float:
        """Get account balance from OANDA."""
        try:
            account_request = AccountDetails(accountID=account_id)
            response = await self.robust_oanda_request(account_request)
            return float(response['account']['balance'])
        except Exception as e:
            logger.error(f"Error getting account balance for {account_id}: {e}")
            raise

    async def start(self):
        """Initialize & start all components, including optional broker reconciliation."""
        if self._started:
            logger.info("EnhancedAlertHandler.start() called, but already running.")
            return True

        logger.info("Starting EnhancedAlertHandler and its components...")
        startup_errors = []

        try:
            # 1. System Monitor
            self.system_monitor = SystemMonitor()
            await self.system_monitor.register_component("alert_handler", "initializing")

            # 2. Initialize Components
            # Only create new position_tracker if one wasn't injected
            if not self.position_tracker:
                logger.info("Creating new PositionTracker (none was injected)")
                self.position_tracker = PositionTracker(db_manager=self.db_manager)
            else:
                logger.info("Using injected PositionTracker")
            await self.system_monitor.register_component("position_tracker", "initializing")

            self.risk_manager = EnhancedRiskManager()
            await self.system_monitor.register_component("risk_manager", "initializing")

            self.volatility_monitor = VolatilityMonitor()
            self.regime_classifier = LorentzianDistanceClassifier()
            self.profit_ride_override = ProfitRideOverride(self.regime_classifier, self.volatility_monitor)
            self.notification_system = NotificationSystem()
            self.health_checker = HealthChecker(self, self.db_manager)

            # 3. Configure Notifications
            # ... (configure channels as before)

            # 4. Start Components
            # Only start position_tracker if it's not already running
            if hasattr(self.position_tracker, '_running') and not self.position_tracker._running:
                await self.position_tracker.start()
            elif not hasattr(self.position_tracker, '_running'):
                await self.position_tracker.start()
            else:
                logger.info("PositionTracker already running, skipping start()")
            await self.system_monitor.update_component_status("position_tracker", "ok")

            initial_balance = await self.get_account_balance(config.oanda_account_id)
            await self.risk_manager.initialize(initial_balance)
            await self.system_monitor.update_component_status("risk_manager", "ok")
            
            await self.health_checker.start()
            
            if config.enable_exit_signal_monitoring:
                await exit_monitor.start_monitoring()

            # 5. Finalize Startup
            self._running = True
            self._started = True
            self.task_handler = asyncio.create_task(self.handle_scheduled_tasks())

            if not startup_errors:
                logger.info("EnhancedAlertHandler started successfully.")
                await self.system_monitor.update_component_status("alert_handler", "ok")
            else:
                logger.warning(f"EnhancedAlertHandler started with {len(startup_errors)} errors.")
                await self.system_monitor.update_component_status("alert_handler", "warning", f"{len(startup_errors)} startup errors")

            return True

        except Exception as e:
            logger.critical(f"CRITICAL FAILURE during startup: {e}", exc_info=True)
            if self.system_monitor:
                await self.system_monitor.update_component_status("alert_handler", "error", f"Startup failure: {e}")
            return False

    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        """(This method would contain the logic from your original execute_trade, now with multi-account support)"""
        # ... logic for execute_trade ...
        # For simplicity, assuming multi-account logic is handled here or called from here.
        # This is a placeholder for your existing trade execution logic.
        return True, {"message": "Trade executed (placeholder)."}


    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for processing an incoming alert from a webhook."""
        if not self._started:
            logger.critical("process_alert called before EnhancedAlertHandler.start(). This is a fatal usage error.")
            return {"status": "error", "message": "Handler not started"}
        
        if self.position_tracker is None:
            logger.critical("position_tracker is not initialized! This is a critical error.")
            return {"status": "error", "message": "Internal component not initialized"}

        async with self._lock:
            # Standardize alert data
            mapped_fields = {}
            for tv_field, expected_field in TV_FIELD_MAP.items():
                if tv_field in alert_data:
                    # FIX: Correct variable name
                    mapped_fields[expected_field] = alert_data[tv_field]

            alert_data.update(mapped_fields)
            
            # ... (the rest of your alert processing logic remains here)
            # This includes symbol standardization, action detection (BUY/SELL/CLOSE),
            # and calling the appropriate execution logic.

            # Simplified example for demonstration of the fix
            direction = alert_data.get("direction", "").upper()
            if direction == "CLOSE":
                # This part will now work because self.position_tracker is initialized
                logger.info("Processing CLOSE signal...")
                candidate_id = alert_data.get("position_id", "some_id") # Example
                position_info = await self.position_tracker.get_position_info(candidate_id)
                # ... rest of close logic
                return {"status": "success", "message": f"Closed position {candidate_id}"}
            elif direction in ["BUY", "SELL"]:
                logger.info(f"Processing {direction} signal...")
                success, result = await self.execute_trade(alert_data)
                return result
            else:
                return {"status": "error", "message": f"Invalid action: {direction}"}

    # ... (all other helper methods like handle_scheduled_tasks, stop, etc.)
    # The content of the other methods from your file would go here.
    # The provided code focuses on fixing the __init__ and start methods.
    
    async def handle_scheduled_tasks(self):
        """Placeholder for your scheduled tasks."""
        while self._running:
            logger.debug("Running scheduled tasks...")
            await asyncio.sleep(300)

    async def stop(self):
        """Stops the alert handler and all its components."""
        if not self._running:
            return
        self._running = False
        if self.task_handler:
            self.task_handler.cancel()
        if self.health_checker:
            await self.health_checker.stop()
        if config.enable_exit_signal_monitoring:
            await exit_monitor.stop_monitoring()
        logger.info("EnhancedAlertHandler stopped.")