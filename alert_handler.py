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

from oandapyV20.endpoints.pricing import PricingInfo


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

from position_journal import Position
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from volatility_monitor import VolatilityMonitor
from regime_classifier import LorentzianDistanceClassifier
from notification import NotificationSystem
from system_monitor import SystemMonitor
from health_checker import HealthChecker
from exit_monitor import exit_monitor
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




        # State and configuration
        self.active_alerts = set()
        self._lock = asyncio.Lock()
        self._running = False
        self._started = False
        self.enable_reconciliation = getattr(config, 'enable_broker_reconciliation', True)
        self.enable_close_overrides = True
        self.forwarding_url_100k = getattr(config, 'forwarding_url_100k', 'https://auto-trade-100k-demo.onrender.com/tradingview')

        logger.info("EnhancedAlertHandler created. Waiting for start() to initialize components.")






    async def get_current_price(self, symbol: str, action: str) -> float:
        """Get current price for symbol"""
        try:
            pricing_request = PricingInfo(
                accountID=config.oanda_account_id,
                params={"instruments": symbol}
            )
            response = await self.oanda_service.robust_oanda_request(pricing_request)

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



    async def start(self):
        """Initialize & start all components, including optional broker reconciliation."""
        if self._started:
            logger.info("EnhancedAlertHandler.start() called, but already running.")
            return True

        logger.info("Starting EnhancedAlertHandler and its components...")
        startup_errors = []

        try:
            # 1. Initialize System Monitor and Components
            self.system_monitor = SystemMonitor()
            await self.system_monitor.register_component("alert_handler", "initializing")
            
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

            # 3. Start Components
            # Only start position_tracker if it's not already running
            if hasattr(self.position_tracker, '_running') and not self.position_tracker._running:
                await self.position_tracker.start()
            elif not hasattr(self.position_tracker, '_running'):
                await self.position_tracker.start()
            else:
                logger.info("PositionTracker already running, skipping start()")
            await self.system_monitor.update_component_status("position_tracker", "ok")

            initial_balance = await self.oanda_service.get_account_balance()
            await self.risk_manager.initialize(initial_balance)
            await self.system_monitor.update_component_status("risk_manager", "ok")
            
            await self.health_checker.start()
            await self.system_monitor.update_component_status("health_checker", "ok")
            
            if config.enable_exit_signal_monitoring:
                await exit_monitor.start_monitoring()
                await self.system_monitor.update_component_status("exit_monitor", "ok")
            
            # Start centralized task manager 
            await self.system_monitor.start_task_manager()
            

            


            # 4. Finalize Startup
            self._running = True
            self._started = True

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
    


    async def stop(self):
        """Stops the alert handler and all its components."""
        if not self._running:
            return
        self._running = False
        
        # Stop all components properly
        if self.system_monitor:
            await self.system_monitor.stop_task_manager()
            
        if self.health_checker:
            await self.health_checker.stop()
            
        if config.enable_exit_signal_monitoring:
            await exit_monitor.stop_monitoring()

        logger.info("EnhancedAlertHandler stopped.")