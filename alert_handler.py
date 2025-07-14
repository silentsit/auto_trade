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
from config import config
import error_recovery
from utils import (
    get_module_logger, normalize_timeframe, standardize_symbol, 
    is_instrument_tradeable, get_atr, get_instrument_type, 
    get_atr_multiplier, get_trading_logger, parse_iso_datetime,
    _get_simulated_price, validate_trade_inputs, TV_FIELD_MAP, MarketDataUnavailableError, calculate_simple_position_size, get_position_size_limits, get_instrument_leverage, calculate_notional_position_size, round_position_size
)
from exit_monitor import exit_monitor
from dataclasses import dataclass
from typing import Dict, Any
from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from position_journal import Position
from services_x import ProfitRideOverride, OverrideDecision, PositionTracker

@dataclass
class OverrideDecision:
    ignore_close: bool
    sl_atr_multiple: float = 2.0
    tp_atr_multiple: float = 4.0

class ProfitRideOverride:
    def __init__(self, regime: LorentzianDistanceClassifier, vol: VolatilityMonitor):
        self.regime = regime
        self.vol = vol

    async def should_override(self, position: Position, current_price: float, drawdown: float = 0.0) -> OverrideDecision:
        atr = await get_atr(position.symbol, position.timeframe)
        if atr <= 0:
            return OverrideDecision(ignore_close=False)

        reg = self.regime.get_regime_data(position.symbol).get("regime", "")
        is_trending = "trending" in reg

        vol_state = self.vol.get_volatility_state(position.symbol)
        vol_ok = vol_state.get("volatility_ratio", 2.0) < 1.5

        # Placeholder for SMA10 calculation
        sma10 = current_price  # TODO: Replace with actual SMA10 logic
        momentum = abs(current_price - sma10) / atr if atr else 0
        strong_momentum = momentum > 0.15

        hh = True  # TODO: Replace with actual higher-highs logic

        initial_risk = abs(position.entry_price - (position.stop_loss or position.entry_price)) * position.size
        realized_ok = getattr(position, "pnl", 0) >= initial_risk

        ignore = (is_trending or vol_ok) and (strong_momentum or hh) and realized_ok

        return OverrideDecision(
            ignore_close=ignore,
            sl_atr_multiple=2.0 if ignore else 1.5,
            tp_atr_multiple=4.0 if ignore else 2.5
        )

    async def _higher_highs(self, symbol: str, tf: str, bars: int) -> bool:
        # TODO: Implement candle fetching and higher-highs logic
        return True

logger = logging.getLogger(__name__)

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
        self.task_handler = None  # Initialize task_handler attribute
        self.profit_ride_override = None  # Add this line
        
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

    async def robust_oanda_request(self, request, max_retries: int = 5, initial_delay: float = 3.0):
        """Enhanced OANDA API request with sophisticated retry logic"""
        if not self.oanda:
            self._init_oanda_client()
            if not self.oanda:
                raise Exception("OANDA client not initialized")
        
        def is_connection_error(exception):
            """Check if the exception is a connection-related error"""
            from urllib3.exceptions import ProtocolError
            from http.client import RemoteDisconnected
            from requests.exceptions import ConnectionError, Timeout
            import socket
            
            connection_errors = (
                ConnectionError,
                RemoteDisconnected, 
                ProtocolError,
                Timeout,
                socket.timeout,
                socket.error,
                OSError,
                error_recovery.BrokerConnectionError
            )
            
            if isinstance(exception, connection_errors):
                return True
                
            # Check for specific error messages
            error_str = str(exception).lower()
            connection_indicators = [
                'connection aborted',
                'remote end closed connection',
                'connection reset',
                'timeout',
                'network is unreachable',
                'connection refused',
                'broken pipe',
                'connection timed out'
            ]
            
            return any(indicator in error_str for indicator in connection_indicators)
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"OANDA request attempt {attempt + 1}/{max_retries}")
                response = self.oanda.request(request)
                return response
                
            except Exception as e:
                is_conn_error = is_connection_error(e)
                is_final_attempt = attempt == max_retries - 1
                
                if is_final_attempt:
                    logger.error(f"OANDA request failed after {max_retries} attempts: {e}")
                    raise error_recovery.BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")
                
                # Calculate delay with jitter for connection errors
                if is_conn_error:
                    # Longer delays for connection errors
                    delay = initial_delay * (2 ** attempt) + (attempt * 0.5)  # Add jitter
                    logger.warning(f"OANDA connection error attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")
                    
                    # Reinitialize client for connection errors after 2nd attempt
                    if attempt >= 1:
                        logger.info("Reinitializing OANDA client after connection error")
                        self._init_oanda_client()
                else:
                    # Shorter delays for other errors
                    delay = initial_delay * (1.5 ** attempt)
                    logger.warning(f"OANDA request error attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")
                
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

    async def execute_trade_multi_account(self, payload: dict, target_accounts: list = None) -> dict:
        """Execute trade on multiple OANDA accounts"""
        try:
            results = {}
            
            # Determine which accounts to target
            if target_accounts is None:
                # Check for accounts array in payload
                if "accounts" in payload and isinstance(payload["accounts"], list):
                    target_accounts = payload["accounts"]
                # Check for broadcast mode
                elif payload.get("account") == "BROADCAST":
                    # Get all configured accounts (you can configure this in config.py)
                    target_accounts = getattr(config, 'multi_accounts', [
                        "101-003-26651494-006",
                        "101-003-26651494-011"
                    ])
                # Check for single account
                elif payload.get("account"):
                    target_accounts = [payload["account"]]
                # Fall back to config default
                else:
                    target_accounts = [config.oanda_account_id]
            
            logger.info(f"Executing trade on {len(target_accounts)} accounts: {target_accounts}")
            
            # Execute trade on each account
            for account_id in target_accounts:
                try:
                    logger.info(f"Executing trade on account: {account_id}")
                    
                    # Create a copy of the payload for this account
                    account_payload = payload.copy()
                    account_payload["target_account"] = account_id
                    
                    # Execute trade for this specific account
                    success, result = await self.execute_trade_single_account(account_payload, account_id)
                    
                    results[account_id] = {
                        "success": success,
                        "result": result,
                        "account_id": account_id
                    }
                    
                    if success:
                        logger.info(f"✅ Trade executed successfully on account {account_id}")
                    else:
                        logger.error(f"❌ Trade failed on account {account_id}: {result}")
                        
                except Exception as e:
                    logger.error(f"❌ Error executing trade on account {account_id}: {str(e)}")
                    results[account_id] = {
                        "success": False,
                        "result": {"error": str(e)},
                        "account_id": account_id
                    }
            
            # Calculate overall success
            successful_accounts = [acc for acc, res in results.items() if res["success"]]
            total_accounts = len(target_accounts)
            success_rate = len(successful_accounts) / total_accounts if total_accounts > 0 else 0
            
            summary = {
                "multi_account_execution": True,
                "total_accounts": total_accounts,
                "successful_accounts": len(successful_accounts),
                "success_rate": success_rate,
                "successful_account_ids": successful_accounts,
                "detailed_results": results
            }
            
            logger.info(f"Multi-account execution summary: {len(successful_accounts)}/{total_accounts} accounts successful")
            
            return summary
            
        except Exception as e:
            logger.error(f"Multi-account execution failed: {str(e)}")
            return {
                "multi_account_execution": True,
                "error": str(e),
                "successful_accounts": 0,
                "total_accounts": len(target_accounts) if target_accounts else 0
            }

    async def execute_trade_single_account(self, payload: dict, account_id: str) -> tuple[bool, dict]:
        """Execute trade on a single specific OANDA account"""
        try:
            # Import all required modules at the top of the try block
            from config import config
            from utils import (
                round_position_size, calculate_simple_position_size, 
                get_position_size_limits, validate_trade_inputs, 
                get_atr, MarketDataUnavailableError
            )
            from oandapyV20.endpoints.orders import OrderCreate
            
            symbol = payload.get("symbol")
            action = payload.get("action")
            risk_percent = payload.get("risk_percent", 1.0)
            
            # Pre-trade checks
            if not symbol or not action:
                logger.error(f"Trade execution aborted: Missing symbol or action in payload: {payload}")
                return False, {"error": "Missing symbol or action in trade payload"}
                
            # Get account balance (you may need to modify this to work with different accounts)
            account_balance = await self.get_account_balance()
            
            # Get current price
            try:
                current_price = await self.get_current_price(symbol, action)
            except MarketDataUnavailableError as e:
                logger.error(f"Trade execution aborted: {e}")
                return False, {"error": str(e)}
                
            # Get signal price (intended entry price)
            signal_price = payload.get("signal_price", current_price)
            
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
                
            # Use notional allocation position sizing (15% of equity, with leverage)
            allocation_percent = 15.0
            position_size = calculate_notional_position_size(
                account_balance=account_balance,
                allocation_percent=allocation_percent,
                current_price=current_price,
                symbol=symbol
            )

            logger.info(f"[NOTIONAL SIZING] {symbol} on {account_id}: Alloc%={allocation_percent:.2f}, Units={position_size}")
            
            if position_size <= 0:
                logger.error(f"Trade execution aborted: Calculated position size is zero or negative")
                return False, {"error": "Calculated position size is zero or negative"}
            
            # Round position size to OANDA requirements
            raw_position_size = position_size
            position_size = round_position_size(symbol, position_size)
            
            logger.info(f"[NOTIONAL SIZING] {symbol} on {account_id}: Raw={raw_position_size:.2f}, Rounded={position_size}, Alloc%={allocation_percent:.2f}")
            
            if position_size <= 0:
                logger.error(f"Trade execution aborted: Rounded position size is zero")
                return False, {"error": "Rounded position size is zero"}
            
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
                logger.error(f"Trade validation failed for {symbol} on {account_id}: {validation_reason}")
                return False, {"error": f"Trade validation failed: {validation_reason}"}
            
            logger.info(f"Trade validation passed for {symbol} on {account_id}: {validation_reason}")
            
            # Create OANDA order - ensure units are integers for forex
            units_value = int(position_size) if action.upper() == "BUY" else -int(position_size)
            
            order_data = {
                "order": {
                    "type": "MARKET",
                    "instrument": symbol,
                    "units": str(units_value),  # Convert to string for OANDA API
                    "timeInForce": "FOK"
                }
            }
            
            # Use the specific account ID for this trade
            order_request = OrderCreate(
                accountID=account_id,  # This is the key change - use specific account
                data=order_data
            )
            
            response = await self.robust_oanda_request(order_request)
            
            if 'orderFillTransaction' in response:
                fill_info = response['orderFillTransaction']
                fill_price = float(fill_info.get('price', current_price))
                actual_units = abs(float(fill_info.get('units', position_size)))
                
                # Calculate slippage
                if action.upper() == "BUY":
                    slippage = fill_price - signal_price
                else:
                    slippage = signal_price - fill_price
                    
                logger.info(
                    f"Trade execution for {symbol} on account {account_id}: "
                    f"Account Balance=${account_balance:.2f}, "
                    f"Risk%={risk_percent:.2f}, "
                    f"Signal Price={signal_price}, Fill Price={fill_price}, Slippage={slippage:.5f}, "
                    f"Entry={current_price}, Stop={stop_loss}, "
                    f"Position Size={position_size}"
                )
                
                # Record position with account-specific ID
                if self.position_tracker:
                    base_position_id = payload.get("request_id", payload.get("alert_id", f"{symbol}_{action}_{int(datetime.now().timestamp())}"))
                    position_id = f"{base_position_id}_{account_id}"  # Make position ID unique per account
                    
                    if base_position_id:
                        try:
                            await self.position_tracker.record_position(
                                position_id=position_id,
                                symbol=symbol,
                                action=action.upper(),
                                timeframe=payload.get("timeframe", "H1"),
                                entry_price=fill_price,
                                size=actual_units,
                                stop_loss=stop_loss,
                                take_profit=None,
                                metadata={
                                    "signal_price": signal_price,
                                    "slippage": slippage,
                                    "transaction_id": fill_info.get('id'),
                                    "risk_percent": risk_percent,
                                    "comment": payload.get("comment", ""),
                                    "account": account_id,  # Store the actual account used
                                    "original_alert_id": base_position_id
                                }
                            )
                            logger.info(f"Position {position_id} recorded successfully for account {account_id}")
                        except Exception as e:
                            logger.error(f"Failed to record position for account {account_id}: {str(e)}")
                
                return True, {
                    "success": True,
                    "fill_price": fill_price,
                    "units": actual_units,
                    "transaction_id": fill_info.get('id'),
                    "symbol": symbol,
                    "action": action,
                    "signal_price": signal_price,
                    "slippage": slippage,
                    "account_id": account_id,
                    "position_id": position_id if 'position_id' in locals() else None
                }
            else:
                logger.error(f"Order not filled for {symbol} on account {account_id}: {response}")
                return False, {"error": "Order not filled", "response": response, "account_id": account_id}
                
        except Exception as e:
            logger.error(f"Trade execution error for account {account_id}: {str(e)}")
            return False, {"error": str(e), "account_id": account_id}

    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        """Execute trade with OANDA, with support for multiple accounts"""
        try:
            # Check if this is a multi-account request
            is_multi_account = (
                "accounts" in payload and isinstance(payload["accounts"], list) and len(payload["accounts"]) > 1
            ) or payload.get("account") == "BROADCAST"
            
            if is_multi_account:
                logger.info("Multi-account execution detected")
                result = await self.execute_trade_multi_account(payload)
                
                # Return in the expected format for compatibility
                overall_success = result.get("successful_accounts", 0) > 0
                return overall_success, result
            else:
                # Single account execution (backward compatibility)
                account_id = payload.get("account", config.oanda_account_id)
                logger.info(f"Single account execution on: {account_id}")
                return await self.execute_trade_single_account(payload, account_id)
                
        except Exception as e:
            logger.error(f"Execute trade error: {str(e)}")
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

            # Initialize profit ride override
            self.profit_ride_override = ProfitRideOverride(self.regime_classifier, self.volatility_monitor)

            self.position_journal = PositionJournal()
            await self.system_monitor.register_component("position_journal", "initializing")

            self.notification_system = NotificationSystem()
            await self.system_monitor.register_component("notification_system", "initializing")

            # Initialize HealthChecker with weekend position monitoring
            from health_checker import HealthChecker
            self.health_checker = HealthChecker(self, self.db_manager)
            await self.system_monitor.register_component("health_checker", "initializing")

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

            # Start health checker with weekend monitoring
            try:
                logger.info("Starting HealthChecker with weekend position monitoring...")
                await self.health_checker.start()
                await self.system_monitor.update_component_status("health_checker", "ok")
                logger.info("HealthChecker started successfully")
            except Exception as e:
                startup_errors.append(f"HealthChecker failed: {e}")
                await self.system_monitor.update_component_status("health_checker", "error", str(e))
                logger.error(f"HealthChecker startup failed: {e}")

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

                        # Start exit signal monitoring
            if config.enable_exit_signal_monitoring:
                try:
                    logger.info("Starting exit signal monitoring...")
                    await exit_monitor.start_monitoring()
                    logger.info("Exit signal monitoring started successfully")
                except Exception as e:
                    startup_errors.append(f"Exit monitor failed: {e}")
                    logger.error(f"Failed to start exit monitor: {e}")

            # Finalize startup
            self._running = True
            
            # Start background scheduled tasks
            try:
                logger.info("Starting background scheduled tasks...")
                self.task_handler = asyncio.create_task(self.handle_scheduled_tasks())
                logger.info("Background scheduled tasks started successfully")
            except Exception as e:
                startup_errors.append(f"Background tasks failed: {e}")
                logger.error(f"Failed to start background tasks: {e}")
            
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

    async def _should_override_close(self, position_id: str, position_data: dict) -> tuple[bool, str]:
        """Check if a close signal should be overridden (e.g., due to risk management rules)"""
        try:
            # For now, we don't override any close signals
            # You can add logic here later for:
            # - Maximum loss protection
            # - Time-based overrides
            # - Volatility-based overrides
            # - Correlation-based overrides
            return False, "No override conditions met"
        except Exception as e:
            logger.error(f"Error checking close override for {position_id}: {str(e)}")
            return False, f"Error in override check: {str(e)}"

    def _resolve_template_variables(self, template_string: str, alert_data: dict) -> str:
        """
        Resolve template variables in strings like position IDs
        """
        if not template_string or "{{" not in template_string:
            return template_string
            
        resolved = template_string
        
        # Get values from alert data
        timeframe = alert_data.get("timeframe", "H1")
        direction = alert_data.get("direction", alert_data.get("action", ""))
        symbol = alert_data.get("symbol", "")
        
        # Standardize symbol for template replacement
        if symbol:
            symbol_clean = standardize_symbol(symbol)
            if symbol_clean:
                symbol = symbol_clean.replace("_", "")
        
        # Replace template variables
        template_map = {
            "{{timeframe}}": timeframe,
            "{{direction}}": direction.upper(),
            "{{symbol}}": symbol,
            "{{ticker}}": symbol,
            "{{instrument}}": symbol
        }
        
        for template, value in template_map.items():
            if template in resolved and value:
                resolved = resolved.replace(template, str(value))
                
        return resolved

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
        if self.position_tracker is None:
            logger.error("position_tracker is not initialized! This is a critical error and should never happen.")
            raise RuntimeError("position_tracker is not initialized! This is a critical error.")
        async with self._lock:
            
            # === ENHANCED ALERT PROCESSING FOR EXITS ===
            # Add comprehensive logging for exit signals
            if alert_data.get("action") == "CLOSE" or alert_data.get("direction") == "CLOSE":
                logger.info(f"[EXIT SIGNAL RECEIVED] Full alert data: {alert_data}")
            
            mapped_fields = {}
            for tv_field, expected_field in TV_FIELD_MAP.items():
                if tv_field in alert_data:
                    mapped_fields[expected_field] = alert_data[tv_field]
                    logger.info(f"[FIELD MAPPING] {tv_field}='{alert_data[tv_field]}' → {expected_field}")
            
            # Merge mapped fields back into alert_data
            alert_data.update(mapped_fields)
            
            # === ENHANCED EXIT SIGNAL PREPROCESSING ===
            # Handle JSON string parsing if needed
            if isinstance(alert_data.get('message'), str):
                try:
                    message_data = json.loads(alert_data['message'])
                    # Merge message data into alert_data
                    alert_data.update(message_data)
                    logger.info(f"[JSON PARSE] Parsed message data: {message_data}")
                except (json.JSONDecodeError, TypeError):
                    # Not JSON, continue with original processing
                    pass
            
            # === CRYPTO SIGNAL DETECTION ===
            # DISABLED: Auto-rejection of crypto signals to allow OANDA crypto CFDs/ETFs
            # Check if this is a crypto signal and handle appropriately
            symbol = alert_data.get("symbol") or alert_data.get("instrument", "")
            if symbol:
                logger.info(f"[CRYPTO PROCESSING] Processing symbol {symbol} normally (auto-rejection disabled)")
                # TODO: Re-enable with proper OANDA crypto instrument detection if needed
                # try:
                #     from crypto_signal_handler import is_crypto_signal, handle_crypto_signal_rejection
                #     
                #     if is_crypto_signal(symbol):
                #         handled, reason = handle_crypto_signal_rejection(alert_data)
                #         if handled:
                #             logger.warning(f"[CRYPTO SIGNAL REJECTED] {symbol}: {reason}")
                #             return {"status": "rejected", "reason": reason, "signal_type": "crypto_unsupported"}
                # except ImportError:
                #     # Crypto handler not available, continue normal processing
                #     logger.info(f"[CRYPTO HANDLER] Module not available, processing {symbol} normally")
                #     pass
            
            # === COMPREHENSIVE CLOSE SIGNAL DETECTION ===
            # Check for close signals in multiple formats TradingView might send
            close_signal_detected = False
            close_signal_source = ""
            
            # Method 1: Check message field for close keywords
            message = alert_data.get('message', '').upper()
            close_keywords = ['CLOSE_POSITION', 'CLOSE', 'EXIT', 'STOP', 'SELL_TO_CLOSE', 'BUY_TO_CLOSE']
            if any(keyword in message for keyword in close_keywords):
                close_signal_detected = True
                close_signal_source = f"message='{message}'"
                logger.info(f"[CLOSE DETECTION] Close signal detected in message: {message}")
            
            # Method 2: Check action/direction fields
            action = alert_data.get('action', '').upper()
            direction = alert_data.get('direction', '').upper()
            side = alert_data.get('side', '').upper()
            
            if action in ['CLOSE', 'EXIT', 'STOP'] or direction in ['CLOSE', 'EXIT', 'STOP'] or side in ['CLOSE', 'EXIT', 'STOP']:
                close_signal_detected = True
                close_signal_source = f"action='{action}', direction='{direction}', side='{side}'"
                logger.info(f"[CLOSE DETECTION] Close signal detected in action/direction fields: {close_signal_source}")
            
            # Method 3: Check for specific alert condition messages from Pine Script
            alert_condition = alert_data.get('alertcondition', '').upper()
            if 'CLOSE' in alert_condition or 'EXIT' in alert_condition:
                close_signal_detected = True
                close_signal_source = f"alertcondition='{alert_condition}'"
                logger.info(f"[CLOSE DETECTION] Close signal detected in alertcondition: {alert_condition}")
            
            # If close signal detected, standardize the direction field
            if close_signal_detected:
                alert_data["direction"] = "CLOSE"
                alert_data["action"] = "CLOSE"  # Also set action for consistency
                logger.info(f"[CLOSE SIGNAL STANDARDIZED] Set direction=CLOSE, action=CLOSE (source: {close_signal_source})")
                logger.info(f"[EXIT SIGNAL RECEIVED] Full alert data: {alert_data}")
            
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
                symbol = standardize_symbol(original_symbol)
                
                if not symbol:
                    logger.error(f"[SYMBOL ERROR] Failed to standardize symbol: '{original_symbol}'")
                    return {"status": "error", "message": f"Failed to standardize symbol: {original_symbol}", "alert_id": str(uuid.uuid4())}
                
                alert_data["symbol"] = symbol
                alert_data["instrument"] = symbol  # OANDA expects 'instrument' field
                
                logger.info(f"[SYMBOL MAPPING] '{original_symbol}' → '{symbol}'")
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
                    # from utils import standardize_symbol  # REMOVE THIS LINE
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
                
                # === ENHANCED CLOSE ACTION HANDLING ===
                if direction == "CLOSE":
                    # Record exit signal in monitor if enabled
                    signal_id = None
                    start_time = time.time()
                    if getattr(config, 'enable_exit_signal_monitoring', False):
                        try:
                            signal_id = await exit_monitor.record_exit_signal(alert_data)
                        except Exception as e:
                            logger_instance.warning(f"Failed to record exit signal in monitor: {e}")
                    
                    if not symbol:
                        if signal_id:
                            await exit_monitor.record_exit_failure(signal_id, "Symbol not provided for CLOSE action")
                        logger_instance.error(f"Symbol not provided for CLOSE action. Alert ID: {alert_id}")
                        return {"status": "error", "message": "Symbol required for CLOSE action", "alert_id": alert_id}
                    
                    standardized = standardize_symbol(symbol)
                    if not standardized:
                        if signal_id:
                            await exit_monitor.record_exit_failure(signal_id, f"Failed to standardize symbol '{symbol}'")
                        logger_instance.error(f"[ID: {alert_id}] Failed to standardize symbol '{symbol}' for CLOSE")
                        return {"status": "error", "message": f"Cannot close—invalid symbol format: {symbol}", "alert_id": alert_id}
        
                    # === SIMPLIFIED AND MORE RELIABLE POSITION MATCHING ===
                    position_to_close = None
                    close_method = "unknown"
                    
                    logger_instance.info(f"[EXIT PROCESSING] Starting exit for symbol={standardized}")
                    
                    # Primary method: Use exact position_id or alert_id from the alert
                    position_id_candidates = [
                        alert_data.get("position_id"),
                        alert_data.get("alert_id"), 
                        alert_data.get("request_id"),
                        alert_data.get("id")
                    ]
                    
                    # Filter out None values and log all candidates
                    valid_candidates = [pid for pid in position_id_candidates if pid]
                    logger_instance.info(f"[EXIT] Position ID candidates: {valid_candidates}")
                    
                    # Try exact matches first
                    for candidate_id in valid_candidates:
                        if candidate_id:
                            if not self.position_tracker:
                                logger_instance.error("[EXIT] position_tracker is not initialized! Cannot call get_position_info.")
                                return {
                                    "status": "error",
                                    "message": "Internal error: position_tracker is not initialized.",
                                    "alert_id": alert_id
                                }
                            position_data = await self.position_tracker.get_position_info(candidate_id)
                            if position_data is None:
                                logger_instance.error(f"[EXIT] get_position_info returned None for candidate_id: {candidate_id}")
                                continue
                            if position_data.get("status") == "open":
                                position_to_close = {
                                    "position_id": candidate_id,
                                    "data": position_data
                                }
                                close_method = f"exact_match_{candidate_id}"
                                logger_instance.info(f"[EXIT] Found exact position match: {candidate_id}")
                                break
                    
                    # NEW: Try account-specific position ID matching if exact match failed
                    if not position_to_close and valid_candidates:
                        account_id = alert_data.get("account", "")
                        if account_id:
                            logger_instance.info(f"[EXIT] Trying account-specific position ID matching for account: {account_id}")
                            for candidate_id in valid_candidates:
                                if candidate_id:
                                    # Try with account suffix
                                    account_specific_id = f"{candidate_id}_{account_id}"
                                    if not self.position_tracker:
                                        logger_instance.error("[EXIT] position_tracker is not initialized! Cannot call get_position_info.")
                                        return {
                                            "status": "error",
                                            "message": "Internal error: position_tracker is not initialized.",
                                            "alert_id": alert_id
                                        }
                                    position_data = await self.position_tracker.get_position_info(account_specific_id)
                                    if position_data is None:
                                        logger_instance.error(f"[EXIT] get_position_info returned None for account_specific_id: {account_specific_id}")
                                        continue
                                    if position_data.get("status") == "open":
                                        position_to_close = {
                                            "position_id": account_specific_id,
                                            "data": position_data
                                        }
                                        close_method = f"account_specific_match_{account_specific_id}"
                                        logger_instance.info(f"[EXIT] Found account-specific position match: {account_specific_id}")
                                        break
                        else:
                            # If no account specified, try all configured accounts
                            for account_id in config.multi_accounts:
                                logger_instance.info(f"[EXIT] Trying account-specific position ID matching for account: {account_id}")
                                for candidate_id in valid_candidates:
                                    if candidate_id:
                                        # Try with account suffix
                                        account_specific_id = f"{candidate_id}_{account_id}"
                                        if not self.position_tracker:
                                            logger_instance.error("[EXIT] position_tracker is not initialized! Cannot call get_position_info.")
                                            return {
                                                "status": "error",
                                                "message": "Internal error: position_tracker is not initialized.",
                                                "alert_id": alert_id
                                            }
                                        position_data = await self.position_tracker.get_position_info(account_specific_id)
                                        if position_data is None:
                                            logger_instance.error(f"[EXIT] get_position_info returned None for account_specific_id: {account_specific_id}")
                                            continue
                                        if position_data.get("status") == "open":
                                            position_to_close = {
                                                "position_id": account_specific_id,
                                                "data": position_data
                                            }
                                            close_method = f"multi_account_match_{account_specific_id}"
                                            logger_instance.info(f"[EXIT] Found multi-account position match: {account_specific_id}")
                                            break
                                if position_to_close:
                                    break
                    
                    # Enhanced Fallback: Find latest open position for this symbol with matching direction
                    if not position_to_close:
                        open_positions = await self.position_tracker.get_open_positions()
                        symbol_positions = open_positions.get(standardized, {})
                        
                        if symbol_positions:
                            logger_instance.info(f"[EXIT FALLBACK] Found {len(symbol_positions)} open positions for {standardized}")
                            
                            # === DETERMINE TARGET DIRECTION TO CLOSE ===
                            # Extract the direction/action of position we want to close from various fields
                            target_direction = None
                            
                            # Method 1: Check for explicit close direction fields
                            direction_candidates = [
                                alert_data.get("close_direction"),
                                alert_data.get("original_direction"), 
                                alert_data.get("position_direction"),
                                alert_data.get("trade_direction"),
                                alert_data.get("side"),
                                alert_data.get("action_to_close")
                            ]
                            
                            for candidate in direction_candidates:
                                if candidate and str(candidate).upper() in ["BUY", "SELL"]:
                                    target_direction = str(candidate).upper()
                                    logger_instance.info(f"[EXIT] Target direction from explicit field: {target_direction}")
                                    break
                            
                            # Method 1.5: Parse comment field for direction hints (e.g., "Close Long Signal", "Close Short Signal")
                            if not target_direction:
                                comment = alert_data.get("comment", "").upper()
                                if comment:
                                    logger_instance.info(f"[EXIT] Analyzing comment for direction hints: '{comment}'")
                                    
                                    # Check for Long/Short patterns
                                    if any(pattern in comment for pattern in ["CLOSE LONG", "LONG SIGNAL", "LONG EXIT", "EXIT LONG", "LONG CLOSE"]):
                                        target_direction = "BUY"  # Long positions are BUY positions
                                        logger_instance.info(f"[EXIT] Target direction from comment (Long): {target_direction}")
                                    elif any(pattern in comment for pattern in ["CLOSE SHORT", "SHORT SIGNAL", "SHORT EXIT", "EXIT SHORT", "SHORT CLOSE"]):
                                        target_direction = "SELL"  # Short positions are SELL positions
                                        logger_instance.info(f"[EXIT] Target direction from comment (Short): {target_direction}")
                                    
                                    # Check for direct BUY/SELL mentions in comment
                                    elif "BUY" in comment and "SELL" not in comment:
                                        target_direction = "BUY"
                                        logger_instance.info(f"[EXIT] Target direction from comment (BUY): {target_direction}")
                                    elif "SELL" in comment and "BUY" not in comment:
                                        target_direction = "SELL"
                                        logger_instance.info(f"[EXIT] Target direction from comment (SELL): {target_direction}")
                            
                            # Method 2: Infer from original trade action if this is related to specific signal
                            if not target_direction:
                                # Try to match with recently opened position action
                                for pos_id, pos_data in symbol_positions.items():
                                    pos_action = pos_data.get("action", "").upper()
                                    if pos_action in ["BUY", "SELL"]:
                                        target_direction = pos_action
                                        logger_instance.info(f"[EXIT] Inferred direction from most recent position: {target_direction}")
                                        break
                            
                            # === FIND MATCHING POSITIONS ===
                            matching_positions = []
                            
                            for pos_id, pos_data in symbol_positions.items():
                                pos_action = pos_data.get("action", "").upper()
                                
                                # If we have target direction, filter by it
                                if target_direction and pos_action != target_direction:
                                    logger_instance.debug(f"[EXIT] Skipping position {pos_id} - direction mismatch: {pos_action} != {target_direction}")
                                    continue
                                
                                # Add to matching positions
                                open_time_str = pos_data.get("open_time")
                                if open_time_str:
                                    try:
                                        from utils import parse_iso_datetime
                                        open_time = parse_iso_datetime(open_time_str)
                                        matching_positions.append({
                                            "position_id": pos_id,
                                            "data": pos_data,
                                            "open_time": open_time,
                                            "action": pos_action
                                        })
                                    except Exception as e:
                                        logger_instance.warning(f"[EXIT] Could not parse open_time for {pos_id}: {e}")
                                        # Still add it but without timestamp
                                        matching_positions.append({
                                            "position_id": pos_id,
                                            "data": pos_data,
                                            "open_time": None,
                                            "action": pos_action
                                        })
                            
                            logger_instance.info(f"[EXIT] Found {len(matching_positions)} positions matching criteria (target_direction: {target_direction})")
                            
                            # === SELECT LATEST MATCHING POSITION ===
                            if matching_positions:
                                # Sort by open_time (latest first), handling None values
                                from datetime import datetime, timezone
                                matching_positions.sort(
                                    key=lambda x: x["open_time"] if x["open_time"] else datetime.min.replace(tzinfo=timezone.utc), 
                                    reverse=True
                                )
                                
                                latest_position = matching_positions[0]
                                position_to_close = {
                                    "position_id": latest_position["position_id"],
                                    "data": latest_position["data"]
                                }
                                
                                if target_direction:
                                    close_method = f"direction_aware_fallback_{target_direction}"
                                    logger_instance.info(f"[EXIT] Using direction-aware fallback - closing latest {target_direction} position: {latest_position['position_id']}")
                                else:
                                    close_method = "latest_position_fallback"
                                    logger_instance.info(f"[EXIT] Using latest position fallback - closing: {latest_position['position_id']}")
                                
                                # Log all matching positions for transparency
                                for i, pos in enumerate(matching_positions):
                                    status = "SELECTED" if i == 0 else "AVAILABLE"
                                    logger_instance.info(f"[EXIT] {status}: {pos['position_id']} ({pos['action']}, opened: {pos['open_time']})")
                            
                            else:
                                logger_instance.warning(f"[EXIT] No positions found matching direction criteria for {standardized}")
                                # Fallback to any position if direction filtering failed
                                all_positions = list(symbol_positions.items())
                                if all_positions:
                                    pos_id, pos_data = all_positions[0]  # Just take first available
                                    position_to_close = {"position_id": pos_id, "data": pos_data}
                                    close_method = "any_position_fallback"
                                    logger_instance.warning(f"[EXIT] Using any-position fallback - closing: {pos_id}")
                    
                    # Execute close or report failure
                    if not position_to_close:
                        # Enhanced error reporting
                        all_open = await self.position_tracker.get_open_positions()
                        total_positions = sum(len(positions) for positions in all_open.values())
                        
                        logger_instance.error(f"[EXIT FAILED] No position found to close!")
                        logger_instance.error(f"  - Symbol: {standardized}")
                        logger_instance.error(f"  - Candidates tried: {valid_candidates}")
                        logger_instance.error(f"  - Total open positions: {total_positions}")
                        logger_instance.error(f"  - Open symbols: {list(all_open.keys())}")
                        
                        return {
                            "status": "error", 
                            "message": "No matching position found for exit signal", 
                            "alert_id": alert_id,
                            "symbol": standardized,
                            "debug_info": {
                                "position_candidates": valid_candidates,
                                "open_symbols": list(all_open.keys()),
                                "total_open_positions": total_positions
                            }
                        }
                    
                    # === DYNAMIC EXIT OVERRIDE LOGIC ===
                    if self.profit_ride_override and position_to_close:
                        position_data = position_to_close["data"]
                        position = Position(
                            position_id=position_to_close["position_id"],
                            symbol=position_data["symbol"],
                            action=position_data["action"],
                            timeframe=position_data.get("timeframe", "H1"),
                            entry_price=position_data["entry_price"],
                            size=position_data["size"],
                            stop_loss=position_data.get("stop_loss"),
                            take_profit=position_data.get("take_profit"),
                            metadata=position_data.get("metadata", {})
                        )
                        current_price = await self.get_current_price(position.symbol, position.action)
                        # --- Get drawdown from risk manager ---
                        drawdown = 0.0
                        if self.risk_manager and hasattr(self.risk_manager, 'get_risk_metrics'):
                            try:
                                risk_metrics = await self.risk_manager.get_risk_metrics()
                                drawdown = float(risk_metrics.get('drawdown', 0.0))
                            except Exception as e:
                                logger.warning(f"Could not fetch drawdown from risk manager: {e}")
                        # --- Call override logic with drawdown ---
                        override_decision = await self.profit_ride_override.should_override(position, current_price, drawdown=drawdown)
                        if override_decision.ignore_close:
                            atr = await get_atr(position.symbol, position.timeframe)
                            sign = 1 if position.action.upper() == "BUY" else -1
                            new_sl = current_price - sign * override_decision.sl_atr_multiple * atr
                            new_tp = current_price + sign * override_decision.tp_atr_multiple * atr
                            # Update broker SL/TP (implement _update_broker_sl_tp if not present)
                            if hasattr(self, '_update_broker_sl_tp'):
                                await self._update_broker_sl_tp(position.position_id, new_sl, new_tp)
                            # Update tracker SL/TP
                            await self.position_tracker.update_stop_loss(position.position_id, new_sl)
                            await self.position_tracker.update_take_profit(position.position_id, new_tp)
                            # --- Set override flag in metadata and persist ---
                            position.metadata['profit_ride_override_fired'] = True
                            await self.position_tracker.update_metadata(position.position_id, position.metadata)
                            logger.info(f"[DYNAMIC EXIT OVERRIDE] Ignored close for {position.position_id}, updated SL/TP dynamically.")
                            return {
                                "status": "ignored",
                                "message": "Dynamic exit override: let profit ride, updated SL/TP.",
                                "position_id": position.position_id
                            }

                    # Execute the position close
                    try:
                        exit_price = alert_data.get("exit_price") or await self.get_current_price(standardized, position_to_close["data"].get("action"))
                        
                        logger_instance.info(f"[EXIT] Closing position {position_to_close['position_id']} at price {exit_price}")
                        
                        # *** FIX: ACTUALLY CLOSE THE POSITION ON OANDA FIRST ***
                        logger_instance.info(f"[OANDA CLOSE] Executing OANDA position close for {standardized}")
                        
                        # Import and call the OANDA close position function
                        from main import _close_position
                        oanda_close_result = await _close_position(standardized)
                        
                        # Check if OANDA close was successful
                        if oanda_close_result.get("status") == "error":
                            logger_instance.error(f"[OANDA CLOSE FAILED] {oanda_close_result.get('message')}")
                            raise Exception(f"OANDA close failed: {oanda_close_result.get('message')}")
                        
                        logger_instance.info(f"[OANDA CLOSE SUCCESS] Position closed on OANDA: {oanda_close_result}")
                        
                        # Now update internal tracking ONLY after successful OANDA close
                        result = await self.position_tracker.close_position(
                            position_to_close["position_id"], 
                            exit_price, 
                            reason="pine_script_exit_signal"
                        )
                        
                        # Clear from risk manager
                        if self.risk_manager and result and hasattr(result, 'success') and result.success:
                            try:
                                await self.risk_manager.clear_position(position_to_close["position_id"])
                                logger_instance.info(f"[EXIT] Position cleared from risk manager")
                            except Exception as e:
                                logger_instance.error(f"Failed to clear position from risk manager: {str(e)}")
                        
                        logger_instance.info(f"[EXIT SUCCESS] Position {position_to_close['position_id']} closed successfully using {close_method}")
                        
                        # Record successful exit in monitor
                        if signal_id:
                            processing_time = time.time() - start_time
                            await exit_monitor.record_exit_success(signal_id, processing_time)
                        
                        return {
                            "status": "success",
                            "message": "Position closed successfully",
                            "alert_id": alert_id,
                            "symbol": standardized,
                            "position_id": position_to_close["position_id"],
                            "exit_price": exit_price,
                            "close_method": close_method
                        }
                        
                    except Exception as e:
                        logger_instance.error(f"[EXIT ERROR] Failed to close position: {str(e)}")
                        
                        # Record failed exit in monitor
                        if signal_id:
                            processing_time = time.time() - start_time
                            await exit_monitor.record_exit_failure(signal_id, str(e), processing_time)
                        
                        return {
                            "status": "error",
                            "message": f"Failed to close position: {str(e)}",
                            "alert_id": alert_id
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
                # Check correlation limits before executing trade
                if self.risk_manager and getattr(config, 'enable_correlation_limits', False):
                    risk_allowed, risk_reason = await self.risk_manager.is_trade_allowed(
                        risk_percent / 100.0, 
                        standardized_instrument, 
                        direction
                    )
                    
                    if not risk_allowed:
                        logger_instance.warning(f"[ID: {alert_id}] Trade rejected due to risk limits: {risk_reason}")
                        return {
                            "status": "rejected",
                            "message": f"Risk limit violation: {risk_reason}",
                            "alert_id": alert_id,
                            "symbol": standardized_instrument,
                            "action": direction,
                            "risk_percent": risk_percent
                        }
                
                # Resolve template variables in position_id
                raw_position_id = alert_data.get("position_id", alert_id)
                resolved_position_id = self._resolve_template_variables(raw_position_id, alert_data)
                
                # *** ENHANCED DEBUGGING: Log position ID resolution details ***
                logger_instance.info(f"[OPEN] Position ID resolution details:")
                logger_instance.info(f"  - alert_id: {alert_id}")
                logger_instance.info(f"  - raw_position_id from alert: {raw_position_id}")
                logger_instance.info(f"  - resolved_position_id: {resolved_position_id}")
                logger_instance.info(f"  - Alert data position ID fields:")
                logger_instance.info(f"    - alert_data.get('position_id'): {alert_data.get('position_id')}")
                logger_instance.info(f"    - alert_data.get('alert_id'): {alert_data.get('alert_id')}")
                logger_instance.info(f"    - alert_data.get('request_id'): {alert_data.get('request_id')}")
                logger_instance.info(f"    - alert_data.get('id'): {alert_data.get('id')}")
                
                if raw_position_id != resolved_position_id:
                    logger_instance.info(f"[ID: {alert_id}] Position ID template resolution: '{raw_position_id}' → '{resolved_position_id}'")
                
                payload_for_execute_trade = {
                    "symbol": standardized_instrument,
                    "action": direction, # Should be "BUY" or "SELL" at this point
                    "risk_percent": risk_percent,
                    "timeframe": timeframe,
                    "comment": comment,
                    "account": account,
                    "request_id": resolved_position_id  # Use resolved position_id
                }
                
                # *** CRITICAL FIX: Preserve multi-account data ***
                # If original alert contains multiple accounts, pass them through
                if "accounts" in alert_data and isinstance(alert_data["accounts"], list):
                    payload_for_execute_trade["accounts"] = alert_data["accounts"]
                    logger_instance.info(f"[MULTI-ACCOUNT] Detected accounts array: {alert_data['accounts']}")
                elif alert_data.get("account") == "BROADCAST":
                    payload_for_execute_trade["account"] = "BROADCAST"
                    logger_instance.info(f"[MULTI-ACCOUNT] Detected BROADCAST mode")
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
        """Handle recurring tasks every 5 minutes"""
        while self._running:
            try:
                # Update position prices
                await self._update_position_prices()
                
                # Update weekend status for all positions
                if self.position_tracker:
                    await self.position_tracker.update_weekend_status_for_all_positions()
                
                # Sync with database
                await self._sync_database()
                
                # Daily reset (checks internally if it's time)
                await self._perform_daily_reset()
                
                # Weekly cleanup (checks internally if it's time)
                await self._cleanup_old_positions()
                
            except Exception as e:
                logger.error(f"Error in scheduled tasks: {e}")
            
            await asyncio.sleep(300)  # 5 minutes

    async def stop(self):
        """Stop all components and background tasks"""
        if not self._running:
            return
        
        logger.info("Stopping EnhancedAlertHandler...")
        self._running = False
        
        # Cancel background tasks first
        if self.task_handler:
            self.task_handler.cancel()
            try:
                await self.task_handler
            except asyncio.CancelledError:
                pass
        
        # Stop health checker
        if hasattr(self, 'health_checker') and self.health_checker:
            try:
                await self.health_checker.stop()
                logger.info("HealthChecker stopped")
            except Exception as e:
                logger.error(f"Error stopping HealthChecker: {e}")
        
        # Stop position tracker
        if self.position_tracker:
            try:
                await self.position_tracker.stop()
                logger.info("PositionTracker stopped")
            except Exception as e:
                logger.error(f"Error stopping PositionTracker: {e}")
        
        logger.info("EnhancedAlertHandler stopped successfully")

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
                # Use rsplit to handle symbols with underscores (e.g., EUR_USD_BUY -> EUR_USD, BUY)
                parts = pos_key.rsplit('_', 1)
                if len(parts) == 2:
                    symbol, action = parts
                    await self._close_stale_position(symbol, action)
                else:
                    logger.warning(f"Invalid position key format: {pos_key}")
            
            logger.info(f"Reconciliation complete: {len(stale_positions)} stale positions closed")
        
        except Exception as e:
            logger.warning(f"Position reconciliation failed: {e}")

    async def _close_stale_position(self, symbol, action):
        # Placeholder for closing a stale position in the database
        logger.info(f"Closing stale position: {symbol} {action}")
        # Implement actual close logic as needed
