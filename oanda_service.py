#
# file: oanda_service.py
#
import oandapyV20
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.instruments import InstrumentsCandles
from oandapyV20.endpoints.trades import TradeCRCDO
from oandapyV20.exceptions import V20Error
from pydantic import SecretStr
import asyncio
import logging
import random
import time
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from config import config
from utils import _get_simulated_price, get_atr, get_instrument_leverage, round_position_size, get_position_size_limits, MarketDataUnavailableError, calculate_position_size, round_price_for_instrument
from risk_manager import EnhancedRiskManager
from typing import Dict, Any
import numpy as np
from backoff import ConnectionState

class RateLimitFilter(logging.Filter):
    """Filter to rate-limit repetitive log messages"""
    def __init__(self, min_interval=60):
        self.last = 0
        self.min = min_interval
    
    def filter(self, record):
        import time
        now = time.time()
        if now - self.last >= self.min:
            self.last = now
            return True
        return False

logger = logging.getLogger("OandaService")
# Add rate limiting to reduce log spam during maintenance
logger.addFilter(RateLimitFilter(30))  # Limit to once every 30 seconds

class OandaService:
    def __init__(self, config_obj=None, success_probe_seconds: int = 600):
        self.config = config_obj or config
        self.oanda = None
        self.last_successful_request = None
        self.connection_errors_count = 0
        self.last_health_check = None
        self.health_check_interval = 1800  # INCREASED: 30 minutes instead of 15 (reduce connection churn)
        self.circuit_breaker_failures = 0
        self.circuit_breaker_threshold = 5  # REDUCED: More sensitive circuit breaker
        self.circuit_breaker_reset_time = None
        self.session_created_at = None
        
        # INSTITUTIONAL FIX: Enhanced connection management
        self.consecutive_timeouts = 0
        self.max_consecutive_timeouts = 3
        self.last_timeout_reset = datetime.now()
        self.connection_health_score = 100  # 0-100 scale
        self.min_health_threshold = 20  # Lower threshold to prevent unnecessary disconnections
        
        # INSTITUTIONAL FIX: Rate limiting for API protection
        self.request_timestamps = []
        self.max_requests_per_minute = 30  # Conservative limit
        self.last_rate_limit_reset = datetime.now()
        
        # NEW: State management with backoff
        self.connection_state = ConnectionState(success_probe_seconds)
        
        # Initialize OANDA client asynchronously
        self.oanda = None  # Will be set in initialize()

    async def _init_oanda_client(self):
        """Async OANDA client initialization with proper error handling"""
        try:
            access_token = self.config.oanda_access_token
            if isinstance(access_token, object) and hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            
            # Create OANDA client with async-compatible settings and connection persistence
            self.oanda = oandapyV20.API(
                access_token=access_token,
                environment=self.config.oanda_environment
            )
            
            # Configure connection persistence
            if hasattr(self.oanda, 'session'):
                self.oanda.session.headers.update({
                    'Connection': 'keep-alive',
                    'Keep-Alive': 'timeout=60, max=1000'
                })
            
            self.session_created_at = datetime.now()
            self.connection_errors_count = 0
            self.circuit_breaker_failures = 0
            self.circuit_breaker_reset_time = None
            
            # Test the connection immediately
            if not await self._test_connection():
                raise Exception("OANDA connection test failed")
            
            logger.info(f"✅ OANDA client initialized successfully for {self.config.oanda_environment} environment")
        except Exception as e:
            status = self._status_from_error(e)
            if status == 503:
                logger.warning(f"⚠️ OANDA maintenance mode during init: {e}")
            else:
                logger.error(f"❌ Failed to initialize OANDA client: {e}")
            self.oanda = None
            raise

    def _status_from_error(self, err: Exception) -> int | None:
        """Extract HTTP status code from various error types."""
        if isinstance(err, V20Error):
            # oandapyV20 attaches response in some cases
            resp = getattr(err, "response", None)
            if resp is not None and hasattr(resp, "status_code"):
                return resp.status_code
            # some versions use 'code'
            code = getattr(err, "code", None)
            if isinstance(code, int):
                return code
        if isinstance(err, requests.exceptions.RequestException):
            resp = getattr(err, "response", None)
            if resp is not None and hasattr(resp, "status_code"):
                return resp.status_code
        return None

    async def _test_connection(self):
        """Test the OANDA connection with a simple API call"""
        try:
            if not self.oanda:
                raise Exception("OANDA client not initialized")
            
            # Simple connection test - get account details
            account_request = AccountDetails(accountID=self.config.oanda_account_id)
            
            # Run synchronous OANDA request in thread pool to avoid blocking with timeout
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(None, self.oanda.request, account_request),
                timeout=30.0  # 30 second timeout
            )
            
            if not response:
                raise Exception("No response from OANDA API")
                
            logger.info(f"✅ OANDA connection test successful")
            return True
            
        except Exception as e:
            status = self._status_from_error(e)
            if status == 503:
                logger.warning(f"⚠️ OANDA maintenance mode: {e}")
            else:
                logger.error(f"❌ OANDA connection test failed: {e}")
            # Don't raise here - let the caller handle it
            return False

    async def _health_check(self) -> bool:
        """NEW: State-aware health check with maintenance handling."""
        # Skip if we shouldn't probe yet
        if not self.connection_state._should_probe():
            return self.connection_state.state == "OK"
        
        # Initialize client if needed
        if not self.oanda:
            await self._init_oanda_client()
            if not self.oanda:
                return self.connection_state.handle_other_error(Exception("OANDA client not initialized"))
        
        try:
            # Lightweight account details request
            account_request = AccountDetails(accountID=self.config.oanda_account_id)
            start_time = time.time()
            
            # Run synchronous OANDA request in thread pool
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(None, self.oanda.request, account_request),
                timeout=30.0
            )
            
            response_time = time.time() - start_time
            self.last_health_check = datetime.now()
            self.last_successful_request = datetime.now()
            self.consecutive_timeouts = 0
            
            # Update health score based on response time
            if response_time < 2.0:
                self._update_health_score(+10)
            elif response_time < 5.0:
                self._update_health_score(+5)
            else:
                self._update_health_score(-5)
            
            logger.debug(f"Health check passed in {response_time:.3f}s (health score: {self.connection_health_score})")
            return self.connection_state.handle_success()
            
        except Exception as e:
            status = self._status_from_error(e)
            
            # Handle 503 maintenance specifically
            if status == 503:
                return self.connection_state.handle_503_maintenance()
            
            # Handle other errors
            return self.connection_state.handle_other_error(e)

    def is_healthy(self) -> bool:
        """Check if the OANDA service is healthy"""
        return (self.oanda is not None and 
                self.connection_health_score >= self.min_health_threshold and
                not self._should_circuit_break())

    def can_trade(self) -> bool:
        """Check if trading is allowed in current state."""
        return self.connection_state.can_trade()

    async def _ensure_connection(self):
        """Ensure we have a working connection before making requests"""
        max_connection_attempts = 3
        
        for attempt in range(max_connection_attempts):
            try:
                if not self.oanda:
                    await self._init_oanda_client()
                    if not self.oanda:
                        raise Exception("OANDA client not initialized")
                
                # Check if connection is healthy
                if await self._health_check():
                    logger.debug(f"✅ OANDA connection healthy (attempt {attempt + 1})")
                    return  # Connection is healthy, proceed
                
                logger.warning(f"⚠️ OANDA connection unhealthy (attempt {attempt + 1}/{max_connection_attempts})")
                
                # Try to reinitialize if health check fails
                if attempt < max_connection_attempts - 1:
                    await self._reinitialize_client()
                    await asyncio.sleep(2)  # Brief pause before retry
                else:
                    raise Exception("Failed to establish healthy OANDA connection after all attempts")
                    
            except Exception as e:
                if attempt == max_connection_attempts - 1:
                    raise Exception(f"Failed to establish healthy OANDA connection: {e}")
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}, retrying...")
                await asyncio.sleep(2)

    def _should_circuit_break(self) -> bool:
        """Check if circuit breaker should be activated"""
        if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
            if not self.circuit_breaker_reset_time:
                self.circuit_breaker_reset_time = datetime.now() + timedelta(minutes=5)
                logger.error(f"Circuit breaker activated due to {self.circuit_breaker_failures} consecutive failures")
                return True
            elif datetime.now() < self.circuit_breaker_reset_time:
                return True
            else:
                # Reset circuit breaker
                logger.info("Circuit breaker reset - attempting to resume operations")
                self.circuit_breaker_failures = 0
                self.circuit_breaker_reset_time = None
                return False
        return False

    async def _warm_connection(self):
        """Warm up the connection with multiple test requests"""
        try:
            logger.info("🔥 Warming up OANDA connection...")
            
            # Make multiple test requests to establish connection stability
            for i in range(3):
                try:
                    if await self._health_check():
                        logger.debug(f"Connection warm-up test {i+1}/3 successful")
                        await asyncio.sleep(1)  # Brief pause between tests
                    else:
                        logger.warning(f"Connection warm-up test {i+1}/3 failed")
                        return False
                except Exception as e:
                    logger.warning(f"Connection warm-up test {i+1}/3 failed: {e}")
                    return False
            
            logger.info("✅ OANDA connection warmed up successfully")
            return True
            
        except Exception as e:
            logger.warning(f"❌ Connection warming failed: {e}")
            return False

    async def _reinitialize_client(self):
        """Reinitialize the OANDA client with exponential backoff"""
        try:
            logger.info("🔄 Reinitializing OANDA client...")
            await self._init_oanda_client()
            
            # Test the new connection
            if await self._health_check():
                logger.info("✅ OANDA client reinitialized successfully")
                return True
            else:
                logger.warning("⚠️ OANDA client reinitialized but health check failed")
                return False
        except Exception as e:
            logger.error(f"❌ Failed to reinitialize OANDA client: {e}")
            return False

    async def _handle_connection_error(self, error: Exception, attempt: int, max_retries: int):
        """ENHANCED: Handle connection errors with intelligent recovery strategies"""
        error_str = str(error).lower()
        
        # INSTITUTIONAL FIX: More granular error categorization
        if 'remote end closed connection' in error_str:
            severity = 'critical'
            self._update_health_score(-30)
        elif 'connection aborted' in error_str:
            severity = 'high'
            self._update_health_score(-25)
        elif 'timeout' in error_str:
            severity = 'medium'
            self._update_health_score(-15)
            self.consecutive_timeouts += 1
        else:
            severity = 'low'
            self._update_health_score(-10)
        
        # Update failure counters
        self.connection_errors_count += 1
        self.circuit_breaker_failures += 1
        
        # Log the error with context and health score
        logger.warning(f"OANDA connection error (severity: {severity}) attempt {attempt + 1}/{max_retries}: {error} (health: {self.connection_health_score}%)")
        
        # INSTITUTIONAL FIX: More aggressive reinit for severe errors
        if severity in ['critical', 'high'] or attempt >= 1 or self.connection_health_score < 30:
            logger.info(f"Forcing client reinitialization due to {severity} error or low health score")
            await self._reinitialize_client()
        
        # Force circuit breaker if too many consecutive severe errors
        if severity == 'critical' and self.circuit_breaker_failures >= 3:
            logger.error("Multiple critical connection errors, activating circuit breaker")
            if not self.circuit_breaker_reset_time:
                self.circuit_breaker_reset_time = datetime.now() + timedelta(minutes=10)  # Longer circuit breaker

    async def initialize(self):
        """Initialize the OandaService."""
        try:
            # Initialize OANDA client
            await self._init_oanda_client()
            
            # Warm up the connection
            await self._warm_connection()
            
            logger.info("✅ OANDA service initialized successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to initialize OANDA service: {e}")
            raise

    async def start_connection_monitor(self):
        """Start background connection monitoring"""
        logger.info("Starting OANDA connection monitor...")
        asyncio.create_task(self._connection_monitor_loop())

    async def _connection_monitor_loop(self):
        """NEW: State-aware connection monitoring with backoff."""
        while True:
            try:
                # Use state-aware health check
                if not await self._health_check():
                    # Only attempt reinit for non-maintenance states
                    if self.connection_state.state != "MAINTENANCE":
                        logger.warning(f"Connection health check failed (state: {self.connection_state.state}), attempting recovery...")
                        await self._reinitialize_client()
                
                # Adaptive sleep based on state
                if self.connection_state.state == "OK":
                    await asyncio.sleep(300)  # 5 minutes for healthy connections
                elif self.connection_state.state == "DEGRADED":
                    await asyncio.sleep(120)  # 2 minutes for degraded connections
                else:  # MAINTENANCE
                    await asyncio.sleep(60)   # 1 minute for maintenance (will be overridden by backoff)
                    
            except Exception as e:
                logger.error(f"Error in connection monitor: {e}")
                self._update_health_score(-10)
                await asyncio.sleep(30)  # Wait before retrying

    async def get_connection_status(self) -> Dict[str, Any]:
        """ENHANCED: Get comprehensive connection status and health metrics"""
        state_info = self.connection_state.get_status()
        
        return {
            "connected": self.oanda is not None,
            "health_score": self.connection_health_score,
            "health_status": "excellent" if self.connection_health_score >= 90 else 
                          "good" if self.connection_health_score >= 70 else
                          "degraded" if self.connection_health_score >= 50 else
                          "poor" if self.connection_health_score >= 30 else "critical",
            "last_successful_request": self.last_successful_request.isoformat() if self.last_successful_request else None,
            "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None,
            "connection_errors_count": self.connection_errors_count,
            "consecutive_timeouts": self.consecutive_timeouts,
            "circuit_breaker_failures": self.circuit_breaker_failures,
            "circuit_breaker_active": self._should_circuit_break(),
            "requests_last_minute": len(self.request_timestamps),
            "rate_limit_status": "ok" if len(self.request_timestamps) < self.max_requests_per_minute * 0.8 else "warning",
            "session_age_minutes": (datetime.now() - self.session_created_at).total_seconds() / 60 if self.session_created_at else None,
            "health_check_interval_minutes": self.health_check_interval / 60,
            "monitoring_status": "adaptive" if hasattr(self, 'connection_health_score') else "basic",
            # NEW: State management info
            "connection_state": state_info["state"],
            "can_trade": self.can_trade(),
            "next_probe_at": state_info["next_probe_at"],
            "consecutive_503s": state_info["consecutive_503s"],
            "backoff_current": state_info["backoff_current"],
            "backoff_reset_age_seconds": state_info["backoff_reset_age_seconds"]
        }

    async def stop(self):
        """Stop the OandaService."""
        logger.info("OANDA service is shutting down.")

    async def robust_oanda_request(self, request, max_retries: int = 5, initial_delay: float = 3.0):
        """Enhanced OANDA API request with sophisticated retry logic"""
        
        # Check circuit breaker
        if self._should_circuit_break():
            raise Exception("Circuit breaker is active - too many consecutive failures")
        
        # Ensure we have a healthy connection with enhanced retry logic
        await self._ensure_connection()
        
        # Additional connection health check before proceeding
        if not await self._health_check():
            logger.warning("⚠️ Connection health check failed before request, attempting recovery...")
            await self._reinitialize_client()
            if not await self._health_check():
                raise Exception("Failed to establish healthy OANDA connection for request")
        
        def is_connection_error(exception):
            """Check if the exception is a connection-related error"""
            from urllib3.exceptions import ProtocolError
            from http.client import RemoteDisconnected
            from requests.exceptions import ConnectionError, Timeout, ReadTimeout
            import socket
            
            connection_errors = (
                ConnectionError,
                RemoteDisconnected,
                ProtocolError,
                Timeout,
                ReadTimeout,
                socket.timeout,
                socket.error,
                OSError
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
                'connection timed out',
                'max retries exceeded',
                'read timed out',
                'ssl',
                'certificate'
            ]
            
            return any(indicator in error_str for indicator in connection_indicators)
        
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # Warm connection on first attempt if it's been a while
                if attempt == 0 and (not self.last_successful_request or 
                                   datetime.now() - self.last_successful_request > timedelta(minutes=10)):
                    await self._warm_connection()
                
                logger.debug(f"OANDA request attempt {attempt + 1}/{max_retries}")
                
                # Run synchronous OANDA request in thread pool to avoid blocking with timeout
                loop = asyncio.get_event_loop()
                response = await asyncio.wait_for(
                    loop.run_in_executor(None, self.oanda.request, request),
                    timeout=45.0  # 45 second timeout for general requests
                )
                
                # Success - reset failure counters
                self.last_successful_request = datetime.now()
                self.connection_errors_count = 0
                self.circuit_breaker_failures = 0
                return response
                
            except Exception as e:
                last_exception = e
                is_conn_error = is_connection_error(e)
                is_final_attempt = attempt == max_retries - 1
                
                if is_final_attempt:
                    logger.error(f"OANDA request failed after {max_retries} attempts: {e}")
                    raise Exception(f"OANDA request failed after {max_retries} attempts: {e}")
                
                # INSTITUTIONAL FIX: Use intelligent backoff for all errors
                if is_conn_error:
                    await self._handle_connection_error(e, attempt, max_retries)
                    
                # Apply intelligent backoff based on error type and connection health
                final_delay = await self._apply_intelligent_backoff(attempt, e)
                
                if is_conn_error:
                    logger.info(f"Connection error, retrying in {final_delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                else:
                    logger.warning(f"OANDA request error, retrying in {final_delay:.1f}s (attempt {attempt + 1}/{max_retries}): {e}")
                
                await asyncio.sleep(final_delay)

    async def get_current_price(self, symbol: str, action: str) -> float:
        try:
            pricing_request = PricingInfo(
                accountID=self.config.oanda_account_id,
                params={"instruments": symbol}
            )
            response = await self.robust_oanda_request(pricing_request)
            
            if 'prices' in response and response['prices']:
                price_data = response['prices'][0]

                if not price_data.get('tradeable', False):
                    reason = price_data.get('reason', 'Unknown')
                    logger.warning(f"Price for {symbol} is not tradeable at the moment. Reason: {reason}")
                    logger.debug(f"Non-tradeable price data: {price_data}")
                    
                    # Immediately raise exception for non-tradeable prices to prevent processing invalid data
                    if reason and 'MARKET_HALTED' in reason:
                        raise MarketDataUnavailableError(f"Market for {symbol} is currently halted.")
                    else:
                        raise MarketDataUnavailableError(f"Price for {symbol} is not tradeable: {reason}")

                # Only process price data if it's tradeable
                price = None
                if action.upper() == "BUY":
                    price = float(price_data.get('ask') or price_data.get('closeoutAsk', 0))
                else: # SELL or CLOSE
                    price = float(price_data.get('bid') or price_data.get('closeoutBid', 0))

                if price and price > 0:
                    logger.info(f"✅ Live price for {symbol} {action}: {price}")
                    return price
                
                logger.error(f"Invalid or zero price data received for {symbol}: {price_data}")
                raise MarketDataUnavailableError(f"Invalid price data received for {symbol}")
            
            else:
                logger.error(f"No price data in OANDA response for {symbol}: {response}")
                raise MarketDataUnavailableError(f"No price data in OANDA response for {symbol}")

        except Exception as e:
            logger.error(f"Failed to get current price for {symbol} after all retries: {e}")
            raise MarketDataUnavailableError(f"Market data for {symbol} is unavailable: {e}")

    async def get_account_balance(self, use_fallback: bool = False) -> float:
        if use_fallback:
            logger.warning("Using fallback account balance. This should only be for testing.")
            return 10000.0
        
        # Try multiple times with progressive fallback
        for attempt in range(3):
            try:
                account_request = AccountDetails(accountID=self.config.oanda_account_id)
                response = await self.robust_oanda_request(account_request)
                balance = float(response['account']['balance'])
                logger.info(f"Successfully fetched account balance: ${balance:.2f}")
                return balance
            except Exception as e:
                logger.warning(f"Account balance attempt {attempt + 1}/3 failed: {e}")
                if attempt < 2:  # Not the last attempt
                    await asyncio.sleep(2 ** attempt)  # Progressive backoff
                    continue
                
                # Final attempt failed - use conservative fallback
                logger.error(f"All account balance attempts failed. Using fallback for trading continuity.")
                fallback_balance = 100000.0  # Conservative fallback for practice account
                logger.warning(f"Using fallback balance: ${fallback_balance:.2f}")
                return fallback_balance

    async def get_account_info(self) -> Dict[str, Any]:
        """
        Get comprehensive account information including balance, margin, and leverage.
        
        Returns:
            Dict containing account details including:
            - balance: Account balance
            - margin_used: Currently used margin
            - margin_available: Available margin
            - margin_rate: Margin rate (inverse of leverage)
            - leverage: Calculated leverage ratio
            - currency: Account currency
            - account_id: OANDA account ID
        """
        try:
            account_request = AccountDetails(accountID=self.config.oanda_account_id)
            response = await self.robust_oanda_request(account_request)
            
            account = response.get('account', {})
            
            # Extract basic account information
            balance = float(account.get('balance', 0))
            margin_used = float(account.get('marginUsed', 0))
            margin_available = float(account.get('marginAvailable', 0))
            margin_rate = float(account.get('marginRate', 0))
            currency = account.get('currency', 'USD')
            account_id = account.get('id', self.config.oanda_account_id)
            
            # Calculate actual leverage from margin rate
            # margin_rate is typically 0.05 for 20:1 leverage, 0.02 for 50:1 leverage
            actual_leverage = 1.0 / margin_rate if margin_rate > 0 else 50.0  # Default fallback
            
            # Validate leverage is within reasonable bounds
            if actual_leverage > 100:
                logger.warning(f"Calculated leverage {actual_leverage:.1f}:1 seems unusually high, using fallback")
                actual_leverage = 50.0
            elif actual_leverage < 1:
                logger.warning(f"Calculated leverage {actual_leverage:.1f}:1 seems unusually low, using fallback")
                actual_leverage = 50.0
            
            account_info = {
                'balance': balance,
                'margin_used': margin_used,
                'margin_available': margin_available,
                'margin_rate': margin_rate,
                'leverage': actual_leverage,
                'currency': currency,
                'account_id': account_id,
                'margin_utilization_pct': (margin_used / balance * 100) if balance > 0 else 0
            }
            
            logger.info(f"✅ Account info retrieved: Balance=${balance:.2f}, Leverage={actual_leverage:.1f}:1, Margin Rate={margin_rate:.4f}")
            return account_info
            
        except Exception as e:
            logger.error(f"Failed to get account info: {e}")
            # Return fallback values
            return {
                'balance': 10000.0,
                'margin_used': 0.0,
                'margin_available': 10000.0,
                'margin_rate': 0.02,  # 50:1 leverage fallback
                'leverage': 50.0,
                'currency': 'USD',
                'account_id': self.config.oanda_account_id,
                'margin_utilization_pct': 0.0
            }

    async def debug_crypto_availability(self) -> Dict[str, Any]:
        """Debug crypto availability with detailed logging"""
        try:
            from oandapyV20.endpoints.accounts import AccountInstruments
            
            request = AccountInstruments(accountID=self.config.oanda_account_id)
            response = await self.robust_oanda_request(request)
            
            available_instruments = []
            if response and 'instruments' in response:
                available_instruments = [inst['name'] for inst in response['instruments']]
            
            # Find all crypto-like instruments
            crypto_instruments = [inst for inst in available_instruments if any(
                crypto in inst.upper() for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'ADA', 'DOT', 'SOL']
            )]
            
            # Find all USD pairs
            usd_instruments = [inst for inst in available_instruments if 'USD' in inst]
            
            debug_info = {
                "environment": self.config.oanda_environment,
                "total_instruments": len(available_instruments),
                "crypto_instruments": crypto_instruments,
                "usd_pairs_sample": usd_instruments[:20],  # First 20 USD pairs
                "btc_variants": [inst for inst in available_instruments if 'BTC' in inst.upper()],
                "eth_variants": [inst for inst in available_instruments if 'ETH' in inst.upper()]
            }
            
            logger.info(f"CRYPTO DEBUG: {debug_info}")
            return debug_info
            
        except Exception as e:
            logger.error(f"Crypto debug failed: {e}")
            return {"error": str(e)}    

    async def check_crypto_availability(self) -> Dict[str, bool]:
        """Check which crypto symbols are available for trading"""
        try:
            from oandapyV20.endpoints.accounts import AccountInstruments
            
            request = AccountInstruments(accountID=self.config.oanda_account_id)
            response = await self.robust_oanda_request(request)
            
            available_instruments = []
            if response and 'instruments' in response:
                available_instruments = [inst['name'] for inst in response['instruments']]
            
            # Based on your transaction history showing successful BTC/USD and ETH/USD trades,
            # we know these are supported. Let's check what's actually available.
            crypto_availability = {}
            
            # Check for common crypto symbols in different formats
            crypto_symbols_to_check = [
                'BTC_USD', 'BTC/USD', 'BTCUSD',
                'ETH_USD', 'ETH/USD', 'ETHUSD',
                'LTC_USD', 'LTC/USD', 'LTCUSD',
                'XRP_USD', 'XRP/USD', 'XRPUSD',
                'BCH_USD', 'BCH/USD', 'BCHUSD',
                'ADA_USD', 'ADA/USD', 'ADAUSD',
                'DOT_USD', 'DOT/USD', 'DOTUSD',
                'SOL_USD', 'SOL/USD', 'SOLUSD'
            ]
            
            for symbol in crypto_symbols_to_check:
                crypto_availability[symbol] = symbol in available_instruments
            
            # Log the results
            supported_cryptos = [sym for sym, available in crypto_availability.items() if available]
            logger.info(f"✅ Supported crypto symbols: {supported_cryptos}")
            
            return crypto_availability
            
        except Exception as e:
            logger.error(f"Failed to check crypto availability: {e}")
            # Fallback: Based on your transaction history, we know BTC/USD and ETH/USD work
            return {
                'BTC_USD': True,
                'BTC/USD': True,
                'ETH_USD': True,
                'ETH/USD': True,
                'LTC_USD': False,
                'XRP_USD': False,
                'BCH_USD': False,
                'ADA_USD': False,
                'DOT_USD': False,
                'SOL_USD': False
            }

    async def is_crypto_supported(self, symbol: str) -> bool:
        """Check if a specific crypto symbol is supported"""
        crypto_availability = await self.check_crypto_availability()
        return crypto_availability.get(symbol, False)

    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        symbol = payload.get("symbol")
        action = payload.get("action")
        units = payload.get("units")
        stop_loss = payload.get("stop_loss")
        take_profit = payload.get("take_profit")
        
        if not symbol or not action or not units:
            logger.error(f"Missing required trade parameters: symbol={symbol}, action={action}, units={units}")
            return False, {"error": "Missing required trade parameters"}

        # INSTITUTIONAL FIX: Enhanced crypto handling with fallback options
        crypto_symbols = ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'ADA', 'DOT', 'SOL']
        is_crypto_signal = any(crypto in symbol.upper() for crypto in crypto_symbols)
        
        if is_crypto_signal:
            logger.info(f"Crypto signal detected for {symbol}")
            
            # Try to format symbol properly for OANDA
            from utils import format_crypto_symbol_for_oanda
            formatted_symbol = format_crypto_symbol_for_oanda(symbol)
            
            logger.info(f"Formatted crypto symbol: {symbol} -> {formatted_symbol}")
            symbol = formatted_symbol  # Update symbol for the rest of the function
            
            # INSTITUTIONAL FIX: Based on transaction history, BTC/USD and ETH/USD are supported
            # So we'll proceed with the trade even if initial price check fails
            try:
                # Test if we can get price data (this indicates crypto is supported)
                test_price = await self.get_current_price(symbol, action)
                if test_price is None:
                    raise Exception("Price data unavailable")
                logger.info(f"✅ Crypto {symbol} is tradeable - price: {test_price}")
            except Exception as crypto_error:
                logger.warning(f"Crypto symbol {symbol} initial price check failed: {crypto_error}")
                logger.info(f"⚠️ Proceeding with crypto trade anyway - transaction history shows {symbol} is supported")
                
                # Don't reject the trade, just log the warning and continue
                # The actual price check will happen later in the function

        # Get current price for the trade
        try:
            current_price = await self.get_current_price(symbol, action)
        except Exception as e:
            logger.error(f"Failed to get current price for {symbol}: {e}")
            return False, {"error": f"Failed to get current price: {e}"}

        # SIMPLE APPROACH - NO COMPLEX VALIDATION (like working past version)
        # Only basic price rounding for OANDA precision
        if stop_loss is not None:
            stop_loss = round_price_for_instrument(stop_loss, symbol)
        if take_profit is not None:
            take_profit = round_price_for_instrument(take_profit, symbol)

        logger.info(f"Order submission: {action} {symbol} entry={current_price} TP={take_profit} SL={stop_loss}")

        # Prepare trade data
        data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(units),
                "timeInForce": "FOK"
            }
        }

        # Add stop loss if provided
        if stop_loss is not None:
            stop_loss = round_price_for_instrument(stop_loss, symbol)
            data["order"]["stopLossOnFill"] = {
                "timeInForce": "GTC",
                "price": str(stop_loss)
            }

        # Add take profit if provided
        if take_profit is not None:
            take_profit = round_price_for_instrument(take_profit, symbol)
            data["order"]["takeProfitOnFill"] = {
                "timeInForce": "GTC",
                "price": str(take_profit)
            }

        # === AUTO-RETRY WITH SIZE REDUCTION ON INSUFFICIENT_LIQUIDITY ===
        max_retries = 3
        attempt = 0
        min_units = 1 if is_crypto_signal else 1000
        last_error = None
        current_units = float(units)
        while attempt < max_retries:
            # Update units in data for each attempt
            # FIX: Don't convert crypto units to int (they can be fractional)
            if is_crypto_signal:
                data["order"]["units"] = str(current_units)  # Keep fractional units for crypto
            else:
                data["order"]["units"] = str(int(current_units))  # Integer units for forex
            try:
                from oandapyV20.endpoints.orders import OrderCreate
                request = OrderCreate(self.config.oanda_account_id, data=data)
                response = await self.robust_oanda_request(request)
                
                if response and 'orderFillTransaction' in response:
                    fill_transaction = response['orderFillTransaction']
                    transaction_id = fill_transaction.get('id')
                    fill_price = float(fill_transaction.get('price', current_price))
                    actual_units = int(fill_transaction.get('units', current_units))
                    
                    logger.info(f"✅ Trade executed successfully: {symbol} {action} {actual_units} units at {fill_price}")
                    
                    return True, {
                        "transaction_id": transaction_id,
                        "fill_price": fill_price,
                        "units": actual_units,
                        "symbol": symbol,
                        "action": action
                    }
                elif response and 'orderCancelTransaction' in response:
                    cancel_reason = response['orderCancelTransaction'].get('reason', 'Unknown')
                    logger.error(f"Order was cancelled: {cancel_reason} (attempt {attempt+1}/{max_retries}, units={int(current_units)})")
                    last_error = cancel_reason
                    if cancel_reason == 'INSUFFICIENT_LIQUIDITY':
                        # Reduce size and retry
                        current_units = current_units / 2
                        if current_units < min_units:
                            logger.error(f"Order size reduced below minimum ({min_units}). Aborting retries.")
                            return False, {"error": f"Order cancelled: {cancel_reason} (final size below minimum {min_units})"}
                        attempt += 1
                        continue
                    else:
                        return False, {"error": f"Order cancelled: {cancel_reason}"}
                else:
                    logger.error(f"Unexpected response format from OANDA: {response}")
                    return False, {"error": "Unexpected response format from OANDA"}
            except Exception as e:
                logger.error(f"Failed to execute trade for {symbol}: {e}")
                return False, {"error": f"Trade execution failed: {e}"}
        # If we exit the loop, all retries failed
        return False, {"error": f"Order cancelled after {max_retries} attempts: {last_error}"}
    
    async def get_historical_data(self, symbol: str, count: int, granularity: str):
        """Fetch historical candle data from OANDA for technical analysis."""
        try:
            logger.info(f"Fetching {count} candles for {symbol} at {granularity} granularity")
            
            # Prepare request parameters
            params = {
                "granularity": granularity,
                "count": count,
                "price": "M"  # Mid prices for analysis
            }
            
            # Create candles request
            candles_request = InstrumentsCandles(instrument=symbol, params=params)
            
            # Execute request with retry logic
            response = await self.robust_oanda_request(candles_request)
            
            if not response or 'candles' not in response:
                logger.error(f"No candles data received for {symbol}")
                return None
                
            candles = response['candles']
            if not candles:
                logger.error(f"Empty candles list received for {symbol}")
                return None
                
            # Convert to DataFrame format expected by technical analysis
            data = []
            for candle in candles:
                if candle.get('complete', False):  # Only use complete candles
                    mid = candle.get('mid', {})
                    data.append({
                        'timestamp': candle.get('time'),
                        'open': float(mid.get('o', 0)),
                        'high': float(mid.get('h', 0)),
                        'low': float(mid.get('l', 0)),
                        'close': float(mid.get('c', 0)),
                        'volume': int(candle.get('volume', 0))
                    })
            
            if not data:
                logger.error(f"No complete candles found for {symbol}")
                return None
                
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            logger.info(f"✅ Fetched {len(df)} historical candles for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            raise MarketDataUnavailableError(f"Failed to fetch historical data for {symbol}: {e}")

    async def modify_position(self, trade_id: str, stop_loss: float = None, take_profit: float = None) -> bool:
        """Modify stop-loss and/or take-profit for an existing trade."""
        try:
            data = {}
            symbol = None
            # Try to get symbol from position tracker if available
            if hasattr(self, 'position_tracker') and self.position_tracker is not None:
                pos_info = await self.position_tracker.get_position_info(trade_id)
                if pos_info and 'symbol' in pos_info:
                    symbol = pos_info['symbol']
            # If still not found, fallback to OANDA API (get trade details)
            if symbol is None:
                try:
                    from oandapyV20.endpoints.trades import TradeDetails
                    trade_details_req = TradeDetails(self.config.oanda_account_id, trade_id)
                    response = await self.robust_oanda_request(trade_details_req)
                    symbol = response.get('trade', {}).get('instrument')
                except Exception as e:
                    logger.warning(f"Could not fetch symbol for trade_id {trade_id}: {e}")
            # Now round prices if possible
            if stop_loss is not None:
                if symbol:
                    stop_loss = round_price_for_instrument(stop_loss, symbol)
                data["stopLoss"] = {
                    "timeInForce": "GTC",
                    "price": str(stop_loss)
                }
            if take_profit is not None:
                if symbol:
                    take_profit = round_price_for_instrument(take_profit, symbol)
                data["takeProfit"] = {
                    "timeInForce": "GTC",
                    "price": str(take_profit)
                }
            if not data:
                logger.warning(f"No stop_loss or take_profit provided for trade {trade_id}")
                return False
            request = TradeCRCDO(self.config.oanda_account_id, tradeID=trade_id, data=data)
            response = await self.robust_oanda_request(request)
            logger.info(f"Modified trade {trade_id} SL/TP: {data}, response: {response}")
            return True
        except Exception as e:
            logger.error(f"Failed to modify trade {trade_id} SL/TP: {e}")
            return False

    async def execute_trade_with_smart_routing(self, 
                                             payload: dict,
                                             execution_strategy: str = "adaptive") -> tuple[bool, dict]:
        """
        Institutional-grade trade execution with smart order routing.
        
        Args:
            payload: Trade parameters
            execution_strategy: Execution strategy (adaptive, aggressive, conservative)
            
        Returns:
            Tuple of (success, response_data)
        """
        start_time = time.time()
        
        try:
            # Pre-execution validation
            validation_result = await self._validate_trade_payload(payload)
            if not validation_result['valid']:
                return False, {'error': f"Trade validation failed: {validation_result['reason']}"}
            
            # Market condition analysis
            market_conditions = await self._analyze_market_conditions(payload['symbol'])
            
            # Select execution strategy based on market conditions
            if execution_strategy == "adaptive":
                execution_strategy = self._select_adaptive_strategy(market_conditions)
            
            # Execute with selected strategy
            execution_result = await self._execute_with_strategy(payload, execution_strategy, market_conditions)
            
            # Post-execution analysis
            execution_quality = await self._analyze_execution_quality(
                start_time, execution_result, market_conditions
            )
            
            # Update execution metrics
            await self._update_execution_metrics(execution_quality)
            
            return execution_result['success'], execution_result['data']
            
        except Exception as e:
            logger.error(f"Smart routing execution failed: {e}")
            return False, {'error': str(e)}
    
    async def _validate_trade_payload(self, payload: dict) -> dict:
        """Validate trade payload before execution"""
        required_fields = ['symbol', 'side', 'units']
        
        for field in required_fields:
            if field not in payload:
                return {'valid': False, 'reason': f"Missing required field: {field}"}
        
        # Validate position size
        if payload['units'] <= 0:
            return {'valid': False, 'reason': "Position size must be positive"}
        
        # Validate symbol format
        if not self._is_valid_symbol(payload['symbol']):
            return {'valid': False, 'reason': f"Invalid symbol format: {payload['symbol']}"}
        
        return {'valid': True}
    
    async def _analyze_market_conditions(self, symbol: str) -> dict:
        """Analyze current market conditions for execution"""
        try:
            # Get current spread
            pricing_info = await self._get_pricing_info(symbol)
            spread = pricing_info.get('spread', 0)
            
            # Get recent volatility
            historical_data = await self.get_historical_data(symbol, 20, "M5")
            if historical_data and len(historical_data) > 0:
                prices = [float(candle['mid']['c']) for candle in historical_data]
                volatility = self._calculate_volatility(prices)
            else:
                volatility = 0.0
            
            # Get market depth (simplified)
            market_depth = await self._estimate_market_depth(symbol)
            
            return {
                'spread': spread,
                'volatility': volatility,
                'market_depth': market_depth,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.warning(f"Market condition analysis failed: {e}")
            return {
                'spread': 0.0,
                'volatility': 0.0,
                'market_depth': 'unknown',
                'timestamp': datetime.now()
            }
    
    def _select_adaptive_strategy(self, market_conditions: dict) -> str:
        """Select execution strategy based on market conditions"""
        spread = market_conditions.get('spread', 0)
        volatility = market_conditions.get('volatility', 0)
        
        # High spread or volatility -> conservative
        if spread > 0.0005 or volatility > 0.002:
            return "conservative"
        
        # Low spread and volatility -> aggressive
        elif spread < 0.0002 and volatility < 0.0005:
            return "aggressive"
        
        # Default to adaptive
        else:
            return "adaptive"
    
    async def _execute_with_strategy(self, 
                                   payload: dict, 
                                   strategy: str, 
                                   market_conditions: dict) -> dict:
        """Execute trade with specific strategy"""
        
        if strategy == "aggressive":
            return await self._execute_aggressive(payload, market_conditions)
        elif strategy == "conservative":
            return await self._execute_conservative(payload, market_conditions)
        else:  # adaptive
            return await self._execute_adaptive(payload, market_conditions)
    
    async def _execute_aggressive(self, payload: dict, market_conditions: dict) -> dict:
        """Aggressive execution - immediate market orders"""
        try:
            # Use market orders for immediate execution
            order_payload = {
                **payload,
                'type': 'MARKET',
                'timeInForce': 'FOK'  # Fill or Kill
            }
            
            result = await self.execute_trade(order_payload)
            return {
                'success': result[0],
                'data': result[1],
                'strategy': 'aggressive',
                'execution_type': 'market'
            }
            
        except Exception as e:
            logger.error(f"Aggressive execution failed: {e}")
            return {'success': False, 'data': {'error': str(e)}}
    
    async def _execute_conservative(self, payload: dict, market_conditions: dict) -> dict:
        """Conservative execution - limit orders with wider spreads"""
        try:
            # Calculate conservative price levels
            current_price = await self.get_current_price(payload['symbol'], payload['side'])
            spread = market_conditions.get('spread', 0.0002)
            
            # Use wider spread for conservative execution
            conservative_spread = spread * 2
            
            if payload['side'] == 'buy':
                limit_price = current_price + conservative_spread
            else:
                limit_price = current_price - conservative_spread
            
            order_payload = {
                **payload,
                'type': 'LIMIT',
                'price': str(limit_price),
                'timeInForce': 'GTC'  # Good Till Cancelled
            }
            
            result = await self.execute_trade(order_payload)
            return {
                'success': result[0],
                'data': result[1],
                'strategy': 'conservative',
                'execution_type': 'limit'
            }
            
        except Exception as e:
            logger.error(f"Conservative execution failed: {e}")
            return {'success': False, 'data': {'error': str(e)}}
    
    async def _execute_adaptive(self, payload: dict, market_conditions: dict) -> dict:
        """Adaptive execution - dynamic strategy selection"""
        try:
            # Start with limit order, fallback to market if needed
            current_price = await self.get_current_price(payload['symbol'], payload['side'])
            spread = market_conditions.get('spread', 0.0002)
            
            # Use moderate spread
            adaptive_spread = spread * 1.5
            
            if payload['side'] == 'buy':
                limit_price = current_price + adaptive_spread
            else:
                limit_price = current_price - adaptive_spread
            
            # Try limit order first
            limit_payload = {
                **payload,
                'type': 'LIMIT',
                'price': str(limit_price),
                'timeInForce': 'IOC'  # Immediate or Cancel
            }
            
            result = await self.execute_trade(limit_payload)
            
            # If limit order fails, try market order
            if not result[0]:
                logger.info("Limit order failed, trying market order")
                market_payload = {
                    **payload,
                    'type': 'MARKET',
                    'timeInForce': 'FOK'
                }
                result = await self.execute_trade(market_payload)
            
            return {
                'success': result[0],
                'data': result[1],
                'strategy': 'adaptive',
                'execution_type': 'hybrid'
            }
            
        except Exception as e:
            logger.error(f"Adaptive execution failed: {e}")
            return {'success': False, 'data': {'error': str(e)}}
    
    async def _analyze_execution_quality(self, 
                                       start_time: float, 
                                       execution_result: dict, 
                                       market_conditions: dict) -> dict:
        """Analyze execution quality metrics"""
        execution_time = time.time() - start_time
        
        quality_metrics = {
            'execution_time': execution_time,
            'success': execution_result.get('success', False),
            'strategy_used': execution_result.get('strategy', 'unknown'),
            'execution_type': execution_result.get('execution_type', 'unknown'),
            'market_conditions': market_conditions,
            'timestamp': datetime.now()
        }
        
        # Add slippage analysis if available
        if execution_result.get('success') and 'data' in execution_result:
            data = execution_result['data']
            if 'price' in data and 'requested_price' in data:
                slippage = abs(float(data['price']) - float(data['requested_price']))
                quality_metrics['slippage'] = slippage
        
        return quality_metrics
    
    async def _update_execution_metrics(self, quality_metrics: dict):
        """Update execution performance metrics"""
        # Store metrics for analysis (could be sent to monitoring system)
        logger.info(f"Execution quality: {quality_metrics}")
        
        # Update circuit breaker if needed
        if not quality_metrics.get('success', False):
            self.circuit_breaker_failures += 1
        else:
            # Reset failures on success
            self.circuit_breaker_failures = max(0, self.circuit_breaker_failures - 1)
    
    def _is_valid_symbol(self, symbol: str) -> bool:
        """Validate symbol format"""
        # Basic validation for OANDA symbols
        valid_pairs = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'AUD_USD', 'USD_CAD', 'USD_CHF']
        return symbol in valid_pairs or '_' in symbol
    
    def _calculate_volatility(self, prices: list) -> float:
        """Calculate price volatility"""
        if len(prices) < 2:
            return 0.0
        
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] != 0:
                returns.append((prices[i] - prices[i-1]) / prices[i-1])
        
        return np.std(returns) if returns else 0.0
    
    async def _estimate_market_depth(self, symbol: str) -> str:
        """Estimate market depth (simplified)"""
        # In a real implementation, this would analyze order book depth
        # For now, return a simplified estimate
        return "medium"  # low, medium, high

    async def _get_pricing_info(self, symbol: str) -> Dict[str, Any]:
        """Get pricing information for a symbol including spread calculation"""
        try:
            # Get current bid and ask prices
            bid_price = await self.get_current_price(symbol, "SELL")
            ask_price = await self.get_current_price(symbol, "BUY")
            
            # Calculate spread
            spread = ask_price - bid_price
            
            return {
                'bid': bid_price,
                'ask': ask_price,
                'spread': spread,
                'mid_price': (bid_price + ask_price) / 2,
                'timestamp': datetime.now()
            }
            
        except MarketDataUnavailableError as e:
            logger.warning(f"Could not get pricing info for {symbol}: {e}")
            return {
                'bid': 0.0,
                'ask': 0.0,
                'spread': 0.0,
                'mid_price': 0.0,
                'timestamp': datetime.now(),
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Error getting pricing info for {symbol}: {e}")
            return {
                'bid': 0.0,
                'ask': 0.0,
                'spread': 0.0,
                'mid_price': 0.0,
                'timestamp': datetime.now(),
                'error': str(e)
            }

    def _update_health_score(self, delta: int):
        """Update connection health score (0-100 scale)"""
        self.connection_health_score = max(0, min(100, self.connection_health_score + delta))
        
        # Log significant health changes
        if delta <= -20:
            logger.warning(f"Connection health degraded to {self.connection_health_score}% (delta: {delta})")
        elif delta >= +10 and self.connection_health_score >= 90:
            logger.info(f"Connection health excellent: {self.connection_health_score}%")
    
    async def _check_rate_limits(self) -> bool:
        """INSTITUTIONAL FIX: Check if we're within rate limits"""
        now = datetime.now()
        
        # Clean old timestamps (older than 1 minute)
        cutoff = now - timedelta(minutes=1)
        self.request_timestamps = [ts for ts in self.request_timestamps if ts > cutoff]
        
        # Check if we're under the limit
        if len(self.request_timestamps) >= self.max_requests_per_minute:
            logger.warning(f"Rate limit protection: {len(self.request_timestamps)} requests in last minute (limit: {self.max_requests_per_minute})")
            return False
        
        # Add current timestamp
        self.request_timestamps.append(now)
        return True
    
    async def _apply_intelligent_backoff(self, attempt: int, error: Exception) -> float:
        """INSTITUTIONAL FIX: Apply intelligent backoff based on error type and connection health"""
        base_delay = 2.0
        
        # Adjust base delay based on connection health
        if self.connection_health_score < 30:
            base_delay = 10.0  # Longer delays for unhealthy connections
        elif self.connection_health_score < 60:
            base_delay = 5.0   # Medium delays for degraded connections
        
        # Adjust for error type
        error_str = str(error).lower()
        if 'remote end closed' in error_str:
            base_delay *= 2.0  # Double delay for connection closures
        elif 'timeout' in error_str:
            base_delay *= 1.5  # Increase delay for timeouts
        
        # Exponential backoff with jitter
        delay = base_delay * (2 ** min(attempt, 5))  # Cap at 2^5
        jitter = random.uniform(0, delay * 0.2)  # Add 20% jitter
        
        final_delay = min(delay + jitter, 60.0)  # Cap at 60 seconds
        
        logger.info(f"Applying intelligent backoff: {final_delay:.1f}s (attempt {attempt+1}, health: {self.connection_health_score}%)")
        return final_delay

    # async def analyze_market_conditions(self, symbol: str) -> Dict[str, Any]: