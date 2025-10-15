#
# file: oanda_service.py
#
import oandapyV20
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.instruments import InstrumentsCandles
from oandapyV20.endpoints.trades import TradeCRCDO
from oandapyV20.endpoints.positions import OpenPositions
from oandapyV20.exceptions import V20Error
import asyncio
import logging
import random
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Dict, List
from config import config
from utils import round_price_for_instrument, standardize_symbol, MarketDataUnavailableError, get_pip_value
import numpy as np
# Backoff state management (compat import)
try:
    from backoff import ConnectionState  # preferred path
except Exception:  # pragma: no cover - runtime compat
    try:
        from legacy.backoff import ConnectionState  # fallback path in repo
    except Exception:
        # Minimal fallback to keep service operational if module not found
        from datetime import datetime
        class ConnectionState:  # type: ignore
            def __init__(self, success_probe_seconds: int = 600):
                self.state = "DEGRADED"
                self._success_probe_seconds = success_probe_seconds
                self._next_probe_at = datetime.now()
                self._consecutive_503s = 0
            def _should_probe(self) -> bool:
                return True
            def handle_503_maintenance(self) -> bool:
                self.state = "MAINTENANCE"; return False
            def handle_other_error(self, _error: Exception) -> bool:
                self.state = "DEGRADED"; return False
            def handle_success(self) -> bool:
                self.state = "OK"; return True
            def can_trade(self) -> bool:
                return self.state == "OK"
            def get_status(self) -> dict:
                return {
                    "state": self.state,
                    "consecutive_503s": self._consecutive_503s,
                    "next_probe_at": self._next_probe_at.isoformat(),
                    "backoff_current": 0.0,
                    "backoff_reset_age_seconds": 0.0,
                }

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
# Re-enable rate limiting to reduce log noise in production
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
        
        # NEW: Lightweight per-symbol price cache to collapse duplicate requests
        self.price_cache: Dict[str, Dict[str, float | bool | datetime]] = {}
        self.price_cache_ttl_seconds: int = 2  # collapse bursts across components

        # Reduce third-party library log noise
        try:
            logging.getLogger("oandapyV20.oandapyV20").setLevel(logging.WARNING)
        except Exception:
            pass

        # Initialize OANDA client asynchronously
        self.oanda = None  # Will be set in initialize()

    async def _init_oanda_client(self):
        """
        Async OANDA client initialization with proper error handling.
        
        INSTITUTIONAL FIX: Properly close existing session before creating new one
        to prevent TCP connection pool exhaustion.
        """
        try:
            # CRITICAL FIX: Close existing session to prevent connection leaks
            if self.oanda is not None and hasattr(self.oanda, 'session'):
                try:
                    if hasattr(self.oanda.session, 'close'):
                        self.oanda.session.close()
                        logger.debug("🔌 Closed existing OANDA session")
                except Exception as e:
                    logger.warning(f"⚠️ Error closing existing session: {e}")
            
            access_token = self.config.oanda_access_token
            if isinstance(access_token, object) and hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            
            # Create NEW OANDA client with fresh session
            self.oanda = oandapyV20.API(
                access_token=access_token,
                environment=self.config.oanda_environment
            )
            
            # INSTITUTIONAL FIX: Configure session with robust connection pooling and retry logic
            if hasattr(self.oanda, 'session'):
                # Set connection pool limits
                from requests.adapters import HTTPAdapter
                from urllib3.util.retry import Retry
                
                # CRITICAL FIX: Aggressive retry strategy for connection stability
                # This handles "RemoteDisconnected" and other transient connection errors at HTTP layer
                retry_strategy = Retry(
                    total=5,    # Increased from 3 - more retries at HTTP layer
                    connect=5,  # Retry on connection failures (RemoteDisconnected, ConnectionError)
                    read=5,     # Retry on read timeouts
                    status=3,   # Retry on HTTP status errors
                    backoff_factor=0.5,  # 0.5s, 1s, 2s, 4s, 8s progressive delays
                    status_forcelist=[429, 500, 502, 503, 504],  # Rate limit + server errors
                    allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
                    raise_on_status=False,
                    raise_on_redirect=False
                )
                
                # INSTITUTIONAL: Larger connection pool to prevent "connection pool is full" errors
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=20,  # Increased from 10 - support more concurrent requests
                    pool_maxsize=30,      # Increased from 10 - prevent pool exhaustion with batch requests
                    pool_block=False      # Non-blocking - fail fast if pool exhausted
                )
                
                self.oanda.session.mount("https://", adapter)
                self.oanda.session.mount("http://", adapter)
                
                # CRITICAL: Configure connection persistence with TCP keepalive
                self.oanda.session.headers.update({
                    'Connection': 'keep-alive',
                    'Keep-Alive': 'timeout=60, max=1000'  # Increased: 60s timeout, up to 1000 requests per connection
                })
                
                # Set timeouts at session level to prevent infinite hangs
                # This will be the default for all requests unless overridden
                self.oanda.session.timeout = (10.0, 30.0)  # (connect timeout, read timeout)
            
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
        """
        INSTITUTIONAL FIX: Lightweight connection validation without aggressive reconnection.
        
        Only reinitializes if client is None or circuit breaker demands it.
        Avoids health check overhead on every request - rely on request-level error handling instead.
        """
        # Quick check: If client exists and circuit breaker is open, proceed
        if self.oanda and not self._should_circuit_break():
            # Trust existing connection - errors will be caught in robust_oanda_request
            return
        
        # Client doesn't exist or circuit breaker is active
        if not self.oanda:
            logger.info("🔌 OANDA client not initialized, creating fresh connection...")
            max_init_attempts = 2
            
            for attempt in range(max_init_attempts):
                try:
                    await self._init_oanda_client()
                    if self.oanda:
                        logger.info(f"✅ OANDA client initialized (attempt {attempt + 1})")
                        return
                except Exception as e:
                    if attempt == max_init_attempts - 1:
                        raise Exception(f"Failed to initialize OANDA client: {e}")
                    logger.warning(f"Init attempt {attempt + 1} failed: {e}, retrying...")
                    await asyncio.sleep(3 * (attempt + 1))  # Progressive backoff
        
        # If circuit breaker is active, throw exception to stop request flood
        if self._should_circuit_break():
            raise Exception("Circuit breaker is active - connection in failure mode")

    def _should_circuit_break(self) -> bool:
        """
        INSTITUTIONAL FIX: Enhanced circuit breaker with exponential recovery time.
        
        Prevents request flooding during sustained OANDA outages or connection issues.
        Auto-resets after cooling period with health validation.
        """
        if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
            if not self.circuit_breaker_reset_time:
                # Exponential cooldown: 5 min * (failures / threshold)
                cooldown_minutes = 5 * (self.circuit_breaker_failures / self.circuit_breaker_threshold)
                cooldown_minutes = min(cooldown_minutes, 30)  # Cap at 30 minutes
                self.circuit_breaker_reset_time = datetime.now() + timedelta(minutes=cooldown_minutes)
                logger.error(
                    f"🔴 CIRCUIT BREAKER ACTIVATED - {self.circuit_breaker_failures} consecutive failures. "
                    f"Trading paused for {cooldown_minutes:.1f} minutes to prevent request flooding."
                )
                return True
            elif datetime.now() < self.circuit_breaker_reset_time:
                # Still in cooldown period
                return True
            else:
                # Cooldown expired - attempt recovery
                logger.warning(
                    f"🟡 Circuit breaker cooldown expired. Attempting recovery... "
                    f"(Health score: {self.connection_health_score})"
                )
                # Partial reset - reduce failures but don't fully clear until we get a successful request
                self.circuit_breaker_failures = max(0, self.circuit_breaker_failures - 2)
                self.circuit_breaker_reset_time = None
                
                # If health is still critical, re-activate circuit breaker with shorter cooldown
                if self.connection_health_score < 30:
                    logger.error("🔴 Health still critical, re-activating circuit breaker for 2 minutes")
                    self.circuit_breaker_reset_time = datetime.now() + timedelta(minutes=2)
                    return True
                    
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

    async def get_connection_status(self) -> dict:
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
        """
        Stop the OandaService and clean up connections.
        
        INSTITUTIONAL FIX: Properly close HTTP session to prevent connection leaks.
        """
        logger.info("OANDA service is shutting down.")
        
        # CRITICAL: Close the requests session to free TCP connections
        if self.oanda is not None and hasattr(self.oanda, 'session'):
            try:
                if hasattr(self.oanda.session, 'close'):
                    self.oanda.session.close()
                    logger.info("🔌 OANDA session closed successfully")
            except Exception as e:
                logger.warning(f"⚠️ Error closing OANDA session during shutdown: {e}")

    # ---- Safe stubs for broker SL/TP removal (used by trailing stop activation) ----
    async def remove_stop_loss(self, trade_id_or_position_id: str) -> bool:
        try:
            from oandapyV20.endpoints.trades import TradeCRCDO
            # Resolve trade id if a position_id was passed
            resolved_id = str(trade_id_or_position_id)
            if not resolved_id.isdigit():
                try:
                    from oandapyV20.endpoints.trades import OpenTrades
                    open_req = OpenTrades(self.config.oanda_account_id)
                    open_resp = await self.robust_oanda_request(open_req)
                    # pick first trade as conservative fallback
                    if isinstance(open_resp, dict):
                        trades = open_resp.get('trades', [])
                        if trades:
                            resolved_id = str(trades[0].get('id', resolved_id))
                except Exception:
                    pass
            data = {"stopLoss": None}
            req = TradeCRCDO(self.config.oanda_account_id, tradeID=resolved_id, data=data)
            await self.robust_oanda_request(req)
            return True
        except Exception as e:
            logger.warning(f"remove_stop_loss noop/fail for {trade_id_or_position_id}: {e}")
            return False

    async def remove_take_profit(self, trade_id_or_position_id: str) -> bool:
        try:
            from oandapyV20.endpoints.trades import TradeCRCDO
            resolved_id = str(trade_id_or_position_id)
            if not resolved_id.isdigit():
                try:
                    from oandapyV20.endpoints.trades import OpenTrades
                    open_req = OpenTrades(self.config.oanda_account_id)
                    open_resp = await self.robust_oanda_request(open_req)
                    if isinstance(open_resp, dict):
                        trades = open_resp.get('trades', [])
                        if trades:
                            resolved_id = str(trades[0].get('id', resolved_id))
                except Exception:
                    pass
            data = {"takeProfit": None}
            req = TradeCRCDO(self.config.oanda_account_id, tradeID=resolved_id, data=data)
            await self.robust_oanda_request(req)
            return True
        except Exception as e:
            logger.warning(f"remove_take_profit noop/fail for {trade_id_or_position_id}: {e}")
            return False

    async def robust_oanda_request(self, request, max_retries: int = 5, initial_delay: float = 3.0):
        """
        INSTITUTIONAL FIX: Enhanced OANDA API request with intelligent retry logic.
        
        Removed aggressive pre-request health checks that cause connection churn.
        Let actual request failures drive recovery logic instead of preemptive checking.
        """
        # Check circuit breaker first - stop request flood during failures
        if self._should_circuit_break():
            raise Exception("Circuit breaker is active - too many consecutive failures")
        
        # Lightweight connection validation (doesn't make extra API calls)
        await self._ensure_connection()
        
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
                # Simple per-minute request rate protection
                if not await self._check_rate_limits():
                    await asyncio.sleep(1.0)
                    # continue the loop without counting as an attempt
                    raise Exception("Rate limit protection - delaying request")

                # INSTITUTIONAL FIX: Conservative session management - avoid unnecessary reconnections
                # Let TCP keepalive and connection pooling handle staleness
                # Only refresh on actual connection failures, not preemptively
                if attempt == 0 and self.session_created_at:
                    session_age = (datetime.now() - self.session_created_at).total_seconds()
                    
                    # CRITICAL: Only refresh if session is extremely old (>30 minutes)
                    # This prevents connection churn - OANDA maintains keepalive automatically
                    if session_age > 1800:  # 30 minutes
                        logger.info(f"🔄 Refreshing OANDA session due to age ({session_age/60:.1f}min)")
                        await self._reinitialize_client()
                    # Removed idle-time based refresh - it causes thrashing with batch requests
                
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
                    self.circuit_breaker_failures += 1
                    raise Exception(f"OANDA request failed after {max_retries} attempts: {e}")
                
                # INSTITUTIONAL FIX: Exponential backoff with jitter for connection errors
                if is_conn_error:
                    # Severe connection error - increment circuit breaker
                    self.circuit_breaker_failures += 1
                    self._update_health_score(-20)  # Penalize health score
                    
                    # Exponential backoff: 2^attempt * base_delay + jitter
                    base_delay = initial_delay * (2 ** attempt)
                    jitter = random.uniform(0, base_delay * 0.3)  # 30% jitter
                    final_delay = min(base_delay + jitter, 60.0)  # Cap at 60 seconds
                    
                    logger.warning(
                        f"🔌 Connection error (health: {self.connection_health_score}), "
                        f"retrying in {final_delay:.1f}s (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    
                    # Force reconnection after 2 consecutive connection errors
                    if attempt >= 1:
                        logger.info("🔄 Forcing reconnection due to repeated connection errors")
                        await self._reinitialize_client()
                else:
                    # Non-connection error - use lighter backoff
                    final_delay = initial_delay * (1.5 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"OANDA request error, retrying in {final_delay:.1f}s (attempt {attempt + 1}/{max_retries}): {e}")
                
                await asyncio.sleep(final_delay)

    def _get_cached_price(self, symbol: str) -> Dict[str, float] | None:
        """Return cached bid/ask if within TTL."""
        cache = self.price_cache.get(symbol)
        if not cache:
            return None
        ts = cache.get("timestamp")
        if not isinstance(ts, datetime):
            return None
        if (datetime.now() - ts).total_seconds() <= self.price_cache_ttl_seconds:
            return {"bid": float(cache.get("bid", 0.0)), "ask": float(cache.get("ask", 0.0))}
        return None

    def _store_price_cache(self, symbol: str, bid: float, ask: float, tradeable: bool):
        self.price_cache[symbol] = {
            "bid": bid,
            "ask": ask,
            "tradeable": tradeable,
            "timestamp": datetime.now()
        }

    async def _get_pricing_info_batch(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Fetch pricing for multiple instruments in a single request."""
        if not symbols:
            return {}
        params = {"instruments": ",".join(symbols)}
        results: Dict[str, Dict[str, float]] = {}
        # INSTITUTIONAL FIX: Chunk large symbol lists and add per-symbol fallback to reduce batch fragility
        chunk_size = 20
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            try:
                chunk_params = {"instruments": ",".join(chunk)}
                chunk_request = PricingInfo(
                    accountID=self.config.oanda_account_id,
                    params=chunk_params
                )
                response = await self.robust_oanda_request(chunk_request)
                prices_list = response.get('prices', []) if isinstance(response, dict) else []
                for price_data in prices_list:
                    instrument = price_data.get('instrument')
                    tradeable = price_data.get('tradeable', False)
                    bid = float(price_data.get('bid') or price_data.get('closeoutBid') or 0.0)
                    ask = float(price_data.get('ask') or price_data.get('closeoutAsk') or 0.0)
                    if instrument:
                        if tradeable and bid > 0 and ask > 0:
                            results[instrument] = {"bid": bid, "ask": ask}
                        else:
                            self._store_price_cache(instrument, bid, ask, tradeable)
                # Per-symbol fallback for any instruments not returned in this chunk
                missing_syms = [sym for sym in chunk if sym not in results]
                for sym in missing_syms:
                    try:
                        single_params = {"instruments": sym}
                        single_req = PricingInfo(
                            accountID=self.config.oanda_account_id,
                            params=single_params
                        )
                        single_resp = await self.robust_oanda_request(single_req)
                        single_prices = single_resp.get('prices', []) if isinstance(single_resp, dict) else []
                        for price_data in single_prices:
                            instrument = price_data.get('instrument')
                            tradeable = price_data.get('tradeable', False)
                            bid = float(price_data.get('bid') or price_data.get('closeoutBid') or 0.0)
                            ask = float(price_data.get('ask') or price_data.get('closeoutAsk') or 0.0)
                            if instrument and tradeable and bid > 0 and ask > 0:
                                results[instrument] = {"bid": bid, "ask": ask}
                            else:
                                self._store_price_cache(instrument or sym, bid, ask, tradeable)
                    except Exception as e3:
                        logger.warning(f"Per-symbol fallback (missing in chunk) failed for {sym}: {e3}")
            except Exception as e:
                logger.warning(f"Batch pricing chunk failed ({len(chunk)} symbols): {e}. Falling back to per-symbol requests.")
                # Per-symbol fallback for this chunk
                for sym in chunk:
                    try:
                        single_params = {"instruments": sym}
                        single_req = PricingInfo(
                            accountID=self.config.oanda_account_id,
                            params=single_params
                        )
                        single_resp = await self.robust_oanda_request(single_req)
                        prices_list = single_resp.get('prices', []) if isinstance(single_resp, dict) else []
                        for price_data in prices_list:
                            instrument = price_data.get('instrument')
                            tradeable = price_data.get('tradeable', False)
                            bid = float(price_data.get('bid') or price_data.get('closeoutBid') or 0.0)
                            ask = float(price_data.get('ask') or price_data.get('closeoutAsk') or 0.0)
                            if instrument and tradeable and bid > 0 and ask > 0:
                                results[instrument] = {"bid": bid, "ask": ask}
                            else:
                                self._store_price_cache(instrument or sym, bid, ask, tradeable)
                    except Exception as e2:
                        logger.warning(f"Per-symbol pricing failed for {sym}: {e2}")
            # Stagger chunk requests to reduce TCP pressure
            if i + chunk_size < len(symbols):
                await asyncio.sleep(0.2)
        # Update cache for fetched symbols
        for sym, px in results.items():
            self._store_price_cache(sym, px["bid"], px["ask"], True)
        return results

    async def get_current_prices(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Get current bid/ask for multiple symbols with caching and batching."""
        symbols_to_fetch: List[str] = []
        results: Dict[str, Dict[str, float]] = {}
        for sym in symbols:
            cached = self._get_cached_price(sym)
            if cached and cached.get("bid", 0) > 0 and cached.get("ask", 0) > 0:
                results[sym] = cached
            else:
                symbols_to_fetch.append(sym)
        if symbols_to_fetch:
            fetched = await self._get_pricing_info_batch(symbols_to_fetch)
            results.update(fetched)
        return results

    async def get_current_price(self, symbol: str, action: str) -> float:
        try:
            # Try cache first (avoids duplicate network calls within TTL)
            cached = self._get_cached_price(symbol)
            if cached:
                px = cached["ask"] if action.upper() == "BUY" else cached["bid"]
                if px and px > 0:
                    logger.info(f"✅ Live price for {symbol} {action}: {px}")
                    return px

            # Fetch fresh via batched path for a single symbol
            prices = await self.get_current_prices([symbol])
            if symbol not in prices:
                raise MarketDataUnavailableError(f"No price data in OANDA response for {symbol}")
            bid = prices[symbol]["bid"]
            ask = prices[symbol]["ask"]
            price = ask if action.upper() == "BUY" else bid
            if price and price > 0:
                logger.info(f"✅ Live price for {symbol} {action}: {price}")
                return price
            raise MarketDataUnavailableError(f"Invalid price data received for {symbol}")

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

    async def get_account_info(self) -> dict:
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

    async def debug_crypto_availability(self) -> dict:
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

    async def check_crypto_availability(self) -> dict:
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
        symbol = standardize_symbol(symbol)
        action = payload.get("action")
        units = payload.get("units")
        stop_loss = payload.get("stop_loss")
        take_profit = payload.get("take_profit")
        
        # DIAGNOSTIC: Log incoming payload
        logger.info(f"🔍 EXECUTE_TRADE RECEIVED: symbol={symbol}, action={action}, units={units} (type={type(units)}), stop_loss={stop_loss}")
        
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
            "timeInForce": "IOC"
            }
        }

        # Add stop loss if provided (use distance to avoid price drift rejections)
        if stop_loss is not None:
            stop_loss = round_price_for_instrument(stop_loss, symbol)
            try:
                pip = float(get_pip_value(symbol))
            except Exception:
                pip = 0.0001
            
            # Calculate initial distance
            sl_distance = 0.0
            if action and str(action).upper() == "BUY":
                sl_distance = max((current_price - float(stop_loss)), 0.0)
            else:
                sl_distance = max((float(stop_loss) - current_price), 0.0)
            
            # INSTITUTIONAL FIX: Enforce OANDA minimum distance requirements
            # Different instruments have different minimum stop distances
            min_distance_pips = 5  # Default: 5 pips minimum
            
            # Adjust minimums based on instrument type
            if any(crypto in symbol.upper() for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH']):
                # Crypto: Higher minimum due to volatility
                min_distance_pips = 20  # 20 pips for crypto
            elif any(cross in symbol for cross in ['JPY', 'CHF', 'CAD', 'AUD', 'NZD']):
                # Cross pairs: Moderate minimum
                min_distance_pips = 10  # 10 pips for crosses
            else:
                # Major pairs: Standard minimum
                min_distance_pips = 5  # 5 pips for majors
            
            min_distance = pip * min_distance_pips
            
            # Enforce minimum with safety buffer
            if sl_distance < min_distance:
                logger.warning(
                    f"⚠️ Stop distance too tight: {sl_distance:.5f} < {min_distance:.5f} "
                    f"({min_distance_pips} pips minimum for {symbol}). Adjusting..."
                )
                sl_distance = min_distance
            else:
                # Add safety cushion (2 pips) only if distance is already valid
                sl_distance = sl_distance + (pip * 2.0)
            
            # INSTITUTIONAL FIX: Round distance to instrument precision to avoid PRECISION_EXCEEDED errors
            # Different instruments have different decimal precision requirements
            if any(crypto in symbol.upper() for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH']):
                # Crypto: 2 decimal places (e.g., 358.90 for ETH_USD)
                sl_distance = round(sl_distance, 2)
            elif 'JPY' in symbol:
                # JPY pairs: 2 decimal places (e.g., 175.65)
                sl_distance = round(sl_distance, 2)
            else:
                # Major FX pairs: 5 decimal places (e.g., 0.00050)
                sl_distance = round(sl_distance, 5)
            
            logger.info(f"✅ Final stop distance for {symbol}: {sl_distance} ({sl_distance/pip:.1f} pips)")
            
            data["order"]["stopLossOnFill"] = {
                "timeInForce": "GTC",
                "distance": str(sl_distance)
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
        min_units = 1 if is_crypto_signal else 1
        last_error = None
        current_units = float(units)
        # Enforce minimum units upfront to avoid OANDA cancellations for tiny sizes
        if current_units < min_units:
            current_units = float(min_units)
        while attempt < max_retries:
            # Update units in data for each attempt
            # FIX: OANDA requires whole number units for crypto pairs (UNITS_PRECISION_EXCEEDED error)
            if is_crypto_signal:
                data["order"]["units"] = str(int(max(current_units, min_units)))  # Whole number units for crypto
            else:
                data["order"]["units"] = str(int(max(current_units, min_units)))  # Integer units for forex
            
            # DIAGNOSTIC: Log what we're sending to OANDA
            logger.info(f"🔍 SENDING TO OANDA: attempt={attempt+1}, current_units={current_units}, min_units={min_units}, final={data['order']['units']}")
            
            try:
                from oandapyV20.endpoints.orders import OrderCreate
                request = OrderCreate(self.config.oanda_account_id, data=data)
                response = await self.robust_oanda_request(request)
                
                if response and 'orderFillTransaction' in response:
                    fill_transaction = response['orderFillTransaction']
                    transaction_id = fill_transaction.get('id')
                    fill_price = float(fill_transaction.get('price', current_price))
                    # FIX: OANDA requires whole number units for crypto pairs
                    units_str = fill_transaction.get('units', current_units)
                    if is_crypto_signal:
                        actual_units = int(float(units_str))  # Whole number units for crypto
                    else:
                        actual_units = int(float(units_str))  # Integer units for forex
                    
                    # DIAGNOSTIC: Log if executed units differ from requested
                    if abs(actual_units) != abs(int(float(data["order"]["units"]))):
                        logger.error(f"🚨 UNIT MISMATCH: Requested {data['order']['units']} but OANDA filled {actual_units} units!")
                        logger.error(f"🚨 FULL OANDA RESPONSE: {response}")
                    
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
                    logger.error(f"Order was cancelled: {cancel_reason} (attempt {attempt+1}/{max_retries}, units={float(current_units)})")
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
    
    async def get_open_positions_from_oanda(self) -> list[dict]:
        """Fetch open positions directly from OANDA (instrument, units, prices)."""
        try:
            request = OpenPositions(self.config.oanda_account_id)
            response = await self.robust_oanda_request(request)
            positions = []
            for pos in response.get('positions', []) if isinstance(response, dict) else []:
                instrument = pos.get('instrument')
                long_units = float(pos.get('long', {}).get('units', 0) or 0)
                short_units = float(pos.get('short', {}).get('units', 0) or 0)
                units = long_units if long_units > 0 else (-abs(short_units) if short_units < 0 else 0)
                if instrument and units != 0:
                    positions.append({
                        'instrument': instrument,
                        'units': units,
                        'long': pos.get('long', {}),
                        'short': pos.get('short', {})
                    })
            return positions
        except Exception as e:
            logger.error(f"Failed to fetch open positions from OANDA: {e}")
            return []
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
            # If caller passed a position_id (like OANDA_EUR_JPY_BUY), resolve to instrument and numeric trade id
            resolved_trade_id = str(trade_id)
            if not resolved_trade_id.isdigit():
                # Attempt to get from position tracker first
                if hasattr(self, 'position_tracker') and self.position_tracker is not None:
                    pos_info = await self.position_tracker.get_position_info(trade_id)
                    if pos_info:
                        symbol = pos_info.get('symbol')
                # As a fallback, search open trades from OANDA and match by instrument
                try:
                    from oandapyV20.endpoints.trades import OpenTrades
                    open_req = OpenTrades(self.config.oanda_account_id)
                    open_resp = await self.robust_oanda_request(open_req)
                    for t in open_resp.get('trades', []) if isinstance(open_resp, dict) else []:
                        inst = t.get('instrument')
                        tid = t.get('id')
                        if symbol is None and inst:
                            symbol = inst
                        # If we have a symbol, prefer the trade that matches it
                        if symbol and inst == symbol:
                            resolved_trade_id = str(tid)
                            break
                except Exception as e:
                    logger.warning(f"Could not resolve trade id from position_id {trade_id}: {e}")
            # If still no symbol, try TradeDetails on resolved id
            if symbol is None:
                try:
                    from oandapyV20.endpoints.trades import TradeDetails
                    td = TradeDetails(self.config.oanda_account_id, resolved_trade_id)
                    tresp = await self.robust_oanda_request(td)
                    symbol = tresp.get('trade', {}).get('instrument')
                except Exception as e:
                    logger.warning(f"Could not fetch symbol for trade_id {resolved_trade_id}: {e}")
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
            request = TradeCRCDO(self.config.oanda_account_id, tradeID=str(resolved_trade_id), data=data)
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
                'timeInForce': 'IOC'  # Immediate or Cancel
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
                    'timeInForce': 'IOC'
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

    async def _get_pricing_info(self, symbol: str) -> dict:
        """Get pricing information for a symbol including spread calculation."""
        try:
            prices = await self.get_current_prices([symbol])
            if symbol not in prices:
                raise MarketDataUnavailableError("No pricing returned")
            bid_price = float(prices[symbol]["bid"])
            ask_price = float(prices[symbol]["ask"])
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

    async def estimate_implied_slippage_bps(self, symbol: str, clip_fraction: float = 0.2) -> float:
        """
        Estimate implied slippage in basis points using spread and a conservative
        volatility proxy when detailed order book is not available.

        Args:
            symbol: Instrument symbol
            clip_fraction: Fraction of current position to close (affects impact)

        Returns:
            Estimated slippage in basis points (bps)
        """
        try:
            px = await self._get_pricing_info(symbol)
            mid = px.get('mid_price', 0.0)
            spread = px.get('spread', 0.0)
            if mid <= 0:
                return 100.0  # fallback worst-case 100 bps

            # Base slippage: half-spread (crossing the spread) in bps
            half_spread_bps = (spread / mid) * 10000 / 2.0

            # Impact proxy: larger clips incur more impact; simple convex function
            impact_bps = max(0.0, (clip_fraction ** 1.5) * 10.0)

            # Volatility proxy: add a portion of short-term volatility when available
            # Reuse simplified volatility calc if we have recent mid prices (not implemented here)
            vol_bps = 0.0

            return float(half_spread_bps + impact_bps + vol_bps)
        except Exception as e:
            logger.warning(f"Could not estimate implied slippage for {symbol}: {e}")
            return 100.0

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

    async def close_position(self, symbol: str, units: float) -> tuple[bool, dict]:
        """
        Close an existing position using OANDA's position closeout API
        """
        try:
            original_symbol = symbol
            symbol = standardize_symbol(symbol)
            logger.info(f"🔄 [CLOSE DEBUG] Starting close attempt")
            logger.info(f"🔄 [CLOSE DEBUG] Original symbol: {original_symbol}")
            logger.info(f"🔄 [CLOSE DEBUG] Standardized symbol: {symbol}")
            logger.info(f"🔄 [CLOSE DEBUG] Units to close: {units}")
            
            # Get current positions to find the exact position to close
            from oandapyV20.endpoints.positions import OpenPositions
            positions_request = OpenPositions(accountID=self.config.oanda_account_id)
            logger.info(f"🔍 [CLOSE DEBUG] Requesting positions from OANDA account: {self.config.oanda_account_id}")
            
            positions_response = await self.robust_oanda_request(positions_request)
            
            if not positions_response:
                logger.error(f"❌ [CLOSE DEBUG] No response from OANDA positions request")
                return False, {"error": "No response from OANDA"}
            
            if 'positions' not in positions_response:
                logger.error(f"❌ [CLOSE DEBUG] No 'positions' key in response: {list(positions_response.keys())}")
                return False, {"error": "Invalid OANDA response format"}
            
            positions_list = positions_response['positions']
            logger.info(f"🔍 [CLOSE DEBUG] OANDA returned {len(positions_list)} position entries")
            
            # Debug: Log all available positions in detail
            logger.info(f"🔍 [CLOSE DEBUG] === ALL OANDA POSITIONS ===")
            for i, pos in enumerate(positions_list):
                instrument = pos.get('instrument', 'UNKNOWN')
                long_units = pos.get('long', {}).get('units', '0')
                short_units = pos.get('short', {}).get('units', '0')
                long_pl = pos.get('long', {}).get('unrealizedPL', '0')
                short_pl = pos.get('short', {}).get('unrealizedPL', '0')
                
                logger.info(f"🔍 [CLOSE DEBUG] Position {i+1}: {instrument}")
                logger.info(f"🔍 [CLOSE DEBUG]   Long: {long_units} units, PL: {long_pl}")
                logger.info(f"🔍 [CLOSE DEBUG]   Short: {short_units} units, PL: {short_pl}")
            
            # Find the position for this symbol - try multiple symbol formats
            target_position = None
            symbol_variants = [
                symbol,  # Original symbol
                symbol.replace('_', ''),  # Without underscore
                symbol.replace('_', '/'),  # With slash
                symbol.upper(),  # Uppercase
                symbol.lower(),  # Lowercase
            ]
            
            logger.info(f"🔍 [CLOSE DEBUG] Testing symbol variants: {symbol_variants}")
            
            for position in positions_list:
                position_instrument = position.get('instrument', 'UNKNOWN')
                logger.info(f"🔍 [CLOSE DEBUG] Checking position instrument: '{position_instrument}'")
                logger.info(f"🔍 [CLOSE DEBUG] Against symbol variants: {symbol_variants}")
                
                # Check each variant individually for detailed logging
                for variant in symbol_variants:
                    logger.info(f"🔍 [CLOSE DEBUG] Testing '{position_instrument}' == '{variant}': {position_instrument == variant}")
                
                if position_instrument in symbol_variants:
                    target_position = position
                    logger.info(f"✅ [CLOSE DEBUG] MATCH FOUND! Position instrument: '{position_instrument}'")
                    break
                else:
                    logger.info(f"❌ [CLOSE DEBUG] No match for instrument: '{position_instrument}'")
            
            if not target_position:
                logger.error(f"❌ [CLOSE DEBUG] === POSITION NOT FOUND ===")
                logger.error(f"❌ [CLOSE DEBUG] Target symbol: '{symbol}' (from '{original_symbol}')")
                logger.error(f"❌ [CLOSE DEBUG] Tried variants: {symbol_variants}")
                logger.error(f"❌ [CLOSE DEBUG] Available instruments: {[pos.get('instrument', 'UNKNOWN') for pos in positions_list]}")
                logger.error(f"❌ [CLOSE DEBUG] Total positions checked: {len(positions_list)}")
                return False, {"error": f"Position not found for {symbol}"}
            
            # Determine which side to close (long or short)
            long_units = float(target_position.get('long', {}).get('units', 0))
            short_units = float(target_position.get('short', {}).get('units', 0))
            
            logger.info(f"📊 [CLOSE DEBUG] Position details for '{target_position.get('instrument')}':")
            logger.info(f"📊 [CLOSE DEBUG]   Long units: {long_units}")
            logger.info(f"📊 [CLOSE DEBUG]   Short units: {short_units}")
            
            # Prepare closeout data
            close_data = {}
            if long_units > 0:
                close_data["longUnits"] = "ALL"
                logger.info(f"📉 [CLOSE DEBUG] Will close LONG position: {long_units} units")
            if short_units < 0:
                close_data["shortUnits"] = "ALL"
                logger.info(f"📈 [CLOSE DEBUG] Will close SHORT position: {abs(short_units)} units")
            
            logger.info(f"🔧 [CLOSE DEBUG] Close data prepared: {close_data}")
            
            if not close_data:
                logger.error(f"❌ [CLOSE DEBUG] No units to close for {symbol} (long: {long_units}, short: {short_units})")
                return False, {"error": "No units to close"}
            
            # Execute position closeout
            from oandapyV20.endpoints.positions import PositionClose
            logger.info(f"🚀 [CLOSE DEBUG] Executing OANDA PositionClose request:")
            logger.info(f"🚀 [CLOSE DEBUG]   Account: {self.config.oanda_account_id}")
            logger.info(f"🚀 [CLOSE DEBUG]   Instrument: '{symbol}'")
            logger.info(f"🚀 [CLOSE DEBUG]   Data: {close_data}")
            
            close_request = PositionClose(
                accountID=self.config.oanda_account_id,
                instrument=symbol,
                data=close_data
            )
            
            logger.info(f"⏳ [CLOSE DEBUG] Sending request to OANDA...")
            close_response = await self.robust_oanda_request(close_request)
            logger.info(f"📨 [CLOSE DEBUG] OANDA response received: {close_response}")
            
            if close_response and 'longOrderCreateTransaction' in close_response:
                # Long position closed
                transaction = close_response['longOrderCreateTransaction']
                logger.info(f"✅ Long position closed: {transaction.get('units')} units at {transaction.get('price')}")
                return True, {
                    "transaction_id": transaction.get('id'),
                    "price": transaction.get('price'),
                    "units": transaction.get('units'),
                    "side": "long"
                }
            elif close_response and 'shortOrderCreateTransaction' in close_response:
                # Short position closed
                transaction = close_response['shortOrderCreateTransaction']
                logger.info(f"✅ Short position closed: {transaction.get('units')} units at {transaction.get('price')}")
                return True, {
                    "transaction_id": transaction.get('id'),
                    "price": transaction.get('price'),
                    "units": transaction.get('units'),
                    "side": "short"
                }
            else:
                # Check for errors
                if 'longOrderRejectTransaction' in close_response:
                    error_msg = close_response['longOrderRejectTransaction'].get('rejectReason', 'Unknown error')
                    logger.error(f"Long order rejected: {error_msg}")
                    return False, {"error": f"Long order rejected: {error_msg}"}
                elif 'shortOrderRejectTransaction' in close_response:
                    error_msg = close_response['shortOrderRejectTransaction'].get('rejectReason', 'Unknown error')
                    logger.error(f"Short order rejected: {error_msg}")
                    return False, {"error": f"Short order rejected: {error_msg}"}
                else:
                    logger.error(f"Unexpected close response: {close_response}")
                    return False, {"error": "Unexpected close response"}
                    
        except Exception as e:
            logger.error(f"Failed to close position {symbol}: {e}")
            return False, {"error": str(e)}

    # async def analyze_market_conditions(self, symbol: str) -> Dict[str, Any]: