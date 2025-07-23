#
# file: oanda_service.py
#
import oandapyV20
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.instruments import InstrumentsCandles
from oandapyV20.endpoints.trades import TradeCRCDO
from pydantic import SecretStr
import asyncio
import logging
import random
import time
import pandas as pd
from datetime import datetime, timedelta
from config import config
from utils import _get_simulated_price, get_atr, get_instrument_leverage, round_position_size, get_position_size_limits, validate_trade_inputs, MarketDataUnavailableError, calculate_position_size
from risk_manager import EnhancedRiskManager
from typing import Dict

logger = logging.getLogger("OandaService")

class OandaService:
    def __init__(self, config_obj=None):
        self.config = config_obj or config
        self.oanda = None
        self.last_successful_request = None
        self.connection_errors_count = 0
        self.last_health_check = None
        self.health_check_interval = 300  # 5 minutes
        self.circuit_breaker_failures = 0
        self.circuit_breaker_threshold = 10
        self.circuit_breaker_reset_time = None
        self.session_created_at = None
        self._init_oanda_client()

    def _init_oanda_client(self):
        try:
            access_token = self.config.oanda_access_token
            if isinstance(access_token, object) and hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            
            self.oanda = oandapyV20.API(
                access_token=access_token,
                environment=self.config.oanda_environment
            )
            
            self.session_created_at = datetime.now()
            self.connection_errors_count = 0
            self.circuit_breaker_failures = 0
            self.circuit_breaker_reset_time = None
            
            logger.info(f"OANDA client initialized in OandaService for {self.config.oanda_environment} environment")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA client: {e}")
            self.oanda = None

    async def _health_check(self) -> bool:
        """Perform a lightweight health check to ensure connection is alive"""
        try:
            if not self.oanda:
                return False
                
            # Skip if we've done a health check recently
            if (self.last_health_check and 
                datetime.now() - self.last_health_check < timedelta(seconds=self.health_check_interval)):
                return True
            
            # Perform lightweight account details request
            account_request = AccountDetails(accountID=self.config.oanda_account_id)
            start_time = time.time()
            
            response = self.oanda.request(account_request)
            response_time = time.time() - start_time
            
            self.last_health_check = datetime.now()
            self.last_successful_request = datetime.now()
            
            logger.debug(f"Health check passed in {response_time:.3f}s")
            return True
            
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

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
        """Warm up the connection with a simple request"""
        try:
            if await self._health_check():
                logger.debug("Connection warmed successfully")
                return True
        except Exception as e:
            logger.warning(f"Connection warming failed: {e}")
        return False

    async def initialize(self):
        """Initialize the OandaService."""
        await self._warm_connection()
        logger.info("OANDA service initialized.")

    async def stop(self):
        """Stop the OandaService."""
        logger.info("OANDA service is shutting down.")

    async def robust_oanda_request(self, request, max_retries: int = 5, initial_delay: float = 3.0):
        """Enhanced OANDA API request with sophisticated retry logic"""
        
        # Check circuit breaker
        if self._should_circuit_break():
            raise Exception("Circuit breaker is active - too many consecutive failures")
        
        if not self.oanda:
            self._init_oanda_client()
            if not self.oanda:
                raise Exception("OANDA client not initialized")
        
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
                response = self.oanda.request(request)
                
                # Success - reset failure counters
                self.last_successful_request = datetime.now()
                self.connection_errors_count = 0
                self.circuit_breaker_failures = 0
                return response
                
            except Exception as e:
                last_exception = e
                is_conn_error = is_connection_error(e)
                is_final_attempt = attempt == max_retries - 1
                
                if is_conn_error:
                    self.connection_errors_count += 1
                    self.circuit_breaker_failures += 1
                
                if is_final_attempt:
                    logger.error(f"OANDA request failed after {max_retries} attempts: {e}")
                    raise Exception(f"OANDA request failed after {max_retries} attempts: {e}")
                
                # Calculate delay with jitter for connection errors
                if is_conn_error:
                    # Implement exponential backoff with full jitter
                    delay = initial_delay * (2 ** attempt)
                    jitter = random.uniform(0, delay * 0.1) # Add up to 10% jitter
                    final_delay = delay + jitter
                    logger.warning(f"OANDA connection error attempt {attempt + 1}/{max_retries}, retrying in {final_delay:.2f}s: {e}")
                    
                    # Reinitialize client for connection errors after 1st attempt
                    if attempt >= 0:  # Reinitialize immediately for connection issues
                        logger.info("Reinitializing OANDA client after connection error")
                        self._init_oanda_client()
                        
                        # Additional wait for severe connection issues
                        if 'remote end closed connection' in str(e).lower():
                            logger.info("Adding extra delay for remote disconnection error")
                            final_delay += 2.0
                else:
                    # Shorter delays for other errors
                    delay = initial_delay * (1.5 ** attempt)
                    logger.warning(f"OANDA request error attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")
                    final_delay = delay
                
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
                    reason = price_data.get('reason')
                    if reason and 'MARKET_HALTED' in reason:
                         logger.error(f"Market for {symbol} is currently halted.")
                         raise MarketDataUnavailableError(f"Market for {symbol} is halted.")
                    logger.warning(f"Price for {symbol} is not tradeable at the moment. Data: {price_data}")

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
        try:
            account_request = AccountDetails(accountID=self.config.oanda_account_id)
            response = await self.robust_oanda_request(account_request)
            balance = float(response['account']['balance'])
            logger.info(f"Successfully fetched account balance: ${balance:.2f}")
            return balance
        except Exception as e:
            logger.error(f"Failed to get account balance after all retries: {e}")
            raise MarketDataUnavailableError(f"Could not fetch account balance: {e}")

    async def check_crypto_availability(self) -> Dict[str, bool]:
        """Check which crypto instruments are available in the current OANDA environment"""
        try:
            from oandapyV20.endpoints.accounts import AccountInstruments
            
            # Get available instruments
            request = AccountInstruments(accountID=self.config.oanda_account_id)
            response = await self.robust_oanda_request(request)
            
            available_instruments = []
            if 'instruments' in response:
                available_instruments = [inst['name'] for inst in response['instruments']]
            
            # Check crypto availability
            crypto_symbols = {
                'BTC_USD': 'BTC_USD' in available_instruments,
                'ETH_USD': 'ETH_USD' in available_instruments,
                'LTC_USD': 'LTC_USD' in available_instruments,
                'XRP_USD': 'XRP_USD' in available_instruments,
                'BCH_USD': 'BCH_USD' in available_instruments
            }
            
            logger.info(f"Crypto availability check: {crypto_symbols}")
            return crypto_symbols
            
        except Exception as e:
            logger.error(f"Failed to check crypto availability: {e}")
            return {
                'BTC_USD': False,
                'ETH_USD': False,
                'LTC_USD': False,
                'XRP_USD': False,
                'BCH_USD': False
            }

    async def is_crypto_supported(self, symbol: str) -> bool:
        """Check if a specific crypto symbol is supported"""
        crypto_availability = await self.check_crypto_availability()
        return crypto_availability.get(symbol, False)

    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        symbol = payload.get("symbol")
        action = payload.get("action")
        risk_percent = payload.get("risk_percent", 1.0)
        signal_price = payload.get("signal_price")
        
        # INSTITUTIONAL FIX: Check crypto availability before attempting trade
        if any(crypto in symbol.upper() for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH']):
            is_supported = await self.is_crypto_supported(symbol)
            if not is_supported:
                logger.warning(f"Crypto symbol {symbol} not supported in current OANDA environment")
                return False, {
                    "error": f"Crypto trading not available for {symbol} in {self.config.oanda_environment} environment",
                    "suggestion": "Switch to live environment or use supported forex pairs"
                }
        
        if signal_price is None:
            logger.info("No signal_price provided in payload; falling back to current market price.")
            signal_price = await self.get_current_price(symbol, action)
        account_balance = await self.get_account_balance()
        try:
            current_price = await self.get_current_price(symbol, action)
        except MarketDataUnavailableError as e:
            logger.error(f"Trade execution aborted: {e}")
            return False, {"error": str(e)}
        atr = None  # Always define atr at the top
        stop_loss = payload.get("stop_loss")
        if stop_loss is None:
            try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"), oanda_service=self)
            except MarketDataUnavailableError as e:
                logger.error(f"Trade execution aborted: {e}")
                return False, {"error": str(e)}
            stop_loss = signal_price - (atr * self.config.atr_stop_loss_multiplier) if action.upper() == "BUY" else signal_price + (atr * self.config.atr_stop_loss_multiplier)
        
        stop_distance = abs(signal_price - stop_loss)
        if stop_distance <= 0:
            logger.error(f"Trade execution aborted: Invalid stop loss distance: {stop_distance}")
            return False, {"error": "Invalid stop loss distance"}
        
        # INSTITUTIONAL FIX: Use unified risk-based position sizing
        target_percent = getattr(self.config, 'allocation_percent', 10.0)
        leverage = get_instrument_leverage(symbol)
        
        # Use our new unified position sizing function for consistency
        position_size, sizing_info = await calculate_position_size(
            symbol=symbol,
            entry_price=signal_price,
            risk_percent=target_percent,
            account_balance=account_balance,
            leverage=leverage,
            stop_loss_price=stop_loss,
            timeframe=payload.get("timeframe", "H1")
        )
        if position_size <= 0:
            logger.error(f"Trade execution aborted: Calculated position size is zero or negative")
            return False, {"error": "Calculated position size is zero or negative"}
        raw_position_size = position_size
        position_size = round_position_size(symbol, position_size)
        logger.info(f"[INSTITUTIONAL SIZING] {symbol}: Raw={raw_position_size}, Rounded={position_size}, Leverage={leverage}, Target%={target_percent}")
        if position_size <= 0:
            logger.error(f"Trade execution aborted: Rounded position size is zero")
            return False, {"error": "Rounded position size is zero"}
        min_units, max_units = get_position_size_limits(symbol)
        if atr is None:
            try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"), oanda_service=self)
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
        units_value = int(position_size) if action.upper() == "BUY" else -int(position_size)
        order_data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(units_value),
                "timeInForce": "FOK"
            }
        }
        order_request = OrderCreate(
            accountID=self.config.oanda_account_id,
            data=order_data
        )
        response = await self.robust_oanda_request(order_request)
        if 'orderFillTransaction' in response:
            fill_info = response['orderFillTransaction']
            fill_price = float(fill_info.get('price', current_price))
            if action.upper() == "BUY":
                slippage = fill_price - signal_price
            else:
                slippage = signal_price - fill_price
            logger.info(
                f"Trade execution for {symbol}: "
                f"Account Balance=${account_balance:.2f}, "
                f"Risk%={risk_percent:.2f}, "
                f"Signal Price={signal_price}, Fill Price={fill_price}, Slippage={slippage:.5f}, "
                f"Entry={current_price}, Stop={stop_loss}, "
                f"Position Size={position_size}"
            )
            return True, {
                "success": True,
                "fill_price": fill_price,
                "units": abs(float(fill_info.get('units', position_size))),
                "transaction_id": fill_info.get('id'),
                "symbol": symbol,
                "action": action,
                "signal_price": signal_price,
                "slippage": slippage
            }
        else:
            logger.error(f"Order not filled for {symbol}: {response}")
            return False, {"error": "Order not filled", "response": response} 
    
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
            if stop_loss is not None:
                data["stopLoss"] = {
                    "timeInForce": "GTC",
                    "price": str(stop_loss)
                }
            if take_profit is not None:
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
