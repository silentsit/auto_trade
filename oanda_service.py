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
from utils import _get_simulated_price, get_atr, get_instrument_leverage, round_position_size, get_position_size_limits, validate_trade_inputs, MarketDataUnavailableError, calculate_position_size, round_price_for_instrument, validate_tp_sl, enforce_min_tp_distance, validate_tp_with_slippage_buffer
from risk_manager import EnhancedRiskManager
from typing import Dict, Any

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
            
            # INSTITUTIONAL FIX: Try to execute crypto trade, fallback to logging if not supported
            try:
                # Test if we can get price data (this indicates crypto is supported)
                test_price = await self.get_current_price(symbol, action)
                if test_price is None:
                    raise Exception("Price data unavailable")
                logger.info(f"✅ Crypto {symbol} is tradeable - price: {test_price}")
            except Exception as crypto_error:
                logger.warning(f"Crypto symbol {symbol} not supported: {crypto_error}")
                
                # Log to crypto handler for tracking
                from crypto_signal_handler import crypto_handler
                crypto_handler.log_crypto_signal({
                "symbol": symbol,
                "action": action,
                    "risk_percent": payload.get("risk_percent", 1.0),
                    "environment": self.config.oanda_environment,
                    "reason": f"crypto_not_supported: {str(crypto_error)}"
                })
                
                return False, {
                    "error": f"Crypto trading not available for {symbol}",
                    "reason": str(crypto_error),
                    "suggestion": "Check OANDA crypto availability or use supported forex pairs",
                    "logged": "Signal logged for future crypto trading setup"
                }

        # Get current price for the trade
        try:
            current_price = await self.get_current_price(symbol, action)
        except Exception as e:
            logger.error(f"Failed to get current price for {symbol}: {e}")
            return False, {"error": f"Failed to get current price: {e}"}

        # Defensive rounding
        if stop_loss is not None:
            stop_loss = round_price_for_instrument(stop_loss, symbol)
        if take_profit is not None:
            take_profit = round_price_for_instrument(take_profit, symbol)

        # Enforce minimum TP distance (25 pips: 0.0025 for non-JPY, 0.25 for JPY pairs)
        orig_tp = take_profit
        take_profit = enforce_min_tp_distance(symbol, current_price, take_profit, action)
        if orig_tp != take_profit:
            logger.warning(f"TP adjusted to enforce 25-pip min distance: original={orig_tp}, adjusted={take_profit}")

        # Apply slippage buffer to prevent TAKE_PROFIT_ON_FILL_LOSS
        take_profit = validate_tp_with_slippage_buffer(current_price, take_profit, action, symbol)

        # Validate TP/SL logic
        try:
            validate_tp_sl(current_price, take_profit, stop_loss, action)
        except ValueError as e:
            logger.error(f"TP/SL validation failed: {e}")
            return False, {"error": f"Invalid TP/SL configuration: {e}"}

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
            data["order"]["units"] = str(int(current_units))
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