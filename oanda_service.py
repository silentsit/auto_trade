#
# file: oanda_service.py
#
import oandapyV20
import pandas as pd
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.instruments import InstrumentsCandles # FIX: Import InstrumentsCandles
from pydantic import SecretStr
import asyncio
import logging
from config import config
# FIX: The circular import is now resolved. This file no longer imports from technical_analysis.
from utils import _get_simulated_price, get_instrument_leverage, round_position_size, get_position_size_limits, validate_trade_inputs, MarketDataUnavailableError
from risk_manager import EnhancedRiskManager

logger = logging.getLogger("OandaService")

class OandaService:
    def __init__(self, config_obj=None):
        self.config = config_obj or config
        self.oanda = None
        self._init_oanda_client()

    def _init_oanda_client(self):
        try:
            access_token = self.config.oanda.access_token
            if isinstance(access_token, SecretStr):
                access_token = access_token.get_secret_value()
            self.oanda = oandapyV20.API(
                access_token=access_token,
                environment=self.config.oanda.environment
            )
            logger.info("OANDA client initialized in OandaService")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA client: {e}")
            self.oanda = None

    async def robust_oanda_request(self, request, max_retries: int = 5, initial_delay: float = 3.0):
        # ... (no changes to this method) ...
        if not self.oanda:
            self._init_oanda_client()
            if not self.oanda:
                raise Exception("OANDA client not initialized")
        
        def is_connection_error(exception):
            from urllib3.exceptions import ProtocolError
            from http.client import RemoteDisconnected
            from requests.exceptions import ConnectionError, Timeout
            import socket
            
            connection_errors = (ConnectionError, RemoteDisconnected, ProtocolError, Timeout, socket.timeout, socket.error, OSError)
            if isinstance(exception, connection_errors):
                return True
            error_str = str(exception).lower()
            connection_indicators = ['connection aborted', 'remote end closed connection', 'connection reset', 'timeout', 'network is unreachable', 'connection refused', 'broken pipe', 'connection timed out']
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
                    raise Exception(f"OANDA request failed after {max_retries} attempts: {e}")
                delay = initial_delay * (2 ** attempt) + (attempt * 0.5) if is_conn_error else initial_delay * (1.5 ** attempt)
                log_level = "warning" if is_conn_error else "info"
                getattr(logger, log_level)(f"OANDA request error on attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")
                if is_conn_error and attempt >= 1:
                    logger.info("Reinitializing OANDA client after connection error")
                    self._init_oanda_client()
                await asyncio.sleep(delay)

    # FIX: Add a new method to fetch historical data as a DataFrame
    async def get_historical_data(self, symbol: str, count: int, granularity: str) -> pd.DataFrame:
        """Fetches historical candle data and returns it as a pandas DataFrame."""
        params = {
            "count": count,
            "granularity": granularity
        }
        request = InstrumentsCandles(instrument=symbol, params=params)
        try:
            response = await self.robust_oanda_request(request)
            data = []
            for candle in response.get('candles', []):
                time = pd.to_datetime(candle['time'])
                volume = candle['volume']
                mid_prices = candle.get('mid', {})
                if 'o' in mid_prices and 'h' in mid_prices and 'l' in mid_prices and 'c' in mid_prices:
                    data.append([
                        time,
                        float(mid_prices['o']),
                        float(mid_prices['h']),
                        float(mid_prices['l']),
                        float(mid_prices['c']),
                        volume
                    ])
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df.set_index('time', inplace=True)
            return df
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return pd.DataFrame() # Return empty dataframe on error

    async def get_current_price(self, symbol: str, action: str) -> float:
        try:
            pricing_request = PricingInfo(
                accountID=self.config.oanda.account_id,
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
        return _get_simulated_price(symbol, action)

    async def get_account_balance(self, use_fallback: bool = False) -> float:
        if use_fallback:
            return 10000.0
        try:
            account_request = AccountDetails(accountID=self.config.oanda.account_id)
            response = await self.robust_oanda_request(account_request)
            return float(response['account']['balance'])
        except Exception as e:
            logger.error(f"Error getting account balance: {e}")
            return 10000.0

    # FIX: The execute_trade method is simplified.
    # It no longer calculates the stop_loss itself. It expects a complete order in the payload.
    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        """Executes a trade based on a fully-formed payload."""
        symbol = payload.get("symbol")
        action = payload.get("action")
        units = payload.get("units")
        stop_loss_price = payload.get("stop_loss")

        if not all([symbol, action, units]):
            return False, {"error": "Invalid trade payload: symbol, action, and units are required."}

        units_value = int(units) if action.upper() == "BUY" else -int(units)
        
        order_definition = {
            "type": "MARKET",
            "instrument": symbol,
            "units": str(units_value),
            "timeInForce": "FOK" # Fill or Kill
        }
        
        if stop_loss_price:
            order_definition["stopLossOnFill"] = {
                "price": str(round(stop_loss_price, 5))
            }
        
        order_request = OrderCreate(
            accountID=self.config.oanda.account_id,
            data={"order": order_definition}
        )
        
        try:
            response = await self.robust_oanda_request(order_request)
            if 'orderFillTransaction' in response:
                fill_info = response['orderFillTransaction']
                fill_price = float(fill_info.get('price'))
                filled_units = abs(float(fill_info.get('units')))
                
                logger.info(f"✅ TRADE EXECUTED for {symbol}: {action} {filled_units} units @ {fill_price}")
                return True, {
                    "success": True,
                    "fill_price": fill_price,
                    "units": filled_units,
                    "transaction_id": fill_info.get('id'),
                }
            else:
                logger.error(f"Order not filled for {symbol}: {response}")
                return False, {"error": "Order not filled", "response": response}
        except Exception as e:
            logger.error(f"Exception during trade execution for {symbol}: {e}")
            return False, {"error": str(e)}
