import oandapyV20
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.pricing import PricingInfo
from pydantic import SecretStr
import asyncio
import logging
from config import config
from utils import _get_simulated_price, get_atr, get_instrument_leverage, round_position_size, get_position_size_limits, validate_trade_inputs, MarketDataUnavailableError
from risk_manager import EnhancedRiskManager

logger = logging.getLogger("OandaService")

class OandaService:
    def __init__(self, config_obj=None):
        self.config = config_obj or config
        self.oanda = None
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
            logger.info("OANDA client initialized in OandaService")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA client: {e}")
            self.oanda = None

    async def robust_oanda_request(self, request, max_retries: int = 3, initial_delay: float = 1.0):
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
                    raise Exception(f"OANDA request failed after {max_retries} attempts: {e}")
                await asyncio.sleep(initial_delay * (2 ** attempt))
                logger.warning(f"OANDA request attempt {attempt + 1} failed, retrying: {e}")

    async def get_current_price(self, symbol: str, action: str) -> float:
        try:
            pricing_request = PricingInfo(
                accountID=self.config.oanda_account_id,
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
            account_request = AccountDetails(accountID=self.config.oanda_account_id)
            response = await self.robust_oanda_request(account_request)
            return float(response['account']['balance'])
        except Exception as e:
            logger.error(f"Error getting account balance: {e}")
            return 10000.0

    async def execute_trade(self, payload: dict) -> tuple[bool, dict]:
        symbol = payload.get("symbol")
        action = payload.get("action")
        risk_percent = payload.get("risk_percent", 1.0)
        signal_price = payload.get("signal_price")
        if signal_price is None:
            logger.warning("No signal_price provided in payload; falling back to current market price.")
            signal_price = await self.get_current_price(symbol, action)
        account_balance = await self.get_account_balance()
        try:
            current_price = await self.get_current_price(symbol, action)
        except MarketDataUnavailableError as e:
            logger.error(f"Trade execution aborted: {e}")
            return False, {"error": str(e)}
        stop_loss = payload.get("stop_loss")
        if stop_loss is None:
            try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"))
            except MarketDataUnavailableError as e:
                logger.error(f"Trade execution aborted: {e}")
                return False, {"error": str(e)}
            stop_loss = signal_price - (atr * self.config.atr_stop_loss_multiplier) if action.upper() == "BUY" else signal_price + (atr * self.config.atr_stop_loss_multiplier)
        else:
            atr = None
        stop_distance = abs(signal_price - stop_loss)
        if stop_distance <= 0:
            logger.error(f"Trade execution aborted: Invalid stop loss distance: {stop_distance}")
            return False, {"error": "Invalid stop loss distance"}
        target_percent = getattr(self.config, 'allocation_percent', 10.0)
        leverage = get_instrument_leverage(symbol)
        equity = account_balance
        irm = EnhancedRiskManager()
        position_size = irm.calculate_position_units(
            equity=equity,
            target_percent=target_percent,
            leverage=leverage,
            current_price=signal_price
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