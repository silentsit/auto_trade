import asyncio
import logging
import uuid
from typing import Any, Dict, Optional

from oandapyV20 import V20Error

from config import settings
from oanda_service import OandaService
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from technical_analysis import get_atr
from utils import (
    get_module_logger,
    format_symbol_for_oanda,
    calculate_position_size,
    TV_FIELD_MAP
)
from position_journal import position_journal

logger = get_module_logger(__name__)

class AlertHandler:
    """
    Orchestrates the processing of trading alerts by coordinating with various services
    like the OANDA service, position tracker, and risk manager.
    """
    def __init__(
        self,
        oanda_service: OandaService,
        position_tracker: PositionTracker,
        db_manager, # Kept for potential future use
        risk_manager: EnhancedRiskManager
    ):
        """Initializes the AlertHandler with all required components."""
        self.oanda_service = oanda_service
        self.position_tracker = position_tracker
        self.risk_manager = risk_manager
        self.db_manager = db_manager # The db_manager is managed via the position_tracker
        self._lock = asyncio.Lock()
        self._started = False
        logger.info("✅ AlertHandler initialized with all components.")

    async def start(self):
        """Starts the alert handler."""
        # In a more complex system, this might start background monitoring tasks.
        self._started = True
        logger.info("✅ AlertHandler started and ready to process alerts.")

    async def stop(self):
        """Stops the alert handler."""
        self._started = False
        logger.info("🛑 AlertHandler stopped.")

    def _standardize_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Standardizes the incoming alert data using TV_FIELD_MAP."""
        standardized_data = alert_data.copy()
        for tv_field, expected_field in TV_FIELD_MAP.items():
            if tv_field in standardized_data:
                standardized_data[expected_field] = standardized_data.pop(tv_field)
        
        # Ensure 'symbol' is in OANDA format (e.g., EUR_USD)
        if 'symbol' in standardized_data:
            standardized_data['symbol'] = format_symbol_for_oanda(standardized_data['symbol'])
            
        # Standardize action to uppercase
        if 'action' in standardized_data:
            standardized_data['action'] = standardized_data['action'].upper()

        return standardized_data

    async def process_alert(self, raw_alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for processing an alert."""
        alert_id = str(uuid.uuid4())
        logger.info(f"--- Processing Alert ID: {alert_id} ---")
        
        if not self._started:
            logger.error("Cannot process alert: Handler is not started.")
            return {"status": "error", "message": "Handler not started"}

        async with self._lock:
            alert = self._standardize_alert(raw_alert_data)
            action = alert.get("action")
            
            if action in ["BUY", "SELL"]:
                return await self._handle_open_position(alert, alert_id)
            elif action == "CLOSE":
                return await self._handle_close_position(alert, alert_id)
            else:
                logger.warning(f"Invalid action '{action}' in alert.")
                return {"status": "error", "message": f"Invalid action: {action}"}

    async def _handle_open_position(self, alert: Dict[str, Any], alert_id: str) -> Dict[str, Any]:
        """Handles the logic for opening a new position."""
        symbol = alert.get("symbol")
        action = alert.get("action")
        risk_percent = float(alert.get("risk_percent", settings.trading.max_risk_per_trade))
        
        try:
            # 1. Risk Pre-Check
            is_allowed, reason = await self.risk_manager.is_trade_allowed(risk_percentage=risk_percent / 100.0, symbol=symbol)
            if not is_allowed:
                logger.warning(f"Trade rejected by Risk Manager: {reason}")
                return {"status": "rejected", "reason": reason, "alert_id": alert_id}

            # 2. Get necessary market data
            account_balance = await self.oanda_service.get_account_balance()
            entry_price = await self.oanda_service.get_current_price(symbol, action)
            df = await self.oanda_service.get_historical_data(symbol, count=50, granularity="H1") # Using H1 for stable ATR
            atr = get_atr(df)

            if not all([account_balance, entry_price, atr > 0]):
                logger.error("Failed to get required market data for trade.")
                return {"status": "error", "message": "Market data unavailable."}

            # 3. Calculate Position Size and Stop Loss
            leverage = get_instrument_leverage(symbol)
            position_size, sizing_info = calculate_position_size(
                symbol, entry_price, risk_percent, account_balance, leverage
            )
            stop_loss_price = entry_price - (atr * 2) if action == "BUY" else entry_price + (atr * 2)
            
            # 4. Construct and Execute Order
            trade_payload = {
                "symbol": symbol,
                "action": action,
                "units": position_size,
                "stop_loss": stop_loss_price
            }
            success, result = await self.oanda_service.execute_trade(trade_payload)

            # 5. Record Keeping
            if success:
                position_id = f"{symbol}_{int(time.time())}"
                await self.position_tracker.record_position(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], stop_loss=stop_loss_price,
                    metadata={"alert_id": alert_id, "sizing_info": sizing_info}
                )
                await position_journal.record_entry(
                    position_id=position_id, symbol=symbol, action=action,
                    timeframe=alert.get("timeframe", "N/A"), entry_price=result['fill_price'],
                    size=result['units'], strategy=alert.get("strategy", "N/A"),
                    stop_loss=stop_loss_price
                )
                logger.info(f"✅ Successfully opened position {position_id} for {symbol}.")
                return {"status": "success", "position_id": position_id, "result": result}
            else:
                logger.error(f"Failed to execute trade: {result.get('error')}")
                return {"status": "error", "message": "Trade execution failed", "details": result}

        except Exception as e:
            logger.error(f"Unhandled exception in _handle_open_position: {e}", exc_info=True)
            return {"status": "error", "message": "An internal error occurred."}

    async def _handle_close_position(self, alert: Dict[str, Any], alert_id: str) -> Dict[str, Any]:
        """Handles the logic for closing an existing position."""
        symbol = alert.get("symbol")
        
        try:
            # 1. Find the open position for the symbol
            position = await self.position_tracker.get_position_by_symbol(symbol)
            if not position:
                logger.warning(f"Received CLOSE signal for {symbol}, but no open position found.")
                return {"status": "ignored", "reason": "No open position found"}

            position_id = position['position_id']
            action_to_close = "SELL" if position['action'] == "BUY" else "BUY"

            # 2. Get current price for closing
            exit_price = await self.oanda_service.get_current_price(symbol, action_to_close)
            if not exit_price:
                 logger.error(f"Could not get exit price for {symbol}.")
                 return {"status": "error", "message": "Market data unavailable."}

            # 3. Construct and Execute Close Order
            close_payload = {
                "symbol": symbol,
                "action": action_to_close,
                "units": position['size']
            }
            success, result = await self.oanda_service.execute_trade(close_payload)

            # 4. Record Keeping
            if success:
                close_result = await self.position_tracker.close_position(position_id, exit_price, "Signal")
                await position_journal.record_exit(
                    position_id=position_id, exit_price=exit_price,
                    exit_reason="Signal", pnl=close_result.position_data.get('pnl', 0)
                )
                logger.info(f"✅ Successfully closed position {position_id} for {symbol}.")
                return {"status": "success", "position_id": position_id, "result": result}
            else:
                logger.error(f"Failed to close trade: {result.get('error')}")
                return {"status": "error", "message": "Close trade execution failed", "details": result}

        except Exception as e:
            logger.error(f"Unhandled exception in _handle_close_position: {e}", exc_info=True)
            return {"status": "error", "message": "An internal error occurred."}
