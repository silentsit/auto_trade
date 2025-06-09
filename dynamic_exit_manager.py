import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List

from utils import logger, get_module_logger, get_atr, get_instrument_type, get_atr_multiplier
from config import config

class DynamicExitManager:
    """
    Manages dynamic exits based on Lorentzian classifier market regimes.
    Adjusts stop losses, take profits, and trailing stops based on market conditions.
    """
    pass  # Placeholder class

class HybridExitManager:
    """
    Implements the "Hybrid Volatility + Momentum Conditional Exit":
     1) 20% off at 0.75 R,
     2) 40% at 1.5 R,
     3) 40% at 2.5 R (remaining runner),
     4) Volatility-adaptive trailing on the runner,
     5) RSI(5)-based early exit if momentum flips,
     6) Hard time stop (12 h for 1H, etc.).
    """

    def __init__(self, position_tracker=None):
        self.position_tracker = position_tracker
        self.lorentzian_classifier = None  # Will be assigned by EnhancedAlertHandler
        self.exit_levels: Dict[str, Any] = {}

        # Tier splits & percentages for each timeframe
        self.TIER_SPLITS = {
            "15M": {
                "early_partial": {"threshold": 0.75, "pct": 20},
                "mid_tp":       {"threshold": 1.5,  "pct": 40},
                "final_tp":     {"threshold": 2.5,  "pct": 40},
            },
            "1H": {
                "early_partial": {"threshold": 1.0,  "pct": 40},
                "mid_tp":       {"threshold": 2.0,  "pct": 30},
                "final_tp":     {"threshold": 3.0,  "pct": 30}, 
            },
            "4H": {
                "early_partial": {"threshold": 1.0,  "pct": 40},
                "mid_tp":       {"threshold": 2.0,  "pct": 30},
                "final_tp":     {"threshold": 3.0,  "pct": 30},
            },
            "1D": {
                "early_partial": {"threshold": 1.0,  "pct": 40},
                "mid_tp":       {"threshold": 2.0,  "pct": 30},
                "final_tp":     {"threshold": 3.0,  "pct": 30},
            },
        }

        # Volatility‐adaptive trailing (R-multipliers conditional on regime)
        self.TRAILING_SETTINGS = {
            "15M": {
                "strong": {"multiplier": 2.0},
                "weak":   {"multiplier": 1.0},
            },
            "1H": {
                "strong": {"multiplier": 2.0},
                "weak":   {"multiplier": 1.0},
            },
            "4H": {
                "strong": {"multiplier": 2.0},
                "weak":   {"multiplier": 1.0},
            },
            "1D": {
                "strong": {"multiplier": 2.0},
                "weak":   {"multiplier": 1.0},
            },
        }

        # Hard time stops (hours) if no TP is hit
        self.TIME_STOPS = {
            "15M": 2,   # exit after 2 hours if still open
            "1H": 12,   # exit after 12 hours
            "4H": 36,   # exit after 36 hours (~1.5 days)
            "1D": 72,   # exit after 72 hours (3 days)
        }

    async def start(self):
        # No special startup logic required
        return True

    async def _compute_rsi5(self, price_history: list) -> float:
        """
        Compute a 5-period RSI.
        Expects price_history to be a list of 6 recent close prices
        (to calculate 5 price differences).
        """
        if len(price_history) < 6:
            logger.warning(f"HybridExitManager._compute_rsi5: Not enough price history ({len(price_history)}) for RSI(5). Returning 50.0")
            return 50.0  # Neutral if insufficient history
        gains = 0.0 
        losses = 0.0
        for i in range(1, len(price_history)):
            delta = price_history[i] - price_history[i-1]
            if delta > 0:
                gains += delta
            else:
                losses -= delta
        avg_gain = gains / 5.0
        avg_loss = losses / 5.0
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    async def initialize_hybrid_exits(
        self,
        position_id: str,
        symbol: str,
        entry_price: float,
        position_direction: str, # Should be "BUY" or "SELL"
        stop_loss: float,        # Initial calculated stop loss
        atr: float,              # ATR value at the time of entry
        timeframe: str,
        regime: str              # Market regime at the time of entry
    ) -> bool:
        """
        Create the exit plan for a newly opened position based on the hybrid strategy.
        Calculates tiered take profit levels based on R-multiples and percentages of original size.
        Stores all necessary state for the HybridExitManager to manage this position.
        """
        logger.info(f"HybridExitManager: Initializing exits for {position_id} ({symbol} {position_direction} @ {entry_price}). SL: {stop_loss}, ATR: {atr}, TF: {timeframe}, Regime: {regime}")
        if stop_loss is None:
            logger.error(f"HybridExitManager.initialize_hybrid_exits: stop_loss is None for {position_id}. Cannot initialize.")
            return False
        if atr <= 1e-9:
            logger.error(f"HybridExitManager.initialize_hybrid_exits: ATR value ({atr}) is too small or invalid for {position_id}.")
            return False
        risk_distance = abs(entry_price - stop_loss)
        if risk_distance <= 1e-9:
            logger.error(f"HybridExitManager.initialize_hybrid_exits: Risk distance for {position_id} is zero or negligible ({risk_distance}). Entry: {entry_price}, SL: {stop_loss}.")
            return False
        if not self.position_tracker:
            logger.error(f"HybridExitManager.initialize_hybrid_exits: PositionTracker not available for {position_id}.")
            return False
        pos_info_at_init = await self.position_tracker.get_position_info(position_id)
        if not pos_info_at_init:
            logger.error(f"HybridExitManager.initialize_hybrid_exits: Could not retrieve position info for {position_id} from tracker.")
            return False
        original_size_at_entry = pos_info_at_init.get("size")
        if not isinstance(original_size_at_entry, (float, int)) or original_size_at_entry <= 1e-9:
            logger.error(f"HybridExitManager.initialize_hybrid_exits: Position {position_id} has invalid or zero original size ({original_size_at_entry}) from tracker.")
            return False
        tiers_config = self.TIER_SPLITS.get(timeframe, self.TIER_SPLITS["1H"])
        tp1_r_multiple = tiers_config["early_partial"]["threshold"]
        tp2_r_multiple = tiers_config["mid_tp"]["threshold"]
        tp3_r_multiple = tiers_config["final_tp"]["threshold"]
        tp1_price_offset = tp1_r_multiple * risk_distance
        tp2_price_offset = tp2_r_multiple * risk_distance
        tp3_price_offset = tp3_r_multiple * risk_distance
        if position_direction.upper() == "BUY":
            tp1_price = entry_price + tp1_price_offset
            tp2_price = entry_price + tp2_price_offset
            tp3_price = entry_price + tp3_price_offset
        else:
            tp1_price = entry_price - tp1_price_offset
            tp2_price = entry_price - tp2_price_offset
            tp3_price = entry_price - tp3_price_offset
        units_for_tp1 = int( (tiers_config["early_partial"]["pct"] / 100.0) * original_size_at_entry )
        units_for_tp2 = int( (tiers_config["mid_tp"]["pct"] / 100.0) * original_size_at_entry )
        units_for_tp3_runner = int( (tiers_config["final_tp"]["pct"] / 100.0) * original_size_at_entry )
        total_tiered_units = units_for_tp1 + units_for_tp2 + units_for_tp3_runner
        if total_tiered_units > original_size_at_entry :
            units_for_tp3_runner -= (total_tiered_units - int(original_size_at_entry))
        elif total_tiered_units < original_size_at_entry and units_for_tp3_runner > 0 :
            units_for_tp3_runner += (int(original_size_at_entry) - total_tiered_units)
        self.exit_levels[position_id] = {
            "symbol": symbol,
            "direction": position_direction,
            "entry_price": entry_price,
            "initial_stop_loss": stop_loss,
            "current_active_stop": stop_loss,
            "atr_at_entry": atr,
            "timeframe": timeframe,
            "regime_at_entry": regime,
            "original_size_at_entry": original_size_at_entry,
            "risk_distance_at_entry": risk_distance,
            "tier_config": {
                "tp1_threshold_R": tp1_r_multiple,
                "tp2_threshold_R": tp2_r_multiple,
                "tp3_threshold_R": tp3_r_multiple,
            },
            "targets_and_units": {
                "tp1": {"price": round(tp1_price, 5), "units_to_close": units_for_tp1},
                "tp2": {"price": round(tp2_price, 5), "units_to_close": units_for_tp2},
                "tp3_runner": {"price": round(tp3_price, 5), "units_to_close": units_for_tp3_runner},
            },
            "status": {
                "tp1_hit": False,
                "rsi_exit_active": False,
                "rsi_exit_units_closed": 0,
                "tp2_hit": False,
                "tp3_runner_tier_hit": False,
                "runner_active": False,
                "trailing_stop_active_price": stop_loss,
                "time_stop_hit": False,
                "fully_closed": False
            },
            "entry_timestamp": datetime.now(timezone.utc),
            "time_stop_hours": self.TIME_STOPS.get(timeframe, self.TIME_STOPS["1H"])
        }
        logger.info(f"HybridExitManager: Successfully initialized exits for {position_id}. Orig Size: {original_size_at_entry:.4f}. RiskDist: {risk_distance:.5f}. TP1: {units_for_tp1} units @ {tp1_price:.5f}, TP2: {units_for_tp2} units @ {tp2_price:.5f}, TP3/Runner: {units_for_tp3_runner} units @ {tp3_price:.5f}.")
        logger.debug(f"HybridExitManager: Full state for {position_id}: {self.exit_levels[position_id]}")
        return True

    async def update_exits(
        self, position_id: str, current_price: float, recent_candles: list
    ):
        """
        Called every ~5 minutes by EnhancedAlertHandler's scheduled loop.
        Checks whether any of the partial exits or trailer/RSI/timeout should fire.
        """
        if position_id not in self.exit_levels:
            return
        data = self.exit_levels[position_id]
        pos_info = await self.position_tracker.get_position_info(position_id)
        if not pos_info:
            logger.warning(
                f"HybridExitManager: Position {position_id} not found in tracker for update_exits."
            )
            self.exit_levels.pop(position_id, None)
            return
        current_tracked_size = pos_info.get("size")
        if not isinstance(current_tracked_size, (float, int)):
            logger.error(
                f"HybridExitManager: Invalid size type ({type(current_tracked_size)}) for position {position_id} from tracker."
            )
            return
        if current_tracked_size <= 0:
            logger.info(
                f"HybridExitManager: Position {position_id} has zero or negative size ({current_tracked_size}) in tracker. Removing from internal state."
            )
            self.exit_levels.pop(position_id, None)
            return
        symbol = data["symbol"]
        direction = data["direction"]
        atr = data["atr_at_entry"]
        targets = data["targets_and_units"]
        status = data["status"]
        original_size_for_tiers = data.get("original_size_at_entry", current_tracked_size)
        if original_size_for_tiers <= 0:
            logger.warning(
                f"HybridExitManager: Original size for {position_id} is zero or negative. Cannot calculate partials."
            )
            return
        remaining_units = current_tracked_size
        if not status["tp1_hit"]:
            if (direction == "BUY" and current_price >= targets["tp1"]["price"]) or (
                direction == "SELL" and current_price <= targets["tp1"]["price"]
            ):
                qty_to_close_tier1 = int(0.20 * original_size_for_tiers)
                if (
                    qty_to_close_tier1 >= 1 and remaining_units > 0
                ):
                    await self._close_pct(
                        position_id, qty_to_close_tier1, current_price, "0.75R_partial"
                    )
                    status["tp1_hit"] = True
                    new_pos_info = await self.position_tracker.get_position_info(
                        position_id
                    )
                    remaining_units = new_pos_info.get("size", 0.0) if new_pos_info else 0.0
                    if remaining_units <= 0:
                        logger.info(
                            f"HybridExitManager: Position {position_id} fully closed after 0.75R partial."
                        )
                        self.exit_levels.pop(position_id, None)
                        return
        if (
            not status.get("rsi_exit_hit", False)
            and status["tp1_hit"]
            and not status["tp2_hit"]
            and not status.get("tp3_hit", False)
            and remaining_units > 0
        ):
            closes = [c.get("close") for c in recent_candles if c.get("close") is not None][
                -6:
            ]
            if len(closes) >= 6:
                rsi_val = await self._compute_rsi5(closes[-6:])
                if (direction == "BUY" and rsi_val < 40) or (
                    direction == "SELL" and rsi_val > 60
                ):
                    current_pos_info_before_rsi_exit = (
                        await self.position_tracker.get_position_info(position_id)
                    )
                    if (
                        current_pos_info_before_rsi_exit
                        and current_pos_info_before_rsi_exit.get("size", 0) > 0
                    ):
                        qty_to_close_rsi = int(
                            0.30 * original_size_for_tiers
                        )
                        if qty_to_close_rsi >= 1:
                            await self._close_pct(
                                position_id,
                                qty_to_close_rsi,
                                current_price,
                                "RSI_momentum_flip",
                            )
                            status["rsi_exit_hit"] = True
                            new_pos_info = await self.position_tracker.get_position_info(
                                position_id
                            )
                            remaining_units = (
                                new_pos_info.get("size", 0.0) if new_pos_info else 0.0
                            )
                            if remaining_units <= 0:
                                logger.info(
                                    f"HybridExitManager: Position {position_id} fully closed after RSI exit."
                                )
                                self.exit_levels.pop(position_id, None)
                                return
                    else:
                        logger.info(
                            f"HybridExitManager: RSI exit condition met for {position_id}, but position already closed or size is zero."
                        )
                        self.exit_levels.pop(
                            position_id, None
                        )
                        return
        
        if not status["tp2_hit"] and remaining_units > 0:
            if (direction == "BUY" and current_price >= targets["tp2"]["price"]) or (
                direction == "SELL" and current_price <= targets["tp2"]["price"]
            ):
                qty_to_close_tier2 = int(
                    0.40 * original_size_for_tiers
                )
                if qty_to_close_tier2 >= 1:
                    await self._close_pct(
                        position_id, qty_to_close_tier2, current_price, "1.5R_partial"
                    )
                    status["tp2_hit"] = True
                    new_pos_info = await self.position_tracker.get_position_info(
                        position_id
                    )
                    remaining_units = (
                        new_pos_info.get("size", 0.0) if new_pos_info else 0.0
                    )
                    if remaining_units <= 0:
                        logger.info(
                            f"HybridExitManager: Position {position_id} fully closed after 1.5R partial."
                        )
                        self.exit_levels.pop(position_id, None)
                        return
        
        if not status.get("tp3_hit", False) and remaining_units > 0:
            if (direction == "BUY" and current_price >= targets["tp3_runner"]["price"]) or (
                direction == "SELL" and current_price <= targets["tp3_runner"]["price"]
            ):
                initial_tier3_units = int(0.40 * original_size_for_tiers)
                qty_to_close_tier3 = min(initial_tier3_units, int(remaining_units))
                if qty_to_close_tier3 >= 1:
                    await self._close_pct(
                        position_id,
                        qty_to_close_tier3,
                        current_price,
                        "2.5R_partial_activate_runner",
                    )
                    status["tp3_hit"] = True
                    status["runner_active"] = True
                    new_pos_info = await self.position_tracker.get_position_info(
                        position_id
                    )
                    remaining_units = (
                        new_pos_info.get("size", 0.0) if new_pos_info else 0.0
                    )
                    if remaining_units <= 0:
                        logger.info(
                            f"HybridExitManager: Position {position_id} fully closed after 2.5R partial."
                        )
                        self.exit_levels.pop(position_id, None)
                        return
                    else:
                        logger.info(
                            f"HybridExitManager: Runner activated for {position_id} with {remaining_units} units."
                        )
                        data["current_trailing_stop"] = data[
                            "initial_stop_loss"
                        ]
        
        if status.get("runner_active", False) and remaining_units > 0:
            current_regime_for_trailing = data.get(
                "regime_at_entry", "weak"
            )
            if self.lorentzian_classifier:
                fresh_regime_data = self.lorentzian_classifier.get_regime_data(
                    symbol
                )
                current_regime_for_trailing = fresh_regime_data.get(
                    "regime", data.get("regime_at_entry", "weak")
                )
            trail_mult_config = self.TRAILING_SETTINGS.get(
                data["timeframe"], self.TRAILING_SETTINGS["1H"]
            )
            trail_mult = trail_mult_config.get("strong", {"multiplier": 2.0})["multiplier"] if "strong" in current_regime_for_trailing else trail_mult_config.get("weak", {"multiplier": 1.0})["multiplier"]
            trail_distance = atr * trail_mult
            current_active_stop = data.get(
                "current_trailing_stop", data["initial_stop_loss"]
            )
            new_stop_candidate = 0.0
            if direction == "BUY":
                new_stop_candidate = current_price - trail_distance
                if (
                    new_stop_candidate > current_active_stop
                ):
                    success = await self.position_tracker.update_position(
                        position_id, stop_loss=new_stop_candidate
                    )
                    if success:
                        data["current_trailing_stop"] = new_stop_candidate
                        logger.info(
                            f"HybridExitManager: BUY Trailing stop for {position_id} updated to {new_stop_candidate}"
                        )
            else:
                new_stop_candidate = current_price + trail_distance
                if (
                    new_stop_candidate < current_active_stop
                ):
                    success = await self.position_tracker.update_position(
                        position_id, stop_loss=new_stop_candidate
                    )
                    if success:
                        data["current_trailing_stop"] = new_stop_candidate
                        logger.info(
                            f"HybridExitManager: SELL Trailing stop for {position_id} updated to {new_stop_candidate}"
                        )
            
            if (
                direction == "BUY"
                and current_price
                <= data.get("current_trailing_stop", float("-inf"))
            ) or (
                direction == "SELL"
                and current_price >= data.get("current_trailing_stop", float("inf"))
            ):
                logger.info(
                    f"HybridExitManager: Trailing stop hit for {position_id} at {current_price}. Current Stop: {data.get('current_trailing_stop')}"
                )
                if remaining_units > 0:
                    await self._close_pct(
                        position_id,
                        int(remaining_units),
                        current_price,
                        "trailing_stop_hit",
                    )
                self.exit_levels.pop(position_id, None)
                return
        
        open_time_str = pos_info.get("open_time")
        entry_time_for_calc = None
        if open_time_str:
            try:
                from utils import parse_iso_datetime
                entry_time_for_calc = parse_iso_datetime(open_time_str)
            except Exception as e:
                logger.error(
                    f"HybridExitManager: Could not parse open_time '{open_time_str}' for {position_id} for time stop: {e}"
                )
        if entry_time_for_calc and (
            datetime.now(timezone.utc) - entry_time_for_calc
        ) > timedelta(hours=data["time_stop_hours"]):
            logger.info(
                f"HybridExitManager: Time stop triggered for {position_id}. Age: {(datetime.now(timezone.utc) - entry_time_for_calc).total_seconds()/3600:.2f} hrs."
            )
            if remaining_units > 0:
                await self._close_pct(
                    position_id,
                    int(remaining_units),
                    current_price,
                    "hard_time_stop",
                )
            self.exit_levels.pop(position_id, None)
            return
        
        self.exit_levels[
            position_id
        ] = data

    async def _close_pct(self, position_id: str, units_to_close_for_tier: float, current_price_for_exit: float, reason: str):
        """
        Instructs PositionTracker to close a specific number of units for the given position.
        units_to_close_for_tier: The absolute number of units to close for this tier/reason.
        current_price_for_exit: The market price at which this closure is being triggered.
        """
        if not self.position_tracker:
            logger.error(f"HybridExitManager._close_pct: Position tracker not available for position {position_id}.")
            return
        if units_to_close_for_tier <= 1e-9:
            logger.info(f"HybridExitManager._close_pct: Attempt to close zero or negative units ({units_to_close_for_tier}) for {position_id}. Reason: {reason}. Skipping.")
            return
        logger.info(f"HybridExitManager._close_pct: Requesting PositionTracker to close {units_to_close_for_tier:.4f} units for {position_id} at {current_price_for_exit:.5f} (Reason: {reason}).")
        close_result = await self.position_tracker.close_partial_position(
            position_id=position_id,
            exit_price=current_price_for_exit,
            units_to_close=units_to_close_for_tier,
            reason=reason
        )
        if close_result.success:
            logger.info(f"HybridExitManager._close_pct: Partial close for {position_id} (Reason: {reason}) reported as successful by PositionTracker.")
        else:
            logger.error(f"HybridExitManager._close_pct: Partial close for {position_id} (Reason: {reason}) failed. Error: {close_result.error}")

class MarketRegimeExitStrategy:
    """
    Adapts exit strategies based on the current market regime
    and volatility conditions.
    """
    def __init__(self, volatility_monitor=None, regime_classifier=None):
        """Initialize market regime exit strategy"""
        self.volatility_monitor = volatility_monitor
        self.regime_classifier = regime_classifier
        self.exit_configs = {}  # position_id -> exit configuration
        self._lock = asyncio.Lock()
        
    async def initialize_exit_strategy(self,
                                     position_id: str,
                                     symbol: str,
                                     entry_price: float,
                                     direction: str,
                                     atr_value: float,
                                     market_regime: str) -> Dict[str, Any]:
        """Initialize exit strategy based on market regime"""
        async with self._lock:
            # Get volatility state if available
            volatility_ratio = 1.0
            volatility_state = "normal"
            
            if self.volatility_monitor:
                vol_data = self.volatility_monitor.get_volatility_state(symbol)
                volatility_ratio = vol_data.get("volatility_ratio", 1.0)
                volatility_state = vol_data.get("volatility_state", "normal")
                
            if "trending" in market_regime:
                config = self._trend_following_exits(entry_price, direction, atr_value, volatility_ratio)
            elif market_regime == "ranging":
                config = self._mean_reversion_exits(entry_price, direction, atr_value, volatility_ratio)
            elif market_regime == "volatile":
                config = self._volatile_market_exits(entry_price, direction, atr_value, volatility_ratio)
            else: 
                config = self._standard_exits(entry_price, direction, atr_value, volatility_ratio)
                
            # Store config
            self.exit_configs[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": direction,
                "atr_value": atr_value,
                "market_regime": market_regime,
                "volatility_ratio": volatility_ratio,
                "volatility_state": volatility_state,
                "exit_config": config,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            return config

    def _trend_following_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure exits for trending markets"""
        # Wider stops and targets for trend following
        stop_multiplier = 2.5 * volatility_ratio
        target_multiplier = 4.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * stop_multiplier)
            take_profit = entry_price + (atr_value * target_multiplier)
        else:
            stop_loss = entry_price + (atr_value * stop_multiplier)
            take_profit = entry_price - (atr_value * target_multiplier)
            
        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_enabled": True,
            "trailing_distance": atr_value * 1.5,
            "strategy_type": "trend_following"
        }

    def _mean_reversion_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure exits for ranging markets"""
        # Tighter stops and targets for mean reversion
        stop_multiplier = 1.5 * volatility_ratio
        target_multiplier = 2.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * stop_multiplier)
            take_profit = entry_price + (atr_value * target_multiplier)
        else:
            stop_loss = entry_price + (atr_value * stop_multiplier)
            take_profit = entry_price - (atr_value * target_multiplier)
            
        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_enabled": False,
            "strategy_type": "mean_reversion"
        }

    def _volatile_market_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure exits for volatile markets"""
        # Wider stops, moderate targets
        stop_multiplier = 3.0 * volatility_ratio
        target_multiplier = 3.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * stop_multiplier)
            take_profit = entry_price + (atr_value * target_multiplier)
        else:
            stop_loss = entry_price + (atr_value * stop_multiplier)
            take_profit = entry_price - (atr_value * target_multiplier)
            
        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_enabled": True,
            "trailing_distance": atr_value * 2.0,
            "strategy_type": "volatile_market"
        }

    def _standard_exits(self, entry_price: float, direction: str, atr_value: float, volatility_ratio: float) -> Dict[str, Any]:
        """Configure standard exits"""
        stop_multiplier = 2.0 * volatility_ratio
        target_multiplier = 3.0 * volatility_ratio
        
        if direction == "BUY":
            stop_loss = entry_price - (atr_value * stop_multiplier)
            take_profit = entry_price + (atr_value * target_multiplier)
        else:
            stop_loss = entry_price + (atr_value * stop_multiplier)
            take_profit = entry_price - (atr_value * target_multiplier)
            
        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_enabled": True,
            "trailing_distance": atr_value * 1.8,
            "strategy_type": "standard"
        }
        
class TimeBasedTakeProfitManager:
    """
    Manages take profit levels that adjust based on time in trade,
    allowing for holding positions longer in trending markets.
    """
    def __init__(self):
        """Initialize time-based take profit manager"""
        self.take_profits = {}  # position_id -> take profit data
        self._lock = asyncio.Lock()
        
    async def initialize_take_profits(self,
                                    position_id: str,
                                    symbol: str,
                                    entry_price: float,
                                    direction: str,
                                    timeframe: str,
                                    atr_value: float) -> List[float]:
        """Initialize time-based take profit levels for a position"""
        async with self._lock:
            # Define time periods based on timeframe
            time_periods = self._get_time_periods(timeframe)
            
            # Define take profit levels for each time period
            if direction == "BUY":
                tp_levels = [
                    entry_price + (atr_value * 1.0),  # Short-term TP
                    entry_price + (atr_value * 2.0),  # Medium-term TP
                    entry_price + (atr_value * 3.5),  # Long-term TP
                ]
            else:  # SELL
                tp_levels = [
                    entry_price - (atr_value * 1.0),  # Short-term TP
                    entry_price - (atr_value * 2.0),  # Medium-term TP
                    entry_price - (atr_value * 3.5),  # Long-term TP
                ]
                
            # Store take profit data
            self.take_profits[position_id] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "direction": direction,
                "timeframe": timeframe,
                "atr_value": atr_value,
                "time_periods": time_periods,
                "tp_levels": tp_levels,
                "created_at": datetime.now(timezone.utc),
                "last_updated": datetime.now(timezone.utc),
                "status": "active"
            }
            
            return tp_levels

    def _get_time_periods(self, timeframe: str) -> List[int]:
        """Get time periods based on timeframe"""
        timeframe_periods = {
            "M15": [30, 60, 120],  # 30 min, 1 hour, 2 hours
            "H1": [2, 6, 12],      # 2 hours, 6 hours, 12 hours
            "H4": [8, 24, 48],     # 8 hours, 1 day, 2 days
            "D1": [1, 3, 7]        # 1 day, 3 days, 1 week
        }
        return timeframe_periods.get(timeframe, [2, 6, 12])
