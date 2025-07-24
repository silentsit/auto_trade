from dataclasses import dataclass
from typing import Dict, Any
from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from .position_journal import Position
from datetime import datetime, timezone, timedelta

@dataclass
class OverrideDecision:
    ignore_close: bool
    sl_atr_multiple: float = 2.0
    tp_atr_multiple: float = 4.0
    reason: str = ""

class ProfitRideOverride:
    def __init__(self, regime: LorentzianDistanceClassifier, vol: VolatilityMonitor):
        self.regime = regime
        self.vol = vol

    async def should_override(self, position: Position, current_price: float, drawdown: float = 0.0) -> OverrideDecision:
        # --- Risk Control 3: Only fire once per position ---
        if position.metadata.get('profit_ride_override_fired', False):
            return OverrideDecision(ignore_close=False, reason="Override already fired for this position.")

        # --- Risk Control 2: Hard stop on equity drawdown > 70% ---
        if drawdown > 0.70:
            return OverrideDecision(ignore_close=False, reason="Account drawdown exceeds 70%.")

        # --- Risk Control 1: Maximum ride time (8 bars) ---
        open_time = position.metadata.get('open_time') or position.metadata.get('opened_at')
        if open_time:
            if isinstance(open_time, str):
                try:
                    open_time_dt = datetime.fromisoformat(open_time)
                except Exception:
                    open_time_dt = None
            elif isinstance(open_time, datetime):
                open_time_dt = open_time
            else:
                open_time_dt = None
            if open_time_dt:
                now = datetime.now(timezone.utc)
                tf = position.timeframe.upper()
                if tf == "15M" or tf == "15MIN":
                    max_ride = timedelta(minutes=8*15)
                elif tf == "1H" or tf == "1HR":
                    max_ride = timedelta(hours=8)
                elif tf == "4H":
                    max_ride = timedelta(hours=8*4)
                else:
                    max_ride = timedelta(hours=8)
                if now - open_time_dt > max_ride:
                    return OverrideDecision(ignore_close=False, reason="Maximum ride time exceeded.")

        atr = await get_atr(position.symbol, position.timeframe)
        if atr <= 0:
            return OverrideDecision(ignore_close=False, reason="ATR not available.")

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

        # --- R:R logic by timeframe ---
        tf = position.timeframe.upper()
        if tf == "15M" or tf == "15MIN":
            # 1.5:1 R:R for 15m
            sl_mult = 2.0 if ignore else 1.5
            tp_mult = 3.0 if ignore else 2.25  # 1.5x SL multiple
        else:
            # 2:1 R:R for 1h/4h
            sl_mult = 2.0 if ignore else 1.5
            tp_mult = 4.0 if ignore else 2.5  # 2x SL multiple

        # If override is allowed, set the flag in metadata (caller must persist this)
        return OverrideDecision(
            ignore_close=ignore,
            sl_atr_multiple=sl_mult,
            tp_atr_multiple=tp_mult,
            reason="Override allowed." if ignore else "Override conditions not met."
        )

    async def _higher_highs(self, symbol: str, tf: str, bars: int) -> bool:
        # TODO: Implement candle fetching and higher-highs logic
        return True 