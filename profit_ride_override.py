from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from position_journal import Position
from datetime import datetime, timezone, timedelta

@dataclass
class TieredTPLevel:
    """Represents a single tiered take profit level"""
    level: int  # 1, 2, 3, etc.
    atr_multiple: float  # ATR multiplier for this level
    percentage: float  # Percentage of position to close at this level (0.0-1.0)
    price: Optional[float] = None  # Calculated price level
    units: Optional[int] = None  # Calculated units to close
    triggered: bool = False  # Whether this level has been triggered
    closed_at: Optional[datetime] = None  # When this level was closed

@dataclass
class OverrideDecision:
    ignore_close: bool
    sl_atr_multiple: float = 2.0
    tp_atr_multiple: float = 4.0
    reason: str = ""
    tiered_tp_levels: Optional[List[TieredTPLevel]] = None

class ProfitRideOverride:
    def __init__(self, regime: LorentzianDistanceClassifier, vol: VolatilityMonitor):
        self.regime = regime
        self.vol = vol

    def _create_tiered_tp_levels(self, entry_price: float, action: str, atr: float, timeframe: str) -> List[TieredTPLevel]:
        """
        Create tiered take profit levels based on timeframe and market conditions
        """
        levels = []
        
        # Define tiered TP structure based on timeframe
        if timeframe.upper() in ["15", "15M", "15MIN"]:
            # 15M: More aggressive tiering
            tp_configs = [
                {"level": 1, "atr_mult": 1.5, "percentage": 0.25},  # 25% at 1.5 ATR
                {"level": 2, "atr_mult": 2.5, "percentage": 0.35},  # 35% at 2.5 ATR  
                {"level": 3, "atr_mult": 4.0, "percentage": 0.40},  # 40% at 4.0 ATR
            ]
        elif timeframe.upper() in ["1H", "1HR", "60"]:
            # 1H: Balanced tiering
            tp_configs = [
                {"level": 1, "atr_mult": 2.0, "percentage": 0.30},  # 30% at 2.0 ATR
                {"level": 2, "atr_mult": 3.0, "percentage": 0.35},  # 35% at 3.0 ATR
                {"level": 3, "atr_mult": 5.0, "percentage": 0.35},  # 35% at 5.0 ATR
            ]
        else:  # 4H and higher
            # 4H+: Conservative tiering for longer trends
            tp_configs = [
                {"level": 1, "atr_mult": 2.5, "percentage": 0.25},  # 25% at 2.5 ATR
                {"level": 2, "atr_mult": 4.0, "percentage": 0.30},  # 30% at 4.0 ATR
                {"level": 3, "atr_mult": 6.0, "percentage": 0.25},  # 25% at 6.0 ATR
                {"level": 4, "atr_mult": 8.0, "percentage": 0.20},  # 20% at 8.0 ATR
            ]
        
        # Calculate price levels and units for each tier
        for config in tp_configs:
            atr_distance = atr * config["atr_mult"]
            
            if action.upper() == "BUY":
                tp_price = entry_price + atr_distance
            else:  # SELL
                tp_price = entry_price - atr_distance
            
            levels.append(TieredTPLevel(
                level=config["level"],
                atr_multiple=config["atr_mult"],
                percentage=config["percentage"],
                price=tp_price
            ))
        
        return levels

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

        # Create tiered TP levels if override is allowed
        tiered_tp_levels = None
        if ignore:
            tiered_tp_levels = self._create_tiered_tp_levels(
                position.entry_price, 
                position.action, 
                atr, 
                position.timeframe
            )
            
            # Calculate units for each level
            for level in tiered_tp_levels:
                level.units = int(position.size * level.percentage)

        # If override is allowed, set the flag in metadata (caller must persist this)
        return OverrideDecision(
            ignore_close=ignore,
            sl_atr_multiple=sl_mult,
            tp_atr_multiple=tp_mult,
            reason="Override allowed with tiered TP." if ignore else "Override conditions not met.",
            tiered_tp_levels=tiered_tp_levels
        )

    async def check_tiered_tp_triggers(self, position: Position, current_price: float) -> List[TieredTPLevel]:
        """
        Check if any tiered TP levels have been triggered
        Returns list of levels that should be closed
        """
        triggered_levels = []
        
        # Get tiered TP levels from position metadata
        tiered_tp_data = position.metadata.get('tiered_tp_levels', [])
        if not tiered_tp_data:
            return triggered_levels
        
        # Convert back to TieredTPLevel objects
        levels = []
        for level_data in tiered_tp_data:
            level = TieredTPLevel(
                level=level_data['level'],
                atr_multiple=level_data['atr_multiple'],
                percentage=level_data['percentage'],
                price=level_data['price'],
                units=level_data['units'],
                triggered=level_data.get('triggered', False),
                closed_at=datetime.fromisoformat(level_data['closed_at']) if level_data.get('closed_at') else None
            )
            levels.append(level)
        
        # Check each level for triggers
        for level in levels:
            if level.triggered or level.closed_at:
                continue  # Skip already triggered/closed levels
                
            if position.action.upper() == "BUY":
                # For BUY positions, check if price reached TP level
                if current_price >= level.price:
                    level.triggered = True
                    triggered_levels.append(level)
            else:  # SELL positions
                # For SELL positions, check if price reached TP level
                if current_price <= level.price:
                    level.triggered = True
                    triggered_levels.append(level)
        
        return triggered_levels

    async def _higher_highs(self, symbol: str, tf: str, bars: int) -> bool:
        # TODO: Implement candle fetching and higher-highs logic
        return True 