from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from position_journal import Position
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)

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
        Ensures 100% position coverage with maximum 3 tiers
        """
        levels = []
        
        # Define tiered TP structure based on timeframe - MAXIMUM 3 TIERS, 100% COVERAGE
        if timeframe.upper() in ["15", "15M", "15MIN"]:
            # 15M: 3 tiers, aggressive tiering, 100% coverage
            tp_configs = [
                {"level": 1, "atr_mult": 1.5, "percentage": 0.35},  # 35% at 1.5 ATR
                {"level": 2, "atr_mult": 2.5, "percentage": 0.35},  # 35% at 2.5 ATR  
                {"level": 3, "atr_mult": 4.0, "percentage": 0.30},  # 30% at 4.0 ATR
            ]
        elif timeframe.upper() in ["1H", "1HR", "60"]:
            # 1H: 3 tiers, balanced tiering, 100% coverage
            tp_configs = [
                {"level": 1, "atr_mult": 2.0, "percentage": 0.35},  # 35% at 2.0 ATR
                {"level": 2, "atr_mult": 3.0, "percentage": 0.35},  # 35% at 3.0 ATR
                {"level": 3, "atr_mult": 5.0, "percentage": 0.30},  # 30% at 5.0 ATR
            ]
        else:  # 4H and higher
            # 4H+: 3 tiers, conservative tiering, 100% coverage
            tp_configs = [
                {"level": 1, "atr_mult": 2.5, "percentage": 0.30},  # 30% at 2.5 ATR
                {"level": 2, "atr_mult": 4.0, "percentage": 0.35},  # 35% at 4.0 ATR
                {"level": 3, "atr_mult": 6.0, "percentage": 0.35},  # 35% at 6.0 ATR
            ]
        
        # Validate total percentage equals 100%
        total_percentage = sum(config["percentage"] for config in tp_configs)
        if abs(total_percentage - 1.0) > 0.001:  # Allow small floating point precision error
            logger.error(f"Tiered TP configuration error: total percentage {total_percentage:.3f} != 100%")
            # Fallback to simple 3-tier system
            tp_configs = [
                {"level": 1, "atr_mult": 2.0, "percentage": 0.35},  # 35% at 2.0 ATR
                {"level": 2, "atr_mult": 3.0, "percentage": 0.35},  # 35% at 3.0 ATR
                {"level": 3, "atr_mult": 5.0, "percentage": 0.30},  # 30% at 5.0 ATR
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
        
        logger.info(f"Created {len(levels)} tiered TP levels for {timeframe} timeframe: "
                   f"Total coverage: {sum(level.percentage for level in levels):.1%}")
        
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

        # Calculate SMA10
        sma10 = await self._calculate_sma10(position.symbol, position.timeframe, current_price)
        momentum = abs(current_price - sma10) / atr if atr else 0
        strong_momentum = momentum > 0.15

        hh = await self._check_price_pattern(position.symbol, position.timeframe, position.action) # Check for appropriate price pattern (higher highs for BUY, lower lows for SELL)

        initial_risk = abs(position.entry_price - (position.stop_loss or position.entry_price)) * position.size
        realized_ok = getattr(position, "pnl", 0) >= initial_risk

        ignore = (is_trending or vol_ok) and (strong_momentum or hh) and realized_ok

        # --- R:R logic by timeframe ---
        tf = position.timeframe.upper()
        if tf == "15M" or tf == "15MIN" or tf == "15":
            # 15M: Use 1.5x ATR for stop loss
            sl_mult = 1.5
            tp_mult = 4.0  # Far TP as backup (tiered TP handles actual exits)
        else:
            # 1H/4H: Use 2.0x ATR for stop loss
            sl_mult = 2.0
            tp_mult = 4.0  # Far TP as backup (tiered TP handles actual exits)

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

    async def _calculate_sma10(self, symbol: str, tf: str, current_price: float) -> float:
        """
        Calculate 10-period Simple Moving Average for momentum analysis.
        
        Args:
            symbol: Trading symbol (e.g., 'EUR_USD')
            tf: Timeframe (e.g., '15M', '1H', '4H')
            
        Returns:
            float: SMA10 value or current price if calculation fails
        """
        try:
            # Import here to avoid circular imports
            from oanda_service import OandaService
            
            # Initialize OANDA service
            oanda_service = OandaService()
            
            # Convert timeframe to OANDA format
            granularity_map = {
                "M1": "M1", "M5": "M5", "M15": "M15", "M30": "M30",
                "15M": "M15", "15MIN": "M15", "15": "M15",
                "1H": "H1", "1HR": "H1", "1": "H1",
                "4H": "H4", "4HR": "H4", "4": "H4",
                "D1": "D", "1D": "D", "D": "D"
            }
            
            oanda_granularity = granularity_map.get(tf.upper(), "M15")
            
            # Fetch historical data for SMA calculation
            historical_data = await oanda_service.get_historical_data(
                symbol=symbol,
                count=15,  # Need at least 10 bars for SMA10
                granularity=oanda_granularity
            )
            
            if not historical_data or 'candles' not in historical_data:
                logger.warning(f"No historical data available for SMA10 calculation on {symbol}")
                return current_price  # Fallback to current price
                
            candles = historical_data['candles']
            if len(candles) < 10:
                logger.warning(f"Insufficient candle data for SMA10 calculation on {symbol}: {len(candles)} < 10")
                return current_price  # Fallback to current price
            
            # Extract close prices for the last 10 bars
            closes = []
            for i in range(len(candles) - 10, len(candles)):
                candle = candles[i]
                if 'mid' in candle and 'c' in candle['mid']:
                    close_price = float(candle['mid']['c'])
                    closes.append(close_price)
                else:
                    logger.warning(f"Invalid candle data structure for SMA10 calculation on {symbol}")
                    return current_price  # Fallback to current price
            
            if len(closes) < 10:
                logger.warning(f"Could not extract 10 close prices for SMA10 calculation on {symbol}")
                return current_price  # Fallback to current price
            
            # Calculate SMA10
            sma10 = sum(closes) / len(closes)
            
            logger.debug(f"SMA10 calculated for {symbol} ({tf}): {sma10:.5f}")
            return sma10
            
        except Exception as e:
            logger.error(f"Error calculating SMA10 for {symbol}: {str(e)}")
            return current_price  # Fallback to current price

    async def _higher_highs(self, symbol: str, tf: str, bars: int) -> bool:
        """
        Check for consecutive higher highs pattern in the last N bars.
        
        Args:
            symbol: Trading symbol (e.g., 'EUR_USD')
            tf: Timeframe (e.g., '15M', '1H', '4H')
            bars: Number of bars to check for higher highs
            
        Returns:
            bool: True if consecutive higher highs pattern is detected
        """
        try:
            # Import here to avoid circular imports
            from oanda_service import OandaService
            from utils import get_atr
            
            # Initialize OANDA service
            oanda_service = OandaService()
            
            # Get historical data for pattern analysis
            # We need more bars than requested to ensure we have enough data
            count = max(bars + 5, 20)  # At least 20 bars for reliable analysis
            
            # Convert timeframe to OANDA format
            granularity_map = {
                "M1": "M1", "M5": "M5", "M15": "M15", "M30": "M30",
                "15M": "M15", "15MIN": "M15", "15": "M15",
                "1H": "H1", "1HR": "H1", "1": "H1",
                "4H": "H4", "4HR": "H4", "4": "H4",
                "D1": "D", "1D": "D", "D": "D"
            }
            
            oanda_granularity = granularity_map.get(tf.upper(), "M15")
            
            # Fetch historical data
            historical_data = await oanda_service.get_historical_data(
                symbol=symbol,
                count=count,
                granularity=oanda_granularity
            )
            
            if not historical_data or 'candles' not in historical_data:
                logger.warning(f"No historical data available for {symbol} on {tf} timeframe")
                return False
                
            candles = historical_data['candles']
            if len(candles) < bars:
                logger.warning(f"Insufficient candle data for {symbol}: {len(candles)} < {bars}")
                return False
            
            # Extract high prices from the last N bars
            highs = []
            for i in range(len(candles) - bars, len(candles)):
                candle = candles[i]
                if 'mid' in candle and 'h' in candle['mid']:
                    high_price = float(candle['mid']['h'])
                    highs.append(high_price)
                else:
                    logger.warning(f"Invalid candle data structure for {symbol}")
                    return False
            
            if len(highs) < bars:
                logger.warning(f"Could not extract {bars} high prices for {symbol}")
                return False
            
            # Check for consecutive higher highs
            # A higher high pattern means each high is higher than the previous one
            consecutive_higher_highs = 0
            for i in range(1, len(highs)):
                if highs[i] > highs[i-1]:
                    consecutive_higher_highs += 1
                else:
                    # Reset counter if we find a lower high
                    consecutive_higher_highs = 0
            
            # Calculate the percentage of bars that show higher highs
            higher_high_ratio = consecutive_higher_highs / (len(highs) - 1) if len(highs) > 1 else 0
            
            # Define what constitutes a "higher high pattern"
            # We want at least 70% of the bars to show higher highs
            min_higher_high_ratio = 0.7
            
            # Additional check: ensure the overall trend is upward
            first_high = highs[0]
            last_high = highs[-1]
            overall_trend_up = last_high > first_high
            
            # Calculate trend strength (how much higher the last high is compared to first)
            trend_strength = (last_high - first_high) / first_high if first_high > 0 else 0
            min_trend_strength = 0.001  # 0.1% minimum trend strength
            
            # Log detailed analysis
            logger.info(f"Higher highs analysis for {symbol} ({tf}): "
                       f"Consecutive HH: {consecutive_higher_highs}/{len(highs)-1}, "
                       f"Ratio: {higher_high_ratio:.2f}, "
                       f"Overall trend: {'UP' if overall_trend_up else 'DOWN'}, "
                       f"Trend strength: {trend_strength:.4f}")
            
            # Return True if we have a strong higher high pattern
            pattern_detected = (higher_high_ratio >= min_higher_high_ratio and 
                              overall_trend_up and 
                              trend_strength >= min_trend_strength)
            
            if pattern_detected:
                logger.info(f"✅ Higher high pattern detected for {symbol} on {tf} timeframe")
            else:
                logger.info(f"❌ No higher high pattern for {symbol} on {tf} timeframe")
            
            return pattern_detected
            
        except Exception as e:
            logger.error(f"Error checking higher highs pattern for {symbol}: {str(e)}")
            return False  # Conservative approach - assume no pattern on error

    async def _lower_lows(self, symbol: str, tf: str, bars: int) -> bool:
        """
        Check for consecutive lower lows pattern in the last N bars (for SELL positions).
        
        Args:
            symbol: Trading symbol (e.g., 'EUR_USD')
            tf: Timeframe (e.g., '15M', '1H', '4H')
            bars: Number of bars to check for lower lows
            
        Returns:
            bool: True if consecutive lower lows pattern is detected
        """
        try:
            # Import here to avoid circular imports
            from oanda_service import OandaService
            
            # Initialize OANDA service
            oanda_service = OandaService()
            
            # Get historical data for pattern analysis
            count = max(bars + 5, 20)  # At least 20 bars for reliable analysis
            
            # Convert timeframe to OANDA format
            granularity_map = {
                "M1": "M1", "M5": "M5", "M15": "M15", "M30": "M30",
                "15M": "M15", "15MIN": "M15", "15": "M15",
                "1H": "H1", "1HR": "H1", "1": "H1",
                "4H": "H4", "4HR": "H4", "4": "H4",
                "D1": "D", "1D": "D", "D": "D"
            }
            
            oanda_granularity = granularity_map.get(tf.upper(), "M15")
            
            # Fetch historical data
            historical_data = await oanda_service.get_historical_data(
                symbol=symbol,
                count=count,
                granularity=oanda_granularity
            )
            
            if not historical_data or 'candles' not in historical_data:
                logger.warning(f"No historical data available for {symbol} on {tf} timeframe")
                return False
                
            candles = historical_data['candles']
            if len(candles) < bars:
                logger.warning(f"Insufficient candle data for {symbol}: {len(candles)} < {bars}")
                return False
            
            # Extract low prices from the last N bars
            lows = []
            for i in range(len(candles) - bars, len(candles)):
                candle = candles[i]
                if 'mid' in candle and 'l' in candle['mid']:
                    low_price = float(candle['mid']['l'])
                    lows.append(low_price)
                else:
                    logger.warning(f"Invalid candle data structure for {symbol}")
                    return False
            
            if len(lows) < bars:
                logger.warning(f"Could not extract {bars} low prices for {symbol}")
                return False
            
            # Check for consecutive lower lows
            # A lower low pattern means each low is lower than the previous one
            consecutive_lower_lows = 0
            for i in range(1, len(lows)):
                if lows[i] < lows[i-1]:
                    consecutive_lower_lows += 1
                else:
                    # Reset counter if we find a higher low
                    consecutive_lower_lows = 0
            
            # Calculate the percentage of bars that show lower lows
            lower_low_ratio = consecutive_lower_lows / (len(lows) - 1) if len(lows) > 1 else 0
            
            # Define what constitutes a "lower low pattern"
            # We want at least 70% of the bars to show lower lows
            min_lower_low_ratio = 0.7
            
            # Additional check: ensure the overall trend is downward
            first_low = lows[0]
            last_low = lows[-1]
            overall_trend_down = last_low < first_low
            
            # Calculate trend strength (how much lower the last low is compared to first)
            trend_strength = (first_low - last_low) / first_low if first_low > 0 else 0
            min_trend_strength = 0.001  # 0.1% minimum trend strength
            
            # Log detailed analysis
            logger.info(f"Lower lows analysis for {symbol} ({tf}): "
                       f"Consecutive LL: {consecutive_lower_lows}/{len(lows)-1}, "
                       f"Ratio: {lower_low_ratio:.2f}, "
                       f"Overall trend: {'DOWN' if overall_trend_down else 'UP'}, "
                       f"Trend strength: {trend_strength:.4f}")
            
            # Return True if we have a strong lower low pattern
            pattern_detected = (lower_low_ratio >= min_lower_low_ratio and 
                              overall_trend_down and 
                              trend_strength >= min_trend_strength)
            
            if pattern_detected:
                logger.info(f"✅ Lower low pattern detected for {symbol} on {tf} timeframe")
            else:
                logger.info(f"❌ No lower low pattern for {symbol} on {tf} timeframe")
            
            return pattern_detected
            
        except Exception as e:
            logger.error(f"Error checking lower lows pattern for {symbol}: {str(e)}")
            return False  # Conservative approach - assume no pattern on error

    async def _check_price_pattern(self, symbol: str, tf: str, action: str, bars: int = 5) -> bool:
        """
        Check for appropriate price pattern based on position direction.
        
        Args:
            symbol: Trading symbol
            tf: Timeframe
            action: Position action (BUY/SELL)
            bars: Number of bars to check
            
        Returns:
            bool: True if appropriate pattern is detected
        """
        if action.upper() == "BUY":
            # For BUY positions, check for higher highs
            return await self._higher_highs(symbol, tf, bars)
        elif action.upper() == "SELL":
            # For SELL positions, check for lower lows
            return await self._lower_lows(symbol, tf, bars)
        else:
            logger.warning(f"Unknown action type: {action}")
            return False 