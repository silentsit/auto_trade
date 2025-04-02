"""
Market Analytics Module

This module provides classes for analyzing market structure and volatility,
which are used to make trading decisions.
"""

import asyncio
import logging
import statistics
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union

# Import trading_api functions in a way that avoids circular dependencies
# These imports will be resolved when python_bridge.py loads everything
trading_api_imported = False
get_candles = None
get_atr = None

def resolve_imports():
    """Resolve imports once trading_api is fully loaded"""
    global trading_api_imported, get_candles, get_atr
    if not trading_api_imported:
        try:
            from trading_api import get_candles as gc, get_atr as ga
            get_candles = gc
            get_atr = ga
            trading_api_imported = True
        except ImportError:
            # Will be resolved on next call after trading_api is loaded
            pass

# Configure logger
logger = logging.getLogger("market_analytics")

class MarketStructureAnalyzer:
    """
    Analyzes market structure to identify key levels like support, resistance,
    and trend direction for improved trade entries and exits.
    """
    def __init__(self):
        self.cache = {}
        self.cache_expiry = {}
        self._lock = asyncio.Lock()

    async def analyze_market_structure(
        self, 
        symbol: str, 
        timeframe: str, 
        current_price: float,
        high_price: float = None,
        low_price: float = None
    ) -> Dict[str, Any]:
        """
        Analyze market structure for a symbol to identify key levels
        
        Returns a dictionary with:
        - trend: "bullish", "bearish", or "sideways"
        - strength: value from 0-10 indicating trend strength
        - nearest_support: closest support level below current price
        - nearest_resistance: closest resistance level above current price
        - key_levels: list of identified key price levels
        """
        # Resolve imports first
        resolve_imports()
        
        # Check cache first
        cache_key = f"{symbol}_{timeframe}"
        
        async with self._lock:
            if cache_key in self.cache and datetime.now() < self.cache_expiry.get(cache_key, datetime.now()):
                cached_data = self.cache[cache_key]
                
                # Update with current price if needed
                if current_price:
                    cached_data["nearest_support"] = self._get_nearest_level(
                        current_price, cached_data["supports"], below=True
                    )
                    cached_data["nearest_resistance"] = self._get_nearest_level(
                        current_price, cached_data["resistances"], below=False
                    )
                    
                return cached_data
        
        # Get historical candles
        if get_candles is not None:
            success, candles = await get_candles(symbol, timeframe, count=200)
        else:
            logger.error("get_candles function not available - imports not resolved")
            success, candles = False, []
        
        if not success or len(candles) < 20:
            # Return basic structure with default values if we don't have enough data
            return {
                "trend": "sideways",
                "strength": 0,
                "nearest_support": current_price * 0.98 if current_price else None,
                "nearest_resistance": current_price * 1.02 if current_price else None,
                "key_levels": [],
                "supports": [],
                "resistances": []
            }
            
        # Process candles to identify key levels
        highs = [float(c["mid"]["h"]) for c in candles]
        lows = [float(c["mid"]["l"]) for c in candles]
        closes = [float(c["mid"]["c"]) for c in candles]
        
        # Calculate moving averages for trend determination
        ma20 = sum(closes[-20:]) / 20
        ma50 = sum(closes[-min(50, len(closes)):]) / min(50, len(closes))
        
        # Identify trend
        trend = "sideways"
        if ma20 > ma50 and closes[-1] > ma20:
            trend = "bullish"
        elif ma20 < ma50 and closes[-1] < ma20:
            trend = "bearish"
            
        # Calculate trend strength (0-10)
        # This is a simple measure based on the percentage difference between MA20 and MA50
        strength = min(10, int(abs(ma20 - ma50) / ma50 * 1000))
        
        # Identify support levels
        supports = self._identify_support_levels(candles, lows)
        
        # Identify resistance levels
        resistances = self._identify_resistance_levels(candles, highs)
        
        # Find nearest support and resistance to current price
        nearest_support = self._get_nearest_level(current_price, supports, below=True)
        nearest_resistance = self._get_nearest_level(current_price, resistances, below=False)
        
        # Combine supports and resistances for key levels
        key_levels = sorted(list(set(supports + resistances)))
        
        # Create result
        result = {
            "trend": trend,
            "strength": strength,
            "nearest_support": nearest_support,
            "nearest_resistance": nearest_resistance,
            "key_levels": key_levels,
            "supports": supports,
            "resistances": resistances
        }
        
        # Cache result
        async with self._lock:
            self.cache[cache_key] = result
            self.cache_expiry[cache_key] = datetime.now() + timedelta(minutes=15)
            
        return result
    
    def _identify_support_levels(self, candles: List[Dict[str, Any]], lows: List[float]) -> List[float]:
        """Identify key support levels from historical price data"""
        # Find swing lows (local minima)
        swing_lows = []
        
        # Loop through candles (except first and last two)
        for i in range(2, len(candles) - 2):
            # Check if center candle has lowest low compared to neighbors
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and 
                lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                swing_lows.append(lows[i])
                
        # Group similar levels (within 0.5% of each other)
        grouped_supports = self._group_similar_levels(swing_lows)
        
        return grouped_supports
        
    def _identify_resistance_levels(self, candles: List[Dict[str, Any]], highs: List[float]) -> List[float]:
        """Identify key resistance levels from historical price data"""
        # Find swing highs (local maxima)
        swing_highs = []
        
        # Loop through candles (except first and last two)
        for i in range(2, len(candles) - 2):
            # Check if center candle has highest high compared to neighbors
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and 
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                swing_highs.append(highs[i])
                
        # Group similar levels (within 0.5% of each other)
        grouped_resistances = self._group_similar_levels(swing_highs)
        
        return grouped_resistances
        
    def _group_similar_levels(self, levels: List[float]) -> List[float]:
        """Group similar price levels within 0.5% of each other"""
        if not levels:
            return []
            
        # Sort levels
        sorted_levels = sorted(levels)
        
        grouped = []
        current_group = [sorted_levels[0]]
        
        for level in sorted_levels[1:]:
            # Calculate the percentage difference
            previous = current_group[0]
            perc_diff = abs(level - previous) / previous
            
            if perc_diff <= 0.005:  # 0.5%
                # Add to current group
                current_group.append(level)
            else:
                # Calculate average of current group and start a new group
                grouped.append(sum(current_group) / len(current_group))
                current_group = [level]
                
        # Add the last group
        if current_group:
            grouped.append(sum(current_group) / len(current_group))
            
        return grouped
        
    def _get_nearest_level(self, price: float, levels: List[float], below: bool = True) -> Optional[float]:
        """Get the nearest price level above or below the current price"""
        if not levels or price is None:
            return None
            
        if below:
            # Get levels below price
            below_levels = [l for l in levels if l < price]
            if below_levels:
                return max(below_levels)
        else:
            # Get levels above price
            above_levels = [l for l in levels if l > price]
            if above_levels:
                return min(above_levels)
                
        return None


class VolatilityMonitor:
    """
    Monitors market volatility to adjust trading parameters 
    based on current market conditions.
    """
    def __init__(self):
        self.volatility_data = {}  # symbol -> timeframe -> data
        self.cache_expiry = {}
        self._lock = asyncio.Lock()
        
    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str) -> Dict[str, Any]:
        """Update volatility data for a symbol and timeframe"""
        cache_key = f"{symbol}_{timeframe}"
        
        async with self._lock:
            # Initialize data if needed
            if symbol not in self.volatility_data:
                self.volatility_data[symbol] = {}
                
            if timeframe not in self.volatility_data[symbol]:
                self.volatility_data[symbol][timeframe] = {
                    "atr_history": [],
                    "current_atr": None,
                    "baseline_atr": None,
                    "volatility_state": "normal",
                    "volatility_ratio": 1.0,
                    "last_update": None
                }
                
            data = self.volatility_data[symbol][timeframe]
            
            # If we need to re-calculate baseline ATR (daily)
            recalculate = False
            if not data["last_update"] or (datetime.now() - data["last_update"]).days >= 1:
                recalculate = True
            
            # Update current ATR
            data["current_atr"] = current_atr
            
            # Update ATR history
            data["atr_history"].append(current_atr)
            
            # Keep only last 100 ATR values
            if len(data["atr_history"]) > 100:
                data["atr_history"] = data["atr_history"][-100:]
                
            # Calculate baseline ATR if needed
            if recalculate or data["baseline_atr"] is None:
                if len(data["atr_history"]) >= 14:
                    data["baseline_atr"] = statistics.mean(data["atr_history"][-14:])
                else:
                    data["baseline_atr"] = current_atr
                    
            # Calculate volatility ratio (current / baseline)
            if data["baseline_atr"] and data["baseline_atr"] > 0:
                data["volatility_ratio"] = current_atr / data["baseline_atr"]
            else:
                data["volatility_ratio"] = 1.0
                
            # Determine volatility state
            if data["volatility_ratio"] >= 1.5:
                data["volatility_state"] = "high"
            elif data["volatility_ratio"] <= 0.7:
                data["volatility_state"] = "low"
            else:
                data["volatility_state"] = "normal"
                
            # Update last update time
            data["last_update"] = datetime.now()
            
            # Update cache expiry
            self.cache_expiry[cache_key] = datetime.now() + timedelta(minutes=15)
            
            return data.copy()  # Return a copy to avoid concurrent modification
            
    async def get_market_condition(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """Get the current market condition for a symbol"""
        # Resolve imports first
        resolve_imports()
        
        # If we don't have a timeframe, use default
        if not timeframe:
            timeframe = "1H"
        
        cache_key = f"{symbol}_{timeframe}"
        
        # Check if we need to update the volatility data
        need_update = False
        
        async with self._lock:
            if (symbol not in self.volatility_data or 
                timeframe not in self.volatility_data.get(symbol, {}) or
                self.volatility_data[symbol][timeframe].get("last_update") is None or
                (datetime.now() - self.volatility_data[symbol][timeframe]["last_update"]).total_seconds() > 300):
                need_update = True
            elif cache_key in self.cache_expiry and datetime.now() < self.cache_expiry[cache_key]:
                # Return cached data
                return self.volatility_data[symbol][timeframe].copy()
        
        if need_update:
            # We need to update the volatility data
            if get_atr is not None:
                try:
                    current_atr = await get_atr(symbol, timeframe)
                    return await self.update_volatility(symbol, current_atr, timeframe)
                except Exception as e:
                    logger.error(f"Error getting ATR for {symbol} on {timeframe}: {str(e)}")
            else:
                logger.error("get_atr function not available - imports not resolved")
        
        # Return default condition if we couldn't update or get cached data
        return {
            "volatility_state": "normal",
            "volatility_ratio": 1.0,
            "current_atr": None,
            "baseline_atr": None,
            "atr_history": []
        }

# Create singleton instances
market_structure_analyzer = MarketStructureAnalyzer()
volatility_monitor = VolatilityMonitor()
