"""
Market Structure Analyzer for FX Trading

This module provides comprehensive market structure analysis including:
- Support and resistance level identification
- Trend analysis and classification
- Chart pattern recognition
- Market regime identification
- Multi-timeframe market structure mapping
- Key price level detection and tracking
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import statistics
import math
import numpy as np
from enum import Enum


# Decorator for error handling in asynchronous functions
def async_error_handler(func):
    """Decorator to handle errors in async methods."""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {str(e)}")
            # Re-raise if it's a critical error
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            return None
    return wrapper


# Decorator for error handling in synchronous functions
def error_handler(func):
    """Decorator to handle errors in methods."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {str(e)}")
            # Re-raise if it's a critical error
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            return None
    return wrapper


class MarketStructureAnalyzer:
    """
    Advanced market structure analyzer for trend detection and execution timing.
    
    Features:
    - Support and resistance level identification
    - Trend analysis across multiple timeframes
    - Market regime classification
    - Chart pattern detection
    - Orderflow analysis
    - Supply/demand zone mapping
    - Key level tracking and breakout detection
    """
    
    class TrendType(str, Enum):
        """Trend classification types"""
        STRONG_UPTREND = "strong_uptrend"
        UPTREND = "uptrend"
        WEAK_UPTREND = "weak_uptrend"
        RANGE = "range"
        WEAK_DOWNTREND = "weak_downtrend"
        DOWNTREND = "downtrend"
        STRONG_DOWNTREND = "strong_downtrend"
        UNDEFINED = "undefined"
    
    class MarketRegime(str, Enum):
        """Market regime classification"""
        TRENDING = "trending"
        RANGING = "ranging"
        VOLATILE = "volatile"
        BREAKOUT = "breakout"
        REVERSAL = "reversal"
        CHOPPY = "choppy"
    
    class SignalStrength(str, Enum):
        """Signal strength classification"""
        STRONG = "strong"
        MODERATE = "moderate"
        WEAK = "weak"
        CONFLICTING = "conflicting"
        NEUTRAL = "neutral"
    
    def __init__(self, config=None):
        """
        Initialize the MarketStructureAnalyzer.
        
        Args:
            config: Optional configuration parameters
        """
        self.config = config or {}
        self.logger = logging.getLogger("MarketStructureAnalyzer")
        self.lock = threading.RLock()
        self.running = False
        
        # Data storage
        self.trend_data = {}
        self.support_resistance_levels = {}
        self.key_levels = {}
        self.patterns = {}
        self.regimes = {}
        self.volatility_windows = {}
        self.order_flow = {}
        self.liquidity_zones = {}
        self.price_data = {}
        
        # Analysis parameters (configurable)
        self.sr_detection_periods = self.config.get("sr_detection_periods", [14, 50, 200])
        self.volatility_windows_sizes = self.config.get("volatility_windows", [14, 30, 50])
        self.trend_detection_periods = self.config.get("trend_detection_periods", [20, 50, 200])
        self.pattern_detection_enabled = self.config.get("detect_patterns", True)
        self.multi_timeframe_analysis = self.config.get("multi_timeframe", True)
        self.timeframes = self.config.get("timeframes", ["M5", "M15", "H1", "H4", "D1"])
        
        # Performance metrics
        self.performance_metrics = {
            "signals_generated": 0,
            "correct_signals": 0,
            "incorrect_signals": 0,
            "accuracy": 0.0
        }
    
    @error_handler
    def start(self):
        """Start the market structure analyzer."""
        self.running = True
        self.logger.info("MarketStructureAnalyzer started")
    
    @error_handler
    def stop(self):
        """Stop the market structure analyzer."""
        self.running = False
        self.logger.info("MarketStructureAnalyzer stopped")
    
    @error_handler
    def initialize_session(self, symbols, timeframes=None):
        """
        Initialize market structure analysis for a new trading session.
        
        Args:
            symbols: List of symbols to analyze
            timeframes: Optional list of timeframes to analyze
        """
        with self.lock:
            if timeframes is None:
                timeframes = self.timeframes
            
            # Initialize data structures for each symbol and timeframe
            for symbol in symbols:
                self.trend_data[symbol] = {}
                self.support_resistance_levels[symbol] = {}
                self.key_levels[symbol] = {}
                self.patterns[symbol] = {}
                self.regimes[symbol] = {}
                self.volatility_windows[symbol] = {}
                self.order_flow[symbol] = {}
                self.liquidity_zones[symbol] = {}
                self.price_data[symbol] = {}
                
                for tf in timeframes:
                    self.trend_data[symbol][tf] = {
                        "trend": self.TrendType.UNDEFINED,
                        "strength": 0.0,
                        "direction": 0,
                        "last_update": None
                    }
                    
                    self.support_resistance_levels[symbol][tf] = {
                        "support": [],
                        "resistance": [],
                        "key_levels": [],
                        "last_update": None
                    }
                    
                    self.regimes[symbol][tf] = {
                        "regime": self.MarketRegime.UNDEFINED,
                        "confidence": 0.0,
                        "last_update": None
                    }
                    
                    self.patterns[symbol][tf] = {
                        "active_patterns": [],
                        "completed_patterns": [],
                        "last_update": None
                    }
                    
                    self.volatility_windows[symbol][tf] = {
                        "current": 0.0,
                        "historical": [],
                        "percentile": 50.0,
                        "last_update": None
                    }
            
            self.logger.info(f"Initialized {len(symbols)} symbols for market structure analysis")
    
    @error_handler
    def update_price_data(self, symbol, timeframe, candles):
        """
        Update price data with new candles.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe of the data
            candles: List of candle data
        """
        with self.lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = {}
            
            self.price_data[symbol][timeframe] = candles
            current_time = datetime.now()
            
            # Update various analyses
            self._update_trend_analysis(symbol, timeframe, candles)
            self._update_support_resistance(symbol, timeframe, candles)
            self._update_volatility(symbol, timeframe, candles)
            self._update_market_regime(symbol, timeframe, candles)
            
            if self.pattern_detection_enabled:
                self._detect_chart_patterns(symbol, timeframe, candles)
            
            self.logger.debug(f"Updated price data for {symbol} {timeframe} with {len(candles)} candles")
    
    @error_handler
    def get_market_structure(self, symbol, timeframe=None):
        """
        Get comprehensive market structure analysis.
        
        Args:
            symbol: Trading symbol
            timeframe: Optional specific timeframe (None for all)
            
        Returns:
            Dict with market structure analysis
        """
        with self.lock:
            if symbol not in self.trend_data:
                return None
            
            if timeframe is not None:
                # Return data for specific timeframe
                if timeframe not in self.trend_data[symbol]:
                    return None
                
                return {
                    "trend": self.trend_data[symbol][timeframe],
                    "support_resistance": self.support_resistance_levels[symbol][timeframe],
                    "regime": self.regimes[symbol][timeframe],
                    "patterns": self.patterns[symbol][timeframe],
                    "volatility": self.volatility_windows[symbol][timeframe]
                }
            else:
                # Return data for all timeframes
                return {
                    "trend": self.trend_data[symbol],
                    "support_resistance": self.support_resistance_levels[symbol],
                    "regime": self.regimes[symbol],
                    "patterns": self.patterns[symbol],
                    "volatility": self.volatility_windows[symbol]
                }
    
    @error_handler
    def get_trend(self, symbol, timeframe):
        """
        Get trend analysis for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe to analyze
            
        Returns:
            Dict with trend information
        """
        with self.lock:
            if (symbol in self.trend_data and 
                timeframe in self.trend_data[symbol]):
                return self.trend_data[symbol][timeframe]
            return None
    
    @error_handler
    def get_support_resistance(self, symbol, timeframe):
        """
        Get support and resistance levels.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe to analyze
            
        Returns:
            Dict with support and resistance levels
        """
        with self.lock:
            if (symbol in self.support_resistance_levels and 
                timeframe in self.support_resistance_levels[symbol]):
                return self.support_resistance_levels[symbol][timeframe]
            return None
    
    @error_handler
    def get_market_regime(self, symbol, timeframe):
        """
        Get market regime classification.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe to analyze
            
        Returns:
            Dict with market regime information
        """
        with self.lock:
            if (symbol in self.regimes and 
                timeframe in self.regimes[symbol]):
                return self.regimes[symbol][timeframe]
            return None
    
    @error_handler
    def get_multi_timeframe_trend_alignment(self, symbol, timeframes=None):
        """
        Calculate trend alignment across multiple timeframes.
        
        Args:
            symbol: Trading symbol
            timeframes: Optional list of timeframes to check
            
        Returns:
            Dict with trend alignment analysis
        """
        with self.lock:
            if symbol not in self.trend_data:
                return None
            
            if timeframes is None:
                timeframes = self.timeframes
            
            # Check which timeframes have data
            available_timeframes = [tf for tf in timeframes if tf in self.trend_data[symbol]]
            
            if not available_timeframes:
                return None
            
            # Calculate trend alignment
            trends = [self.trend_data[symbol][tf]["trend"] for tf in available_timeframes]
            directions = [self.trend_data[symbol][tf]["direction"] for tf in available_timeframes]
            
            # Count trend directions
            uptrend_count = sum(1 for d in directions if d > 0)
            downtrend_count = sum(1 for d in directions if d < 0)
            neutral_count = sum(1 for d in directions if d == 0)
            
            # Calculate alignment percentage
            if uptrend_count > downtrend_count:
                alignment = uptrend_count / len(directions) * 100
                primary_direction = "up"
            elif downtrend_count > uptrend_count:
                alignment = downtrend_count / len(directions) * 100
                primary_direction = "down"
            else:
                alignment = neutral_count / len(directions) * 100
                primary_direction = "neutral"
            
            # Determine alignment strength
            if alignment >= 80:
                strength = self.SignalStrength.STRONG
            elif alignment >= 60:
                strength = self.SignalStrength.MODERATE
            elif alignment >= 40:
                strength = self.SignalStrength.WEAK
            else:
                strength = self.SignalStrength.CONFLICTING
            
            return {
                "alignment_percent": alignment,
                "primary_direction": primary_direction,
                "strength": strength,
                "uptrend_timeframes": uptrend_count,
                "downtrend_timeframes": downtrend_count,
                "neutral_timeframes": neutral_count,
                "timeframes_analyzed": available_timeframes
            }
    
    @error_handler
    def get_execution_timing(self, symbol, direction, timeframes=None):
        """
        Evaluate optimal execution timing based on market structure.
        
        Args:
            symbol: Trading symbol
            direction: Intended trade direction ('buy' or 'sell')
            timeframes: Optional list of timeframes to check
            
        Returns:
            Dict with execution timing evaluation
        """
        with self.lock:
            if timeframes is None:
                timeframes = self.timeframes
            
            available_timeframes = [tf for tf in timeframes 
                                    if symbol in self.trend_data 
                                    and tf in self.trend_data[symbol]]
            
            if not available_timeframes:
                return None
            
            # Check trend alignment
            alignment = self.get_multi_timeframe_trend_alignment(symbol, available_timeframes)
            
            # Determine if intended direction aligns with market structure
            direction_aligned = (
                (direction.lower() == "buy" and alignment["primary_direction"] == "up") or
                (direction.lower() == "sell" and alignment["primary_direction"] == "down")
            )
            
            # Check volatility conditions
            volatility_favorable = True
            for tf in available_timeframes:
                if (symbol in self.volatility_windows and 
                    tf in self.volatility_windows[symbol] and
                    self.volatility_windows[symbol][tf]["percentile"] > 80):
                    volatility_favorable = False
                    break
            
            # Check regime
            regimes_favorable = True
            for tf in available_timeframes:
                if (symbol in self.regimes and 
                    tf in self.regimes[symbol] and
                    self.regimes[symbol][tf]["regime"] in [
                        self.MarketRegime.CHOPPY, 
                        self.MarketRegime.VOLATILE
                    ]):
                    regimes_favorable = False
                    break
            
            # Calculate timing score (0-100)
            base_score = 50
            
            if direction_aligned:
                base_score += 20
            else:
                base_score -= 20
            
            if volatility_favorable:
                base_score += 15
            else:
                base_score -= 15
            
            if regimes_favorable:
                base_score += 15
            else:
                base_score -= 15
            
            # Ensure score is within bounds
            score = max(0, min(100, base_score))
            
            # Determine timing quality
            if score >= 80:
                timing = "Excellent"
            elif score >= 65:
                timing = "Good"
            elif score >= 50:
                timing = "Neutral"
            elif score >= 35:
                timing = "Poor"
            else:
                timing = "Very Poor"
            
            return {
                "score": score,
                "timing": timing,
                "direction_aligned": direction_aligned,
                "volatility_favorable": volatility_favorable,
                "regimes_favorable": regimes_favorable,
                "alignment": alignment
            }
    
    @error_handler
    def detect_key_levels(self, symbol, timeframe, price_range=0.01):
        """
        Detect and update key price levels.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            price_range: Price range tolerance for level clustering
            
        Returns:
            List of key price levels
        """
        with self.lock:
            if (symbol not in self.support_resistance_levels or
                timeframe not in self.support_resistance_levels[symbol]):
                return []
            
            sr_data = self.support_resistance_levels[symbol][timeframe]
            
            # Combine support and resistance levels
            all_levels = sr_data["support"] + sr_data["resistance"]
            
            # Sort by price
            all_levels.sort()
            
            # Cluster nearby levels
            clustered_levels = []
            current_cluster = []
            
            for level in all_levels:
                if not current_cluster:
                    current_cluster.append(level)
                elif abs(level - current_cluster[-1]) / current_cluster[-1] <= price_range:
                    current_cluster.append(level)
                else:
                    # Calculate average of cluster
                    avg_level = sum(current_cluster) / len(current_cluster)
                    clustered_levels.append(avg_level)
                    current_cluster = [level]
            
            # Add the last cluster if exists
            if current_cluster:
                avg_level = sum(current_cluster) / len(current_cluster)
                clustered_levels.append(avg_level)
            
            # Update key levels
            self.key_levels[symbol][timeframe] = clustered_levels
            
            return clustered_levels
    
    @error_handler
    def _update_trend_analysis(self, symbol, timeframe, candles):
        """
        Update trend analysis for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
        """
        if len(candles) < max(self.trend_detection_periods):
            return
        
        # Get close prices
        closes = [candle['close'] for candle in candles]
        
        # Calculate EMAs for different periods
        ema_values = {}
        for period in self.trend_detection_periods:
            ema_values[period] = self._calculate_ema(closes, period)
        
        # Determine trend based on EMA relationships
        current_price = closes[-1]
        
        # Initialize scores
        trend_score = 0
        
        # Check EMA alignment
        sorted_periods = sorted(self.trend_detection_periods)
        
        for i, period in enumerate(sorted_periods):
            # Shorter EMAs above longer EMAs = uptrend
            for longer_period in sorted_periods[i+1:]:
                if ema_values[period][-1] > ema_values[longer_period][-1]:
                    trend_score += 1
                elif ema_values[period][-1] < ema_values[longer_period][-1]:
                    trend_score -= 1
        
        # Price relative to EMAs
        for period in sorted_periods:
            if current_price > ema_values[period][-1]:
                trend_score += 1
            elif current_price < ema_values[period][-1]:
                trend_score -= 1
        
        # Calculate maximum possible score
        max_score = len(sorted_periods) + sum(range(len(sorted_periods)))
        
        # Normalize to -1 to 1 range
        normalized_score = trend_score / max_score if max_score > 0 else 0
        
        # Determine trend type
        if normalized_score >= 0.7:
            trend = self.TrendType.STRONG_UPTREND
        elif normalized_score >= 0.3:
            trend = self.TrendType.UPTREND
        elif normalized_score >= 0.1:
            trend = self.TrendType.WEAK_UPTREND
        elif normalized_score <= -0.7:
            trend = self.TrendType.STRONG_DOWNTREND
        elif normalized_score <= -0.3:
            trend = self.TrendType.DOWNTREND
        elif normalized_score <= -0.1:
            trend = self.TrendType.WEAK_DOWNTREND
        else:
            trend = self.TrendType.RANGE
        
        # Update trend data
        self.trend_data[symbol][timeframe] = {
            "trend": trend,
            "strength": abs(normalized_score),
            "direction": 1 if normalized_score > 0 else (-1 if normalized_score < 0 else 0),
            "raw_score": trend_score,
            "normalized_score": normalized_score,
            "last_update": datetime.now()
        }
    
    @error_handler
    def _update_support_resistance(self, symbol, timeframe, candles):
        """
        Update support and resistance levels.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
        """
        if len(candles) < 50:  # Need enough data for meaningful S/R
            return
        
        # Extract highs and lows
        highs = [candle['high'] for candle in candles]
        lows = [candle['low'] for candle in candles]
        
        # Find swing highs and lows
        swing_highs = self._find_swing_points(highs, 'high')
        swing_lows = self._find_swing_points(lows, 'low')
        
        # Cluster nearby levels
        resistance_levels = self._cluster_price_levels(swing_highs)
        support_levels = self._cluster_price_levels(swing_lows)
        
        # Update support and resistance levels
        self.support_resistance_levels[symbol][timeframe] = {
            "support": support_levels,
            "resistance": resistance_levels,
            "last_update": datetime.now()
        }
        
        # Update key levels by combining supports and resistances
        self.key_levels[symbol][timeframe] = support_levels + resistance_levels
    
    @error_handler
    def _update_volatility(self, symbol, timeframe, candles):
        """
        Update volatility measures.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
        """
        if len(candles) < max(self.volatility_windows_sizes):
            return
        
        # Calculate volatility using average true range (ATR)
        atr_values = {}
        for window in self.volatility_windows_sizes:
            atr_values[window] = self._calculate_atr(candles, window)
        
        # Use the middle window as the primary volatility measure
        primary_window = self.volatility_windows_sizes[len(self.volatility_windows_sizes) // 2]
        current_volatility = atr_values[primary_window][-1]
        
        # Keep historical values for percentile calculation
        historical = self.volatility_windows[symbol][timeframe]["historical"]
        historical.append(current_volatility)
        
        # Limit history to last 100 values
        if len(historical) > 100:
            historical = historical[-100:]
        
        # Calculate percentile of current volatility
        if historical:
            percentile = sum(1 for x in historical if x <= current_volatility) / len(historical) * 100
        else:
            percentile = 50
        
        # Update volatility data
        self.volatility_windows[symbol][timeframe] = {
            "current": current_volatility,
            "historical": historical,
            "percentile": percentile,
            "window_size": primary_window,
            "last_update": datetime.now()
        }
    
    @error_handler
    def _update_market_regime(self, symbol, timeframe, candles):
        """
        Update market regime classification.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
        """
        if len(candles) < 50:  # Need enough data for regime analysis
            return
        
        # Get trend data
        trend_data = self.trend_data[symbol][timeframe]
        
        # Get volatility data
        volatility_data = self.volatility_windows[symbol][timeframe]
        
        # Classify regime based on trend and volatility
        if trend_data["strength"] > 0.7 and abs(trend_data["direction"]) > 0:
            regime = self.MarketRegime.TRENDING
            confidence = trend_data["strength"]
        elif volatility_data["percentile"] > 80:
            regime = self.MarketRegime.VOLATILE
            confidence = volatility_data["percentile"] / 100
        elif trend_data["trend"] == self.TrendType.RANGE:
            regime = self.MarketRegime.RANGING
            confidence = 0.7
        elif volatility_data["percentile"] < 20:
            regime = self.MarketRegime.CHOPPY
            confidence = 1.0 - (volatility_data["percentile"] / 100)
        else:
            # Default case
            regime = self.MarketRegime.UNDEFINED
            confidence = 0.5
        
        # Check for breakout conditions
        if self._detect_breakout(symbol, timeframe, candles):
            regime = self.MarketRegime.BREAKOUT
            confidence = 0.8
        
        # Check for reversal conditions
        if self._detect_reversal(symbol, timeframe, candles):
            regime = self.MarketRegime.REVERSAL
            confidence = 0.8
        
        # Update regime data
        self.regimes[symbol][timeframe] = {
            "regime": regime,
            "confidence": confidence,
            "last_update": datetime.now()
        }
    
    @error_handler
    def _detect_chart_patterns(self, symbol, timeframe, candles):
        """
        Detect chart patterns.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
        """
        # This is a placeholder for pattern detection logic
        # In a full implementation, this would detect patterns like
        # head and shoulders, double tops/bottoms, triangles, etc.
        
        # For now, we'll just initialize the data structure
        self.patterns[symbol][timeframe] = {
            "active_patterns": [],
            "completed_patterns": [],
            "last_update": datetime.now()
        }
    
    @error_handler
    def _detect_breakout(self, symbol, timeframe, candles):
        """
        Detect price breakouts.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
            
        Returns:
            Boolean indicating if a breakout was detected
        """
        if len(candles) < 20:
            return False
        
        # Simple breakout detection: price closes outside recent range by significant margin
        close_prices = [candle['close'] for candle in candles[-20:]]
        recent_high = max(close_prices[:-1])
        recent_low = min(close_prices[:-1])
        current_close = close_prices[-1]
        
        # Get average true range for volatility context
        atr = self._calculate_atr(candles, 14)[-1]
        
        # Breakout threshold as a multiple of ATR
        threshold = 1.5 * atr
        
        # Check for breakout
        if current_close > recent_high + threshold:
            return True
        if current_close < recent_low - threshold:
            return True
        
        return False
    
    @error_handler
    def _detect_reversal(self, symbol, timeframe, candles):
        """
        Detect price reversals.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for analysis
            candles: Price candles
            
        Returns:
            Boolean indicating if a reversal was detected
        """
        if len(candles) < 20:
            return False
        
        # Simple reversal detection based on trend change
        # A more sophisticated implementation would consider candlestick patterns,
        # momentum divergence, etc.
        
        # Check if we have prior trend data
        if symbol not in self.trend_data or timeframe not in self.trend_data[symbol]:
            return False
        
        previous_trend = self.trend_data[symbol][timeframe]
        
        # Calculate current trend using shorter lookback
        closes = [candle['close'] for candle in candles[-15:]]
        short_trend = 1 if closes[-1] > closes[0] else -1
        
        # Detect reversal: previous trend direction changes
        if previous_trend["direction"] * short_trend < 0 and abs(previous_trend["direction"]) > 0:
            return True
        
        return False
    
    #
    # Helper methods
    #
    
    def _calculate_ema(self, values, period):
        """Calculate exponential moving average."""
        if len(values) < period:
            return [values[-1]] * len(values)
        
        # Calculate simple moving average first
        sma = sum(values[:period]) / period
        
        # Calculate multiplier
        multiplier = 2 / (period + 1)
        
        # Calculate EMA for each value
        ema_values = [0] * len(values)
        ema_values[period-1] = sma
        
        for i in range(period, len(values)):
            ema_values[i] = (values[i] - ema_values[i-1]) * multiplier + ema_values[i-1]
        
        return ema_values
    
    def _calculate_atr(self, candles, period):
        """Calculate Average True Range."""
        if len(candles) < period + 1:
            return [0] * len(candles)
        
        # Calculate true ranges
        true_ranges = []
        
        for i in range(1, len(candles)):
            high = candles[i]['high']
            low = candles[i]['low']
            prev_close = candles[i-1]['close']
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        # Calculate ATR
        atr_values = [0] * len(candles)
        
        # First ATR is simple average of true ranges
        atr_values[period] = sum(true_ranges[:period]) / period
        
        # Calculate subsequent ATRs
        for i in range(period + 1, len(candles)):
            atr_values[i] = (atr_values[i-1] * (period-1) + true_ranges[i-1]) / period
        
        return atr_values
    
    def _find_swing_points(self, prices, point_type, window=5):
        """Find swing highs or lows."""
        swing_points = []
        
        if len(prices) < 2 * window + 1:
            return swing_points
        
        for i in range(window, len(prices) - window):
            if point_type == 'high':
                # Check if this is a local maximum
                is_swing = all(prices[i] >= prices[i-j] for j in range(1, window+1)) and \
                           all(prices[i] >= prices[i+j] for j in range(1, window+1))
            else:
                # Check if this is a local minimum
                is_swing = all(prices[i] <= prices[i-j] for j in range(1, window+1)) and \
                           all(prices[i] <= prices[i+j] for j in range(1, window+1))
            
            if is_swing:
                swing_points.append(prices[i])
        
        return swing_points
    
    def _cluster_price_levels(self, price_points, tolerance=0.0005):
        """Cluster nearby price levels."""
        if not price_points:
            return []
        
        clusters = []
        current_cluster = [price_points[0]]
        
        for price in price_points[1:]:
            # Check if the price is within tolerance of the average of the current cluster
            cluster_avg = sum(current_cluster) / len(current_cluster)
            
            if abs(price - cluster_avg) / cluster_avg <= tolerance:
                current_cluster.append(price)
            else:
                # Start a new cluster
                clusters.append(sum(current_cluster) / len(current_cluster))
                current_cluster = [price]
        
        # Add the last cluster
        if current_cluster:
            clusters.append(sum(current_cluster) / len(current_cluster))
        
        return clusters
