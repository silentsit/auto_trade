"""
Enhanced Lorentzian Distance Classifier with ML Extensions
Institutional-grade market regime detection using advanced kernel methods
Integrates MLExtensions and KernelFunctions for superior regime classification
"""

import asyncio
import statistics
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from utils import logger, get_module_logger
from config import config

# Import ML enhancements
from ml_extensions import MLExtensions
from kernel_functions import KernelFunctions

class EnhancedLorentzianDistanceClassifier:
    """
    Enhanced Lorentzian Distance Classifier with ML Extensions and Kernel Functions.
    Provides institutional-grade market regime detection with adaptive parameters.
    """
    
    def __init__(self, lookback_period: int = 20, max_history: int = 1000):
        """Initialize with ML enhancements"""
        self.lookback_period = lookback_period
        self.max_history = max_history
        
        # Price and regime data storage
        self.price_history = {}  # symbol -> List[float]
        self.regime_history = {} # symbol -> List[str]
        self.volatility_history = {} # symbol -> List[float]
        self.atr_history = {}  # symbol -> List[float]
        self.regimes = {}  # symbol -> Dict[str, Any]
        
        # ML components
        self.ml_ext = MLExtensions()
        self.kernel_func = KernelFunctions()
        
        # Enhanced data storage for ML processing
        self.price_series = {}  # symbol -> pd.Series
        self.ml_indicators = {}  # symbol -> Dict[str, pd.Series]
        self.kernel_smoothed = {}  # symbol -> Dict[str, pd.Series]
        
        self._lock = asyncio.Lock()
        self.logger = get_module_logger(__name__)
        
        # Adaptive parameters
        self.regime_confidence_threshold = 0.7
        self.ml_signal_weight = 0.6
        self.kernel_weight = 0.4
        
        self.logger.info("✅ Enhanced Lorentzian Distance Classifier initialized with ML extensions")

    async def add_price_data(self, symbol: str, price: float, timeframe: str, 
                           atr: Optional[float] = None, volume: Optional[float] = None):
        """Enhanced price data processing with ML feature extraction"""
        async with self._lock:
            try:
                # Initialize storage if needed
                self._initialize_symbol_storage(symbol)
                
                # Add to traditional storage
                self.price_history[symbol].append(price)
                if len(self.price_history[symbol]) > self.max_history:
                    self.price_history[symbol] = self.price_history[symbol][-self.max_history:]
                
                # Update ATR history
                if atr is not None:
                    self.atr_history[symbol].append(atr)
                    if len(self.atr_history[symbol]) > self.lookback_period:
                        self.atr_history[symbol].pop(0)
                
                # Convert to pandas Series for ML processing
                price_series = pd.Series(self.price_history[symbol])
                self.price_series[symbol] = price_series
                
                # Generate ML indicators if we have enough data
                if len(price_series) >= 50:
                    await self._update_ml_indicators(symbol, price_series, atr)
                
                # Enhanced regime classification
                if len(self.price_history[symbol]) >= 10:
                    await self._classify_enhanced_regime(symbol, price, atr, volume)
                    
            except Exception as e:
                self.logger.error(f"Error adding price data for {symbol}: {e}")

    def _initialize_symbol_storage(self, symbol: str):
        """Initialize storage structures for a new symbol"""
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            self.regime_history[symbol] = []
            self.volatility_history[symbol] = []
            self.atr_history[symbol] = []
            self.ml_indicators[symbol] = {}
            self.kernel_smoothed[symbol] = {}

    async def _update_ml_indicators(self, symbol: str, price_series: pd.Series, atr: Optional[float]):
        """Update ML indicators for enhanced regime detection"""
        try:
            # Create dummy high/low series for indicators that need them
            # In real implementation, you'd pass actual OHLC data
            high_series = price_series * 1.001  # Approximate high
            low_series = price_series * 0.999   # Approximate low
            
            # ML-Enhanced Indicators
            indicators = {}
            
            # Normalized indicators
            indicators['n_rsi'] = self.ml_ext.n_rsi(price_series)
            indicators['n_cci'] = self.ml_ext.n_cci(price_series, high_series, low_series)
            indicators['n_wt'] = self.ml_ext.n_wt(price_series)
            indicators['n_adx'] = self.ml_ext.n_adx(high_series, low_series, price_series)
            
            # Advanced signal processing
            indicators['tanh_transform'] = self.ml_ext.tanh_transform(price_series)
            indicators['normalized_deriv'] = self.ml_ext.normalize_deriv(price_series)
            
            # Kernel smoothing
            indicators['kernel_gaussian'] = self.kernel_func.gaussian(price_series)
            indicators['kernel_rational_quad'] = self.kernel_func.rational_quadratic(price_series)
            indicators['kernel_trend_strength'] = self.kernel_func.kernel_trend_strength(price_series)
            
            # Multi-timeframe analysis
            indicators['mtf_kernel'] = self.kernel_func.multi_timeframe_kernel(
                price_series, [5, 10, 20], [0.5, 0.3, 0.2]
            )
            
            # Adaptive kernel based on volatility
            if atr and len(self.atr_history[symbol]) > 5:
                avg_atr = np.mean(self.atr_history[symbol])
                current_vol_ratio = atr / (avg_atr + 1e-10)
                
                if current_vol_ratio > 1.5:
                    market_condition = "volatile"
                elif current_vol_ratio < 0.7:
                    market_condition = "ranging"
                else:
                    market_condition = "normal"
                    
                indicators['adaptive_kernel'] = self.kernel_func.adaptive_kernel_ensemble(
                    price_series, market_condition=market_condition
                )
            
            # Regime filters
            indicators['regime_filter'] = self.ml_ext.regime_filter(price_series)
            indicators['volatility_filter'] = self.ml_ext.filter_volatility(
                price_series, high_series, low_series
            )
            
            self.ml_indicators[symbol] = indicators
            
        except Exception as e:
            self.logger.error(f"Error updating ML indicators for {symbol}: {e}")

    async def _classify_enhanced_regime(self, symbol: str, current_price: float, 
                                      atr: Optional[float], volume: Optional[float]):
        """Enhanced regime classification using ML and kernel methods"""
        try:
            if len(self.price_history[symbol]) < 10:
                self._set_unknown_regime(symbol)
                return
            
            # Traditional Lorentzian distance
            price_distance = await self.calculate_lorentzian_distance(
                current_price, self.price_history[symbol][:-1]
            )
            
            # Traditional momentum and volatility
            prices = self.price_history[symbol]
            returns = []
            for i in range(1, len(prices)):
                if prices[i-1] != 0:
                    returns.append(prices[i] / prices[i-1] - 1)
            
            volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
            momentum = (current_price - prices[0]) / prices[0] if prices[0] != 0 else 0.0
            
            # ML-Enhanced Classification
            ml_regime, ml_confidence = await self._classify_with_ml(symbol)
            kernel_regime, kernel_confidence = await self._classify_with_kernels(symbol)
            
            # Combined regime determination
            regime, regime_strength = await self._combine_regime_signals(
                symbol, price_distance, volatility, momentum, atr,
                ml_regime, ml_confidence, kernel_regime, kernel_confidence
            )
            
            # Update regime history
            self.regime_history[symbol].append(regime)
            if len(self.regime_history[symbol]) > self.lookback_period:
                self.regime_history[symbol].pop(0)
            
            self.volatility_history[symbol].append(volatility)
            if len(self.volatility_history[symbol]) > self.lookback_period:
                self.volatility_history[symbol].pop(0)
            
            # Enhanced result with ML metrics
            result = {
                "regime": regime,
                "regime_strength": regime_strength,
                "regime_confidence": max(ml_confidence, kernel_confidence),
                "price_distance": price_distance,
                "volatility": volatility,
                "momentum": momentum,
                "ml_regime": ml_regime,
                "ml_confidence": ml_confidence,
                "kernel_regime": kernel_regime,
                "kernel_confidence": kernel_confidence,
                "adaptive_signals": await self._get_adaptive_signals(symbol),
                "last_update": datetime.now(timezone.utc).isoformat()
            }
            
            self.regimes[symbol] = result
            
        except Exception as e:
            self.logger.error(f"Error in enhanced regime classification for {symbol}: {e}")
            self._set_unknown_regime(symbol)

    async def _classify_with_ml(self, symbol: str) -> Tuple[str, float]:
        """Classify regime using ML indicators"""
        try:
            indicators = self.ml_indicators.get(symbol, {})
            if not indicators:
                return "unknown", 0.0
            
            # Get latest values
            latest_values = {}
            for key, series in indicators.items():
                if len(series) > 0:
                    latest_values[key] = series.iloc[-1] if not pd.isna(series.iloc[-1]) else 0.0
                else:
                    latest_values[key] = 0.0
            
            # ML-based regime classification
            n_rsi = latest_values.get('n_rsi', 0.5)
            n_adx = latest_values.get('n_adx', 0.5)
            tanh_transform = latest_values.get('tanh_transform', 0.0)
            kernel_trend = latest_values.get('kernel_trend_strength', 0.0)
            normalized_deriv = abs(latest_values.get('normalized_deriv', 0.0))
            
            # Classification logic
            confidence = 0.5
            
            # Strong trending conditions
            if n_adx > 0.7 and abs(kernel_trend) > 0.4:
                regime = "trending_up" if kernel_trend > 0 else "trending_down"
                confidence = min(0.95, 0.6 + n_adx * 0.3 + abs(kernel_trend) * 0.2)
            
            # Volatile conditions
            elif normalized_deriv > 0.6 or (n_adx > 0.6 and abs(tanh_transform) > 0.5):
                regime = "volatile"
                confidence = min(0.9, 0.6 + normalized_deriv * 0.3)
            
            # Ranging conditions
            elif n_adx < 0.3 and abs(kernel_trend) < 0.1 and normalized_deriv < 0.2:
                regime = "ranging"
                confidence = min(0.85, 0.7 + (0.3 - n_adx))
            
            # Momentum conditions
            elif abs(tanh_transform) > 0.3:
                regime = "momentum_up" if tanh_transform > 0 else "momentum_down"
                confidence = min(0.8, 0.6 + abs(tanh_transform) * 0.4)
            
            else:
                regime = "mixed"
                confidence = 0.5
            
            return regime, confidence
            
        except Exception as e:
            self.logger.error(f"Error in ML classification for {symbol}: {e}")
            return "unknown", 0.0

    async def _classify_with_kernels(self, symbol: str) -> Tuple[str, float]:
        """Classify regime using kernel methods"""
        try:
            indicators = self.ml_indicators.get(symbol, {})
            if not indicators:
                return "unknown", 0.0
            
            # Get kernel indicators
            kernel_trend = indicators.get('kernel_trend_strength', pd.Series([0]))
            kernel_gaussian = indicators.get('kernel_gaussian', pd.Series([0]))
            kernel_rational = indicators.get('kernel_rational_quad', pd.Series([0]))
            adaptive_kernel = indicators.get('adaptive_kernel', pd.Series([0]))
            
            if len(kernel_trend) == 0:
                return "unknown", 0.0
            
            # Latest kernel values
            trend_strength = kernel_trend.iloc[-1] if not pd.isna(kernel_trend.iloc[-1]) else 0.0
            
            # Compare different kernels for regime detection
            recent_prices = pd.Series(self.price_history[symbol][-10:])
            if len(recent_prices) < 5:
                return "unknown", 0.0
            
            # Kernel divergence analysis
            gaussian_diff = abs(kernel_gaussian.iloc[-1] - recent_prices.iloc[-1]) if len(kernel_gaussian) > 0 else 0
            rational_diff = abs(kernel_rational.iloc[-1] - recent_prices.iloc[-1]) if len(kernel_rational) > 0 else 0
            
            # Regime classification based on kernel behavior
            confidence = 0.5
            
            if abs(trend_strength) > 0.3:
                regime = "kernel_trending_up" if trend_strength > 0 else "kernel_trending_down"
                confidence = min(0.9, 0.6 + abs(trend_strength) * 0.5)
            
            elif gaussian_diff < 0.001 and rational_diff < 0.001:
                regime = "kernel_stable"
                confidence = 0.8
            
            elif max(gaussian_diff, rational_diff) > 0.01:
                regime = "kernel_volatile"
                confidence = min(0.85, 0.6 + max(gaussian_diff, rational_diff) * 50)
            
            else:
                regime = "kernel_mixed"
                confidence = 0.5
            
            return regime, confidence
            
        except Exception as e:
            self.logger.error(f"Error in kernel classification for {symbol}: {e}")
            return "unknown", 0.0

    async def _combine_regime_signals(self, symbol: str, price_distance: float, 
                                    volatility: float, momentum: float, atr: Optional[float],
                                    ml_regime: str, ml_confidence: float,
                                    kernel_regime: str, kernel_confidence: float) -> Tuple[str, float]:
        """Combine traditional, ML, and kernel signals for final regime determination"""
        try:
            # Traditional regime (fallback)
            traditional_regime = "mixed"
            traditional_strength = 0.5
            
            mean_atr = np.mean(self.atr_history[symbol]) if self.atr_history.get(symbol) else 0.0
            
            if price_distance < 0.1 and volatility < 0.001:
                traditional_regime = "ranging"
                traditional_strength = min(1.0, 0.7 + (0.1 - price_distance) * 3)
            elif price_distance > 0.3 and abs(momentum) > 0.002:
                traditional_regime = "trending_up" if momentum > 0 else "trending_down"
                traditional_strength = min(1.0, 0.6 + price_distance + abs(momentum) * 10)
            elif volatility > 0.003 or (mean_atr > 0 and atr is not None and atr > 1.5 * mean_atr):
                traditional_regime = "volatile"
                traditional_strength = min(1.0, 0.6 + volatility * 100)
            
            # Weighted combination of signals
            total_weight = 0.0
            weighted_regime_score = {}
            
            # Add traditional signal (lower weight)
            traditional_weight = 0.2
            weighted_regime_score[traditional_regime] = traditional_weight * traditional_strength
            total_weight += traditional_weight
            
            # Add ML signal
            ml_weight = self.ml_signal_weight * ml_confidence
            if ml_regime in weighted_regime_score:
                weighted_regime_score[ml_regime] += ml_weight
            else:
                weighted_regime_score[ml_regime] = ml_weight
            total_weight += ml_weight
            
            # Add kernel signal
            kernel_weight = self.kernel_weight * kernel_confidence
            if kernel_regime in weighted_regime_score:
                weighted_regime_score[kernel_regime] += kernel_weight
            else:
                weighted_regime_score[kernel_regime] = kernel_weight
            total_weight += kernel_weight
            
            # Find regime with highest weighted score
            if total_weight > 0:
                final_regime = max(weighted_regime_score, key=weighted_regime_score.get)
                final_strength = weighted_regime_score[final_regime] / total_weight
            else:
                final_regime = "unknown"
                final_strength = 0.0
            
            # Apply confidence threshold
            if final_strength < self.regime_confidence_threshold:
                final_regime = "mixed"
                final_strength = 0.5
            
            return final_regime, final_strength
            
        except Exception as e:
            self.logger.error(f"Error combining regime signals for {symbol}: {e}")
            return "mixed", 0.5

    async def _get_adaptive_signals(self, symbol: str) -> Dict[str, Any]:
        """Get adaptive signals based on current regime"""
        try:
            indicators = self.ml_indicators.get(symbol, {})
            if not indicators:
                return {}
            
            signals = {
                "regime_filter_active": bool(indicators.get('regime_filter', pd.Series([False])).iloc[-1]),
                "volatility_filter_active": bool(indicators.get('volatility_filter', pd.Series([False])).iloc[-1]),
                "trend_alignment": self._calculate_trend_alignment(indicators),
                "momentum_strength": self._calculate_momentum_strength(indicators),
                "volatility_percentile": self._calculate_volatility_percentile(symbol),
                "adaptive_lookback": self._calculate_adaptive_lookback(symbol)
            }
            
            return signals
            
        except Exception as e:
            self.logger.error(f"Error getting adaptive signals for {symbol}: {e}")
            return {}

    def _calculate_trend_alignment(self, indicators: Dict[str, pd.Series]) -> float:
        """Calculate trend alignment score"""
        try:
            trend_indicators = ['kernel_trend_strength', 'tanh_transform']
            values = []
            
            for indicator in trend_indicators:
                if indicator in indicators and len(indicators[indicator]) > 0:
                    value = indicators[indicator].iloc[-1]
                    if not pd.isna(value):
                        values.append(value)
            
            if not values:
                return 0.0
            
            # Calculate alignment (positive values align, negative values align)
            positive_count = sum(1 for v in values if v > 0.1)
            negative_count = sum(1 for v in values if v < -0.1)
            
            if positive_count > negative_count:
                return positive_count / len(values)
            elif negative_count > positive_count:
                return -negative_count / len(values)
            else:
                return 0.0
                
        except Exception as e:
            self.logger.error(f"Error calculating trend alignment: {e}")
            return 0.0

    def _calculate_momentum_strength(self, indicators: Dict[str, pd.Series]) -> float:
        """Calculate momentum strength"""
        try:
            momentum_indicators = ['n_rsi', 'tanh_transform', 'normalized_deriv']
            strength = 0.0
            count = 0
            
            for indicator in momentum_indicators:
                if indicator in indicators and len(indicators[indicator]) > 0:
                    value = indicators[indicator].iloc[-1]
                    if not pd.isna(value):
                        if indicator == 'n_rsi':
                            # RSI extremes indicate momentum
                            strength += abs(value - 0.5) * 2
                        else:
                            strength += abs(value)
                        count += 1
            
            return strength / count if count > 0 else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating momentum strength: {e}")
            return 0.0

    def _calculate_volatility_percentile(self, symbol: str) -> float:
        """Calculate current volatility percentile"""
        try:
            if len(self.volatility_history[symbol]) < 10:
                return 0.5
            
            recent_vol = self.volatility_history[symbol][-1]
            historical_vols = self.volatility_history[symbol][:-1]
            
            if not historical_vols:
                return 0.5
            
            percentile = sum(1 for v in historical_vols if v < recent_vol) / len(historical_vols)
            return percentile
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility percentile for {symbol}: {e}")
            return 0.5

    def _calculate_adaptive_lookback(self, symbol: str) -> int:
        """Calculate adaptive lookback period based on market conditions"""
        try:
            base_lookback = self.lookback_period
            
            # Adjust based on volatility
            vol_percentile = self._calculate_volatility_percentile(symbol)
            
            if vol_percentile > 0.8:
                # High volatility - shorter lookback
                return max(5, int(base_lookback * 0.7))
            elif vol_percentile < 0.2:
                # Low volatility - longer lookback
                return min(50, int(base_lookback * 1.3))
            else:
                return base_lookback
                
        except Exception as e:
            self.logger.error(f"Error calculating adaptive lookback for {symbol}: {e}")
            return self.lookback_period

    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """Enhanced Lorentzian distance calculation"""
        if not history:
            return 0.0
        
        try:
            # Traditional Lorentzian distance
            distances = [np.log(1 + abs(price - hist_price)) for hist_price in history[-20:]]  # Use recent history
            base_distance = float(np.mean(distances)) if distances else 0.0
            
            # Apply kernel smoothing to the distance calculation
            if len(history) >= 10:
                price_series = pd.Series(history[-10:] + [price])
                kernel_smoothed = self.kernel_func.gaussian(price_series, 5)
                
                if len(kernel_smoothed) > 0:
                    smoothed_price = kernel_smoothed.iloc[-1]
                    kernel_distance = abs(price - smoothed_price) / (smoothed_price + 1e-10)
                    
                    # Combine traditional and kernel-based distances
                    combined_distance = 0.7 * base_distance + 0.3 * kernel_distance
                    return combined_distance
            
            return base_distance
            
        except Exception as e:
            self.logger.error(f"Error calculating Lorentzian distance: {e}")
            return 0.0

    def _set_unknown_regime(self, symbol: str):
        """Set unknown regime state"""
        unknown_state = {
            "regime": "unknown", 
            "regime_strength": 0.0, 
            "regime_confidence": 0.0,
            "price_distance": 0.0, 
            "volatility": 0.0, 
            "momentum": 0.0,
            "ml_regime": "unknown",
            "ml_confidence": 0.0,
            "kernel_regime": "unknown", 
            "kernel_confidence": 0.0,
            "adaptive_signals": {},
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        self.regimes[symbol] = unknown_state

    async def get_regime_analysis(self, symbol: str) -> Dict[str, Any]:
        """Get comprehensive regime analysis"""
        try:
            if symbol not in self.regimes:
                return {"error": f"No regime data for {symbol}"}
            
            regime_data = self.regimes[symbol].copy()
            
            # Add historical analysis
            if symbol in self.regime_history and len(self.regime_history[symbol]) > 5:
                recent_regimes = self.regime_history[symbol][-5:]
                regime_stability = len(set(recent_regimes)) / len(recent_regimes)  # Lower = more stable
                regime_data["regime_stability"] = regime_stability
                regime_data["recent_regime_sequence"] = recent_regimes
            
            # Add ML indicator summary
            if symbol in self.ml_indicators:
                indicators = self.ml_indicators[symbol]
                ml_summary = {}
                
                for key, series in indicators.items():
                    if len(series) > 0:
                        latest_value = series.iloc[-1]
                        ml_summary[key] = latest_value if not pd.isna(latest_value) else 0.0
                
                regime_data["ml_indicators"] = ml_summary
            
            return regime_data
            
        except Exception as e:
            self.logger.error(f"Error getting regime analysis for {symbol}: {e}")
            return {"error": str(e)}

# Legacy compatibility
LorentzianDistanceClassifier = EnhancedLorentzianDistanceClassifier
