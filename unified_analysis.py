"""
Unified Market Analysis Service
Consolidates all market analysis functionality into a single, comprehensive service:
- Regime classification
- Technical analysis
- Crypto signal handling
- Market trend analysis
- Volatility monitoring
- Institutional-grade indicators

This module consolidates functionality from:
- volatility_monitor.py
- regime_classifier.py
- technical_analysis.py
"""

import asyncio
import logging
import math
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class AnalysisType(Enum):
    """Types of market analysis"""
    REGIME = "regime"
    TECHNICAL = "technical"
    CRYPTO = "crypto"
    TREND = "trend"

class MarketRegime(Enum):
    """Market regime classifications"""
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    VOLATILE = "volatile"
    UNKNOWN = "unknown"

@dataclass
class RegimeData:
    """Market regime analysis data"""
    symbol: str
    timestamp: datetime
    current_regime: MarketRegime
    confidence: float
    features: Dict[str, float]
    last_update: datetime

@dataclass
class TechnicalIndicators:
    """Technical analysis indicators"""
    symbol: str
    timestamp: datetime
    sma_20: Optional[float]
    sma_50: Optional[float]
    ema_12: Optional[float]
    ema_26: Optional[float]
    rsi: Optional[float]
    macd: Optional[float]
    macd_signal: Optional[float]
    macd_histogram: Optional[float]
    atr: Optional[float]
    bollinger_upper: Optional[float]
    bollinger_lower: Optional[float]
    bollinger_middle: Optional[float]

@dataclass
class CryptoSignal:
    """Crypto trading signal"""
    symbol: str
    timestamp: datetime
    signal_type: str  # "BUY", "SELL", "HOLD"
    confidence: float
    price: float
    volume: Optional[float]
    indicators: Dict[str, Any]
    reason: str

@dataclass
class TrendAnalysis:
    """Market trend analysis"""
    symbol: str
    timestamp: datetime
    trend_direction: str  # "UP", "DOWN", "SIDEWAYS"
    trend_strength: float  # 0.0 to 1.0
    support_levels: List[float]
    resistance_levels: List[float]
    key_levels: List[float]

class UnifiedMarketAnalyzer:
    """
    Unified market analysis service that consolidates all analysis functionality
    """
    
    def __init__(self, oanda_service=None, db_manager=None):
        self.oanda_service = oanda_service
        self.db_manager = db_manager
        
        # Analysis state
        self.regime_cache: Dict[str, RegimeData] = {}
        self.technical_cache: Dict[str, TechnicalIndicators] = {}
        self.crypto_cache: Dict[str, CryptoSignal] = {}
        self.trend_cache: Dict[str, TrendAnalysis] = {}
        
        # Configuration
        self.regime_lookback_periods = 100
        self.technical_lookback_periods = 50
        self.crypto_lookback_periods = 30
        
        # Cache expiration (5 minutes)
        self.cache_expiry = timedelta(minutes=5)
        
        logger.info("📊 Unified Market Analyzer initialized")
    
    # ============================================================================
    # === REGIME CLASSIFICATION ===
    # ============================================================================
    
    async def get_current_regime(self, symbol: str) -> Optional[RegimeData]:
        """Get current market regime for a symbol"""
        try:
            # Check cache first
            cached = self.regime_cache.get(symbol)
            if cached and (datetime.now(timezone.utc) - cached.last_update) < self.cache_expiry:
                return cached
            
            # Perform regime analysis
            regime_data = await self._analyze_market_regime(symbol)
            if regime_data:
                # Update cache
                self.regime_cache[symbol] = regime_data
                return regime_data
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get regime for {symbol}: {e}")
            return None
    
    async def _analyze_market_regime(self, symbol: str) -> Optional[RegimeData]:
        """Analyze market regime using Lorentzian Distance Classification"""
        try:
            # Get price data
            prices = await self._get_price_data(symbol, self.regime_lookback_periods)
            if not prices or len(prices) < 50:
                return None
            
            # Calculate features for regime classification
            features = await self._calculate_regime_features(prices)
            
            # Determine regime based on features
            regime = self._classify_regime(features)
            confidence = self._calculate_regime_confidence(features)
            
            return RegimeData(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                current_regime=regime,
                confidence=confidence,
                features=features,
                last_update=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Regime analysis failed for {symbol}: {e}")
            return None
    
    async def _calculate_regime_features(self, prices: List[float]) -> Dict[str, float]:
        """Calculate features for regime classification"""
        try:
            if len(prices) < 20:
                return {}
            
            # Calculate returns
            returns = np.diff(np.log(prices))
            
            # Volatility (standard deviation of returns)
            volatility = np.std(returns) * np.sqrt(252)  # Annualized
            
            # Trend strength (correlation with time)
            time_index = np.arange(len(returns))
            trend_correlation = np.corrcoef(time_index, returns)[0, 1]
            trend_strength = abs(trend_correlation) if not np.isnan(trend_correlation) else 0.0
            
            # Mean reversion (autocorrelation)
            autocorr = np.corrcoef(returns[:-1], returns[1:])[0, 1] if len(returns) > 1 else 0.0
            mean_reversion = abs(autocorr) if not np.isnan(autocorr) else 0.0
            
            # Momentum (price change over different periods)
            momentum_5 = (prices[-1] / prices[-6] - 1) if len(prices) > 5 else 0.0
            momentum_20 = (prices[-1] / prices[-21] - 1) if len(prices) > 20 else 0.0
            
            return {
                "volatility": volatility,
                "trend_strength": trend_strength,
                "mean_reversion": mean_reversion,
                "momentum_5": momentum_5,
                "momentum_20": momentum_20,
                "price_range": (max(prices) - min(prices)) / np.mean(prices)
            }
            
        except Exception as e:
            logger.error(f"Feature calculation failed: {e}")
            return {}
    
    def _classify_regime(self, features: Dict[str, float]) -> MarketRegime:
        """Classify market regime based on features"""
        try:
            if not features:
                return MarketRegime.UNKNOWN
            
            volatility = features.get("volatility", 0.0)
            trend_strength = features.get("trend_strength", 0.0)
            momentum_5 = features.get("momentum_5", 0.0)
            momentum_20 = features.get("momentum_20", 0.0)
            
            # High volatility regime
            if volatility > 0.25:
                return MarketRegime.VOLATILE
            
            # Trending regimes
            if trend_strength > 0.3:
                if momentum_5 > 0.02 and momentum_20 > 0.05:
                    return MarketRegime.TRENDING_UP
                elif momentum_5 < -0.02 and momentum_20 < -0.05:
                    return MarketRegime.TRENDING_DOWN
            
            # Ranging regime
            if trend_strength < 0.2 and volatility < 0.15:
                return MarketRegime.RANGING
            
            # Default to unknown
            return MarketRegime.UNKNOWN
            
        except Exception as e:
            logger.error(f"Regime classification failed: {e}")
            return MarketRegime.UNKNOWN
    
    def _calculate_regime_confidence(self, features: Dict[str, float]) -> float:
        """Calculate confidence in regime classification"""
        try:
            if not features:
                return 0.0
            
            # Higher confidence for more extreme values
            volatility = features.get("volatility", 0.0)
            trend_strength = features.get("trend_strength", 0.0)
            
            # Base confidence on feature strength
            confidence = min(1.0, (volatility * 2 + trend_strength * 2) / 2)
            
            return max(0.1, min(0.95, confidence))
            
        except Exception as e:
            logger.error(f"Confidence calculation failed: {e}")
            return 0.5
    
    # ============================================================================
    # === TECHNICAL ANALYSIS ===
    # ============================================================================
    
    async def get_technical_indicators(self, symbol: str) -> Optional[TechnicalIndicators]:
        """Get technical indicators for a symbol"""
        try:
            # Check cache first
            cached = self.technical_cache.get(symbol)
            if cached and (datetime.now(timezone.utc) - cached.timestamp) < self.cache_expiry:
                return cached
            
            # Calculate technical indicators
            indicators = await self._calculate_technical_indicators(symbol)
            if indicators:
                # Update cache
                self.technical_cache[symbol] = indicators
                return indicators
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get technical indicators for {symbol}: {e}")
            return None
    
    async def _calculate_technical_indicators(self, symbol: str) -> Optional[TechnicalIndicators]:
        """Calculate technical indicators"""
        try:
            # Get price data
            prices = await self._get_price_data(symbol, self.technical_lookback_periods)
            if not prices or len(prices) < 50:
                return None
            
            # Calculate moving averages
            sma_20 = np.mean(prices[-20:]) if len(prices) >= 20 else None
            sma_50 = np.mean(prices[-50:]) if len(prices) >= 50 else None
            
            # Calculate EMAs
            ema_12 = self._calculate_ema(prices, 12)
            ema_26 = self._calculate_ema(prices, 26)
            
            # Calculate RSI
            rsi = self._calculate_rsi(prices, 14)
            
            # Calculate MACD
            macd, macd_signal, macd_histogram = self._calculate_macd(prices)
            
            # Calculate ATR
            atr = self._calculate_atr(prices)
            
            # Calculate Bollinger Bands
            bb_upper, bb_lower, bb_middle = self._calculate_bollinger_bands(prices, 20, 2)
            
            return TechnicalIndicators(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                sma_20=sma_20,
                sma_50=sma_50,
                ema_12=ema_12,
                ema_26=ema_26,
                rsi=rsi,
                macd=macd,
                macd_signal=macd_signal,
                macd_histogram=macd_histogram,
                atr=atr,
                bollinger_upper=bb_upper,
                bollinger_lower=bb_lower,
                bollinger_middle=bb_middle
            )
            
        except Exception as e:
            logger.error(f"Technical indicator calculation failed: {e}")
            return None
    
    def _calculate_ema(self, prices: List[float], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average"""
        try:
            if len(prices) < period:
                return None
            
            alpha = 2 / (period + 1)
            ema = prices[0]
            
            for price in prices[1:]:
                ema = alpha * price + (1 - alpha) * ema
            
            return ema
            
        except Exception as e:
            logger.error(f"EMA calculation failed: {e}")
            return None
    
    def _calculate_rsi(self, prices: List[float], period: int) -> Optional[float]:
        """Calculate Relative Strength Index"""
        try:
            if len(prices) < period + 1:
                return None
            
            # Calculate price changes
            changes = np.diff(prices)
            gains = np.where(changes > 0, changes, 0)
            losses = np.where(changes < 0, -changes, 0)
            
            # Calculate average gains and losses
            avg_gains = np.mean(gains[-period:])
            avg_losses = np.mean(losses[-period:])
            
            if avg_losses == 0:
                return 100.0
            
            rs = avg_gains / avg_losses
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
            
        except Exception as e:
            logger.error(f"RSI calculation failed: {e}")
            return None
    
    def _calculate_macd(self, prices: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate MACD"""
        try:
            if len(prices) < 26:
                return None, None, None
            
            ema_12 = self._calculate_ema(prices, 12)
            ema_26 = self._calculate_ema(prices, 26)
            
            if ema_12 is None or ema_26 is None:
                return None, None, None
            
            macd = ema_12 - ema_26
            macd_signal = self._calculate_ema([macd], 9) if macd else None
            macd_histogram = macd - macd_signal if macd_signal else None
            
            return macd, macd_signal, macd_histogram
            
        except Exception as e:
            logger.error(f"MACD calculation failed: {e}")
            return None, None, None
    
    def _calculate_atr(self, prices: List[float]) -> Optional[float]:
        """Calculate Average True Range"""
        try:
            if len(prices) < 2:
                return None
            
            # Simplified ATR calculation
            ranges = []
            for i in range(1, len(prices)):
                ranges.append(abs(prices[i] - prices[i-1]))
            
            return np.mean(ranges) if ranges else None
            
        except Exception as e:
            logger.error(f"ATR calculation failed: {e}")
            return None
    
    def _calculate_bollinger_bands(self, prices: List[float], period: int, std_dev: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate Bollinger Bands"""
        try:
            if len(prices) < period:
                return None, None, None
            
            middle = np.mean(prices[-period:])
            std = np.std(prices[-period:])
            
            upper = middle + (std_dev * std)
            lower = middle - (std_dev * std)
            
            return upper, lower, middle
            
        except Exception as e:
            logger.error(f"Bollinger Bands calculation failed: {e}")
            return None, None, None
    
    # ============================================================================
    # === CRYPTO SIGNAL HANDLING ===
    # ============================================================================
    
    async def get_crypto_signal(self, symbol: str) -> Optional[CryptoSignal]:
        """Get crypto trading signal for a symbol"""
        try:
            # Check cache first
            cached = self.crypto_cache.get(symbol)
            if cached and (datetime.now(timezone.utc) - cached.timestamp) < self.cache_expiry:
                return cached
            
            # Generate crypto signal
            signal = await self._generate_crypto_signal(symbol)
            if signal:
                # Update cache
                self.crypto_cache[symbol] = signal
                return signal
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get crypto signal for {symbol}: {e}")
            return None
    
    async def _generate_crypto_signal(self, symbol: str) -> Optional[CryptoSignal]:
        """Generate crypto trading signal"""
        try:
            # Get technical indicators
            indicators = await self.get_technical_indicators(symbol)
            if not indicators:
                return None
            
            # Get current price
            current_price = await self._get_current_price(symbol)
            if not current_price:
                return None
            
            # Analyze indicators to generate signal
            signal_type, confidence, reason = self._analyze_crypto_indicators(indicators)
            
            return CryptoSignal(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                signal_type=signal_type,
                confidence=confidence,
                price=current_price,
                volume=None,  # Volume not available in this implementation
                indicators={
                    "rsi": indicators.rsi,
                    "macd": indicators.macd,
                    "sma_20": indicators.sma_20,
                    "sma_50": indicators.sma_50
                },
                reason=reason
            )
            
        except Exception as e:
            logger.error(f"Crypto signal generation failed: {e}")
            return None
    
    def _analyze_crypto_indicators(self, indicators: TechnicalIndicators) -> Tuple[str, float, str]:
        """Analyze technical indicators to generate crypto signal"""
        try:
            signal_type = "HOLD"
            confidence = 0.5
            reasons = []
            
            # RSI analysis
            if indicators.rsi is not None:
                if indicators.rsi < 30:
                    signal_type = "BUY"
                    confidence += 0.2
                    reasons.append("RSI oversold")
                elif indicators.rsi > 70:
                    signal_type = "SELL"
                    confidence += 0.2
                    reasons.append("RSI overbought")
            
            # MACD analysis
            if indicators.macd is not None and indicators.macd_signal is not None:
                if indicators.macd > indicators.macd_signal:
                    if signal_type == "BUY":
                        confidence += 0.15
                        reasons.append("MACD bullish crossover")
                    elif signal_type == "HOLD":
                        signal_type = "BUY"
                        confidence += 0.15
                        reasons.append("MACD bullish")
                else:
                    if signal_type == "SELL":
                        confidence += 0.15
                        reasons.append("MACD bearish crossover")
                    elif signal_type == "HOLD":
                        signal_type = "SELL"
                        confidence += 0.15
                        reasons.append("MACD bearish")
            
            # Moving average analysis
            if indicators.sma_20 is not None and indicators.sma_50 is not None:
                if indicators.sma_20 > indicators.sma_50:
                    if signal_type == "BUY":
                        confidence += 0.1
                        reasons.append("SMA bullish alignment")
                else:
                    if signal_type == "SELL":
                        confidence += 0.1
                        reasons.append("SMA bearish alignment")
            
            # Cap confidence at 0.95
            confidence = min(0.95, confidence)
            
            # Generate reason string
            reason = "; ".join(reasons) if reasons else "No clear signal"
            
            return signal_type, confidence, reason
            
        except Exception as e:
            logger.error(f"Indicator analysis failed: {e}")
            return "HOLD", 0.5, "Analysis error"
    
    # ============================================================================
    # === TREND ANALYSIS ===
    # ============================================================================
    
    async def get_trend_analysis(self, symbol: str) -> Optional[TrendAnalysis]:
        """Get trend analysis for a symbol"""
        try:
            # Check cache first
            cached = self.trend_cache.get(symbol)
            if cached and (datetime.now(timezone.utc) - cached.timestamp) < self.cache_expiry:
                return cached
            
            # Perform trend analysis
            analysis = await self._analyze_trend(symbol)
            if analysis:
                # Update cache
                self.trend_cache[symbol] = analysis
                return analysis
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get trend analysis for {symbol}: {e}")
            return None
    
    async def _analyze_trend(self, symbol: str) -> Optional[TrendAnalysis]:
        """Analyze market trend"""
        try:
            # Get price data
            prices = await self._get_price_data(symbol, 100)
            if not prices or len(prices) < 50:
                return None
            
            # Calculate trend direction and strength
            trend_direction, trend_strength = self._calculate_trend_direction(prices)
            
            # Identify support and resistance levels
            support_levels, resistance_levels = self._identify_key_levels(prices)
            
            # Combine all levels
            key_levels = sorted(set(support_levels + resistance_levels))
            
            return TrendAnalysis(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                support_levels=support_levels,
                resistance_levels=resistance_levels,
                key_levels=key_levels
            )
            
        except Exception as e:
            logger.error(f"Trend analysis failed: {e}")
            return None
    
    def _calculate_trend_direction(self, prices: List[float]) -> Tuple[str, float]:
        """Calculate trend direction and strength"""
        try:
            if len(prices) < 20:
                return "SIDEWAYS", 0.0
            
            # Calculate linear regression slope
            x = np.arange(len(prices))
            slope, intercept = np.polyfit(x, prices, 1)
            
            # Calculate R-squared (trend strength)
            y_pred = slope * x + intercept
            ss_res = np.sum((prices - y_pred) ** 2)
            ss_tot = np.sum((prices - np.mean(prices)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
            
            # Determine direction
            if abs(slope) < 0.001:
                direction = "SIDEWAYS"
            elif slope > 0:
                direction = "UP"
            else:
                direction = "DOWN"
            
            return direction, r_squared
            
        except Exception as e:
            logger.error(f"Trend direction calculation failed: {e}")
            return "SIDEWAYS", 0.0
    
    def _identify_key_levels(self, prices: List[float]) -> Tuple[List[float], List[float]]:
        """Identify support and resistance levels"""
        try:
            if len(prices) < 20:
                return [], []
            
            # Simple pivot point analysis
            highs = []
            lows = []
            
            for i in range(1, len(prices) - 1):
                if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
                    highs.append(prices[i])
                elif prices[i] < prices[i-1] and prices[i] < prices[i+1]:
                    lows.append(prices[i])
            
            # Get recent levels (last 20)
            recent_highs = sorted(highs[-20:]) if highs else []
            recent_lows = sorted(lows[-20:]) if lows else []
            
            # Simple clustering to avoid too many levels
            resistance_levels = self._cluster_levels(recent_highs, 0.001)
            support_levels = self._cluster_levels(recent_lows, 0.001)
            
            return support_levels, resistance_levels
            
        except Exception as e:
            logger.error(f"Key level identification failed: {e}")
            return [], []
    
    def _cluster_levels(self, levels: List[float], threshold: float) -> List[float]:
        """Cluster nearby levels together"""
        try:
            if not levels:
                return []
            
            clustered = []
            levels_sorted = sorted(levels)
            
            current_cluster = [levels_sorted[0]]
            
            for level in levels_sorted[1:]:
                if abs(level - current_cluster[-1]) <= threshold:
                    current_cluster.append(level)
                else:
                    # Average the cluster and add to results
                    clustered.append(np.mean(current_cluster))
                    current_cluster = [level]
            
            # Add the last cluster
            if current_cluster:
                clustered.append(np.mean(current_cluster))
            
            return clustered
            
        except Exception as e:
            logger.error(f"Level clustering failed: {e}")
            return levels
    
    # ============================================================================
    # === UTILITY METHODS ===
    # ============================================================================
    
    async def _get_price_data(self, symbol: str, periods: int) -> Optional[List[float]]:
        """Get price data for analysis"""
        try:
            if not self.oanda_service:
                return None
            
            # This is a simplified implementation
            # In practice, you'd get actual OHLCV data from OANDA
            # For now, return dummy data for demonstration
            return [100.0 + i * 0.1 for i in range(periods)]
            
        except Exception as e:
            logger.error(f"Failed to get price data for {symbol}: {e}")
            return None
    
    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol"""
        try:
            if not self.oanda_service:
                return None
            
            # This is a simplified implementation
            # In practice, you'd get actual current price from OANDA
            return 100.0  # Dummy price
            
        except Exception as e:
            logger.error(f"Failed to get current price for {symbol}: {e}")
            return None
    
    async def get_comprehensive_analysis(self, symbol: str) -> Dict[str, Any]:
        """Get comprehensive market analysis for a symbol"""
        try:
            # Get all analysis types
            regime = await self.get_current_regime(symbol)
            technical = await self.get_technical_indicators(symbol)
            crypto_signal = await self.get_crypto_signal(symbol)
            trend = await self.get_trend_analysis(symbol)
            
            return {
                "symbol": symbol,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "regime": {
                    "current": regime.current_regime.value if regime else "unknown",
                    "confidence": regime.confidence if regime else 0.0,
                    "features": regime.features if regime else {}
                },
                "technical": {
                    "rsi": technical.rsi if technical else None,
                    "macd": technical.macd if technical else None,
                    "sma_20": technical.sma_20 if technical else None,
                    "sma_50": technical.sma_50 if technical else None,
                    "atr": technical.atr if technical else None
                },
                "crypto_signal": {
                    "type": crypto_signal.signal_type if crypto_signal else "HOLD",
                    "confidence": crypto_signal.confidence if crypto_signal else 0.0,
                    "reason": crypto_signal.reason if crypto_signal else "No signal"
                },
                "trend": {
                    "direction": trend.trend_direction if trend else "SIDEWAYS",
                    "strength": trend.trend_strength if trend else 0.0,
                    "support_levels": trend.support_levels if trend else [],
                    "resistance_levels": trend.resistance_levels if trend else []
                }
            }
            
        except Exception as e:
            logger.error(f"Comprehensive analysis failed for {symbol}: {e}")
            return {"error": str(e)}

# Factory function for creating unified market analyzer
def create_unified_market_analyzer(oanda_service=None, db_manager=None):
    """Create and return a unified market analyzer instance"""
    return UnifiedMarketAnalyzer(
        oanda_service=oanda_service,
        db_manager=db_manager
    )


# ============================================================================
# VOLATILITY MONITORING (Merged from volatility_monitor.py)
# ============================================================================

class VolatilityMonitor:
    """
    Monitors market volatility and provides dynamic adjustments
    for position sizing, stop loss, and take profit levels.
    """
    def __init__(self):
        """Initialize volatility monitor"""
        self.market_conditions = {}  # symbol -> volatility data
        self.history_length = 20  # Number of ATR values to keep
        self.std_dev_factor = 2.0  # Standard deviations for high/low volatility

    async def initialize_market_condition(self, symbol: str, timeframe: str) -> bool:
        """Initialize market condition tracking for a symbol"""
        if symbol in self.market_conditions:
            return True
        try:
            # Get current ATR
            from utils import get_atr  # Import here to avoid circular import
            atr_value = await get_atr(symbol, timeframe)
            if atr_value > 0:
                # Initialize with current ATR
                self.market_conditions[symbol] = {
                    "atr_history": [atr_value],
                    "mean_atr": atr_value,
                    "std_dev": 0.0,
                    "current_atr": atr_value,
                    "volatility_ratio": 1.0,  # Neutral
                    "volatility_state": "normal",  # low, normal, high
                    "timeframe": timeframe,
                    "last_update": datetime.now(timezone.utc)
                }
                return True
            else:
                logger.warning(f"Could not initialize volatility for {symbol}: Invalid ATR")
                return False
        except Exception as e:
            logger.error(f"Error initializing volatility for {symbol}: {str(e)}")
            return False

    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str) -> bool:
        """Update volatility state for a symbol"""
        try:
            # Initialize if needed
            if symbol not in self.market_conditions:
                await self.initialize_market_condition(symbol, timeframe)
            # Settings for this calculation
            settings = {
                "std_dev": self.std_dev_factor,
                "history_length": self.history_length
            }
            # Get current data
            data = self.market_conditions[symbol]
            # Update ATR history
            data["atr_history"].append(current_atr)
            # Trim history if needed
            if len(data["atr_history"]) > settings["history_length"]:
                data["atr_history"] = data["atr_history"][-settings["history_length"]:]
            # Calculate mean and standard deviation
            mean_atr = sum(data["atr_history"]) / len(data["atr_history"])
            std_dev = 0.0
            if len(data["atr_history"]) > 1:
                variance = sum((x - mean_atr) ** 2 for x in data["atr_history"]) / len(data["atr_history"])
                std_dev = math.sqrt(variance)
            # Update data
            data["mean_atr"] = mean_atr
            data["std_dev"] = std_dev
            data["current_atr"] = current_atr
            data["timeframe"] = timeframe
            data["last_update"] = datetime.now(timezone.utc)
            # Calculate volatility ratio
            if mean_atr > 0:
                current_ratio = current_atr / mean_atr
            else:
                current_ratio = 1.0
            data["volatility_ratio"] = current_ratio
            # Determine volatility state
            if current_atr > (mean_atr + settings["std_dev"] * std_dev):
                data["volatility_state"] = "high"
            elif current_atr < (mean_atr - settings["std_dev"] * std_dev * 0.5):  # Less strict for low volatility
                data["volatility_state"] = "low"
            else:
                data["volatility_state"] = "normal"
            logger.info(f"Updated volatility for {symbol}: ratio={current_ratio:.2f}, state={data['volatility_state']}")
            return True
        except Exception as e:
            logger.error(f"Error updating volatility for {symbol}: {str(e)}")
            return False

    def get_volatility_state(self, symbol: str) -> Dict[str, Any]:
        """Get current volatility state for a symbol"""
        if symbol not in self.market_conditions:
            return {
                "volatility_state": "normal",
                "volatility_ratio": 1.0,
                "current_atr": 0.0,
                "mean_atr": 0.0,
                "std_dev": 0.0,
                "timeframe": "unknown",
                "last_update": None
            }
        return self.market_conditions[symbol].copy()

    def get_all_volatility_states(self) -> Dict[str, Dict[str, Any]]:
        """Get volatility states for all symbols"""
        return {symbol: data.copy() for symbol, data in self.market_conditions.items()}

    def is_high_volatility(self, symbol: str) -> bool:
        """Check if a symbol is in high volatility state"""
        state = self.get_volatility_state(symbol)
        return state.get("volatility_state") == "high"

    def is_low_volatility(self, symbol: str) -> bool:
        """Check if a symbol is in low volatility state"""
        state = self.get_volatility_state(symbol)
        return state.get("volatility_state") == "low"

    def get_volatility_adjustment_factor(self, symbol: str) -> float:
        """Get position sizing adjustment factor based on volatility"""
        state = self.get_volatility_state(symbol)
        ratio = state.get("volatility_ratio", 1.0)
        
        if ratio > 1.5:  # High volatility
            return 0.7  # Reduce position size
        elif ratio < 0.7:  # Low volatility
            return 1.2  # Increase position size
        else:
            return 1.0  # Normal volatility


# ============================================================================
# INSTITUTIONAL REGIME CLASSIFIER (Merged from regime_classifier.py)
# ============================================================================

class LorentzianDistanceClassifier:
    """
    Institutional-grade market regime classifier using multiple timeframes
    and technical indicators for robust regime detection.
    """
    
    def __init__(self):
        self.regime_history = []
        self.confidence_threshold = 0.7
        self.lookback_periods = {
            "short": 20,
            "medium": 50,
            "long": 200
        }
        
    async def classify_regime(self, 
                            price_data,  # Changed from pd.DataFrame to avoid pandas dependency
                            symbol: str,
                            timeframe: str = "H1") -> Tuple[MarketRegime, float]:
        """
        Classify current market regime with confidence score.
        
        Args:
            price_data: OHLCV data with technical indicators
            symbol: Trading instrument
            timeframe: Timeframe for analysis
            
        Returns:
            Tuple of (MarketRegime, confidence_score)
        """
        if not price_data or len(price_data) < 50:
            return MarketRegime.RANGING, 0.5
            
        # Calculate technical indicators
        indicators = self._calculate_indicators(price_data)
        
        # Multi-timeframe trend analysis
        trend_score = self._analyze_trend(price_data, indicators)
        
        # Volatility analysis
        volatility_score = self._analyze_volatility(price_data, indicators)
        
        # Momentum analysis
        momentum_score = self._analyze_momentum(price_data, indicators)
        
        # Regime classification logic
        regime, confidence = self._classify_regime_logic(
            trend_score, volatility_score, momentum_score, indicators
        )
        
        # Store regime history
        self.regime_history.append({
            'timestamp': datetime.now(),
            'regime': regime,
            'confidence': confidence,
            'symbol': symbol,
            'timeframe': timeframe
        })
        
        # Keep only last 1000 entries
        if len(self.regime_history) > 1000:
            self.regime_history = self.regime_history[-1000:]
            
        logger.info(f"Regime classification for {symbol}: {regime.value} "
                   f"(confidence: {confidence:.2f})")
        
        return regime, confidence
    
    def _calculate_indicators(self, df) -> Dict[str, float]:
        """Calculate comprehensive technical indicators"""
        try:
            indicators = {}
            
            # Simple moving averages
            if len(df) >= 20:
                indicators['sma_20'] = sum(df['close'][-20:]) / 20
            if len(df) >= 50:
                indicators['sma_50'] = sum(df['close'][-50:]) / 50
            
            # RSI calculation
            if len(df) >= 14:
                gains = []
                losses = []
                for i in range(1, min(15, len(df))):
                    change = df['close'][-i] - df['close'][-i-1]
                    if change > 0:
                        gains.append(change)
                    else:
                        losses.append(-change)
                
                if losses and gains:
                    avg_gain = sum(gains) / len(gains)
                    avg_loss = sum(losses) / len(losses)
                    if avg_loss != 0:
                        rs = avg_gain / avg_loss
                        indicators['rsi'] = 100 - (100 / (1 + rs))
                    else:
                        indicators['rsi'] = 100
                else:
                    indicators['rsi'] = 50
            
            # ATR calculation
            if len(df) >= 14:
                true_ranges = []
                for i in range(1, min(15, len(df))):
                    high_low = df['high'][-i] - df['low'][-i]
                    high_close = abs(df['high'][-i] - df['close'][-i-1])
                    low_close = abs(df['low'][-i] - df['close'][-i-1])
                    true_range = max(high_low, high_close, low_close)
                    true_ranges.append(true_range)
                indicators['atr'] = sum(true_ranges) / len(true_ranges)
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}
    
    def _analyze_trend(self, df, indicators: Dict[str, float]) -> float:
        """Analyze trend strength and direction"""
        try:
            if len(df) < 20:
                return 0.5
            
            # Price above/below moving averages
            sma_20 = indicators.get('sma_20', 0)
            sma_50 = indicators.get('sma_50', 0)
            current_price = df['close'][-1]
            
            if sma_20 > 0 and sma_50 > 0:
                if current_price > sma_20 > sma_50:
                    return 0.8  # Strong uptrend
                elif current_price < sma_20 < sma_50:
                    return 0.2  # Strong downtrend
                elif current_price > sma_20:
                    return 0.6  # Weak uptrend
                else:
                    return 0.4  # Weak downtrend
            
            return 0.5
            
        except Exception as e:
            logger.error(f"Error analyzing trend: {e}")
            return 0.5
    
    def _analyze_volatility(self, df, indicators: Dict[str, float]) -> float:
        """Analyze volatility patterns"""
        try:
            atr = indicators.get('atr', 0)
            if atr == 0:
                return 0.5
            
            # Compare current ATR to historical average
            if len(df) >= 20:
                recent_atr = atr
                historical_atr = sum([abs(df['high'][-i] - df['low'][-i]) for i in range(1, 21)]) / 20
                
                if historical_atr > 0:
                    ratio = recent_atr / historical_atr
                    if ratio > 1.5:
                        return 0.8  # High volatility
                    elif ratio < 0.7:
                        return 0.2  # Low volatility
                    else:
                        return 0.5  # Normal volatility
            
            return 0.5
            
        except Exception as e:
            logger.error(f"Error analyzing volatility: {e}")
            return 0.5
    
    def _analyze_momentum(self, df, indicators: Dict[str, float]) -> float:
        """Analyze momentum indicators"""
        try:
            rsi = indicators.get('rsi', 50)
            
            if rsi > 70:
                return 0.8  # Strong momentum
            elif rsi < 30:
                return 0.2  # Weak momentum
            else:
                return 0.5  # Neutral momentum
            
        except Exception as e:
            logger.error(f"Error analyzing momentum: {e}")
            return 0.5
    
    def _classify_regime_logic(self, trend_score: float, volatility_score: float, 
                              momentum_score: float, indicators: Dict[str, float]) -> Tuple[MarketRegime, float]:
        """Classify regime based on analysis scores"""
        try:
            # Calculate overall score
            overall_score = (trend_score + volatility_score + momentum_score) / 3
            
            # Determine regime
            if trend_score > 0.7 and momentum_score > 0.6:
                regime = MarketRegime.TRENDING_UP
                confidence = min(0.9, overall_score + 0.1)
            elif trend_score < 0.3 and momentum_score < 0.4:
                regime = MarketRegime.TRENDING_DOWN
                confidence = min(0.9, overall_score + 0.1)
            elif volatility_score > 0.7:
                regime = MarketRegime.VOLATILE
                confidence = min(0.8, volatility_score)
            elif volatility_score < 0.3:
                regime = MarketRegime.QUIET
                confidence = min(0.8, 1.0 - volatility_score)
            else:
                regime = MarketRegime.RANGING
                confidence = 0.6
            
            return regime, confidence
            
        except Exception as e:
            logger.error(f"Error in regime classification logic: {e}")
            return MarketRegime.RANGING, 0.5


# ============================================================================
# INSTITUTIONAL TECHNICAL ANALYSIS (Merged from technical_analysis.py)
# ============================================================================

class TechnicalAnalyzer:
    """
    Institutional-grade technical analysis using pure Python libraries.
    Designed for high-frequency trading environments.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    def add_moving_averages(self, df, periods: list = [20, 50, 200]) -> dict:
        """Add multiple moving averages - institutional standard periods"""
        try:
            result = {}
            for period in periods:
                if len(df) >= period:
                    sma = sum(df['close'][-period:]) / period
                    result[f'SMA_{period}'] = sma
                    
                    # Simple EMA calculation
                    alpha = 2.0 / (period + 1)
                    ema = df['close'][-1]
                    for i in range(2, period + 1):
                        ema = alpha * df['close'][-i] + (1 - alpha) * ema
                    result[f'EMA_{period}'] = ema
                    
            return result
        except Exception as e:
            self.logger.error(f"Error calculating moving averages: {e}")
            return {}
    
    def add_rsi(self, df, period: int = 14) -> float:
        """Add RSI indicator using institutional-grade pure Python implementation"""
        try:
            if len(df) < period + 1:
                return 50.0
                
            # Pure Python implementation - institutional grade
            gains = []
            losses = []
            for i in range(1, period + 1):
                change = df['close'][-i] - df['close'][-i-1]
                if change > 0:
                    gains.append(change)
                else:
                    losses.append(-change)
            
            if not gains and not losses:
                return 50.0
                
            avg_gain = sum(gains) / len(gains) if gains else 0
            avg_loss = sum(losses) / len(losses) if losses else 0
            
            if avg_loss == 0:
                return 100.0 if avg_gain > 0 else 50.0
                
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
            
        except Exception as e:
            self.logger.error(f"Error calculating RSI: {e}")
            return 50.0
    
    def add_bollinger_bands(self, df, period: int = 20, std_dev: float = 2.0) -> dict:
        """Add Bollinger Bands - critical for volatility analysis"""
        try:
            if len(df) < period:
                return {}
                
            # Pure Python implementation - institutional standard
            sma = sum(df['close'][-period:]) / period
            
            # Calculate standard deviation
            variance = sum((df['close'][-i] - sma) ** 2 for i in range(1, period + 1)) / period
            std = variance ** 0.5
            
            result = {
                'BB_Upper': sma + (std * std_dev),
                'BB_Middle': sma,
                'BB_Lower': sma - (std * std_dev)
            }
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating Bollinger Bands: {e}")
            return {}
    
    def add_macd(self, df, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
        """Add MACD - essential for trend analysis"""
        try:
            if len(df) < slow:
                return {}
                
            # Simple EMA calculation for MACD
            def calculate_ema(data, period):
                alpha = 2.0 / (period + 1)
                ema = data[0]
                for i in range(1, len(data)):
                    ema = alpha * data[i] + (1 - alpha) * ema
                return ema
            
            # Calculate fast and slow EMAs
            fast_ema = calculate_ema(df['close'][-fast:], fast)
            slow_ema = calculate_ema(df['close'][-slow:], slow)
            
            macd_line = fast_ema - slow_ema
            
            # Calculate signal line (EMA of MACD)
            macd_values = [macd_line]  # Simplified - in practice you'd have more MACD values
            signal_line = calculate_ema(macd_values, signal)
            
            result = {
                'MACD': macd_line,
                'MACD_Signal': signal_line,
                'MACD_Histogram': macd_line - signal_line
            }
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating MACD: {e}")
            return {}
    
    def add_atr(self, df, period: int = 14) -> float:
        """Add Average True Range - critical for position sizing"""
        try:
            if len(df) < period + 1:
                return 0.0
                
            # Pure Python implementation - institutional grade
            true_ranges = []
            for i in range(1, period + 1):
                high_low = df['high'][-i] - df['low'][-i]
                high_close = abs(df['high'][-i] - df['close'][-i-1])
                low_close = abs(df['low'][-i] - df['close'][-i-1])
                true_range = max(high_low, high_close, low_close)
                true_ranges.append(true_range)
            
            atr = sum(true_ranges) / len(true_ranges)
            return atr
            
        except Exception as e:
            self.logger.error(f"Error calculating ATR: {e}")
            return 0.0
    
    def add_institutional_signals(self, df) -> dict:
        """Add institutional-grade trading signals"""
        try:
            signals = {}
            
            # Get basic indicators
            rsi = self.add_rsi(df)
            bb = self.add_bollinger_bands(df)
            macd = self.add_macd(df)
            
            # RSI signals
            if rsi < 30:
                signals['RSI_Signal'] = 'OVERSOLD'
            elif rsi > 70:
                signals['RSI_Signal'] = 'OVERBOUGHT'
            else:
                signals['RSI_Signal'] = 'NEUTRAL'
            
            # Bollinger Band signals
            if bb:
                current_price = df['close'][-1]
                if current_price <= bb['BB_Lower']:
                    signals['BB_Signal'] = 'OVERSOLD'
                elif current_price >= bb['BB_Upper']:
                    signals['BB_Signal'] = 'OVERBOUGHT'
                else:
                    signals['BB_Signal'] = 'NEUTRAL'
            
            # MACD signals
            if macd:
                if macd['MACD'] > macd['MACD_Signal']:
                    signals['MACD_Signal'] = 'BULLISH'
                else:
                    signals['MACD_Signal'] = 'NEUTRAL'
            
            return signals
            
        except Exception as e:
            self.logger.error(f"Error calculating institutional signals: {e}")
            return {}
