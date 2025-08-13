"""
Unified Market Analysis Service
Consolidates all market analysis functionality into a single, comprehensive service:
- Regime classification
- Technical analysis
- Crypto signal handling
- Market trend analysis
"""

import asyncio
import logging
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
