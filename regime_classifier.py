"""
INSTITUTIONAL MARKET REGIME CLASSIFIER
Advanced market regime detection using multiple technical indicators
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
import logging
from datetime import datetime, timedelta
from enum import Enum
import statistics
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from utils import logger, get_module_logger
from config import config
import asyncio

logger = logging.getLogger(__name__)

class MarketRegime(Enum):
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    VOLATILE = "volatile"
    QUIET = "quiet"
    BREAKOUT = "breakout"

class InstitutionalRegimeClassifier:
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
                            price_data: pd.DataFrame,
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
        if price_data.empty or len(price_data) < 50:
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
    
    def _calculate_indicators(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate comprehensive technical indicators"""
        indicators = {}
        
        # ATR for volatility
        indicators['atr'] = self._calculate_atr(df, period=14)
        
        # Moving averages
        indicators['sma_20'] = df['close'].rolling(20).mean().iloc[-1]
        indicators['sma_50'] = df['close'].rolling(50).mean().iloc[-1]
        indicators['ema_12'] = df['close'].ewm(span=12).mean().iloc[-1]
        indicators['ema_26'] = df['close'].ewm(span=26).mean().iloc[-1]
        
        # RSI
        indicators['rsi'] = self._calculate_rsi(df, period=14)
        
        # Bollinger Bands
        bb_upper, bb_middle, bb_lower = self._calculate_bollinger_bands(df, period=20)
        indicators['bb_upper'] = bb_upper
        indicators['bb_middle'] = bb_middle
        indicators['bb_lower'] = bb_lower
        indicators['bb_width'] = (bb_upper - bb_lower) / bb_middle
        
        # MACD
        macd, signal, histogram = self._calculate_macd(df)
        indicators['macd'] = macd
        indicators['macd_signal'] = signal
        indicators['macd_histogram'] = histogram
        
        # ADX for trend strength
        indicators['adx'] = self._calculate_adx(df, period=14)
        
        return indicators
    
    def _analyze_trend(self, df: pd.DataFrame, indicators: Dict[str, float]) -> float:
        """Analyze trend strength and direction"""
        current_price = df['close'].iloc[-1]
        
        # Moving average alignment
        if current_price > indicators['sma_20'] > indicators['sma_50']:
            ma_alignment = 1.0
        elif current_price < indicators['sma_20'] < indicators['sma_50']:
            ma_alignment = -1.0
        else:
            ma_alignment = 0.0            
        # EMA alignment
        ema_alignment = 1.0 if indicators['ema_12'] > indicators['ema_26'] else -1.0
        
        # MACD trend
        macd_trend = 1.0 if indicators['macd'] > indicators['macd_signal'] else -1.0
        
        # ADX trend strength (normalized)
        adx_strength = min(indicators['adx'] / 50.0, 1.0)
        
        # Combined trend score
        trend_score = (ma_alignment * 0.3 + 
                      ema_alignment * 0.3 + 
                      macd_trend * 0.2 + 
                      adx_strength * 0.2)
        
        return trend_score
    
    def _analyze_volatility(self, df: pd.DataFrame, indicators: Dict[str, float]) -> float:
        """Analyze volatility conditions"""
        # Historical ATR average
        atr_history = self._calculate_atr_series(df, period=14)
        current_atr = indicators['atr']
        avg_atr = atr_history.mean()
        
        # Volatility ratio
        vol_ratio = current_atr / avg_atr if avg_atr > 0 else 1.0
        
        # Bollinger Band width
        bb_width = indicators['bb_width']
        
        # Volatility classification
        if vol_ratio > 1.5 or bb_width > 0.1:
            return 1.0  # High volatility
        elif vol_ratio < 0.7 or bb_width < 0.03:
            return -1.0  # Low volatility
        else:
            return 0.0  # Normal volatility
    
    def _analyze_momentum(self, df: pd.DataFrame, indicators: Dict[str, float]) -> float:
        """Analyze momentum conditions"""
        rsi = indicators['rsi']
        
        # RSI momentum
        if rsi > 70:
            momentum = -0.5  # Overbought
        elif rsi < 30:
            momentum = 0.5   # Oversold
        else:
            momentum = 0.0
            
        # MACD momentum
        macd_momentum = np.sign(indicators['macd_histogram'])
        
        # Combined momentum
        momentum_score = (momentum * 0.6 + macd_momentum * 0.4)
        
        return momentum_score
    
    def _classify_regime_logic(self, 
                             trend_score: float, 
                             volatility_score: float, 
                             momentum_score: float,
                             indicators: Dict[str, float]) -> Tuple[MarketRegime, float]:
        """Classify regime based on technical analysis"""
        
        # Trend-dominated regimes
        if abs(trend_score) > 0.6:
            if trend_score > 0:
                return MarketRegime.TRENDING_UP, min(abs(trend_score), 1.0)
            else:
                return MarketRegime.TRENDING_DOWN, min(abs(trend_score), 1.0)
        
        # Volatility-dominated regimes
        if abs(volatility_score) > 0.7:
            if volatility_score > 0:
                return MarketRegime.VOLATILE, min(abs(volatility_score), 1.0)
            else:
                return MarketRegime.QUIET, min(abs(volatility_score), 1.0)
        
        # Breakout detection
        current_price = indicators.get('close', 0)
        bb_upper = indicators.get('bb_upper', 0)
        bb_lower = indicators.get('bb_lower', 0)
        
        if current_price > bb_upper * 1.01 or current_price < bb_lower * 0.99:
            return MarketRegime.BREAKOUT, 0.8
        
        # Default to ranging
        return MarketRegime.RANGING, 0.6
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        
        return atr
    
    def _calculate_atr_series(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate ATR series"""
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        return atr
    
    def _calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate RSI"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1]
    
    def _calculate_bollinger_bands(self, df: pd.DataFrame, period: int = 20) -> Tuple[float, float, float]:
        """Calculate Bollinger Bands"""
        sma = df['close'].rolling(period).mean()
        std = df['close'].rolling(period).std()
        
        upper = sma + (std * 2)
        lower = sma - (std * 2)
        
        return upper.iloc[-1], sma.iloc[-1], lower.iloc[-1]
    
    def _calculate_macd(self, df: pd.DataFrame) -> Tuple[float, float, float]:
        """Calculate MACD"""
        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        histogram = macd - signal
        
        return macd.iloc[-1], signal.iloc[-1], histogram.iloc[-1]
    
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ADX (simplified version)"""
        # Simplified ADX calculation
        high = df['high']
        low = df['low']
        close = df['close']
        
        # Calculate +DM and -DM
        high_diff = high.diff()
        low_diff = low.diff()
        
        plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0)
        minus_dm = -low_diff.where((low_diff > high_diff) & (low_diff > 0), 0)
        
        # Calculate TR
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Smooth the values
        plus_di = 100 * (plus_dm.rolling(period).mean() / tr.rolling(period).mean())
        minus_di = 100 * (minus_dm.rolling(period).mean() / tr.rolling(period).mean())
        
        # Calculate DX and ADX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(period).mean()
        
        return adx.iloc[-1]
    
    def get_regime_history(self, symbol: str = None, 
                          timeframe: str = None, 
                          hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent regime history"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        filtered_history = [
            entry for entry in self.regime_history
            if entry['timestamp'] >= cutoff_time
        ]
        
        if symbol:
            filtered_history = [
                entry for entry in filtered_history
                if entry['symbol'] == symbol
            ]
            
        if timeframe:
            filtered_history = [
                entry for entry in filtered_history
                if entry['timeframe'] == timeframe
            ]
            
        return filtered_history

class LorentzianDistanceClassifier:
    def __init__(self, lookback_period: int = 20, max_history: int = 1000):
        """Initialize with history limits"""
        self.lookback_period = lookback_period
        self.max_history = max_history
        self.price_history = {}  # symbol -> List[float]
        self.regime_history = {} # symbol -> List[str] (history of classified regimes)
        self.volatility_history = {} # symbol -> List[float]
        self.atr_history = {}  # symbol -> List[float]
        self.regimes = {}  # symbol -> Dict[str, Any] (stores latest regime data)
        self._lock = asyncio.Lock()
        self.logger = get_module_logger(__name__)

    async def add_price_data(self, symbol: str, price: float, timeframe: str, atr: Optional[float] = None):
        """Add price data with limits"""
        async with self._lock:
            # Initialize if needed
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            if symbol not in self.regime_history:
                self.regime_history[symbol] = []
            if symbol not in self.volatility_history:
                self.volatility_history[symbol] = []
            if symbol not in self.atr_history:
                self.atr_history[symbol] = []
            # Add new data
            self.price_history[symbol].append(price)
            # Enforce history limit
            if len(self.price_history[symbol]) > self.max_history:
                self.price_history[symbol] = self.price_history[symbol][-self.max_history:]
            # Update ATR history if provided
            if atr is not None:
                self.atr_history[symbol].append(atr)
                if len(self.atr_history[symbol]) > self.lookback_period:
                    self.atr_history[symbol].pop(0)
            # Update regime if we have enough data
            if len(self.price_history[symbol]) >= 2:
                await self.classify_market_regime(symbol, price, atr)

    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """Calculate Lorentzian distance between current price and historical prices"""
        if not history:
            return 0.0
        distances = [np.log(1 + abs(price - hist_price)) for hist_price in history]
        return float(np.mean(distances)) if distances else 0.0

    async def classify_market_regime(self, symbol: str, current_price: float, atr: Optional[float] = None) -> Dict[str, Any]:
        """Classify current market regime using multiple factors"""
        async with self._lock:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
                unknown_state = {"regime": "unknown", "volatility": 0.0, "momentum": 0.0, "price_distance": 0.0, "regime_strength": 0.0, "last_update": datetime.now(timezone.utc).isoformat()}
                self.regimes[symbol] = unknown_state
                return unknown_state
            try:
                price_distance = await self.calculate_lorentzian_distance(
                    current_price, self.price_history[symbol][:-1]
                )
                returns = []
                prices = self.price_history[symbol]
                for i in range(1, len(prices)):
                    if prices[i-1] != 0:
                        returns.append(prices[i] / prices[i-1] - 1)
                volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
                momentum = (current_price - prices[0]) / prices[0] if prices[0] != 0 else 0.0
                mean_atr = np.mean(self.atr_history[symbol]) if self.atr_history.get(symbol) else 0.0
                regime = "unknown"
                regime_strength = 0.5
                if price_distance < 0.1 and volatility < 0.001:
                    regime = "ranging"
                    regime_strength = min(1.0, 0.7 + (0.1 - price_distance) * 3)
                elif price_distance > 0.3 and abs(momentum) > 0.002:
                    regime = "trending_up" if momentum > 0 else "trending_down"
                    regime_strength = min(1.0, 0.6 + price_distance + abs(momentum) * 10)
                elif volatility > 0.003 or (mean_atr > 0 and atr is not None and atr > 1.5 * mean_atr):
                    regime = "volatile"
                    regime_strength = min(1.0, 0.6 + volatility * 100)
                elif abs(momentum) > 0.003:
                    regime = "momentum_up" if momentum > 0 else "momentum_down"
                    regime_strength = min(1.0, 0.6 + abs(momentum) * 50)
                else:
                    regime = "mixed"
                    regime_strength = 0.5
                self.regime_history[symbol].append(regime)
                if len(self.regime_history[symbol]) > self.lookback_period:
                    self.regime_history[symbol].pop(0)
                self.volatility_history[symbol].append(volatility)
                if len(self.volatility_history[symbol]) > self.lookback_period:
                    self.volatility_history[symbol].pop(0)
                result = {
                    "regime": regime,
                    "regime_strength": regime_strength,
                    "price_distance": price_distance,
                    "volatility": volatility,
                    "momentum": momentum,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "metrics": {
                        "price_distance": price_distance,
                        "volatility": volatility,
                        "momentum": momentum,
                        "lookback_period": self.lookback_period
                    }
                }
                self.regimes[symbol] = result
                if "classification_history" not in self.regimes[symbol]:
                    self.regimes[symbol]["classification_history"] = []
                self.regimes[symbol]["classification_history"].append({
                    "regime": regime,
                    "strength": regime_strength,
                    "timestamp": result["last_update"]
                })
                hist_limit = 20
                if len(self.regimes[symbol]["classification_history"]) > hist_limit:
                    self.regimes[symbol]["classification_history"] = self.regimes[symbol]["classification_history"][-hist_limit:]
                return result
            except Exception as e:
                self.logger.error(f"Error classifying regime for {symbol}: {str(e)}", exc_info=True)
                error_state = {"regime": "error", "regime_strength": 0.0, "last_update": datetime.now(timezone.utc).isoformat(), "error": str(e)}
                self.regimes[symbol] = error_state
                return error_state

    def get_dominant_regime(self, symbol: str) -> str:
        """Get the dominant regime over recent history (uses internal state)"""
        if symbol not in self.regime_history or len(self.regime_history[symbol]) < 3:
            return "unknown"
        recent_regimes = self.regime_history[symbol][-5:]
        regime_counts = {}
        for regime in recent_regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        if regime_counts:
            dominant_regime, count = max(regime_counts.items(), key=lambda item: item[1])
            if count / len(recent_regimes) >= 0.6:
                return dominant_regime
        return "mixed"

    def get_regime_data(self, symbol: str) -> Dict[str, Any]:
        """Get the latest calculated market regime data for a symbol"""
        regime_data = self.regimes.get(symbol, {})
        if "last_update" not in regime_data:
            regime_data["last_update"] = datetime.now(timezone.utc).isoformat()
        elif isinstance(regime_data["last_update"], datetime):
            regime_data["last_update"] = regime_data["last_update"].isoformat()
        if not regime_data:
            return {
                "regime": "unknown",
                "regime_strength": 0.0,
                "last_update": datetime.now(timezone.utc).isoformat()
            }
        return regime_data.copy()

    def is_suitable_for_strategy(self, symbol: str, strategy_type: str) -> bool:
        """Determine if the current market regime is suitable for a strategy"""
        regime_data = self.regimes.get(symbol)
        if not regime_data or "regime" not in regime_data:
            self.logger.warning(f"No regime data found for {symbol}, allowing strategy '{strategy_type}' by default.")
            return True
        regime = regime_data["regime"]
        self.logger.debug(f"Checking strategy suitability for {symbol}: Strategy='{strategy_type}', Regime='{regime}'")
        if strategy_type == "trend_following":
            is_suitable = "trending" in regime
        elif strategy_type == "mean_reversion":
            is_suitable = regime in ["ranging", "mixed"]
        elif strategy_type == "breakout":
            is_suitable = regime in ["ranging", "volatile"]
        elif strategy_type == "momentum":
            is_suitable = "momentum" in regime
        else:
            self.logger.warning(f"Unknown or default strategy type '{strategy_type}', allowing trade by default.")
            is_suitable = True
        self.logger.info(f"Strategy '{strategy_type}' suitability for {symbol} in regime '{regime}': {is_suitable}")
        return is_suitable

    async def clear_history(self, symbol: str):
        """Clear historical data and current regime for a symbol"""
        async with self._lock:
            if symbol in self.price_history: del self.price_history[symbol]
            if symbol in self.regime_history: del self.regime_history[symbol]
            if symbol in self.volatility_history: del self.volatility_history[symbol]
            if symbol in self.atr_history: del self.atr_history[symbol]
            if symbol in self.regimes: del self.regimes[symbol]
            self.logger.info(f"Cleared history and regime data for {symbol}")