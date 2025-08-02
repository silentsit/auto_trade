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