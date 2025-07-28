"""
Institutional-Grade Technical Analysis Module - ML Enhanced
Pure Python implementation with ML Extensions and Kernel Functions
Enhanced for institutional trading with advanced signal processing

Now includes:
- ML-optimized normalized indicators
- Kernel-based smoothing and filtering
- Advanced regime detection
- Non-repainting estimators
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime, timedelta

# Import our ML enhancements
from ml_extensions import MLExtensions
from kernel_functions import KernelFunctions

logger = logging.getLogger(__name__)

class InstitutionalTechnicalAnalyzer:
    """
    Institutional-grade technical analysis with ML enhancements.
    Combines traditional indicators with advanced ML-based signal processing.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.ml_ext = MLExtensions()
        self.kernel_func = KernelFunctions()
        
    def add_ml_enhanced_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add ML-enhanced technical indicators optimized for algorithmic trading.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            DataFrame with enhanced indicators
        """
        try:
            # Ensure we have required columns
            required_cols = ['high', 'low', 'close']
            if not all(col in df.columns for col in required_cols):
                self.logger.error(f"Missing required columns. Need: {required_cols}")
                return df
            
            # ML-Optimized Normalized Indicators
            df['n_rsi'] = self.ml_ext.n_rsi(df['close'])
            df['n_cci'] = self.ml_ext.n_cci(df['close'], df['high'], df['low'])
            df['n_wt'] = self.ml_ext.n_wt(df['close'])
            df['n_adx'] = self.ml_ext.n_adx(df['high'], df['low'], df['close'])
            
            # Kernel-Based Smoothing
            df['kernel_gaussian'] = self.kernel_func.gaussian(df['close'])
            df['kernel_rational_quad'] = self.kernel_func.rational_quadratic(df['close'])
            df['kernel_trend_strength'] = self.kernel_func.kernel_trend_strength(df['close'])
            
            # Advanced Signal Processing
            df['tanh_transform'] = self.ml_ext.tanh_transform(df['close'])
            df['normalized_deriv'] = self.ml_ext.normalize_deriv(df['close'])
            
            # Regime and Volatility Filters
            df['regime_filter'] = self.ml_ext.regime_filter(df['close'])
            df['adx_filter'] = self.ml_ext.filter_adx(df['close'], df['high'], df['low'])
            df['volatility_filter'] = self.ml_ext.filter_volatility(df['close'], df['high'], df['low'])
            
            # Kernel Regression Bands (Advanced Bollinger Bands)
            upper, middle, lower = self.kernel_func.kernel_regression_bands(df['close'])
            df['kernel_upper'] = upper
            df['kernel_middle'] = middle
            df['kernel_lower'] = lower
            
            # Multi-timeframe Kernel Analysis
            df['mtf_kernel'] = self.kernel_func.multi_timeframe_kernel(df['close'])
            
            self.logger.info("✅ ML-enhanced indicators added successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Error adding ML-enhanced indicators: {e}")
            return df
    
    def add_adaptive_indicators(self, df: pd.DataFrame, market_regime: str = "normal") -> pd.DataFrame:
        """
        Add adaptive indicators that adjust based on market conditions.
        
        Args:
            df: DataFrame with OHLCV data
            market_regime: Current market regime ("trending", "ranging", "volatile", "normal")
            
        Returns:
            DataFrame with adaptive indicators
        """
        try:
            # Adaptive Kernel Ensemble
            df['adaptive_kernel'] = self.kernel_func.adaptive_kernel_ensemble(
                df['close'], market_condition=market_regime
            )
            
            # Adaptive Lookback Periods
            df['adaptive_lookback'] = self.kernel_func.adaptive_lookback(df['close'])
            
            # Regime-Specific Indicators
            if market_regime == "trending":
                df['trend_kernel'] = self.kernel_func.gaussian(df['close'], 8)
                df['trend_strength'] = self.kernel_func.kernel_trend_strength(df['close'], 5, 15)
            elif market_regime == "ranging":
                df['range_kernel'] = self.kernel_func.rational_quadratic(df['close'], 12, 0.5)
                df['range_oscillator'] = self.ml_ext.n_rsi(df['close'], 21, 14)
            elif market_regime == "volatile":
                df['volatile_kernel'] = self.kernel_func.rational_quadratic(df['close'], 16, 2.0)
                df['volatility_normalized'] = self.ml_ext.normalize_deriv(df['close'], 21)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error adding adaptive indicators: {e}")
            return df
    
    def generate_ml_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate ML-enhanced trading signals with advanced filtering.
        
        Args:
            df: DataFrame with indicators
            
        Returns:
            DataFrame with trading signals
        """
        try:
            # Initialize signal columns
            df['ml_signal'] = 0
            df['signal_strength'] = 0.0
            df['signal_quality'] = 0.0
            
            # ML Signal Generation Logic
            conditions = []
            
            # Condition 1: Normalized RSI with trend confirmation
            rsi_bullish = (df['n_rsi'] < 0.3) & (df['kernel_trend_strength'] > 0.1)
            rsi_bearish = (df['n_rsi'] > 0.7) & (df['kernel_trend_strength'] < -0.1)
            
            # Condition 2: Kernel breakout signals
            kernel_bullish = (df['close'] > df['kernel_upper']) & (df['kernel_trend_strength'] > 0.2)
            kernel_bearish = (df['close'] < df['kernel_lower']) & (df['kernel_trend_strength'] < -0.2)
            
            # Condition 3: Multi-indicator confluence
            confluence_bullish = (
                (df['n_rsi'] < 0.4) & 
                (df['n_adx'] > 0.3) & 
                (df['tanh_transform'] > 0.1) &
                (df['kernel_trend_strength'] > 0.15)
            )
            
            confluence_bearish = (
                (df['n_rsi'] > 0.6) & 
                (df['n_adx'] > 0.3) & 
                (df['tanh_transform'] < -0.1) &
                (df['kernel_trend_strength'] < -0.15)
            )
            
            # Apply filters
            valid_conditions = (
                df['regime_filter'] & 
                df['adx_filter'] & 
                df['volatility_filter']
            )
            
            # Generate signals
            bullish_signal = (rsi_bullish | kernel_bullish | confluence_bullish) & valid_conditions
            bearish_signal = (rsi_bearish | kernel_bearish | confluence_bearish) & valid_conditions
            
            df.loc[bullish_signal, 'ml_signal'] = 1
            df.loc[bearish_signal, 'ml_signal'] = -1
            
            # Calculate signal strength
            df['signal_strength'] = np.abs(df['kernel_trend_strength']) * df['n_adx']
            
            # Calculate signal quality (confluence measure)
            quality_factors = [
                np.abs(df['n_rsi'] - 0.5),  # RSI extremity
                df['n_adx'],  # Trend strength
                np.abs(df['tanh_transform']),  # Signal clarity
                np.abs(df['kernel_trend_strength'])  # Kernel trend
            ]
            
            df['signal_quality'] = np.mean(quality_factors, axis=0)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error generating ML signals: {e}")
            return df
    
    def add_traditional_indicators(self, df: pd.DataFrame, periods: list = [20, 50, 200]) -> pd.DataFrame:
        """Add traditional indicators for comparison and baseline analysis."""
        try:
            # Moving Averages
            for period in periods:
                df[f'SMA_{period}'] = df['close'].rolling(window=period).mean()
                df[f'EMA_{period}'] = df['close'].ewm(span=period).mean()
            
            # Traditional RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, 1e-10)
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # Traditional Bollinger Bands
            sma_20 = df['close'].rolling(window=20).mean()
            std_20 = df['close'].rolling(window=20).std()
            df['BB_Upper'] = sma_20 + (std_20 * 2)
            df['BB_Middle'] = sma_20
            df['BB_Lower'] = sma_20 - (std_20 * 2)
            
            # MACD
            ema_12 = df['close'].ewm(span=12).mean()
            ema_26 = df['close'].ewm(span=26).mean()
            df['MACD'] = ema_12 - ema_26
            df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
            df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
            
            # ATR
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            true_range = np.maximum(high_low, np.maximum(high_close, low_close))
            df['ATR'] = true_range.rolling(window=14).mean()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating traditional indicators: {e}")
            return df
    
    def detect_market_regime(self, df: pd.DataFrame) -> str:
        """
        Detect current market regime using ML-enhanced analysis.
        
        Args:
            df: DataFrame with indicators
            
        Returns:
            Market regime string
        """
        try:
            if len(df) < 50:
                return "insufficient_data"
            
            # Get recent data (last 20 bars)
            recent_data = df.tail(20)
            
            # Trend strength analysis
            avg_trend_strength = recent_data['kernel_trend_strength'].mean()
            trend_consistency = recent_data['kernel_trend_strength'].std()
            
            # Volatility analysis
            avg_volatility = recent_data['normalized_deriv'].abs().mean()
            
            # ADX for trend strength
            avg_adx = recent_data['n_adx'].mean()
            
            # Regime classification
            if avg_adx > 0.6 and abs(avg_trend_strength) > 0.3 and trend_consistency < 0.2:
                return "trending"
            elif avg_adx < 0.3 and abs(avg_trend_strength) < 0.1:
                return "ranging"
            elif avg_volatility > 0.5:
                return "volatile"
            elif trend_consistency > 0.4:
                return "choppy"
            else:
                return "normal"
                
        except Exception as e:
            self.logger.error(f"Error detecting market regime: {e}")
            return "unknown"
    
    def get_signal_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get comprehensive signal summary with ML metrics.
        
        Args:
            df: DataFrame with signals and indicators
            
        Returns:
            Signal summary dictionary
        """
        try:
            if len(df) == 0:
                return {"error": "No data available"}
            
            recent_data = df.tail(10)
            
            summary = {
                "current_regime": self.detect_market_regime(df),
                "latest_signal": recent_data['ml_signal'].iloc[-1] if len(recent_data) > 0 else 0,
                "signal_strength": recent_data['signal_strength'].iloc[-1] if len(recent_data) > 0 else 0,
                "signal_quality": recent_data['signal_quality'].iloc[-1] if len(recent_data) > 0 else 0,
                "trend_strength": recent_data['kernel_trend_strength'].iloc[-1] if len(recent_data) > 0 else 0,
                "normalized_rsi": recent_data['n_rsi'].iloc[-1] if len(recent_data) > 0 else 0.5,
                "normalized_adx": recent_data['n_adx'].iloc[-1] if len(recent_data) > 0 else 0.5,
                "filter_status": {
                    "regime_filter": bool(recent_data['regime_filter'].iloc[-1]) if len(recent_data) > 0 else False,
                    "adx_filter": bool(recent_data['adx_filter'].iloc[-1]) if len(recent_data) > 0 else False,
                    "volatility_filter": bool(recent_data['volatility_filter'].iloc[-1]) if len(recent_data) > 0 else False
                },
                "kernel_analysis": {
                    "price_vs_kernel_upper": recent_data['close'].iloc[-1] / recent_data['kernel_upper'].iloc[-1] if len(recent_data) > 0 else 1.0,
                    "price_vs_kernel_lower": recent_data['close'].iloc[-1] / recent_data['kernel_lower'].iloc[-1] if len(recent_data) > 0 else 1.0,
                    "kernel_trend": recent_data['kernel_trend_strength'].iloc[-1] if len(recent_data) > 0 else 0
                }
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating signal summary: {e}")
            return {"error": str(e)}

# Legacy compatibility class
class TechnicalAnalyzer(InstitutionalTechnicalAnalyzer):
    """
    Legacy compatibility wrapper for existing code.
    Maintains backward compatibility while providing ML enhancements.
    """
    
    def __init__(self):
        super().__init__()
        self.logger.info("TechnicalAnalyzer initialized with ML enhancements")
    
    def add_moving_averages(self, df: pd.DataFrame, periods: list = [20, 50, 200]) -> pd.DataFrame:
        """Legacy method - enhanced with kernel smoothing"""
        df = self.add_traditional_indicators(df, periods)
        
        # Add kernel-enhanced moving averages
        df['kernel_sma_20'] = self.kernel_func.gaussian(df['close'], 20)
        df['kernel_sma_50'] = self.kernel_func.gaussian(df['close'], 50)
        
        return df
    
    def add_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Legacy method - enhanced with normalized RSI"""
        # Traditional RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, 1e-10)
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # ML-enhanced normalized RSI
        df['n_rsi'] = self.ml_ext.n_rsi(df['close'], period)
        
        return df
    
    def add_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
        """Legacy method - enhanced with kernel regression bands"""
        # Traditional Bollinger Bands
        sma = df['close'].rolling(window=period).mean()
        std = df['close'].rolling(window=period).std()
        df['BB_Upper'] = sma + (std * std_dev)
        df['BB_Middle'] = sma
        df['BB_Lower'] = sma - (std * std_dev)
        
        # Kernel regression bands
        upper, middle, lower = self.kernel_func.kernel_regression_bands(df['close'], period, std_dev)
        df['kernel_upper'] = upper
        df['kernel_middle'] = middle
        df['kernel_lower'] = lower
        
        return df
    
    def add_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """Legacy method - enhanced with kernel smoothing"""
        # Traditional MACD
        ema_fast = df['close'].ewm(span=fast).mean()
        ema_slow = df['close'].ewm(span=slow).mean()
        df['MACD'] = ema_fast - ema_slow
        df['MACD_Signal'] = df['MACD'].ewm(span=signal).mean()
        df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
        
        # Kernel-enhanced MACD
        kernel_fast = self.kernel_func.gaussian(df['close'], fast)
        kernel_slow = self.kernel_func.gaussian(df['close'], slow)
        df['kernel_MACD'] = kernel_fast - kernel_slow
        df['kernel_MACD_signal'] = self.kernel_func.gaussian(df['kernel_MACD'], signal)
        
        return df
    
    def add_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Legacy method - enhanced with kernel smoothing"""
        # Traditional ATR
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        df['ATR'] = true_range.rolling(window=period).mean()
        
        # Kernel-smoothed ATR
        df['kernel_ATR'] = self.kernel_func.gaussian(pd.Series(true_range), period)
        
        return df
    
    def add_institutional_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhanced institutional signals with ML processing"""
        df = self.add_ml_enhanced_indicators(df)
        df = self.generate_ml_signals(df)
        return df

# Convenience function for backward compatibility
def get_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Get ATR values - enhanced with kernel smoothing
    """
    try:
        analyzer = TechnicalAnalyzer()
        df_with_atr = analyzer.add_atr(df, period)
        return df_with_atr['ATR']
    except Exception as e:
        logger.error(f"Error calculating ATR: {e}")
        return pd.Series(0, index=df.index)

# Enhanced ATR function with kernel smoothing
def get_kernel_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Get kernel-smoothed ATR values for better signal quality
    """
    try:
        analyzer = TechnicalAnalyzer()
        df_with_atr = analyzer.add_atr(df, period)
        return df_with_atr['kernel_ATR']
    except Exception as e:
        logger.error(f"Error calculating kernel ATR: {e}")
        return pd.Series(0, index=df.index) 