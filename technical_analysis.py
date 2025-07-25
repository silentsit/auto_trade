"""
Institutional-Grade Technical Analysis Module
Pure Python implementation - Cloud deployment friendly

Uses pure pandas for maximum compatibility:
- No external technical analysis dependencies
- Custom institutional-grade indicators
- Optimized for cloud deployment reliability
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Using pure pandas implementations for maximum cloud compatibility and reliability
logger.info("Using institutional-grade pure pandas technical analysis implementations")

class TechnicalAnalyzer:
    """
    Institutional-grade technical analysis using pure Python libraries.
    Designed for high-frequency trading environments.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    def add_moving_averages(self, df: pd.DataFrame, periods: list = [20, 50, 200]) -> pd.DataFrame:
        """Add multiple moving averages - institutional standard periods"""
        try:
            for period in periods:
                df[f'SMA_{period}'] = df['close'].rolling(window=period).mean()
                df[f'EMA_{period}'] = df['close'].ewm(span=period).mean()
            return df
        except Exception as e:
            self.logger.error(f"Error calculating moving averages: {e}")
            return df
    
    def add_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Add RSI indicator using institutional-grade pure pandas implementation"""
        try:
            # Pure pandas implementation - institutional grade
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            return df
        except Exception as e:
            self.logger.error(f"Error calculating RSI: {e}")
            return df
    
    def add_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
        """Add Bollinger Bands - critical for volatility analysis"""
        try:
            # Pure pandas implementation - institutional standard
            sma = df['close'].rolling(window=period).mean()
            std = df['close'].rolling(window=period).std()
            df['BB_Upper'] = sma + (std * std_dev)
            df['BB_Middle'] = sma
            df['BB_Lower'] = sma - (std * std_dev)
            return df
        except Exception as e:
            self.logger.error(f"Error calculating Bollinger Bands: {e}")
            return df
    
    def add_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """Add MACD - essential for trend analysis"""
        try:
            # Pure pandas implementation - institutional standard
            ema_fast = df['close'].ewm(span=fast).mean()
            ema_slow = df['close'].ewm(span=slow).mean()
            df['MACD'] = ema_fast - ema_slow
            df['MACD_Signal'] = df['MACD'].ewm(span=signal).mean()
            df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
            return df
        except Exception as e:
            self.logger.error(f"Error calculating MACD: {e}")
            return df
    
    def add_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Add Average True Range - critical for position sizing"""
        try:
            # Pure pandas implementation - institutional grade
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            true_range = np.maximum(high_low, np.maximum(high_close, low_close))
            df['ATR'] = true_range.rolling(window=period).mean()
            return df
        except Exception as e:
            self.logger.error(f"Error calculating ATR: {e}")
            return df
    
    def add_institutional_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add institutional-grade trading signals"""
        try:
            # Trend strength
            df['Trend_Strength'] = (df['EMA_20'] - df['EMA_50']) / df['ATR']
            
            # Volatility regime
            df['Vol_Regime'] = pd.cut(df['ATR'], bins=3, labels=['Low', 'Medium', 'High'])
            
            # Mean reversion signal
            df['Mean_Reversion'] = (df['close'] - df['BB_Middle']) / (df['BB_Upper'] - df['BB_Lower'])
            
            # Momentum divergence
            df['Price_Momentum'] = df['close'].pct_change(5)
            df['MACD_Momentum'] = df['MACD'].pct_change(5)
            
            return df
        except Exception as e:
            self.logger.error(f"Error calculating institutional signals: {e}")
            return df
    
    def analyze_market_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Complete technical analysis pipeline for market data"""
        try:
            # Add all technical indicators
            df = self.add_moving_averages(df)
            df = self.add_rsi(df)
            df = self.add_bollinger_bands(df)
            df = self.add_macd(df)
            df = self.add_atr(df)
            df = self.add_institutional_signals(df)
            
            # Current market analysis
            latest = df.iloc[-1]
            
            analysis = {
                'timestamp': datetime.now(),
                'price': latest['close'],
                'trend': {
                    'sma_20': latest['SMA_20'],
                    'ema_20': latest['EMA_20'],
                    'trend_strength': latest['Trend_Strength']
                },
                'momentum': {
                    'rsi': latest['RSI'],
                    'macd': latest['MACD'],
                    'macd_signal': latest['MACD_Signal']
                },
                'volatility': {
                    'atr': latest['ATR'],
                    'bb_position': latest['Mean_Reversion'],
                    'vol_regime': latest['Vol_Regime']
                },
                'signals': {
                    'oversold': latest['RSI'] < 30,
                    'overbought': latest['RSI'] > 70,
                    'bullish_macd': latest['MACD'] > latest['MACD_Signal'],
                    'mean_reversion_buy': latest['Mean_Reversion'] < -0.8,
                    'mean_reversion_sell': latest['Mean_Reversion'] > 0.8
                }
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error in market analysis: {e}")
            return {'error': str(e)}

# Convenience functions for quick access
def get_analyzer() -> TechnicalAnalyzer:
    """Get technical analyzer instance"""
    return TechnicalAnalyzer()

def quick_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """Quick technical analysis of market data"""
    analyzer = TechnicalAnalyzer()
    return analyzer.analyze_market_data(df)

def get_atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """
    Calculate the Average True Range (ATR) from a DataFrame.
    This standalone function is for convenience and leverages the robust TechnicalAnalyzer.
    """
    if df is None or df.empty:
        logger.warning("get_atr received an empty or None DataFrame.")
        return None
    try:
        analyzer = TechnicalAnalyzer()
        df_with_atr = analyzer.add_atr(df.copy(), period=period) # Use a copy to avoid side-effects
        if 'ATR' in df_with_atr.columns and not df_with_atr['ATR'].empty:
            atr_value = df_with_atr['ATR'].iloc[-1]
            return atr_value if pd.notna(atr_value) else None
        return None
    except Exception as e:
        logger.error(f"Error calculating ATR in standalone function: {e}")
        return None 