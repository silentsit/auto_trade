"""
MLExtensions - Machine Learning Extensions for Institutional Trading
Python implementation of jdehorty's MLExtensions Pine Script library
Optimized for real-time trading applications with numpy/pandas
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

class MLExtensions:
    """
    Machine Learning Extensions for advanced signal processing and filtering.
    Based on jdehorty's TradingView MLExtensions library.
    """
    
    @staticmethod
    def normalize_deriv(src: pd.Series, quadratic_mean_length: int = 14) -> pd.Series:
        """
        Returns the normalized derivative of the input series.
        
        Args:
            src: Input price series
            quadratic_mean_length: Length for quadratic mean (RMS) calculation
            
        Returns:
            Normalized derivative series
        """
        try:
            # Calculate first-order derivative
            deriv = src - src.shift(2)
            
            # Calculate quadratic mean (RMS) of derivative
            quadratic_mean = np.sqrt(
                deriv.pow(2).rolling(window=quadratic_mean_length, min_periods=1).sum() / 
                quadratic_mean_length
            )
            
            # Return normalized derivative
            normalized_deriv = deriv / quadratic_mean
            return normalized_deriv.fillna(0)
            
        except Exception as e:
            logger.error(f"Error in normalize_deriv: {e}")
            return pd.Series(0, index=src.index)
    
    @staticmethod
    def normalize(src: pd.Series, min_val: float = 0.0, max_val: float = 1.0) -> pd.Series:
        """
        Rescales a source value with unbounded range to target range.
        
        Args:
            src: Input series
            min_val: Minimum value of target range
            max_val: Maximum value of target range
            
        Returns:
            Normalized series
        """
        try:
            historic_min = src.expanding().min()
            historic_max = src.expanding().max()
            
            # Avoid division by zero
            range_diff = historic_max - historic_min
            range_diff = range_diff.replace(0, 1e-10)
            
            normalized = min_val + (max_val - min_val) * (src - historic_min) / range_diff
            return normalized.fillna(min_val)
            
        except Exception as e:
            logger.error(f"Error in normalize: {e}")
            return pd.Series(min_val, index=src.index)
    
    @staticmethod
    def rescale(src: pd.Series, old_min: float, old_max: float, 
                new_min: float, new_max: float) -> pd.Series:
        """
        Rescales a source value from one bounded range to another.
        
        Args:
            src: Input series
            old_min: Minimum of source range
            old_max: Maximum of source range  
            new_min: Minimum of target range
            new_max: Maximum of target range
            
        Returns:
            Rescaled series
        """
        try:
            old_range = old_max - old_min
            if old_range == 0:
                old_range = 1e-10
                
            rescaled = new_min + (new_max - new_min) * (src - old_min) / old_range
            return rescaled.fillna(new_min)
            
        except Exception as e:
            logger.error(f"Error in rescale: {e}")
            return pd.Series(new_min, index=src.index)
    
    @staticmethod
    def tanh(src: pd.Series) -> pd.Series:
        """
        Returns hyperbolic tangent of input series (compresses to -1 to 1).
        
        Args:
            src: Input series
            
        Returns:
            Hyperbolic tangent of input
        """
        try:
            return np.tanh(src).fillna(0)
        except Exception as e:
            logger.error(f"Error in tanh: {e}")
            return pd.Series(0, index=src.index)
    
    @staticmethod
    def dual_pole_filter(src: pd.Series, lookback: int = 8) -> pd.Series:
        """
        Returns smoothed hyperbolic tangent using dual pole filter.
        
        Args:
            src: Input series (hyperbolic tangent)
            lookback: Lookback window for smoothing
            
        Returns:
            Filtered series
        """
        try:
            omega = -99 * np.pi / (70 * lookback)
            alpha = np.exp(omega)
            beta = -alpha ** 2
            gamma = np.cos(omega) * 2 * alpha
            delta = 1 - gamma - beta
            
            sliding_avg = 0.5 * (src + src.shift(1)).fillna(src)
            
            # Initialize filter
            filtered = pd.Series(0.0, index=src.index)
            
            for i in range(len(src)):
                if i == 0:
                    filtered.iloc[i] = delta * sliding_avg.iloc[i]
                elif i == 1:
                    filtered.iloc[i] = (delta * sliding_avg.iloc[i] + 
                                      gamma * filtered.iloc[i-1])
                else:
                    filtered.iloc[i] = (delta * sliding_avg.iloc[i] + 
                                      gamma * filtered.iloc[i-1] + 
                                      beta * filtered.iloc[i-2])
            
            return filtered
            
        except Exception as e:
            logger.error(f"Error in dual_pole_filter: {e}")
            return src.copy()
    
    @staticmethod
    def tanh_transform(src: pd.Series, smoothing_frequency: int = 8, 
                      quadratic_mean_length: int = 14) -> pd.Series:
        """
        Returns tanh transform of input series.
        
        Args:
            src: Input series
            smoothing_frequency: Smoothing parameter
            quadratic_mean_length: Length for normalization
            
        Returns:
            Transformed signal
        """
        try:
            normalized_deriv = MLExtensions.normalize_deriv(src, quadratic_mean_length)
            tanh_signal = MLExtensions.tanh(normalized_deriv)
            filtered_signal = MLExtensions.dual_pole_filter(tanh_signal, smoothing_frequency)
            return filtered_signal
            
        except Exception as e:
            logger.error(f"Error in tanh_transform: {e}")
            return pd.Series(0, index=src.index)
    
    @staticmethod
    def n_rsi(src: pd.Series, n1: int = 14, n2: int = 10) -> pd.Series:
        """
        Returns normalized RSI ideal for ML algorithms.
        
        Args:
            src: Input price series
            n1: RSI length
            n2: Smoothing length
            
        Returns:
            Normalized RSI (0-1 range)
        """
        try:
            # Calculate RSI
            delta = src.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=n1, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=n1, min_periods=1).mean()
            rs = gain / loss.replace(0, 1e-10)
            rsi = 100 - (100 / (1 + rs))
            
            # Smooth and normalize
            smoothed_rsi = rsi.ewm(span=n2).mean()
            normalized_rsi = MLExtensions.rescale(smoothed_rsi, 0, 100, 0, 1)
            
            return normalized_rsi.fillna(0.5)
            
        except Exception as e:
            logger.error(f"Error in n_rsi: {e}")
            return pd.Series(0.5, index=src.index)
    
    @staticmethod
    def n_cci(src: pd.Series, high: pd.Series, low: pd.Series, 
              n1: int = 20, n2: int = 10) -> pd.Series:
        """
        Returns normalized CCI ideal for ML algorithms.
        
        Args:
            src: Input price series (typically close)
            high: High price series
            low: Low price series  
            n1: CCI length
            n2: Smoothing length
            
        Returns:
            Normalized CCI (0-1 range)
        """
        try:
            # Calculate typical price
            typical_price = (high + low + src) / 3
            
            # Calculate CCI
            sma_tp = typical_price.rolling(window=n1, min_periods=1).mean()
            mad = typical_price.rolling(window=n1, min_periods=1).apply(
                lambda x: np.mean(np.abs(x - x.mean()))
            )
            cci = (typical_price - sma_tp) / (0.015 * mad)
            
            # Smooth and normalize
            smoothed_cci = cci.ewm(span=n2).mean()
            normalized_cci = MLExtensions.normalize(smoothed_cci, 0, 1)
            
            return normalized_cci.fillna(0.5)
            
        except Exception as e:
            logger.error(f"Error in n_cci: {e}")
            return pd.Series(0.5, index=src.index)
    
    @staticmethod
    def n_wt(src: pd.Series, n1: int = 10, n2: int = 11) -> pd.Series:
        """
        Returns normalized WaveTrend Classic series ideal for ML algorithms.
        
        Args:
            src: Input price series
            n1: First smoothing length
            n2: Second smoothing length
            
        Returns:
            Normalized WaveTrend signal
        """
        try:
            # Calculate WaveTrend
            ema1 = src.ewm(span=n1).mean()
            ema2 = np.abs(src - ema1).ewm(span=n1).mean()
            ci = (src - ema1) / (0.015 * ema2)
            
            wt1 = ci.ewm(span=n2).mean()  # TCI
            wt2 = wt1.rolling(window=4, min_periods=1).mean()
            wt_diff = wt1 - wt2
            
            # Normalize
            normalized_wt = MLExtensions.normalize(wt_diff, 0, 1)
            return normalized_wt.fillna(0.5)
            
        except Exception as e:
            logger.error(f"Error in n_wt: {e}")
            return pd.Series(0.5, index=src.index)
    
    @staticmethod
    def n_adx(high: pd.Series, low: pd.Series, close: pd.Series, 
              n1: int = 14) -> pd.Series:
        """
        Returns normalized ADX ideal for ML algorithms.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            n1: ADX length
            
        Returns:
            Normalized ADX (0-1 range)
        """
        try:
            # Calculate True Range and Directional Movement
            tr1 = high - low
            tr2 = np.abs(high - close.shift(1))
            tr3 = np.abs(low - close.shift(1))
            tr = np.maximum(tr1, np.maximum(tr2, tr3))
            
            dm_plus = np.where((high - high.shift(1)) > (low.shift(1) - low),
                              np.maximum(high - high.shift(1), 0), 0)
            dm_minus = np.where((low.shift(1) - low) > (high - high.shift(1)),
                               np.maximum(low.shift(1) - low, 0), 0)
            
            # Smooth the values
            tr_smooth = pd.Series(tr).rolling(window=n1, min_periods=1).mean()
            dm_plus_smooth = pd.Series(dm_plus).rolling(window=n1, min_periods=1).mean()
            dm_minus_smooth = pd.Series(dm_minus).rolling(window=n1, min_periods=1).mean()
            
            # Calculate DI+ and DI-
            di_plus = 100 * dm_plus_smooth / tr_smooth
            di_minus = 100 * dm_minus_smooth / tr_smooth
            
            # Calculate DX and ADX
            dx = 100 * np.abs(di_plus - di_minus) / (di_plus + di_minus)
            adx = dx.rolling(window=n1, min_periods=1).mean()
            
            # Normalize to 0-1 range
            normalized_adx = MLExtensions.rescale(adx, 0, 100, 0, 1)
            return normalized_adx.fillna(0.5)
            
        except Exception as e:
            logger.error(f"Error in n_adx: {e}")
            return pd.Series(0.5, index=high.index)
    
    @staticmethod
    def regime_filter(src: pd.Series, threshold: float = 0.5, 
                     use_regime_filter: bool = True) -> pd.Series:
        """
        Advanced regime filter for market condition detection.
        
        Args:
            src: Input price series (typically OHLC4)
            threshold: Regime threshold
            use_regime_filter: Whether to apply filtering
            
        Returns:
            Boolean series indicating regime filter pass
        """
        try:
            if not use_regime_filter:
                return pd.Series(True, index=src.index)
            
            # Calculate slope of curve using Kalman-like filter
            value1 = pd.Series(0.0, index=src.index)
            value2 = pd.Series(0.0, index=src.index)
            klmf = pd.Series(0.0, index=src.index)
            
            for i in range(1, len(src)):
                value1.iloc[i] = 0.2 * (src.iloc[i] - src.iloc[i-1]) + 0.8 * value1.iloc[i-1]
                high_low_diff = 0.01 if i == 0 else 0.01  # Simplified for demo
                value2.iloc[i] = 0.1 * high_low_diff + 0.8 * value2.iloc[i-1]
                
                omega = abs(value1.iloc[i] / (value2.iloc[i] + 1e-10))
                alpha = (-omega**2 + np.sqrt(omega**4 + 16 * omega**2)) / 8
                klmf.iloc[i] = alpha * src.iloc[i] + (1 - alpha) * klmf.iloc[i-1]
            
            # Calculate slope and normalized decline
            abs_curve_slope = np.abs(klmf - klmf.shift(1))
            exp_avg_slope = abs_curve_slope.ewm(span=200).mean()
            normalized_slope_decline = (abs_curve_slope - exp_avg_slope) / (exp_avg_slope + 1e-10)
            
            return (normalized_slope_decline >= threshold).fillna(True)
            
        except Exception as e:
            logger.error(f"Error in regime_filter: {e}")
            return pd.Series(True, index=src.index)
    
    @staticmethod
    def filter_adx(src: pd.Series, high: pd.Series, low: pd.Series,
                   length: int = 14, adx_threshold: int = 25, 
                   use_adx_filter: bool = True) -> pd.Series:
        """
        ADX-based signal filter.
        
        Args:
            src: Close price series
            high: High price series
            low: Low price series
            length: ADX calculation length
            adx_threshold: ADX threshold for filtering
            use_adx_filter: Whether to apply ADX filtering
            
        Returns:
            Boolean series indicating ADX filter pass
        """
        try:
            if not use_adx_filter:
                return pd.Series(True, index=src.index)
            
            # Calculate ADX (reuse n_adx but without normalization)
            adx_normalized = MLExtensions.n_adx(high, low, src, length)
            adx = adx_normalized * 100  # Convert back to 0-100 scale
            
            return (adx > adx_threshold).fillna(False)
            
        except Exception as e:
            logger.error(f"Error in filter_adx: {e}")
            return pd.Series(True, index=src.index)
    
    @staticmethod
    def filter_volatility(src: pd.Series, high: pd.Series, low: pd.Series,
                         min_length: int = 1, max_length: int = 10,
                         use_volatility_filter: bool = True) -> pd.Series:
        """
        Volatility-based signal filter using ATR.
        
        Args:
            src: Close price series
            high: High price series  
            low: Low price series
            min_length: Minimum ATR length (recent)
            max_length: Maximum ATR length (historical)
            use_volatility_filter: Whether to apply volatility filtering
            
        Returns:
            Boolean series indicating volatility filter pass
        """
        try:
            if not use_volatility_filter:
                return pd.Series(True, index=src.index)
            
            # Calculate ATR for both periods
            tr1 = high - low
            tr2 = np.abs(high - src.shift(1))
            tr3 = np.abs(low - src.shift(1))
            tr = np.maximum(tr1, np.maximum(tr2, tr3))
            
            recent_atr = pd.Series(tr).rolling(window=min_length, min_periods=1).mean()
            historical_atr = pd.Series(tr).rolling(window=max_length, min_periods=1).mean()
            
            return (recent_atr > historical_atr).fillna(False)
            
        except Exception as e:
            logger.error(f"Error in filter_volatility: {e}")
            return pd.Series(True, index=src.index)
    
    @staticmethod
    def get_prediction_color_intensity(prediction: float, neighbors_count: int = 10) -> float:
        """
        Determines color intensity based on prediction percentile.
        
        Args:
            prediction: Prediction value
            neighbors_count: Number of neighbors in classification
            
        Returns:
            Intensity value (0-1)
        """
        try:
            percentile = prediction / neighbors_count * 100
            
            if percentile >= 75:
                return 1.0  # Most intense
            elif percentile >= 50:
                return 0.75
            elif percentile >= 25:
                return 0.5
            else:
                return 0.25  # Least intense
                
        except Exception as e:
            logger.error(f"Error in get_prediction_color_intensity: {e}")
            return 0.5 