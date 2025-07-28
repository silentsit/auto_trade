"""
KernelFunctions - Non-Repainting Kernel Functions for Institutional Trading
Python implementation of jdehorty's KernelFunctions Pine Script library
Provides advanced Nadaraya-Watson estimator implementations for trading algorithms
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

class KernelFunctions:
    """
    Non-repainting kernel functions for Nadaraya-Watson estimator implementations.
    Based on jdehorty's TradingView KernelFunctions library.
    """
    
    @staticmethod
    def rational_quadratic(src: pd.Series, lookback: int = 8, 
                          relative_weight: float = 1.0, start_at_bar: int = 25) -> pd.Series:
        """
        Rational Quadratic Kernel - An infinite sum of Gaussian Kernels of different length scales.
        
        Args:
            src: Input price series
            lookback: Number of bars for estimation (sliding window)
            relative_weight: Relative weighting of time frames. Smaller values = more stretched curve
            start_at_bar: Bar index to start regression (skip volatile initial bars)
            
        Returns:
            Estimated values according to Rational Quadratic Kernel
        """
        try:
            result = pd.Series(np.nan, index=src.index)
            
            for idx in range(start_at_bar, len(src)):
                current_weight = 0.0
                cumulative_weight = 0.0
                
                # Look back through recent history
                start_idx = max(0, idx - lookback)
                
                for i in range(start_idx, idx + 1):
                    if pd.notna(src.iloc[i]):
                        y = src.iloc[i]
                        distance = idx - i
                        
                        # Rational Quadratic kernel weight calculation
                        weight = (1 + (distance**2 / (lookback**2 * 2 * relative_weight))) ** (-relative_weight)
                        
                        current_weight += y * weight
                        cumulative_weight += weight
                
                if cumulative_weight > 0:
                    result.iloc[idx] = current_weight / cumulative_weight
                else:
                    result.iloc[idx] = src.iloc[idx] if pd.notna(src.iloc[idx]) else 0
            
            # Forward fill initial values
            result.iloc[:start_at_bar] = src.iloc[:start_at_bar]
            
            return result.fillna(method='ffill').fillna(0)
            
        except Exception as e:
            logger.error(f"Error in rational_quadratic: {e}")
            return src.copy()
    
    @staticmethod
    def gaussian(src: pd.Series, lookback: int = 16, start_at_bar: int = 25) -> pd.Series:
        """
        Gaussian Kernel - Weighted average using Radial Basis Function (RBF).
        
        Args:
            src: Input price series
            lookback: Number of bars for estimation
            start_at_bar: Bar index to start regression
            
        Returns:
            Estimated values according to Gaussian Kernel
        """
        try:
            result = pd.Series(np.nan, index=src.index)
            
            for idx in range(start_at_bar, len(src)):
                current_weight = 0.0
                cumulative_weight = 0.0
                
                # Look back through recent history
                start_idx = max(0, idx - lookback)
                
                for i in range(start_idx, idx + 1):
                    if pd.notna(src.iloc[i]):
                        y = src.iloc[i]
                        distance = idx - i
                        
                        # Gaussian kernel weight calculation (RBF)
                        weight = np.exp(-(distance**2) / (2 * lookback**2))
                        
                        current_weight += y * weight
                        cumulative_weight += weight
                
                if cumulative_weight > 0:
                    result.iloc[idx] = current_weight / cumulative_weight
                else:
                    result.iloc[idx] = src.iloc[idx] if pd.notna(src.iloc[idx]) else 0
            
            # Forward fill initial values
            result.iloc[:start_at_bar] = src.iloc[:start_at_bar]
            
            return result.fillna(method='ffill').fillna(0)
            
        except Exception as e:
            logger.error(f"Error in gaussian: {e}")
            return src.copy()
    
    @staticmethod
    def periodic(src: pd.Series, lookback: int = 8, period: int = 100, 
                start_at_bar: int = 25) -> pd.Series:
        """
        Periodic Kernel - Models functions that repeat themselves exactly.
        Based on David Mackay's periodic kernel.
        
        Args:
            src: Input price series
            lookback: Number of bars for estimation
            period: Distance between repetitions of the function
            start_at_bar: Bar index to start regression
            
        Returns:
            Estimated values according to Periodic Kernel
        """
        try:
            result = pd.Series(np.nan, index=src.index)
            
            for idx in range(start_at_bar, len(src)):
                current_weight = 0.0
                cumulative_weight = 0.0
                
                # Look back through recent history
                start_idx = max(0, idx - lookback * 2)  # Extended lookback for periodic patterns
                
                for i in range(start_idx, idx + 1):
                    if pd.notna(src.iloc[i]):
                        y = src.iloc[i]
                        distance = idx - i
                        
                        # Periodic kernel weight calculation
                        sin_component = np.sin(np.pi * distance / period)
                        weight = np.exp(-2 * sin_component**2 / lookback**2)
                        
                        current_weight += y * weight
                        cumulative_weight += weight
                
                if cumulative_weight > 0:
                    result.iloc[idx] = current_weight / cumulative_weight
                else:
                    result.iloc[idx] = src.iloc[idx] if pd.notna(src.iloc[idx]) else 0
            
            # Forward fill initial values
            result.iloc[:start_at_bar] = src.iloc[:start_at_bar]
            
            return result.fillna(method='ffill').fillna(0)
            
        except Exception as e:
            logger.error(f"Error in periodic: {e}")
            return src.copy()
    
    @staticmethod
    def locally_periodic(src: pd.Series, lookback: int = 8, period: int = 24, 
                        start_at_bar: int = 25) -> pd.Series:
        """
        Locally Periodic Kernel - Periodic function that slowly varies with time.
        Product of Periodic Kernel and Gaussian Kernel.
        
        Args:
            src: Input price series
            lookback: Number of bars for estimation
            period: Distance between repetitions
            start_at_bar: Bar index to start regression
            
        Returns:
            Estimated values according to Locally Periodic Kernel
        """
        try:
            result = pd.Series(np.nan, index=src.index)
            
            for idx in range(start_at_bar, len(src)):
                current_weight = 0.0
                cumulative_weight = 0.0
                
                # Look back through recent history
                start_idx = max(0, idx - lookback * 2)
                
                for i in range(start_idx, idx + 1):
                    if pd.notna(src.iloc[i]):
                        y = src.iloc[i]
                        distance = idx - i
                        
                        # Locally periodic kernel weight calculation
                        # Combines periodic and Gaussian components
                        sin_component = np.sin(np.pi * distance / period)
                        periodic_weight = np.exp(-2 * sin_component**2 / lookback**2)
                        gaussian_weight = np.exp(-(distance**2) / (2 * lookback**2))
                        
                        weight = periodic_weight * gaussian_weight
                        
                        current_weight += y * weight
                        cumulative_weight += weight
                
                if cumulative_weight > 0:
                    result.iloc[idx] = current_weight / cumulative_weight
                else:
                    result.iloc[idx] = src.iloc[idx] if pd.notna(src.iloc[idx]) else 0
            
            # Forward fill initial values
            result.iloc[:start_at_bar] = src.iloc[:start_at_bar]
            
            return result.fillna(method='ffill').fillna(0)
            
        except Exception as e:
            logger.error(f"Error in locally_periodic: {e}")
            return src.copy()
    
    @staticmethod
    def adaptive_kernel_ensemble(src: pd.Series, lookback: int = 8, 
                                market_condition: str = "normal") -> pd.Series:
        """
        Adaptive kernel ensemble that selects optimal kernel based on market conditions.
        
        Args:
            src: Input price series
            lookback: Base lookback period
            market_condition: Market condition ("trending", "ranging", "volatile", "normal")
            
        Returns:
            Adaptive kernel-smoothed series
        """
        try:
            if market_condition == "trending":
                # Use Gaussian for trending markets
                return KernelFunctions.gaussian(src, lookback, 25)
            elif market_condition == "ranging":
                # Use Rational Quadratic for ranging markets
                return KernelFunctions.rational_quadratic(src, lookback, 0.5, 25)
            elif market_condition == "volatile":
                # Use Rational Quadratic with higher relative weight for volatile markets
                return KernelFunctions.rational_quadratic(src, lookback * 2, 2.0, 25)
            elif market_condition == "cyclical":
                # Use Locally Periodic for cyclical patterns
                period = lookback * 4  # Adaptive period
                return KernelFunctions.locally_periodic(src, lookback, period, 25)
            else:
                # Default: Gaussian kernel for normal conditions
                return KernelFunctions.gaussian(src, lookback, 25)
                
        except Exception as e:
            logger.error(f"Error in adaptive_kernel_ensemble: {e}")
            return src.copy()
    
    @staticmethod
    def kernel_regression_bands(src: pd.Series, lookback: int = 8, 
                               std_multiplier: float = 2.0,
                               kernel_type: str = "gaussian") -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Creates kernel regression bands similar to Bollinger Bands but using kernel smoothing.
        
        Args:
            src: Input price series
            lookback: Lookback period for kernel
            std_multiplier: Standard deviation multiplier for bands
            kernel_type: Type of kernel ("gaussian", "rational_quadratic", "periodic")
            
        Returns:
            Tuple of (upper_band, middle_band, lower_band)
        """
        try:
            # Calculate kernel-smoothed middle line
            if kernel_type == "gaussian":
                middle = KernelFunctions.gaussian(src, lookback, 25)
            elif kernel_type == "rational_quadratic":
                middle = KernelFunctions.rational_quadratic(src, lookback, 1.0, 25)
            elif kernel_type == "periodic":
                middle = KernelFunctions.periodic(src, lookback, lookback * 4, 25)
            else:
                middle = KernelFunctions.gaussian(src, lookback, 25)
            
            # Calculate residuals and standard deviation
            residuals = src - middle
            std_dev = residuals.rolling(window=lookback, min_periods=1).std()
            
            # Create bands
            upper_band = middle + (std_dev * std_multiplier)
            lower_band = middle - (std_dev * std_multiplier)
            
            return upper_band.fillna(0), middle.fillna(0), lower_band.fillna(0)
            
        except Exception as e:
            logger.error(f"Error in kernel_regression_bands: {e}")
            middle = src.rolling(window=lookback).mean()
            std_dev = src.rolling(window=lookback).std()
            upper = middle + (std_dev * std_multiplier)
            lower = middle - (std_dev * std_multiplier)
            return upper.fillna(0), middle.fillna(0), lower.fillna(0)
    
    @staticmethod
    def kernel_trend_strength(src: pd.Series, short_lookback: int = 8, 
                             long_lookback: int = 21) -> pd.Series:
        """
        Calculates trend strength using kernel smoothing comparison.
        
        Args:
            src: Input price series
            short_lookback: Short-term kernel lookback
            long_lookback: Long-term kernel lookback
            
        Returns:
            Trend strength indicator (-1 to 1)
        """
        try:
            # Calculate short and long-term kernel smoothing
            short_kernel = KernelFunctions.gaussian(src, short_lookback, 25)
            long_kernel = KernelFunctions.gaussian(src, long_lookback, 25)
            
            # Calculate trend strength
            trend_diff = short_kernel - long_kernel
            trend_strength = trend_diff / (long_kernel + 1e-10)  # Normalize by price level
            
            # Smooth the trend strength
            smoothed_strength = KernelFunctions.gaussian(trend_strength, short_lookback, 25)
            
            # Clamp to -1, 1 range
            return np.clip(smoothed_strength, -1, 1).fillna(0)
            
        except Exception as e:
            logger.error(f"Error in kernel_trend_strength: {e}")
            return pd.Series(0, index=src.index)
    
    @staticmethod
    def adaptive_lookback(src: pd.Series, base_lookback: int = 8, 
                         volatility_factor: float = 1.0) -> pd.Series:
        """
        Calculates adaptive lookback period based on market volatility.
        
        Args:
            src: Input price series
            base_lookback: Base lookback period
            volatility_factor: Volatility adjustment factor
            
        Returns:
            Adaptive lookback periods
        """
        try:
            # Calculate volatility using rolling standard deviation
            returns = src.pct_change()
            volatility = returns.rolling(window=base_lookback, min_periods=1).std()
            
            # Normalize volatility
            vol_percentile = volatility.rolling(window=base_lookback * 4, min_periods=1).rank(pct=True)
            
            # Calculate adaptive lookback (higher volatility = shorter lookback)
            adaptive_lookback = base_lookback * (2 - vol_percentile * volatility_factor)
            adaptive_lookback = np.clip(adaptive_lookback, base_lookback * 0.5, base_lookback * 2)
            
            return adaptive_lookback.fillna(base_lookback)
            
        except Exception as e:
            logger.error(f"Error in adaptive_lookback: {e}")
            return pd.Series(base_lookback, index=src.index)
    
    @staticmethod
    def multi_timeframe_kernel(src: pd.Series, lookbacks: List[int] = [8, 16, 32],
                              weights: Optional[List[float]] = None) -> pd.Series:
        """
        Multi-timeframe kernel ensemble for robust trend detection.
        
        Args:
            src: Input price series
            lookbacks: List of lookback periods
            weights: Weights for each timeframe (if None, equal weights)
            
        Returns:
            Multi-timeframe kernel-smoothed series
        """
        try:
            if weights is None:
                weights = [1.0 / len(lookbacks)] * len(lookbacks)
            
            if len(weights) != len(lookbacks):
                raise ValueError("Weights and lookbacks must have same length")
            
            # Calculate weighted combination of different timeframe kernels
            combined_kernel = pd.Series(0.0, index=src.index)
            
            for lookback, weight in zip(lookbacks, weights):
                kernel_result = KernelFunctions.gaussian(src, lookback, 25)
                combined_kernel += kernel_result * weight
            
            return combined_kernel.fillna(0)
            
        except Exception as e:
            logger.error(f"Error in multi_timeframe_kernel: {e}")
            return KernelFunctions.gaussian(src, lookbacks[0] if lookbacks else 8, 25) 