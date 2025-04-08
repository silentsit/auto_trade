##############################################################################
# Core Setup - Block 1: Imports, Error Handling, Configuration
##############################################################################

import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import time
import json
import signal
import statistics
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable, TypeVar, ParamSpec
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError, Field, root_validator, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import wraps
from redis.asyncio import Redis
import httpx
import copy
import sys
import ast
import inspect
import traceback
from enum import Enum, auto
from collections import defaultdict, deque
import hashlib
import hmac
import random
import math
import pandas as pd
from dataclasses import dataclass, field
from sklearn.cluster import KMeans
from market_regime import MarketRegimeClassifier, MarketRegime

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

##############################################################################
# Configuration
##############################################################################

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables with defaults.
    Pydantic's BaseSettings handles environment variables automatically.
    """
    # API settings
    host: str = "0.0.0.0"
    port: int = 10000
    environment: str = "production"  # production, development
    allowed_origins: str = "*"
    
    # OANDA settings
    oanda_account_id: str = ""
    oanda_api_token: str = ""
    oanda_api_url: str = "https://api-fxtrade.oanda.com/v3"
    oanda_environment: str = "practice"  # practice, live
    
    # Redis settings (optional)
    redis_url: Optional[str] = None
    
    # Risk management settings
    default_risk_percentage: float = 2.0
    max_risk_percentage: float = 5.0
    max_daily_loss: float = 20.0  # 20% max daily loss
    max_portfolio_heat: float = 15.0  # 15% max portfolio risk
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

# Initialize settings
config = Settings()

##############################################################################
# Constants for Advanced Risk Management (from Script 1)
##############################################################################

# Constants for risk management
DEFAULT_RISK_PERCENTAGE = 2.0    # Default 2% risk per trade
MAX_RISK_PERCENTAGE = 5.0        # Maximum 5% risk per trade
MAX_DAILY_LOSS = 0.2             # Maximum 20% daily loss
POSITION_SIZE_MULTIPLIER = 100000  # Standard lot multiplier

# Instrument-specific multipliers
INSTRUMENT_MULTIPLIERS = {
    'FOREX': 1.0,
    'FOREX_JPY': 0.01,
    'METAL': 0.1,
    'CRYPTO': 0.1,
    'INDEX': 0.1,
    'ENERGY': 0.1
}

# Timeframe-specific take profit levels
TIMEFRAME_TAKE_PROFIT_LEVELS = {
    '1M': {'first_exit': 0.3, 'second_exit': 0.3, 'runner': 0.4},
    '5M': {'first_exit': 0.3, 'second_exit': 0.3, 'runner': 0.4},
    '15M': {'first_exit': 0.25, 'second_exit': 0.35, 'runner': 0.4},
    '30M': {'first_exit': 0.25, 'second_exit': 0.35, 'runner': 0.4},
    '1H': {'first_exit': 0.2, 'second_exit': 0.4, 'runner': 0.4},
    '4H': {'first_exit': 0.2, 'second_exit': 0.3, 'runner': 0.5},
    'D1': {'first_exit': 0.15, 'second_exit': 0.25, 'runner': 0.6}
}

# Timeframe-specific time stops
TIMEFRAME_TIME_STOPS = {
    '1M': {'optimal_duration': 15/60, 'max_duration': 30/60, 'stop_adjustment': 0.5},
    '5M': {'optimal_duration': 1, 'max_duration': 2, 'stop_adjustment': 0.5},
    '15M': {'optimal_duration': 3, 'max_duration': 6, 'stop_adjustment': 0.5},
    '30M': {'optimal_duration': 6, 'max_duration': 12, 'stop_adjustment': 0.6},
    '1H': {'optimal_duration': 12, 'max_duration': 24, 'stop_adjustment': 0.7},
    '4H': {'optimal_duration': 24, 'max_duration': 48, 'stop_adjustment': 0.8},
    'D1': {'optimal_duration': 72, 'max_duration': 120, 'stop_adjustment': 0.8}
}

# Timeframe-specific trailing stop settings
TIMEFRAME_TRAILING_SETTINGS = {
    '1M': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.7},
            {'threshold': 2.0, 'multiplier': 0.5},
            {'threshold': 3.0, 'multiplier': 0.3}
        ]
    },
    '5M': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.7},
            {'threshold': 2.0, 'multiplier': 0.5},
            {'threshold': 3.0, 'multiplier': 0.3}
        ]
    },
    '15M': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.8},
            {'threshold': 2.0, 'multiplier': 0.6},
            {'threshold': 3.0, 'multiplier': 0.4}
        ]
    },
    '30M': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.8},
            {'threshold': 2.0, 'multiplier': 0.6},
            {'threshold': 3.0, 'multiplier': 0.4}
        ]
    },
    '1H': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.8},
            {'threshold': 2.0, 'multiplier': 0.6},
            {'threshold': 3.0, 'multiplier': 0.4}
        ]
    },
    '4H': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.8},
            {'threshold': 2.0, 'multiplier': 0.5},
            {'threshold': 3.0, 'multiplier': 0.3}
        ]
    },
    'D1': {
        'initial_multiplier': 1.0,
        'profit_levels': [
            {'threshold': 1.0, 'multiplier': 0.8},
            {'threshold': 2.0, 'multiplier': 0.5},
            {'threshold': 3.0, 'multiplier': 0.3}
        ]
    }
}

# Timeframe-specific constants for risk management
TIMEFRAME_RISK_SETTINGS = {
    "M1": {
        "max_risk_per_trade": 0.01,         # Lower risk for shorter timeframes
        "max_open_trades": 5,               # Higher max trades count
        "min_win_rate": 0.55,               # Need higher win rate for shorter timeframes
        "min_risk_reward": 1.5,             # Lower R:R acceptable due to shorter duration
        "max_correlation_threshold": 0.5,   # Stricter correlation limits
        "position_timeout_minutes": 120,    # 2 hour max for M1 trades
        "trailing_stop_activation": 1.0     # Activate trailing at 1R profit
    },
    "M5": {
        "max_risk_per_trade": 0.012,
        "max_open_trades": 5,
        "min_win_rate": 0.53,
        "min_risk_reward": 1.5,
        "max_correlation_threshold": 0.5,
        "position_timeout_minutes": 240,    # 4 hour max
        "trailing_stop_activation": 1.0
    },
    "M15": {
        "max_risk_per_trade": 0.015,
        "max_open_trades": 4,
        "min_win_rate": 0.52,
        "min_risk_reward": 1.5,
        "max_correlation_threshold": 0.6,
        "position_timeout_minutes": 720,    # 12 hour max
        "trailing_stop_activation": 1.2
    },
    "M30": {
        "max_risk_per_trade": 0.015,
        "max_open_trades": 4,
        "min_win_rate": 0.5,
        "min_risk_reward": 1.8,
        "max_correlation_threshold": 0.6,
        "position_timeout_minutes": 1440,   # 24 hour max
        "trailing_stop_activation": 1.2
    },
    "H1": {
        "max_risk_per_trade": 0.02,         # Base risk reference
        "max_open_trades": 3,
        "min_win_rate": 0.45,
        "min_risk_reward": 2.0,
        "max_correlation_threshold": 0.7,
        "position_timeout_minutes": 2880,   # 2 day max
        "trailing_stop_activation": 1.5
    },
    "H4": {
        "max_risk_per_trade": 0.025,        # Higher risk for longer-term trades
        "max_open_trades": 3,
        "min_win_rate": 0.45,
        "min_risk_reward": 2.2,
        "max_correlation_threshold": 0.7,
        "position_timeout_minutes": 5760,   # 4 day max
        "trailing_stop_activation": 1.5
    },
    "D1": {
        "max_risk_per_trade": 0.03,
        "max_open_trades": 2,
        "min_win_rate": 0.42,
        "min_risk_reward": 2.5,
        "max_correlation_threshold": 0.8,
        "position_timeout_minutes": 11520,  # 8 day max
        "trailing_stop_activation": 2.0
    },
    "W1": {
        "max_risk_per_trade": 0.035,
        "max_open_trades": 2,
        "min_win_rate": 0.4,
        "min_risk_reward": 3.0,
        "max_correlation_threshold": 0.8,
        "position_timeout_minutes": 40320,  # 28 day max
        "trailing_stop_activation": 2.0
    }
}

# Multi-stage take profit levels by timeframe
TIMEFRAME_TAKE_PROFIT_LEVELS = {
    "M1": {
        "first_exit": 0.4,      # Exit 40% at first target
        "second_exit": 0.4,     # Exit 40% at second target
        "runner": 0.2           # Let 20% run for maximum target
    },
    "M5": {
        "first_exit": 0.4,
        "second_exit": 0.4,
        "runner": 0.2
    },
    "M15": {
        "first_exit": 0.3,
        "second_exit": 0.4,
        "runner": 0.3
    },
    "M30": {
        "first_exit": 0.3,
        "second_exit": 0.4,
        "runner": 0.3
    },
    "H1": {
        "first_exit": 0.3,
        "second_exit": 0.3,
        "runner": 0.4
    },
    "H4": {
        "first_exit": 0.3,
        "second_exit": 0.3,
        "runner": 0.4
    },
    "D1": {
        "first_exit": 0.25,
        "second_exit": 0.25,
        "runner": 0.5
    },
    "W1": {
        "first_exit": 0.2,
        "second_exit": 0.3,
        "runner": 0.5
    }
}

# Instrument type risk multipliers
INSTRUMENT_MULTIPLIERS = {
    "FOREX": 1.0,       # Base reference
    "FOREX_JPY": 0.9,   # Slightly lower for JPY due to volatility
    "CRYPTO": 0.7,      # Lowest due to high volatility
    "INDICES": 0.9,     # Slightly lower than base
    "METALS": 0.85,     # Gold can be volatile
    "ENERGY": 0.8,      # Oil can be volatile
    "STOCKS": 0.9,      # Slightly lower than base
    "BONDS": 1.1,       # Higher due to lower volatility
    "ETF": 0.95,        # Slightly lower than base
    "OTHER": 0.8        # Conservative default
}

##############################################################################
# Advanced Market Analysis Classes (from Script 1)
##############################################################################

class LorentzianClassifier:
    """
    A market regime classifier that uses Lorentzian distance metrics to classify market conditions.
    This helps determine if the market is trending, ranging, or in a high volatility state.
    """
    def __init__(self, lookback_period=20):
        self.lookback_period = lookback_period
        self.price_history = []
        self.regime_history = []
        self.current_regime = "UNKNOWN"
        self.confidence = 0.0
        
    def add_price(self, price: float) -> None:
        """Add a new price point to the classifier's history"""
        try:
            self.price_history.append(price)
            if len(self.price_history) > self.lookback_period:
                self.price_history.pop(0)
            self._classify_regime()
        except Exception as e:
            logging.error(f"Error adding price to Lorentzian classifier: {e}")
            
    def _calculate_lorentzian_distance(self, prices: List[float]) -> float:
        """
        Calculate Lorentzian distance metric for the given price series.
        This is more robust to outliers than Euclidean distance.
        """
        try:
            if len(prices) < 2:
                return 0.0
                
            distances = []
            for i in range(1, len(prices)):
                # Lorentzian distance: ln(1 + |x_i - x_{i-1}|)
                dist = math.log(1 + abs(prices[i] - prices[i-1]))
                distances.append(dist)
                
            return sum(distances) / len(distances)
        except Exception as e:
            logging.error(f"Error calculating Lorentzian distance: {e}")
            return 0.0
    
    def _calculate_fractal_dimension(self, prices: List[float]) -> float:
        """
        Calculate the approximate fractal dimension of the price series.
        Higher values indicate more randomness/complexity.
        """
        try:
            if len(prices) < 4:
                return 1.0
                
            # Using simplified box-counting dimension estimate
            n = len(prices)
            range_full = max(prices) - min(prices)
            if range_full == 0:
                return 1.0
                
            # Count the number of "boxes" needed to cover the curve
            box_size = range_full / 10.0
            boxes = set()
            
            for i, price in enumerate(prices):
                box_x = int(i / (n/10))
                box_y = int(price / box_size)
                boxes.add((box_x, box_y))
                
            # Fractal dimension approximation
            if len(boxes) <= 1:
                return 1.0
            return math.log(len(boxes)) / math.log(10)
        except Exception as e:
            logging.error(f"Error calculating fractal dimension: {e}")
            return 1.5  # Default mid-value
            
    def _classify_regime(self) -> None:
        """
        Classify the current market regime based on Lorentzian metrics
        and fractal dimension of recent price action.
        """
        try:
            if len(self.price_history) < self.lookback_period // 2:
                self.current_regime = "INSUFFICIENT_DATA"
                self.confidence = 0.0
                return
                
            lorentzian_dist = self._calculate_lorentzian_distance(self.price_history)
            fractal_dim = self._calculate_fractal_dimension(self.price_history)
            
            # Calculate price movement direction and strength
            price_change = self.price_history[-1] - self.price_history[0]
            price_strength = abs(price_change) / (max(self.price_history) - min(self.price_history) + 0.0001)
            
            # Classify regime based on metrics
            if fractal_dim > 1.7:
                self.current_regime = "VOLATILE"
                self.confidence = min(1.0, (fractal_dim - 1.7) * 3)
            elif lorentzian_dist < 0.1 and fractal_dim < 1.3:
                self.current_regime = "RANGING"
                self.confidence = min(1.0, (1.3 - fractal_dim) * 3)
            elif price_strength > 0.6 and lorentzian_dist > 0.15:
                if price_change > 0:
                    self.current_regime = "TRENDING_UP"
                else:
                    self.current_regime = "TRENDING_DOWN"
                self.confidence = min(1.0, price_strength * lorentzian_dist * 5)
            else:
                self.current_regime = "MIXED"
                self.confidence = 0.5
                
            self.regime_history.append(self.current_regime)
            if len(self.regime_history) > self.lookback_period:
                self.regime_history.pop(0)
        except Exception as e:
            logging.error(f"Error classifying market regime: {e}")
            self.current_regime = "ERROR"
            self.confidence = 0.0
            
    def get_regime(self) -> Tuple[str, float]:
        """
        Get the current market regime classification and confidence level.
        
        Returns:
            Tuple[str, float]: (regime_name, confidence_level)
        """
        return self.current_regime, self.confidence
        
    def get_risk_adjustment(self) -> float:
        """
        Get risk adjustment multiplier based on current regime.
        
        Returns:
            float: A multiplier to adjust risk (position size and stop distance)
        """
        regime, confidence = self.get_regime()
        
        if regime == "INSUFFICIENT_DATA" or regime == "ERROR":
            return 1.0  # Default - no adjustment
            
        if regime == "VOLATILE":
            # Reduce risk in volatile markets
            return max(0.3, 1.0 - confidence * 0.7)
            
        if regime == "RANGING":
            # Slightly reduce risk in ranging markets
            return max(0.7, 1.0 - confidence * 0.3)
            
        if regime in ["TRENDING_UP", "TRENDING_DOWN"]:
            # Increase risk in trending markets
            return min(1.5, 1.0 + confidence * 0.5)
            
        # Mixed regime - use default risk
        return 1.0

class VolatilityMonitor:
    """
    Tracks and analyzes market volatility to make dynamic adjustments to risk parameters.
    Uses historical volatility and ATR calculations to determine optimal position sizing
    and stop placement.
    """
    def __init__(self, lookback_period=14, atr_multiplier=1.5):
        self.lookback_period = lookback_period
        self.atr_multiplier = atr_multiplier
        self.high_prices = []
        self.low_prices = []
        self.close_prices = []
        self.current_atr = None
        self.historical_volatility = None
        self.volatility_percentile = 50  # Default middle percentile
        self.atr_history = []
        
    def add_candle(self, high: float, low: float, close: float) -> None:
        """Add a new price candle to the volatility monitor"""
        try:
            self.high_prices.append(high)
            self.low_prices.append(low)
            self.close_prices.append(close)
            
            if len(self.high_prices) > self.lookback_period:
                self.high_prices.pop(0)
                self.low_prices.pop(0)
                self.close_prices.pop(0)
                
            self._calculate_atr()
            self._calculate_historical_volatility()
            self._update_volatility_percentile()
        except Exception as e:
            logging.error(f"Error adding candle to volatility monitor: {e}")
            
    def _calculate_atr(self) -> None:
        """Calculate Average True Range (ATR) from price history"""
        try:
            if len(self.close_prices) < 2:
                self.current_atr = None
                return
                
            true_ranges = []
            for i in range(1, len(self.close_prices)):
                prev_close = self.close_prices[i-1]
                high = self.high_prices[i]
                low = self.low_prices[i]
                
                tr1 = high - low
                tr2 = abs(high - prev_close)
                tr3 = abs(low - prev_close)
                
                true_range = max(tr1, tr2, tr3)
                true_ranges.append(true_range)
                
            self.current_atr = sum(true_ranges) / len(true_ranges)
            self.atr_history.append(self.current_atr)
            
            if len(self.atr_history) > self.lookback_period * 3:
                self.atr_history.pop(0)
        except Exception as e:
            logging.error(f"Error calculating ATR: {e}")
            self.current_atr = None
            
    def _calculate_historical_volatility(self) -> None:
        """Calculate historical volatility (standard deviation of returns)"""
        try:
            if len(self.close_prices) < 2:
                self.historical_volatility = None
                return
                
            returns = []
            for i in range(1, len(self.close_prices)):
                ret = math.log(self.close_prices[i] / self.close_prices[i-1])
                returns.append(ret)
                
            std_dev = statistics.stdev(returns) if len(returns) > 1 else 0
            self.historical_volatility = std_dev * math.sqrt(252)  # Annualized
        except Exception as e:
            logging.error(f"Error calculating historical volatility: {e}")
            self.historical_volatility = None
            
    def _update_volatility_percentile(self) -> None:
        """Calculate the current volatility percentile relative to recent history"""
        try:
            if not self.current_atr or len(self.atr_history) < 5:
                self.volatility_percentile = 50  # Default
                return
                
            # Calculate what percentile the current ATR is in the history
            lower_values = sum(1 for atr in self.atr_history if atr < self.current_atr)
            self.volatility_percentile = (lower_values / len(self.atr_history)) * 100
        except Exception as e:
            logging.error(f"Error updating volatility percentile: {e}")
            self.volatility_percentile = 50
            
    def get_stop_distance(self, price: float) -> float:
        """
        Calculate the recommended stop loss distance based on current volatility.
        
        Args:
            price: Current market price
            
        Returns:
            float: Recommended stop distance in price units
        """
        try:
            if not self.current_atr:
                return price * 0.01  # Default 1% stop if no ATR available
                
            # Adjust ATR multiplier based on volatility percentile
            volatility_factor = 1.0
            if self.volatility_percentile > 80:
                volatility_factor = 1.2  # Wider stops in high volatility
            elif self.volatility_percentile < 20:
                volatility_factor = 0.8  # Tighter stops in low volatility
                
            return self.current_atr * self.atr_multiplier * volatility_factor
        except Exception as e:
            logging.error(f"Error calculating stop distance: {e}")
            return price * 0.01
            
    def get_risk_adjustment(self) -> float:
        """
        Get a risk adjustment multiplier based on current volatility conditions.
        
        Returns:
            float: Risk adjustment multiplier (0.5-1.5)
        """
        try:
            if not self.volatility_percentile:
                return 1.0
                
            # Higher volatility = lower position sizes
            if self.volatility_percentile > 80:
                return 0.7  # Reduce risk in high volatility
            elif self.volatility_percentile > 60:
                return 0.85
            elif self.volatility_percentile < 20:
                return 1.3  # Increase risk in low volatility
            elif self.volatility_percentile < 40:
                return 1.15
            else:
                return 1.0  # Normal risk
        except Exception as e:
            logging.error(f"Error getting risk adjustment: {e}")
            return 1.0
            
    def get_volatility_state(self) -> Dict[str, Any]:
        """
        Get the current volatility state information.
        
        Returns:
            Dict with current volatility metrics
        """
        return {
            "atr": self.current_atr,
            "historical_volatility": self.historical_volatility,
            "volatility_percentile": self.volatility_percentile,
            "risk_adjustment": self.get_risk_adjustment()
        }

class LorentzianDistanceClassifier:
    """
    Enhanced classifier that uses Lorentzian distance metrics to identify market conditions
    and trading opportunities. This implementation includes more robust feature extraction
    and better handling of different market phases.
    
    The classifier works by:
    1. Calculating Lorentzian distances between price points
    2. Extracting momentum and volatility features
    3. Using these to classify market conditions
    4. Generating trading signals based on the classification
    """
    def __init__(self, lookback_window=50, signal_threshold=0.75, feature_count=5):
        self.lookback_window = lookback_window
        self.signal_threshold = signal_threshold
        self.feature_count = feature_count
        self.price_history = []
        self.volume_history = []
        self.feature_matrix = []
        self.signals = []
        self.current_classification = "NEUTRAL"
        self.confidence_score = 0.0
        self.last_update_time = datetime.now()
        
    def add_data_point(self, price: float, volume: Optional[float] = None, 
                      additional_metrics: Optional[Dict[str, float]] = None) -> None:
        """
        Add a new data point to the classifier.
        
        Args:
            price: The current price
            volume: Optional volume data
            additional_metrics: Optional dictionary of additional features
        """
        try:
            self.price_history.append(price)
            if volume is not None:
                self.volume_history.append(volume)
            else:
                # Use placeholder if no volume data available
                self.volume_history.append(1.0)
                
            # Keep history within lookback window
            if len(self.price_history) > self.lookback_window:
                self.price_history.pop(0)
                self.volume_history.pop(0)
                
            # Only calculate features when we have enough data
            if len(self.price_history) >= 10:
                self._extract_features(additional_metrics)
                self._classify_market_condition()
                
            self.last_update_time = datetime.now()
        except Exception as e:
            logging.error(f"Error adding data to Lorentzian Distance Classifier: {e}")
            
    def _extract_features(self, additional_metrics: Optional[Dict[str, float]] = None) -> None:
        """
        Extract relevant features for classification from price and volume data.
        """
        try:
            if len(self.price_history) < 10:
                return
                
            # Basic features
            features = []
            
            # 1. Lorentzian distance features at multiple lags
            price_array = np.array(self.price_history)
            for lag in [1, 2, 3, 5, 8, 13]:
                if len(price_array) > lag:
                    # Calculate log(1 + |price_t - price_{t-lag}|)
                    diff = np.abs(price_array[lag:] - price_array[:-lag])
                    lorentzian_dist = np.log1p(diff).mean()
                    features.append(lorentzian_dist)
            
            # 2. Momentum features
            if len(price_array) >= 10:
                # Rate of change
                roc_5 = (price_array[-1] / price_array[-6] - 1) if price_array[-6] != 0 else 0
                roc_10 = (price_array[-1] / price_array[-11] - 1) if price_array[-11] != 0 else 0
                features.append(roc_5)
                features.append(roc_10)
                
                # Acceleration (change in momentum)
                momentum_change = roc_5 - roc_10
                features.append(momentum_change)
            
            # 3. Volatility features
            if len(price_array) >= 20:
                # Standard deviation of returns
                returns = np.diff(price_array) / price_array[:-1]
                vol_5 = np.std(returns[-5:]) if len(returns) >= 5 else 0
                vol_20 = np.std(returns) if len(returns) >= 20 else 0
                features.append(vol_5)
                features.append(vol_5 / vol_20 if vol_20 != 0 else 1.0)  # Volatility ratio
            
            # 4. Volume-weighted features (if volume data available)
            if len(self.volume_history) >= 10:
                volume_array = np.array(self.volume_history)
                # Volume momentum
                vol_change = (volume_array[-1] / volume_array[-10:].mean()) if volume_array[-10:].mean() != 0 else 1.0
                features.append(vol_change)
                
                # Volume-weighted price momentum
                if len(price_array) >= 10:
                    vwp_change = np.sum(np.diff(price_array[-10:]) * volume_array[-10:]) / np.sum(volume_array[-10:]) if np.sum(volume_array[-10:]) != 0 else 0
                    features.append(vwp_change)
                    
            # 5. Add any additional metrics provided
            if additional_metrics:
                for _, value in additional_metrics.items():
                    features.append(value)
                    
            # Ensure feature vector is consistent length by padding if necessary
            while len(features) < self.feature_count:
                features.append(0.0)
                
            # Or truncating if we have too many
            features = features[:self.feature_count]
                
            self.feature_matrix.append(features)
            if len(self.feature_matrix) > self.lookback_window:
                self.feature_matrix.pop(0)
                
        except Exception as e:
            logging.error(f"Error extracting features in Lorentzian Distance Classifier: {e}")
            
    def _calculate_distance_matrix(self) -> np.ndarray:
        """
        Calculate the matrix of Lorentzian distances between all pairs of feature vectors.
        """
        try:
            if not self.feature_matrix or len(self.feature_matrix) < 2:
                return np.array([[0.0]])
                
            n_samples = len(self.feature_matrix)
            dist_matrix = np.zeros((n_samples, n_samples))
            
            # Calculate distances between all pairs of feature vectors
            for i in range(n_samples):
                for j in range(i, n_samples):
                    # Lorentzian distance metric
                    vec1 = np.array(self.feature_matrix[i])
                    vec2 = np.array(self.feature_matrix[j])
                    distance = np.sum(np.log1p(np.abs(vec1 - vec2)))
                    
                    dist_matrix[i, j] = distance
                    dist_matrix[j, i] = distance  # Symmetric
                    
            return dist_matrix
        except Exception as e:
            logging.error(f"Error calculating distance matrix: {e}")
            return np.array([[0.0]])
            
    def _classify_market_condition(self) -> None:
        """
        Classify the current market condition based on extracted features.
        """
        try:
            if not self.feature_matrix or len(self.feature_matrix) < 5:
                self.current_classification = "INSUFFICIENT_DATA"
                self.confidence_score = 0.0
                return
                
            # Get the most recent feature vector
            current_features = np.array(self.feature_matrix[-1])
            
            # Simple threshold-based classification logic
            momentum_feature_idx = min(2, len(current_features) - 1)
            volatility_feature_idx = min(3, len(current_features) - 1)
            
            momentum = current_features[momentum_feature_idx]
            volatility = current_features[volatility_feature_idx]
            
            # Calculate a trending score and a volatility score
            trending_score = abs(momentum) / (volatility + 0.001)
            
            # Classification logic
            if trending_score > 2.0:
                # Strong trend
                if momentum > 0:
                    self.current_classification = "STRONG_UPTREND"
                    self.confidence_score = min(0.9, trending_score / 5.0)
                else:
                    self.current_classification = "STRONG_DOWNTREND"
                    self.confidence_score = min(0.9, trending_score / 5.0)
            elif trending_score > 1.0:
                # Moderate trend
                if momentum > 0:
                    self.current_classification = "UPTREND"
                    self.confidence_score = min(0.7, trending_score / 4.0)
                else:
                    self.current_classification = "DOWNTREND"
                    self.confidence_score = min(0.7, trending_score / 4.0)
            elif volatility > 0.02:
                # Volatile/choppy market
                self.current_classification = "VOLATILE"
                self.confidence_score = min(0.8, volatility * 20)
            else:
                # Ranging market
                self.current_classification = "RANGING"
                self.confidence_score = min(0.6, (0.03 - volatility) * 30)
                
            # Generate signal based on classification
            self._generate_signal()
            
        except Exception as e:
            logging.error(f"Error classifying market condition: {e}")
            self.current_classification = "ERROR"
            self.confidence_score = 0.0
            
    def _generate_signal(self) -> None:
        """
        Generate trading signals based on the current classification.
        """
        try:
            if self.current_classification in ["INSUFFICIENT_DATA", "ERROR"]:
                self.signals.append({"type": "NEUTRAL", "strength": 0.0, "timestamp": datetime.now()})
                return
                
            signal_type = "NEUTRAL"
            signal_strength = 0.0
            
            # Signal generation logic based on market classification
            if self.current_classification == "STRONG_UPTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "BUY"
                signal_strength = self.confidence_score
            elif self.current_classification == "STRONG_DOWNTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "SELL"
                signal_strength = self.confidence_score
            elif self.current_classification == "UPTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "WEAK_BUY"
                signal_strength = self.confidence_score
            elif self.current_classification == "DOWNTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "WEAK_SELL"
                signal_strength = self.confidence_score
            elif self.current_classification == "VOLATILE":
                signal_type = "NEUTRAL"  # Avoid trading in highly volatile conditions
                signal_strength = 0.3
            elif self.current_classification == "RANGING":
                # In ranging markets, consider mean-reversion strategies instead
                recent_momentum = self.feature_matrix[-1][2] if len(self.feature_matrix) > 0 and len(self.feature_matrix[-1]) > 2 else 0
                if recent_momentum > 0.01:
                    signal_type = "RANGE_SELL"  # Approaching upper range, potential sell
                    signal_strength = min(0.6, abs(recent_momentum) * 20)
                elif recent_momentum < -0.01:
                    signal_type = "RANGE_BUY"  # Approaching lower range, potential buy
                    signal_strength = min(0.6, abs(recent_momentum) * 20)
                    
            # Store the signal
            signal = {
                "type": signal_type,
                "strength": signal_strength,
                "classification": self.current_classification,
                "confidence": self.confidence_score,
                "timestamp": datetime.now()
            }
            
            self.signals.append(signal)
            if len(self.signals) > self.lookback_window:
                self.signals.pop(0)
                
        except Exception as e:
            logging.error(f"Error generating signal in Lorentzian Distance Classifier: {e}")
            
    def get_current_classification(self) -> Dict[str, Any]:
        """
        Get the current market classification and confidence.
        
        Returns:
            Dict with classification details
        """
        return {
            "classification": self.current_classification,
            "confidence": self.confidence_score,
            "last_update": self.last_update_time.isoformat()
        }
        
    def get_latest_signal(self) -> Dict[str, Any]:
        """
        Get the latest trading signal.
        
        Returns:
            Dict with signal details or empty dict if no signals
        """
        if not self.signals:
            return {}
        return self.signals[-1]
        
    def get_signal_distribution(self, lookback: int = 10) -> Dict[str, float]:
        """
        Get the distribution of recent signals to identify persistent patterns.
        
        Args:
            lookback: Number of recent signals to consider
            
        Returns:
            Dict mapping signal types to their frequency
        """
        if not self.signals:
            return {"NEUTRAL": 1.0}
            
        recent_signals = self.signals[-min(lookback, len(self.signals)):]
        signal_counts = {}
        
        for signal in recent_signals:
            signal_type = signal["type"]
            if signal_type not in signal_counts:
                signal_counts[signal_type] = 0
            signal_counts[signal_type] += 1
            
        # Convert to frequencies
        total = len(recent_signals)
        return {k: v/total for k, v in signal_counts.items()}
        
    def get_trend_strength(self) -> float:
        """
        Get the current trend strength as a normalized value between -1 and 1.
        Negative values indicate downtrend, positive values indicate uptrend.
        
        Returns:
            float: Trend strength indicator
        """
        if self.current_classification == "STRONG_UPTREND":
            return self.confidence_score
        elif self.current_classification == "UPTREND":
            return self.confidence_score * 0.7
        elif self.current_classification == "STRONG_DOWNTREND":
            return -self.confidence_score
        elif self.current_classification == "DOWNTREND":
            return -self.confidence_score * 0.7
        elif self.current_classification == "RANGING":
            # Slight bias based on recent momentum
            if len(self.feature_matrix) > 0 and len(self.feature_matrix[-1]) > 2:
                recent_momentum = self.feature_matrix[-1][2]
                return recent_momentum * 0.3  # Reduced strength in ranging markets
        
        return 0.0  # Neutral or insufficient data

class VolatilityBasedRiskManager:
    """
    Advanced risk management system that adapts position sizing and stop loss distances
    based on current market volatility and price action characteristics.
    
    This risk manager:
    1. Dynamically adjusts position sizes based on volatility
    2. Sets appropriate stop loss distances using ATR and volatility patterns
    3. Implements sophisticated risk management rules
    4. Provides position sizing recommendations
    5. Adapts to changing market conditions
    """
    def __init__(self, 
                base_risk_per_trade: float = 0.01,  # Default 1% risk per trade
                max_risk_per_trade: float = 0.03,   # Maximum 3% risk per trade
                volatility_lookback: int = 20,      # Lookback period for volatility
                atr_multiplier_base: float = 1.5,   # Base ATR multiplier for stop loss
                max_correlation_impact: float = 0.5, # Maximum impact of correlation on risk
                max_portfolio_heat: float = 0.15,    # Maximum 15% portfolio risk
                max_single_market_heat: float = 0.10, # Maximum 10% risk in a single market
                max_instrument_category_heat: float = 0.12 # Maximum 12% risk per instrument category
               ):
        self.base_risk_per_trade = base_risk_per_trade
        self.max_risk_per_trade = max_risk_per_trade
        self.volatility_lookback = volatility_lookback
        self.atr_multiplier_base = atr_multiplier_base
        self.max_correlation_impact = max_correlation_impact
        
        # Portfolio heat management
        self.max_portfolio_heat = max_portfolio_heat
        self.max_single_market_heat = max_single_market_heat
        self.max_instrument_category_heat = max_instrument_category_heat
        
        # Track volatility state for different instruments
        self.instrument_volatility = {}
        
        # Track correlations between instruments in the portfolio
        self.correlation_matrix = {}
        
        # Enhanced portfolio heat tracking
        self.current_portfolio_risk = 0.0
        self.portfolio_heat_map = {
            "total": 0.0,
            "by_instrument": {},
            "by_direction": {"LONG": 0.0, "SHORT": 0.0},
            "by_category": {
                "FOREX": 0.0,
                "CRYPTO": 0.0,
                "METALS": 0.0,
                "INDICES": 0.0,
                "OTHER": 0.0
            },
            "by_session": {
                "ASIAN": 0.0,
                "LONDON": 0.0,
                "NEWYORK": 0.0,
                "SYDNEY": 0.0
            },
            "by_timeframe": {}
        }
        
        # Track open positions
        self.open_positions = {}
        
        # Performance tracking
        self.trade_history = []
        self.drawdown_history = []
        self.max_drawdown = 0.0
        
        # Heat thresholds monitoring
        self.heat_thresholds = {
            "warning": 0.7,  # 70% of max heat
            "critical": 0.9   # 90% of max heat
        }
        
        # Track heat peaks and patterns
        self.heat_snapshots = []
        
    def add_price_data(self, 
                       instrument: str, 
                       high: float, 
                       low: float, 
                       close: float) -> None:
        """
        Add new price data for an instrument to update its volatility profile.
        
        Args:
            instrument: Instrument identifier
            high: High price
            low: Low price 
            close: Close price
        """
        try:
            if instrument not in self.instrument_volatility:
                self.instrument_volatility[instrument] = {
                    "highs": [],
                    "lows": [],
                    "closes": [],
                    "atr": None,
                    "volatility": None,
                    "atr_percentile": 50,  # Default middle percentile
                    "updated": datetime.now()
                }
                
            volatility_data = self.instrument_volatility[instrument]
            
            # Add new data
            volatility_data["highs"].append(high)
            volatility_data["lows"].append(low)
            volatility_data["closes"].append(close)
            
            # Keep within lookback period
            if len(volatility_data["highs"]) > self.volatility_lookback:
                volatility_data["highs"].pop(0)
                volatility_data["lows"].pop(0)
                volatility_data["closes"].pop(0)
                
            # Update calculations
            self._calculate_instrument_volatility(instrument)
            volatility_data["updated"] = datetime.now()
            
        except Exception as e:
            logging.error(f"Error adding price data to volatility risk manager: {e}")
            
    def _calculate_instrument_volatility(self, instrument: str) -> None:
        """Calculate volatility metrics for an instrument"""
        try:
            vol_data = self.instrument_volatility[instrument]
            
            # Need at least 2 data points
            if len(vol_data["closes"]) < 2:
                return
                
            # Calculate ATR
            true_ranges = []
            for i in range(1, len(vol_data["closes"])):
                high = vol_data["highs"][i]
                low = vol_data["lows"][i]
                prev_close = vol_data["closes"][i-1]
                
                tr1 = high - low
                tr2 = abs(high - prev_close)
                tr3 = abs(low - prev_close)
                
                true_range = max(tr1, tr2, tr3)
                true_ranges.append(true_range)
                
            vol_data["atr"] = sum(true_ranges) / len(true_ranges) if true_ranges else None
            
            # Calculate historical volatility (standard deviation of returns)
            if len(vol_data["closes"]) > 1:
                prices = vol_data["closes"]
                returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
                vol_data["volatility"] = statistics.stdev(returns) * math.sqrt(252) if len(returns) > 1 else None
                
            # Store ATR history for percentile calculation
            if "atr_history" not in vol_data:
                vol_data["atr_history"] = []
                
            if vol_data["atr"] is not None:
                vol_data["atr_history"].append(vol_data["atr"])
                if len(vol_data["atr_history"]) > self.volatility_lookback * 3:
                    vol_data["atr_history"].pop(0)
                    
                # Calculate ATR percentile
                if len(vol_data["atr_history"]) > 5:
                    current_atr = vol_data["atr"]
                    history = vol_data["atr_history"]
                    lower_values = sum(1 for atr in history if atr < current_atr)
                    vol_data["atr_percentile"] = (lower_values / len(history)) * 100
                    
        except Exception as e:
            logging.error(f"Error calculating instrument volatility: {e}")
            
    def update_correlation_matrix(self, price_data: Dict[str, List[float]]) -> None:
        """
        Update the correlation matrix between instruments.
        
        Args:
            price_data: Dict mapping instrument names to lists of closing prices
        """
        try:
            # Need at least 2 instruments with data
            instruments = list(price_data.keys())
            if len(instruments) < 2:
                return
                
            # Convert to returns
            returns_data = {}
            for instrument, prices in price_data.items():
                if len(prices) < 2:
                    continue
                returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
                returns_data[instrument] = returns
                
            # Calculate pairwise correlations
            for i in range(len(instruments)):
                inst1 = instruments[i]
                if inst1 not in returns_data:
                    continue
                    
                for j in range(i+1, len(instruments)):
                    inst2 = instruments[j]
                    if inst2 not in returns_data:
                        continue
                        
                    # Get common length
                    ret1 = returns_data[inst1]
                    ret2 = returns_data[inst2]
                    common_length = min(len(ret1), len(ret2))
                    
                    if common_length < 10:  # Need minimum data for meaningful correlation
                        continue
                        
                    # Calculate correlation
                    ret1 = ret1[-common_length:]
                    ret2 = ret2[-common_length:]
                    
                    # Check for zero variance
                    if np.std(ret1) == 0 or np.std(ret2) == 0:
                        correlation = 0
                    else:
                        correlation = np.corrcoef(ret1, ret2)[0, 1]
                        
                    # Store correlation
                    if inst1 not in self.correlation_matrix:
                        self.correlation_matrix[inst1] = {}
                    if inst2 not in self.correlation_matrix:
                        self.correlation_matrix[inst2] = {}
                        
                    self.correlation_matrix[inst1][inst2] = correlation
                    self.correlation_matrix[inst2][inst1] = correlation
                    
        except Exception as e:
            logging.error(f"Error updating correlation matrix: {e}")
            
    def calculate_position_size(self, 
                               account_balance: float,
                               instrument: str,
                               entry_price: float,
                               stop_loss: float,
                               risk_modifier: float = 1.0,
                               timeframe: str = "H1") -> Dict[str, Any]:
        """
        Calculate optimal position size based on account balance, volatility,
        and risk parameters.
        
        Args:
            account_balance: Current account balance
            instrument: Trading instrument
            entry_price: Entry price
            stop_loss: Stop loss price
            risk_modifier: Optional multiplier to adjust risk (0.5 = half risk, 2.0 = double risk)
            timeframe: Trading timeframe
            
        Returns:
            Dict with position sizing information
        """
        try:
            # Validate inputs
            if entry_price <= 0 or stop_loss <= 0:
                logging.error(f"Invalid entry price or stop loss: {entry_price}, {stop_loss}")
                return {"error": "Invalid price inputs", "units": 0}
                
            risk_amount_pct = self.base_risk_per_trade
            
            # Adjust risk based on volatility
            if instrument in self.instrument_volatility:
                vol_data = self.instrument_volatility[instrument]
                
                # Higher volatility = lower position size
                if vol_data.get("atr_percentile", 50) > 80:
                    risk_amount_pct *= 0.7  # Reduce risk in high volatility
                elif vol_data.get("atr_percentile", 50) > 60:
                    risk_amount_pct *= 0.85
                elif vol_data.get("atr_percentile", 50) < 20:
                    risk_amount_pct *= 1.3  # Increase risk in low volatility
                elif vol_data.get("atr_percentile", 50) < 40:
                    risk_amount_pct *= 1.15
            
            # Adjust risk based on timeframe
            if timeframe == "M1":
                risk_amount_pct *= 0.7  # Lower risk for very short timeframes
            elif timeframe == "M5":
                risk_amount_pct *= 0.8
            elif timeframe == "H4":
                risk_amount_pct *= 1.1  # Higher risk for longer timeframes
            elif timeframe == "D1":
                risk_amount_pct *= 1.2
                
            # Apply risk modifier
            risk_amount_pct *= risk_modifier
            
            # Cap at maximum risk percentage
            risk_amount_pct = min(risk_amount_pct, self.max_risk_per_trade)
            
            # Calculate risk amount
            risk_amount = account_balance * risk_amount_pct
            
            # Calculate risk per unit
            stop_distance = abs(entry_price - stop_loss)
            if stop_distance == 0:
                logging.error("Stop distance cannot be zero")
                return {"error": "Invalid stop distance", "units": 0}
                
            # Calculate appropriate units based on risk amount and stop distance
            units = risk_amount / stop_distance
            
            # Adjust for instrument type
            instrument_type = self._get_instrument_type(instrument)
            if instrument_type == "FOREX":
                units = units * 100  # Standard conversion for forex
            elif instrument_type == "CRYPTO":
                units = units * 1  # Direct units for crypto
            elif instrument_type == "INDICES":
                units = units * 10  # Adjusted for indices
                
            # Check correlation impact
            correlation_adjustment = self._calculate_correlation_adjustment(instrument)
            units = units * correlation_adjustment
            
            # Check if this would exceed portfolio risk limits
            new_position_risk = risk_amount_pct
            projected_portfolio_risk = self.current_portfolio_risk + new_position_risk
            
            if projected_portfolio_risk > self.max_portfolio_heat:
                # Scale down to fit within portfolio risk limit
                scale_factor = (self.max_portfolio_heat - self.current_portfolio_risk) / new_position_risk
                units = units * max(0.1, scale_factor)  # Ensure at least 10% of calculated size
                new_position_risk = new_position_risk * scale_factor
                
            return {
                "units": units,
                "risk_amount": risk_amount,
                "risk_percentage": risk_amount_pct * 100,  # Convert to percentage
                "stop_distance": stop_distance,
                "correlation_adjustment": correlation_adjustment,
                "portfolio_risk_contribution": new_position_risk * 100,  # Convert to percentage
                "current_portfolio_risk": self.current_portfolio_risk * 100,  # Convert to percentage
                "projected_portfolio_risk": projected_portfolio_risk * 100  # Convert to percentage
            }
            
        except Exception as e:
            logging.error(f"Error calculating position size: {e}")
            return {"error": str(e), "units": 0}
            
    def _get_instrument_type(self, instrument: str) -> str:
        """Determine the type of instrument based on its name"""
        instrument = instrument.upper()
        
        if instrument.startswith("XAU") or instrument.startswith("XAG"):
            return "METALS"
        elif "_" in instrument and any(x in instrument for x in ["USD", "EUR", "GBP", "JPY", "AUD", "CAD"]):
            return "FOREX"
        elif any(x in instrument for x in ["BTC", "ETH", "XRP", "LTC"]):
            return "CRYPTO"
        elif any(x in instrument for x in ["SPX", "NDX", "UK", "JP", "DE"]):
            return "INDICES"
        else:
            return "OTHER"
            
    def _calculate_correlation_adjustment(self, instrument: str) -> float:
        """
        Calculate a risk adjustment factor based on correlation with existing positions.
        Returns a value between (1 - max_correlation_impact) and 1.0.
        """
        try:
            # If no open positions or no correlation data, no adjustment
            if not self.open_positions or instrument not in self.correlation_matrix:
                return 1.0
                
            # Calculate weighted average correlation with existing positions
            total_weight = 0
            weighted_correlation = 0
            
            for position_instrument, position_data in self.open_positions.items():
                position_size = position_data.get("risk_contribution", 0)
                
                if position_instrument in self.correlation_matrix.get(instrument, {}):
                    correlation = abs(self.correlation_matrix[instrument][position_instrument])
                    weighted_correlation += correlation * position_size
                    total_weight += position_size
                    
            if total_weight == 0:
                return 1.0
                
            avg_correlation = weighted_correlation / total_weight
            
            # Higher correlation = lower position size (more conservative)
            correlation_adjustment = 1.0 - (avg_correlation * self.max_correlation_impact)
            
            return max(1.0 - self.max_correlation_impact, correlation_adjustment)
            
        except Exception as e:
            logging.error(f"Error calculating correlation adjustment: {e}")
            return 1.0
            
    def calculate_stop_loss_distance(self, 
                                    instrument: str, 
                                    price: float, 
                                    direction: str,
                                    timeframe: str) -> float:
        """
        Calculate optimal stop loss distance based on instrument volatility.
        
        Args:
            instrument: Trading instrument
            price: Current price
            direction: Trade direction ("BUY" or "SELL")
            timeframe: Trading timeframe
            
        Returns:
            float: Recommended stop loss distance in price units
        """
        try:
            base_atr_multiplier = self.atr_multiplier_base
            
            # Adjust multiplier based on timeframe
            if timeframe == "M1":
                base_atr_multiplier *= 1.2  # Wider stops for very short timeframes (noise)
            elif timeframe == "M5":
                base_atr_multiplier *= 1.1
            elif timeframe == "H4":
                base_atr_multiplier *= 0.9  # Tighter stops for longer timeframes
            elif timeframe == "D1":
                base_atr_multiplier *= 0.8
                
            # Get ATR if available
            atr = None
            if instrument in self.instrument_volatility:
                atr = self.instrument_volatility[instrument].get("atr")
                
                # Adjust based on ATR percentile
                atr_percentile = self.instrument_volatility[instrument].get("atr_percentile", 50)
                
                if atr_percentile > 80:
                    base_atr_multiplier *= 1.2  # Wider stops in high volatility
                elif atr_percentile > 60:
                    base_atr_multiplier *= 1.1
                elif atr_percentile < 20:
                    base_atr_multiplier *= 0.8  # Tighter stops in low volatility
                elif atr_percentile < 40:
                    base_atr_multiplier *= 0.9
                    
            # If no ATR available, use a percentage of price
            if atr is None:
                # Default percentages based on instrument type
                instrument_type = self._get_instrument_type(instrument)
                if instrument_type == "FOREX":
                    return price * 0.005  # 0.5% for forex
                elif instrument_type == "CRYPTO":
                    return price * 0.03   # 3% for crypto
                elif instrument_type == "METALS":
                    return price * 0.01   # 1% for metals
                else:
                    return price * 0.02   # 2% for other instruments
                    
            # Calculate stop distance
            stop_distance = atr * base_atr_multiplier
            
            # Return raw distance (to be applied based on direction)
            return stop_distance
            
        except Exception as e:
            logging.error(f"Error calculating stop loss distance: {e}")
            return price * 0.02  # Fallback to 2% of price
            
    def register_position(self, 
                         position_id: str,
                         instrument: str,
                         direction: str,
                         entry_price: float,
                         stop_loss: float,
                         position_size: float,
                         risk_percentage: float,
                         timeframe: str = "H1",
                         session: str = None) -> Dict[str, Any]:
        """
        Register a new position with the risk manager.
        
        Args:
            position_id: Unique identifier for the position
            instrument: Trading instrument
            direction: Trade direction ("BUY" or "SELL")
            entry_price: Entry price
            stop_loss: Stop loss price
            position_size: Position size (units/contracts/etc)
            risk_percentage: Risk percentage for this position
            timeframe: Trading timeframe
            session: Trading session (ASIAN, LONDON, etc.)
            
        Returns:
            Dict with registration result and heat metrics
        """
        try:
            # Calculate risk contribution
            risk_contribution = risk_percentage / 100.0  # Convert percentage to decimal
            
            # Determine instrument category
            instrument_category = self._get_instrument_type(instrument)
            
            # Determine current session if not provided
            if session is None:
                session = get_current_market_session(datetime.now())
            
            # Get current heat levels before adding new position
            current_heat = self.get_heat_levels()
            
            # Check if this position would violate heat limits
            new_category_heat = current_heat["by_category"].get(instrument_category, 0) + risk_contribution
            new_instrument_heat = current_heat["by_instrument"].get(instrument, 0) + risk_contribution
            new_direction_heat = current_heat["by_direction"].get(direction, 0) + risk_contribution
            new_total_heat = current_heat["total"] + risk_contribution
            
            heat_violations = []
            
            if new_category_heat > self.max_instrument_category_heat:
                heat_violations.append(f"Category heat limit exceeded: {instrument_category} would reach {new_category_heat*100:.1f}%")
                
            if new_instrument_heat > self.max_single_market_heat:
                heat_violations.append(f"Instrument heat limit exceeded: {instrument} would reach {new_instrument_heat*100:.1f}%")
                
            if new_total_heat > self.max_portfolio_heat:
                heat_violations.append(f"Total portfolio heat limit exceeded: would reach {new_total_heat*100:.1f}%")
            
            # Check if adding this position would exceed limits
            if heat_violations:
                logging.warning(f"Position {position_id} registration refused due to heat limits: {heat_violations}")
                return {
                    "success": False,
                    "message": "Heat limits exceeded",
                    "violations": heat_violations,
                    "heat_levels": current_heat
                }
            
            # Add to open positions
            self.open_positions[position_id] = {
                "instrument": instrument,
                "direction": direction,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "position_size": position_size,
                "risk_contribution": risk_contribution,
                "entry_time": datetime.now(),
                "category": instrument_category,
                "timeframe": timeframe,
                "session": session
            }
            
            # Update portfolio heat map
            self._update_heat_map_for_position(position_id, risk_contribution, instrument, direction, instrument_category, session, timeframe, is_adding=True)
            
            # Take a heat snapshot
            self._add_heat_snapshot()
            
            logging.info(f"Registered position {position_id} with risk contribution {risk_contribution*100:.2f}%")
            
            return {
                "success": True,
                "message": f"Position registered with {risk_contribution*100:.2f}% risk",
                "new_heat_levels": self.get_heat_levels(),
                "position_id": position_id
            }
            
        except Exception as e:
            logging.error(f"Error registering position: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "heat_levels": self.get_heat_levels()
            }
            
    def _update_heat_map_for_position(self, position_id, risk_amount, instrument, direction, category, session, timeframe, is_adding=True):
        """Update the heat map when adding or removing a position"""
        multiplier = 1 if is_adding else -1
        
        # Update total heat
        self.portfolio_heat_map["total"] += risk_amount * multiplier
        
        # Update by instrument
        if instrument not in self.portfolio_heat_map["by_instrument"]:
            self.portfolio_heat_map["by_instrument"][instrument] = 0
        self.portfolio_heat_map["by_instrument"][instrument] += risk_amount * multiplier
        
        # Update by direction
        direction_key = direction.upper()
        if direction_key not in self.portfolio_heat_map["by_direction"]:
            self.portfolio_heat_map["by_direction"][direction_key] = 0
        self.portfolio_heat_map["by_direction"][direction_key] += risk_amount * multiplier
        
        # Update by category
        if category not in self.portfolio_heat_map["by_category"]:
            self.portfolio_heat_map["by_category"][category] = 0
        self.portfolio_heat_map["by_category"][category] += risk_amount * multiplier
        
        # Update by session
        if session and session in self.portfolio_heat_map["by_session"]:
            self.portfolio_heat_map["by_session"][session] += risk_amount * multiplier
        
        # Update by timeframe
        if timeframe not in self.portfolio_heat_map["by_timeframe"]:
            self.portfolio_heat_map["by_timeframe"][timeframe] = 0
        self.portfolio_heat_map["by_timeframe"][timeframe] += risk_amount * multiplier
        
        # Also update portfolio risk (for backward compatibility)
        self.current_portfolio_risk = self.portfolio_heat_map["total"]
        
    def _add_heat_snapshot(self):
        """Take a snapshot of current heat levels for trend analysis"""
        snapshot = {
            "timestamp": datetime.now(),
            "total_heat": self.portfolio_heat_map["total"],
            "direction_ratio": self._calculate_direction_ratio(),
            "category_distribution": self._calculate_category_distribution(),
            "position_count": len(self.open_positions)
        }
        
        self.heat_snapshots.append(snapshot)
        
        # Keep snapshot history manageable
        if len(self.heat_snapshots) > 100:
            self.heat_snapshots.pop(0)
            
    def _calculate_direction_ratio(self):
        """Calculate the ratio of long to short exposure"""
        long_exposure = self.portfolio_heat_map["by_direction"].get("LONG", 0)
        short_exposure = self.portfolio_heat_map["by_direction"].get("SHORT", 0)
        
        if short_exposure == 0:
            return long_exposure if long_exposure > 0 else 0
            
        return long_exposure / short_exposure
        
    def _calculate_category_distribution(self):
        """Calculate the percentage distribution of risk across categories"""
        total = self.portfolio_heat_map["total"]
        if total == 0:
            return {k: 0 for k in self.portfolio_heat_map["by_category"]}
            
        return {k: v/total for k, v in self.portfolio_heat_map["by_category"].items()}
        
    def close_position(self, position_id: str, exit_price: float, win: bool) -> Dict[str, Any]:
        """
        Close a position and update risk manager state.
        
        Args:
            position_id: Position identifier
            exit_price: Exit price
            win: Whether the trade was a winner
            
        Returns:
            Dict with closure details and updated heat metrics
        """
        try:
            if position_id not in self.open_positions:
                logging.warning(f"Attempted to close unknown position: {position_id}")
                return {
                    "success": False,
                    "message": "Position not found",
                    "heat_levels": self.get_heat_levels()
                }
                
            position = self.open_positions[position_id]
            
            # Calculate profit/loss
            entry = position["entry_price"]
            direction = position["direction"]
            
            pnl_pct = 0
            if direction.upper() == "BUY" or direction.upper() == "LONG":
                pnl_pct = (exit_price - entry) / entry
            else:
                pnl_pct = (entry - exit_price) / entry
                
            # Add to trade history
            trade_record = {
                "position_id": position_id,
                "instrument": position["instrument"],
                "direction": direction,
                "entry_price": entry,
                "exit_price": exit_price,
                "position_size": position["position_size"],
                "pnl_percentage": pnl_pct,
                "risk_percentage": position["risk_contribution"] * 100,
                "entry_time": position["entry_time"],
                "exit_time": datetime.now(),
                "duration": (datetime.now() - position["entry_time"]).total_seconds() / 3600.0,  # Hours
                "win": win,
                "category": position.get("category", self._get_instrument_type(position["instrument"])),
                "timeframe": position.get("timeframe", "unknown"),
                "session": position.get("session", "unknown")
            }
            
            self.trade_history.append(trade_record)
            
            # Update heat map
            self._update_heat_map_for_position(
                position_id, 
                position["risk_contribution"], 
                position["instrument"], 
                position["direction"], 
                position.get("category", self._get_instrument_type(position["instrument"])),
                position.get("session", "unknown"),
                position.get("timeframe", "unknown"),
                is_adding=False
            )
            
            # Track drawdown
            if not win:
                self.drawdown_history.append(position["risk_contribution"])
                # Calculate trailing drawdown
                if len(self.drawdown_history) > 20:
                    self.drawdown_history.pop(0)
                    
                current_drawdown = sum(self.drawdown_history)
                self.max_drawdown = max(self.max_drawdown, current_drawdown)
                
            # Remove from open positions
            del self.open_positions[position_id]
            
            # Take a heat snapshot
            self._add_heat_snapshot()
            
            logging.info(f"Closed position {position_id} with P&L {pnl_pct*100:.2f}%")
            
            return {
                "success": True,
                "message": f"Position closed with P&L: {pnl_pct*100:.2f}%",
                "pnl_percentage": pnl_pct * 100,
                "risk_percentage": trade_record["risk_percentage"],
                "duration_hours": trade_record["duration"],
                "heat_levels": self.get_heat_levels()
            }
            
        except Exception as e:
            logging.error(f"Error closing position: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "heat_levels": self.get_heat_levels()
            }
            
    def get_heat_levels(self) -> Dict[str, Any]:
        """
        Get detailed portfolio heat levels and distribution.
        
        Returns:
            Dict with comprehensive heat metrics
        """
        # Calculate heat utilization percentages
        heat_utilization = {
            "total": (self.portfolio_heat_map["total"] / self.max_portfolio_heat) * 100 if self.max_portfolio_heat > 0 else 0,
            "by_category": {}
        }
        
        for category, heat in self.portfolio_heat_map["by_category"].items():
            heat_utilization["by_category"][category] = (heat / self.max_instrument_category_heat) * 100 if self.max_instrument_category_heat > 0 else 0
            
        # Get top instruments by heat
        instruments_by_heat = sorted(
            [(k, v) for k, v in self.portfolio_heat_map["by_instrument"].items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        top_instruments = [{"instrument": k, "heat": v * 100} for k, v in instruments_by_heat[:5]]
        
        # Determine overall heat status
        heat_status = "NORMAL"
        if heat_utilization["total"] > self.heat_thresholds["critical"] * 100:
            heat_status = "CRITICAL"
        elif heat_utilization["total"] > self.heat_thresholds["warning"] * 100:
            heat_status = "WARNING"
            
        # Calculate direction bias
        direction_ratio = self._calculate_direction_ratio()
        if direction_ratio > 1.5:
            direction_bias = "LONG_HEAVY"
        elif direction_ratio < 0.67:
            direction_bias = "SHORT_HEAVY"
        else:
            direction_bias = "BALANCED"
            
        return {
            "total": self.portfolio_heat_map["total"] * 100,  # As percentage
            "by_direction": {k: v * 100 for k, v in self.portfolio_heat_map["by_direction"].items()},
            "by_category": {k: v * 100 for k, v in self.portfolio_heat_map["by_category"].items()},
            "by_timeframe": {k: v * 100 for k, v in self.portfolio_heat_map["by_timeframe"].items()},
            "by_session": {k: v * 100 for k, v in self.portfolio_heat_map["by_session"].items()},
            "by_instrument": {k: v * 100 for k, v in self.portfolio_heat_map["by_instrument"].items()},
            "utilization": heat_utilization,
            "heat_status": heat_status,
            "direction_bias": direction_bias,
            "position_count": len(self.open_positions),
            "top_instruments": top_instruments,
            "limits": {
                "max_portfolio_heat": self.max_portfolio_heat * 100,
                "max_single_market_heat": self.max_single_market_heat * 100,
                "max_category_heat": self.max_instrument_category_heat * 100
            }
        }
        
    def get_heat_trends(self) -> Dict[str, Any]:
        """
        Get trends in portfolio heat over time.
        
        Returns:
            Dict with heat trend analysis
        """
        if not self.heat_snapshots:
            return {"trend": "NEUTRAL", "data": [], "average": 0}
            
        # Extract heat values and timestamps
        timestamps = [s["timestamp"] for s in self.heat_snapshots]
        heat_values = [s["total_heat"] * 100 for s in self.heat_snapshots]  # As percentages
        
        # Calculate average and recent trend
        avg_heat = sum(heat_values) / len(heat_values)
        
        # Need at least 2 snapshots for trend
        if len(heat_values) < 2:
            trend = "NEUTRAL"
        else:
            # Look at last 5 snapshots or all if fewer
            recent_values = heat_values[-min(5, len(heat_values)):]
            
            if recent_values[-1] > recent_values[0] * 1.1:
                trend = "INCREASING"
            elif recent_values[-1] < recent_values[0] * 0.9:
                trend = "DECREASING"
            else:
                trend = "STABLE"
                
        return {
            "trend": trend,
            "data": [{"timestamp": t.isoformat(), "heat": h} for t, h in zip(timestamps, heat_values)],
            "average": avg_heat,
            "current": heat_values[-1] if heat_values else 0,
            "min": min(heat_values) if heat_values else 0,
            "max": max(heat_values) if heat_values else 0
        }
        
    def get_portfolio_diversity_score(self) -> float:
        """
        Calculate portfolio diversity score (0-100).
        Higher scores indicate better diversification.
        
        Returns:
            float: Diversity score
        """
        try:
            if not self.open_positions:
                return 0.0
                
            # Factors that contribute to diversity:
            # 1. Distribution across categories
            # 2. Direction balance
            # 3. Correlation between positions
            # 4. Distribution across timeframes
            
            # 1. Category distribution (higher entropy = more diverse)
            category_weights = list(self.portfolio_heat_map["by_category"].values())
            category_weights = [w for w in category_weights if w > 0]
            
            if not category_weights:
                category_score = 0
            else:
                # Normalize weights
                total = sum(category_weights)
                if total > 0:
                    category_weights = [w/total for w in category_weights]
                    # Calculate entropy
                    entropy = -sum(w * math.log(w) for w in category_weights if w > 0)
                    # Max entropy for n categories is log(n)
                    max_entropy = math.log(len(self.portfolio_heat_map["by_category"]))
                    category_score = (entropy / max_entropy) * 100 if max_entropy > 0 else 0
                else:
                    category_score = 0
                    
            # 2. Direction balance (50/50 is ideal)
            long_pct = self.portfolio_heat_map["by_direction"].get("LONG", 0)
            short_pct = self.portfolio_heat_map["by_direction"].get("SHORT", 0)
            total_dir = long_pct + short_pct
            
            if total_dir > 0:
                balance = min(long_pct, short_pct) / total_dir
                direction_score = balance * 100
            else:
                direction_score = 0
                
            # 3. Correlation score (lower average correlation = better)
            if len(self.open_positions) > 1 and self.correlation_matrix:
                correlations = []
                instruments = [p["instrument"] for p in self.open_positions.values()]
                
                for i in range(len(instruments)):
                    for j in range(i+1, len(instruments)):
                        inst1, inst2 = instruments[i], instruments[j]
                        if inst1 in self.correlation_matrix and inst2 in self.correlation_matrix[inst1]:
                            correlations.append(abs(self.correlation_matrix[inst1][inst2]))
                
                if correlations:
                    avg_correlation = sum(correlations) / len(correlations)
                    correlation_score = (1 - avg_correlation) * 100
                else:
                    correlation_score = 50  # Neutral if no correlation data
            else:
                correlation_score = 50
                
            # 4. Timeframe diversity
            timeframe_weights = list(self.portfolio_heat_map["by_timeframe"].values())
            timeframe_weights = [w for w in timeframe_weights if w > 0]
            
            if not timeframe_weights:
                timeframe_score = 0
            else:
                total = sum(timeframe_weights)
                if total > 0:
                    timeframe_weights = [w/total for w in timeframe_weights]
                    entropy = -sum(w * math.log(w) for w in timeframe_weights if w > 0)
                    max_entropy = math.log(len(self.portfolio_heat_map["by_timeframe"]))
                    timeframe_score = (entropy / max_entropy) * 100 if max_entropy > 0 else 0
                else:
                    timeframe_score = 0
                    
            # Combine scores with weights
            final_score = (
                category_score * 0.4 +
                direction_score * 0.2 +
                correlation_score * 0.3 +
                timeframe_score * 0.1
            )
            
            return min(100, max(0, final_score))
            
        except Exception as e:
            logging.error(f"Error calculating portfolio diversity score: {e}")
            return 50  # Neutral fallback

    def get_risk_profile(self) -> Dict[str, Any]:
        """
        Get the current risk profile of the portfolio with enhanced metrics.
        
        Returns:
            Dict with comprehensive risk metrics
        """
        # Get base risk profile
        base_profile = {
            "current_portfolio_risk": self.portfolio_heat_map["total"] * 100,  # Convert to percentage
            "max_portfolio_risk": self.max_portfolio_heat * 100,              # Convert to percentage
            "risk_utilization": (self.portfolio_heat_map["total"] / self.max_portfolio_heat) * 100 if self.max_portfolio_heat > 0 else 0,
            "open_positions": len(self.open_positions),
            "current_drawdown": sum(self.drawdown_history) * 100,             # Convert to percentage
            "max_drawdown": self.max_drawdown * 100,                          # Convert to percentage
            "risk_per_trade": self.base_risk_per_trade * 100,                 # Convert to percentage
            "win_rate": self._calculate_win_rate()
        }
        
        # Add enhanced metrics
        enhanced_metrics = {
            "diversity_score": self.get_portfolio_diversity_score(),
            "direction_exposure": {
                "long": self.portfolio_heat_map["by_direction"].get("LONG", 0) * 100,
                "short": self.portfolio_heat_map["by_direction"].get("SHORT", 0) * 100,
                "long_to_short_ratio": self._calculate_direction_ratio()
            },
            "category_exposure": {k: v * 100 for k, v in self.portfolio_heat_map["by_category"].items()},
            "position_concentration": self._calculate_position_concentration(),
            "heat_status": self.get_heat_levels()["heat_status"],
            "heat_trend": self.get_heat_trends()["trend"]
        }
        
        # Calculate performance metrics if we have trade history
        performance_metrics = {}
        if self.trade_history:
            recent_trades = self.trade_history[-min(50, len(self.trade_history)):]
            
            wins = [t for t in recent_trades if t.get("win", False)]
            losses = [t for t in recent_trades if not t.get("win", False)]
            
            if wins:
                avg_win_pct = sum(t.get("pnl_percentage", 0) for t in wins) / len(wins)
            else:
                avg_win_pct = 0
                
            if losses:
                avg_loss_pct = sum(abs(t.get("pnl_percentage", 0)) for t in losses) / len(losses)
            else:
                avg_loss_pct = 0
                
            win_rate = len(wins) / len(recent_trades) if recent_trades else 0
            
            # Calculate expectancy
            if avg_loss_pct > 0:
                profit_factor = (avg_win_pct * win_rate) / (avg_loss_pct * (1 - win_rate)) if (1 - win_rate) > 0 else 0
            else:
                profit_factor = 0
                
            expectancy = (win_rate * avg_win_pct) - ((1 - win_rate) * avg_loss_pct)
            
            performance_metrics = {
                "win_rate": win_rate * 100,
                "avg_win_pct": avg_win_pct,
                "avg_loss_pct": avg_loss_pct,
                "profit_factor": profit_factor,
                "expectancy": expectancy,
                "avg_trade_duration": sum(t.get("duration", 0) for t in recent_trades) / len(recent_trades) if recent_trades else 0
            }
            
        # Combine all metrics
        return {
            **base_profile,
            **enhanced_metrics,
            "performance": performance_metrics
        }
        
    def _calculate_position_concentration(self) -> float:
        """
        Calculate position concentration (0-100).
        Higher values indicate concentration in fewer positions/instruments.
        
        Returns:
            float: Concentration score
        """
        if not self.open_positions:
            return 0.0
            
        # Get instrument weights
        instrument_weights = {}
        for position in self.open_positions.values():
            instrument = position["instrument"]
            if instrument not in instrument_weights:
                instrument_weights[instrument] = 0
            instrument_weights[instrument] += position["risk_contribution"]
            
        # Calculate Herfindahl-Hirschman Index (HHI)
        weights = list(instrument_weights.values())
        total = sum(weights)
        
        if total == 0:
            return 0.0
            
        normalized_weights = [w/total for w in weights]
        hhi = sum(w*w for w in normalized_weights)
        
        # Normalize to 0-100 scale
        # HHI ranges from 1/N to 1 (where N is number of instruments)
        min_hhi = 1 / len(instrument_weights) if instrument_weights else 0
        normalized_hhi = (hhi - min_hhi) / (1 - min_hhi) if (1 - min_hhi) > 0 else 0
        
        return normalized_hhi * 100

class LorentzianDistanceClassifier:
    """
    Enhanced classifier that uses Lorentzian distance metrics to identify market conditions
    and trading opportunities. This implementation includes more robust feature extraction
    and better handling of different market phases.
    
    The classifier works by:
    1. Calculating Lorentzian distances between price points
    2. Extracting momentum and volatility features
    3. Using these to classify market conditions
    4. Generating trading signals based on the classification
    """
    def __init__(self, lookback_window=50, signal_threshold=0.75, feature_count=5):
        self.lookback_window = lookback_window
        self.signal_threshold = signal_threshold
        self.feature_count = feature_count
        self.price_history = []
        self.volume_history = []
        self.feature_matrix = []
        self.signals = []
        self.current_classification = "NEUTRAL"
        self.confidence_score = 0.0
        self.last_update_time = datetime.now()
        
    def add_data_point(self, price: float, volume: Optional[float] = None, 
                      additional_metrics: Optional[Dict[str, float]] = None) -> None:
        """
        Add a new data point to the classifier.
        
        Args:
            price: The current price
            volume: Optional volume data
            additional_metrics: Optional dictionary of additional features
        """
        try:
            self.price_history.append(price)
            if volume is not None:
                self.volume_history.append(volume)
            else:
                # Use placeholder if no volume data available
                self.volume_history.append(1.0)
                
            # Keep history within lookback window
            if len(self.price_history) > self.lookback_window:
                self.price_history.pop(0)
                self.volume_history.pop(0)
                
            # Only calculate features when we have enough data
            if len(self.price_history) >= 10:
                self._extract_features(additional_metrics)
                self._classify_market_condition()
                
            self.last_update_time = datetime.now()
        except Exception as e:
            logging.error(f"Error adding data to Lorentzian Distance Classifier: {e}")
            
    def _extract_features(self, additional_metrics: Optional[Dict[str, float]] = None) -> None:
        """
        Extract relevant features for classification from price and volume data.
        """
        try:
            if len(self.price_history) < 10:
                return
                
            # Basic features
            features = []
            
            # 1. Lorentzian distance features at multiple lags
            price_array = np.array(self.price_history)
            for lag in [1, 2, 3, 5, 8, 13]:
                if len(price_array) > lag:
                    # Calculate log(1 + |price_t - price_{t-lag}|)
                    diff = np.abs(price_array[lag:] - price_array[:-lag])
                    lorentzian_dist = np.log1p(diff).mean()
                    features.append(lorentzian_dist)
            
            # 2. Momentum features
            if len(price_array) >= 10:
                # Rate of change
                roc_5 = (price_array[-1] / price_array[-6] - 1) if price_array[-6] != 0 else 0
                roc_10 = (price_array[-1] / price_array[-11] - 1) if price_array[-11] != 0 else 0
                features.append(roc_5)
                features.append(roc_10)
                
                # Acceleration (change in momentum)
                momentum_change = roc_5 - roc_10
                features.append(momentum_change)
            
            # 3. Volatility features
            if len(price_array) >= 20:
                # Standard deviation of returns
                returns = np.diff(price_array) / price_array[:-1]
                vol_5 = np.std(returns[-5:]) if len(returns) >= 5 else 0
                vol_20 = np.std(returns) if len(returns) >= 20 else 0
                features.append(vol_5)
                features.append(vol_5 / vol_20 if vol_20 != 0 else 1.0)  # Volatility ratio
            
            # 4. Volume-weighted features (if volume data available)
            if len(self.volume_history) >= 10:
                volume_array = np.array(self.volume_history)
                # Volume momentum
                vol_change = (volume_array[-1] / volume_array[-10:].mean()) if volume_array[-10:].mean() != 0 else 1.0
                features.append(vol_change)
                
                # Volume-weighted price momentum
                if len(price_array) >= 10:
                    vwp_change = np.sum(np.diff(price_array[-10:]) * volume_array[-10:]) / np.sum(volume_array[-10:]) if np.sum(volume_array[-10:]) != 0 else 0
                    features.append(vwp_change)
                    
            # 5. Add any additional metrics provided
            if additional_metrics:
                for _, value in additional_metrics.items():
                    features.append(value)
                    
            # Ensure feature vector is consistent length by padding if necessary
            while len(features) < self.feature_count:
                features.append(0.0)
                
            # Or truncating if we have too many
            features = features[:self.feature_count]
                
            self.feature_matrix.append(features)
            if len(self.feature_matrix) > self.lookback_window:
                self.feature_matrix.pop(0)
                
        except Exception as e:
            logging.error(f"Error extracting features in Lorentzian Distance Classifier: {e}")
            
    def _calculate_distance_matrix(self) -> np.ndarray:
        """
        Calculate the matrix of Lorentzian distances between all pairs of feature vectors.
        """
        try:
            if not self.feature_matrix or len(self.feature_matrix) < 2:
                return np.array([[0.0]])
                
            n_samples = len(self.feature_matrix)
            dist_matrix = np.zeros((n_samples, n_samples))
            
            # Calculate distances between all pairs of feature vectors
            for i in range(n_samples):
                for j in range(i, n_samples):
                    # Lorentzian distance metric
                    vec1 = np.array(self.feature_matrix[i])
                    vec2 = np.array(self.feature_matrix[j])
                    distance = np.sum(np.log1p(np.abs(vec1 - vec2)))
                    
                    dist_matrix[i, j] = distance
                    dist_matrix[j, i] = distance  # Symmetric
                    
            return dist_matrix
        except Exception as e:
            logging.error(f"Error calculating distance matrix: {e}")
            return np.array([[0.0]])
            
    def _classify_market_condition(self) -> None:
        """
        Classify the current market condition based on extracted features.
        """
        try:
            if not self.feature_matrix or len(self.feature_matrix) < 5:
                self.current_classification = "INSUFFICIENT_DATA"
                self.confidence_score = 0.0
                return
                
            # Get the most recent feature vector
            current_features = np.array(self.feature_matrix[-1])
            
            # Simple threshold-based classification logic
            momentum_feature_idx = min(2, len(current_features) - 1)
            volatility_feature_idx = min(3, len(current_features) - 1)
            
            momentum = current_features[momentum_feature_idx]
            volatility = current_features[volatility_feature_idx]
            
            # Calculate a trending score and a volatility score
            trending_score = abs(momentum) / (volatility + 0.001)
            
            # Classification logic
            if trending_score > 2.0:
                # Strong trend
                if momentum > 0:
                    self.current_classification = "STRONG_UPTREND"
                    self.confidence_score = min(0.9, trending_score / 5.0)
                else:
                    self.current_classification = "STRONG_DOWNTREND"
                    self.confidence_score = min(0.9, trending_score / 5.0)
            elif trending_score > 1.0:
                # Moderate trend
                if momentum > 0:
                    self.current_classification = "UPTREND"
                    self.confidence_score = min(0.7, trending_score / 4.0)
                else:
                    self.current_classification = "DOWNTREND"
                    self.confidence_score = min(0.7, trending_score / 4.0)
            elif volatility > 0.02:
                # Volatile/choppy market
                self.current_classification = "VOLATILE"
                self.confidence_score = min(0.8, volatility * 20)
            else:
                # Ranging market
                self.current_classification = "RANGING"
                self.confidence_score = min(0.6, (0.03 - volatility) * 30)
                
            # Generate signal based on classification
            self._generate_signal()
            
        except Exception as e:
            logging.error(f"Error classifying market condition: {e}")
            self.current_classification = "ERROR"
            self.confidence_score = 0.0
            
    def _generate_signal(self) -> None:
        """
        Generate trading signals based on the current classification.
        """
        try:
            if self.current_classification in ["INSUFFICIENT_DATA", "ERROR"]:
                self.signals.append({"type": "NEUTRAL", "strength": 0.0, "timestamp": datetime.now()})
                return
                
            signal_type = "NEUTRAL"
            signal_strength = 0.0
            
            # Signal generation logic based on market classification
            if self.current_classification == "STRONG_UPTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "BUY"
                signal_strength = self.confidence_score
            elif self.current_classification == "STRONG_DOWNTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "SELL"
                signal_strength = self.confidence_score
            elif self.current_classification == "UPTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "WEAK_BUY"
                signal_strength = self.confidence_score
            elif self.current_classification == "DOWNTREND" and self.confidence_score > self.signal_threshold:
                signal_type = "WEAK_SELL"
                signal_strength = self.confidence_score
            elif self.current_classification == "VOLATILE":
                signal_type = "NEUTRAL"  # Avoid trading in highly volatile conditions
                signal_strength = 0.3
            elif self.current_classification == "RANGING":
                # In ranging markets, consider mean-reversion strategies instead
                recent_momentum = self.feature_matrix[-1][2] if len(self.feature_matrix) > 0 and len(self.feature_matrix[-1]) > 2 else 0
                if recent_momentum > 0.01:
                    signal_type = "RANGE_SELL"  # Approaching upper range, potential sell
                    signal_strength = min(0.6, abs(recent_momentum) * 20)
                elif recent_momentum < -0.01:
                    signal_type = "RANGE_BUY"  # Approaching lower range, potential buy
                    signal_strength = min(0.6, abs(recent_momentum) * 20)
                    
            # Store the signal
            signal = {
                "type": signal_type,
                "strength": signal_strength,
                "classification": self.current_classification,
                "confidence": self.confidence_score,
                "timestamp": datetime.now()
            }
            
            self.signals.append(signal)
            if len(self.signals) > self.lookback_window:
                self.signals.pop(0)
                
        except Exception as e:
            logging.error(f"Error generating signal in Lorentzian Distance Classifier: {e}")
            
    def get_current_classification(self) -> Dict[str, Any]:
        """
        Get the current market classification and confidence.
        
        Returns:
            Dict with classification details
        """
        return {
            "classification": self.current_classification,
            "confidence": self.confidence_score,
            "last_update": self.last_update_time.isoformat()
        }
        
    def get_latest_signal(self) -> Dict[str, Any]:
        """
        Get the latest trading signal.
        
        Returns:
            Dict with signal details or empty dict if no signals
        """
        if not self.signals:
            return {}
        return self.signals[-1]
        
    def get_signal_distribution(self, lookback: int = 10) -> Dict[str, float]:
        """
        Get the distribution of recent signals to identify persistent patterns.
        
        Args:
            lookback: Number of recent signals to consider
            
        Returns:
            Dict mapping signal types to their frequency
        """
        if not self.signals:
            return {"NEUTRAL": 1.0}
            
        recent_signals = self.signals[-min(lookback, len(self.signals)):]
        signal_counts = {}
        
        for signal in recent_signals:
            signal_type = signal["type"]
            if signal_type not in signal_counts:
                signal_counts[signal_type] = 0
            signal_counts[signal_type] += 1
            
        # Convert to frequencies
        total = len(recent_signals)
        return {k: v/total for k, v in signal_counts.items()}
        
    def get_trend_strength(self) -> float:
        """
        Get the current trend strength as a normalized value between -1 and 1.
        Negative values indicate downtrend, positive values indicate uptrend.
        
        Returns:
            float: Trend strength indicator
        """
        if self.current_classification == "STRONG_UPTREND":
            return self.confidence_score
        elif self.current_classification == "UPTREND":
            return self.confidence_score * 0.7
        elif self.current_classification == "STRONG_DOWNTREND":
            return -self.confidence_score
        elif self.current_classification == "DOWNTREND":
            return -self.confidence_score * 0.7
        elif self.current_classification == "RANGING":
            # Slight bias based on recent momentum
            if len(self.feature_matrix) > 0 and len(self.feature_matrix[-1]) > 2:
                recent_momentum = self.feature_matrix[-1][2]
                return recent_momentum * 0.3  # Reduced strength in ranging markets
        
        return 0.0  # Neutral or insufficient data

class AdvancedPositionSizer:
    """
    Advanced position sizing calculator that incorporates multiple factors:
    - Account balance and risk percentage
    - Market volatility
    - Market regime
    - Trade timeframe
    - Portfolio heat
    
    This class helps determine optimal position size based on multiple risk factors.
    """
    def __init__(self, 
                max_risk_per_trade=0.02,     # Default 2% per trade
                max_portfolio_risk=0.06,     # Default 6% total portfolio risk
                apply_kelly_criterion=True,  # Whether to apply Kelly criterion
                kelly_fraction=0.5,          # Conservative Kelly (half-Kelly)
                use_atr_for_stops=True,      # Use ATR for stop loss calculation
                position_size_rounding=True, # Round position sizes to standard lots
                limit_risk_per_instrument=True, # Limit risk per instrument category
                adaptive_sizing=True         # Adjust size based on win rate and expectancy
                ):
        self.max_risk_per_trade = max_risk_per_trade  # Default 2% per trade
        self.max_portfolio_risk = max_portfolio_risk  # Default 6% total portfolio risk
        self.open_positions_risk = 0.0  # Track current portfolio heat
        self.recent_trades = []  # Track recent performance for adaptive sizing
        self.winning_streak = 0
        self.losing_streak = 0
        self.apply_kelly_criterion = apply_kelly_criterion
        self.kelly_fraction = kelly_fraction
        self.use_atr_for_stops = use_atr_for_stops
        self.position_size_rounding = position_size_rounding
        self.limit_risk_per_instrument = limit_risk_per_instrument
        self.adaptive_sizing = adaptive_sizing
        
        # Standard lot sizes for different instrument types
        self.standard_lot_sizes = {
            "FOREX": 100000,
            "CRYPTO": 1,
            "INDICES": 1,
            "METALS": 100,
            "ENERGY": 1000,
            "STOCKS": 100
        }
        
        # Risk multipliers for different instrument types (based on volatility)
        self.instrument_risk_multipliers = INSTRUMENT_MULTIPLIERS
        
        # Timeframe-specific adjustments
        self.timeframe_risk_multipliers = {
            "M1": 0.7,   # Reduce risk for very short timeframes
            "M5": 0.85,
            "M15": 0.9,
            "M30": 0.95,
            "H1": 1.0,   # Base reference
            "H4": 1.1,
            "D1": 1.2,
            "W1": 1.3    # Higher risk allowed for longer timeframes
        }
        
        # Performance tracking
        self.performance_stats = {
            "win_rate": 0.5,  # Default 50% win rate
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "expectancy": 0.0,
            "trades_analyzed": 0
        }
        
    def calculate_position_size(self, 
                               account_balance: float, 
                               risk_percentage: float, 
                               entry_price: float, 
                               stop_loss: float,
                               volatility_state: Dict[str, Any] = None,
                               market_regime: Tuple[str, float] = None,
                               risk_manager: Any = None,
                               timeframe: str = "H1",
                               instrument: str = "",
                               instrument_type: str = "FOREX") -> Dict[str, Any]:
        """
        Calculate the optimal position size based on account risk management and market conditions.
        
        Args:
            account_balance: Current account balance
            risk_percentage: Base risk percentage for this trade (e.g., 1% = 0.01)
            entry_price: Entry price of the trade
            stop_loss: Stop loss price
            volatility_state: Output from VolatilityMonitor.get_volatility_state()
            market_regime: Output from LorentzianClassifier.get_regime()
            risk_manager: VolatilityBasedRiskManager instance for portfolio heat check
            timeframe: Trade timeframe (e.g., "M5", "H1", "D1")
            instrument: The trading instrument symbol
            instrument_type: Type of instrument (e.g., "FOREX", "CRYPTO")
            
        Returns:
            Dict with position size details and risk metrics
        """
        try:
            # Calculate risk amount in account currency
            base_risk_percentage = min(risk_percentage, self.max_risk_per_trade)
            base_risk_amount = account_balance * base_risk_percentage
            
            # Calculate base position size
            if entry_price == 0 or stop_loss == 0:
                logging.error("Invalid entry or stop loss price (zero)")
                return {"units": 0, "error": "Invalid price"}
                
            # Risk per pip/point
            risk_per_unit = abs(entry_price - stop_loss)
            if risk_per_unit == 0:
                logging.error("Stop loss cannot be equal to entry price")
                return {"units": 0, "error": "Invalid stop distance"}
                
            # Apply Kelly criterion if enabled
            kelly_multiplier = 1.0
            if self.apply_kelly_criterion and self.performance_stats["trades_analyzed"] > 10:
                kelly_multiplier = self._calculate_kelly_fraction()
                
            # Apply volatility adjustment
            volatility_multiplier = 1.0
            if volatility_state and 'risk_adjustment' in volatility_state:
                volatility_multiplier = volatility_state['risk_adjustment']
                
            # Apply market regime adjustment
            regime_multiplier = 1.0
            if market_regime:
                regime, confidence = market_regime
                # Get adjustment based on regime
                if regime == "VOLATILE":
                    regime_multiplier = max(0.5, 1.0 - (confidence * 0.5))
                elif regime in ["TRENDING_UP", "TRENDING_DOWN"]:
                    regime_multiplier = min(1.3, 1.0 + (confidence * 0.3))
                    
            # Apply timeframe adjustment
            timeframe_multiplier = self.timeframe_risk_multipliers.get(timeframe, 1.0)
                
            # Apply instrument type adjustment
            instrument_multiplier = self.instrument_risk_multipliers.get(instrument_type, 1.0)
            
            # Apply adaptive sizing based on win/loss streaks if enabled
            streak_multiplier = 1.0
            if self.adaptive_sizing:
                streak_multiplier = self._calculate_streak_multiplier()
                
            # Combine all multipliers
            combined_multiplier = (
                kelly_multiplier *
                volatility_multiplier *
                regime_multiplier *
                timeframe_multiplier *
                instrument_multiplier *
                streak_multiplier
            )
            
            # Adjust risk amount
            adjusted_risk_amount = base_risk_amount * combined_multiplier
            
            # Check portfolio heat if risk manager provided
            if risk_manager:
                # Get current portfolio heat
                heat_levels = risk_manager.get_heat_levels()
                current_heat = heat_levels["total"] / 100.0  # Convert from percentage
                
                # Calculate available risk capacity
                available_capacity = risk_manager.max_portfolio_heat - current_heat
                
                # If current trade would exceed capacity, scale it down
                if current_heat + base_risk_percentage > risk_manager.max_portfolio_heat:
                    heat_scaling = max(0.1, available_capacity / base_risk_percentage)
                    adjusted_risk_amount = adjusted_risk_amount * heat_scaling
                    
                # Also check instrument-specific limits if enabled
                if self.limit_risk_per_instrument:
                    if instrument_type in heat_levels["by_category"]:
                        category_heat = heat_levels["by_category"][instrument_type] / 100.0
                        category_limit = risk_manager.max_instrument_category_heat
                        
                        if category_heat + base_risk_percentage > category_limit:
                            category_scaling = max(0.1, (category_limit - category_heat) / base_risk_percentage)
                            adjusted_risk_amount = adjusted_risk_amount * category_scaling
            
            # Calculate adjusted position size
            position_size = adjusted_risk_amount / risk_per_unit
            
            # Round to standard lot sizes if enabled
            if self.position_size_rounding:
                standard_lot = self.standard_lot_sizes.get(instrument_type, 1)
                position_size = self._round_to_standard_size(position_size, standard_lot)
            
            # Create detailed response
            result = {
                "units": position_size,
                "risk_amount": adjusted_risk_amount,
                "risk_percentage": base_risk_percentage * 100,  # Convert to percentage
                "adjusted_risk_percentage": (adjusted_risk_amount / account_balance) * 100,
                "stop_distance": risk_per_unit,
                "stop_distance_pips": self._convert_to_pips(risk_per_unit, instrument_type),
                "multipliers": {
                    "kelly": kelly_multiplier,
                    "volatility": volatility_multiplier,
                    "regime": regime_multiplier,
                    "timeframe": timeframe_multiplier,
                    "instrument": instrument_multiplier,
                    "streak": streak_multiplier,
                    "combined": combined_multiplier
                },
                "account_balance": account_balance
            }
            
            # Add portfolio heat info if available
            if risk_manager:
                result["portfolio_heat"] = {
                    "current": current_heat * 100,  # As percentage
                    "after_trade": (current_heat + (adjusted_risk_amount / account_balance)) * 100,
                    "limit": risk_manager.max_portfolio_heat * 100
                }
                
            return result
            
        except Exception as e:
            logging.error(f"Error calculating position size: {e}")
            return {"units": 0, "error": str(e)}
            
    def _convert_to_pips(self, price_distance: float, instrument_type: str) -> float:
        """Convert price distance to pips based on instrument type"""
        if instrument_type == "FOREX":
            # For most forex pairs, 0.0001 = 1 pip
            return price_distance * 10000
        elif instrument_type == "FOREX_JPY":
            # For JPY pairs, 0.01 = 1 pip
            return price_distance * 100
        elif instrument_type == "CRYPTO":
            # For crypto, use price points
            return price_distance
        else:
            # For other instruments, use price points
            return price_distance
            
    def _round_to_standard_size(self, position_size: float, standard_lot: float) -> float:
        """Round position size to standard lot sizes"""
        # Calculate how many standard lots this represents
        lots = position_size / standard_lot
        
        # Round to 2 decimal places for mini lots
        rounded_lots = round(lots, 2)
        
        # Ensure minimum size of 0.01 lots if non-zero
        if rounded_lots > 0 and rounded_lots < 0.01:
            rounded_lots = 0.01
            
        return rounded_lots * standard_lot
        
    def _calculate_kelly_fraction(self) -> float:
        """
        Calculate Kelly criterion fraction for optimal position sizing.
        Uses the conservative Kelly approach (half-Kelly) to reduce risk.
        
        Returns:
            float: Kelly multiplier to apply to position size
        """
        win_rate = self.performance_stats["win_rate"]
        avg_win = self.performance_stats["avg_win_pct"]
        avg_loss = self.performance_stats["avg_loss_pct"]
        
        # Avoid division by zero
        if avg_loss == 0 or win_rate == 0 or win_rate == 1:
            return 1.0
            
        # Calculate Kelly fraction: f* = (p/q) * (b/a) - (q/p)
        # where p = win rate, q = 1-p, b = avg win, a = avg loss
        q = 1 - win_rate
        kelly = (win_rate / q) * (avg_win / avg_loss) - (q / win_rate)
        
        # Apply conservative Kelly (half-Kelly) and cap it
        kelly = kelly * self.kelly_fraction
        
        # Ensure sensible bounds (0.1 to 2.0)
        kelly = min(2.0, max(0.1, kelly))
        
        return kelly
        
    def _calculate_streak_multiplier(self) -> float:
        """
        Calculate a multiplier based on current winning or losing streak.
        This implements an adaptive anti-martingale on winning streaks
        and position size reduction on losing streaks.
        
        Returns:
            float: Streak-based multiplier
        """
        if self.winning_streak >= 3:
            # Gradually increase position size on winning streaks (anti-martingale)
            return min(1.5, 1.0 + (self.winning_streak * 0.1))
        elif self.losing_streak >= 2:
            # Gradually decrease position size on losing streaks
            return max(0.5, 1.0 - (self.losing_streak * 0.1))
        else:
            # No significant streak
            return 1.0
            
    def update_performance_stats(self, 
                                win: bool, 
                                profit_loss_percentage: float, 
                                risk_percentage: float) -> None:
        """
        Update trading performance statistics for adaptive position sizing.
        
        Args:
            win: Whether the trade was a winner
            profit_loss_percentage: Profit/loss as percentage
            risk_percentage: Risk taken as percentage
        """
        try:
            # Update streaks
            if win:
                self.winning_streak += 1
                self.losing_streak = 0
            else:
                self.losing_streak += 1
                self.winning_streak = 0
                
            # Add to recent trades history
            self.recent_trades.append({
                "win": win,
                "pnl_percentage": profit_loss_percentage,
                "risk_percentage": risk_percentage,
                "timestamp": datetime.now()
            })
            
            # Keep recent trades list at reasonable size
            if len(self.recent_trades) > 50:
                self.recent_trades.pop(0)
                
            # Recalculate performance stats
            if len(self.recent_trades) > 0:
                wins = [t for t in self.recent_trades if t["win"]]
                losses = [t for t in self.recent_trades if not t["win"]]
                
                # Update win rate
                self.performance_stats["win_rate"] = len(wins) / len(self.recent_trades)
                
                # Update average win and loss percentages
                if wins:
                    self.performance_stats["avg_win_pct"] = sum(t["pnl_percentage"] for t in wins) / len(wins)
                if losses:
                    self.performance_stats["avg_loss_pct"] = sum(abs(t["pnl_percentage"]) for t in losses) / len(losses)
                    
                # Calculate expectancy
                win_contribution = self.performance_stats["win_rate"] * self.performance_stats["avg_win_pct"]
                loss_contribution = (1 - self.performance_stats["win_rate"]) * self.performance_stats["avg_loss_pct"]
                self.performance_stats["expectancy"] = win_contribution - loss_contribution
                
                # Update trades analyzed count
                self.performance_stats["trades_analyzed"] = len(self.recent_trades)
                
        except Exception as e:
            logging.error(f"Error updating performance stats: {e}")
            
    def register_open_position(self, position_risk: float) -> None:
        """
        Register an open position to track portfolio heat.
        
        Args:
            position_risk: Risk percentage of the position
        """
        self.open_positions_risk += position_risk
        
    def remove_closed_position(self, position_risk: float) -> None:
        """
        Remove a closed position from portfolio heat tracking.
        
        Args:
            position_risk: Risk percentage of the position
        """
        self.open_positions_risk = max(0, self.open_positions_risk - position_risk)
        
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get current performance statistics.
        
        Returns:
            Dict with performance metrics
        """
        return {
            **self.performance_stats,
            "winning_streak": self.winning_streak,
            "losing_streak": self.losing_streak,
            "recent_trades_count": len(self.recent_trades),
            "current_portfolio_risk": self.open_positions_risk * 100,  # As percentage
            "max_portfolio_risk": self.max_portfolio_risk * 100        # As percentage
        }
        
    def suggest_stop_loss(self, 
                         entry_price: float, 
                         direction: str, 
                         atr_value: float = None,
                         instrument_type: str = "FOREX",
                         timeframe: str = "H1") -> float:
        """
        Suggest an optimal stop loss level based on ATR or default percentages.
        
        Args:
            entry_price: Entry price
            direction: "BUY" or "SELL"
            atr_value: ATR value if available
            instrument_type: Type of instrument
            timeframe: Trading timeframe
            
        Returns:
            float: Suggested stop loss price
        """
        try:
            # Default percentages by instrument type
            default_percentages = {
                "FOREX": 0.01,      # 1% for forex
                "FOREX_JPY": 0.01,  # 1% for JPY pairs
                "CRYPTO": 0.05,     # 5% for crypto
                "INDICES": 0.02,    # 2% for indices
                "METALS": 0.015,    # 1.5% for metals
                "ENERGY": 0.025,    # 2.5% for energy
                "STOCKS": 0.03      # 3% for stocks
            }
            
            # ATR multipliers by timeframe
            atr_multipliers = {
                "M1": 1.5,
                "M5": 1.5,
                "M15": 1.4,
                "M30": 1.3,
                "H1": 1.2,
                "H4": 1.1,
                "D1": 1.0,
                "W1": 0.9
            }
            
            if self.use_atr_for_stops and atr_value is not None and atr_value > 0:
                # Use ATR-based stop loss
                multiplier = atr_multipliers.get(timeframe, 1.2)
                stop_distance = atr_value * multiplier
            else:
                # Use percentage-based stop loss
                percentage = default_percentages.get(instrument_type, 0.02)
                stop_distance = entry_price * percentage
                
            # Calculate stop loss price based on direction
            if direction.upper() in ["BUY", "LONG"]:
                stop_loss = entry_price - stop_distance
            else:
                stop_loss = entry_price + stop_distance
                
            return stop_loss
            
        except Exception as e:
            logging.error(f"Error suggesting stop loss: {e}")
            # Fallback to simple percentage
            if direction.upper() in ["BUY", "LONG"]:
                return entry_price * 0.98  # 2% below entry
            else:
                return entry_price * 1.02  # 2% above entry
                
    def suggest_take_profit_levels(self, 
                                  entry_price: float, 
                                  stop_loss: float, 
                                  direction: str,
                                  timeframe: str = "H1") -> Dict[str, float]:
        """
        Suggest multi-level take profit targets based on timeframe and risk-reward.
        
        Args:
            entry_price: Entry price
            stop_loss: Stop loss price
            direction: "BUY" or "SELL"
            timeframe: Trading timeframe
            
        Returns:
            Dict with take profit levels
        """
        try:
            # Get risk distance
            risk_distance = abs(entry_price - stop_loss)
            
            # Get timeframe-specific take profit levels or use defaults
            if timeframe in TIMEFRAME_TAKE_PROFIT_LEVELS:
                tp_levels = TIMEFRAME_TAKE_PROFIT_LEVELS[timeframe]
            else:
                # Default values if timeframe not found
                tp_levels = {
                    'first_exit': 0.3,
                    'second_exit': 0.3,
                    'runner': 0.4
                }
                
            # Calculate take profit prices
            is_buy = direction.upper() in ["BUY", "LONG"]
            
            tp1_distance = risk_distance * 1.5  # 1.5R for first target
            tp2_distance = risk_distance * 2.5  # 2.5R for second target
            tp3_distance = risk_distance * 4.0  # 4.0R for runner
            
            if is_buy:
                tp1 = entry_price + tp1_distance
                tp2 = entry_price + tp2_distance
                tp3 = entry_price + tp3_distance
            else:
                tp1 = entry_price - tp1_distance
                tp2 = entry_price - tp2_distance
                tp3 = entry_price - tp3_distance
                
            return {
                "tp1": {
                    "price": tp1,
                    "size_percentage": tp_levels['first_exit'] * 100,
                    "r_multiple": 1.5
                },
                "tp2": {
                    "price": tp2,
                    "size_percentage": tp_levels['second_exit'] * 100,
                    "r_multiple": 2.5
                },
                "tp3": {
                    "price": tp3,
                    "size_percentage": tp_levels['runner'] * 100,
                    "r_multiple": 4.0
                }
            }
            
        except Exception as e:
            logging.error(f"Error suggesting take profit levels: {e}")
            # Fallback to simple 1:2 risk-reward
            risk_distance = abs(entry_price - stop_loss)
            if direction.upper() in ["BUY", "LONG"]:
                return {
                    "tp1": {"price": entry_price + risk_distance * 2, "size_percentage": 100, "r_multiple": 2.0}
                }
            else:
                return {
                    "tp1": {"price": entry_price - risk_distance * 2, "size_percentage": 100, "r_multiple": 2.0}
                }

class MultiStageTakeProfitManager:
    """
    Manages a sophisticated take-profit strategy with multiple exit stages.
    
    This class handles:
    - Multiple take-profit levels with partial position closure at each level
    - Timeframe-specific take-profit distribution (different for scalping vs swing)
    - Trailing stops that activate after certain profit thresholds
    - Dynamic adjustment based on volatility and market conditions
    """
    def __init__(self, 
                timeframe: str = "H1",
                atr_multiplier: float = 1.0,
                use_dynamic_targets: bool = True,
                use_trailing_stops: bool = True,
                trailing_activation_threshold: float = None,
                market_volatility: float = None):
        """
        Initialize the multi-stage take profit manager.
        
        Args:
            timeframe: Trading timeframe (e.g., "M5", "H1", "D1")
            atr_multiplier: Multiplier for ATR when calculating targets
            use_dynamic_targets: Whether to adjust targets based on volatility
            use_trailing_stops: Whether to use trailing stops after certain threshold
            trailing_activation_threshold: R-multiple at which to activate trailing
            market_volatility: Current market volatility level (0-100)
        """
        self.timeframe = timeframe
        self.atr_multiplier = atr_multiplier
        self.use_dynamic_targets = use_dynamic_targets
        self.use_trailing_stops = use_trailing_stops
        self.market_volatility = market_volatility or 50  # Default to medium volatility
        
        # Set default trailing activation threshold based on timeframe if not provided
        if trailing_activation_threshold is None and timeframe in TIMEFRAME_RISK_SETTINGS:
            self.trailing_activation_threshold = TIMEFRAME_RISK_SETTINGS[timeframe]["trailing_stop_activation"]
        else:
            self.trailing_activation_threshold = trailing_activation_threshold or 1.5
        
        # Initialize take profit stages
        self.take_profit_stages = []
        self.entry_price = None
        self.stop_loss = None
        self.position_direction = None
        self.total_units = 0
        self.original_units = 0
        self.initial_risk = 0
        self.profit_locked_in = 0
        self.trailing_stop_level = None
        self.atr_value = None
        
        # Track which stages have been hit
        self.stages_hit = []
        
        # Get exit percentages based on timeframe
        if timeframe in TIMEFRAME_TAKE_PROFIT_LEVELS:
            self.exit_percentages = TIMEFRAME_TAKE_PROFIT_LEVELS[timeframe]
        else:
            # Default to H1 if timeframe not found
            self.exit_percentages = TIMEFRAME_TAKE_PROFIT_LEVELS["H1"]
            
        # Adjusted take profit levels based on volatility
        self.adjusted_exit_percentages = self._adjust_for_volatility(self.exit_percentages)
        
    def _adjust_for_volatility(self, base_percentages: Dict[str, float]) -> Dict[str, float]:
        """
        Adjust take profit distribution based on market volatility.
        
        Args:
            base_percentages: Base take profit distribution percentages
            
        Returns:
            Adjusted take profit distribution
        """
        if not self.use_dynamic_targets or self.market_volatility is None:
            return base_percentages
            
        # Higher volatility = close more at first targets
        if self.market_volatility > 75:  # Very high volatility
            return {
                "first_exit": min(0.6, base_percentages["first_exit"] + 0.15),
                "second_exit": min(0.5, base_percentages["second_exit"] + 0.1),
                "runner": max(0.05, 1.0 - (base_percentages["first_exit"] + 0.15) - (base_percentages["second_exit"] + 0.1))
            }
        elif self.market_volatility > 60:  # High volatility
            return {
                "first_exit": min(0.5, base_percentages["first_exit"] + 0.1),
                "second_exit": min(0.5, base_percentages["second_exit"] + 0.05),
                "runner": max(0.05, 1.0 - (base_percentages["first_exit"] + 0.1) - (base_percentages["second_exit"] + 0.05))
            }
        elif self.market_volatility < 30:  # Low volatility
            return {
                "first_exit": max(0.1, base_percentages["first_exit"] - 0.1),
                "second_exit": max(0.1, base_percentages["second_exit"] - 0.05),
                "runner": min(0.8, 1.0 - (base_percentages["first_exit"] - 0.1) - (base_percentages["second_exit"] - 0.05))
            }
        # For medium volatility, return base percentages
        return base_percentages
        
    def _calculate_take_profit_levels(self, 
                                    entry_price: float, 
                                    stop_loss: float, 
                                    position_direction: str,
                                    atr_value: float = None) -> Dict[int, float]:
        """
        Calculate multiple take profit levels based on risk-reward and ATR.
        """
        risk_distance = abs(entry_price - stop_loss)
        
        # If ATR provided, use it to potentially adjust the targets
        if atr_value and self.atr_multiplier:
            # If risk distance is too small compared to ATR, increase it
            if risk_distance < atr_value * 0.5:
                risk_distance = max(risk_distance, atr_value * 0.5)
            # If risk distance is too large compared to ATR, cap it
            elif risk_distance > atr_value * 2.5:
                risk_distance = min(risk_distance, atr_value * 2.5)
        
        # Calculate take profit levels with increasing RR values
        if position_direction == "BUY":
            tp1 = entry_price + (risk_distance * 1.5)  # 1.5R
            tp2 = entry_price + (risk_distance * 2.5)  # 2.5R
            tp3 = entry_price + (risk_distance * 4.0)  # 4.0R
        else:  # SELL
            tp1 = entry_price - (risk_distance * 1.5)  # 1.5R
            tp2 = entry_price - (risk_distance * 2.5)  # 2.5R
            tp3 = entry_price - (risk_distance * 4.0)  # 4.0R
            
        return {1: tp1, 2: tp2, 3: tp3}
        
    def initialize_take_profit_stages(self, 
                                   entry_price: float, 
                                   stop_loss: float, 
                                   initial_take_profit: float,
                                   position_direction: str,
                                   total_units: float,
                                   atr_value: float = None,
                                   timeframe: str = None) -> List[Dict[str, Any]]:
        """
        Initialize the multi-stage take profit strategy for a position.
        """
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.position_direction = position_direction
        self.total_units = total_units
        self.original_units = total_units
        self.atr_value = atr_value
        
        # Update timeframe if provided
        if timeframe:
            self.timeframe = timeframe
            # Update exit percentages based on new timeframe
            if timeframe in TIMEFRAME_TAKE_PROFIT_LEVELS:
                self.exit_percentages = TIMEFRAME_TAKE_PROFIT_LEVELS[timeframe]
            else:
                # Default to H1 if timeframe not found
                self.exit_percentages = TIMEFRAME_TAKE_PROFIT_LEVELS["H1"]
            
            # Re-adjust for volatility
            self.adjusted_exit_percentages = self._adjust_for_volatility(self.exit_percentages)
            
            # Update trailing threshold
            if timeframe in TIMEFRAME_RISK_SETTINGS:
                self.trailing_activation_threshold = TIMEFRAME_RISK_SETTINGS[timeframe]["trailing_stop_activation"]
        
        # Calculate initial risk (used for R-multiple calculations)
        self.initial_risk = abs(entry_price - stop_loss)
        
        # Calculate take profit levels
        tp_levels = self._calculate_take_profit_levels(
            entry_price,
            stop_loss,
            position_direction,
            atr_value
        )
        
        # Override first TP level with the provided initial TP if it exists and is valid
        if initial_take_profit is not None:
            # Verify the direction makes sense
            if (position_direction == "BUY" and initial_take_profit > entry_price) or \
               (position_direction == "SELL" and initial_take_profit < entry_price):
                tp_levels[1] = initial_take_profit
        
        # Create stages based on the adjusted exit percentages
        first_exit_units = self.total_units * self.adjusted_exit_percentages["first_exit"]
        second_exit_units = self.total_units * self.adjusted_exit_percentages["second_exit"]
        runner_units = self.total_units * self.adjusted_exit_percentages["runner"]
        
        # Ensure units add up to total (prevent float precision issues)
        if abs((first_exit_units + second_exit_units + runner_units) - self.total_units) > 0.00001:
            # Adjust the runner to make sure sum equals total_units
            runner_units = self.total_units - first_exit_units - second_exit_units
        
        # Create take profit stages
        self.take_profit_stages = [
            {
                "level": 1,
                "price": tp_levels[1],
                "units": first_exit_units,
                "percentage": self.adjusted_exit_percentages["first_exit"] * 100,
                "r_multiple": self._calculate_r_multiple(tp_levels[1]),
                "hit": False,
                "active": True
            },
            {
                "level": 2,
                "price": tp_levels[2],
                "units": second_exit_units,
                "percentage": self.adjusted_exit_percentages["second_exit"] * 100,
                "r_multiple": self._calculate_r_multiple(tp_levels[2]),
                "hit": False,
                "active": True
            },
            {
                "level": 3,
                "price": tp_levels[3],
                "units": runner_units,
                "percentage": self.adjusted_exit_percentages["runner"] * 100,
                "r_multiple": self._calculate_r_multiple(tp_levels[3]),
                "hit": False,
                "active": True
            }
        ]
        
        # Initialize trailing stop at entry price initially
        self.trailing_stop_level = entry_price
        
        # Reset stages hit
        self.stages_hit = []
        
        # Reset profit locked
        self.profit_locked_in = 0
        
        logging.info(f"Initialized {len(self.take_profit_stages)} take profit stages for {self.timeframe} position")
        for stage in self.take_profit_stages:
            logging.info(f"TP Level {stage['level']}: {stage['units']} units ({stage['percentage']}%) at price {stage['price']} ({stage['r_multiple']:.2f}R)")
        
        return self.take_profit_stages
    
    def _calculate_r_multiple(self, price: float) -> float:
        """
        Calculate the R-multiple (risk multiple) for a given price.
        """
        if self.initial_risk == 0:
            return 0
        
        if self.position_direction == "BUY":
            return (price - self.entry_price) / self.initial_risk
        else:  # SELL
            return (self.entry_price - price) / self.initial_risk
    
    def update_trailing_stop(self, current_price: float) -> float:
        """
        Updates the trailing stop based on current price and returns the new level.
        """
        if not self.use_trailing_stops:
            return self.trailing_stop_level
            
        # Calculate R-multiple at current price
        current_r = self._calculate_r_multiple(current_price)
        
        # Only update trailing stop if we've reached the activation threshold
        if current_r < self.trailing_activation_threshold:
            return self.trailing_stop_level
            
        # Calculate trail distance based on ATR and timeframe
        if self.atr_value:
            # Shorter timeframes use smaller trails
            if self.timeframe in ["M1", "M5", "M15"]:
                trail_distance = self.atr_value * 0.5
            elif self.timeframe in ["M30", "H1"]:
                trail_distance = self.atr_value * 0.75
            else:
                trail_distance = self.atr_value
        else:
            # If no ATR, use percentage of initial risk
            trail_distance = self.initial_risk * 0.5
        
        # Update trailing stop based on direction
        if self.position_direction == "BUY":
            new_stop = current_price - trail_distance
            if new_stop > self.trailing_stop_level:
                self.trailing_stop_level = new_stop
        else:  # SELL
            new_stop = current_price + trail_distance
            if new_stop < self.trailing_stop_level or self.trailing_stop_level == self.entry_price:
                self.trailing_stop_level = new_stop
                
        return self.trailing_stop_level
    
    def _check_stage_hit(self, current_price: float, stage: Dict[str, Any]) -> bool:
        """
        Check if a take profit stage has been hit.
        """
        if not stage["active"] or stage["hit"]:
            return False
            
        if self.position_direction == "BUY":
            return current_price >= stage["price"]
        else:  # SELL
            return current_price <= stage["price"]
    
    def update_take_profit_stages(self, current_price: float, position_direction: str) -> Dict[str, Any]:
        """
        Update take profit stages based on current price and check if any stages are hit.
        """
        if not self.take_profit_stages or position_direction != self.position_direction:
            return {
                "status": "error",
                "message": "Take profit stages not initialized or direction mismatch",
                "stages_hit": [],
                "actions": []
            }
            
        # Update trailing stop
        trailing_stop = self.update_trailing_stop(current_price)
        
        # Check if trailing stop is hit
        trailing_stop_hit = False
        if self.position_direction == "BUY":
            trailing_stop_hit = current_price <= trailing_stop and current_price > self.entry_price
        else:  # SELL
            trailing_stop_hit = current_price >= trailing_stop and current_price < self.entry_price
            
        # Check if any TP stages are hit
        stages_hit = []
        actions = []
        units_to_close = 0
        profit_locked = 0
        
        # First check individual take profit levels
        for stage in self.take_profit_stages:
            if self._check_stage_hit(current_price, stage):
                stage["hit"] = True
                stage["active"] = False
                stages_hit.append(stage["level"])
                
                # Calculate profit for this stage
                if self.position_direction == "BUY":
                    stage_profit = (stage["price"] - self.entry_price) * stage["units"]
                else:  # SELL
                    stage_profit = (self.entry_price - stage["price"]) * stage["units"]
                    
                profit_locked += stage_profit
                units_to_close += stage["units"]
                
                actions.append({
                    "action": "close_partial",
                    "units": stage["units"],
                    "price": stage["price"],
                    "level": stage["level"],
                    "profit": stage_profit
                })
        
        # Check trailing stop for remaining units
        remaining_units = self.total_units - units_to_close
        if trailing_stop_hit and remaining_units > 0:
            # Calculate profit for trailing stop
            if self.position_direction == "BUY":
                trailing_profit = (trailing_stop - self.entry_price) * remaining_units
            else:  # SELL
                trailing_profit = (self.entry_price - trailing_stop) * remaining_units
                
            profit_locked += trailing_profit
            
            actions.append({
                "action": "close_remaining_trailing",
                "units": remaining_units,
                "price": trailing_stop,
                "profit": trailing_profit
            })
            
            # Mark all remaining stages as hit via trailing stop
            for stage in self.take_profit_stages:
                if not stage["hit"]:
                    stage["hit"] = True
                    stage["active"] = False
                    stages_hit.append(f"{stage['level']}_trail")
        
        # Update total units and profit locked in
        self.total_units -= units_to_close
        self.profit_locked_in += profit_locked
        
        # Track which stages were hit this update
        self.stages_hit.extend(stages_hit)
        
        return {
            "status": "success",
            "current_price": current_price,
            "trailing_stop": trailing_stop,
            "trailing_stop_hit": trailing_stop_hit,
            "stages_hit": stages_hit,
            "actions": actions,
            "remaining_units": self.total_units,
            "profit_locked_in": self.profit_locked_in,
            "all_stages_complete": self.total_units == 0
        }
    
    def get_partial_close_percentages(self) -> Dict[int, float]:
        """
        Get the percentage of position to close at each TP level.
        """
        return {
            1: self.adjusted_exit_percentages["first_exit"] * 100,
            2: self.adjusted_exit_percentages["second_exit"] * 100,
            3: self.adjusted_exit_percentages["runner"] * 100
        }
    
    def get_take_profit_levels(self) -> Dict[int, float]:
        """
        Get the price levels for each take profit stage.
        """
        return {
            stage["level"]: stage["price"] 
            for stage in self.take_profit_stages
        }
    
    def get_current_status(self) -> Dict[str, Any]:
        """
        Get the current status of the take profit strategy.
        """
        return {
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "trailing_stop": self.trailing_stop_level,
            "direction": self.position_direction,
            "original_units": self.original_units,
            "remaining_units": self.total_units,
            "profit_locked_in": self.profit_locked_in,
            "stages": self.take_profit_stages,
            "stages_hit": self.stages_hit,
            "timeframe": self.timeframe,
            "exit_percentages": self.adjusted_exit_percentages
        }
        
    def apply_market_context(self, 
                          market_volatility: float = None, 
                          trend_strength: float = None,
                          regime: str = None) -> None:
        """
        Adjust take profit strategy based on market context.
        """
        if market_volatility is not None:
            self.market_volatility = market_volatility
            # Readjust take profit distribution
            self.adjusted_exit_percentages = self._adjust_for_volatility(self.exit_percentages)
            
            # Recalculate unit distribution if we have active stages
            if self.take_profit_stages and any(not stage["hit"] for stage in self.take_profit_stages):
                remaining_units = self.total_units
                active_stages = [stage for stage in self.take_profit_stages if not stage["hit"]]
                
                # Calculate total percentage of remaining position
                total_pct = sum(stage["percentage"]/100 for stage in active_stages)
                
                if total_pct > 0:
                    # Redistribute remaining units based on new percentages
                    for stage in active_stages:
                        level = stage["level"]
                        if level == 1:
                            stage["percentage"] = self.adjusted_exit_percentages["first_exit"] * 100
                        elif level == 2:
                            stage["percentage"] = self.adjusted_exit_percentages["second_exit"] * 100
                        elif level == 3:
                            stage["percentage"] = self.adjusted_exit_percentages["runner"] * 100
                            
                        # Update units based on new percentage
                        stage["units"] = remaining_units * (stage["percentage"] / 100) / total_pct
                
        # Adjust trailing stop activation based on trend strength
        if trend_strength is not None:
            # In strong trends, activate trailing stops later to capture more movement
            if trend_strength > 70:  # Strong trend
                # Increase threshold by 25-50%
                base_threshold = TIMEFRAME_RISK_SETTINGS.get(
                    self.timeframe, {"trailing_stop_activation": 1.5}
                )["trailing_stop_activation"]
                self.trailing_activation_threshold = base_threshold * 1.5
            elif trend_strength < 30:  # Weak trend
                # Decrease threshold to lock in profits faster
                base_threshold = TIMEFRAME_RISK_SETTINGS.get(
                    self.timeframe, {"trailing_stop_activation": 1.5}
                )["trailing_stop_activation"]
                self.trailing_activation_threshold = base_threshold * 0.75
                
        # Adjust take profit levels based on market regime
        if regime is not None:
            active_stages = [stage for stage in self.take_profit_stages if not stage["hit"]]
            
            if active_stages:
                if regime == "TRENDING":
                    # In trending markets, move take profits further away
                    for stage in active_stages:
                        r_multiple = stage["r_multiple"]
                        new_r = r_multiple * 1.2  # Increase by 20%
                        
                        # Calculate new price
                        if self.position_direction == "BUY":
                            stage["price"] = self.entry_price + (new_r * self.initial_risk)
                        else:  # SELL
                            stage["price"] = self.entry_price - (new_r * self.initial_risk)
                            
                        # Update R-multiple
                        stage["r_multiple"] = new_r
                        
                elif regime == "RANGING":
                    # In ranging markets, bring take profits closer
                    for stage in active_stages:
                        r_multiple = stage["r_multiple"]
                        new_r = max(1.0, r_multiple * 0.8)  # Decrease by 20%, but keep minimum 1R
                        
                        # Calculate new price
                        if self.position_direction == "BUY":
                            stage["price"] = self.entry_price + (new_r * self.initial_risk)
                        else:  # SELL
                            stage["price"] = self.entry_price - (new_r * self.initial_risk)
                            
                        # Update R-multiple
                        stage["r_multiple"] = new_r
                        
                elif regime == "VOLATILE":
                    # In volatile markets, bring first target closer but keep runner target
                    for stage in active_stages:
                        if stage["level"] == 1:
                            r_multiple = stage["r_multiple"]
                            new_r = max(1.0, r_multiple * 0.7)  # First target much closer
                            
                            # Calculate new price
                            if self.position_direction == "BUY":
                                stage["price"] = self.entry_price + (new_r * self.initial_risk)
                            else:  # SELL
                                stage["price"] = self.entry_price - (new_r * self.initial_risk)
                                
                            # Update R-multiple
                            stage["r_multiple"] = new_r
                        elif stage["level"] == 3:  # Don't change the runner target
                            pass
                        else:
                            r_multiple = stage["r_multiple"]
                            new_r = max(1.5, r_multiple * 0.85)  # Second target somewhat closer
                            
                            # Calculate new price
                            if self.position_direction == "BUY":
                                stage["price"] = self.entry_price + (new_r * self.initial_risk)
                            else:  # SELL
                                stage["price"] = self.entry_price - (new_r * self.initial_risk)
                                
                            # Update R-multiple
                            stage["r_multiple"] = new_r

class TimeBasedExitManager:
    """
    Manages time-based exit rules for positions.
    This allows closing trades based on time criteria, avoiding holding positions
    for too long when they're not moving favorably.
    """
    def __init__(self):
        self.open_positions = {}  # Dict to track open positions and their time data
        
    def register_position(self, 
                         position_id: str, 
                         entry_time: datetime.datetime, 
                         timeframe: str,
                         position_data: Dict[str, Any] = None) -> None:
        """
        Register a new position for time-based exit tracking.
        
        Args:
            position_id: Unique identifier for this position
            entry_time: Entry time of the position
            timeframe: Trading timeframe (e.g., "M5", "H1")
            position_data: Additional position data to store
        """
        try:
            # Determine maximum hold time based on timeframe
            max_hold_time = self._get_max_hold_time(timeframe)
            
            # Calculate target exit time
            target_exit_time = entry_time + max_hold_time
            
            # Store position data
            self.open_positions[position_id] = {
                "entry_time": entry_time,
                "timeframe": timeframe,
                "target_exit_time": target_exit_time,
                "position_data": position_data or {},
                "partial_exits": [],
                "profit_locked": False,
                "time_extensions": 0,
                "last_check_time": entry_time
            }
            
            logging.info(f"Registered position {position_id} for time-based exit at {target_exit_time}")
        except Exception as e:
            logging.error(f"Error registering position for time-based exit: {e}")
            
    def _get_max_hold_time(self, timeframe: str) -> datetime.timedelta:
        """
        Determine maximum holding time based on the timeframe.
        
        Args:
            timeframe: Trading timeframe (e.g., "M5", "H1", "D1")
            
        Returns:
            datetime.timedelta representing maximum hold time
        """
        # Default values based on timeframe
        if timeframe == "M1":
            return datetime.timedelta(hours=2)
        elif timeframe == "M5":
            return datetime.timedelta(hours=8)
        elif timeframe == "M15":
            return datetime.timedelta(hours=24)
        elif timeframe == "H1":
            return datetime.timedelta(days=2)
        elif timeframe == "H4":
            return datetime.timedelta(days=5)
        elif timeframe == "D1":
            return datetime.timedelta(days=14)
        elif timeframe == "W1":
            return datetime.timedelta(days=45)
        else:
            # Default to 24 hours if unknown timeframe
            return datetime.timedelta(hours=24)
            
    def update_position_status(self, 
                              position_id: str, 
                              current_time: datetime.datetime,
                              current_profit_pips: float = None, 
                              position_price_moved: bool = None,
                              has_reached_target: bool = None) -> Dict[str, Any]:
        """
        Check if a position should be closed based on time criteria.
        
        Args:
            position_id: Unique identifier for the position
            current_time: Current time
            current_profit_pips: Current profit in pips (if available)
            position_price_moved: Whether price has moved significantly since last check
            has_reached_target: Whether position has reached any take profit targets
            
        Returns:
            Dict with exit recommendation and reason
        """
        try:
            if position_id not in self.open_positions:
                return {"should_exit": False, "reason": "Position not found"}
                
            position = self.open_positions[position_id]
            
            # Update last check time
            position["last_check_time"] = current_time
            
            # If profit has been locked in (hit at least one take profit), extend the time
            if has_reached_target and not position["profit_locked"]:
                position["profit_locked"] = True
                # Extend deadline by 50% of the original time
                original_duration = position["target_exit_time"] - position["entry_time"]
                extension = original_duration * 0.5
                position["target_exit_time"] += extension
                position["time_extensions"] += 1
                
                logging.info(f"Position {position_id} has reached target, extending time by {extension}")
                
            # Check if current time exceeds target exit time
            if current_time >= position["target_exit_time"]:
                logging.info(f"Position {position_id} reached time-based exit criteria")
                return {
                    "should_exit": True, 
                    "reason": "Time-based exit triggered",
                    "position_age": (current_time - position["entry_time"]).total_seconds() / 3600,
                    "timeframe": position["timeframe"]
                }
                
            # Check for stagnation (no significant price movement for too long)
            if position_price_moved is not None and not position_price_moved:
                # Calculate time since entry
                time_since_entry = current_time - position["entry_time"]
                max_hold_time = self._get_max_hold_time(position["timeframe"])
                
                # If position has been open for >50% of max time and not moving, consider closing
                if time_since_entry > max_hold_time * 0.5:
                    logging.info(f"Position {position_id} stagnant for {time_since_entry}")
                    return {
                        "should_exit": True,
                        "reason": "Position stagnant for too long",
                        "position_age": time_since_entry.total_seconds() / 3600,
                        "timeframe": position["timeframe"]
                    }
                
            # Check if in slight loss for too long
            if current_profit_pips is not None and current_profit_pips < 0:
                time_since_entry = current_time - position["entry_time"]
                max_hold_time = self._get_max_hold_time(position["timeframe"])
                
                # If in loss for >75% of max time, consider closing
                if time_since_entry > max_hold_time * 0.75:
                    logging.info(f"Position {position_id} in loss for {time_since_entry}")
                    return {
                        "should_exit": True,
                        "reason": "Position in loss for too long",
                        "position_age": time_since_entry.total_seconds() / 3600,
                        "timeframe": position["timeframe"]
                    }
                    
            # No exit needed
            return {"should_exit": False, "reason": "Within time parameters"}
        except Exception as e:
            logging.error(f"Error checking time-based exit for position {position_id}: {e}")
            return {"should_exit": False, "reason": f"Error: {str(e)}"}
            
    def remove_position(self, position_id: str) -> None:
        """
        Remove a position from time-based exit tracking.
        
        Args:
            position_id: Unique identifier for the position
        """
        try:
            if position_id in self.open_positions:
                position_data = self.open_positions.pop(position_id)
                logging.info(f"Removed position {position_id} from time-based exit tracking. "
                           f"Age: {(position_data['last_check_time'] - position_data['entry_time']).total_seconds() / 3600:.2f} hours")
        except Exception as e:
            logging.error(f"Error removing position from time-based exit tracking: {e}")
            
    def get_position_time_info(self, position_id: str) -> Dict[str, Any]:
        """
        Get time information for a specific position.
        
        Args:
            position_id: Unique identifier for the position
            
        Returns:
            Dict with position time information
        """
        try:
            if position_id not in self.open_positions:
                return {"error": "Position not found"}
                
            position = self.open_positions[position_id]
            current_time = datetime.datetime.now()
            
            return {
                "entry_time": position["entry_time"],
                "target_exit_time": position["target_exit_time"],
                "time_remaining_hours": (position["target_exit_time"] - current_time).total_seconds() / 3600,
                "age_hours": (current_time - position["entry_time"]).total_seconds() / 3600,
                "timeframe": position["timeframe"],
                "profit_locked": position["profit_locked"],
                "time_extensions": position["time_extensions"]
            }
        except Exception as e:
            logging.error(f"Error getting position time info: {e}")
            return {"error": str(e)}

class CorrelationAnalyzer:
    """
    Analyzes correlations between trading instruments to manage portfolio risk
    by avoiding over-exposure to highly correlated assets.
    """
    def __init__(self, correlation_threshold=0.7, max_update_frequency_hours=24):
        self.correlation_matrix = {}  # Format: {(symbol1, symbol2): correlation_value}
        self.correlation_threshold = correlation_threshold
        self.max_update_frequency_hours = max_update_frequency_hours
        self.last_update_time = {}  # Format: {(symbol1, symbol2): last_update_timestamp}
        self.price_data = {}  # Format: {symbol: [price_data]}
        
    def update_price_data(self, symbol: str, price: float, max_data_points=100) -> None:
        """
        Update stored price data for correlation calculation.
        
        Args:
            symbol: The trading symbol
            price: Current price
            max_data_points: Maximum number of price points to store
        """
        try:
            if symbol not in self.price_data:
                self.price_data[symbol] = []
                
            self.price_data[symbol].append(price)
            
            # Trim data to max length
            if len(self.price_data[symbol]) > max_data_points:
                self.price_data[symbol] = self.price_data[symbol][-max_data_points:]
        except Exception as e:
            logging.error(f"Error updating price data for correlation: {e}")
            
    def calculate_correlation(self, symbol1: str, symbol2: str) -> float:
        """
        Calculate correlation coefficient between two symbols.
        
        Args:
            symbol1: First trading symbol
            symbol2: Second trading symbol
            
        Returns:
            Correlation coefficient (-1 to 1)
        """
        try:
            # Check if we need to update correlation
            pair = tuple(sorted([symbol1, symbol2]))
            current_time = datetime.datetime.now()
            
            # Skip calculation if we've updated recently and have a value
            if pair in self.correlation_matrix and pair in self.last_update_time:
                hours_since_update = (current_time - self.last_update_time[pair]).total_seconds() / 3600
                if hours_since_update < self.max_update_frequency_hours:
                    return self.correlation_matrix[pair]
                    
            # Check if we have enough data for both symbols
            if (symbol1 not in self.price_data or symbol2 not in self.price_data or
                len(self.price_data[symbol1]) < 30 or len(self.price_data[symbol2]) < 30):
                # Not enough data, return 0 (uncorrelated)
                return 0.0
                
            # Get the last N matching data points
            min_length = min(len(self.price_data[symbol1]), len(self.price_data[symbol2]))
            data1 = self.price_data[symbol1][-min_length:]
            data2 = self.price_data[symbol2][-min_length:]
            
            # Calculate returns instead of using raw prices
            returns1 = [data1[i] / data1[i-1] - 1 for i in range(1, len(data1))]
            returns2 = [data2[i] / data2[i-1] - 1 for i in range(1, len(data2))]
            
            # Calculate correlation on returns
            if len(returns1) < 2:
                return 0.0
                
            # Calculate means
            mean1 = sum(returns1) / len(returns1)
            mean2 = sum(returns2) / len(returns2)
            
            # Calculate correlation coefficient
            numerator = sum((x - mean1) * (y - mean2) for x, y in zip(returns1, returns2))
            denominator = (sum((x - mean1) ** 2 for x in returns1) * 
                         sum((y - mean2) ** 2 for y in returns2)) ** 0.5
            
            # Handle division by zero
            if denominator == 0:
                correlation = 0.0
            else:
                correlation = numerator / denominator
                
            # Store the result
            self.correlation_matrix[pair] = correlation
            self.last_update_time[pair] = current_time
            
            return correlation
        except Exception as e:
            logging.error(f"Error calculating correlation: {e}")
            return 0.0
            
    def get_correlated_symbols(self, target_symbol: str, all_symbols: List[str]) -> List[Tuple[str, float]]:
        """
        Find all symbols that are highly correlated with the target symbol.
        
        Args:
            target_symbol: Symbol to check correlations against
            all_symbols: List of all symbols to check
            
        Returns:
            List of tuples (symbol, correlation) where correlation > threshold
        """
        try:
            correlated_symbols = []
            
            for symbol in all_symbols:
                if symbol == target_symbol:
                    continue
                    
                correlation = abs(self.calculate_correlation(target_symbol, symbol))
                
                if correlation >= self.correlation_threshold:
                    correlated_symbols.append((symbol, correlation))
                    
            # Sort by correlation (highest first)
            correlated_symbols.sort(key=lambda x: x[1], reverse=True)
            
            return correlated_symbols
        except Exception as e:
            logging.error(f"Error getting correlated symbols: {e}")
            return []

##############################################################################
# Application State Management
##############################################################################

# Global instances for application state
position_manager = None
alert_handler = None

async def get_position_manager() -> PositionManager:
    """Get or create the global position manager instance"""
    global position_manager
    if position_manager is None:
        position_manager = PositionManager()
        await position_manager.initialize()
    return position_manager

async def get_alert_handler() -> AlertHandler:
    """Get or create the AlertHandler singleton"""
    try:
        position_manager = await get_position_manager()
        
        alert_handler = AlertHandler(position_manager)
        await alert_handler.initialize()
        return alert_handler
    except Exception as e:
        logger.error(f"Failed to create AlertHandler: {str(e)}")
        raise

##############################################################################
# FastAPI Setup & Lifespan
##############################################################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper initialization and cleanup"""
    logger.info("Initializing application...")
    
    try:
        # Initialize position manager
        pm = await get_position_manager()
        logger.info("Position manager initialized")
        
        # Initialize alert handler with position manager
        ah = await get_alert_handler()
        logger.info("Alert handler initialized")
        
        # Set up shutdown signal handling
        handle_shutdown_signals()
        
        logger.info("Services initialized successfully")
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup_resources()
        logger.info("Shutdown complete")

async def cleanup_resources():
    """Clean up application resources"""
    global alert_handler, position_manager, redis_client
    
    tasks = []
    
    # Stop alert handler if initialized
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    
    # Close any open sessions
    if hasattr(get_session, 'session') and not get_session.session.closed:
        tasks.append(get_session.session.close())
    
    # Close Redis connection
    if redis_client is not None:
        try:
            await redis_client.close()
            redis_client = None
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}")
    
    # Wait for all cleanup tasks
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def handle_shutdown_signals():
    """Set up signal handlers for graceful shutdown"""
    async def shutdown(sig: signal.Signals):
        logger.info(f"Received exit signal {sig.name}")
        await cleanup_resources()
        
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig,
            lambda s=sig: asyncio.create_task(shutdown(s))
        )

# Create FastAPI app with proper configuration
app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.2.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

##############################################################################
# API Endpoints
##############################################################################

@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "ok", "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()}

@app.get("/api/positions")
async def get_positions_endpoint(position_manager: PositionManager = Depends(get_position_manager)):
    """Get all open positions"""
    try:
        positions = await position_manager.get_positions()
        return {"status": "success", "positions": positions}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching positions: {str(e)}"
        )

@app.post("/api/alerts")
async def process_alert_endpoint(
    alert_data: AlertData,
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Process an alert from TradingView or other sources"""
    try:
        result = await alert_handler.process_alert(alert_data)
        return result
    except ValidationError as e:
        logger.error(f"Validation error processing alert: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid alert data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error processing alert: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing alert: {str(e)}"
        )

@app.post("/tradingview")
async def tradingview_webhook(
    request: Request,
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Webhook for TradingView alerts"""
    try:
        # Parse incoming JSON
        payload = await request.json()
        logger.info(f"Received TradingView alert: {payload}")
        
        # Extract alert data
        alert_data = AlertData(
            symbol=payload.get("symbol"),
            timeframe=payload.get("timeframe", "1H"),
            action=payload.get("action"),
            price=float(payload.get("price", 0)),
            stop_loss=float(payload.get("stop_loss", 0)) if payload.get("stop_loss") else None,
            take_profit=float(payload.get("take_profit", 0)) if payload.get("take_profit") else None,
            risk_percentage=float(payload.get("risk_percentage", 2.0)),
            message=payload.get("message")
        )
        
        # Process the alert
        result = await alert_handler.process_alert(alert_data)
        return result
    except ValidationError as e:
        logger.error(f"Validation error from TradingView: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "error", "detail": f"Invalid alert data: {str(e)}"}
        )
    except Exception as e:
        logger.error(f"Error processing TradingView alert: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": f"Error processing alert: {str(e)}"}
        )

@app.get("/api/account")
async def get_account_summary_endpoint():
    """Get account summary from OANDA"""
    try:
        success, data = await get_account_summary()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=data.get("error", "Unknown error getting account summary")
            )
        return data
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching account summary: {str(e)}"
        )

@app.get("/api/test/risk-management")
async def test_risk_management_endpoint():
    """API endpoint to test timeframe risk management and take profit functionality"""
    # Test TimeframeRiskManager
    trm = TimeframeRiskManager(default_timeframe="H1")
    
    # Get settings for different timeframes
    settings = {
        "H1": trm.get_risk_settings("H1"),
        "M5": trm.get_risk_settings("M5"),
        "D1": trm.get_risk_settings("D1")
    }
    
    # Test Multi-stage Take Profit Manager
    tp_manager = MultiStageTakeProfitManager(
        timeframe="H1",
        atr_multiplier=1.0,
        use_dynamic_targets=True,
        use_trailing_stops=True,
        market_volatility=50
    )
    
    # Create sample position
    entry_price = 100.0
    stop_loss = 98.0
    take_profit = 105.0
    position_direction = "BUY"
    total_units = 10000
    
    # Initialize TP stages
    tp_stages = tp_manager.initialize_take_profit_stages(
        entry_price=entry_price,
        stop_loss=stop_loss, 
        initial_take_profit=take_profit,
        position_direction=position_direction,
        total_units=total_units
    )
    
    # Create results at different prices
    tp_results = {}
    for price in [101.0, 102.0, 103.0, 104.0, 105.0, 106.0]:
        update_result = tp_manager.update_take_profit_stages(price, position_direction)
        tp_results[f"price_{price}"] = {
            "stages_hit": update_result["stages_hit"],
            "actions": update_result["actions"],
            "remaining_units": update_result["remaining_units"],
            "trailing_stop": update_result["trailing_stop"]
        }
    
    return {
        "status": "success",
        "risk_settings": settings,
        "take_profit_stages": [
            {k: v for k, v in stage.items() if k != 'r_multiple'} 
            for stage in tp_stages
        ],
        "take_profit_results": tp_results
    }

@app.post("/api/close")
async def close_position_endpoint(
    close_data: Dict[str, Any],
    position_manager: PositionManager = Depends(get_position_manager),
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Close a specific position or all positions"""
    try:
        # Check if closing all positions
        if close_data.get("all", False):
            positions = await position_manager.get_positions()
            results = {}
            
            for position in positions.values():
                if 'symbol' in position:
                    # Close in alert handler (which will handle risk managers)
                    await alert_handler._close_position(
                        position['symbol'], 
                        "manual_close"
                    )
                    results[position['symbol']] = {
                        "status": "closed"
                    }
            
            return {"status": "success", "closed": results}
        
        # Close a specific position
        elif "symbol" in close_data:
            symbol = standardize_symbol(close_data["symbol"])
            position = await position_manager.get_position(symbol)
            
            if not position:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No open position found for {symbol}"
                )
                
            # Close in alert handler (which will handle risk managers)
            await alert_handler._close_position(
                symbol, 
                "manual_close"
            )
            
            return {
                "status": "success",
                "closed": {
                    "symbol": symbol
                }
            }
        
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Must specify 'all' or 'symbol' to close positions"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error closing position: {str(e)}"
        )

##############################################################################
# Market Utility Functions
##############################################################################

def check_market_hours(session_config: dict) -> bool:
    """Check if market is open based on session config"""
    try:
        current_time = datetime.now(timezone('Asia/Bangkok'))
        weekday = current_time.weekday()  # 0 is Monday, 6 is Sunday
        
        # Check for holidays first
        holidays = session_config.get('holidays', [])
        current_date = current_time.strftime('%Y-%m-%d')
        if current_date in holidays:
            logger.info(f"Market closed due to holiday on {current_date}")
            return False
            
        # Check for weekend (Saturday and Sunday typically closed)
        if weekday >= 5 and not session_config.get('weekend_trading', False):
            logger.info(f"Market closed for weekend (day {weekday})")
            return False
            
        # Check trading hours
        trading_hours = session_config.get('trading_hours', {}).get(str(weekday), None)
        if not trading_hours:
            logger.info(f"No trading hours defined for day {weekday}")
            return False
            
        # Convert current time to seconds since midnight for comparison
        current_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
        
        # Check if current time falls within any trading sessions
        for session in trading_hours:
            start_time = session.get('start', '00:00:00')
            end_time = session.get('end', '23:59:59')
            
            # Convert times to seconds
            start_h, start_m, start_s = map(int, start_time.split(':'))
            end_h, end_m, end_s = map(int, end_time.split(':'))
            
            start_seconds = start_h * 3600 + start_m * 60 + start_s
            end_seconds = end_h * 3600 + end_m * 60 + end_s
            
            if start_seconds <= current_seconds <= end_seconds:
                return True
                
        logger.info(f"Market closed at current time {current_time.strftime('%H:%M:%S')}")
        return False
    except Exception as e:
        logger.error(f"Error checking market hours: {str(e)}")
        return False  # Default to closed if any error

async def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """Check if an instrument is tradeable - determine session and check market hours"""
    # This is a duplicate implementation and should be removed in favor of the async version
    pass  # This function will be replaced by references to the async version

def get_atr(instrument: str, timeframe: str) -> float:
    """Get the ATR value for risk management"""
    try:
        # Default ATR values for different instrument types
        default_atrs = {
            "FOREX": {
                "15M": 0.0005,  # 5 pips for major pairs
                "1H": 0.0010,   # 10 pips
                "4H": 0.0020,   # 20 pips
                "1D": 0.0050    # 50 pips
            },
            "CRYPTO": {
                "15M": 50.0,
                "1H": 100.0,
                "4H": 250.0,
                "1D": 500.0
            },
            "XAU_USD": {
                "15M": 0.5,
                "1H": 1.0,
                "4H": 2.0,
                "1D": 5.0
            }
        }
        
        instrument_type = get_instrument_type(instrument)
        
        # Return default ATR for this instrument type and timeframe
        return default_atrs.get(instrument_type, default_atrs["FOREX"]).get(timeframe, default_atrs[instrument_type]["1H"])
    except Exception as e:
        logger.error(f"Error getting ATR for {instrument} on {timeframe}: {str(e)}")
        return 0.0010  # Default fallback to 10 pips

def get_instrument_type(symbol: str) -> str:
    """Determine instrument type from standardized symbol"""
    symbol = standardize_symbol(symbol)
    
    # Crypto check
    if any(crypto in symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return "CRYPTO"
    
    # Gold check
    if "XAU" in symbol:
        return "XAU_USD"
    
    # Default to forex
    return "FOREX"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """Get ATR multiplier based on instrument type and timeframe"""
    multipliers = {
        "FOREX": {
            "15M": 1.5,
            "1H": 1.75,
            "4H": 2.0,
            "1D": 2.25
        },
        "CRYPTO": {
            "15M": 2.0,
            "1H": 2.25,
            "4H": 2.5,
            "1D": 2.75
        },
        "XAU_USD": {
            "15M": 1.75,
            "1H": 2.0,
            "4H": 2.25,
            "1D": 2.5
        }
    }
    
    return multipliers.get(instrument_type, multipliers["FOREX"]).get(timeframe, multipliers[instrument_type]["1H"])

@handle_async_errors
async def get_current_price(instrument: str, action: str = "BUY") -> float:
    """Get current price of instrument with error handling and timeout"""
    try:
        # Get OANDA pricing
        session = await get_session()
        normalized = standardize_symbol(instrument)
        
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account_id}/pricing"
        params = {
            "instruments": normalized,
            "includeHomeConversions": True
        }
        
        async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"OANDA price fetch error for {normalized}: {error_text}")
                return 0.0
                
            data = await response.json()
            
            if "prices" not in data or not data["prices"]:
                logger.error(f"No price data returned for {normalized}")
                return 0.0
                
            price_data = data["prices"][0]
            
            if action.upper() == "BUY":
                # Use ask price for buying
                ask_details = price_data.get("asks", [{}])[0]
                return float(ask_details.get("price", 0))
            else:
                # Use bid price for selling
                bid_details = price_data.get("bids", [{}])[0]
                return float(bid_details.get("price", 0))
                
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting price for {instrument}")
        return 0.0
    except Exception as e:
        logger.error(f"Error getting price for {instrument}: {str(e)}")
        return 0.0

def get_current_market_session(current_time: datetime) -> str:
    """Determine the current market session based on time"""
    # Define market sessions based on hour (UTC)
    weekday = current_time.weekday()
    hour = current_time.hour
    
    # Weekend check
    if weekday >= 5:  # Saturday and Sunday
        return "WEEKEND"
        
    # Asian session: 00:00-08:00 UTC
    if 0 <= hour < 8:
        return "ASIAN"
        
    # London session: 08:00-16:00 UTC
    elif 8 <= hour < 16:
        return "LONDON"
        
    # New York session: 13:00-21:00 UTC (overlap with London for 3 hours)
    elif 13 <= hour < 21:
        if 13 <= hour < 16:
            return "LONDON_NY_OVERLAP"
        else:
            return "NEWYORK"
            
    # Sydney session: 21:00-00:00 UTC
    else:
        return "SYDNEY"

@handle_async_errors
async def get_account_summary() -> Tuple[bool, Dict[str, Any]]:
    """Get account summary from OANDA API"""
    try:
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account_id}/summary"
        
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to fetch account summary: {error_text}")
                return False, {"error": error_text}
                
            data = await response.json()
            
            # Extract key account metrics
            account = data.get("account", {})
            
            # Format and return the account summary
            return True, {
                "id": account.get("id"),
                "name": account.get("alias"),
                "balance": float(account.get("balance", 0)),
                "currency": account.get("currency"),
                "margin_available": float(account.get("marginAvailable", 0)),
                "margin_used": float(account.get("marginUsed", 0)),
                "margin_closeout_percent": float(account.get("marginCloseoutPercent", 0)),
                "open_trade_count": account.get("openTradeCount", 0),
                "open_position_count": account.get("openPositionCount", 0),
                "unrealized_pl": float(account.get("unrealizedPL", 0)),
                "nav": float(account.get("NAV", 0)),
                "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()
            }
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching account summary")
        return False, {"error": "Request timed out"}
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        return False, {"error": str(e)}

##############################################################################
# Main Application Entry Point
##############################################################################

def start():
    """Start the application using uvicorn"""
    import uvicorn
    setup_logging()
    logger.info(f"Starting application in {config.environment} mode")
    
    host = config.host
    port = config.port
    
    logger.info(f"Server starting at {host}:{port}")
    uvicorn.run(
        app,  # Use the app instance directly
        host=host,
        port=port,
        reload=config.environment == "development"
    )

if __name__ == "__main__":
    start() 
