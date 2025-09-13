"""
Regime Classifier Module
Provides market regime classification using Lorentzian Distance
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class LorentzianDistanceClassifier:
    """
    Market regime classifier using Lorentzian distance
    """
    
    def __init__(self):
        self.regimes = {}
        self.lookback_period = 50
        self.regime_threshold = 0.5
        
    def classify_regime(self, symbol: str, price_data: np.ndarray) -> str:
        """
        Classify market regime for a given symbol
        
        Args:
            symbol: Trading symbol
            price_data: Array of price data
            
        Returns:
            str: Regime classification ('trending', 'ranging', 'volatile')
        """
        try:
            if len(price_data) < self.lookback_period:
                return 'unknown'
                
            # Calculate price momentum
            momentum = self._calculate_momentum(price_data)
            
            # Calculate volatility
            volatility = self._calculate_volatility(price_data)
            
            # Calculate trend strength
            trend_strength = self._calculate_trend_strength(price_data)
            
            # Classify regime based on metrics
            if trend_strength > 0.6 and volatility < 0.3:
                regime = 'trending'
            elif volatility > 0.5:
                regime = 'volatile'
            else:
                regime = 'ranging'
                
            # Store regime data
            self.regimes[symbol] = {
                'regime': regime,
                'confidence': min(trend_strength, 1.0 - volatility),
                'last_update': datetime.now(timezone.utc),
                'momentum': momentum,
                'volatility': volatility,
                'trend_strength': trend_strength
            }
            
            return regime
            
        except Exception as e:
            logger.error(f"Error classifying regime for {symbol}: {e}")
            return 'unknown'
    
    def _calculate_momentum(self, price_data: np.ndarray) -> float:
        """Calculate price momentum"""
        if len(price_data) < 10:
            return 0.0
            
        # Simple momentum calculation
        recent_avg = np.mean(price_data[-10:])
        older_avg = np.mean(price_data[-20:-10]) if len(price_data) >= 20 else np.mean(price_data[:-10])
        
        if older_avg == 0:
            return 0.0
            
        return (recent_avg - older_avg) / older_avg
    
    def _calculate_volatility(self, price_data: np.ndarray) -> float:
        """Calculate price volatility"""
        if len(price_data) < 10:
            return 0.0
            
        # Calculate returns
        returns = np.diff(price_data) / price_data[:-1]
        
        # Calculate standard deviation of returns
        return np.std(returns)
    
    def _calculate_trend_strength(self, price_data: np.ndarray) -> float:
        """Calculate trend strength using linear regression"""
        if len(price_data) < 10:
            return 0.0
            
        # Create time series
        x = np.arange(len(price_data))
        
        # Calculate linear regression
        coeffs = np.polyfit(x, price_data, 1)
        slope = coeffs[0]
        
        # Normalize slope to 0-1 range
        price_range = np.max(price_data) - np.min(price_data)
        if price_range == 0:
            return 0.0
            
        normalized_slope = abs(slope) / (price_range / len(price_data))
        return min(normalized_slope, 1.0)
    
    def get_regime(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current regime for symbol"""
        return self.regimes.get(symbol)
    
    def get_all_regimes(self) -> Dict[str, Dict[str, Any]]:
        """Get all regime data"""
        return self.regimes.copy()
    
    def is_trending(self, symbol: str) -> bool:
        """Check if symbol is in trending regime"""
        regime_data = self.get_regime(symbol)
        return regime_data and regime_data['regime'] == 'trending'
    
    def is_volatile(self, symbol: str) -> bool:
        """Check if symbol is in volatile regime"""
        regime_data = self.get_regime(symbol)
        return regime_data and regime_data['regime'] == 'volatile'
    
    def get_confidence(self, symbol: str) -> float:
        """Get regime confidence for symbol"""
        regime_data = self.get_regime(symbol)
        return regime_data.get('confidence', 0.0) if regime_data else 0.0
