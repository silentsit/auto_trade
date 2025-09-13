"""
Volatility Monitor Module
Monitors and tracks market volatility for risk management
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class VolatilityMonitor:
    """
    Monitors market volatility and provides volatility-based risk adjustments
    """
    
    def __init__(self):
        self.volatility_states = {}
        self.lookback_period = 20
        self.volatility_threshold_high = 0.02  # 2% high volatility threshold
        self.volatility_threshold_low = 0.005  # 0.5% low volatility threshold
        
    def update_volatility(self, symbol: str, price_data: np.ndarray) -> Dict[str, Any]:
        """
        Update volatility metrics for a symbol
        
        Args:
            symbol: Trading symbol
            price_data: Array of price data
            
        Returns:
            Dict containing volatility metrics
        """
        try:
            if len(price_data) < self.lookback_period:
                return self._get_default_volatility_state()
                
            # Calculate various volatility metrics
            current_vol = self._calculate_current_volatility(price_data)
            historical_vol = self._calculate_historical_volatility(price_data)
            volatility_ratio = current_vol / historical_vol if historical_vol > 0 else 1.0
            
            # Determine volatility state
            if current_vol > self.volatility_threshold_high:
                state = 'high'
            elif current_vol < self.volatility_threshold_low:
                state = 'low'
            else:
                state = 'normal'
                
            # Calculate volatility trend
            trend = self._calculate_volatility_trend(price_data)
            
            # Store volatility data
            self.volatility_states[symbol] = {
                'current_volatility': current_vol,
                'historical_volatility': historical_vol,
                'volatility_ratio': volatility_ratio,
                'state': state,
                'trend': trend,
                'last_update': datetime.now(timezone.utc),
                'is_spiking': volatility_ratio > 1.5,
                'is_calm': volatility_ratio < 0.7
            }
            
            return self.volatility_states[symbol]
            
        except Exception as e:
            logger.error(f"Error updating volatility for {symbol}: {e}")
            return self._get_default_volatility_state()
    
    def _calculate_current_volatility(self, price_data: np.ndarray) -> float:
        """Calculate current volatility using recent data"""
        if len(price_data) < 10:
            return 0.0
            
        # Use last 10 periods for current volatility
        recent_data = price_data[-10:]
        returns = np.diff(recent_data) / recent_data[:-1]
        return np.std(returns)
    
    def _calculate_historical_volatility(self, price_data: np.ndarray) -> float:
        """Calculate historical volatility using full dataset"""
        if len(price_data) < 20:
            return 0.0
            
        returns = np.diff(price_data) / price_data[:-1]
        return np.std(returns)
    
    def _calculate_volatility_trend(self, price_data: np.ndarray) -> str:
        """Calculate volatility trend (increasing, decreasing, stable)"""
        if len(price_data) < 30:
            return 'unknown'
            
        # Calculate rolling volatility
        window_size = 10
        rolling_vol = []
        
        for i in range(window_size, len(price_data)):
            window_data = price_data[i-window_size:i]
            returns = np.diff(window_data) / window_data[:-1]
            vol = np.std(returns)
            rolling_vol.append(vol)
            
        if len(rolling_vol) < 3:
            return 'unknown'
            
        # Calculate trend
        recent_avg = np.mean(rolling_vol[-3:])
        older_avg = np.mean(rolling_vol[:-3])
        
        if recent_avg > older_avg * 1.1:
            return 'increasing'
        elif recent_avg < older_avg * 0.9:
            return 'decreasing'
        else:
            return 'stable'
    
    def _get_default_volatility_state(self) -> Dict[str, Any]:
        """Get default volatility state"""
        return {
            'current_volatility': 0.0,
            'historical_volatility': 0.0,
            'volatility_ratio': 1.0,
            'state': 'unknown',
            'trend': 'unknown',
            'last_update': datetime.now(timezone.utc),
            'is_spiking': False,
            'is_calm': True
        }
    
    def get_volatility_state(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current volatility state for symbol"""
        return self.volatility_states.get(symbol)
    
    def get_all_volatility_states(self) -> Dict[str, Dict[str, Any]]:
        """Get all volatility states"""
        return self.volatility_states.copy()
    
    def is_high_volatility(self, symbol: str) -> bool:
        """Check if symbol has high volatility"""
        state = self.get_volatility_state(symbol)
        return state and state['state'] == 'high'
    
    def is_low_volatility(self, symbol: str) -> bool:
        """Check if symbol has low volatility"""
        state = self.get_volatility_state(symbol)
        return state and state['state'] == 'low'
    
    def is_volatility_spiking(self, symbol: str) -> bool:
        """Check if volatility is spiking"""
        state = self.get_volatility_state(symbol)
        return state and state.get('is_spiking', False)
    
    def is_volatility_calm(self, symbol: str) -> bool:
        """Check if volatility is calm"""
        state = self.get_volatility_state(symbol)
        return state and state.get('is_calm', False)
    
    def get_volatility_adjustment_factor(self, symbol: str) -> float:
        """
        Get position size adjustment factor based on volatility
        
        Returns:
            float: Adjustment factor (1.0 = no adjustment, <1.0 = reduce size, >1.0 = increase size)
        """
        state = self.get_volatility_state(symbol)
        if not state:
            return 1.0
            
        current_vol = state['current_volatility']
        historical_vol = state['historical_volatility']
        
        if historical_vol == 0:
            return 1.0
            
        ratio = current_vol / historical_vol
        
        # Adjust position size based on volatility
        if ratio > 1.5:  # High volatility
            return 0.7  # Reduce position size
        elif ratio < 0.7:  # Low volatility
            return 1.2  # Increase position size
        else:
            return 1.0  # Normal volatility
    
    def get_risk_multiplier(self, symbol: str) -> float:
        """
        Get risk multiplier based on volatility state
        
        Returns:
            float: Risk multiplier for stop loss and take profit calculations
        """
        state = self.get_volatility_state(symbol)
        if not state:
            return 1.0
            
        if state['state'] == 'high':
            return 1.5  # Wider stops in high volatility
        elif state['state'] == 'low':
            return 0.8  # Tighter stops in low volatility
        else:
            return 1.0  # Normal stops
