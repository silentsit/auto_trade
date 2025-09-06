"""
INSTITUTIONAL MARKET MICROSTRUCTURE ANALYSIS
Advanced order flow and market structure analysis for high-frequency trading
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

@dataclass
class OrderFlowMetrics:
    """Order flow analysis metrics"""
    bid_ask_spread: float
    mid_price: float
    volume_weighted_price: float
    order_imbalance: float
    price_impact: float
    market_depth: float
    volatility_regime: str
    liquidity_score: float

@dataclass
class MarketRegime:
    """Market regime classification"""
    regime_type: str  # "trending", "ranging", "volatile", "breakout"
    confidence: float
    volatility_percentile: float
    trend_strength: float
    volume_profile: str  # "high", "normal", "low"

class MarketMicrostructureAnalyzer:
    """
    Institutional-grade market microstructure analysis
    Provides order flow, liquidity, and regime detection
    """
    
    def __init__(self):
        self.lookback_periods = 100
        self.volatility_window = 20
        self.regime_threshold = 0.7
        
    async def analyze_order_flow(self, 
                               symbol: str, 
                               bid_prices: List[float],
                               ask_prices: List[float],
                               volumes: List[float],
                               timestamps: List[datetime]) -> OrderFlowMetrics:
        """
        Analyze order flow and market microstructure
        
        Args:
            symbol: Trading instrument
            bid_prices: Historical bid prices
            ask_prices: Historical ask prices  
            volumes: Historical volumes
            timestamps: Price timestamps
            
        Returns:
            OrderFlowMetrics with comprehensive analysis
        """
        try:
            if len(bid_prices) < 10:
                return self._default_metrics()
            
            # Calculate basic metrics
            current_bid = bid_prices[-1]
            current_ask = ask_prices[-1]
            spread = current_ask - current_bid
            mid_price = (current_bid + current_ask) / 2
            
            # Volume-weighted price
            vwp = self._calculate_vwp(bid_prices, ask_prices, volumes)
            
            # Order imbalance (buy vs sell pressure)
            order_imbalance = self._calculate_order_imbalance(bid_prices, ask_prices, volumes)
            
            # Price impact analysis
            price_impact = self._calculate_price_impact(bid_prices, ask_prices, volumes)
            
            # Market depth estimation
            market_depth = self._estimate_market_depth(volumes)
            
            # Volatility regime classification
            volatility_regime = self._classify_volatility_regime(bid_prices, ask_prices)
            
            # Liquidity score (0-1, higher = more liquid)
            liquidity_score = self._calculate_liquidity_score(spread, volumes, market_depth)
            
            return OrderFlowMetrics(
                bid_ask_spread=spread,
                mid_price=mid_price,
                volume_weighted_price=vwp,
                order_imbalance=order_imbalance,
                price_impact=price_impact,
                market_depth=market_depth,
                volatility_regime=volatility_regime,
                liquidity_score=liquidity_score
            )
            
        except Exception as e:
            logger.error(f"Order flow analysis failed for {symbol}: {e}")
            return self._default_metrics()
    
    def _calculate_vwp(self, bid_prices: List[float], ask_prices: List[float], volumes: List[float]) -> float:
        """Calculate volume-weighted price"""
        if not volumes or sum(volumes) == 0:
            return (bid_prices[-1] + ask_prices[-1]) / 2
        
        mid_prices = [(b + a) / 2 for b, a in zip(bid_prices, ask_prices)]
        total_volume = sum(volumes)
        vwp = sum(p * v for p, v in zip(mid_prices, volumes)) / total_volume
        return vwp
    
    def _calculate_order_imbalance(self, bid_prices: List[float], ask_prices: List[float], volumes: List[float]) -> float:
        """Calculate order imbalance (-1 to 1, positive = buy pressure)"""
        if len(bid_prices) < 2:
            return 0.0
        
        # Price momentum as proxy for order flow
        price_changes = []
        for i in range(1, len(bid_prices)):
            mid_change = ((bid_prices[i] + ask_prices[i]) / 2) - ((bid_prices[i-1] + ask_prices[i-1]) / 2)
            price_changes.append(mid_change)
        
        # Volume-weighted momentum
        if volumes and sum(volumes[1:]) > 0:
            weighted_momentum = sum(c * v for c, v in zip(price_changes, volumes[1:])) / sum(volumes[1:])
            # Normalize to -1 to 1 range
            return np.tanh(weighted_momentum * 1000)  # Scale factor for normalization
        else:
            return 0.0
    
    def _calculate_price_impact(self, bid_prices: List[float], ask_prices: List[float], volumes: List[float]) -> float:
        """Calculate price impact of trades"""
        if len(bid_prices) < 5:
            return 0.0
        
        # Use spread as proxy for price impact
        spreads = [ask - bid for ask, bid in zip(ask_prices, bid_prices)]
        avg_spread = np.mean(spreads[-10:])  # Last 10 periods
        return avg_spread
    
    def _estimate_market_depth(self, volumes: List[float]) -> float:
        """Estimate market depth from volume patterns"""
        if not volumes:
            return 0.0
        
        # Use recent volume as depth proxy
        recent_volumes = volumes[-10:] if len(volumes) >= 10 else volumes
        return np.mean(recent_volumes)
    
    def _classify_volatility_regime(self, bid_prices: List[float], ask_prices: List[float]) -> str:
        """Classify current volatility regime"""
        if len(bid_prices) < self.volatility_window:
            return "normal"
        
        # Calculate realized volatility
        mid_prices = [(b + a) / 2 for b, a in zip(bid_prices, ask_prices)]
        returns = np.diff(np.log(mid_prices[-self.volatility_window:]))
        realized_vol = np.std(returns) * np.sqrt(252)  # Annualized
        
        # Historical volatility for comparison
        if len(mid_prices) >= self.volatility_window * 2:
            hist_returns = np.diff(np.log(mid_prices[-self.volatility_window*2:-self.volatility_window]))
            hist_vol = np.std(hist_returns) * np.sqrt(252)
        else:
            hist_vol = realized_vol
        
        # Classify regime
        vol_ratio = realized_vol / hist_vol if hist_vol > 0 else 1.0
        
        if vol_ratio > 1.5:
            return "high_volatility"
        elif vol_ratio < 0.7:
            return "low_volatility"
        else:
            return "normal_volatility"
    
    def _calculate_liquidity_score(self, spread: float, volumes: List[float], market_depth: float) -> float:
        """Calculate liquidity score (0-1)"""
        if not volumes or spread <= 0:
            return 0.5
        
        # Normalize spread (lower is better)
        spread_score = max(0, 1 - (spread * 10000))  # Scale for typical FX spreads
        
        # Normalize volume (higher is better)
        avg_volume = np.mean(volumes[-10:]) if len(volumes) >= 10 else np.mean(volumes)
        volume_score = min(1, avg_volume / 1000)  # Scale for typical volumes
        
        # Combine scores
        liquidity_score = (spread_score * 0.6 + volume_score * 0.4)
        return min(1.0, max(0.0, liquidity_score))
    
    def _default_metrics(self) -> OrderFlowMetrics:
        """Return default metrics when analysis fails"""
        return OrderFlowMetrics(
            bid_ask_spread=0.0001,
            mid_price=1.0,
            volume_weighted_price=1.0,
            order_imbalance=0.0,
            price_impact=0.0001,
            market_depth=100.0,
            volatility_regime="normal_volatility",
            liquidity_score=0.5
        )
    
    async def detect_market_regime(self, 
                                 symbol: str,
                                 prices: List[float],
                                 volumes: List[float],
                                 timestamps: List[datetime]) -> MarketRegime:
        """
        Detect current market regime using multiple indicators
        
        Args:
            symbol: Trading instrument
            prices: Historical prices
            volumes: Historical volumes
            timestamps: Price timestamps
            
        Returns:
            MarketRegime classification
        """
        try:
            if len(prices) < 50:
                return MarketRegime("ranging", 0.5, 0.5, 0.0, "normal")
            
            # Calculate trend strength
            trend_strength = self._calculate_trend_strength(prices)
            
            # Calculate volatility percentile
            volatility_percentile = self._calculate_volatility_percentile(prices)
            
            # Analyze volume profile
            volume_profile = self._analyze_volume_profile(volumes)
            
            # Regime classification logic
            if trend_strength > 0.7 and volatility_percentile > 0.8:
                regime_type = "breakout"
                confidence = min(0.95, (trend_strength + volatility_percentile) / 2)
            elif trend_strength > 0.6:
                regime_type = "trending"
                confidence = trend_strength
            elif volatility_percentile > 0.8:
                regime_type = "volatile"
                confidence = volatility_percentile
            else:
                regime_type = "ranging"
                confidence = 1 - max(trend_strength, volatility_percentile)
            
            return MarketRegime(
                regime_type=regime_type,
                confidence=confidence,
                volatility_percentile=volatility_percentile,
                trend_strength=trend_strength,
                volume_profile=volume_profile
            )
            
        except Exception as e:
            logger.error(f"Market regime detection failed for {symbol}: {e}")
            return MarketRegime("ranging", 0.5, 0.5, 0.0, "normal")
    
    def _calculate_trend_strength(self, prices: List[float]) -> float:
        """Calculate trend strength using linear regression"""
        if len(prices) < 20:
            return 0.0
        
        x = np.arange(len(prices))
        y = np.array(prices)
        
        # Linear regression
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        
        # Calculate R-squared
        y_pred = slope * x + coeffs[1]
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # Normalize slope for trend strength
        price_range = max(prices) - min(prices)
        normalized_slope = abs(slope) / (price_range / len(prices)) if price_range > 0 else 0
        
        return min(1.0, normalized_slope * r_squared)
    
    def _calculate_volatility_percentile(self, prices: List[float]) -> float:
        """Calculate current volatility percentile vs historical"""
        if len(prices) < 20:
            return 0.5
        
        # Calculate rolling volatility
        returns = np.diff(np.log(prices))
        window = min(20, len(returns))
        
        if window < 5:
            return 0.5
        
        # Current volatility
        current_vol = np.std(returns[-window:])
        
        # Historical volatilities for comparison
        if len(returns) >= window * 2:
            hist_vols = []
            for i in range(window, len(returns) - window + 1):
                hist_vols.append(np.std(returns[i-window:i]))
            
            # Calculate percentile
            percentile = np.percentile(hist_vols, current_vol)
            return percentile / 100.0
        else:
            return 0.5
    
    def _analyze_volume_profile(self, volumes: List[float]) -> str:
        """Analyze volume profile"""
        if not volumes or len(volumes) < 10:
            return "normal"
        
        recent_avg = np.mean(volumes[-10:])
        historical_avg = np.mean(volumes[:-10]) if len(volumes) > 10 else recent_avg
        
        if historical_avg == 0:
            return "normal"
        
        ratio = recent_avg / historical_avg
        
        if ratio > 1.5:
            return "high"
        elif ratio < 0.7:
            return "low"
        else:
            return "normal"

class SmartOrderRouter:
    """
    Intelligent order routing based on market microstructure
    """
    
    def __init__(self, microstructure_analyzer: MarketMicrostructureAnalyzer):
        self.analyzer = microstructure_analyzer
        self.execution_strategies = {
            "aggressive": self._aggressive_execution,
            "passive": self._passive_execution,
            "adaptive": self._adaptive_execution
        }
    
    async def route_order(self, 
                         symbol: str,
                         side: str,
                         size: float,
                         urgency: str = "normal") -> Dict[str, Any]:
        """
        Route order based on market conditions
        
        Args:
            symbol: Trading instrument
            side: "BUY" or "SELL"
            size: Order size
            urgency: "low", "normal", "high", "urgent"
            
        Returns:
            Order routing recommendations
        """
        try:
            # Get market microstructure data
            # Note: In practice, you'd get this from your data provider
            order_flow = await self.analyzer.analyze_order_flow(symbol, [], [], [], [])
            market_regime = await self.analyzer.detect_market_regime(symbol, [], [], [])
            
            # Select execution strategy
            strategy = self._select_execution_strategy(order_flow, market_regime, urgency)
            
            # Generate routing recommendations
            routing = await self.execution_strategies[strategy](symbol, side, size, order_flow, market_regime)
            
            return {
                "strategy": strategy,
                "routing": routing,
                "market_conditions": {
                    "liquidity_score": order_flow.liquidity_score,
                    "volatility_regime": order_flow.volatility_regime,
                    "market_regime": market_regime.regime_type,
                    "confidence": market_regime.confidence
                }
            }
            
        except Exception as e:
            logger.error(f"Order routing failed for {symbol}: {e}")
            return {"strategy": "aggressive", "routing": {"type": "market", "reason": "fallback"}}
    
    def _select_execution_strategy(self, 
                                 order_flow: OrderFlowMetrics,
                                 market_regime: MarketRegime,
                                 urgency: str) -> str:
        """Select optimal execution strategy"""
        
        # High urgency always uses aggressive
        if urgency in ["high", "urgent"]:
            return "aggressive"
        
        # Low liquidity or high volatility -> aggressive
        if order_flow.liquidity_score < 0.3 or market_regime.regime_type == "volatile":
            return "aggressive"
        
        # Trending markets with good liquidity -> adaptive
        if market_regime.regime_type == "trending" and order_flow.liquidity_score > 0.7:
            return "adaptive"
        
        # Ranging markets with good liquidity -> passive
        if market_regime.regime_type == "ranging" and order_flow.liquidity_score > 0.6:
            return "passive"
        
        # Default to adaptive
        return "adaptive"
    
    async def _aggressive_execution(self, symbol: str, side: str, size: float, 
                                  order_flow: OrderFlowMetrics, market_regime: MarketRegime) -> Dict[str, Any]:
        """Aggressive execution strategy"""
        return {
            "type": "market",
            "time_in_force": "IOC",  # Immediate or Cancel
            "max_slippage": 0.0005,  # 5 pips max slippage
            "reason": "High urgency or poor market conditions"
        }
    
    async def _passive_execution(self, symbol: str, side: str, size: float,
                               order_flow: OrderFlowMetrics, market_regime: MarketRegime) -> Dict[str, Any]:
        """Passive execution strategy"""
        # Calculate limit price with spread consideration
        if side.upper() == "BUY":
            limit_price = order_flow.mid_price + (order_flow.bid_ask_spread * 0.3)
        else:
            limit_price = order_flow.mid_price - (order_flow.bid_ask_spread * 0.3)
        
        return {
            "type": "limit",
            "price": limit_price,
            "time_in_force": "GTC",  # Good Till Cancelled
            "reason": "Good market conditions allow for better pricing"
        }
    
    async def _adaptive_execution(self, symbol: str, side: str, size: float,
                                order_flow: OrderFlowMetrics, market_regime: MarketRegime) -> Dict[str, Any]:
        """Adaptive execution strategy"""
        # Start with limit order, fallback to market if needed
        if side.upper() == "BUY":
            limit_price = order_flow.mid_price + (order_flow.bid_ask_spread * 0.5)
        else:
            limit_price = order_flow.mid_price - (order_flow.bid_ask_spread * 0.5)
        
        return {
            "type": "limit_with_fallback",
            "limit_price": limit_price,
            "fallback_type": "market",
            "time_in_force": "IOC",
            "max_wait_seconds": 30,
            "reason": "Balanced approach for optimal execution"
        }
