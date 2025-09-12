"""
REAL-TIME P&L ATTRIBUTION SYSTEM
Institutional-grade profit and loss attribution with real-time updates
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np
from collections import defaultdict

logger = logging.getLogger(__name__)

class AttributionType(Enum):
    SYMBOL = "symbol"
    STRATEGY = "strategy"
    TIMEFRAME = "timeframe"
    RISK_FACTOR = "risk_factor"
    MARKET_REGIME = "market_regime"
    VOLATILITY = "volatility"
    CORRELATION = "correlation"

@dataclass
class PnLAttribution:
    """P&L attribution breakdown"""
    timestamp: str
    position_id: str
    symbol: str
    strategy: str
    timeframe: str
    total_pnl: float
    attribution_breakdown: Dict[str, float]
    market_regime: str
    volatility_state: str
    correlation_impact: float
    risk_factor_impact: float

@dataclass
class AttributionMetrics:
    """Attribution performance metrics"""
    period_start: str
    period_end: str
    total_pnl: float
    attribution_by_symbol: Dict[str, float]
    attribution_by_strategy: Dict[str, float]
    attribution_by_timeframe: Dict[str, float]
    attribution_by_risk_factor: Dict[str, float]
    attribution_by_market_regime: Dict[str, float]
    top_performers: List[Dict[str, Any]]
    bottom_performers: List[Dict[str, Any]]
    risk_adjusted_returns: Dict[str, float]

class RealTimePnLAttribution:
    """Real-time P&L attribution system"""
    
    def __init__(self):
        self.attributions = []
        self.position_attributions = {}
        self.aggregated_metrics = {}
        self._lock = asyncio.Lock()
        self._last_update = datetime.now(timezone.utc)
        
    async def calculate_position_attribution(self, 
                                           position_id: str,
                                           symbol: str,
                                           strategy: str,
                                           timeframe: str,
                                           entry_price: float,
                                           current_price: float,
                                           size: float,
                                           market_regime: str = "normal",
                                           volatility_state: str = "normal",
                                           correlation_data: Dict[str, float] = None) -> PnLAttribution:
        """Calculate real-time P&L attribution for a position"""
        
        # Calculate base P&L
        pnl = (current_price - entry_price) * size
        
        # Calculate attribution breakdown
        attribution_breakdown = await self._calculate_attribution_breakdown(
            symbol, strategy, timeframe, pnl, market_regime, 
            volatility_state, correlation_data
        )
        
        # Calculate correlation impact
        correlation_impact = self._calculate_correlation_impact(
            symbol, correlation_data or {}
        )
        
        # Calculate risk factor impact
        risk_factor_impact = self._calculate_risk_factor_impact(
            symbol, strategy, timeframe, pnl
        )
        
        attribution = PnLAttribution(
            timestamp=datetime.now(timezone.utc).isoformat(),
            position_id=position_id,
            symbol=symbol,
            strategy=strategy,
            timeframe=timeframe,
            total_pnl=pnl,
            attribution_breakdown=attribution_breakdown,
            market_regime=market_regime,
            volatility_state=volatility_state,
            correlation_impact=correlation_impact,
            risk_factor_impact=risk_factor_impact
        )
        
        # Store attribution
        async with self._lock:
            self.position_attributions[position_id] = attribution
            self.attributions.append(attribution)
            
            # Keep only last 1000 attributions for memory efficiency
            if len(self.attributions) > 1000:
                self.attributions = self.attributions[-1000:]
        
        return attribution
    
    async def _calculate_attribution_breakdown(self, 
                                             symbol: str,
                                             strategy: str,
                                             timeframe: str,
                                             pnl: float,
                                             market_regime: str,
                                             volatility_state: str,
                                             correlation_data: Dict[str, float]) -> Dict[str, float]:
        """Calculate detailed attribution breakdown"""
        
        breakdown = {}
        
        # Symbol attribution (base factor)
        symbol_factor = self._get_symbol_factor(symbol)
        breakdown["symbol"] = pnl * symbol_factor
        
        # Strategy attribution
        strategy_factor = self._get_strategy_factor(strategy)
        breakdown["strategy"] = pnl * strategy_factor
        
        # Timeframe attribution
        timeframe_factor = self._get_timeframe_factor(timeframe)
        breakdown["timeframe"] = pnl * timeframe_factor
        
        # Market regime attribution
        regime_factor = self._get_market_regime_factor(market_regime)
        breakdown["market_regime"] = pnl * regime_factor
        
        # Volatility attribution
        volatility_factor = self._get_volatility_factor(volatility_state)
        breakdown["volatility"] = pnl * volatility_factor
        
        # Correlation attribution
        if correlation_data:
            correlation_factor = self._calculate_correlation_factor(correlation_data)
            breakdown["correlation"] = pnl * correlation_factor
        
        # Risk factor attribution
        risk_factor = self._calculate_risk_factor(symbol, strategy, timeframe)
        breakdown["risk_factor"] = pnl * risk_factor
        
        # Normalize to ensure total adds up to actual P&L
        total_attribution = sum(breakdown.values())
        if total_attribution != 0:
            normalization_factor = pnl / total_attribution
            breakdown = {k: v * normalization_factor for k, v in breakdown.items()}
        
        return breakdown
    
    def _get_symbol_factor(self, symbol: str) -> float:
        """Get symbol-specific attribution factor"""
        # Major pairs typically have different risk profiles
        major_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "AUD_USD", "USD_CAD"]
        if symbol in major_pairs:
            return 0.4  # 40% attribution to symbol for major pairs
        else:
            return 0.3  # 30% for minor pairs
    
    def _get_strategy_factor(self, strategy: str) -> float:
        """Get strategy-specific attribution factor"""
        strategy_factors = {
            "momentum": 0.3,
            "mean_reversion": 0.25,
            "breakout": 0.35,
            "scalping": 0.2,
            "swing": 0.4
        }
        return strategy_factors.get(strategy, 0.3)
    
    def _get_timeframe_factor(self, timeframe: str) -> float:
        """Get timeframe-specific attribution factor"""
        timeframe_factors = {
            "M1": 0.1,
            "M5": 0.15,
            "M15": 0.2,
            "M30": 0.25,
            "H1": 0.3,
            "H4": 0.35,
            "D1": 0.4
        }
        return timeframe_factors.get(timeframe, 0.25)
    
    def _get_market_regime_factor(self, market_regime: str) -> float:
        """Get market regime attribution factor"""
        regime_factors = {
            "trending": 0.4,
            "ranging": 0.2,
            "volatile": 0.3,
            "quiet": 0.1
        }
        return regime_factors.get(market_regime, 0.25)
    
    def _get_volatility_factor(self, volatility_state: str) -> float:
        """Get volatility state attribution factor"""
        volatility_factors = {
            "high": 0.3,
            "normal": 0.2,
            "low": 0.1
        }
        return volatility_factors.get(volatility_state, 0.2)
    
    def _calculate_correlation_factor(self, correlation_data: Dict[str, float]) -> float:
        """Calculate correlation impact factor"""
        if not correlation_data:
            return 0.0
        
        # Average correlation impact
        avg_correlation = np.mean(list(correlation_data.values()))
        return min(0.2, max(0.0, avg_correlation * 0.5))
    
    def _calculate_correlation_impact(self, symbol: str, correlation_data: Dict[str, float]) -> float:
        """Calculate correlation impact on P&L"""
        if not correlation_data:
            return 0.0
        
        # Calculate weighted correlation impact
        weights = {
            "EUR_USD": 0.3,
            "GBP_USD": 0.25,
            "USD_JPY": 0.2,
            "AUD_USD": 0.15,
            "USD_CAD": 0.1
        }
        
        weight = weights.get(symbol, 0.1)
        avg_correlation = np.mean(list(correlation_data.values()))
        return avg_correlation * weight
    
    def _calculate_risk_factor_impact(self, symbol: str, strategy: str, timeframe: str, pnl: float) -> float:
        """Calculate risk factor impact on P&L"""
        # Risk factor based on symbol volatility, strategy risk, and timeframe
        symbol_risk = self._get_symbol_risk(symbol)
        strategy_risk = self._get_strategy_risk(strategy)
        timeframe_risk = self._get_timeframe_risk(timeframe)
        
        combined_risk = (symbol_risk + strategy_risk + timeframe_risk) / 3
        return pnl * combined_risk * 0.1  # 10% of P&L attributed to risk factors
    
    def _get_symbol_risk(self, symbol: str) -> float:
        """Get symbol-specific risk factor"""
        risk_factors = {
            "EUR_USD": 0.2,
            "GBP_USD": 0.4,
            "USD_JPY": 0.3,
            "AUD_USD": 0.35,
            "USD_CAD": 0.25,
            "NZD_USD": 0.45
        }
        return risk_factors.get(symbol, 0.3)
    
    def _get_strategy_risk(self, strategy: str) -> float:
        """Get strategy-specific risk factor"""
        risk_factors = {
            "momentum": 0.4,
            "mean_reversion": 0.3,
            "breakout": 0.5,
            "scalping": 0.2,
            "swing": 0.6
        }
        return risk_factors.get(strategy, 0.4)
    
    def _get_timeframe_risk(self, timeframe: str) -> float:
        """Get timeframe-specific risk factor"""
        risk_factors = {
            "M1": 0.1,
            "M5": 0.2,
            "M15": 0.3,
            "M30": 0.4,
            "H1": 0.5,
            "H4": 0.6,
            "D1": 0.7
        }
        return risk_factors.get(timeframe, 0.4)
    
    def _calculate_risk_factor(self, symbol: str, strategy: str, timeframe: str) -> float:
        """Calculate overall risk factor"""
        symbol_risk = self._get_symbol_risk(symbol)
        strategy_risk = self._get_strategy_risk(strategy)
        timeframe_risk = self._get_timeframe_risk(timeframe)
        
        return (symbol_risk + strategy_risk + timeframe_risk) / 3
    
    async def get_aggregated_attribution(self, 
                                       period_hours: int = 24) -> AttributionMetrics:
        """Get aggregated attribution metrics for specified period"""
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=period_hours)
        
        # Filter attributions by time period
        recent_attributions = [
            attr for attr in self.attributions 
            if datetime.fromisoformat(attr.timestamp) >= cutoff_time
        ]
        
        if not recent_attributions:
            return AttributionMetrics(
                period_start=cutoff_time.isoformat(),
                period_end=datetime.now(timezone.utc).isoformat(),
                total_pnl=0.0,
                attribution_by_symbol={},
                attribution_by_strategy={},
                attribution_by_timeframe={},
                attribution_by_risk_factor={},
                attribution_by_market_regime={},
                top_performers=[],
                bottom_performers=[],
                risk_adjusted_returns={}
            )
        
        # Calculate aggregated metrics
        total_pnl = sum(attr.total_pnl for attr in recent_attributions)
        
        # Attribution by category
        attribution_by_symbol = defaultdict(float)
        attribution_by_strategy = defaultdict(float)
        attribution_by_timeframe = defaultdict(float)
        attribution_by_risk_factor = defaultdict(float)
        attribution_by_market_regime = defaultdict(float)
        
        for attr in recent_attributions:
            attribution_by_symbol[attr.symbol] += attr.attribution_breakdown.get("symbol", 0)
            attribution_by_strategy[attr.strategy] += attr.attribution_breakdown.get("strategy", 0)
            attribution_by_timeframe[attr.timeframe] += attr.attribution_breakdown.get("timeframe", 0)
            attribution_by_risk_factor[attr.symbol] += attr.risk_factor_impact
            attribution_by_market_regime[attr.market_regime] += attr.attribution_breakdown.get("market_regime", 0)
        
        # Top and bottom performers
        symbol_performance = [(symbol, pnl) for symbol, pnl in attribution_by_symbol.items()]
        symbol_performance.sort(key=lambda x: x[1], reverse=True)
        
        top_performers = [
            {"symbol": symbol, "pnl": pnl, "percentage": (pnl / total_pnl * 100) if total_pnl != 0 else 0}
            for symbol, pnl in symbol_performance[:5]
        ]
        
        bottom_performers = [
            {"symbol": symbol, "pnl": pnl, "percentage": (pnl / total_pnl * 100) if total_pnl != 0 else 0}
            for symbol, pnl in symbol_performance[-5:]
        ]
        
        # Risk-adjusted returns
        risk_adjusted_returns = {}
        for symbol in attribution_by_symbol:
            symbol_attributions = [attr for attr in recent_attributions if attr.symbol == symbol]
            if symbol_attributions:
                total_symbol_pnl = sum(attr.total_pnl for attr in symbol_attributions)
                avg_risk = np.mean([attr.risk_factor_impact for attr in symbol_attributions])
                risk_adjusted_returns[symbol] = total_symbol_pnl / max(abs(avg_risk), 0.01)
        
        return AttributionMetrics(
            period_start=cutoff_time.isoformat(),
            period_end=datetime.now(timezone.utc).isoformat(),
            total_pnl=total_pnl,
            attribution_by_symbol=dict(attribution_by_symbol),
            attribution_by_strategy=dict(attribution_by_strategy),
            attribution_by_timeframe=dict(attribution_by_timeframe),
            attribution_by_risk_factor=dict(attribution_by_risk_factor),
            attribution_by_market_regime=dict(attribution_by_market_regime),
            top_performers=top_performers,
            bottom_performers=bottom_performers,
            risk_adjusted_returns=risk_adjusted_returns
        )
    
    async def get_position_attribution(self, position_id: str) -> Optional[PnLAttribution]:
        """Get attribution for specific position"""
        return self.position_attributions.get(position_id)
    
    async def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get real-time attribution metrics"""
        async with self._lock:
            current_time = datetime.now(timezone.utc)
            
            # Last hour metrics
            last_hour = await self.get_aggregated_attribution(1)
            
            # Last 24 hours metrics
            last_24h = await self.get_aggregated_attribution(24)
            
            return {
                "timestamp": current_time.isoformat(),
                "active_positions": len(self.position_attributions),
                "total_attributions": len(self.attributions),
                "last_hour": asdict(last_hour),
                "last_24h": asdict(last_24h),
                "last_update": self._last_update.isoformat()
            }

# Global P&L attribution instance
pnl_attribution = RealTimePnLAttribution()

# Convenience functions
async def calculate_position_attribution(position_id: str, symbol: str, strategy: str, 
                                       timeframe: str, entry_price: float, current_price: float, 
                                       size: float, **kwargs) -> PnLAttribution:
    """Calculate position attribution"""
    return await pnl_attribution.calculate_position_attribution(
        position_id, symbol, strategy, timeframe, entry_price, 
        current_price, size, **kwargs
    )

async def get_attribution_metrics(period_hours: int = 24) -> AttributionMetrics:
    """Get attribution metrics"""
    return await pnl_attribution.get_aggregated_attribution(period_hours)

async def get_real_time_attribution() -> Dict[str, Any]:
    """Get real-time attribution data"""
    return await pnl_attribution.get_real_time_metrics()
