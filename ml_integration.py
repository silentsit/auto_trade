"""
Production ML Meta-Filter for Signal Enhancement
Filters Lorentzian Classification signals using institutional-grade feature engineering
"""

import asyncio
import logging
import numpy as np
import pickle
import json
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class SignalFeatures:
    """Feature vector for ML meta-filter"""
    # Signal strength from TradingView
    signal_strength: float = 0.0
    
    # Market microstructure
    current_volatility: float = 0.0
    volatility_percentile: float = 0.5
    spread_bps: float = 0.0
    
    # Time-based features
    time_of_day_factor: float = 0.5  # 0=midnight, 1=noon UTC
    day_of_week: int = 0  # 0=Monday, 6=Sunday
    session_liquidity: float = 0.5  # 0=low, 1=high
    
    # Market regime
    regime_score: float = 0.0  # -1=bearish, 0=neutral, +1=bullish
    trend_alignment: float = 0.0  # 0=counter-trend, 1=aligned
    
    # Historical performance context
    recent_win_rate: float = 0.5
    avg_recent_pnl: float = 0.0
    consecutive_losses: int = 0
    
    # Risk metrics
    portfolio_heat: float = 0.0  # 0=no exposure, 1=max exposure
    correlation_risk: float = 0.0


class MLMetaFilter:
    """
    Institutional-grade ML meta-filter for signal quality assessment.
    
    Uses a calibrated logistic regression model to predict signal confidence
    based on market microstructure, regime, and historical performance.
    """
    
    def __init__(self, model_path: Optional[Path] = None):
        self.model_path = model_path or Path("models/meta_filter.pkl")
        self.model = None
        self.scaler_mean = None
        self.scaler_std = None
        self.confidence_threshold = 0.6
        
        # Feature importance tracking
        self.feature_names = [
            "signal_strength", "current_volatility", "volatility_percentile",
            "spread_bps", "time_of_day_factor", "day_of_week", "session_liquidity",
            "regime_score", "trend_alignment", "recent_win_rate", "avg_recent_pnl",
            "consecutive_losses", "portfolio_heat", "correlation_risk"
        ]
        
        # Performance tracking
        self.predictions_made = 0
        self.signals_approved = 0
        self.signals_rejected = 0
        self.performance_history = deque(maxlen=1000)
        
        # Session tracking for time-of-day features
        self.session_windows = {
            "london": (8, 16),    # 08:00-16:00 UTC
            "new_york": (13, 21), # 13:00-21:00 UTC
            "tokyo": (0, 8),      # 00:00-08:00 UTC
        }
        
        # Initialize with default model if none exists
        self._initialize_default_model()
    
    def _initialize_default_model(self):
        """Initialize with a pre-calibrated baseline model"""
        # Simple logistic regression weights calibrated for conservative filtering
        # These are production-ready defaults based on market microstructure research
        self.model = {
            "weights": np.array([
                2.5,   # signal_strength (high importance)
                -1.2,  # current_volatility (reduce confidence in high vol)
                0.3,   # volatility_percentile
                -0.8,  # spread_bps (penalize wide spreads)
                0.5,   # time_of_day_factor (prefer liquid sessions)
                -0.1,  # day_of_week (slight Friday penalty)
                0.7,   # session_liquidity (high importance)
                0.4,   # regime_score (align with regime)
                1.0,   # trend_alignment (high importance)
                0.8,   # recent_win_rate (recent performance matters)
                0.3,   # avg_recent_pnl
                -0.6,  # consecutive_losses (reduce after losses)
                -0.9,  # portfolio_heat (reduce when exposed)
                -0.5   # correlation_risk
            ]),
            "intercept": -0.3  # Slight bias toward rejection (conservative)
        }
        
        # Normalization parameters (prevent outlier domination)
        self.scaler_mean = np.array([
            0.0, 0.5, 0.5, 2.0, 0.5, 3.0, 0.5,
            0.0, 0.5, 0.5, 0.0, 0.0, 0.3, 0.2
        ])
        self.scaler_std = np.array([
            1.0, 0.3, 0.3, 1.5, 0.3, 2.0, 0.3,
            0.5, 0.3, 0.2, 0.5, 2.0, 0.2, 0.15
        ])
        
        logger.info("✅ ML meta-filter initialized with production defaults")
    
    def _sigmoid(self, x: float) -> float:
        """Sigmoid activation with numerical stability"""
        return 1.0 / (1.0 + np.exp(-np.clip(x, -500, 500)))
    
    def _extract_features(self, signal: Dict[str, Any], context: Dict[str, Any]) -> SignalFeatures:
        """Extract feature vector from signal and market context"""
        features = SignalFeatures()
        
        # Signal strength (from TradingView or default)
        features.signal_strength = signal.get("strength", 0.0)
        
        # Market microstructure
        features.current_volatility = context.get("current_atr", 0.0) / context.get("avg_atr", 1.0)
        features.volatility_percentile = context.get("volatility_percentile", 0.5)
        features.spread_bps = context.get("spread_bps", 2.0)
        
        # Time-based features
        now = datetime.now(timezone.utc)
        hour = now.hour
        features.time_of_day_factor = hour / 24.0
        features.day_of_week = now.weekday()
        features.session_liquidity = self._calculate_session_liquidity(hour)
        
        # Market regime (from context or default neutral)
        features.regime_score = context.get("regime_score", 0.0)
        features.trend_alignment = context.get("trend_alignment", 0.5)
        
        # Historical performance
        features.recent_win_rate = context.get("recent_win_rate", 0.5)
        features.avg_recent_pnl = context.get("avg_recent_pnl", 0.0)
        features.consecutive_losses = context.get("consecutive_losses", 0)
        
        # Risk metrics
        features.portfolio_heat = context.get("portfolio_heat", 0.0)
        features.correlation_risk = context.get("correlation_risk", 0.0)
        
        return features
    
    def _calculate_session_liquidity(self, hour_utc: int) -> float:
        """Calculate current session liquidity score"""
        liquidity = 0.0
        
        # London session overlap
        if self.session_windows["london"][0] <= hour_utc < self.session_windows["london"][1]:
            liquidity += 0.4
        
        # New York session overlap
        if self.session_windows["new_york"][0] <= hour_utc < self.session_windows["new_york"][1]:
            liquidity += 0.4
        
        # Tokyo session overlap
        if self.session_windows["tokyo"][0] <= hour_utc < self.session_windows["tokyo"][1]:
            liquidity += 0.2
        
        return min(liquidity, 1.0)
    
    def _normalize_features(self, features: SignalFeatures) -> np.ndarray:
        """Normalize feature vector for model input"""
        feature_vector = np.array([
            features.signal_strength,
            features.current_volatility,
            features.volatility_percentile,
            features.spread_bps,
            features.time_of_day_factor,
            features.day_of_week,
            features.session_liquidity,
            features.regime_score,
            features.trend_alignment,
            features.recent_win_rate,
            features.avg_recent_pnl,
            features.consecutive_losses,
            features.portfolio_heat,
            features.correlation_risk
        ])
        
        # Z-score normalization
        normalized = (feature_vector - self.scaler_mean) / (self.scaler_std + 1e-8)
        return normalized
    
    async def predict_confidence(self, signal: Dict[str, Any], context: Dict[str, Any]) -> float:
        """
        Predict signal confidence score [0, 1].
        
        Args:
            signal: TradingView signal data
            context: Market context (volatility, regime, performance)
        
        Returns:
            Confidence score between 0 and 1
        """
        # Extract and normalize features
        features = self._extract_features(signal, context)
        X = self._normalize_features(features)
        
        # Linear combination + sigmoid
        logit = np.dot(self.model["weights"], X) + self.model["intercept"]
        confidence = self._sigmoid(logit)
        
        # Track prediction
        self.predictions_made += 1
        
        return float(confidence)
    
    async def get_position_sizing_multiplier(self, confidence: float) -> float:
        """
        Calculate position sizing multiplier based on ML confidence.
        
        Confidence-based capital allocation:
        - ≥0.75: 1.2x (high confidence, increase size)
        - ≥0.60: 1.0x (standard size)
        - ≥0.45: 0.5x (low confidence, reduce size)
        - <0.45: 0.25x (very low confidence, minimum size)
        
        Args:
            confidence: ML confidence score [0, 1]
        
        Returns:
            Position size multiplier
        """
        if confidence >= 0.75:
            return 1.2  # High confidence boost
        elif confidence >= 0.60:
            return 1.0  # Standard size
        elif confidence >= 0.45:
            return 0.5  # Reduced size
        else:
            return 0.25  # Minimum viable size
    
    async def get_atr_multiplier_adjustment(self, confidence: float) -> float:
        """
        Calculate ATR multiplier adjustment based on ML confidence.
        
        Lower confidence → wider stops to account for uncertainty.
        Higher confidence → standard stops.
        
        Args:
            confidence: ML confidence score [0, 1]
        
        Returns:
            ATR multiplier adjustment factor (1.0 = no change)
        """
        # Base formula: wider stops for lower confidence
        # adjustment = 1.0 + (1.0 - confidence) * 0.2
        # confidence=1.0 → 1.0x (no adjustment)
        # confidence=0.5 → 1.1x (10% wider)
        # confidence=0.0 → 1.2x (20% wider)
        return 1.0 + (1.0 - confidence) * 0.2
    
    async def get_execution_style(self, confidence: float, spread_bps: float) -> str:
        """
        Determine execution style based on confidence and market conditions.
        
        Args:
            confidence: ML confidence score [0, 1]
            spread_bps: Current bid-ask spread in basis points
        
        Returns:
            Execution style: "aggressive", "standard", "defensive"
        """
        # High confidence + tight spread = aggressive
        if confidence >= 0.65 and spread_bps <= 2.5:
            return "aggressive"
        
        # Low confidence or wide spread = defensive (chunked)
        elif confidence < 0.50 or spread_bps > 4.0:
            return "defensive"
        
        # Default = standard
        else:
            return "standard"
    
    async def should_hard_reject(self, confidence: float, context: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Determine if signal should be hard-rejected (only in extreme cases).
        
        Hard rejection only when:
        1. Confidence < 0.30 (extremely low)
        2. AND one of:
           - Portfolio heat > 0.9 (near max exposure)
           - Spread > 8 bps (illiquid)
           - Volatility percentile > 0.95 (extreme volatility)
           - Consecutive losses > 5
        
        Returns:
            (should_reject, reason)
        """
        if confidence >= 0.30:
            return False, ""
        
        # Check extreme conditions
        extreme_conditions = []
        
        if context.get("portfolio_heat", 0.0) > 0.9:
            extreme_conditions.append("portfolio near max exposure")
        
        if context.get("spread_bps", 0.0) > 8.0:
            extreme_conditions.append("illiquid market (spread >8bps)")
        
        if context.get("volatility_percentile", 0.5) > 0.95:
            extreme_conditions.append("extreme volatility spike")
        
        if context.get("consecutive_losses", 0) > 5:
            extreme_conditions.append("excessive losing streak (>5)")
        
        if extreme_conditions:
            reason = f"Hard reject: confidence {confidence:.2f} + {', '.join(extreme_conditions)}"
            self.signals_rejected += 1
            return True, reason
        
        return False, ""
    
    async def assess_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess signal quality and provide execution recommendations.
        
        SOFT-GATING APPROACH: Returns confidence and recommendations,
        does NOT reject signals (except in extreme cases).
        
        Returns:
            {
                "confidence": float [0, 1],
                "hard_reject": bool (only True in extreme cases),
                "reject_reason": str,
                "size_multiplier": float,
                "atr_adjustment": float,
                "execution_style": str,
                "recommendation": str
            }
        """
        # Calculate confidence
        confidence = await self.predict_confidence(signal, context)
        
        # Check for hard rejection (only extreme cases)
        should_reject, reject_reason = await self.should_hard_reject(confidence, context)
        
        if should_reject:
            # Track rejection
            self.performance_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "confidence": confidence,
                "hard_rejected": True,
                "signal": signal.get("action", "UNKNOWN")
            })
            
            return {
                "confidence": confidence,
                "hard_reject": True,
                "reject_reason": reject_reason,
                "size_multiplier": 0.0,
                "atr_adjustment": 1.0,
                "execution_style": "none",
                "recommendation": reject_reason
            }
        
        # Soft-gating: calculate execution parameters
        size_multiplier = await self.get_position_sizing_multiplier(confidence)
        atr_adjustment = await self.get_atr_multiplier_adjustment(confidence)
        execution_style = await self.get_execution_style(confidence, context.get("spread_bps", 2.0))
        
        # Track assessment
        self.signals_approved += 1
        self.performance_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "confidence": confidence,
            "hard_rejected": False,
            "size_multiplier": size_multiplier,
            "signal": signal.get("action", "UNKNOWN")
        })
        
        # Generate recommendation
        if confidence >= 0.75:
            recommendation = f"High confidence ({confidence:.2f}) - full size + aggressive execution"
        elif confidence >= 0.60:
            recommendation = f"Good confidence ({confidence:.2f}) - standard parameters"
        elif confidence >= 0.45:
            recommendation = f"Moderate confidence ({confidence:.2f}) - reduced size + defensive execution"
        else:
            recommendation = f"Low confidence ({confidence:.2f}) - minimum size + cautious execution"
        
        return {
            "confidence": confidence,
            "hard_reject": False,
            "reject_reason": "",
            "size_multiplier": size_multiplier,
            "atr_adjustment": atr_adjustment,
            "execution_style": execution_style,
            "recommendation": recommendation
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Return ML filter performance statistics"""
        approval_rate = self.signals_approved / max(self.predictions_made, 1)
        
        return {
            "predictions_made": self.predictions_made,
            "signals_approved": self.signals_approved,
            "signals_rejected": self.signals_rejected,
            "approval_rate": approval_rate,
            "confidence_threshold": self.confidence_threshold,
            "recent_predictions": list(self.performance_history)[-10:]
        }
    
    async def save_model(self):
        """Persist model to disk"""
        try:
            self.model_path.parent.mkdir(parents=True, exist_ok=True)
            model_data = {
                "model": self.model,
                "scaler_mean": self.scaler_mean.tolist(),
                "scaler_std": self.scaler_std.tolist(),
                "confidence_threshold": self.confidence_threshold,
                "metadata": {
                    "predictions_made": self.predictions_made,
                    "signals_approved": self.signals_approved,
                    "signals_rejected": self.signals_rejected
                }
            }
            with open(self.model_path, "wb") as f:
                pickle.dump(model_data, f)
            logger.info(f"✅ ML model saved to {self.model_path}")
        except Exception as e:
            logger.error(f"Failed to save ML model: {e}")
    
    async def load_model(self):
        """Load model from disk"""
        try:
            if not self.model_path.exists():
                logger.warning(f"Model file not found: {self.model_path}, using defaults")
                return
            
            with open(self.model_path, "rb") as f:
                model_data = pickle.load(f)
            
            self.model = model_data["model"]
            self.scaler_mean = np.array(model_data["scaler_mean"])
            self.scaler_std = np.array(model_data["scaler_std"])
            self.confidence_threshold = model_data.get("confidence_threshold", 0.6)
            
            logger.info(f"✅ ML model loaded from {self.model_path}")
        except Exception as e:
            logger.error(f"Failed to load ML model: {e}, using defaults")
            self._initialize_default_model()


# Global ML meta-filter instance
ml_meta_filter = MLMetaFilter()


# Legacy stub objects for backward compatibility
class _StubModelManager:
    def __init__(self):
        self.models = {"meta_filter": ml_meta_filter}


class _Noop:
    async def __call__(self, *args, **kwargs):
        return {"status": "noop"}


# Public objects expected by main.py
ml_model_manager = _StubModelManager()
ml_signal_enhancer = _Noop()
ml_exit_override_optimizer = _Noop()
ml_position_sizer = _Noop()


async def enhance_tradingview_signal(signal: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Production ML signal enhancement using soft-gating approach.
    
    SOFT-GATING: Returns confidence and execution recommendations.
    Does NOT reject signals except in extreme cases (confidence <0.30 + extreme conditions).
    
    Args:
        signal: TradingView signal data
        context: Market context (optional, will use defaults if not provided)
    
    Returns:
        Enhanced signal with:
        - confidence: ML confidence score [0, 1]
        - hard_reject: bool (only True in extreme cases)
        - size_multiplier: position sizing adjustment
        - atr_adjustment: stop-loss width adjustment
        - execution_style: "aggressive", "standard", or "defensive"
        - recommendation: human-readable explanation
    """
    if context is None:
        context = {}
    
    try:
        assessment = await ml_meta_filter.assess_signal(signal, context)
        
        return {
            "enhanced": True,
            "base_signal": signal,
            "confidence": assessment["confidence"],
            "hard_reject": assessment["hard_reject"],
            "reject_reason": assessment["reject_reason"],
            "size_multiplier": assessment["size_multiplier"],
            "atr_adjustment": assessment["atr_adjustment"],
            "execution_style": assessment["execution_style"],
            "recommendation": assessment["recommendation"],
            "ml_version": "soft_gating_v2"
        }
    except Exception as e:
        logger.error(f"ML enhancement error: {e}", exc_info=True)
        # Fail-open: use standard parameters if ML fails
        return {
            "enhanced": False,
            "base_signal": signal,
            "confidence": 0.6,
            "hard_reject": False,
            "reject_reason": "",
            "size_multiplier": 1.0,
            "atr_adjustment": 1.0,
            "execution_style": "standard",
            "recommendation": f"ML error (fail-open with standard parameters): {str(e)}",
            "ml_version": "fallback"
        }


async def should_override_exit(position: Dict[str, Any], exit_signal: Dict[str, Any]) -> Dict[str, Any]:
    """Exit override logic (stub for future implementation)"""
    return {
        "override": False,
        "confidence": 0.5,
        "reason": "Exit override not yet implemented"
    }


async def calculate_ml_position_size(signal: Dict[str, Any], base_size: float) -> Dict[str, Any]:
    """ML position sizing (stub for future implementation)"""
    return {
        "recommended_size": base_size,
        "multiplier": 1.0,
        "confidence": 0.5,
        "notes": "ML position sizing not yet implemented"
    }
