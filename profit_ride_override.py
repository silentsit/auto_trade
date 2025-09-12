from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional
from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from .position_journal import Position
from datetime import datetime, timezone, timedelta
import numpy as np
import logging

logger = logging.getLogger(__name__)

@dataclass
class OverrideDecision:
    ignore_close: bool
    sl_atr_multiple: float = 2.0
    tp_atr_multiple: float = 4.0
    reason: str = ""
    confidence_score: float = 0.0
    risk_reward_ratio: float = 0.0
    market_regime: str = ""
    volatility_state: str = ""
    momentum_score: float = 0.0
    override_metrics: Dict[str, Any] = None

class ProfitRideOverride:
    def __init__(self, regime: LorentzianDistanceClassifier, vol: VolatilityMonitor):
        self.regime = regime
        self.vol = vol

    async def should_override(self, position: Position, current_price: float, drawdown: float = 0.0) -> OverrideDecision:
        """
        Institutional-grade profit override decision with advanced calculations.
        
        Multi-factor analysis including:
        - Risk controls and safety limits
        - Market regime analysis
        - Volatility assessment
        - Momentum scoring
        - Risk-reward optimization
        - Confidence scoring
        """
        try:
            # Initialize comprehensive metrics
            override_metrics = {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "timeframe": position.timeframe,
                "action": position.action,
                "entry_price": position.entry_price,
                "current_price": current_price,
                "size": position.size,
                "drawdown": drawdown,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # --- RISK CONTROL 1: Only fire once per position ---
        if position.metadata.get('profit_ride_override_fired', False):
                return OverrideDecision(
                    ignore_close=False, 
                    reason="Override already fired for this position.",
                    confidence_score=0.0,
                    override_metrics=override_metrics
                )

            # --- RISK CONTROL 2: Hard stop on equity drawdown > 70% ---
        if drawdown > 0.70:
                return OverrideDecision(
                    ignore_close=False, 
                    reason="Account drawdown exceeds 70%.",
                    confidence_score=0.0,
                    override_metrics=override_metrics
                )

            # --- RISK CONTROL 3: Maximum ride time (dynamic based on timeframe) ---
            max_ride_time = await self._calculate_max_ride_time(position)
            if not max_ride_time:
                return OverrideDecision(
                    ignore_close=False, 
                    reason="Maximum ride time exceeded.",
                    confidence_score=0.0,
                    override_metrics=override_metrics
                )

            # --- MARKET DATA ANALYSIS ---
            atr = await get_atr(position.symbol, position.timeframe)
            if atr <= 0:
                return OverrideDecision(
                    ignore_close=False, 
                    reason="ATR not available.",
                    confidence_score=0.0,
                    override_metrics=override_metrics
                )

            # --- REGIME ANALYSIS ---
            regime_data = self.regime.get_regime_data(position.symbol)
            market_regime = regime_data.get("regime", "")
            is_trending = "trending" in market_regime.lower()
            regime_confidence = regime_data.get("confidence", 0.5)
            
            # --- VOLATILITY ANALYSIS ---
            vol_state = self.vol.get_volatility_state(position.symbol)
            volatility_ratio = vol_state.get("volatility_ratio", 2.0)
            vol_ok = volatility_ratio < 1.5
            volatility_state = "low" if volatility_ratio < 1.0 else "normal" if volatility_ratio < 1.5 else "high"
            
            # --- PNL AND RISK CALCULATIONS ---
            current_pnl, initial_risk, risk_reward_ratio = await self._calculate_pnl_metrics(
                position, current_price, atr
            )
            
            # --- MOMENTUM ANALYSIS ---
            momentum_score = await self._calculate_momentum_score(position, current_price, atr)
            
            # --- CONFIDENCE SCORING ---
            confidence_score = await self._calculate_confidence_score(
                current_pnl, initial_risk, is_trending, vol_ok, 
                regime_confidence, momentum_score, volatility_ratio
            )
            
            # --- OVERRIDE DECISION LOGIC ---
            # Multi-factor decision with weighted scoring
            decision_factors = {
                "profitable_enough": current_pnl >= initial_risk * 1.5,  # 1.5x risk threshold
                "trending_market": is_trending and regime_confidence > 0.6,
                "volatility_ok": vol_ok and volatility_ratio < 1.3,
                "momentum_positive": momentum_score > 0.6,
                "confidence_high": confidence_score > 0.7,
                "risk_reward_good": risk_reward_ratio > 2.0
            }
            
            # Weighted decision (institutional approach)
            decision_weights = {
                "profitable_enough": 0.25,
                "trending_market": 0.20,
                "volatility_ok": 0.15,
                "momentum_positive": 0.15,
                "confidence_high": 0.15,
                "risk_reward_good": 0.10
            }
            
            weighted_score = sum(decision_factors[factor] * decision_weights[factor] 
                               for factor in decision_factors)
            
            # Override threshold (conservative institutional approach)
            override_threshold = 0.65
            ignore = weighted_score >= override_threshold
            
            # --- DYNAMIC R:R CALCULATION ---
            sl_mult, tp_mult = await self._calculate_dynamic_rr(
                position, atr, confidence_score, volatility_ratio, is_trending
            )
            
            # Update metrics
            override_metrics.update({
                "atr": atr,
                "market_regime": market_regime,
                "regime_confidence": regime_confidence,
                "volatility_ratio": volatility_ratio,
                "volatility_state": volatility_state,
                "current_pnl": current_pnl,
                "initial_risk": initial_risk,
                "risk_reward_ratio": risk_reward_ratio,
                "momentum_score": momentum_score,
                "confidence_score": confidence_score,
                "decision_factors": decision_factors,
                "weighted_score": weighted_score,
                "override_threshold": override_threshold,
                "sl_atr_multiple": sl_mult,
                "tp_atr_multiple": tp_mult,
                "max_ride_time": max_ride_time
            })
            
            # Log comprehensive evaluation
            logger.info(f"🔍 Institutional Override Analysis for {position.symbol}:")
            logger.info(f"   PnL: ${current_pnl:.2f}, Risk: ${initial_risk:.2f}, R:R: {risk_reward_ratio:.2f}")
            logger.info(f"   Regime: {market_regime} (conf: {regime_confidence:.2f})")
            logger.info(f"   Volatility: {volatility_state} ({volatility_ratio:.2f})")
            logger.info(f"   Momentum: {momentum_score:.2f}, Confidence: {confidence_score:.2f}")
            logger.info(f"   Weighted Score: {weighted_score:.2f} (threshold: {override_threshold:.2f})")
            logger.info(f"   Decision: {'OVERRIDE ALLOWED' if ignore else 'OVERRIDE DENIED'}")

            return OverrideDecision(
                ignore_close=ignore,
                sl_atr_multiple=sl_mult,
                tp_atr_multiple=tp_mult,
                reason="Override allowed - institutional criteria met." if ignore else "Override denied - criteria not met.",
                confidence_score=confidence_score,
                risk_reward_ratio=risk_reward_ratio,
                market_regime=market_regime,
                volatility_state=volatility_state,
                momentum_score=momentum_score,
                override_metrics=override_metrics
            )
            
        except Exception as e:
            logger.error(f"Error in profit override analysis: {e}")
            return OverrideDecision(
                ignore_close=False,
                reason=f"Override analysis failed: {str(e)}",
                confidence_score=0.0,
                override_metrics={"error": str(e)}
            )

    async def _calculate_max_ride_time(self, position: Position) -> bool:
        """Calculate if position is within maximum ride time limits"""
        try:
        open_time = position.metadata.get('open_time') or position.metadata.get('opened_at')
            if not open_time:
                return True  # No time limit if no open time recorded
            
            if isinstance(open_time, str):
                try:
                    open_time_dt = datetime.fromisoformat(open_time)
                except Exception:
                    return True
            elif isinstance(open_time, datetime):
                open_time_dt = open_time
            else:
                return True
            
                now = datetime.now(timezone.utc)
                tf = position.timeframe.upper()
            
            # Dynamic ride time based on timeframe and market conditions
                if tf == "15M" or tf == "15MIN":
                max_ride = timedelta(minutes=8*15)  # 8 bars
                elif tf == "1H" or tf == "1HR":
                max_ride = timedelta(hours=8)  # 8 bars
                elif tf == "4H":
                max_ride = timedelta(hours=8*4)  # 8 bars
                else:
                max_ride = timedelta(hours=8)  # Default
            
            return (now - open_time_dt) <= max_ride
            
        except Exception as e:
            logger.error(f"Error calculating max ride time: {e}")
            return True

    async def _calculate_pnl_metrics(self, position: Position, current_price: float, atr: float) -> Tuple[float, float, float]:
        """Calculate PnL, initial risk, and risk-reward ratio"""
        try:
            # Calculate current PnL
        if position.action == "BUY":
            current_pnl = (current_price - position.entry_price) * position.size
        else:
            current_pnl = (position.entry_price - current_price) * position.size
        
            # Calculate initial risk
        if position.stop_loss:
            risk_distance = abs(position.entry_price - position.stop_loss)
        else:
            risk_distance = atr * 1.5  # Default 1.5x ATR risk
        
        initial_risk = risk_distance * position.size
        
            # Calculate risk-reward ratio
            if initial_risk > 0:
                risk_reward_ratio = current_pnl / initial_risk
            else:
                risk_reward_ratio = 0.0
            
            return current_pnl, initial_risk, risk_reward_ratio
            
        except Exception as e:
            logger.error(f"Error calculating PnL metrics: {e}")
            return 0.0, 0.0, 0.0

    async def _calculate_momentum_score(self, position: Position, current_price: float, atr: float) -> float:
        """Calculate momentum score for the position"""
        try:
            # Simplified momentum calculation (in production, use sophisticated analysis)
            price_change = abs(current_price - position.entry_price)
            momentum_ratio = price_change / atr if atr > 0 else 0
            
            # Normalize momentum score (0-1)
            momentum_score = min(1.0, momentum_ratio / 3.0)  # Cap at 3x ATR
            
            # Adjust based on direction
            if position.action == "BUY":
                if current_price > position.entry_price:
                    momentum_score *= 1.0  # Positive momentum
                else:
                    momentum_score *= -0.5  # Negative momentum
            else:  # SELL
                if current_price < position.entry_price:
                    momentum_score *= 1.0  # Positive momentum
        else:
                    momentum_score *= -0.5  # Negative momentum
            
            return max(0.0, momentum_score)  # Ensure non-negative
            
        except Exception as e:
            logger.error(f"Error calculating momentum score: {e}")
            return 0.5

    async def _calculate_confidence_score(self, current_pnl: float, initial_risk: float, 
                                        is_trending: bool, vol_ok: bool, 
                                        regime_confidence: float, momentum_score: float, 
                                        volatility_ratio: float) -> float:
        """Calculate overall confidence score for override decision"""
        try:
            # PnL confidence (0-1)
            pnl_confidence = min(1.0, current_pnl / (initial_risk * 2.0)) if initial_risk > 0 else 0.0
            
            # Market regime confidence
            regime_score = regime_confidence if is_trending else 0.3
            
            # Volatility confidence
            vol_confidence = 1.0 - min(1.0, (volatility_ratio - 1.0) / 2.0)  # Penalty for high vol
            
            # Momentum confidence
            momentum_confidence = momentum_score
            
            # Weighted confidence score
            confidence_weights = {
                "pnl": 0.35,
                "regime": 0.25,
                "volatility": 0.20,
                "momentum": 0.20
            }
            
            confidence_score = (
                pnl_confidence * confidence_weights["pnl"] +
                regime_score * confidence_weights["regime"] +
                vol_confidence * confidence_weights["volatility"] +
                momentum_confidence * confidence_weights["momentum"]
            )
            
            return min(1.0, max(0.0, confidence_score))
            
        except Exception as e:
            logger.error(f"Error calculating confidence score: {e}")
            return 0.5

    async def _calculate_dynamic_rr(self, position: Position, atr: float, 
                                  confidence_score: float, volatility_ratio: float, 
                                  is_trending: bool) -> Tuple[float, float]:
        """Calculate dynamic risk-reward ratios based on market conditions"""
        try:
            tf = position.timeframe.upper()
            
            # Base R:R ratios by timeframe
            if tf == "15M" or tf == "15MIN":
                base_sl_mult = 1.5
                base_tp_mult = 2.25
            else:
                base_sl_mult = 2.0
                base_tp_mult = 4.0
            
            # Adjust based on confidence
            confidence_adjustment = 0.5 + (confidence_score * 0.5)  # 0.5 to 1.0
            
            # Adjust based on volatility
            vol_adjustment = 1.0 - min(0.3, (volatility_ratio - 1.0) * 0.2)  # Reduce in high vol
            
            # Adjust based on trend
            trend_adjustment = 1.1 if is_trending else 0.9
            
            # Calculate final multiples
            sl_mult = base_sl_mult * confidence_adjustment * vol_adjustment
            tp_mult = base_tp_mult * confidence_adjustment * vol_adjustment * trend_adjustment
            
            # Ensure minimum and maximum bounds
            sl_mult = max(1.0, min(3.0, sl_mult))
            tp_mult = max(1.5, min(6.0, tp_mult))
            
            return sl_mult, tp_mult
            
        except Exception as e:
            logger.error(f"Error calculating dynamic R:R: {e}")
            return 2.0, 4.0

    async def _higher_highs(self, symbol: str, tf: str, bars: int) -> bool:
        """Check for higher highs pattern (placeholder for future implementation)"""
        # TODO: Implement candle fetching and higher-highs logic
        return True 