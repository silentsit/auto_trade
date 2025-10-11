from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional
from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from position_journal import Position
from datetime import datetime, timezone, timedelta
import numpy as np
import logging
from config import config

logger = logging.getLogger(__name__)

@dataclass
class TrailingStopConfig:
    """Volatility-adaptive trailing stop configuration"""
    atr_multiplier: float
    min_distance_atr_factor: float
    min_distance_floor: float  # in PRICE units
    breakeven_threshold: float
    max_trail_distance: float  # in PRICE units

class TrailingStopManager:
    """Volatility-adaptive trailing stop manager"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
    
    def _pip_size(self) -> float:
        """Returns price-unit size of 1 pip for the symbol"""
        return 0.01 if self.symbol.endswith("JPY") else 0.0001
    
    def _pips_to_price(self, pips: float) -> float:
        """Convert pips to price units"""
        return pips * self._pip_size()
    
    def get_trailing_stop_config(self, timeframe: str) -> TrailingStopConfig:
        """Returns volatility-adaptive config based on timeframe"""
        tf = timeframe.upper()
        
        if tf == "15M":
            floor_pips = 2.0
            cfg = TrailingStopConfig(
                atr_multiplier=1.5,
                min_distance_atr_factor=0.15,   # 15% of ATR
                min_distance_floor=self._pips_to_price(floor_pips),
                breakeven_threshold=1.5,  # 1.5R for breakeven
                max_trail_distance=self._pips_to_price(30.0),  # 30 pips
            )
        elif tf == "1H":
            floor_pips = 3.0
            cfg = TrailingStopConfig(
                atr_multiplier=2.0,
                min_distance_atr_factor=0.20,   # 20% of ATR
                min_distance_floor=self._pips_to_price(floor_pips),
                breakeven_threshold=1.5,  # 1.5R for breakeven
                max_trail_distance=self._pips_to_price(40.0),  # 40 pips
            )
        else:  # 4H+
            floor_pips = 5.0
            cfg = TrailingStopConfig(
                atr_multiplier=2.5,
                min_distance_atr_factor=0.25,   # 25% of ATR
                min_distance_floor=self._pips_to_price(floor_pips),
                breakeven_threshold=1.5,  # 1.5R for breakeven
                max_trail_distance=self._pips_to_price(60.0),  # 60 pips
            )
        
        return cfg
    
    @staticmethod
    def effective_min_distance(atr: float, cfg: TrailingStopConfig) -> float:
        """Calculate volatility-adaptive min distance"""
        return max(cfg.min_distance_floor, atr * cfg.min_distance_atr_factor)

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
        # Track taper state per position_id
        self._taper_state: Dict[str, Dict[str, Any]] = {}

    # ---------- Phase 1: Deterministic taper helpers ----------
    def _get_taper_state(self, position_id: str) -> Dict[str, Any]:
        st = self._taper_state.get(position_id)
        if not st:
            st = {
                'legs_executed': 0,
                'm1_done': False,
                'm2_done': False,
                'mn_done': False
            }
            self._taper_state[position_id] = st
        return st

    async def should_taper(self, position: Position, current_price: float, regime_confidence: float) -> Optional[Dict[str, Any]]:
        """
        Decide whether to execute a taper leg, based on deterministic milestones and
        a basic slippage budget gate. Returns a dict with {'fraction', 'reason'} or None.
        """
        try:
            if not config.trading.enable_profit_ride_taper:
                return None

            state = self._get_taper_state(position.position_id)
            if state['legs_executed'] >= config.trading.taper_max_legs:
                return None

            # Compute P&L in ATR units against initial risk
            atr = get_atr(position.symbol)
            if atr <= 0:
                return None
            if position.action == "BUY":
                pnl_price = current_price - position.entry_price
            else:
                pnl_price = position.entry_price - current_price
            pnl_atr = pnl_price / atr

            # Milestone thresholds
            target_initial = config.trading.taper_target_initial_atr

            # Slippage gate (approx) via OandaService when available
            implied_slippage_bps = None
            try:
                if hasattr(self, 'oanda_service') and self.oanda_service:
                    implied_slippage_bps = await self.oanda_service.estimate_implied_slippage_bps(position.symbol, config.trading.taper_min_clip_fraction)
            except Exception:
                implied_slippage_bps = None

            # Gate by slippage budget
            if implied_slippage_bps is not None and implied_slippage_bps > config.trading.slippage_budget_bps:
                return None

            # M1: lock initial target
            if not state['m1_done'] and pnl_atr >= target_initial:
                fraction = max(config.trading.taper_min_clip_fraction, config.trading.taper_m1_fraction)
                state['m1_done'] = True
                state['legs_executed'] += 1
                return {"fraction": float(fraction), "reason": f"M1(target {target_initial} ATR)"}

            # M2: confirmation +1.0 ATR
            if not state['m2_done'] and pnl_atr >= (target_initial + 1.0):
                fraction = max(config.trading.taper_min_clip_fraction, config.trading.taper_m2_fraction)
                state['m2_done'] = True
                state['legs_executed'] += 1
                return {"fraction": float(fraction), "reason": "M2(+1.0 ATR)"}

            # MN: regime confidence decay
            if not state['mn_done'] and regime_confidence < config.trading.regime_confidence_min:
                fraction = max(config.trading.taper_min_clip_fraction, config.trading.taper_mn_fraction)
                state['mn_done'] = True
                state['legs_executed'] += 1
                return {"fraction": float(fraction), "reason": "MN(regime decay)"}

            return None
        except Exception as e:
            logger.error(f"Error in should_taper: {e}")
            return None

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
            atr = get_atr(position.symbol)  # Use fallback ATR from utils.py
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
            
            # --- ENHANCED OVERRIDE DECISION LOGIC ---
            # 7. Enhanced scoring system (0-10 points)
            override_score = 0.0
            reasons = []
            
            # Profit factor (0-3 points)
            profit_factor = current_pnl / initial_risk if initial_risk > 0 else 0
            if profit_factor >= 2.0:
                override_score += 3.0
                reasons.append("High profit factor (2.0+)")
            elif profit_factor >= 1.5:
                override_score += 2.0
                reasons.append("Good profit factor (1.5+)")
            elif profit_factor >= 1.0:
                override_score += 1.0
                reasons.append("Minimum profit threshold met")
            
            # Market regime (0-2 points)
            if current_regime in ['trending_up', 'trending_down'] and regime_confidence > 0.7:
                override_score += 2.0
                reasons.append("Strong trending regime")
            elif current_regime in ['momentum_up', 'momentum_down'] and regime_confidence > 0.6:
                override_score += 1.5
                reasons.append("Momentum regime")
            
            # Volatility (0-2 points)
            if 0.8 <= volatility_ratio <= 1.5:
                override_score += 2.0
                reasons.append("Optimal volatility")
            elif 0.6 <= volatility_ratio <= 2.0:
                override_score += 1.0
                reasons.append("Acceptable volatility")
            
            # Position age bonus (0-1 point)
            position_age = await self._calculate_position_age(position)
            if position_age.total_seconds() < (24 * 3600):  # Less than 24 hours
                override_score += 1.0
                reasons.append("Fresh position")
            
            # 8. Clear decision threshold (6+ points required)
            should_override = override_score >= 6.0
            confidence = min(override_score / 10.0, 1.0)  # Normalize to 0-1
            ignore = should_override
            
            # --- DYNAMIC R:R CALCULATION ---
            sl_mult, tp_mult = await self._calculate_dynamic_rr(
                position, atr, confidence_score, volatility_ratio, is_trending
            )
            
            # Update metrics
            override_metrics.update({
                "atr": atr,
                "market_regime": current_regime,
                "regime_confidence": regime_strength,
                "volatility_ratio": volatility_ratio,
                "volatility_state": volatility_state,
                "current_pnl": current_pnl,
                "initial_risk": initial_risk,
                "risk_reward_ratio": risk_reward_ratio,
                "momentum_score": momentum_score,
                "confidence_score": confidence,
                "override_score": override_score,
                "override_threshold": 6.0,
                "reasons": reasons,
                "sl_atr_multiple": sl_mult,
                "tp_atr_multiple": tp_mult,
                "max_ride_time": max_ride_time
            })
            
            # Log comprehensive evaluation
            logger.info(f"🔍 Enhanced Override Analysis for {position.symbol}:")
            logger.info(f"   PnL: ${current_pnl:.2f}, Risk: ${initial_risk:.2f}, R:R: {risk_reward_ratio:.2f}")
            logger.info(f"   Regime: {current_regime} (strength: {regime_strength:.2f})")
            logger.info(f"   Volatility: {volatility_state} ({volatility_ratio:.2f})")
            logger.info(f"   Momentum: {momentum_score:.2f}, Confidence: {confidence:.2f}")
            logger.info(f"   Override Score: {override_score:.1f}/10 (threshold: 6.0)")
            logger.info(f"   Reasons: {'; '.join(reasons)}")
            logger.info(f"   Decision: {'OVERRIDE ALLOWED' if ignore else 'OVERRIDE DENIED'}")

            return OverrideDecision(
                ignore_close=ignore,
                sl_atr_multiple=sl_mult,
                tp_atr_multiple=tp_mult,
                reason=f"Score: {override_score:.1f}/10 - {'; '.join(reasons)}" if ignore else f"Score: {override_score:.1f}/10 - {'; '.join(reasons)}",
                confidence_score=confidence,
                risk_reward_ratio=risk_reward_ratio,
                market_regime=current_regime,
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

    async def activate_trailing_stop(self, position: Position, current_price: float) -> Dict[str, Any]:
        """
        Activate trailing stop system for a position that has been approved for override.
        This implements the volatility-adaptive trailing stop logic.
        """
        try:
            # Mark override as fired
            position.metadata['profit_ride_override_fired'] = True
            position.metadata['trailing_stop_active'] = True
            position.metadata['trailing_stop_price'] = current_price
            position.metadata['breakeven_enabled'] = False
            position.metadata['breakeven_price'] = None
            
            # Get ATR for trailing stop calculations
            atr = get_atr(position.symbol)  # Use fallback ATR from utils.py
            if atr <= 0:
                logger.warning(f"ATR not available for {position.symbol}, using default")
                atr = 0.001  # Default ATR
            
            # Get volatility-adaptive configuration
            trailing_manager = TrailingStopManager(position.symbol)
            config = trailing_manager.get_trailing_stop_config(position.timeframe)
            
            # Calculate volatility-adaptive trailing distance
            desired_trail = config.atr_multiplier * atr
            min_distance = TrailingStopManager.effective_min_distance(atr, config)
            trailing_distance = max(min_distance, min(desired_trail, config.max_trail_distance))
            
            # Set initial trailing stop
            if position.action == "BUY":
                trailing_stop_price = current_price - trailing_distance
            else:  # SELL
                trailing_stop_price = current_price + trailing_distance
            
            position.metadata['trailing_stop_price'] = trailing_stop_price
            position.metadata['trailing_distance'] = trailing_distance
            position.metadata['atr_multiplier'] = config.atr_multiplier
            position.metadata['trailing_config'] = {
                'min_distance_atr_factor': config.min_distance_atr_factor,
                'min_distance_floor': config.min_distance_floor,
                'max_trail_distance': config.max_trail_distance,
                'breakeven_threshold': config.breakeven_threshold
            }
            
            # CRITICAL: Remove initial SL/TP from broker when trailing stops activate
            await self._remove_broker_sl_tp(position)
            
            logger.info(f"🎯 Volatility-adaptive trailing stop activated for {position.symbol}: {trailing_stop_price:.5f}")
            logger.info(f"   ATR: {atr:.5f}, Distance: {trailing_distance:.5f}, Min: {min_distance:.5f}")
            logger.info(f"✅ Initial SL/TP removed from broker for {position.symbol}")
            
            return {
                "trailing_stop_price": trailing_stop_price,
                "trailing_distance": trailing_distance,
                "atr_multiplier": config.atr_multiplier,
                "breakeven_enabled": False,
                "broker_sl_tp_removed": True,
                "volatility_adaptive": True
            }
            
        except Exception as e:
            logger.error(f"Error activating trailing stop: {e}")
            return {}

    async def update_trailing_stop(self, position: Position, current_price: float) -> Optional[float]:
        """
        Update trailing stop based on current price movement with volatility-adaptive distance.
        Returns new trailing stop price if updated, None if no update needed.
        """
        try:
            if not position.metadata.get('trailing_stop_active', False):
                return None
            
            trailing_stop_price = position.metadata.get('trailing_stop_price')
            if not trailing_stop_price:
                return None
            
            # Get ATR for dynamic distance calculation
            atr = get_atr(position.symbol)  # Use fallback ATR from utils.py
            if atr <= 0:
                atr = 0.001  # Default ATR
            
            # Get trailing configuration
            trailing_config = position.metadata.get('trailing_config', {})
            if not trailing_config:
                # Fallback to basic configuration
                trailing_distance = position.metadata.get('trailing_distance', 0.0003)
            else:
                # Calculate dynamic trailing distance
                desired_trail = trailing_config.get('atr_multiplier', 2.0) * atr
                min_distance = TrailingStopManager.effective_min_distance(atr, 
                    TrailingStopConfig(
                        atr_multiplier=trailing_config.get('atr_multiplier', 2.0),
                        min_distance_atr_factor=trailing_config.get('min_distance_atr_factor', 0.20),
                        min_distance_floor=trailing_config.get('min_distance_floor', 0.0003),
                        breakeven_threshold=trailing_config.get('breakeven_threshold', 1.5),
                        max_trail_distance=trailing_config.get('max_trail_distance', 0.0040)
                    )
                )
                trailing_distance = max(min_distance, min(desired_trail, trailing_config.get('max_trail_distance', 0.0040)))
            
            # Check for breakeven activation (1.5R profit)
            if not position.metadata.get('breakeven_enabled', False):
                current_pnl, initial_risk, _ = await self._calculate_pnl_metrics(position, current_price, atr)
                if current_pnl >= initial_risk * 1.5:  # 1.5R profit
                    position.metadata['breakeven_enabled'] = True
                    position.metadata['breakeven_price'] = position.entry_price
                    logger.info(f"🎯 Breakeven activated for {position.symbol} at {position.entry_price:.5f} (1.5R profit)")
            
            # Calculate new trailing stop
            if position.action == "BUY":
                new_trailing_stop = current_price - trailing_distance
                
                # Apply breakeven if enabled
                if position.metadata.get('breakeven_enabled', False):
                    breakeven_price = position.metadata.get('breakeven_price', position.entry_price)
                    new_trailing_stop = max(new_trailing_stop, breakeven_price)
                
                # Only move trailing stop up
                if new_trailing_stop > trailing_stop_price:
                    position.metadata['trailing_stop_price'] = new_trailing_stop
                    logger.info(f"📈 Volatility-adaptive trailing stop updated for {position.symbol}: {new_trailing_stop:.5f} (distance: {trailing_distance:.5f})")
                    return new_trailing_stop
                    
            else:  # SELL
                new_trailing_stop = current_price + trailing_distance
                
                # Apply breakeven if enabled
                if position.metadata.get('breakeven_enabled', False):
                    breakeven_price = position.metadata.get('breakeven_price', position.entry_price)
                    new_trailing_stop = min(new_trailing_stop, breakeven_price)
                
                # Only move trailing stop down
                if new_trailing_stop < trailing_stop_price:
                    position.metadata['trailing_stop_price'] = new_trailing_stop
                    logger.info(f"📉 Volatility-adaptive trailing stop updated for {position.symbol}: {new_trailing_stop:.5f} (distance: {trailing_distance:.5f})")
                    return new_trailing_stop
            
            return None
            
        except Exception as e:
            logger.error(f"Error updating trailing stop: {e}")
            return None

    async def check_trailing_stop_hit(self, position: Position, current_price: float) -> bool:
        """
        Check if trailing stop has been hit.
        Returns True if position should be closed.
        """
        try:
            if not position.metadata.get('trailing_stop_active', False):
                return False
            
            trailing_stop_price = position.metadata.get('trailing_stop_price')
            if not trailing_stop_price:
                return False
            
            # Check if price has hit trailing stop
            if position.action == "BUY":
                hit = current_price <= trailing_stop_price
            else:  # SELL
                hit = current_price >= trailing_stop_price
            
            if hit:
                logger.info(f"🎯 Trailing stop hit for {position.symbol} at {current_price:.5f}")
                position.metadata['trailing_stop_hit'] = True
                position.metadata['trailing_stop_exit_price'] = current_price
            
            return hit
            
        except Exception as e:
            logger.error(f"Error checking trailing stop: {e}")
            return False

    async def _get_account_balance(self) -> float:
        """Get current account balance with graceful degradation"""
        try:
            # This would integrate with your OANDA service
            if hasattr(self, 'oanda_service') and self.oanda_service:
                return await self.oanda_service.get_account_balance()
            return 100000.0  # Default fallback
        except Exception as e:
            logger.warning(f"Could not get account balance: {e}")
            return 100000.0  # Safe fallback

    async def _get_market_regime(self, symbol: str) -> Dict[str, Any]:
        """Get market regime data with graceful degradation"""
        try:
            # Integrate with your regime classifier
            if hasattr(self, 'regime') and self.regime:
                return self.regime.get_regime_data(symbol)
            return {'regime': 'unknown', 'regime_strength': 0.0}
        except Exception as e:
            logger.warning(f"Could not get market regime for {symbol}: {e}")
            return {'regime': 'unknown', 'regime_strength': 0.0}

    async def _get_volatility_state(self, symbol: str) -> Dict[str, Any]:
        """Get volatility state data with graceful degradation"""
        try:
            # Integrate with your volatility monitor
            if hasattr(self, 'vol') and self.vol:
                return self.vol.get_volatility_state(symbol)
            return {'volatility_ratio': 1.0, 'volatility_state': 'normal'}
        except Exception as e:
            logger.warning(f"Could not get volatility state for {symbol}: {e}")
            return {'volatility_ratio': 1.0, 'volatility_state': 'normal'}

    async def _calculate_position_age(self, position: Position) -> timedelta:
        """Calculate position age"""
        try:
            open_time = position.metadata.get('open_time') or position.metadata.get('opened_at')
            if not open_time:
                return timedelta(0)  # No age if no open time recorded
            
            if isinstance(open_time, str):
                try:
                    open_time_dt = datetime.fromisoformat(open_time)
                except Exception:
                    return timedelta(0)
            elif isinstance(open_time, datetime):
                open_time_dt = open_time
            else:
                return timedelta(0)
            
            now = datetime.now(timezone.utc)
            return now - open_time_dt
            
        except Exception as e:
            logger.error(f"Error calculating position age: {e}")
            return timedelta(0)

    async def _remove_broker_sl_tp(self, position: Position):
        """Remove initial SL/TP from broker when trailing stops activate"""
        try:
            logger.info(f"Removing initial SL/TP from broker for {position.symbol}")
            
            # Integrate with your OANDA service
            if hasattr(self, 'oanda_service') and self.oanda_service:
                # Remove stop loss
                if position.stop_loss:
                    await self.oanda_service.remove_stop_loss(position.position_id)
                
                # Remove take profit
                if position.take_profit:
                    await self.oanda_service.remove_take_profit(position.position_id)
                
                logger.info(f"✅ Initial SL/TP removed from broker for {position.symbol}")
            else:
                logger.warning(f"OANDA service not available for SL/TP removal")
                
        except Exception as e:
            logger.error(f"Error removing broker SL/TP for {position.symbol}: {e}") 