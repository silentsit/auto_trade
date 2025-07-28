#
# file: risk_manager.py - Enhanced with ML-based Risk Management
#
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
from config import config
from correlation_manager import CorrelationManager

# Import ML enhancements
from ml_extensions import MLExtensions
from kernel_functions import KernelFunctions

# Get max daily loss from config with proper fallback
try:
    max_daily_loss_val = getattr(config, 'max_daily_loss', 50.0)
    if isinstance(max_daily_loss_val, str):
        max_daily_loss_val = float(max_daily_loss_val)
    MAX_DAILY_LOSS = max_daily_loss_val / 100.0  # Default to 50% if not set
except Exception as e:
    logger.error(f"Error parsing max_daily_loss from config: {e}")
    MAX_DAILY_LOSS = 0.5

class EnhancedRiskManager:
    """
    Enhanced Risk Management System with ML-based adaptive volatility filtering.
    Integrates MLExtensions and KernelFunctions for institutional-grade risk control.
    """
    
    def __init__(self, 
                 max_risk_per_trade: Optional[float] = None, 
                 max_portfolio_risk: Optional[float] = None):
        """
        Comprehensive risk management system with ML enhancements.
        """
        
        if max_risk_per_trade is not None:
            self.max_risk_per_trade = max_risk_per_trade
        elif hasattr(config, 'max_risk_percentage'):
            val = getattr(config, 'max_risk_percentage')
            if isinstance(val, str):
                val = float(val)
            self.max_risk_per_trade = val / 100.0
        else:
            self.max_risk_per_trade = 0.10 # Fallback if not in config for some reason

        if max_portfolio_risk is not None:
            self.max_portfolio_risk = max_portfolio_risk
        elif hasattr(config, 'max_portfolio_heat'):
            val = getattr(config, 'max_portfolio_heat')
            if isinstance(val, str):
                val = float(val)
            self.max_portfolio_risk = val / 100.0
        else:
            self.max_portfolio_risk = 0.70 # Fallback

        self.account_balance = 0.0
        self.positions = {}  # position_id -> risk data
        self.current_risk = 0.0  # Current portfolio risk exposure
        self.daily_loss = 0.0  # Track daily loss for circuit breaker
        self.drawdown = 0.0  # Current drawdown
        self._lock = asyncio.Lock()
        
        # ML Enhancement Components
        self.ml_ext = MLExtensions()
        self.kernel_func = KernelFunctions()
        
        # Market data storage for ML analysis
        self.market_data_cache = {}  # symbol -> DataFrame
        self.volatility_cache = {}   # symbol -> volatility metrics
        self.regime_cache = {}       # symbol -> regime information
        
        # Adaptive risk parameters
        self.adaptive_risk_enabled = True
        self.volatility_lookback = 20
        self.regime_adjustment_factor = 0.3
        self.ml_risk_weight = 0.4
        
        # Traditional risk parameters
        self.correlation_factor = 1.0
        self.volatility_factor = 1.0
        self.win_streak = 0
        self.loss_streak = 0
        
        self.portfolio_heat_limit = self.max_portfolio_risk 
        self.portfolio_concentration_limit = 0.20  # This could also come from config
        self.correlation_limit = 0.70          # This could also come from config
        
        # Initialize correlation manager
        self.correlation_manager = CorrelationManager()
        
        self.timeframe_risk_weights = {
            "M1": 1.2,
            "M5": 1.1,
            "M15": 1.0,
            "M30": 0.9,
            "H1": 0.8,
            "H4": 0.7,
            "D1": 0.6
        }
        
        logger.info(f"Enhanced RiskManager initialized with ML components: max_risk_per_trade={self.max_risk_per_trade*100:.2f}%, max_portfolio_risk={self.max_portfolio_risk*100:.2f}%")
        
    async def initialize(self, account_balance: float):
        """Initialize the enhanced risk manager with account balance"""
        async with self._lock:
            self.account_balance = float(account_balance)
            logger.info(f"Enhanced risk manager initialized with balance: {self.account_balance}")
            return True

    async def update_account_balance(self, new_balance: float):
        """Update account balance with ML-enhanced tracking"""
        async with self._lock:
            old_balance = self.account_balance
            self.account_balance = float(new_balance)
            
            # Calculate daily loss if balance decreased
            if new_balance < old_balance:
                loss = old_balance - new_balance
                self.daily_loss += loss
                
                # ML-based drawdown analysis
                await self._analyze_drawdown_pattern(old_balance, new_balance)
            
            # Update drawdown calculation
            peak_balance = max(old_balance, new_balance)
            self.drawdown = (peak_balance - new_balance) / peak_balance if peak_balance > 0 else 0.0
            
            logger.info(f"Balance updated: {old_balance:.2f} -> {new_balance:.2f}, Drawdown: {self.drawdown:.2%}")

    async def calculate_adaptive_position_size(self, symbol: str, entry_price: float, 
                                             stop_loss: float, timeframe: str = "M15",
                                             market_data: Optional[pd.DataFrame] = None) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate position size with ML-enhanced adaptive risk management.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price for the position
            stop_loss: Stop loss price
            timeframe: Trading timeframe
            market_data: Optional market data for ML analysis
            
        Returns:
            Tuple of (position_size, risk_analysis)
        """
        try:
            async with self._lock:
                # Base risk calculation
                base_risk_amount = self.account_balance * self.max_risk_per_trade
                stop_distance = abs(entry_price - stop_loss)
                
                if stop_distance <= 0:
                    logger.warning(f"Invalid stop distance for {symbol}: {stop_distance}")
                    return 0.0, {"error": "Invalid stop distance"}
                
                base_position_size = base_risk_amount / stop_distance
                
                # ML-Enhanced Risk Adjustments
                if self.adaptive_risk_enabled and market_data is not None:
                    ml_analysis = await self._analyze_market_risk(symbol, market_data, timeframe)
                    risk_adjustments = await self._calculate_risk_adjustments(symbol, ml_analysis, timeframe)
                    
                    # Apply ML-based adjustments
                    adjusted_position_size = base_position_size * risk_adjustments["position_multiplier"]
                    
                    risk_analysis = {
                        "base_position_size": base_position_size,
                        "adjusted_position_size": adjusted_position_size,
                        "position_multiplier": risk_adjustments["position_multiplier"],
                        "ml_analysis": ml_analysis,
                        "risk_adjustments": risk_adjustments,
                        "risk_level": risk_adjustments.get("risk_level", "medium")
                    }
                    
                    return adjusted_position_size, risk_analysis
                else:
                    # Fallback to traditional calculation
                    traditional_adjustments = self._apply_traditional_adjustments(base_position_size, timeframe)
                    
                    risk_analysis = {
                        "base_position_size": base_position_size,
                        "adjusted_position_size": traditional_adjustments,
                        "method": "traditional",
                        "timeframe_weight": self.timeframe_risk_weights.get(timeframe, 1.0)
                    }
                    
                    return traditional_adjustments, risk_analysis
                    
        except Exception as e:
            logger.error(f"Error calculating adaptive position size for {symbol}: {e}")
            return 0.0, {"error": str(e)}

    async def _analyze_market_risk(self, symbol: str, market_data: pd.DataFrame, 
                                 timeframe: str) -> Dict[str, Any]:
        """Analyze market risk using ML indicators"""
        try:
            # Ensure we have enough data
            if len(market_data) < 50:
                return {"error": "Insufficient market data"}
            
            # Calculate ML indicators for risk assessment
            close_prices = market_data['close']
            high_prices = market_data['high'] if 'high' in market_data.columns else close_prices * 1.001
            low_prices = market_data['low'] if 'low' in market_data.columns else close_prices * 0.999
            
            # Volatility analysis using ML methods
            normalized_deriv = self.ml_ext.normalize_deriv(close_prices)
            tanh_transform = self.ml_ext.tanh_transform(close_prices)
            
            # Normalized technical indicators
            n_rsi = self.ml_ext.n_rsi(close_prices)
            n_adx = self.ml_ext.n_adx(high_prices, low_prices, close_prices)
            
            # Kernel-based trend and volatility analysis
            kernel_trend = self.kernel_func.kernel_trend_strength(close_prices)
            adaptive_kernel = self.kernel_func.adaptive_kernel_ensemble(close_prices)
            
            # Regime and volatility filters
            regime_filter = self.ml_ext.regime_filter(close_prices)
            volatility_filter = self.ml_ext.filter_volatility(close_prices, high_prices, low_prices)
            
            # Calculate risk metrics
            current_volatility = abs(normalized_deriv.iloc[-1]) if len(normalized_deriv) > 0 else 0.0
            trend_strength = abs(kernel_trend.iloc[-1]) if len(kernel_trend) > 0 else 0.0
            regime_stability = float(regime_filter.iloc[-1]) if len(regime_filter) > 0 else False
            
            # Volatility percentile analysis
            recent_volatility = normalized_deriv.abs().tail(10).mean()
            historical_volatility = normalized_deriv.abs().mean()
            volatility_percentile = recent_volatility / (historical_volatility + 1e-10)
            
            # Market regime classification
            if trend_strength > 0.4 and regime_stability:
                market_regime = "trending"
                regime_risk = "low"
            elif current_volatility > 0.6:
                market_regime = "volatile"
                regime_risk = "high"
            elif trend_strength < 0.2:
                market_regime = "ranging"
                regime_risk = "medium"
            else:
                market_regime = "mixed"
                regime_risk = "medium"
            
            # Risk assessment
            risk_factors = []
            if current_volatility > 0.7:
                risk_factors.append("High current volatility")
            if volatility_percentile > 1.5:
                risk_factors.append("Elevated volatility compared to historical")
            if not regime_stability:
                risk_factors.append("Unstable market regime")
            if trend_strength < 0.1:
                risk_factors.append("Weak trend strength")
            
            ml_analysis = {
                "symbol": symbol,
                "timeframe": timeframe,
                "current_volatility": current_volatility,
                "volatility_percentile": volatility_percentile,
                "trend_strength": trend_strength,
                "regime_stability": regime_stability,
                "market_regime": market_regime,
                "regime_risk": regime_risk,
                "risk_factors": risk_factors,
                "ml_indicators": {
                    "n_rsi": float(n_rsi.iloc[-1]) if len(n_rsi) > 0 else 0.5,
                    "n_adx": float(n_adx.iloc[-1]) if len(n_adx) > 0 else 0.5,
                    "tanh_transform": float(tanh_transform.iloc[-1]) if len(tanh_transform) > 0 else 0.0
                },
                "filters": {
                    "regime_filter": regime_stability,
                    "volatility_filter": bool(volatility_filter.iloc[-1]) if len(volatility_filter) > 0 else False
                }
            }
            
            # Cache the analysis
            self.market_data_cache[symbol] = market_data.tail(100)  # Keep recent data
            self.volatility_cache[symbol] = {
                "current": current_volatility,
                "percentile": volatility_percentile,
                "timestamp": datetime.now(timezone.utc)
            }
            self.regime_cache[symbol] = {
                "regime": market_regime,
                "risk": regime_risk,
                "stability": regime_stability,
                "timestamp": datetime.now(timezone.utc)
            }
            
            return ml_analysis
            
        except Exception as e:
            logger.error(f"Error in ML market risk analysis for {symbol}: {e}")
            return {"error": str(e)}

    async def _calculate_risk_adjustments(self, symbol: str, ml_analysis: Dict[str, Any], 
                                        timeframe: str) -> Dict[str, Any]:
        """Calculate risk adjustments based on ML analysis"""
        try:
            if "error" in ml_analysis:
                return {"position_multiplier": 0.5, "risk_level": "high", "reason": "ML analysis failed"}
            
            # Base multiplier
            position_multiplier = 1.0
            risk_level = "medium"
            adjustment_reasons = []
            
            # 1. Volatility-based adjustments
            current_volatility = ml_analysis.get("current_volatility", 0.0)
            volatility_percentile = ml_analysis.get("volatility_percentile", 1.0)
            
            if current_volatility > 0.7:
                position_multiplier *= 0.6  # Reduce size for high volatility
                risk_level = "high"
                adjustment_reasons.append("High current volatility")
            elif volatility_percentile > 1.5:
                position_multiplier *= 0.7  # Reduce size for elevated volatility
                adjustment_reasons.append("Elevated volatility vs historical")
            elif current_volatility < 0.2:
                position_multiplier *= 1.2  # Increase size for low volatility
                adjustment_reasons.append("Low volatility environment")
            
            # 2. Regime-based adjustments
            market_regime = ml_analysis.get("market_regime", "mixed")
            regime_risk = ml_analysis.get("regime_risk", "medium")
            
            if regime_risk == "high":
                position_multiplier *= 0.7
                risk_level = "high"
                adjustment_reasons.append(f"High risk {market_regime} regime")
            elif regime_risk == "low" and market_regime == "trending":
                position_multiplier *= 1.1
                adjustment_reasons.append("Low risk trending regime")
            
            # 3. Trend strength adjustments
            trend_strength = ml_analysis.get("trend_strength", 0.0)
            if trend_strength > 0.5:
                position_multiplier *= 1.1  # Increase for strong trends
                adjustment_reasons.append("Strong trend detected")
            elif trend_strength < 0.2:
                position_multiplier *= 0.8  # Reduce for weak trends
                adjustment_reasons.append("Weak trend strength")
            
            # 4. Filter-based adjustments
            filters = ml_analysis.get("filters", {})
            if not filters.get("regime_filter", False):
                position_multiplier *= 0.8
                adjustment_reasons.append("Regime filter failed")
            if not filters.get("volatility_filter", False):
                position_multiplier *= 0.9
                adjustment_reasons.append("Volatility filter failed")
            
            # 5. Timeframe adjustments
            tf_weight = self.timeframe_risk_weights.get(timeframe, 1.0)
            position_multiplier *= tf_weight
            if tf_weight != 1.0:
                adjustment_reasons.append(f"Timeframe adjustment: {tf_weight}")
            
            # 6. Risk factor accumulation
            risk_factors = ml_analysis.get("risk_factors", [])
            if len(risk_factors) >= 3:
                position_multiplier *= 0.5
                risk_level = "high"
                adjustment_reasons.append(f"Multiple risk factors: {len(risk_factors)}")
            elif len(risk_factors) == 0:
                position_multiplier *= 1.2
                if risk_level != "high":
                    risk_level = "low"
                adjustment_reasons.append("No risk factors detected")
            
            # Apply bounds
            position_multiplier = max(0.1, min(2.0, position_multiplier))
            
            # Final risk level determination
            if position_multiplier < 0.6:
                risk_level = "high"
            elif position_multiplier > 1.3:
                risk_level = "low"
            
            return {
                "position_multiplier": position_multiplier,
                "risk_level": risk_level,
                "adjustment_reasons": adjustment_reasons,
                "volatility_impact": current_volatility,
                "regime_impact": regime_risk,
                "trend_impact": trend_strength,
                "filter_impact": sum(filters.values()) / len(filters) if filters else 0.5
            }
            
        except Exception as e:
            logger.error(f"Error calculating risk adjustments for {symbol}: {e}")
            return {"position_multiplier": 0.5, "risk_level": "high", "reason": f"Calculation error: {str(e)}"}

    def _apply_traditional_adjustments(self, base_size: float, timeframe: str) -> float:
        """Apply traditional risk adjustments as fallback"""
        try:
            # Timeframe adjustment
            tf_weight = self.timeframe_risk_weights.get(timeframe, 1.0)
            adjusted_size = base_size * tf_weight
            
            # Volatility factor adjustment
            adjusted_size *= self.volatility_factor
            
            # Correlation factor adjustment
            adjusted_size *= self.correlation_factor
            
            return max(0.0, adjusted_size)
            
        except Exception as e:
            logger.error(f"Error in traditional adjustments: {e}")
            return base_size * 0.5

    async def _analyze_drawdown_pattern(self, old_balance: float, new_balance: float):
        """Analyze drawdown patterns using ML"""
        try:
            # This could be expanded to include sophisticated drawdown analysis
            # using the ML components for pattern recognition
            drawdown_percentage = (old_balance - new_balance) / old_balance * 100
            
            if drawdown_percentage > 5:  # Significant drawdown
                logger.warning(f"Significant drawdown detected: {drawdown_percentage:.2f}%")
                # Could trigger additional risk reduction measures
                
        except Exception as e:
            logger.error(f"Error analyzing drawdown pattern: {e}")

    async def get_risk_assessment(self, symbol: str) -> Dict[str, Any]:
        """Get comprehensive risk assessment for a symbol"""
        try:
            assessment = {
                "symbol": symbol,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "account_balance": self.account_balance,
                "current_risk": self.current_risk,
                "max_risk_per_trade": self.max_risk_per_trade,
                "max_portfolio_risk": self.max_portfolio_risk,
                "daily_loss": self.daily_loss,
                "drawdown": self.drawdown
            }
            
            # Add cached ML analysis if available
            if symbol in self.volatility_cache:
                assessment["volatility_analysis"] = self.volatility_cache[symbol]
            
            if symbol in self.regime_cache:
                assessment["regime_analysis"] = self.regime_cache[symbol]
            
            return assessment
            
        except Exception as e:
            logger.error(f"Error getting risk assessment for {symbol}: {e}")
            return {"error": str(e)}
            
    async def register_position(self,
                               position_id: str,
                               symbol: str,
                               action: str,
                               size: float,
                               entry_price: float,
                               account_risk: float,
                               stop_loss: Optional[float] = None,
                               timeframe: str = "H1") -> Dict[str, Any]:
        """Register a new position with the risk manager"""
        async with self._lock:
            risk_amount = self.account_balance * account_risk
            risk_percentage = risk_amount / self.account_balance if self.account_balance > 0 else 0
            timeframe_weight = self.timeframe_risk_weights.get(timeframe, 1.0)
            adjusted_risk = risk_percentage * timeframe_weight
            if adjusted_risk > self.max_risk_per_trade:
                logger.warning(f"Position risk {adjusted_risk:.2%} exceeds per-trade limit {self.max_risk_per_trade:.2%}")
            if self.current_risk + adjusted_risk > self.max_portfolio_risk:
                logger.warning(f"Adding position would exceed portfolio risk limit {self.max_portfolio_risk:.2%}")
            risk_data = {
                "symbol": symbol,
                "action": action,
                "size": size,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "risk_amount": risk_amount,
                "risk_percentage": risk_percentage,
                "adjusted_risk": adjusted_risk,
                "timeframe": timeframe,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            correlated_instruments = self._get_correlated_instruments(symbol)
            if correlated_instruments:
                risk_data["correlated_instruments"] = correlated_instruments
                risk_data["correlation_factor"] = self.correlation_factor
                risk_data["correlation_adjusted_risk"] = adjusted_risk * self.correlation_factor
                adjusted_risk = risk_data["correlation_adjusted_risk"]
            streak_factor = self._calculate_streak_factor()
            risk_data["streak_factor"] = streak_factor
            risk_data["streak_adjusted_risk"] = adjusted_risk * streak_factor
            adjusted_risk = risk_data["streak_adjusted_risk"]
            risk_data["final_adjusted_risk"] = adjusted_risk
            self.positions[position_id] = risk_data
            self.current_risk += adjusted_risk
            logger.info(f"Registered position {position_id} with risk: {adjusted_risk:.2%} (total: {self.current_risk:.2%})")
            return risk_data
            
    async def is_trade_allowed(self, risk_percentage: float, symbol: Optional[str] = None, 
                             action: Optional[str] = None) -> Tuple[bool, str]:
        """Check if a trade with specified risk is allowed (now with correlation limits)"""
        async with self._lock:
            max_daily_loss_amount = self.account_balance * MAX_DAILY_LOSS
            if self.daily_loss >= max_daily_loss_amount:
                return False, f"Daily loss limit reached: {self.daily_loss:.2f} >= {max_daily_loss_amount:.2f}"
            if risk_percentage > self.max_risk_per_trade:
                return False, f"Trade risk exceeds limit: {risk_percentage:.2%} > {self.max_risk_per_trade:.2%}"
            if self.current_risk + risk_percentage > self.max_portfolio_risk:
                return False, f"Portfolio risk would exceed limit: {self.current_risk + risk_percentage:.2%} > {self.max_portfolio_risk:.2%}"
            if symbol:
                symbol_exposure = sum(
                    p.get("adjusted_risk", 0) for p in self.positions.values() 
                    if p.get("symbol") == symbol
                )
                if symbol_exposure + risk_percentage > self.portfolio_concentration_limit:
                    return False, f"Symbol concentration would exceed limit: {symbol_exposure + risk_percentage:.2%} > {self.portfolio_concentration_limit:.2%}"
                
                # Check correlation limits if enabled
                if config.enable_correlation_limits and action:
                    # *** FIX: Convert position data format correctly for correlation manager ***
                    # Group ALL positions by symbol (not just the last one per symbol)
                    current_positions = {}
                    for pos_id, pos_data in self.positions.items():
                        pos_symbol = pos_data.get('symbol')
                        if pos_symbol:
                            # If symbol already exists, we need to handle multiple positions
                            if pos_symbol in current_positions:
                                # For same-pair conflict checking, we need the first/any position
                                # The correlation manager will detect the conflict regardless
                                logger.debug(f"Multiple positions found for {pos_symbol}: keeping first for conflict check")
                                continue
                            else:
                                current_positions[pos_symbol] = pos_data
                    
                    # *** ADDITIONAL SAFETY: Direct same-pair check before correlation manager ***
                    if symbol in current_positions:
                        existing_action = current_positions[symbol].get('action', 'BUY')
                        if existing_action != action:
                            logger.info(f"[SAME-PAIR CONFLICT] Blocking {symbol} {action} - existing {existing_action} position found")
                            return False, f"Same-pair conflict: Cannot open {action} position while {existing_action} position exists for {symbol}"
                    
                    allowed, reason, analysis = await self.correlation_manager.check_correlation_limits(
                        symbol, action, risk_percentage, current_positions
                    )
                    
                    if not allowed:
                        return False, f"Correlation limit violation: {reason}"
                    
                    # Log correlation analysis for monitoring
                    if analysis.get('recommendation') == 'warning':
                        logger.info(f"Correlation warning for {symbol}: {analysis}")
            
            return True, "Trade allowed"
    
    async def adjust_position_size(self,
                                 base_size: float,
                                 symbol: str,
                                 risk_percentage: float,
                                 account_balance: Optional[float] = None) -> float:
        """Adjust position size based on risk parameters"""
        async with self._lock:
            if account_balance is not None:
                self.account_balance = account_balance
            remaining_capacity = self.max_portfolio_risk - self.current_risk
            if remaining_capacity <= 0:
                scale = 0.0
            elif remaining_capacity < risk_percentage:
                scale = remaining_capacity / risk_percentage
            else:
                scale = 1.0
            correlated_instruments = self._get_correlated_instruments(symbol)
            if correlated_instruments:
                scale *= self.correlation_factor
            scale *= self.volatility_factor
            streak_factor = self._calculate_streak_factor()
            scale *= streak_factor
            adjusted_size = base_size * scale
            logger.debug(f"Adjusted position size for {symbol}: {base_size} -> {adjusted_size} (scale: {scale:.2f})")
            return adjusted_size
    
    def _get_correlated_instruments(self, symbol: str) -> List[str]:
        correlated = []
        forex_pairs = {
            "EUR_USD": ["EUR_GBP", "EUR_JPY", "USD_CHF"],
            "GBP_USD": ["EUR_GBP", "GBP_JPY"],
            "USD_JPY": ["EUR_JPY", "GBP_JPY"]
        }
        return forex_pairs.get(symbol, [])
        
    def _calculate_streak_factor(self) -> float:
        if self.win_streak >= 3:
            return min(1.5, 1.0 + (self.win_streak - 2) * 0.1)
        elif self.loss_streak >= 2:
            return max(0.5, 1.0 - (self.loss_streak - 1) * 0.2)
        else:
            return 1.0
    
    async def update_win_loss_streak(self, is_win: bool):
        async with self._lock:
            if is_win:
                self.win_streak += 1
                self.loss_streak = 0
            else:
                self.loss_streak += 1
                self.win_streak = 0

    async def clear_position(self, position_id: str):
        """Remove position from risk tracking"""
        async with self._lock:
            if position_id in self.positions:
                risk_data = self.positions.pop(position_id)
                self.current_risk -= risk_data["final_adjusted_risk"]
                logger.info(f"Cleared position {position_id} from risk manager")
                
    async def get_risk_metrics(self) -> Dict[str, Any]:
        """Get current risk metrics"""
        async with self._lock:
            return {
                "current_portfolio_risk": self.current_risk,
                "max_portfolio_risk": self.max_portfolio_risk,
                "max_risk_per_trade": self.max_risk_per_trade,
                "daily_loss": self.daily_loss,
                "drawdown": self.drawdown,
                "win_streak": self.win_streak,
                "loss_streak": self.loss_streak,
                "active_positions": len(self.positions)
            }
            
    def calculate_position_units(self, equity: float, target_percent: float, leverage: float, current_price: float) -> int:
        """
        DEPRECATED: Calculate position size in units based on target equity percentage.
        
        WARNING: This method does not consider stop loss distance and creates inconsistent risk.
        Use the unified calculate_position_size() function from utils.py instead.
        
        Args:
            equity (float): Total account equity.
            target_percent (float): Target percentage of equity to use for the position.
            leverage (float): Account leverage.
            current_price (float): Current market price of the instrument.
            
        Returns:
            int: Position size in units, rounded to the nearest whole number.
        """
        
        logger.warning("DEPRECATED: calculate_position_units() creates inconsistent risk. Use unified calculate_position_size() instead.")
        
        # Calculate the total notional value of the position
        notional_value = equity * (target_percent / 100) * leverage
        
        # Calculate the position size in units
        if current_price > 0:
            units = notional_value / current_price
        else:
            units = 0
            
        # Return the rounded number of units
        return int(round(units))
