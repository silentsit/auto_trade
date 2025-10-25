#
# file: risk_manager.py
#
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from config import config
from correlation_manager import CorrelationManager

logger = logging.getLogger(__name__)


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
    def __init__(self, 
                 max_risk_per_trade: Optional[float] = None, 
                 max_portfolio_risk: Optional[float] = None):
        """
        Comprehensive risk management system.
        Now reads max_risk_per_trade and max_portfolio_risk from the global 'config'
        object by default.
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

        # CRITICAL FIX: Initialize with fallback balance to prevent zero balance issues
        self.account_balance = 100000.0  # Fallback balance: $100,000 USD by default
        self.positions = {}  # position_id -> risk data
        self.current_risk = 0.0  # Current portfolio risk exposure
        self.daily_loss = 0.0  # Track daily loss for circuit breaker
        self.drawdown = 0.0  # Current drawdown
        self._lock = asyncio.Lock()
        logger.info(f"✅ Risk manager created with fallback balance: ${self.account_balance:.2f} (will be updated with real OANDA balance)")
        
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
        logger.info(f"EnhancedRiskManager initialized with: max_risk_per_trade={self.max_risk_per_trade*100:.2f}%, max_portfolio_risk={self.max_portfolio_risk*100:.2f}%")
        
    async def initialize(self, account_balance: float):
        """Initialize the risk manager with account balance"""
        async with self._lock:
            old_balance = self.account_balance
            self.account_balance = float(account_balance)
            logger.info(f"✅ Risk manager initialize() called - Balance updated from ${old_balance:.2f} to ${self.account_balance:.2f}")
            return True

    async def update_account_balance(self, new_balance: float):
        """Update account balance"""
        async with self._lock:
            old_balance = self.account_balance
            self.account_balance = float(new_balance)
            
            # Calculate daily loss if balance decreased
            if new_balance < old_balance:
                loss = old_balance - new_balance
                self.daily_loss += loss
                
                # Calculate drawdown
                self.drawdown = max(self.drawdown, loss / old_balance * 100)
                
            logger.info(f"Updated account balance: {self.account_balance} (daily loss: {self.daily_loss})")
            return True
    
    async def refresh_balance_from_oanda(self, oanda_service):
        """Refresh account balance from OANDA service"""
        try:
            if oanda_service and hasattr(oanda_service, 'get_account_balance'):
                real_balance = await oanda_service.get_account_balance()
                await self.update_account_balance(real_balance)
                logger.info(f"✅ Risk manager balance refreshed from OANDA: ${real_balance:.2f}")
                return True
            else:
                logger.warning("⚠️ OANDA service not available for balance refresh")
                return False
        except Exception as e:
            logger.error(f"❌ Failed to refresh balance from OANDA: {e}")
            return False
            
    async def reset_daily_stats(self):
        """Reset daily statistics"""
        async with self._lock:
            self.daily_loss = 0.0
            logger.info("Reset daily risk statistics")
            return True
            
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
                             action: Optional[str] = None,
                             atr_current: Optional[float] = None,
                             timeframe: Optional[str] = None) -> Tuple[bool, str]:
        """
        Comprehensive institutional-grade risk validation before trade execution.
        
        Multi-layered validation system:
        1. Daily Loss Circuit Breaker
        2. Maximum Open Positions
        3. Maximum Exposure per Symbol
        4. Maximum Exposure per Account
        5. Correlation Limits
        6. Volatility-based adjustments
        """
        async with self._lock:
            # 1. DAILY LOSS CIRCUIT BREAKER
            max_daily_loss_amount = self.account_balance * MAX_DAILY_LOSS
            if self.daily_loss >= max_daily_loss_amount:
                logger.warning(f"🚨 DAILY LOSS CIRCUIT BREAKER TRIGGERED: {self.daily_loss:.2f} >= {max_daily_loss_amount:.2f}")
                return False, f"Daily loss limit reached: {self.daily_loss:.2f} >= {max_daily_loss_amount:.2f}"
            
            # 2. MAXIMUM OPEN POSITIONS VALIDATION
            max_open_positions = getattr(config, 'max_open_positions', 10)
            if len(self.positions) >= max_open_positions:
                logger.warning(f"🚨 MAX OPEN POSITIONS REACHED: {len(self.positions)} >= {max_open_positions}")
                return False, f"Maximum open positions reached: {len(self.positions)} >= {max_open_positions}"
            
            # 3. PER-TRADE RISK VALIDATION
            if risk_percentage > self.max_risk_per_trade:
                logger.warning(f"🚨 TRADE RISK EXCEEDED: {risk_percentage:.2%} > {self.max_risk_per_trade:.2%}")
                return False, f"Trade risk exceeds limit: {risk_percentage:.2%} > {self.max_risk_per_trade:.2%}"
            
            # 4. PORTFOLIO RISK VALIDATION
            if self.current_risk + risk_percentage > self.max_portfolio_risk:
                logger.warning(f"🚨 PORTFOLIO RISK EXCEEDED: {self.current_risk + risk_percentage:.2%} > {self.max_portfolio_risk:.2%}")
                return False, f"Portfolio risk would exceed limit: {self.current_risk + risk_percentage:.2%} > {self.max_portfolio_risk:.2%}"
            
            # 5. SYMBOL CONCENTRATION VALIDATION
            if symbol:
                symbol_exposure = sum(
                    p.get("final_adjusted_risk", 0) for p in self.positions.values() 
                    if p.get("symbol") == symbol
                )
                max_symbol_exposure = getattr(config, 'max_symbol_exposure', 0.20)  # 20% max per symbol
                if symbol_exposure + risk_percentage > max_symbol_exposure:
                    logger.warning(f"🚨 SYMBOL CONCENTRATION EXCEEDED: {symbol} {symbol_exposure + risk_percentage:.2%} > {max_symbol_exposure:.2%}")
                    return False, f"Symbol concentration would exceed limit: {symbol_exposure + risk_percentage:.2%} > {max_symbol_exposure:.2%}"
                
                # 6. ACCOUNT EXPOSURE VALIDATION
                total_exposure = self.current_risk + risk_percentage
                max_account_exposure = getattr(config, 'max_account_exposure', 0.80)  # 80% max account exposure
                if total_exposure > max_account_exposure:
                    logger.warning(f"🚨 ACCOUNT EXPOSURE EXCEEDED: {total_exposure:.2%} > {max_account_exposure:.2%}")
                    return False, f"Account exposure would exceed limit: {total_exposure:.2%} > {max_account_exposure:.2%}"
                
                # 7. CORRELATION LIMITS VALIDATION
                if getattr(config, 'enable_correlation_limits', True) and action:
                    current_positions = {}
                    for pos_id, pos_data in self.positions.items():
                        pos_symbol = pos_data.get('symbol')
                        if pos_symbol and pos_symbol not in current_positions:
                            current_positions[pos_symbol] = pos_data
                    
                    allowed, reason, analysis = await self.correlation_manager.check_correlation_limits(
                        symbol, action, risk_percentage, current_positions
                    )
                    
                    if not allowed:
                        logger.warning(f"🚨 CORRELATION LIMIT VIOLATION: {reason}")
                        return False, f"Correlation limit violation: {reason}"
                    
                    # Log correlation analysis for monitoring
                    if analysis.get('recommendation') == 'warning':
                        logger.info(f"⚠️ Correlation warning for {symbol}: {analysis}")
                
            # 8. VOLATILITY-BASED VALIDATION (ATR percentile scaling)
            if hasattr(config, 'enable_volatility_limits') and config.enable_volatility_limits:
                volatility_check = await self._check_volatility_limits(symbol, risk_percentage, atr_current, timeframe)
                if not volatility_check[0]:
                    logger.warning(f"🚨 VOLATILITY LIMIT VIOLATION: {volatility_check[1]}")
                    return volatility_check
            
            # 9. DRAWDOWN PROTECTION
            current_drawdown = (self.daily_loss / self.account_balance) * 100 if self.account_balance > 0 else 0
            max_drawdown = getattr(config, 'max_drawdown', 15.0)  # 15% max drawdown
            if current_drawdown > max_drawdown:
                logger.warning(f"🚨 DRAWDOWN LIMIT EXCEEDED: {current_drawdown:.2f}% > {max_drawdown:.2f}%")
                return False, f"Drawdown limit exceeded: {current_drawdown:.2f}% > {max_drawdown:.2f}%"
            
            # 10. CLUSTER HEAT VALIDATION (portfolio concentration by currency blocks)
            try:
                cluster_map = {
                    'USD_BLOCK': ['EUR_USD','GBP_USD','AUD_USD','NZD_USD','USD_JPY','USD_CHF','USD_CAD'],
                    'JPY_CROSSES': ['USD_JPY','EUR_JPY','GBP_JPY','CAD_JPY','AUD_JPY','CHF_JPY'],
                    'CHF_BLOCK': ['USD_CHF','EUR_CHF','GBP_CHF','CHF_JPY']
                }
                # Determine cluster(s) for symbol
                symbol_clusters = [k for k,v in cluster_map.items() if symbol in v] if symbol else []
                if symbol_clusters:
                    # Compute current cluster heat
                    cluster_limit = float(getattr(config, 'max_cluster_heat', 0.35))  # 35% default
                    for c in symbol_clusters:
                        heat = 0.0
                        for p in self.positions.values():
                            ps = p.get('symbol')
                            if ps in cluster_map[c]:
                                heat += float(p.get('final_adjusted_risk', p.get('adjusted_risk', 0.0)))
                        projected = heat + risk_percentage
                        if projected > cluster_limit:
                            msg = f"Cluster heat {c} would exceed limit: {projected:.2%} > {cluster_limit:.2%}"
                            logger.warning(f"🚨 {msg}")
                            return False, msg
            except Exception as e:
                logger.warning(f"Cluster heat check skipped: {e}")

            logger.info(f"✅ Trade validation passed for {symbol} {action} with {risk_percentage:.2%} risk")
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
    
    async def _check_volatility_limits(self, symbol: str, risk_percentage: float, atr_current: Optional[float] = None, timeframe: Optional[str] = None) -> Tuple[bool, str]:
        """Check if trade violates volatility-based limits"""
        try:
            # Compute ATR ratio against historical baseline
            # Baseline from config if available; otherwise use atr_current as baseline
            hist_key = f"{symbol}_historical_atr"
            historical_atr = float(getattr(config, hist_key, 0.0) or 0.0)
            cur_atr = float(atr_current or 0.0)
            if cur_atr <= 0.0:
                # no data; pass check
                return True, "Volatility check skipped (no ATR)"
            if historical_atr <= 0.0:
                # fallback: treat current as baseline
                historical_atr = cur_atr
            ratio = cur_atr / historical_atr if historical_atr > 0 else 1.0
            # Policy: block if >1.8x baseline; scale limits between 1.4–1.8x
            if ratio >= 1.8:
                return False, f"ATR spike {ratio:.2f}× baseline (>1.8×)"
            if ratio >= 1.4:
                scaled_max = self.max_risk_per_trade * 0.5
                if risk_percentage > scaled_max:
                    return False, f"Risk {risk_percentage:.2%} exceeds vol-scaled cap {scaled_max:.2%} (ATR {ratio:.2f}×)"
            return True, "Volatility check passed"
        except Exception as e:
            logger.error(f"Error in volatility limits check: {e}")
            return True, "Volatility check skipped due to error"

    async def calculate_atr_based_position_size(self,
                                              account_balance: float,
                                              risk_percentage: float,
                                              symbol: str,
                                              current_price: float,
                                              atr_value: float,
                                              stop_loss_distance: float,
                                              leverage: float = 1.0) -> Tuple[float, Dict[str, Any]]:
        """
        Institutional-grade ATR-based position sizing with leverage calculations.
        
        Args:
            account_balance: Current account balance
            risk_percentage: Risk percentage (0.01 = 1%)
            symbol: Trading instrument
            current_price: Current market price
            atr_value: Current ATR value
            stop_loss_distance: Distance to stop loss in price units
            leverage: Account leverage multiplier
            
        Returns:
            Tuple of (position_size, calculation_details)
        """
        try:
            # Calculate risk amount in account currency
            risk_amount = account_balance * risk_percentage
            
            # ATR-based position sizing formula
            # Position Size = Risk Amount / (ATR * ATR_Multiplier)
            atr_multiplier = getattr(config, 'atr_multiplier', 2.0)  # 2x ATR for stop loss
            atr_stop_distance = atr_value * atr_multiplier
            
            # Use the smaller of ATR-based or user-defined stop distance
            effective_stop_distance = min(atr_stop_distance, stop_loss_distance)
            
            # Calculate position size based on risk amount and stop distance
            if effective_stop_distance > 0:
                position_size = risk_amount / effective_stop_distance
            else:
                position_size = 0
                logger.warning(f"Invalid stop distance for {symbol}: {effective_stop_distance}")
            
            # Apply leverage
            leveraged_position_size = position_size * leverage
            
            # Apply volatility adjustment
            volatility_factor = await self._calculate_volatility_factor(symbol, atr_value)
            adjusted_position_size = leveraged_position_size * volatility_factor
            
            # Apply correlation penalty
            correlation_penalty = await self._get_correlation_penalty(symbol)
            final_position_size = adjusted_position_size * (1 - correlation_penalty)
            
            # Apply streak-based adjustment
            streak_factor = self._calculate_streak_factor()
            final_position_size *= streak_factor
            
            # Ensure position size is within limits
            max_position_size = account_balance * 0.1  # Max 10% of account per position
            final_position_size = min(final_position_size, max_position_size)
            
            calculation_details = {
                "risk_amount": risk_amount,
                "atr_value": atr_value,
                "atr_multiplier": atr_multiplier,
                "atr_stop_distance": atr_stop_distance,
                "effective_stop_distance": effective_stop_distance,
                "base_position_size": position_size,
                "leverage": leverage,
                "leveraged_position_size": leveraged_position_size,
                "volatility_factor": volatility_factor,
                "correlation_penalty": correlation_penalty,
                "streak_factor": streak_factor,
                "final_position_size": final_position_size,
                "max_position_size": max_position_size
            }
            
            logger.info(f"ATR-based position sizing for {symbol}: {final_position_size:.2f} units")
            return final_position_size, calculation_details
            
        except Exception as e:
            logger.error(f"Error in ATR-based position sizing: {e}")
            return 0.0, {"error": str(e)}

    async def _calculate_volatility_factor(self, symbol: str, current_atr: float) -> float:
        """Calculate volatility adjustment factor based on current ATR"""
        try:
            # Get historical ATR for comparison (simplified - in production, use real data)
            historical_atr = getattr(config, f'{symbol}_historical_atr', current_atr)
            
            if historical_atr > 0:
                volatility_ratio = current_atr / historical_atr
                
                # Cap volatility factor between 0.5 and 2.0
                volatility_factor = max(0.5, min(2.0, volatility_ratio))
                
                # Additional adjustment based on market regime
                market_regime = await self._detect_market_regime(symbol)
                regime_multipliers = {
                    "high_volatility": 0.7,
                    "normal": 1.0,
                    "low_volatility": 1.2
                }
                
                regime_multiplier = regime_multipliers.get(market_regime, 1.0)
                return volatility_factor * regime_multiplier
            else:
                return 1.0
                
        except Exception as e:
            logger.error(f"Error calculating volatility factor: {e}")
            return 1.0

    async def _detect_market_regime(self, symbol: str) -> str:
        """Detect current market regime for volatility adjustment"""
        try:
            # Simplified regime detection (in production, use sophisticated analysis)
            # This would typically analyze VIX, market breadth, etc.
            return "normal"  # Placeholder
        except Exception as e:
            logger.error(f"Error detecting market regime: {e}")
            return "normal"

    async def calculate_volatility_adjusted_size(self, 
                                              base_size: float, 
                                              symbol: str, 
                                              current_atr: float,
                                              historical_atr: float,
                                              market_regime: str = "normal") -> float:
        """
        Institutional-grade position sizing with volatility adjustment.
        
        Args:
            base_size: Base position size
            symbol: Trading instrument
            current_atr: Current ATR value
            historical_atr: Historical average ATR
            market_regime: Current market regime (normal, volatile, quiet)
        
        Returns:
            Adjusted position size
        """
        # Volatility adjustment factor
        volatility_ratio = current_atr / historical_atr if historical_atr > 0 else 1.0
        
        # Regime-based adjustments
        regime_multipliers = {
            "normal": 1.0,
            "volatile": 0.7,  # Reduce size in volatile markets
            "quiet": 1.2      # Increase size in quiet markets
        }
        
        regime_multiplier = regime_multipliers.get(market_regime, 1.0)
        
        # Instrument-specific volatility caps
        volatility_caps = {
            "EUR_USD": 1.5,
            "GBP_USD": 1.3,
            "USD_JPY": 1.2,
            "AUD_USD": 1.4
        }
        
        volatility_cap = volatility_caps.get(symbol, 1.0)
        
        # Calculate adjusted size
        volatility_factor = min(volatility_ratio, volatility_cap)
        adjusted_size = base_size * volatility_factor * regime_multiplier
        
        # Apply correlation penalty if needed
        correlation_penalty = await self._get_correlation_penalty(symbol)
        adjusted_size *= (1 - correlation_penalty)
        
        logger.info(f"Volatility-adjusted size for {symbol}: base={base_size:.2f}, "
                   f"vol_ratio={volatility_ratio:.2f}, regime={market_regime}, "
                   f"final={adjusted_size:.2f}")
        
        return adjusted_size

    async def calculate_streak_adjusted_position_size(self,
                                                   base_size: float,
                                                   symbol: str,
                                                   win_streak: int = 0,
                                                   loss_streak: int = 0) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate position size with streak-based adjustments.
        
        Institutional approach to position sizing based on recent performance:
        - Win streaks: Gradually increase position size (up to 50% increase)
        - Loss streaks: Gradually decrease position size (up to 50% decrease)
        - Consecutive losses trigger more aggressive size reduction
        
        Args:
            base_size: Base position size
            symbol: Trading instrument
            win_streak: Current win streak count
            loss_streak: Current loss streak count
            
        Returns:
            Tuple of (adjusted_size, adjustment_details)
        """
        try:
            # Win streak adjustments (conservative approach)
            if win_streak >= 1:
                win_multiplier = min(1.5, 1.0 + (win_streak - 1) * 0.1)  # Max 50% increase
            else:
                win_multiplier = 1.0
            
            # Loss streak adjustments (more aggressive)
            if loss_streak >= 1:
                loss_multiplier = max(0.5, 1.0 - (loss_streak * 0.15))  # Max 50% decrease
            else:
                loss_multiplier = 1.0
            
            # Apply both adjustments
            streak_adjusted_size = base_size * win_multiplier * loss_multiplier
            
            # Additional safety: If loss streak >= 3, cap at 25% of base size
            if loss_streak >= 3:
                streak_adjusted_size = min(streak_adjusted_size, base_size * 0.25)
            
            # Additional safety: If win streak >= 5, cap at 150% of base size
            if win_streak >= 5:
                streak_adjusted_size = min(streak_adjusted_size, base_size * 1.5)
            
            adjustment_details = {
                "base_size": base_size,
                "win_streak": win_streak,
                "loss_streak": loss_streak,
                "win_multiplier": win_multiplier,
                "loss_multiplier": loss_multiplier,
                "combined_multiplier": win_multiplier * loss_multiplier,
                "adjusted_size": streak_adjusted_size,
                "size_change_percent": ((streak_adjusted_size - base_size) / base_size) * 100
            }
            
            logger.info(f"Streak-adjusted size for {symbol}: base={base_size:.2f}, "
                       f"win_streak={win_streak}, loss_streak={loss_streak}, "
                       f"final={streak_adjusted_size:.2f}")
            
            return streak_adjusted_size, adjustment_details
            
        except Exception as e:
            logger.error(f"Error in streak-based position sizing: {e}")
            return base_size, {"error": str(e)}

    async def calculate_comprehensive_position_size(self,
                                                  account_balance: float,
                                                  risk_percentage: float,
                                                  symbol: str,
                                                  current_price: float,
                                                  atr_value: float,
                                                  stop_loss_distance: float,
                                                  leverage: float = 1.0,
                                                  market_regime: str = "normal") -> Tuple[float, Dict[str, Any]]:
        """
        Comprehensive position sizing combining all institutional methods:
        - ATR-based sizing
        - Volatility adjustments
        - Streak-based adjustments
        - Correlation penalties
        - Risk limits
        
        Args:
            account_balance: Current account balance
            risk_percentage: Risk percentage (0.01 = 1%)
            symbol: Trading instrument
            current_price: Current market price
            atr_value: Current ATR value
            stop_loss_distance: Distance to stop loss
            leverage: Account leverage
            market_regime: Current market regime
            
        Returns:
            Tuple of (final_position_size, comprehensive_details)
        """
        try:
            # Step 1: ATR-based base sizing
            atr_size, atr_details = await self.calculate_atr_based_position_size(
                account_balance, risk_percentage, symbol, current_price, 
                atr_value, stop_loss_distance, leverage
            )
            
            # Step 2: Volatility adjustment
            historical_atr = getattr(config, f'{symbol}_historical_atr', atr_value)
            vol_adjusted_size = await self.calculate_volatility_adjusted_size(
                atr_size, symbol, atr_value, historical_atr, market_regime
            )
            
            # Step 3: Streak-based adjustment
            streak_size, streak_details = await self.calculate_streak_adjusted_position_size(
                vol_adjusted_size, symbol, self.win_streak, self.loss_streak
            )
            
            # Step 4: Final risk validation
            final_risk = (streak_size * stop_loss_distance) / account_balance
            if final_risk > self.max_risk_per_trade:
                # Scale down to meet risk limits
                scale_factor = self.max_risk_per_trade / final_risk
                streak_size *= scale_factor
                logger.warning(f"Scaled down position size to meet risk limits: {scale_factor:.2f}")
            
            comprehensive_details = {
                "account_balance": account_balance,
                "symbol": symbol,
                "current_price": current_price,
                "atr_value": atr_value,
                "stop_loss_distance": stop_loss_distance,
                "leverage": leverage,
                "market_regime": market_regime,
                "atr_based_size": atr_size,
                "volatility_adjusted_size": vol_adjusted_size,
                "streak_adjusted_size": streak_size,
                "final_position_size": streak_size,
                "final_risk_percentage": (streak_size * stop_loss_distance) / account_balance,
                "atr_details": atr_details,
                "streak_details": streak_details
            }
            
            logger.info(f"Comprehensive position sizing for {symbol}: {streak_size:.2f} units")
            return streak_size, comprehensive_details
            
        except Exception as e:
            logger.error(f"Error in comprehensive position sizing: {e}")
            return 0.0, {"error": str(e)}
    
    async def _get_correlation_penalty(self, symbol: str) -> float:
        """Calculate correlation penalty for highly correlated positions"""
        correlated_positions = self._get_correlated_instruments(symbol)
        active_correlated = sum(1 for pos in self.positions.values() 
                               if pos['symbol'] in correlated_positions)
        
        if active_correlated >= 2:
            return 0.15  # 15% penalty for high correlation
        elif active_correlated == 1:
            return 0.05  # 5% penalty for moderate correlation
        return 0.0

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