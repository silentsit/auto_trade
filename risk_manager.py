#
# file: risk_manager.py
#
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)
from config import config
from correlation_manager import CorrelationManager

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
                    # DISABLED: Allow both BUY and SELL positions for same symbol
                    # if symbol in current_positions:
                    #     existing_action = current_positions[symbol].get('action', 'BUY')
                    #     if existing_action != action:
                    #         logger.info(f"[SAME-PAIR CONFLICT] Blocking {symbol} {action} - existing {existing_action} position found")
                    #         return False, f"Same-pair conflict: Cannot open {action} position while {existing_action} position exists for {symbol}"
                    
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