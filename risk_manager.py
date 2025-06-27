import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from utils import logger
from config import config
from correlation_manager import CorrelationManager

# Get max daily loss from config with proper fallback
MAX_DAILY_LOSS = getattr(config, 'max_daily_loss', 10.0) / 100.0  # Default to 10% if not set

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
            self.max_risk_per_trade = config.max_risk_percentage / 100.0
        else:
            self.max_risk_per_trade = 0.20 # Fallback if not in config for some reason

        if max_portfolio_risk is not None:
            self.max_portfolio_risk = max_portfolio_risk
        elif hasattr(config, 'max_portfolio_heat'):
            self.max_portfolio_risk = config.max_portfolio_heat / 100.0
        else:
            self.max_portfolio_risk = 0.70 # Fallback

        self.account_balance = 0.0
        self.positions = {}  # position_id -> risk data
        self.current_risk = 0.0  # Current portfolio risk exposure
        self.daily_loss = 0.0  # Track daily loss for circuit breaker
        self.drawdown = 0.0  # Current drawdown
        self._lock = asyncio.Lock()
        
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
            self.account_balance = float(account_balance)
            logger.info(f"Risk manager initialized with balance: {self.account_balance}")
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
                    # Convert position data format for correlation manager
                    current_positions = {}
                    for pos_id, pos_data in self.positions.items():
                        pos_symbol = pos_data.get('symbol')
                        if pos_symbol:
                            current_positions[pos_symbol] = pos_data
                    
                    allowed, reason, analysis = await self.correlation_manager.check_correlation_limits(
                        symbol, action, risk_percentage, current_positions
                    )
                    
                    if not allowed:
                        return False, f"Correlation limit violation: {reason}"
                    
                    # Log correlation analysis for monitoring
                    if analysis.get('recommendation') == 'warning':
                        logger.warning(f"Correlation warning for {symbol}: {analysis}")
            
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
            logger.debug(f"Updated streaks: wins={self.win_streak}, losses={self.loss_streak}")
    
    async def clear_position(self, position_id: str):
        async with self._lock:
            if position_id in self.positions:
                position = self.positions[position_id]
                self.current_risk -= position.get("adjusted_risk", 0)
                self.current_risk = max(0, self.current_risk)
                del self.positions[position_id]
                logger.info(f"Cleared position {position_id} from risk tracking")
                return True
            return False
    
    async def get_risk_metrics(self) -> Dict[str, Any]:
        async with self._lock:
            symbol_counts = {}
            symbol_risks = {}
            for position in self.positions.values():
                symbol = position.get("symbol")
                if symbol:
                    symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
                    symbol_risks[symbol] = symbol_risks.get(symbol, 0) + position.get("adjusted_risk", 0)
            max_symbol = None
            max_risk = 0
            for symbol, risk in symbol_risks.items():
                if risk > max_risk:
                    max_risk = risk
                    max_symbol = symbol
            return {
                "current_risk": self.current_risk,
                "max_risk": self.max_portfolio_risk,
                "remaining_risk": max(0, self.max_portfolio_risk - self.current_risk),
                "daily_loss": self.daily_loss,
                "daily_loss_limit": self.account_balance * MAX_DAILY_LOSS,
                "drawdown": self.drawdown,
                "position_count": len(self.positions),
                "symbols": list(symbol_counts.keys()),
                "symbol_counts": symbol_counts,
                "symbol_risks": symbol_risks,
                "highest_concentration": {
                    "symbol": max_symbol,
                    "risk": max_risk
                },
                "win_streak": self.win_streak,
                "loss_streak": self.loss_streak
            }

    def calculate_position_units(self, equity: float, target_percent: float, leverage: float, current_price: float) -> int:
        """
        Calculate position size (units) based on account equity, leverage, and target percent of equity.
        
        Args:
            equity (float): Total available account equity.
            target_percent (float): Desired position size as a percentage of equity (e.g., 10 for 10% or 0.1 for 10%).
            leverage (float): Account leverage (e.g., 20 for 20:1).
            current_price (float): Current market price of the instrument.
        
        Returns:
            int: Number of units to trade.
        """
        # Ensure percent is in decimal form (e.g., 10% → 0.1)
        percent = target_percent / 100 if target_percent > 1 else target_percent
        total_position_value = equity * leverage * percent
        if current_price <= 0:
            return 0
        units = int(total_position_value / current_price)
        return units
