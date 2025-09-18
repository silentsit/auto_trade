"""
INSTITUTIONAL PORTFOLIO OPTIMIZATION
Advanced portfolio construction and risk management for multi-asset trading
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from scipy.optimize import minimize
from scipy.stats import norm
import asyncio

logger = logging.getLogger(__name__)

@dataclass
class AssetAllocation:
    """Asset allocation recommendation"""
    symbol: str
    weight: float
    expected_return: float
    risk_contribution: float
    sharpe_ratio: float
    max_position_size: float
    recommended_size: float

@dataclass
class PortfolioMetrics:
    """Portfolio performance metrics"""
    expected_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    var_95: float  # Value at Risk 95%
    cvar_95: float  # Conditional Value at Risk 95%
    diversification_ratio: float
    concentration_risk: float

class PortfolioOptimizer:
    """
    Institutional-grade portfolio optimization using modern portfolio theory
    and advanced risk management techniques
    """
    
    def __init__(self):
        self.lookback_periods = 252  # 1 year of daily data
        self.risk_free_rate = 0.02  # 2% risk-free rate
        self.max_weight_per_asset = 0.20  # Maximum 20% per asset
        self.min_weight_per_asset = 0.01  # Minimum 1% per asset
        self.target_volatility = 0.15  # 15% target volatility
        
    async def optimize_portfolio(self, 
                               symbols: List[str],
                               historical_returns: Dict[str, List[float]],
                               current_prices: Dict[str, float],
                               account_balance: float,
                               risk_tolerance: float = 0.15) -> Dict[str, Any]:
        """
        Optimize portfolio allocation using mean-variance optimization
        
        Args:
            symbols: List of trading symbols
            historical_returns: Dict of symbol -> returns list
            current_prices: Dict of symbol -> current price
            account_balance: Total account balance
            risk_tolerance: Target volatility (0.15 = 15%)
            
        Returns:
            Optimized portfolio allocation
        """
        try:
            # Prepare data
            returns_matrix = self._prepare_returns_matrix(symbols, historical_returns)
            
            if returns_matrix is None or returns_matrix.shape[0] < 30:
                logger.warning("Insufficient data for portfolio optimization")
                return self._equal_weight_fallback(symbols, account_balance, current_prices)
            
            # Calculate expected returns and covariance matrix
            expected_returns = self._calculate_expected_returns(returns_matrix)
            cov_matrix = self._calculate_covariance_matrix(returns_matrix)
            
            # Risk parity optimization
            risk_parity_weights = self._risk_parity_optimization(cov_matrix)
            
            # Mean-variance optimization
            mv_weights = self._mean_variance_optimization(expected_returns, cov_matrix, risk_tolerance)
            
            # Black-Litterman optimization (if we have views)
            bl_weights = self._black_litterman_optimization(expected_returns, cov_matrix, symbols)
            
            # Combine strategies with equal weighting
            final_weights = self._combine_strategies([risk_parity_weights, mv_weights, bl_weights])
            
            # Generate allocation recommendations
            allocations = self._generate_allocations(
                symbols, final_weights, expected_returns, 
                cov_matrix, account_balance, current_prices
            )
            
            # Calculate portfolio metrics
            portfolio_metrics = self._calculate_portfolio_metrics(
                final_weights, expected_returns, cov_matrix
            )
            
            return {
                "allocations": allocations,
                "portfolio_metrics": portfolio_metrics,
                "optimization_method": "multi_strategy",
                "risk_tolerance": risk_tolerance,
                "rebalance_frequency": "weekly"
            }
            
        except Exception as e:
            logger.error(f"Portfolio optimization failed: {e}")
            return self._equal_weight_fallback(symbols, account_balance, current_prices)
    
    def _prepare_returns_matrix(self, symbols: List[str], historical_returns: Dict[str, List[float]]) -> Optional[np.ndarray]:
        """Prepare returns matrix for optimization"""
        try:
            # Find common length
            min_length = min(len(returns) for returns in historical_returns.values())
            if min_length < 30:
                return None
            
            # Align returns to same length
            aligned_returns = {}
            for symbol in symbols:
                if symbol in historical_returns:
                    aligned_returns[symbol] = historical_returns[symbol][-min_length:]
            
            # Create returns matrix
            returns_matrix = np.array([aligned_returns[symbol] for symbol in symbols])
            return returns_matrix
            
        except Exception as e:
            logger.error(f"Error preparing returns matrix: {e}")
            return None
    
    def _calculate_expected_returns(self, returns_matrix: np.ndarray) -> np.ndarray:
        """Calculate expected returns using multiple methods"""
        # Method 1: Historical mean
        historical_mean = np.mean(returns_matrix, axis=1)
        
        # Method 2: Exponentially weighted mean (more recent data weighted higher)
        alpha = 0.1  # Decay factor
        weights = np.array([alpha * (1 - alpha) ** i for i in range(returns_matrix.shape[1])])
        weights = weights[::-1]  # Reverse to weight recent data more
        weights = weights / np.sum(weights)
        
        ew_mean = np.average(returns_matrix, axis=1, weights=weights)
        
        # Method 3: Shrinkage towards equal returns
        equal_returns = np.full(len(historical_mean), np.mean(historical_mean))
        shrinkage_factor = 0.3
        shrunk_returns = (1 - shrinkage_factor) * historical_mean + shrinkage_factor * equal_returns
        
        # Combine methods
        final_returns = 0.4 * historical_mean + 0.4 * ew_mean + 0.2 * shrunk_returns
        
        return final_returns
    
    def _calculate_covariance_matrix(self, returns_matrix: np.ndarray) -> np.ndarray:
        """Calculate covariance matrix with shrinkage"""
        # Sample covariance
        sample_cov = np.cov(returns_matrix)
        
        # Shrinkage target (equal correlation model)
        n_assets = returns_matrix.shape[0]
        avg_var = np.trace(sample_cov) / n_assets
        target = np.eye(n_assets) * avg_var
        
        # Shrinkage intensity
        shrinkage_intensity = 0.2
        
        # Shrunk covariance matrix
        shrunk_cov = (1 - shrinkage_intensity) * sample_cov + shrinkage_intensity * target
        
        return shrunk_cov
    
    def _risk_parity_optimization(self, cov_matrix: np.ndarray) -> np.ndarray:
        """Risk parity optimization - equal risk contribution"""
        n_assets = cov_matrix.shape[0]
        
        def risk_parity_objective(weights):
            weights = np.array(weights)
            portfolio_vol = np.sqrt(np.dot(weights, np.dot(cov_matrix, weights)))
            
            # Risk contributions
            risk_contrib = (weights * np.dot(cov_matrix, weights)) / portfolio_vol
            
            # Target risk contribution (equal for all assets)
            target_risk_contrib = 1.0 / n_assets
            
            # Sum of squared deviations from target
            return np.sum((risk_contrib - target_risk_contrib) ** 2)
        
        # Constraints
        constraints = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}
        bounds = [(self.min_weight_per_asset, self.max_weight_per_asset) for _ in range(n_assets)]
        
        # Initial guess (equal weights)
        x0 = np.ones(n_assets) / n_assets
        
        # Optimize
        result = minimize(risk_parity_objective, x0, method='SLSQP', 
                        bounds=bounds, constraints=constraints)
        
        if result.success:
            return result.x
        else:
            logger.warning("Risk parity optimization failed, using equal weights")
            return np.ones(n_assets) / n_assets
    
    def _mean_variance_optimization(self, expected_returns: np.ndarray, 
                                  cov_matrix: np.ndarray, target_vol: float) -> np.ndarray:
        """Mean-variance optimization with target volatility"""
        n_assets = len(expected_returns)
        
        def portfolio_volatility(weights):
            return np.sqrt(np.dot(weights, np.dot(cov_matrix, weights)))
        
        def negative_sharpe(weights):
            portfolio_ret = np.dot(weights, expected_returns)
            portfolio_vol = portfolio_volatility(weights)
            if portfolio_vol == 0:
                return 0
            return -(portfolio_ret - self.risk_free_rate) / portfolio_vol
        
        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
            {'type': 'eq', 'fun': lambda w: portfolio_volatility(w) - target_vol}
        ]
        bounds = [(self.min_weight_per_asset, self.max_weight_per_asset) for _ in range(n_assets)]
        
        # Initial guess
        x0 = np.ones(n_assets) / n_assets
        
        # Optimize
        result = minimize(negative_sharpe, x0, method='SLSQP', 
                        bounds=bounds, constraints=constraints)
        
        if result.success:
            return result.x
        else:
            logger.warning("Mean-variance optimization failed, using equal weights")
            return np.ones(n_assets) / n_assets
    
    def _black_litterman_optimization(self, expected_returns: np.ndarray, 
                                    cov_matrix: np.ndarray, symbols: List[str]) -> np.ndarray:
        """Black-Litterman optimization with market views"""
        n_assets = len(expected_returns)
        
        # Market capitalization weights (simplified - equal weights)
        market_weights = np.ones(n_assets) / n_assets
        
        # Market implied returns
        risk_aversion = 3.0  # Risk aversion parameter
        market_returns = risk_aversion * np.dot(cov_matrix, market_weights)
        
        # Views matrix (no specific views for now)
        P = np.eye(n_assets)  # Identity matrix (no views)
        Q = expected_returns  # View returns
        Omega = np.eye(n_assets) * 0.01  # Uncertainty in views
        
        # Black-Litterman formula
        tau = 0.05  # Scaling factor
        M1 = np.linalg.inv(tau * cov_matrix)
        M2 = np.dot(P.T, np.dot(np.linalg.inv(Omega), P))
        M3 = np.dot(P.T, np.dot(np.linalg.inv(Omega), Q))
        
        # New expected returns
        new_expected_returns = np.dot(np.linalg.inv(M1 + M2), 
                                    np.dot(M1, market_returns) + M3)
        
        # New covariance matrix
        new_cov_matrix = np.linalg.inv(M1 + M2)
        
        # Optimize with new parameters
        return self._mean_variance_optimization(new_expected_returns, new_cov_matrix, 0.15)
    
    def _combine_strategies(self, strategy_weights: List[np.ndarray]) -> np.ndarray:
        """Combine multiple optimization strategies"""
        # Equal weighting of strategies
        combined_weights = np.mean(strategy_weights, axis=0)
        
        # Normalize to ensure weights sum to 1
        combined_weights = combined_weights / np.sum(combined_weights)
        
        return combined_weights
    
    def _generate_allocations(self, symbols: List[str], weights: np.ndarray,
                            expected_returns: np.ndarray, cov_matrix: np.ndarray,
                            account_balance: float, current_prices: Dict[str, float]) -> List[AssetAllocation]:
        """Generate detailed allocation recommendations"""
        allocations = []
        
        for i, symbol in enumerate(symbols):
            weight = weights[i]
            expected_return = expected_returns[i]
            current_price = current_prices.get(symbol, 1.0)
            
            # Calculate risk contribution
            portfolio_vol = np.sqrt(np.dot(weights, np.dot(cov_matrix, weights)))
            risk_contrib = (weight * np.dot(cov_matrix[i], weights)) / portfolio_vol if portfolio_vol > 0 else 0
            
            # Calculate Sharpe ratio
            asset_vol = np.sqrt(cov_matrix[i, i])
            sharpe_ratio = (expected_return - self.risk_free_rate) / asset_vol if asset_vol > 0 else 0
            
            # Calculate position sizes
            allocation_value = account_balance * weight
            max_position_size = account_balance * self.max_weight_per_asset
            recommended_size = allocation_value / current_price if current_price > 0 else 0
            
            allocations.append(AssetAllocation(
                symbol=symbol,
                weight=weight,
                expected_return=expected_return,
                risk_contribution=risk_contrib,
                sharpe_ratio=sharpe_ratio,
                max_position_size=max_position_size,
                recommended_size=recommended_size
            ))
        
        return allocations
    
    def _calculate_portfolio_metrics(self, weights: np.ndarray, expected_returns: np.ndarray,
                                   cov_matrix: np.ndarray) -> PortfolioMetrics:
        """Calculate comprehensive portfolio metrics"""
        # Expected return and volatility
        portfolio_return = np.dot(weights, expected_returns)
        portfolio_vol = np.sqrt(np.dot(weights, np.dot(cov_matrix, weights)))
        
        # Sharpe ratio
        sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_vol if portfolio_vol > 0 else 0
        
        # Value at Risk (95% confidence)
        var_95 = norm.ppf(0.05) * portfolio_vol
        
        # Conditional Value at Risk (Expected Shortfall)
        cvar_95 = -norm.pdf(norm.ppf(0.05)) / 0.05 * portfolio_vol
        
        # Diversification ratio
        weighted_vol = np.sum(weights * np.sqrt(np.diag(cov_matrix)))
        diversification_ratio = weighted_vol / portfolio_vol if portfolio_vol > 0 else 1
        
        # Concentration risk (Herfindahl index)
        concentration_risk = np.sum(weights ** 2)
        
        # Maximum drawdown (simplified estimation)
        max_drawdown = portfolio_vol * 2.5  # Rough estimate
        
        return PortfolioMetrics(
            expected_return=portfolio_return,
            volatility=portfolio_vol,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            var_95=var_95,
            cvar_95=cvar_95,
            diversification_ratio=diversification_ratio,
            concentration_risk=concentration_risk
        )
    
    def _equal_weight_fallback(self, symbols: List[str], account_balance: float, 
                             current_prices: Dict[str, float]) -> Dict[str, Any]:
        """Fallback to equal weight allocation"""
        n_assets = len(symbols)
        equal_weight = 1.0 / n_assets
        
        allocations = []
        for symbol in symbols:
            current_price = current_prices.get(symbol, 1.0)
            allocation_value = account_balance * equal_weight
            recommended_size = allocation_value / current_price if current_price > 0 else 0
            
            allocations.append(AssetAllocation(
                symbol=symbol,
                weight=equal_weight,
                expected_return=0.0,
                risk_contribution=equal_weight,
                sharpe_ratio=0.0,
                max_position_size=account_balance * self.max_weight_per_asset,
                recommended_size=recommended_size
            ))
        
        return {
            "allocations": allocations,
            "portfolio_metrics": PortfolioMetrics(
                expected_return=0.0,
                volatility=0.0,
                sharpe_ratio=0.0,
                max_drawdown=0.0,
                var_95=0.0,
                cvar_95=0.0,
                diversification_ratio=1.0,
                concentration_risk=equal_weight
            ),
            "optimization_method": "equal_weight_fallback",
            "risk_tolerance": 0.15,
            "rebalance_frequency": "monthly"
        }

class DynamicRebalancer:
    """
    Dynamic portfolio rebalancing based on market conditions
    """
    
    def __init__(self, portfolio_optimizer: PortfolioOptimizer):
        self.optimizer = portfolio_optimizer
        self.rebalance_threshold = 0.05  # 5% drift threshold
        self.volatility_threshold = 0.02  # 2% volatility change threshold
        
    async def should_rebalance(self, 
                             current_weights: Dict[str, float],
                             target_weights: Dict[str, float],
                             portfolio_volatility: float,
                             historical_volatility: float) -> Tuple[bool, str]:
        """
        Determine if portfolio should be rebalanced
        
        Args:
            current_weights: Current portfolio weights
            target_weights: Target portfolio weights
            portfolio_volatility: Current portfolio volatility
            historical_volatility: Historical average volatility
            
        Returns:
            Tuple of (should_rebalance, reason)
        """
        try:
            # Check weight drift
            max_drift = 0.0
            for symbol in current_weights:
                if symbol in target_weights:
                    drift = abs(current_weights[symbol] - target_weights[symbol])
                    max_drift = max(max_drift, drift)
            
            if max_drift > self.rebalance_threshold:
                return True, f"Weight drift {max_drift:.3f} exceeds threshold {self.rebalance_threshold}"
            
            # Check volatility regime change
            vol_change = abs(portfolio_volatility - historical_volatility) / historical_volatility
            if vol_change > self.volatility_threshold:
                return True, f"Volatility change {vol_change:.3f} exceeds threshold {self.volatility_threshold}"
            
            return False, "No rebalancing needed"
            
        except Exception as e:
            logger.error(f"Rebalancing check failed: {e}")
            return False, f"Error in rebalancing check: {e}"
    
    async def calculate_rebalance_trades(self,
                                       current_positions: Dict[str, float],
                                       target_allocations: List[AssetAllocation],
                                       account_balance: float) -> List[Dict[str, Any]]:
        """
        Calculate trades needed for rebalancing
        
        Args:
            current_positions: Current position sizes
            target_allocations: Target allocation recommendations
            account_balance: Current account balance
            
        Returns:
            List of rebalancing trades
        """
        trades = []
        
        try:
            for allocation in target_allocations:
                symbol = allocation.symbol
                target_size = allocation.recommended_size
                current_size = current_positions.get(symbol, 0.0)
                
                size_difference = target_size - current_size
                
                if abs(size_difference) > 0.01:  # Minimum trade size
                    trade = {
                        "symbol": symbol,
                        "action": "BUY" if size_difference > 0 else "SELL",
                        "size": abs(size_difference),
                        "current_size": current_size,
                        "target_size": target_size,
                        "reason": "rebalancing"
                    }
                    trades.append(trade)
            
            return trades
            
        except Exception as e:
            logger.error(f"Rebalancing trade calculation failed: {e}")
            return []
