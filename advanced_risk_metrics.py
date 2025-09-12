"""
ADVANCED RISK METRICS SYSTEM
Institutional-grade risk measurement and monitoring
"""

import asyncio
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import math
from scipy import stats
from scipy.optimize import minimize

logger = logging.getLogger(__name__)

class RiskMetricType(Enum):
    VALUE_AT_RISK = "var"
    EXPECTED_SHORTFALL = "es"
    MAXIMUM_DRAWDOWN = "mdd"
    SHARPE_RATIO = "sharpe"
    SORTINO_RATIO = "sortino"
    CALMAR_RATIO = "calmar"
    BETA = "beta"
    CORRELATION_RISK = "correlation"
    CONCENTRATION_RISK = "concentration"
    LIQUIDITY_RISK = "liquidity"
    REGIME_RISK = "regime"
    TAIL_RISK = "tail"
    STRESS_TEST = "stress"

@dataclass
class RiskMetric:
    """Risk metric calculation result"""
    metric_type: RiskMetricType
    value: float
    confidence_level: float
    time_horizon: str
    calculation_method: str
    timestamp: str
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if not self.metadata:
            self.metadata = {}

@dataclass
class PortfolioRiskProfile:
    """Comprehensive portfolio risk profile"""
    total_var_95: float
    total_var_99: float
    expected_shortfall_95: float
    expected_shortfall_99: float
    max_drawdown: float
    current_drawdown: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    portfolio_beta: float
    concentration_risk: float
    correlation_risk: float
    liquidity_risk: float
    regime_risk: float
    tail_risk: float
    stress_test_results: Dict[str, float]
    risk_score: float
    risk_level: str
    recommendations: List[str]
    timestamp: str

class AdvancedRiskCalculator:
    """Advanced risk metrics calculator"""
    
    def __init__(self):
        self.risk_cache = {}
        self.historical_data = {}
        self.correlation_matrix = {}
        self.volatility_estimates = {}
        
    async def calculate_var(self, returns: List[float], confidence_level: float = 0.95, 
                           method: str = "historical") -> RiskMetric:
        """Calculate Value at Risk"""
        try:
            returns_array = np.array(returns)
            
            if method == "historical":
                var_value = np.percentile(returns_array, (1 - confidence_level) * 100)
            elif method == "parametric":
                mean_return = np.mean(returns_array)
                std_return = np.std(returns_array)
                var_value = mean_return + stats.norm.ppf(1 - confidence_level) * std_return
            elif method == "monte_carlo":
                var_value = self._monte_carlo_var(returns_array, confidence_level)
            else:
                raise ValueError(f"Unknown VaR method: {method}")
            
            return RiskMetric(
                metric_type=RiskMetricType.VALUE_AT_RISK,
                value=float(var_value),
                confidence_level=confidence_level,
                time_horizon="1d",
                calculation_method=method,
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"sample_size": len(returns)}
            )
            
        except Exception as e:
            logger.error(f"VaR calculation failed: {e}")
            return None
    
    async def calculate_expected_shortfall(self, returns: List[float], 
                                         confidence_level: float = 0.95) -> RiskMetric:
        """Calculate Expected Shortfall (Conditional VaR)"""
        try:
            returns_array = np.array(returns)
            var_threshold = np.percentile(returns_array, (1 - confidence_level) * 100)
            
            # Calculate ES as mean of returns below VaR threshold
            tail_returns = returns_array[returns_array <= var_threshold]
            es_value = np.mean(tail_returns) if len(tail_returns) > 0 else var_threshold
            
            return RiskMetric(
                metric_type=RiskMetricType.EXPECTED_SHORTFALL,
                value=float(es_value),
                confidence_level=confidence_level,
                time_horizon="1d",
                calculation_method="historical",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"tail_observations": len(tail_returns)}
            )
            
        except Exception as e:
            logger.error(f"Expected Shortfall calculation failed: {e}")
            return None
    
    async def calculate_maximum_drawdown(self, prices: List[float]) -> RiskMetric:
        """Calculate Maximum Drawdown"""
        try:
            prices_array = np.array(prices)
            peak = np.maximum.accumulate(prices_array)
            drawdown = (prices_array - peak) / peak
            max_dd = np.min(drawdown)
            
            # Find drawdown duration
            dd_duration = self._calculate_drawdown_duration(prices_array)
            
            return RiskMetric(
                metric_type=RiskMetricType.MAXIMUM_DRAWDOWN,
                value=float(max_dd),
                confidence_level=1.0,
                time_horizon="all",
                calculation_method="peak_to_trough",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"duration_days": dd_duration}
            )
            
        except Exception as e:
            logger.error(f"Maximum Drawdown calculation failed: {e}")
            return None
    
    async def calculate_sharpe_ratio(self, returns: List[float], 
                                   risk_free_rate: float = 0.02) -> RiskMetric:
        """Calculate Sharpe Ratio"""
        try:
            returns_array = np.array(returns)
            excess_returns = returns_array - risk_free_rate / 252  # Daily risk-free rate
            
            if np.std(returns_array) == 0:
                sharpe = 0.0
            else:
                sharpe = np.mean(excess_returns) / np.std(returns_array) * np.sqrt(252)
            
            return RiskMetric(
                metric_type=RiskMetricType.SHARPE_RATIO,
                value=float(sharpe),
                confidence_level=1.0,
                time_horizon="1y",
                calculation_method="annualized",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"risk_free_rate": risk_free_rate}
            )
            
        except Exception as e:
            logger.error(f"Sharpe Ratio calculation failed: {e}")
            return None
    
    async def calculate_sortino_ratio(self, returns: List[float], 
                                    risk_free_rate: float = 0.02) -> RiskMetric:
        """Calculate Sortino Ratio (downside deviation)"""
        try:
            returns_array = np.array(returns)
            excess_returns = returns_array - risk_free_rate / 252
            
            # Calculate downside deviation
            negative_returns = excess_returns[excess_returns < 0]
            downside_deviation = np.sqrt(np.mean(negative_returns ** 2)) if len(negative_returns) > 0 else 0
            
            if downside_deviation == 0:
                sortino = 0.0
            else:
                sortino = np.mean(excess_returns) / downside_deviation * np.sqrt(252)
            
            return RiskMetric(
                metric_type=RiskMetricType.SORTINO_RATIO,
                value=float(sortino),
                confidence_level=1.0,
                time_horizon="1y",
                calculation_method="downside_deviation",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"downside_observations": len(negative_returns)}
            )
            
        except Exception as e:
            logger.error(f"Sortino Ratio calculation failed: {e}")
            return None
    
    async def calculate_calmar_ratio(self, returns: List[float], 
                                   max_drawdown: float) -> RiskMetric:
        """Calculate Calmar Ratio"""
        try:
            returns_array = np.array(returns)
            annual_return = np.mean(returns_array) * 252
            
            if max_drawdown == 0:
                calmar = 0.0
            else:
                calmar = annual_return / abs(max_drawdown)
            
            return RiskMetric(
                metric_type=RiskMetricType.CALMAR_RATIO,
                value=float(calmar),
                confidence_level=1.0,
                time_horizon="1y",
                calculation_method="annual_return_mdd",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"annual_return": annual_return, "max_drawdown": max_drawdown}
            )
            
        except Exception as e:
            logger.error(f"Calmar Ratio calculation failed: {e}")
            return None
    
    async def calculate_beta(self, portfolio_returns: List[float], 
                           market_returns: List[float]) -> RiskMetric:
        """Calculate portfolio beta"""
        try:
            portfolio_array = np.array(portfolio_returns)
            market_array = np.array(market_returns)
            
            if len(portfolio_array) != len(market_array):
                logger.error("Portfolio and market returns length mismatch")
                return None
            
            # Calculate beta using covariance
            covariance = np.cov(portfolio_array, market_array)[0, 1]
            market_variance = np.var(market_array)
            
            beta = covariance / market_variance if market_variance != 0 else 0
            
            return RiskMetric(
                metric_type=RiskMetricType.BETA,
                value=float(beta),
                confidence_level=1.0,
                time_horizon="1y",
                calculation_method="covariance",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"covariance": covariance, "market_variance": market_variance}
            )
            
        except Exception as e:
            logger.error(f"Beta calculation failed: {e}")
            return None
    
    async def calculate_concentration_risk(self, position_weights: Dict[str, float]) -> RiskMetric:
        """Calculate concentration risk using Herfindahl-Hirschman Index"""
        try:
            weights = np.array(list(position_weights.values()))
            hhi = np.sum(weights ** 2)
            
            # Normalize HHI (0 = perfectly diversified, 1 = completely concentrated)
            n = len(weights)
            normalized_hhi = (hhi - 1/n) / (1 - 1/n) if n > 1 else 1.0
            
            return RiskMetric(
                metric_type=RiskMetricType.CONCENTRATION_RISK,
                value=float(normalized_hhi),
                confidence_level=1.0,
                time_horizon="current",
                calculation_method="hhi",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"hhi": hhi, "num_positions": n}
            )
            
        except Exception as e:
            logger.error(f"Concentration risk calculation failed: {e}")
            return None
    
    async def calculate_correlation_risk(self, returns_matrix: Dict[str, List[float]]) -> RiskMetric:
        """Calculate portfolio correlation risk"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(returns_matrix)
            correlation_matrix = df.corr()
            
            # Calculate average correlation (excluding diagonal)
            n = len(correlation_matrix)
            if n <= 1:
                avg_correlation = 0.0
            else:
                mask = np.triu(np.ones_like(correlation_matrix, dtype=bool), k=1)
                correlations = correlation_matrix.values[mask]
                avg_correlation = np.mean(correlations) if len(correlations) > 0 else 0.0
            
            # Calculate correlation risk score (higher = more correlated = riskier)
            correlation_risk = abs(avg_correlation)
            
            return RiskMetric(
                metric_type=RiskMetricType.CORRELATION_RISK,
                value=float(correlation_risk),
                confidence_level=1.0,
                time_horizon="1y",
                calculation_method="average_correlation",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"avg_correlation": avg_correlation, "num_assets": n}
            )
            
        except Exception as e:
            logger.error(f"Correlation risk calculation failed: {e}")
            return None
    
    async def calculate_liquidity_risk(self, position_sizes: Dict[str, float], 
                                    daily_volumes: Dict[str, float]) -> RiskMetric:
        """Calculate liquidity risk based on position size vs daily volume"""
        try:
            liquidity_ratios = []
            
            for symbol, size in position_sizes.items():
                if symbol in daily_volumes and daily_volumes[symbol] > 0:
                    ratio = size / daily_volumes[symbol]
                    liquidity_ratios.append(ratio)
            
            if not liquidity_ratios:
                return RiskMetric(
                    metric_type=RiskMetricType.LIQUIDITY_RISK,
                    value=1.0,  # Maximum risk if no volume data
                    confidence_level=1.0,
                    time_horizon="1d",
                    calculation_method="position_volume_ratio",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    metadata={"error": "No volume data available"}
                )
            
            # Calculate average liquidity risk
            avg_liquidity_risk = np.mean(liquidity_ratios)
            max_liquidity_risk = np.max(liquidity_ratios)
            
            # Use maximum risk as portfolio liquidity risk
            portfolio_liquidity_risk = min(1.0, max_liquidity_risk)
            
            return RiskMetric(
                metric_type=RiskMetricType.LIQUIDITY_RISK,
                value=float(portfolio_liquidity_risk),
                confidence_level=1.0,
                time_horizon="1d",
                calculation_method="position_volume_ratio",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"avg_ratio": avg_liquidity_risk, "max_ratio": max_liquidity_risk}
            )
            
        except Exception as e:
            logger.error(f"Liquidity risk calculation failed: {e}")
            return None
    
    async def calculate_tail_risk(self, returns: List[float]) -> RiskMetric:
        """Calculate tail risk using higher moments"""
        try:
            returns_array = np.array(returns)
            
            # Calculate skewness and kurtosis
            skewness = stats.skew(returns_array)
            kurtosis = stats.kurtosis(returns_array)
            
            # Tail risk score (higher = more tail risk)
            tail_risk = abs(skewness) + abs(kurtosis - 3) / 3
            
            return RiskMetric(
                metric_type=RiskMetricType.TAIL_RISK,
                value=float(tail_risk),
                confidence_level=1.0,
                time_horizon="1y",
                calculation_method="skewness_kurtosis",
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata={"skewness": skewness, "kurtosis": kurtosis}
            )
            
        except Exception as e:
            logger.error(f"Tail risk calculation failed: {e}")
            return None
    
    async def run_stress_tests(self, portfolio_data: Dict[str, Any]) -> Dict[str, float]:
        """Run comprehensive stress tests"""
        try:
            stress_results = {}
            
            # Market crash scenario (-20% across all positions)
            crash_scenario = self._simulate_market_crash(portfolio_data)
            stress_results["market_crash_20pct"] = crash_scenario
            
            # Volatility spike scenario (3x normal volatility)
            vol_spike_scenario = self._simulate_volatility_spike(portfolio_data)
            stress_results["volatility_spike_3x"] = vol_spike_scenario
            
            # Correlation breakdown scenario (all correlations -> 1)
            correlation_breakdown = self._simulate_correlation_breakdown(portfolio_data)
            stress_results["correlation_breakdown"] = correlation_breakdown
            
            # Liquidity crisis scenario (50% reduction in liquidity)
            liquidity_crisis = self._simulate_liquidity_crisis(portfolio_data)
            stress_results["liquidity_crisis_50pct"] = liquidity_crisis
            
            # Interest rate shock scenario
            rate_shock = self._simulate_interest_rate_shock(portfolio_data)
            stress_results["interest_rate_shock"] = rate_shock
            
            return stress_results
            
        except Exception as e:
            logger.error(f"Stress testing failed: {e}")
            return {}
    
    def _monte_carlo_var(self, returns: np.ndarray, confidence_level: float, 
                        simulations: int = 10000) -> float:
        """Monte Carlo VaR calculation"""
        try:
            mean_return = np.mean(returns)
            std_return = np.std(returns)
            
            # Generate random returns
            random_returns = np.random.normal(mean_return, std_return, simulations)
            
            # Calculate VaR
            var_value = np.percentile(random_returns, (1 - confidence_level) * 100)
            return var_value
            
        except Exception as e:
            logger.error(f"Monte Carlo VaR failed: {e}")
            return np.percentile(returns, (1 - confidence_level) * 100)
    
    def _calculate_drawdown_duration(self, prices: np.ndarray) -> int:
        """Calculate maximum drawdown duration in days"""
        try:
            peak = np.maximum.accumulate(prices)
            drawdown = (prices - peak) / peak
            
            # Find drawdown periods
            in_drawdown = drawdown < 0
            drawdown_periods = []
            current_period = 0
            
            for is_dd in in_drawdown:
                if is_dd:
                    current_period += 1
                else:
                    if current_period > 0:
                        drawdown_periods.append(current_period)
                        current_period = 0
            
            if current_period > 0:
                drawdown_periods.append(current_period)
            
            return max(drawdown_periods) if drawdown_periods else 0
            
        except Exception as e:
            logger.error(f"Drawdown duration calculation failed: {e}")
            return 0
    
    def _simulate_market_crash(self, portfolio_data: Dict[str, Any]) -> float:
        """Simulate 20% market crash scenario"""
        try:
            positions = portfolio_data.get("positions", {})
            total_pnl_impact = 0.0
            
            for symbol, position in positions.items():
                position_value = position.get("value", 0.0)
                crash_impact = position_value * -0.20  # 20% loss
                total_pnl_impact += crash_impact
            
            return total_pnl_impact
            
        except Exception as e:
            logger.error(f"Market crash simulation failed: {e}")
            return 0.0
    
    def _simulate_volatility_spike(self, portfolio_data: Dict[str, Any]) -> float:
        """Simulate 3x volatility spike scenario"""
        try:
            # Simplified volatility impact calculation
            positions = portfolio_data.get("positions", {})
            total_pnl_impact = 0.0
            
            for symbol, position in positions.items():
                position_value = position.get("value", 0.0)
                volatility = position.get("volatility", 0.02)  # 2% daily vol
                spike_impact = position_value * volatility * 3 * -1.96  # 95% confidence
                total_pnl_impact += spike_impact
            
            return total_pnl_impact
            
        except Exception as e:
            logger.error(f"Volatility spike simulation failed: {e}")
            return 0.0
    
    def _simulate_correlation_breakdown(self, portfolio_data: Dict[str, Any]) -> float:
        """Simulate correlation breakdown scenario"""
        try:
            # When correlations go to 1, diversification benefits disappear
            positions = portfolio_data.get("positions", {})
            total_exposure = sum(pos.get("value", 0.0) for pos in positions.values())
            
            # Assume worst-case scenario where all positions move together
            correlation_impact = total_exposure * -0.10  # 10% loss across all positions
            
            return correlation_impact
            
        except Exception as e:
            logger.error(f"Correlation breakdown simulation failed: {e}")
            return 0.0
    
    def _simulate_liquidity_crisis(self, portfolio_data: Dict[str, Any]) -> float:
        """Simulate liquidity crisis scenario"""
        try:
            positions = portfolio_data.get("positions", {})
            total_pnl_impact = 0.0
            
            for symbol, position in positions.items():
                position_value = position.get("value", 0.0)
                liquidity_impact = position_value * -0.05  # 5% liquidity discount
                total_pnl_impact += liquidity_impact
            
            return total_pnl_impact
            
        except Exception as e:
            logger.error(f"Liquidity crisis simulation failed: {e}")
            return 0.0
    
    def _simulate_interest_rate_shock(self, portfolio_data: Dict[str, Any]) -> float:
        """Simulate interest rate shock scenario"""
        try:
            # Simplified interest rate impact
            positions = portfolio_data.get("positions", {})
            total_pnl_impact = 0.0
            
            for symbol, position in positions.items():
                position_value = position.get("value", 0.0)
                duration = position.get("duration", 1.0)  # Simplified duration
                rate_shock = 0.02  # 200bp rate shock
                rate_impact = position_value * duration * rate_shock * -1
                total_pnl_impact += rate_impact
            
            return total_pnl_impact
            
        except Exception as e:
            logger.error(f"Interest rate shock simulation failed: {e}")
            return 0.0

class PortfolioRiskManager:
    """Portfolio-level risk management"""
    
    def __init__(self, risk_calculator: AdvancedRiskCalculator):
        self.risk_calculator = risk_calculator
        self.risk_thresholds = {
            "var_95": 0.05,  # 5% of portfolio
            "var_99": 0.02,  # 2% of portfolio
            "max_drawdown": 0.15,  # 15% max drawdown
            "concentration": 0.30,  # 30% max concentration
            "correlation": 0.70,  # 70% max correlation
            "liquidity": 0.20,  # 20% max liquidity risk
            "tail_risk": 2.0,  # Tail risk threshold
        }
    
    async def calculate_portfolio_risk_profile(self, portfolio_data: Dict[str, Any]) -> PortfolioRiskProfile:
        """Calculate comprehensive portfolio risk profile"""
        try:
            # Extract data
            returns = portfolio_data.get("returns", [])
            prices = portfolio_data.get("prices", [])
            positions = portfolio_data.get("positions", {})
            market_returns = portfolio_data.get("market_returns", [])
            
            # Calculate individual risk metrics
            var_95 = await self.risk_calculator.calculate_var(returns, 0.95)
            var_99 = await self.risk_calculator.calculate_var(returns, 0.99)
            es_95 = await self.risk_calculator.calculate_expected_shortfall(returns, 0.95)
            es_99 = await self.risk_calculator.calculate_expected_shortfall(returns, 0.99)
            mdd = await self.risk_calculator.calculate_maximum_drawdown(prices)
            sharpe = await self.risk_calculator.calculate_sharpe_ratio(returns)
            sortino = await self.risk_calculator.calculate_sortino_ratio(returns)
            
            # Calculate current drawdown
            current_price = prices[-1] if prices else 0
            peak_price = max(prices) if prices else current_price
            current_dd = (current_price - peak_price) / peak_price if peak_price > 0 else 0
            
            # Calculate Calmar ratio
            calmar = await self.risk_calculator.calculate_calmar_ratio(returns, mdd.value if mdd else 0)
            
            # Calculate beta
            beta = await self.risk_calculator.calculate_beta(returns, market_returns)
            
            # Calculate concentration risk
            position_weights = {symbol: pos.get("weight", 0.0) for symbol, pos in positions.items()}
            concentration = await self.risk_calculator.calculate_concentration_risk(position_weights)
            
            # Calculate correlation risk
            returns_matrix = {symbol: pos.get("returns", []) for symbol, pos in positions.items()}
            correlation = await self.risk_calculator.calculate_correlation_risk(returns_matrix)
            
            # Calculate liquidity risk
            position_sizes = {symbol: pos.get("value", 0.0) for symbol, pos in positions.items()}
            daily_volumes = {symbol: pos.get("daily_volume", 1000000) for symbol, pos in positions.items()}
            liquidity = await self.risk_calculator.calculate_liquidity_risk(position_sizes, daily_volumes)
            
            # Calculate tail risk
            tail_risk = await self.risk_calculator.calculate_tail_risk(returns)
            
            # Run stress tests
            stress_tests = await self.risk_calculator.run_stress_tests(portfolio_data)
            
            # Calculate overall risk score
            risk_score = self._calculate_risk_score({
                "var_95": var_95.value if var_95 else 0,
                "var_99": var_99.value if var_99 else 0,
                "max_drawdown": abs(mdd.value) if mdd else 0,
                "concentration": concentration.value if concentration else 0,
                "correlation": correlation.value if correlation else 0,
                "liquidity": liquidity.value if liquidity else 0,
                "tail_risk": tail_risk.value if tail_risk else 0,
            })
            
            # Determine risk level
            risk_level = self._determine_risk_level(risk_score)
            
            # Generate recommendations
            recommendations = self._generate_recommendations({
                "var_95": var_95,
                "var_99": var_99,
                "mdd": mdd,
                "concentration": concentration,
                "correlation": correlation,
                "liquidity": liquidity,
                "tail_risk": tail_risk,
            })
            
            return PortfolioRiskProfile(
                total_var_95=var_95.value if var_95 else 0,
                total_var_99=var_99.value if var_99 else 0,
                expected_shortfall_95=es_95.value if es_95 else 0,
                expected_shortfall_99=es_99.value if es_99 else 0,
                max_drawdown=mdd.value if mdd else 0,
                current_drawdown=current_dd,
                sharpe_ratio=sharpe.value if sharpe else 0,
                sortino_ratio=sortino.value if sortino else 0,
                calmar_ratio=calmar.value if calmar else 0,
                portfolio_beta=beta.value if beta else 0,
                concentration_risk=concentration.value if concentration else 0,
                correlation_risk=correlation.value if correlation else 0,
                liquidity_risk=liquidity.value if liquidity else 0,
                regime_risk=0.0,  # Placeholder
                tail_risk=tail_risk.value if tail_risk else 0,
                stress_test_results=stress_tests,
                risk_score=risk_score,
                risk_level=risk_level,
                recommendations=recommendations,
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            
        except Exception as e:
            logger.error(f"Portfolio risk profile calculation failed: {e}")
            return None
    
    def _calculate_risk_score(self, risk_metrics: Dict[str, float]) -> float:
        """Calculate overall risk score (0-100)"""
        try:
            weights = {
                "var_95": 0.25,
                "var_99": 0.20,
                "max_drawdown": 0.20,
                "concentration": 0.15,
                "correlation": 0.10,
                "liquidity": 0.05,
                "tail_risk": 0.05
            }
            
            weighted_score = 0.0
            total_weight = 0.0
            
            for metric, value in risk_metrics.items():
                if metric in weights:
                    # Normalize to 0-100 scale
                    normalized_value = min(100, max(0, abs(value) * 100))
                    weighted_score += normalized_value * weights[metric]
                    total_weight += weights[metric]
            
            return weighted_score / total_weight if total_weight > 0 else 0.0
            
        except Exception as e:
            logger.error(f"Risk score calculation failed: {e}")
            return 50.0  # Default medium risk
    
    def _determine_risk_level(self, risk_score: float) -> str:
        """Determine risk level based on score"""
        if risk_score < 25:
            return "LOW"
        elif risk_score < 50:
            return "MEDIUM"
        elif risk_score < 75:
            return "HIGH"
        else:
            return "CRITICAL"
    
    def _generate_recommendations(self, risk_metrics: Dict[str, RiskMetric]) -> List[str]:
        """Generate risk management recommendations"""
        recommendations = []
        
        # VaR recommendations
        if risk_metrics.get("var_95") and abs(risk_metrics["var_95"].value) > self.risk_thresholds["var_95"]:
            recommendations.append("Reduce position sizes to lower VaR exposure")
        
        # Drawdown recommendations
        if risk_metrics.get("mdd") and abs(risk_metrics["mdd"].value) > self.risk_thresholds["max_drawdown"]:
            recommendations.append("Implement stricter stop-losses to limit drawdown")
        
        # Concentration recommendations
        if risk_metrics.get("concentration") and risk_metrics["concentration"].value > self.risk_thresholds["concentration"]:
            recommendations.append("Diversify portfolio to reduce concentration risk")
        
        # Correlation recommendations
        if risk_metrics.get("correlation") and risk_metrics["correlation"].value > self.risk_thresholds["correlation"]:
            recommendations.append("Add uncorrelated assets to improve diversification")
        
        # Liquidity recommendations
        if risk_metrics.get("liquidity") and risk_metrics["liquidity"].value > self.risk_thresholds["liquidity"]:
            recommendations.append("Reduce position sizes in illiquid instruments")
        
        # Tail risk recommendations
        if risk_metrics.get("tail_risk") and risk_metrics["tail_risk"].value > self.risk_thresholds["tail_risk"]:
            recommendations.append("Implement tail risk hedging strategies")
        
        if not recommendations:
            recommendations.append("Portfolio risk levels are within acceptable limits")
        
        return recommendations

# Global risk components
risk_calculator = AdvancedRiskCalculator()
portfolio_risk_manager = PortfolioRiskManager(risk_calculator)

# Convenience functions
async def calculate_portfolio_risk(portfolio_data: Dict[str, Any]) -> PortfolioRiskProfile:
    """Calculate comprehensive portfolio risk profile"""
    return await portfolio_risk_manager.calculate_portfolio_risk_profile(portfolio_data)

async def calculate_var(returns: List[float], confidence_level: float = 0.95) -> RiskMetric:
    """Calculate Value at Risk"""
    return await risk_calculator.calculate_var(returns, confidence_level)

async def run_stress_tests(portfolio_data: Dict[str, Any]) -> Dict[str, float]:
    """Run portfolio stress tests"""
    return await risk_calculator.run_stress_tests(portfolio_data)
