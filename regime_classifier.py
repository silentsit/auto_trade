import asyncio
import statistics
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from utils import logger, get_module_logger
from config import config

class LorentzianDistanceClassifier:
    def __init__(self, lookback_period: int = 20, max_history: int = 1000):
        """Initialize with history limits"""
        self.lookback_period = lookback_period
        self.max_history = max_history
        self.price_history = {}  # symbol -> List[float]
        self.regime_history = {} # symbol -> List[str] (history of classified regimes)
        self.volatility_history = {} # symbol -> List[float]
        self.atr_history = {}  # symbol -> List[float]
        self.regimes = {}  # symbol -> Dict[str, Any] (stores latest regime data)
        self._lock = asyncio.Lock()
        self.logger = get_module_logger(__name__)

    async def add_price_data(self, symbol: str, price: float, timeframe: str, atr: Optional[float] = None):
        """Add price data with limits"""
        async with self._lock:
            # Initialize if needed
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            if symbol not in self.regime_history:
                self.regime_history[symbol] = []
            if symbol not in self.volatility_history:
                self.volatility_history[symbol] = []
            if symbol not in self.atr_history:
                self.atr_history[symbol] = []
            # Add new data
            self.price_history[symbol].append(price)
            # Enforce history limit
            if len(self.price_history[symbol]) > self.max_history:
                self.price_history[symbol] = self.price_history[symbol][-self.max_history:]
            # Update ATR history if provided
            if atr is not None:
                self.atr_history[symbol].append(atr)
                if len(self.atr_history[symbol]) > self.lookback_period:
                    self.atr_history[symbol].pop(0)
            # Update regime if we have enough data
            if len(self.price_history[symbol]) >= 2:
                await self.classify_market_regime(symbol, price, atr)

    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """Calculate Lorentzian distance between current price and historical prices"""
        if not history:
            return 0.0
        distances = [np.log(1 + abs(price - hist_price)) for hist_price in history]
        return float(np.mean(distances)) if distances else 0.0

    async def classify_market_regime(self, symbol: str, current_price: float, atr: Optional[float] = None) -> Dict[str, Any]:
        """Classify current market regime using multiple factors"""
        async with self._lock:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
                unknown_state = {"regime": "unknown", "volatility": 0.0, "momentum": 0.0, "price_distance": 0.0, "regime_strength": 0.0, "last_update": datetime.now(timezone.utc).isoformat()}
                self.regimes[symbol] = unknown_state
                return unknown_state
            try:
                price_distance = await self.calculate_lorentzian_distance(
                    current_price, self.price_history[symbol][:-1]
                )
                returns = []
                prices = self.price_history[symbol]
                for i in range(1, len(prices)):
                    if prices[i-1] != 0:
                        returns.append(prices[i] / prices[i-1] - 1)
                volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
                momentum = (current_price - prices[0]) / prices[0] if prices[0] != 0 else 0.0
                mean_atr = np.mean(self.atr_history[symbol]) if self.atr_history.get(symbol) else 0.0
                regime = "unknown"
                regime_strength = 0.5
                if price_distance < 0.1 and volatility < 0.001:
                    regime = "ranging"
                    regime_strength = min(1.0, 0.7 + (0.1 - price_distance) * 3)
                elif price_distance > 0.3 and abs(momentum) > 0.002:
                    regime = "trending_up" if momentum > 0 else "trending_down"
                    regime_strength = min(1.0, 0.6 + price_distance + abs(momentum) * 10)
                elif volatility > 0.003 or (mean_atr > 0 and atr is not None and atr > 1.5 * mean_atr):
                    regime = "volatile"
                    regime_strength = min(1.0, 0.6 + volatility * 100)
                elif abs(momentum) > 0.003:
                    regime = "momentum_up" if momentum > 0 else "momentum_down"
                    regime_strength = min(1.0, 0.6 + abs(momentum) * 50)
                else:
                    regime = "mixed"
                    regime_strength = 0.5
                self.regime_history[symbol].append(regime)
                if len(self.regime_history[symbol]) > self.lookback_period:
                    self.regime_history[symbol].pop(0)
                self.volatility_history[symbol].append(volatility)
                if len(self.volatility_history[symbol]) > self.lookback_period:
                    self.volatility_history[symbol].pop(0)
                result = {
                    "regime": regime,
                    "regime_strength": regime_strength,
                    "price_distance": price_distance,
                    "volatility": volatility,
                    "momentum": momentum,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "metrics": {
                        "price_distance": price_distance,
                        "volatility": volatility,
                        "momentum": momentum,
                        "lookback_period": self.lookback_period
                    }
                }
                self.regimes[symbol] = result
                if "classification_history" not in self.regimes[symbol]:
                    self.regimes[symbol]["classification_history"] = []
                self.regimes[symbol]["classification_history"].append({
                    "regime": regime,
                    "strength": regime_strength,
                    "timestamp": result["last_update"]
                })
                hist_limit = 20
                if len(self.regimes[symbol]["classification_history"]) > hist_limit:
                    self.regimes[symbol]["classification_history"] = self.regimes[symbol]["classification_history"][-hist_limit:]
                return result
            except Exception as e:
                self.logger.error(f"Error classifying regime for {symbol}: {str(e)}", exc_info=True)
                error_state = {"regime": "error", "regime_strength": 0.0, "last_update": datetime.now(timezone.utc).isoformat(), "error": str(e)}
                self.regimes[symbol] = error_state
                return error_state

    def get_dominant_regime(self, symbol: str) -> str:
        """Get the dominant regime over recent history (uses internal state)"""
        if symbol not in self.regime_history or len(self.regime_history[symbol]) < 3:
            return "unknown"
        recent_regimes = self.regime_history[symbol][-5:]
        regime_counts = {}
        for regime in recent_regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        if regime_counts:
            dominant_regime, count = max(regime_counts.items(), key=lambda item: item[1])
            if count / len(recent_regimes) >= 0.6:
                return dominant_regime
        return "mixed"

#     async def should_adjust_exits(self, symbol: str, current_regime: Optional[str] = None) -> Tuple[bool, Dict[str, float]]:
#         """Determine if exit levels should be adjusted based on regime stability and type"""
#         pass

    def get_regime_data(self, symbol: str) -> Dict[str, Any]:
        """Get the latest calculated market regime data for a symbol"""
        regime_data = self.regimes.get(symbol, {})
        if "last_update" not in regime_data:
            regime_data["last_update"] = datetime.now(timezone.utc).isoformat()
        elif isinstance(regime_data["last_update"], datetime):
            regime_data["last_update"] = regime_data["last_update"].isoformat()
        if not regime_data:
            return {
                "regime": "unknown",
                "regime_strength": 0.0,
                "last_update": datetime.now(timezone.utc).isoformat()
            }
        return regime_data.copy()

    def is_suitable_for_strategy(self, symbol: str, strategy_type: str) -> bool:
        """Determine if the current market regime is suitable for a strategy"""
        regime_data = self.regimes.get(symbol)
        if not regime_data or "regime" not in regime_data:
            self.logger.warning(f"No regime data found for {symbol}, allowing strategy '{strategy_type}' by default.")
            return True
        regime = regime_data["regime"]
        self.logger.debug(f"Checking strategy suitability for {symbol}: Strategy='{strategy_type}', Regime='{regime}'")
        if strategy_type == "trend_following":
            is_suitable = "trending" in regime
        elif strategy_type == "mean_reversion":
            is_suitable = regime in ["ranging", "mixed"]
        elif strategy_type == "breakout":
            is_suitable = regime in ["ranging", "volatile"]
        elif strategy_type == "momentum":
            is_suitable = "momentum" in regime
        else:
            self.logger.warning(f"Unknown or default strategy type '{strategy_type}', allowing trade by default.")
            is_suitable = True
        self.logger.info(f"Strategy '{strategy_type}' suitability for {symbol} in regime '{regime}': {is_suitable}")
        return is_suitable

    async def clear_history(self, symbol: str):
        """Clear historical data and current regime for a symbol"""
        async with self._lock:
            if symbol in self.price_history: del self.price_history[symbol]
            if symbol in self.regime_history: del self.regime_history[symbol]
            if symbol in self.volatility_history: del self.volatility_history[symbol]
            if symbol in self.atr_history: del self.atr_history[symbol]
            if symbol in self.regimes: del self.regimes[symbol]
            self.logger.info(f"Cleared history and regime data for {symbol}")