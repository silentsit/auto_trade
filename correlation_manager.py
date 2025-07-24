import asyncio
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Set, Optional, Any
from dataclasses import dataclass
from collections import defaultdict, deque
import logging
from config import config
from utils import logger

@dataclass
class CorrelationData:
    """Data structure for storing correlation information"""
    correlation: float
    last_updated: datetime
    lookback_days: int
    sample_size: int
    strength: str  # 'high', 'medium', 'low'
    
    def is_stale(self, max_age_hours: int = 24) -> bool:
        """Check if correlation data is stale"""
        age = datetime.now(timezone.utc) - self.last_updated
        return age.total_seconds() > (max_age_hours * 3600)

@dataclass
class CurrencyExposure:
    """Track exposure to individual currencies"""
    currency: str
    long_exposure: float = 0.0
    short_exposure: float = 0.0
    net_exposure: float = 0.0
    gross_exposure: float = 0.0
    position_count: int = 0
    
    def update(self):
        """Update calculated fields"""
        self.net_exposure = self.long_exposure - self.short_exposure
        self.gross_exposure = self.long_exposure + self.short_exposure

class CorrelationManager:
    """
    Advanced correlation manager that tracks relationships between instruments
    and calculates currency exposure for forex pairs
    """
    
    def __init__(self):
        self.correlations: Dict[Tuple[str, str], CorrelationData] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.currency_exposure: Dict[str, CurrencyExposure] = {}
        self._lock = asyncio.Lock()
        
        # Predefined correlation groups for major forex pairs
        self.correlation_groups = {
            'EUR_MAJORS': ['EUR_USD', 'EUR_GBP', 'EUR_JPY', 'EUR_CHF', 'EUR_AUD', 'EUR_CAD'],
            'USD_MAJORS': ['EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF', 'AUD_USD', 'USD_CAD'],
            'JPY_CROSSES': ['EUR_JPY', 'GBP_JPY', 'USD_JPY', 'AUD_JPY', 'CAD_JPY', 'CHF_JPY'],
            'COMMODITY_CURRENCIES': ['AUD_USD', 'USD_CAD', 'NZD_USD'],
            'SAFE_HAVENS': ['USD_JPY', 'USD_CHF', 'EUR_CHF']
        }
        
        # Known high correlation pairs (static baseline)
        self.static_correlations = {
            ('EUR_USD', 'GBP_USD'): 0.75,
            ('EUR_USD', 'AUD_USD'): 0.65,
            ('EUR_USD', 'EUR_GBP'): 0.85,
            ('GBP_USD', 'EUR_GBP'): -0.70,
            ('USD_JPY', 'EUR_JPY'): 0.80,
            ('USD_CHF', 'EUR_CHF'): 0.85,
            ('AUD_USD', 'NZD_USD'): 0.90,
            ('USD_CAD', 'EUR_CAD'): 0.75,
        }
        
        logger.info("CorrelationManager initialized")
    
    async def add_price_data(self, symbol: str, price: float, timestamp: Optional[datetime] = None):
        """Add price data point for correlation calculation"""
        async with self._lock:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)
            
            self.price_history[symbol].append({
                'price': price,
                'timestamp': timestamp
            })
            
            # Clean old data
            cutoff = timestamp - timedelta(days=config.correlation_lookback_days + 5)
            while (self.price_history[symbol] and 
                   self.price_history[symbol][0]['timestamp'] < cutoff):
                self.price_history[symbol].popleft()
    
    async def calculate_correlation(self, symbol1: str, symbol2: str, 
                                  lookback_days: Optional[int] = None) -> Optional[CorrelationData]:
        """Calculate correlation between two instruments"""
        if lookback_days is None:
            lookback_days = config.correlation_lookback_days
        
        async with self._lock:
            # Check if we have enough data
            if (symbol1 not in self.price_history or 
                symbol2 not in self.price_history or
                len(self.price_history[symbol1]) < 10 or
                len(self.price_history[symbol2]) < 10):
                
                # Use static correlation if available
                key1 = (symbol1, symbol2)
                key2 = (symbol2, symbol1)
                if key1 in self.static_correlations:
                    correlation = self.static_correlations[key1]
                elif key2 in self.static_correlations:
                    correlation = self.static_correlations[key2]
                else:
                    return None
                
                return CorrelationData(
                    correlation=correlation,
                    last_updated=datetime.now(timezone.utc),
                    lookback_days=lookback_days,
                    sample_size=0,
                    strength=self._get_correlation_strength(correlation)
                )
            
            # Align timestamps and extract prices
            prices1, prices2 = self._align_price_data(symbol1, symbol2, lookback_days)
            
            if len(prices1) < 10:  # Need minimum data points
                return None
            
            # Calculate returns
            returns1 = np.diff(np.log(prices1))
            returns2 = np.diff(np.log(prices2))
            
            # Calculate correlation
            correlation = np.corrcoef(returns1, returns2)[0, 1]
            
            if np.isnan(correlation):
                correlation = 0.0
            
            correlation_data = CorrelationData(
                correlation=correlation,
                last_updated=datetime.now(timezone.utc),
                lookback_days=lookback_days,
                sample_size=len(returns1),
                strength=self._get_correlation_strength(correlation)
            )
            
            # Cache the result
            key = tuple(sorted([symbol1, symbol2]))
            self.correlations[key] = correlation_data
            
            return correlation_data
    
    def _align_price_data(self, symbol1: str, symbol2: str, lookback_days: int) -> Tuple[List[float], List[float]]:
        """Align price data between two symbols for correlation calculation"""
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Get price data within lookback period
        data1 = [d for d in self.price_history[symbol1] if d['timestamp'] >= cutoff]
        data2 = [d for d in self.price_history[symbol2] if d['timestamp'] >= cutoff]
        
        # Simple alignment - just use all available data points
        # In a production system, you'd want more sophisticated timestamp matching
        prices1 = [d['price'] for d in data1]
        prices2 = [d['price'] for d in data2]
        
        # Ensure equal length by truncating to minimum
        min_len = min(len(prices1), len(prices2))
        return prices1[-min_len:], prices2[-min_len:]
    
    def _get_correlation_strength(self, correlation: float) -> str:
        """Classify correlation strength"""
        abs_corr = abs(correlation)
        if abs_corr >= config.correlation_threshold_high:
            return 'high'
        elif abs_corr >= config.correlation_threshold_medium:
            return 'medium'
        else:
            return 'low'
    
    async def get_correlation(self, symbol1: str, symbol2: str, 
                            force_recalculate: bool = False) -> Optional[CorrelationData]:
        """Get correlation between two symbols"""
        key = tuple(sorted([symbol1, symbol2]))
        
        # Return cached correlation if not stale
        if not force_recalculate and key in self.correlations:
            correlation_data = self.correlations[key]
            if not correlation_data.is_stale():
                return correlation_data
        
        # Calculate new correlation
        return await self.calculate_correlation(symbol1, symbol2)
    
    async def get_correlated_instruments(self, symbol: str, 
                                       min_correlation: float = 0.5) -> List[Tuple[str, float]]:
        """Get list of instruments correlated with the given symbol"""
        correlated = []
        
        # Check against all instruments we have data for
        all_instruments = set(self.price_history.keys())
        
        for other_symbol in all_instruments:
            if other_symbol == symbol:
                continue
                
            correlation_data = await self.get_correlation(symbol, other_symbol)
            if correlation_data and abs(correlation_data.correlation) >= min_correlation:
                correlated.append((other_symbol, correlation_data.correlation))
        
        # Sort by correlation strength
        correlated.sort(key=lambda x: abs(x[1]), reverse=True)
        return correlated
    
    async def update_currency_exposure(self, positions: Dict[str, Dict[str, Any]]):
        """Update currency exposure based on current positions"""
        async with self._lock:
            # Reset exposure
            self.currency_exposure.clear()
            
            for symbol, position_data in positions.items():
                if not self._is_forex_pair(symbol):
                    continue
                
                # Parse forex pair
                base_currency, quote_currency = self._parse_forex_pair(symbol)
                if not base_currency or not quote_currency:
                    continue
                
                # Initialize currency exposure if not exists
                if base_currency not in self.currency_exposure:
                    self.currency_exposure[base_currency] = CurrencyExposure(base_currency)
                if quote_currency not in self.currency_exposure:
                    self.currency_exposure[quote_currency] = CurrencyExposure(quote_currency)
                
                # Calculate position value (simplified - assuming equal weighting)
                position_value = position_data.get('risk_percentage', 1.0)  # Using risk % as proxy
                action = position_data.get('action', 'BUY')
                
                if action == 'BUY':
                    # Long base currency, short quote currency
                    self.currency_exposure[base_currency].long_exposure += position_value
                    self.currency_exposure[quote_currency].short_exposure += position_value
                else:
                    # Short base currency, long quote currency
                    self.currency_exposure[base_currency].short_exposure += position_value
                    self.currency_exposure[quote_currency].long_exposure += position_value
                
                self.currency_exposure[base_currency].position_count += 1
                self.currency_exposure[quote_currency].position_count += 1
            
            # Update calculated fields
            for exposure in self.currency_exposure.values():
                exposure.update()
    
    def _is_forex_pair(self, symbol: str) -> bool:
        """Check if symbol is a forex pair"""
        return '_' in symbol and len(symbol.split('_')) == 2
    
    def _parse_forex_pair(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse forex pair into base and quote currencies"""
        if not self._is_forex_pair(symbol):
            return None, None
        
        parts = symbol.split('_')
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None
    
    async def check_correlation_limits(self, new_symbol: str, new_action: str, 
                                     new_risk: float, 
                                     current_positions: Dict[str, Dict[str, Any]]) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Check if adding a new position would create conflicts:
        1. Same-pair opposite direction conflicts (if enabled)
        2. Opposite direction conflicts with highly correlated pairs
        
        Returns: (allowed, reason, analysis)
        """
        if not config.enable_correlation_limits:
            return True, "Correlation limits disabled", {}
        
        # FIRST: Check same-pair conflicts (if enabled)
        if config.enable_same_pair_conflict_prevention:
            same_pair_allowed, same_pair_reason = self._check_same_pair_conflicts(
                new_symbol, new_action, current_positions
            )
            if not same_pair_allowed:
                analysis = {
                    'same_pair_conflict': True,
                    'conflicting_symbol': new_symbol,
                    'recommendation': 'block'
                }
                return False, same_pair_reason, analysis

        analysis = {
            'correlations': [],
            'opposite_direction_conflicts': [],
            'same_pair_conflict': False,
            'recommendation': 'allow'
        }
        
        try:
            # SECOND: Check for opposite direction conflicts on highly correlated pairs ONLY
            for symbol, position_data in current_positions.items():
                if symbol == new_symbol:
                    continue
                
                correlation_data = await self.get_correlation(new_symbol, symbol)
                if correlation_data:
                    position_risk = position_data.get('risk_percentage', 1.0) / 100.0
                    correlation_strength = abs(correlation_data.correlation)
                    existing_action = position_data.get('action', 'BUY')
                    
                    analysis['correlations'].append({
                        'symbol': symbol,
                        'correlation': correlation_data.correlation,
                        'strength': correlation_data.strength,
                        'position_risk': position_risk,
                        'existing_action': existing_action,
                        'new_action': new_action
                    })
                    
                    # CORE PROTECTION: Check for opposite direction conflicts on highly correlated pairs
                    if correlation_strength >= config.correlation_threshold_high:
                        # Check if this creates an opposite direction conflict
                        is_opposite_direction = self._is_opposite_direction_conflict(
                            correlation_data.correlation, existing_action, new_action
                        )
                        
                        if is_opposite_direction:
                            conflict_info = {
                                'existing_symbol': symbol,
                                'existing_action': existing_action,
                                'new_symbol': new_symbol,
                                'new_action': new_action,
                                'correlation': correlation_data.correlation,
                                'risk_conflict': True
                            }
                            analysis['opposite_direction_conflicts'].append(conflict_info)
                            
                            # BLOCK the trade - this is the ONLY correlation protection
                            return False, (
                                f"Opposite direction conflict: {symbol} {existing_action} vs "
                                f"{new_symbol} {new_action} (correlation: {correlation_data.correlation:+.1%}). "
                                f"Highly correlated pairs cannot trade in opposite directions concurrently."
                            ), analysis
            
            # If no conflicts found, allow the trade
            return True, "No conflicts detected", analysis
            
        except Exception as e:
            logger.error(f"Error checking correlation limits: {e}")
            return True, f"Error in correlation check: {e}", analysis

    def _check_same_pair_conflicts(self, new_symbol: str, new_action: str, 
                                 current_positions: Dict[str, Dict[str, Any]]) -> Tuple[bool, str]:
        """
        Check if adding a new position would create a same-pair opposite direction conflict.
        
        Args:
            new_symbol: Symbol for the new trade
            new_action: Action for the new trade (BUY/SELL)
            current_positions: Dictionary of current open positions
            
        Returns:
            (allowed, reason): Tuple indicating if trade is allowed and reason
        """
        # Check if there's already a position for this exact symbol
        if new_symbol in current_positions:
            existing_position = current_positions[new_symbol]
            existing_action = existing_position.get('action', 'BUY')
            
            # If opposite directions, block the trade
            if existing_action != new_action:
                logger.warning(
                    f"Same-pair conflict detected: {new_symbol} has existing {existing_action} "
                    f"position, blocking new {new_action} signal"
                )
                return False, (
                    f"Same-pair opposite direction conflict: Existing {new_symbol} {existing_action} "
                    f"position conflicts with new {new_action} signal. Cannot open opposing trades "
                    f"on the same pair simultaneously."
                )
            else:
                # Same direction is allowed (position sizing/averaging)
                logger.info(
                    f"Same-pair same-direction trade allowed: {new_symbol} {new_action} "
                    f"(adding to existing {existing_action} position)"
                )
                return True, f"Same direction trade allowed for {new_symbol}"
        
        # No existing position for this symbol
        return True, f"No existing position for {new_symbol}"
    
    def _is_opposite_direction_conflict(self, correlation: float, existing_action: str, new_action: str) -> bool:
        """
        Determine if two actions create an opposite direction conflict given their correlation.
        
        Logic:
        - Positive correlation: BUY + SELL = conflict (opposite directions)
        - Negative correlation: BUY + BUY = conflict (same directions on negatively correlated pairs)
        """
        if correlation > 0:
            # Positive correlation: same directions are good, opposite directions are bad
            return existing_action != new_action
        else:
            # Negative correlation: opposite directions are good, same directions are bad
            return existing_action == new_action
    
    async def get_correlation_matrix(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Get correlation matrix for a list of symbols"""
        matrix = {}
        
        for symbol1 in symbols:
            matrix[symbol1] = {}
            for symbol2 in symbols:
                if symbol1 == symbol2:
                    matrix[symbol1][symbol2] = 1.0
                else:
                    correlation_data = await self.get_correlation(symbol1, symbol2)
                    matrix[symbol1][symbol2] = correlation_data.correlation if correlation_data else 0.0
        
        return matrix
    
    async def get_portfolio_correlation_metrics(self, positions: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Get comprehensive correlation metrics for the portfolio"""
        symbols = list(positions.keys())
        if len(symbols) < 2:
            return {"status": "insufficient_positions", "position_count": len(symbols)}
        
        # Calculate correlation matrix
        correlation_matrix = await self.get_correlation_matrix(symbols)
        
        # Calculate portfolio metrics
        total_pairs = len(symbols) * (len(symbols) - 1) / 2
        high_correlations = 0
        medium_correlations = 0
        correlation_sum = 0
        
        for i, symbol1 in enumerate(symbols):
            for symbol2 in symbols[i+1:]:
                correlation = correlation_matrix[symbol1][symbol2]
                abs_correlation = abs(correlation)
                correlation_sum += abs_correlation
                
                if abs_correlation >= config.correlation_threshold_high:
                    high_correlations += 1
                elif abs_correlation >= config.correlation_threshold_medium:
                    medium_correlations += 1
        
        avg_correlation = correlation_sum / total_pairs if total_pairs > 0 else 0
        
        # Update currency exposure
        await self.update_currency_exposure(positions)
        
        return {
            "status": "calculated",
            "position_count": len(symbols),
            "correlation_matrix": correlation_matrix,
            "high_correlations": high_correlations,
            "medium_correlations": medium_correlations,
            "average_correlation": avg_correlation,
            "currency_exposure": {curr: {
                'long': exp.long_exposure,
                'short': exp.short_exposure,
                'net': exp.net_exposure,
                'gross': exp.gross_exposure,
                'position_count': exp.position_count
            } for curr, exp in self.currency_exposure.items()},
            "risk_assessment": self._assess_portfolio_risk(avg_correlation, high_correlations, len(symbols))
        }
    
    def _assess_portfolio_risk(self, avg_correlation: float, high_correlations: int, position_count: int) -> str:
        """Assess overall portfolio correlation risk"""
        if avg_correlation > 0.6:
            return "HIGH - Portfolio heavily correlated"
        elif avg_correlation > 0.4:
            return "MEDIUM - Moderate correlation present"
        elif high_correlations > position_count * 0.3:
            return "MEDIUM - Several highly correlated pairs"
        else:
            return "LOW - Well diversified portfolio" 