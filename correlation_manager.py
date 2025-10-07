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
    data_source: str = "dynamic"  # 'dynamic', 'static_fallback', 'static'
    
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
    Advanced correlation manager with DYNAMIC price data integration
    and real-time correlation updates for institutional-grade risk management
    """
    
    def __init__(self):
        self.correlations: Dict[Tuple[str, str], CorrelationData] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))  # Increased capacity
        self.currency_exposure: Dict[str, CurrencyExposure] = {}
        self._lock = asyncio.Lock()
        
        # DYNAMIC UPDATE SETTINGS - OPTIMIZED FOR EFFICIENCY
        self.correlation_lookback_days = 30  # 30 days for correlation calculation
        self.price_update_interval = 900  # 15 minutes between price updates (more efficient)
        self.correlation_recalc_interval = 3600  # 1 hour between correlation recalculations
        self.last_price_update = {}  # symbol -> last update time
        self.last_correlation_update = datetime.now(timezone.utc) - timedelta(hours=1)
        
        # Predefined correlation groups for major forex pairs
        self.correlation_groups = {
            'EUR_MAJORS': ['EUR_USD', 'EUR_GBP', 'EUR_JPY', 'EUR_CHF', 'EUR_AUD', 'EUR_CAD'],
            'USD_MAJORS': ['EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF', 'AUD_USD', 'USD_CAD'],
            'JPY_CROSSES': ['EUR_JPY', 'GBP_JPY', 'USD_JPY', 'AUD_JPY', 'CAD_JPY', 'CHF_JPY'],
            'COMMODITY_CURRENCIES': ['AUD_USD', 'USD_CAD', 'NZD_USD'],
            'SAFE_HAVENS': ['USD_JPY', 'USD_CHF', 'EUR_CHF']
        }
        
        # Enhanced static correlations with institutional-grade pairs (FALLBACK ONLY)
        self.static_correlations = {
            # Major positive correlations
            ('EUR_USD', 'GBP_USD'): 0.75,
            ('EUR_USD', 'AUD_USD'): 0.65,
            ('EUR_USD', 'EUR_GBP'): 0.85,
            ('USD_JPY', 'EUR_JPY'): 0.80,
            ('USD_CHF', 'EUR_CHF'): 0.85,
            ('AUD_USD', 'NZD_USD'): 0.90,
            ('USD_CAD', 'EUR_CAD'): 0.75,
            
            # Critical negative correlations - INSTITUTIONAL RISK MANAGEMENT
            ('EUR_USD', 'USD_CHF'): -0.87,  # KEY: EUR/USD vs USD/CHF
            ('GBP_USD', 'USD_CHF'): -0.78,  # GBP/USD vs USD/CHF  
            ('AUD_USD', 'USD_CHF'): -0.72,  # AUD/USD vs USD/CHF
            ('EUR_USD', 'USD_JPY'): -0.65,  # EUR/USD vs USD/JPY
            ('GBP_USD', 'USD_JPY'): -0.60,  # GBP/USD vs USD/JPY
            ('EUR_GBP', 'GBP_USD'): -0.70,  # EUR/GBP vs GBP/USD
            
            # Swiss Franc negative correlations
            ('EUR_CHF', 'USD_CHF'): -0.82,  # EUR/CHF vs USD/CHF
            ('GBP_CHF', 'USD_CHF'): -0.75,  # GBP/CHF vs USD/CHF
            
            # JPY cross negative correlations
            ('EUR_JPY', 'USD_JPY'): -0.68,  # EUR/JPY vs USD/JPY
            ('GBP_JPY', 'USD_JPY'): -0.63,  # GBP/JPY vs USD/JPY
        }
        
        logger.info("CorrelationManager initialized with DYNAMIC price data integration (15-min updates)")
    
    async def add_price_data(self, symbol: str, price: float, timestamp: Optional[datetime] = None):
        """
        ENHANCED: Add price data point for dynamic correlation calculation
        
        This method is called every 15 minutes to update correlations efficiently
        """
        async with self._lock:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)
            
            # Add price data
            self.price_history[symbol].append({
                'price': price,
                'timestamp': timestamp
            })
            
            # Update last price update time
            self.last_price_update[symbol] = timestamp
            
            # Clean old data (keep more data for better correlation calculation)
            cutoff = timestamp - timedelta(days=self.correlation_lookback_days + 5)
            while (self.price_history[symbol] and 
                   self.price_history[symbol][0]['timestamp'] < cutoff):
                self.price_history[symbol].popleft()
            
            logger.debug(f"Added price data for {symbol}: {price} (history size: {len(self.price_history[symbol])})")
    
    async def should_update_correlations(self) -> bool:
        """Check if correlations should be recalculated based on time interval"""
        time_since_update = datetime.now(timezone.utc) - self.last_correlation_update
        return time_since_update.total_seconds() >= self.correlation_recalc_interval
    
    async def update_all_correlations(self, force: bool = False):
        """
        ENHANCED: Update all correlations with intelligent fallback handling
        """
        if not force and not await self.should_update_correlations():
            return
        
        logger.info("🔄 Starting dynamic correlation update (15-min price data)...")
        
        # Get all unique symbol pairs from price history
        symbols = list(self.price_history.keys())
        if len(symbols) < 2:
            logger.warning("Insufficient symbols with price data for correlation calculation")
            return
        
        updates_count = 0
        fallback_count = 0
        
        # Calculate correlations for all symbol pairs
        for i, symbol1 in enumerate(symbols):
            for symbol2 in symbols[i+1:]:
                try:
                    correlation_data = await self.calculate_correlation(symbol1, symbol2)
                    if correlation_data:
                        key = tuple(sorted([symbol1, symbol2]))
                        self.correlations[key] = correlation_data
                        updates_count += 1
                        
                        if correlation_data.data_source == "static_fallback":
                            fallback_count += 1
                            
                except Exception as e:
                    logger.warning(f"Failed to calculate correlation for {symbol1}/{symbol2}: {e}")
                    continue
        
        if fallback_count > 0:
            logger.info(f"✅ Dynamic correlation update complete: {updates_count} pairs updated, {fallback_count} using fallback data")
        else:
            logger.info(f"✅ Dynamic correlation update complete: {updates_count} pairs updated (15-min price data)")
        
        self.last_correlation_update = datetime.now(timezone.utc)
    
    async def calculate_correlation(self, symbol1: str, symbol2: str, 
                                  lookback_days: Optional[int] = None) -> Optional[CorrelationData]:
        """
        ENHANCED: Calculate correlation between two instruments using dynamic price data
        with improved fallback mechanisms for insufficient data
        """
        if lookback_days is None:
            lookback_days = self.correlation_lookback_days
        
        async with self._lock:
            # Check if we have enough RECENT data
            min_data_points = 20  # Minimum for reliable correlation
            
            # Check data availability and quality
            data1_available = symbol1 in self.price_history and len(self.price_history[symbol1]) >= min_data_points
            data2_available = symbol2 in self.price_history and len(self.price_history[symbol2]) >= min_data_points
            
            if not data1_available or not data2_available:
                logger.debug(f"Insufficient price data for correlation: {symbol1}({len(self.price_history.get(symbol1, []))} points), {symbol2}({len(self.price_history.get(symbol2, []))} points)")
                
                # Use static correlation as fallback
                key1 = (symbol1, symbol2)
                key2 = (symbol2, symbol1)
                if key1 in self.static_correlations:
                    correlation = self.static_correlations[key1]
                    logger.debug(f"Using static correlation for {symbol1}/{symbol2}: {correlation:+.2f}")
                elif key2 in self.static_correlations:
                    correlation = self.static_correlations[key2]
                    logger.debug(f"Using static correlation for {symbol1}/{symbol2}: {correlation:+.2f}")
                else:
                    # If no static correlation available, use a neutral correlation
                    # Throttle repetitive warnings for the same pair by logging at debug level
                    logger.debug(f"No correlation data available for {symbol1}/{symbol2}, using neutral correlation")
                    correlation = 0.0
                
                return CorrelationData(
                    correlation=correlation,
                    last_updated=datetime.now(timezone.utc),
                    lookback_days=lookback_days,
                    sample_size=0,
                    strength=self._get_correlation_strength(correlation),
                    data_source="static_fallback"
                )
            
            # Calculate DYNAMIC correlation from recent price data
            prices1, prices2 = self._align_price_data(symbol1, symbol2, lookback_days)
            
            if len(prices1) < min_data_points:
                logger.debug(f"Insufficient aligned data for {symbol1}/{symbol2}: {len(prices1)} points, using static fallback")
                
                # Fallback to static correlation
                key1 = (symbol1, symbol2)
                key2 = (symbol2, symbol1)
                if key1 in self.static_correlations:
                    correlation = self.static_correlations[key1]
                elif key2 in self.static_correlations:
                    correlation = self.static_correlations[key2]
                else:
                    correlation = 0.0
                
                return CorrelationData(
                    correlation=correlation,
                    last_updated=datetime.now(timezone.utc),
                    lookback_days=lookback_days,
                    sample_size=len(prices1),
                    strength=self._get_correlation_strength(correlation),
                    data_source="static_fallback"
                )
            
            # Calculate returns
            returns1 = np.diff(np.log(prices1))
            returns2 = np.diff(np.log(prices2))
            
            # Calculate correlation
            correlation_matrix = np.corrcoef(returns1, returns2)
            correlation = correlation_matrix[0, 1]
            
            if np.isnan(correlation):
                correlation = 0.0
            
            correlation_data = CorrelationData(
                correlation=correlation,
                last_updated=datetime.now(timezone.utc),
                lookback_days=lookback_days,
                sample_size=len(returns1),
                strength=self._get_correlation_strength(correlation),
                data_source="dynamic"
            )
            
            logger.debug(f"Dynamic correlation for {symbol1}/{symbol2}: {correlation:+.2f} ({len(returns1)} samples)")
            
            return correlation_data
    
    def _align_price_data(self, symbol1: str, symbol2: str, lookback_days: int) -> Tuple[List[float], List[float]]:
        """
        ENHANCED: Align price data between two symbols with better timestamp matching
        Optimized for 15-minute intervals
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Get price data within lookback period
        data1 = [d for d in self.price_history[symbol1] if d['timestamp'] >= cutoff]
        data2 = [d for d in self.price_history[symbol2] if d['timestamp'] >= cutoff]
        
        if not data1 or not data2:
            return [], []
        
        # Better alignment: match timestamps more precisely
        aligned_prices1 = []
        aligned_prices2 = []
        
        # Create timestamp-price maps
        price_map1 = {d['timestamp']: d['price'] for d in data1}
        price_map2 = {d['timestamp']: d['price'] for d in data2}
        
        # Find common timestamps (within 15 minute tolerance - same as update interval)
        for ts1, price1 in price_map1.items():
            closest_price2 = None
            min_time_diff = timedelta(minutes=15)  # 15 minute tolerance (matches update interval)
            
            for ts2, price2 in price_map2.items():
                time_diff = abs(ts1 - ts2)
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_price2 = price2
            
            if closest_price2 is not None:
                aligned_prices1.append(price1)
                aligned_prices2.append(closest_price2)
        
        return aligned_prices1, aligned_prices2
    
    def _get_correlation_strength(self, correlation: float) -> str:
        """
        Classify correlation strength using STRICTER thresholds:
        - High: ≥70% (more strict protection)
        - Medium: 60-70%  
        - Low: <60%
        """
        abs_corr = abs(correlation)
        if abs_corr >= 0.70:  # config.correlation_threshold_high
            return 'high'
        elif abs_corr >= 0.60:  # config.correlation_threshold_medium
            return 'medium'
        else:
            return 'low'
    
    async def get_correlation(self, symbol1: str, symbol2: str, 
                            force_recalculate: bool = False) -> Optional[CorrelationData]:
        """
        ENHANCED: Get correlation with dynamic updates and staleness checks
        """
        key = tuple(sorted([symbol1, symbol2]))
        
        # Check if we should use cached correlation
        if not force_recalculate and key in self.correlations:
            correlation_data = self.correlations[key]
            if not correlation_data.is_stale(max_age_hours=4):  # 4 hour staleness
                return correlation_data
        
        # Calculate fresh correlation
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
        ENHANCED: Check correlation limits with REAL-TIME correlation data
        
        Now uses dynamic correlations updated every 15 minutes for better risk management
        """
        if not hasattr(config, 'enable_correlation_limits') or not config.enable_correlation_limits:
            return True, "Correlation limits disabled", {}
        
        # FIRST: Check same-pair conflicts (if enabled)
        if hasattr(config, 'enable_same_pair_conflict_prevention') and config.enable_same_pair_conflict_prevention:
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
            # ENHANCED: Use REAL-TIME correlations for more accurate risk assessment
            for symbol, position_data in current_positions.items():
                if symbol == new_symbol:
                    continue
                
                # Get DYNAMIC correlation (updated every 15 minutes)
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
                        'new_action': new_action,
                        'data_source': 'dynamic(15min)' if correlation_data.sample_size > 0 else 'static',
                        'sample_size': correlation_data.sample_size
                    })
                    
                    # STRICTER PROTECTION: Check for conflicts based on correlation level
                    if correlation_strength >= 0.70:  # ≥70% (STRICTER)
                        # HIGH CORRELATION: Block opposite direction conflicts
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
                                'risk_conflict': True,
                                'severity': 'HIGH',
                                'data_source': 'dynamic(15min)' if correlation_data.sample_size > 0 else 'static'
                            }
                            analysis['opposite_direction_conflicts'].append(conflict_info)
                            
                            # BLOCK the trade - stricter institutional risk protection
                            source_info = f"({correlation_data.sample_size} samples, 15min)" if correlation_data.sample_size > 0 else "(static)"
                            return False, (
                                f"HIGH CORRELATION CONFLICT: {symbol} {existing_action} vs "
                                f"{new_symbol} {new_action} (correlation: {correlation_data.correlation:+.1%} {source_info}). "
                                f"Highly correlated pairs (≥70%) cannot trade in conflicting directions."
                            ), analysis
                    
                    elif correlation_strength >= 0.60:  # 60-70%
                        # MEDIUM CORRELATION: Allow with warning
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
                                'risk_conflict': True,
                                'severity': 'MEDIUM',
                                'data_source': 'dynamic(15min)' if correlation_data.sample_size > 0 else 'static'
                            }
                            analysis['opposite_direction_conflicts'].append(conflict_info)
                            analysis['recommendation'] = 'warning'
                            
                            source_info = f"({correlation_data.sample_size} samples, 15min)" if correlation_data.sample_size > 0 else "(static)"
                            logger.warning(
                                f"MEDIUM CORRELATION WARNING: {symbol} {existing_action} vs "
                                f"{new_symbol} {new_action} (correlation: {correlation_data.correlation:+.1%} {source_info}). "
                                f"Moderately correlated pairs (60-70%) in conflicting directions."
                            )
            
            # If no HIGH correlation conflicts found, allow the trade
            return True, "No high correlation conflicts detected", analysis
            
        except Exception as e:
            logger.error(f"Error checking correlation limits: {e}")
            return True, f"Error in correlation check: {e}", analysis

    def _check_same_pair_conflicts(self, new_symbol: str, new_action: str, 
                                 current_positions: Dict[str, Dict[str, Any]]) -> Tuple[bool, str]:
        """Check if adding a new position would create a same-pair opposite direction conflict"""
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
        """Determine if two actions create an opposite direction conflict given their correlation"""
        if correlation > 0:
            # Positive correlation: same directions are good, opposite directions are bad
            return existing_action != new_action
        else:
            # Negative correlation: opposite directions are good, same directions are bad  
            return existing_action == new_action
    
    async def get_correlation_matrix(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Get correlation matrix for a list of symbols using DYNAMIC correlations"""
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
        """Get comprehensive correlation metrics using DYNAMIC correlations (15-min updates)"""
        symbols = list(positions.keys())
        if len(symbols) < 2:
            return {"status": "insufficient_positions", "position_count": len(symbols)}
        
        # Calculate correlation matrix with dynamic data
        correlation_matrix = await self.get_correlation_matrix(symbols)
        
        # Calculate portfolio metrics with DYNAMIC correlations
        total_pairs = len(symbols) * (len(symbols) - 1) / 2
        high_correlations = 0
        medium_correlations = 0
        correlation_sum = 0
        dynamic_correlations = 0
        
        for i, symbol1 in enumerate(symbols):
            for symbol2 in symbols[i+1:]:
                correlation = correlation_matrix[symbol1][symbol2]
                abs_correlation = abs(correlation)
                correlation_sum += abs_correlation
                
                # Check if this is a dynamic correlation
                correlation_data = await self.get_correlation(symbol1, symbol2)
                if correlation_data and correlation_data.sample_size > 0:
                    dynamic_correlations += 1
                
                if abs_correlation >= 0.70:  # ≥70% (STRICTER)
                    high_correlations += 1
                elif abs_correlation >= 0.60:  # 60-70%
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
            "dynamic_correlations": dynamic_correlations,
            "static_correlations": int(total_pairs) - dynamic_correlations,
            "data_quality": dynamic_correlations / total_pairs if total_pairs > 0 else 0,
            "last_correlation_update": self.last_correlation_update.isoformat(),
            "update_frequency": "15 minutes",
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
        """Assess overall portfolio correlation risk with STRICTER thresholds"""
        if avg_correlation > 0.6:
            return "HIGH - Portfolio heavily correlated"
        elif avg_correlation > 0.4:
            return "MEDIUM - Moderate correlation present"
        elif high_correlations > position_count * 0.3:
            return "MEDIUM - Several highly correlated pairs"
        else:
            return "LOW - Well diversified portfolio"

    async def get_price_data_status(self) -> Dict[str, Any]:
        """Get status of price data for monitoring (15-min intervals)"""
        now = datetime.now(timezone.utc)
        
        status = {
            "symbols_tracked": len(self.price_history),
            "total_data_points": sum(len(history) for history in self.price_history.values()),
            "last_correlation_update": self.last_correlation_update.isoformat(),
            "next_correlation_update": (self.last_correlation_update + timedelta(seconds=self.correlation_recalc_interval)).isoformat(),
            "update_frequency": "15 minutes",
            "symbol_status": {}
        }
        
        for symbol, history in self.price_history.items():
            last_update = self.last_price_update.get(symbol)
            status["symbol_status"][symbol] = {
                "data_points": len(history),
                "last_update": last_update.isoformat() if last_update else None,
                "age_minutes": (now - last_update).total_seconds() / 60 if last_update else None,
                "sufficient_data": len(history) >= 20
            }
        
        return status