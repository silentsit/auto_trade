"""
Crypto Signal Handler Module
Handles cryptocurrency-specific signal processing and validation
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CryptoSignal:
    """Represents a cryptocurrency trading signal"""
    symbol: str
    action: str  # 'BUY', 'SELL', 'HOLD'
    price: float
    timestamp: datetime
    confidence: float = 0.0
    volume: Optional[float] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class CryptoSignalHandler:
    """
    Handles cryptocurrency signal processing, validation, and routing
    """
    
    def __init__(self):
        self.supported_cryptos = ['BTC_USD', 'ETH_USD', 'LTC_USD', 'BCH_USD']
        self.active_signals: Dict[str, CryptoSignal] = {}
        self.signal_history: List[CryptoSignal] = []
        
    async def process_signal(self, signal_data: Dict[str, Any]) -> Optional[CryptoSignal]:
        """
        Process incoming cryptocurrency signal
        
        Args:
            signal_data: Raw signal data from external source
            
        Returns:
            CryptoSignal: Processed signal or None if invalid
        """
        try:
            # Extract required fields
            symbol = signal_data.get('symbol', '').upper()
            action = signal_data.get('action', '').upper()
            price = float(signal_data.get('price', 0))
            
            # Validate symbol
            if symbol not in self.supported_cryptos:
                logger.warning(f"Unsupported crypto symbol: {symbol}")
                return None
                
            # Validate action
            if action not in ['BUY', 'SELL', 'HOLD']:
                logger.warning(f"Invalid action: {action}")
                return None
                
            # Validate price
            if price <= 0:
                logger.warning(f"Invalid price: {price}")
                return None
                
            # Create signal
            signal = CryptoSignal(
                symbol=symbol,
                action=action,
                price=price,
                timestamp=datetime.now(timezone.utc),
                confidence=signal_data.get('confidence', 0.0),
                volume=signal_data.get('volume'),
                metadata=signal_data.get('metadata', {})
            )
            
            # Store signal
            self.active_signals[symbol] = signal
            self.signal_history.append(signal)
            
            # Limit history size
            if len(self.signal_history) > 1000:
                self.signal_history = self.signal_history[-500:]
                
            logger.info(f"✅ Processed crypto signal: {symbol} {action} @ {price}")
            return signal
            
        except Exception as e:
            logger.error(f"❌ Error processing crypto signal: {e}")
            return None
    
    async def validate_signal(self, signal: CryptoSignal) -> bool:
        """
        Validate cryptocurrency signal for trading
        
        Args:
            signal: Signal to validate
            
        Returns:
            bool: True if signal is valid for trading
        """
        try:
            # Check if symbol is supported
            if signal.symbol not in self.supported_cryptos:
                logger.warning(f"Signal validation failed: Unsupported symbol {signal.symbol}")
                return False
                
            # Check signal age (max 5 minutes old)
            signal_age = (datetime.now(timezone.utc) - signal.timestamp).total_seconds()
            if signal_age > 300:  # 5 minutes
                logger.warning(f"Signal validation failed: Signal too old ({signal_age}s)")
                return False
                
            # Check confidence level
            if signal.confidence < 0.5:
                logger.warning(f"Signal validation failed: Low confidence ({signal.confidence})")
                return False
                
            logger.info(f"✅ Signal validation passed: {signal.symbol}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error validating signal: {e}")
            return False
    
    def is_crypto_signal(self, symbol: str) -> bool:
        """
        Check if the symbol represents a cryptocurrency
        
        Args:
            symbol: Trading symbol to check
            
        Returns:
            bool: True if symbol is a cryptocurrency
        """
        if not symbol:
            return False
            
        symbol_upper = symbol.upper()
        
        # Check against supported cryptos list
        if symbol_upper in self.supported_cryptos:
            return True
            
        # Check common crypto patterns - comprehensive list
        crypto_patterns = [
            'BTC', 'ETH', 'LTC', 'BCH', 'XRP', 'ADA', 'DOT', 'LINK', 'UNI', 'DOGE',
            'BTCUSD', 'ETHUSD', 'LTCUSD', 'BCHUSD', 'XRPUSD', 'ADAUSD',
            'BTC_USD', 'ETH_USD', 'LTC_USD', 'BCH_USD', 'XRP_USD', 'ADA_USD'
        ]
        
        for pattern in crypto_patterns:
            if pattern in symbol_upper:
                return True
                
        return False
    
    async def get_active_signal(self, symbol: str) -> Optional[CryptoSignal]:
        """
        Get active signal for a specific cryptocurrency
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            CryptoSignal: Active signal or None
        """
        return self.active_signals.get(symbol.upper())
    
    async def clear_active_signal(self, symbol: str) -> bool:
        """
        Clear active signal for a specific cryptocurrency
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            bool: True if signal was cleared
        """
        symbol = symbol.upper()
        if symbol in self.active_signals:
            del self.active_signals[symbol]
            logger.info(f"✅ Cleared active signal for {symbol}")
            return True
        return False
    
    async def get_signal_history(self, symbol: Optional[str] = None, limit: int = 10) -> List[CryptoSignal]:
        """
        Get signal history
        
        Args:
            symbol: Optional symbol filter
            limit: Maximum number of signals to return
            
        Returns:
            List[CryptoSignal]: Historical signals
        """
        history = self.signal_history
        
        if symbol:
            symbol = symbol.upper()
            history = [s for s in history if s.symbol == symbol]
            
        return history[-limit:] if limit > 0 else history
    
    async def get_supported_cryptos(self) -> List[str]:
        """
        Get list of supported cryptocurrency symbols
        
        Returns:
            List[str]: Supported crypto symbols
        """
        return self.supported_cryptos.copy()
    
    async def update_crypto_price(self, symbol: str, price: float) -> bool:
        """
        Update cryptocurrency price for active signals
        
        Args:
            symbol: Cryptocurrency symbol
            price: Current price
            
        Returns:
            bool: True if price was updated
        """
        try:
            symbol = symbol.upper()
            if symbol in self.active_signals:
                # Update metadata with current price
                self.active_signals[symbol].metadata['current_price'] = price
                self.active_signals[symbol].metadata['last_update'] = datetime.now(timezone.utc).isoformat()
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error updating crypto price: {e}")
            return False
    
    async def cleanup_old_signals(self, max_age_minutes: int = 30) -> int:
        """
        Cleanup old active signals
        
        Args:
            max_age_minutes: Maximum age of signals to keep
            
        Returns:
            int: Number of signals cleaned up
        """
        try:
            now = datetime.now(timezone.utc)
            cleaned_count = 0
            
            symbols_to_remove = []
            for symbol, signal in self.active_signals.items():
                age_minutes = (now - signal.timestamp).total_seconds() / 60
                if age_minutes > max_age_minutes:
                    symbols_to_remove.append(symbol)
                    
            for symbol in symbols_to_remove:
                del self.active_signals[symbol]
                cleaned_count += 1
                
            if cleaned_count > 0:
                logger.info(f"✅ Cleaned up {cleaned_count} old crypto signals")
                
            return cleaned_count
            
        except Exception as e:
            logger.error(f"❌ Error cleaning up signals: {e}")
            return 0

# Global instance
crypto_handler = CryptoSignalHandler()

async def get_crypto_signal_handler() -> CryptoSignalHandler:
    """Get global crypto signal handler instance"""
    return crypto_handler