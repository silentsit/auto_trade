#!/usr/bin/env python3
"""
Crypto Signal Handler
Handles cryptocurrency signals when OANDA doesn't support them in practice environment
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

# Configure logging
logger = logging.getLogger(__name__)

class CryptoSignalHandler:
    """
    Handles cryptocurrency signals when main broker doesn't support them
    """
    
    def __init__(self):
        self.crypto_symbols = [
            'BTC_USD', 'BTCUSD', 'BTC/USD',
            'ETH_USD', 'ETHUSD', 'ETH/USD', 
            'LTC_USD', 'LTCUSD', 'LTC/USD',
            'XRP_USD', 'XRPUSD', 'XRP/USD',
            'BCH_USD', 'BCHUSD', 'BCH/USD',
            'ADA_USD', 'ADAUSD', 'ADA/USD',
            'DOT_USD', 'DOTUSD', 'DOT/USD',
            'SOL_USD', 'SOLUSD', 'SOL/USD'
        ]
        self.crypto_log_file = "crypto_signals_log.json"
        self.unsupported_signals = []
        
    def is_crypto_signal(self, symbol: str) -> bool:
        """Check if the signal is for a cryptocurrency"""
        symbol_upper = symbol.upper().replace('/', '_').replace('-', '_')
        
        # Check for common crypto patterns
        crypto_patterns = ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'ADA', 'DOT', 'SOL', 'DOGE', 'SHIB', 'AVAX', 'MATIC']
        
        for pattern in crypto_patterns:
            if pattern in symbol_upper:
                return True
                
        return False
    
    def log_crypto_signal(self, signal_data: Dict[str, Any]) -> None:
        """Log crypto signals for later processing or analysis"""
        try:
            signal_entry = {
                "timestamp": datetime.now().isoformat(),
                "symbol": signal_data.get("symbol"),
                "action": signal_data.get("action"),
                "direction": signal_data.get("direction"),
                "message": signal_data.get("message"),
                "original_data": signal_data,
                "status": "unsupported_in_practice_environment"
            }
            
            self.unsupported_signals.append(signal_entry)
            
            # Write to file
            try:
                with open(self.crypto_log_file, 'r') as f:
                    existing_signals = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                existing_signals = []
            
            existing_signals.append(signal_entry)
            
            with open(self.crypto_log_file, 'w') as f:
                json.dump(existing_signals, f, indent=2, default=str)
            
            logger.info(f"[CRYPTO SIGNAL LOGGED] {signal_data.get('symbol')} - {signal_data.get('action')} | Reason: OANDA Practice environment doesn't support crypto")
            
        except Exception as e:
            logger.error(f"Failed to log crypto signal: {e}")
    
    def handle_unsupported_crypto_signal(self, signal_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Handle crypto signals when they're not supported by the main broker
        Returns (handled, reason)
        """
        try:
            symbol = signal_data.get("symbol", "")
            action = signal_data.get("action", "")
            
            if not self.is_crypto_signal(symbol):
                return False, "Not a crypto signal"
            
            # Log the signal
            self.log_crypto_signal(signal_data)
            
            # Provide helpful feedback
            reason = f"Crypto signal rejected: OANDA Practice environment doesn't support {symbol} trading"
            
            # Optional: Here you could integrate with other crypto exchanges/services
            # For example: Binance, Coinbase Pro, Kraken, etc.
            
            return True, reason
            
        except Exception as e:
            logger.error(f"Error handling crypto signal: {e}")
            return False, f"Error processing crypto signal: {str(e)}"
    
    def get_crypto_signal_stats(self) -> Dict[str, Any]:
        """Get statistics on crypto signals received"""
        try:
            with open(self.crypto_log_file, 'r') as f:
                signals = json.load(f)
            
            stats = {
                "total_crypto_signals": len(signals),
                "symbols": {},
                "actions": {},
                "recent_signals": signals[-10:] if signals else []
            }
            
            for signal in signals:
                symbol = signal.get("symbol", "UNKNOWN")
                action = signal.get("action", "UNKNOWN")
                
                stats["symbols"][symbol] = stats["symbols"].get(symbol, 0) + 1
                stats["actions"][action] = stats["actions"].get(action, 0) + 1
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting crypto stats: {e}")
            return {"error": str(e)}
    
    def suggest_crypto_solutions(self) -> List[str]:
        """Suggest solutions for crypto trading"""
        suggestions = [
            "💡 CRYPTO TRADING SOLUTIONS:",
            "",
            "1. 🔄 OANDA Live Account:",
            "   - Switch to OANDA live environment",
            "   - Contact OANDA about crypto access via Paxos partnership",
            "",
            "2. 🏦 Alternative Crypto Brokers:",
            "   - Binance API for crypto spot trading",
            "   - Coinbase Pro API",
            "   - Kraken API",
            "   - FTX API (if available in your region)",
            "",
            "3. 🔧 Modify TradingView Alerts:",
            "   - Update Pine Script to exclude crypto signals",
            "   - Or create separate webhooks for crypto vs forex",
            "",
            "4. 📊 Hybrid Approach:",
            "   - Use OANDA for forex/traditional instruments",
            "   - Use dedicated crypto exchange for crypto signals",
            "",
            "5. 💱 Crypto CFDs:",
            "   - Some brokers offer crypto CFDs in practice mode",
            "   - Check if OANDA Europe offers crypto CFDs"
        ]
        
        return suggestions

# Global crypto handler instance
crypto_handler = CryptoSignalHandler()

def handle_crypto_signal_rejection(signal_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Global function to handle crypto signal rejections
    """
    return crypto_handler.handle_unsupported_crypto_signal(signal_data)

def is_crypto_symbol(symbol: str) -> bool:
    """
    Global function to check if symbol is cryptocurrency
    """
    return crypto_handler.is_crypto_signal(symbol)

def log_unsupported_crypto_signal(signal_data: Dict[str, Any]) -> None:
    """
    Global function to log unsupported crypto signals
    """
    crypto_handler.log_crypto_signal(signal_data) 