"""
Symbol standardization utilities for the FX Trading Bridge application.
"""
import re
import logging

logger = logging.getLogger(__name__)

# Define common symbol mappings
SYMBOL_MAPPINGS = {
    # Forex pairs
    "EURUSD": "EUR_USD",
    "GBPUSD": "GBP_USD",
    "USDJPY": "USD_JPY",
    "AUDUSD": "AUD_USD",
    "USDCAD": "USD_CAD",
    "NZDUSD": "NZD_USD",
    "USDCHF": "USD_CHF",
    "EURJPY": "EUR_JPY",
    "GBPJPY": "GBP_JPY",
    "EURGBP": "EUR_GBP",
    "AUDNZD": "AUD_NZD",
    "EURCHF": "EUR_CHF",
    "AUDCAD": "AUD_CAD",
    "AUDJPY": "AUD_JPY",
    "CADJPY": "CAD_JPY",
    "CHFJPY": "CHF_JPY",
    "EURAUD": "EUR_AUD",
    "EURCAD": "EUR_CAD",
    "EURNZD": "EUR_NZD",
    "GBPAUD": "GBP_AUD",
    "GBPCAD": "GBP_CAD",
    "GBPCHF": "GBP_CHF",
    "GBPNZD": "GBP_NZD",
    "NZDJPY": "NZD_JPY",
    "NZDCAD": "NZD_CAD",
    "NZDCHF": "NZD_CHF",
    
    # Common indices
    "US30": "US30_USD",
    "SPX500": "SPX500_USD",
    "NAS100": "NAS100_USD",
    "US2000": "US2000_USD",
    "UK100": "UK100_GBP",
    "JP225": "JP225_USD",
    "DE30": "DE30_EUR",
    "HK33": "HK33_HKD",
    "AU200": "AU200_AUD",
    "CHINA50": "CHINA50_USD",
    "INDIA50": "INDIA50_USD",
    
    # Commonly used commodities
    "XAUUSD": "XAU_USD",  # Gold
    "XAGUSD": "XAG_USD",  # Silver
    "BCOUSD": "BCO_USD",  # Brent Crude Oil
    "WTICOUSD": "WTICO_USD",  # WTI Crude Oil
    "NATGAS": "NATGAS_USD",  # Natural Gas
    
    # Cryptocurrencies
    "BTCUSD": "BTC_USD",
    "ETHUSD": "ETH_USD",
    "XRPUSD": "XRP_USD",
    "LTCUSD": "LTC_USD",
}

def standardize_symbol(symbol: str) -> str:
    """
    Standardize a trading symbol to the format required by the broker's API.
    
    Args:
        symbol: The symbol to standardize (e.g., 'EURUSD', 'EUR/USD', 'EUR-USD')
        
    Returns:
        Standardized symbol format (e.g., 'EUR_USD')
    """
    if not symbol:
        logger.error("Received empty symbol for standardization")
        return ""
    
    # Remove any whitespace and convert to uppercase
    clean_symbol = symbol.strip().upper()
    
    # Check direct mapping first
    if clean_symbol in SYMBOL_MAPPINGS:
        return SYMBOL_MAPPINGS[clean_symbol]
    
    # If it's already in the correct format (e.g., EUR_USD), return as is
    if re.match(r'^[A-Z0-9]+_[A-Z0-9]+$', clean_symbol):
        return clean_symbol
    
    # Remove special characters and attempt to split into currency pairs
    # (e.g., EUR/USD, EUR-USD, EUR.USD --> EUR_USD)
    clean_symbol = re.sub(r'[/\-.]', '_', clean_symbol)
    
    # If it now has an underscore, it's likely in the right format
    if '_' in clean_symbol:
        parts = clean_symbol.split('_')
        if len(parts) == 2 and all(len(p) >= 2 for p in parts):
            return clean_symbol
    
    # For symbols like EURUSD without any separator
    if len(clean_symbol) == 6 and re.match(r'^[A-Z]{6}$', clean_symbol):
        # Common currency pairs are 3 letters each
        return f"{clean_symbol[:3]}_{clean_symbol[3:]}"
    
    # For indices that might have numbers (e.g., US30)
    match = re.match(r'^([A-Z]+)([0-9]+)$', clean_symbol)
    if match:
        prefix, suffix = match.groups()
        return f"{prefix}{suffix}_USD"  # Default indices to USD quote
    
    # For precious metals or commodities that might need _USD appended
    for metal in ["XAU", "XAG", "XCU", "XPD", "XPT"]:
        if clean_symbol.startswith(metal) and not clean_symbol.endswith("_USD"):
            return f"{clean_symbol}_USD"
    
    # If we can't standardize it properly, log a warning and return the original with underscore
    logger.warning(f"Unable to properly standardize symbol: {symbol}. Using best guess.")
    
    # For most brokers, the format is typically BASE_QUOTE, e.g., EUR_USD
    # If it's not a standard format, we'll make our best guess and return it
    return clean_symbol

def get_base_currency(symbol: str) -> str:
    """Get the base currency from a standardized symbol (e.g., 'EUR_USD' -> 'EUR')"""
    std_symbol = standardize_symbol(symbol)
    if '_' in std_symbol:
        return std_symbol.split('_')[0]
    elif len(std_symbol) == 6:  # For format like 'EURUSD'
        return std_symbol[:3]
    return ""

def get_quote_currency(symbol: str) -> str:
    """Get the quote currency from a standardized symbol (e.g., 'EUR_USD' -> 'USD')"""
    std_symbol = standardize_symbol(symbol)
    if '_' in std_symbol:
        return std_symbol.split('_')[1]
    elif len(std_symbol) == 6:  # For format like 'EURUSD'
        return std_symbol[3:]
    return ""

def is_valid_symbol(symbol: str) -> bool:
    """Check if a symbol is valid according to our standardization rules"""
    std_symbol = standardize_symbol(symbol)
    return (
        std_symbol and 
        '_' in std_symbol and 
        len(std_symbol.split('_')[0]) >= 2 and 
        len(std_symbol.split('_')[1]) >= 2
    ) 
