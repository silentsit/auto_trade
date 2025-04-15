"""
Trading Configuration Module for FX Trading Bridge

This module provides configuration management for trading parameters,
allowing for runtime configuration updates and defaults.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timezone

# Setup logging
logger = logging.getLogger("fx-trading-bridge.config")

# Global config store for direct function access
_config_values = {}
_config_file = "config.json"

class TradingConfig:
    """
    Trading configuration manager that provides a centralized 
    way to manage trading parameters.
    
    Features:
    - Default configuration values
    - Configuration persistence
    - Runtime updates
    - Configuration validation
    """
    
    def __init__(self, config_file: str = "trading_config.json"):
        """
        Initialize the trading configuration.
        
        Args:
            config_file: Path to the configuration file
        """
        self.config_file = config_file
        self.config = self._load_default_config()
        
        # Try to load from file if it exists
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    loaded_config = json.load(f)
                    self.config.update(loaded_config)
                logger.info(f"Loaded configuration from {config_file}")
            except Exception as e:
                logger.error(f"Error loading configuration from {config_file}: {str(e)}")
        else:
            logger.info(f"Configuration file {config_file} not found, using defaults")
            # Save defaults
            self.save_config()
        
        # Update global config for function access
        global _config_values
        _config_values.update(self.config)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration values"""
        return {
            "general": {
                "environment": "development",
                "debug_mode": True,
                "log_level": "INFO",
                "max_positions": 10,
                "base_currency": "USD"
            },
            "risk": {
                "max_risk_per_trade": 0.2,  # 20% per trade
                "max_daily_risk": 0.5,      # 50% per day
                "max_drawdown": 0.5,        # 50% max drawdown
                "position_sizing_method": "risk_based",
                "baseline_volatility": 0.8
            },
            "execution": {
                "default_order_type": "MARKET",
                "slippage_tolerance": 0.0005,
                "retry_attempts": 3,
                "retry_delay": 1.0
            },
            "exchanges": {
                "primary": "oanda",
                "connections": {
                    "oanda": {
                        "enabled": True,
                        "api_key": "",
                        "account_id": ""
                    },
                    "binance": {
                        "enabled": False,
                        "api_key": "",
                        "api_secret": ""
                    }
                }
            },
            "error_recovery": {
                "max_retries": 3,
                "backoff_factor": 2.0,
                "grace_period": 300
            },
            "connection": {
                "connect_timeout": 10,
                "read_timeout": 30,
                "total_timeout": 45
            },
            "backtesting": {
                "default_capital": 100000,
                "default_period_days": 30,
                "symbols": ["EUR_USD", "GBP_USD", "USD_JPY"]
            },
            "trading": {
                "min_position_size": 1000
            },
            "system": {
                "host": "0.0.0.0",
                "port": 10000,
                "allowed_origins": "*"
            },
            "oanda": {
                "oanda_account_id": "",
                "oanda_api_token": "",
                "oanda_api_url": "https://api-fxtrade.oanda.com/v3"
            },
            "risk_management": {
                "max_daily_loss": 20.0
            },
            # Environment variables
            "VERSION": "1.0.0",
            "ENVIRONMENT": "production",
            "ENABLE_ADVANCED_LOSS_MANAGEMENT": True,
            "ENABLE_MULTI_STAGE_TP": True,
            "ENABLE_MARKET_STRUCTURE_ANALYSIS": True,
            "MAX_DAILY_LOSS": 0.20,
            "MAX_RISK_PERCENTAGE": 2.0
        }
    
    def get_value(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            section: Configuration section
            key: Configuration key
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        if section in self.config and key in self.config[section]:
            return self.config[section][key]
        return default
    
    def set_value(self, section: str, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            section: Configuration section
            key: Configuration key
            value: Value to set
        """
        if section not in self.config:
            self.config[section] = {}
        
        self.config[section][key] = value
        
        # Update global config too
        global _config_values
        if section not in _config_values:
            _config_values[section] = {}
        _config_values[section][key] = value
        
        logger.info(f"Updated configuration {section}.{key}")
    
    def save_config(self) -> bool:
        """
        Save the configuration to file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.config_file)), exist_ok=True)
            
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            logger.info(f"Saved configuration to {self.config_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to {self.config_file}: {str(e)}")
            return False
    
    def reset_to_defaults(self) -> None:
        """Reset configuration to defaults"""
        self.config = self._load_default_config()
        
        # Update global config too
        global _config_values
        _config_values.update(self.config)
        
        logger.info("Reset configuration to defaults")
    
    def get_all(self) -> Dict[str, Any]:
        """Get the entire configuration"""
        return self.config.copy()
    
    def update_section(self, section: str, values: Dict[str, Any]) -> None:
        """
        Update an entire configuration section.
        
        Args:
            section: Section name
            values: Dictionary of values to update
        """
        if section not in self.config:
            self.config[section] = {}
        
        self.config[section].update(values)
        
        # Update global config too
        global _config_values
        if section not in _config_values:
            _config_values[section] = {}
        _config_values[section].update(values)
        
        logger.info(f"Updated configuration section {section}")

# Create a global instance
_config_instance = TradingConfig()

# Global functions for compatibility with the codebase
def get_config_value(section_or_key, key_or_default=None, default=None):
    """
    Get a configuration value flexibly handling different parameter patterns.
    
    This function can handle:
    1. get_config_value("section", "key", default)      - Nested config
    2. get_config_value("section", "key")               - Nested config without default
    3. get_config_value("key", default)                 - Single-level lookup
    4. get_config_value("key")                          - Single-level lookup without default
    
    Args:
        section_or_key: Either the section name or the key for single-level lookup
        key_or_default: Either the key name when using nested config, or default value for single-level lookup
        default: Default value to return if key not found (only used in nested lookup)
        
    Returns:
        The configuration value or default
    """
    # Handle both parameter patterns
    if default is None and key_or_default is not None and not isinstance(key_or_default, str):
        # Pattern: get_config_value("key", default)
        return _get_single_level_config(section_or_key, key_or_default)
    
    if key_or_default is None and default is None:
        # Pattern: get_config_value("key")
        return _get_single_level_config(section_or_key)
    
    # Pattern: get_config_value("section", "key", default)
    # or: get_config_value("section", "key")
    return _get_nested_config(section_or_key, key_or_default, default)

def _get_nested_config(section, key, default=None):
    """
    Get a nested configuration value.
    
    Args:
        section: Section name
        key: Key within section
        default: Default value if not found
        
    Returns:
        Configuration value or default
    """
    # Check environment variables with combined key
    env_key = f"{section}_{key}".upper()
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return env_val
        
    # Check in trading_config
    config = get_config()
    if section in config and key in config[section]:
        return config[section][key]
        
    return default

def _get_single_level_config(key, default=None):
    """
    Get a configuration value by key only (legacy format).
    
    Args:
        key: Configuration key
        default: Default value if not found
        
    Returns:
        Configuration value or default
    """
    # First check if it's in environment variables
    env_val = os.environ.get(key)
    if env_val is not None:
        return env_val
        
    # Then check in config directly
    if key in _config_values:
        return _config_values[key]
        
    return default

def update_config_value(key: str, value: Any) -> bool:
    """
    Update a configuration value globally.
    
    Args:
        key: Configuration key
        value: Value to set
        
    Returns:
        True if successful
    """
    global _config_values
    _config_values[key] = value
    return True

def get_config() -> Dict[str, Any]:
    """
    Get the current configuration.
    
    Returns:
        Current configuration
    """
    return _config_values.copy()

def integrate_with_app(app) -> None:
    """
    Integrate configuration with the FastAPI app.
    
    Args:
        app: FastAPI application
    """
    @app.get("/api/config")
    async def get_config_endpoint(section: Optional[str] = None):
        """API endpoint to retrieve configuration"""
        # For security, only return non-sensitive config items
        safe_config = {
            "app": {
                "version": get_config_value("VERSION", "1.0.0"),
                "environment": get_config_value("ENVIRONMENT", "production")
            },
            "features": {
                "enable_advanced_loss_management": get_config_value("ENABLE_ADVANCED_LOSS_MANAGEMENT", True),
                "enable_multi_stage_tp": get_config_value("ENABLE_MULTI_STAGE_TP", True),
                "enable_market_structure_analysis": get_config_value("ENABLE_MARKET_STRUCTURE_ANALYSIS", True)
            },
            "risk": {
                "max_daily_loss": get_config_value("MAX_DAILY_LOSS", 0.20),
                "max_risk_percentage": get_config_value("MAX_RISK_PERCENTAGE", 2.0)
            }
        }
        
        if section and section in safe_config:
            return {"status": "success", "data": safe_config[section]}
        
        return {"status": "success", "data": safe_config}
