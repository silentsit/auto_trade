"""
Main configuration module for the FX Trading Bridge.

This module provides the central configuration system for the application.
"""

import os
import json
import logging
from typing import Any, Dict, Optional, List, Union
from pathlib import Path

# Configuration class
class Config:
    """Configuration container class"""
    
    def __init__(self):
        # Default values
        self.environment = "production"
        self.debug = False
        self.log_level = "INFO"
        self.oanda_api_url = "https://api-fxtrade.oanda.com/v3"
        self.oanda_environment = "practice"
        self.max_retries = 3
        self.max_daily_loss = 0.20
        self.max_risk_percentage = 2.0
        self.enable_advanced_loss_management = True
        self.enable_multi_stage_tp = True
        self.enable_market_structure_analysis = True
        self.trade_24_7 = True
        
        # Load from environment variables
        self._load_from_env()
    
    def _load_from_env(self):
        """Load configuration from environment variables"""
        for key in dir(self):
            if key.startswith('_'):
                continue
                
            env_key = key.upper()
            env_value = os.environ.get(env_key)
            
            if env_value is not None:
                current_value = getattr(self, key)
                
                # Convert based on the type of the default value
                if isinstance(current_value, bool):
                    if env_value.lower() in ('true', 't', 'yes', 'y', '1'):
                        setattr(self, key, True)
                    elif env_value.lower() in ('false', 'f', 'no', 'n', '0'):
                        setattr(self, key, False)
                elif isinstance(current_value, int):
                    try:
                        setattr(self, key, int(env_value))
                    except ValueError:
                        pass
                elif isinstance(current_value, float):
                    try:
                        setattr(self, key, float(env_value))
                    except ValueError:
                        pass
                else:
                    setattr(self, key, env_value)

# Global config instance
config = Config()

# Configuration error class
class ConfigurationError(Exception):
    """Exception raised for configuration errors"""
    pass

# Default configuration
DEFAULT_CONFIG = {
    "app": {
        "environment": "production",
        "log_level": "INFO"
    },
    "api": {
        "oanda_api_url": "https://api-fxtrade.oanda.com/v3",
        "oanda_environment": "practice"
    },
    "risk": {
        "max_daily_loss": 0.20,
        "max_risk_percentage": 2.0
    },
    "features": {
        "enable_advanced_loss_management": True,
        "enable_multi_stage_tp": True,
        "enable_market_structure_analysis": True,
        "trade_24_7": True
    }
}

def load_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from a file
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dictionary with the loaded configuration
    """
    if not config_file:
        config_file = os.environ.get("CONFIG_FILE", "config.json")
        
    if not os.path.exists(config_file):
        # Return default config if file doesn't exist
        return DEFAULT_CONFIG
        
    try:
        with open(config_file, 'r') as f:
            loaded_config = json.load(f)
            return loaded_config
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration: {str(e)}")

def get_config() -> Dict[str, Any]:
    """
    Get the current configuration
    
    Returns:
        Dictionary with the current configuration
    """
    current_config = {}
    
    # Convert config object to dictionary
    for key in dir(config):
        if not key.startswith('_'):
            current_config[key] = getattr(config, key)
            
    return current_config

def update_config(new_config: Dict[str, Any]) -> None:
    """
    Update the configuration
    
    Args:
        new_config: Dictionary with new configuration values
    """
    for key, value in new_config.items():
        if hasattr(config, key):
            setattr(config, key, value)

def save_config(config_data: Dict[str, Any], config_file: Optional[str] = None) -> None:
    """
    Save configuration to a file
    
    Args:
        config_data: Configuration data to save
        config_file: Path to the configuration file
    """
    if not config_file:
        config_file = os.environ.get("CONFIG_FILE", "config.json")
        
    try:
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
    except Exception as e:
        raise ConfigurationError(f"Failed to save configuration: {str(e)}")

def import_legacy_settings() -> Dict[str, Any]:
    """
    Import settings from legacy format
    
    Returns:
        Dictionary with imported settings
    """
    # This is a placeholder for backwards compatibility
    return {} 