"""
Configuration management module for the FX Trading Bridge.

This module provides functions for managing configuration values across the application.
"""

import os
import json
import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

# Setup logging
logger = logging.getLogger(__name__)

# Config storage
_config_values = {}
_config_file = "config.json"

def get_config_value(key: str, default: Any = None) -> Any:
    """
    Get a configuration value by key.
    
    Args:
        key: The configuration key to retrieve
        default: Default value if key doesn't exist
        
    Returns:
        The configuration value or default if not found
    """
    # First check if it's in environment variables
    env_val = os.environ.get(key)
    if env_val is not None:
        return env_val
        
    # Then check in-memory storage
    if key in _config_values:
        return _config_values[key]
        
    # Finally check config file
    try:
        if os.path.exists(_config_file):
            with open(_config_file, 'r') as f:
                config = json.load(f)
                if key in config:
                    return config[key]
    except Exception as e:
        logger.error(f"Error reading config file: {str(e)}")
    
    return default

def update_config_value(key: str, value: Any, persist: bool = False) -> bool:
    """
    Update a configuration value.
    
    Args:
        key: The configuration key to update
        value: The new value
        persist: Whether to save to the config file
        
    Returns:
        True if successful, False otherwise
    """
    _config_values[key] = value
    
    if persist:
        try:
            config = {}
            if os.path.exists(_config_file):
                with open(_config_file, 'r') as f:
                    config = json.load(f)
            
            config[key] = value
            
            with open(_config_file, 'w') as f:
                json.dump(config, f, indent=2)
                
            return True
        except Exception as e:
            logger.error(f"Error updating config file: {str(e)}")
            return False
    
    return True

def integrate_with_app(app) -> None:
    """
    Integrate configuration with the FastAPI app.
    
    Args:
        app: The FastAPI application instance
    """
    @app.get("/api/config")
    async def get_config(section: Optional[str] = None):
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

    @app.post("/api/config/{section}")
    async def update_config(section: str, data: Dict[str, Any]):
        """API endpoint to update configuration"""
        # This should be properly secured in production
        try:
            for key, value in data.items():
                update_config_value(f"{section.upper()}_{key.upper()}", value, persist=True)
            return {"status": "success", "message": f"Updated {section} configuration"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

# Backwards compatibility for legacy settings
def get_legacy_setting(key: str, default: Any = None) -> Any:
    """Legacy function for backwards compatibility"""
    return get_config_value(key, default) 