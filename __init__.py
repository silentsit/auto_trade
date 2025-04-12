"""
Configuration module for the FX Trading Bridge.

This module handles configuration loading, validation, and access.
"""
import os
import logging
from typing import Any, Dict, Optional, Union

from fx.core.config import config

from fx.config.main import (
    load_config,
    get_config,
    update_config,
    save_config,
    ConfigurationError,
    DEFAULT_CONFIG
)

__all__ = [
    'load_config',
    'get_config',
    'update_config',
    'save_config',
    'ConfigurationError',
    'DEFAULT_CONFIG'
]


def get_config_value(section: str, key: str, default: Any = None) -> Any:
    """
    Get a configuration value from the specified section and key.
    
    Args:
        section: Config section name
        key: Config key name
        default: Default value if key not found
        
    Returns:
        Config value or default
    """
    try:
        # For now, use a flat structure by combining section and key
        env_var = f"{section.upper()}_{key.upper()}"
        
        # First try from environment
        if os.environ.get(env_var):
            return os.environ.get(env_var)
        
        # Then try from config object
        attr_name = key.lower()
        if hasattr(config, attr_name):
            return getattr(config, attr_name)
            
        return default
    except Exception as e:
        logging.error(f"Error getting config {section}.{key}: {str(e)}")
        return default


def update_config_value(section: str, key: str, value: Any) -> bool:
    """
    Update a configuration value in memory (not persisted).
    
    Args:
        section: Config section name
        key: Config key name  
        value: New value
        
    Returns:
        True if successful, False otherwise
    """
    try:
        attr_name = key.lower()
        if hasattr(config, attr_name):
            setattr(config, attr_name, value)
            return True
        return False
    except Exception as e:
        logging.error(f"Error updating config {section}.{key}: {str(e)}")
        return False


def integrate_with_app(app: Any) -> None:
    """
    Integrate configuration with the FastAPI app.
    
    Args:
        app: FastAPI application instance
    """
    # Store config in app state for global access
    app.state.config = config


# Import legacy settings function for backward compatibility
from fx.config.main import import_legacy_settings 