"""
INSTITUTIONAL TRADING BOT CONFIGURATION
Enhanced with robust error handling and multi-environment support
"""

import os
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Database configuration with connection pooling"""
    url: str = Field(default="postgresql://localhost/trading_bot")
    pool_size: int = Field(default=10)
    max_overflow: int = Field(default=20)
    pool_timeout: int = Field(default=30)
    pool_recycle: int = Field(default=3600)


class OANDAConfig(BaseModel):
    """OANDA API configuration with enhanced settings"""
    # Core Authentication
    access_token: str = Field(default="")
    account_id: str = Field(default="")
    environment: str = Field(default="practice")  # practice or live
    
    # Connection Settings - Enhanced for stability
    request_timeout: int = Field(default=45)  # Increased from 30 to 45 seconds
    max_retries: int = Field(default=8)  # Increased from 5 to 8 retries
    retry_delay: float = Field(default=2.0)  # Base retry delay
    connection_pool_size: int = Field(default=15)  # Increased pool size
    keep_alive_timeout: int = Field(default=120)  # Keep connections alive longer
    
    # FIX: Additional connection stability settings
    initial_retry_delay: float = Field(default=1.0)  # Start with shorter delays
    max_retry_delay: float = Field(default=30.0)  # Cap maximum retry delay  
    exponential_backoff: bool = Field(default=True)  # Enable exponential backoff
    connection_retries: int = Field(default=3)  # Separate connection retry count
    
    # Pricing and Trading
    stream_timeout: int = Field(default=60)
    price_precision: int = Field(default=5)
    
    # Rate limiting
    requests_per_second: int = Field(default=100)
    burst_limit: int = Field(default=200)


class TradingConfig(BaseModel):
    """Trading parameters and risk management"""
    # Risk Management
    max_risk_per_trade: float = Field(default=2.0, ge=0.1, le=10.0)
    max_daily_loss: float = Field(default=5.0, ge=1.0, le=20.0)
    max_positions: int = Field(default=10, ge=1, le=50)
    default_position_size: float = Field(default=1.0, ge=0.1, le=10.0)
    
    # Position Management
    enable_stop_loss: bool = Field(default=True)
    enable_take_profit: bool = Field(default=True)
    default_stop_loss_pips: int = Field(default=50, ge=10, le=200)
    default_take_profit_pips: int = Field(default=100, ge=20, le=500)
    max_positions_per_symbol: int = Field(default=3, ge=1, le=10)
    
    # ATR Settings
    atr_stop_loss_multiplier: float = Field(default=2.0, ge=0.5, le=5.0)
    atr_take_profit_multiplier: float = Field(default=3.0, ge=1.0, le=10.0)
    
    # Trading Hours
    trading_enabled: bool = Field(default=True)
    trading_start_hour: int = Field(default=0, ge=0, le=23)
    trading_end_hour: int = Field(default=23, ge=0, le=23)
    
    # Correlation Management
    enable_correlation_limits: bool = Field(default=True)
    correlation_threshold_high: float = Field(default=0.75, ge=0.0, le=1.0)
    correlation_threshold_medium: float = Field(default=0.50, ge=0.0, le=1.0)
    max_correlated_positions: int = Field(default=3, ge=1, le=10)
    
    # Slippage and Execution
    max_slippage_pips: float = Field(default=2.0)
    execution_timeout: int = Field(default=30)
    
    # Weekend Position Management
    enable_weekend_position_limits: bool = Field(default=True)
    weekend_position_max_age_hours: int = Field(default=48, ge=24, le=72)  # Max 48 hours over weekend
    weekend_auto_close_buffer_hours: int = Field(default=6, ge=1, le=12)   # Warning buffer
    weekend_position_check_interval: int = Field(default=3600, ge=300, le=7200)  # Check every hour


class NotificationConfig(BaseModel):
    """Notification settings"""
    enabled: bool = Field(default=True)
    email_enabled: bool = Field(default=False)
    webhook_enabled: bool = Field(default=True)
    
    # Notification levels
    notify_on_trade: bool = Field(default=True)
    notify_on_error: bool = Field(default=True)
    notify_on_system_events: bool = Field(default=True)
    
    # Rate limiting
    max_notifications_per_hour: int = Field(default=50)


class SystemConfig(BaseModel):
    """System-wide configuration"""
    debug_mode: bool = Field(default=False)
    log_level: str = Field(default="INFO")
    log_file_retention_days: int = Field(default=30)
    
    # Performance
    max_concurrent_requests: int = Field(default=10)
    cache_timeout_seconds: int = Field(default=300)
    
    # Health checks
    health_check_interval: int = Field(default=60)
    enable_metrics_collection: bool = Field(default=True)


class Settings(BaseSettings):
    """Main configuration class with environment variable support"""
    
    # Environment
    environment: str = Field(default="development")
    debug: bool = Field(default=False)
    
    # Database
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    
    # OANDA Configuration
    oanda: OANDAConfig = Field(default_factory=OANDAConfig)
    
    # Trading Configuration  
    trading: TradingConfig = Field(default_factory=TradingConfig)
    
    # Notifications
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    
    # System
    system: SystemConfig = Field(default_factory=SystemConfig)
    
    # API Configuration
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=1)
    
    # Security
    secret_key: str = Field(default="your-secret-key-change-in-production")
    allowed_hosts: List[str] = Field(default=["*"])
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_environment_variables()
        self._validate_critical_settings()
    
    def _load_environment_variables(self):
        """Enhanced environment variable loading with multiple fallbacks"""
        settings_dict = {}
        
        # Enhanced environment variable handling with multiple fallbacks
        if os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT"):
            settings_dict["oanda_account_id"] = os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT")

        # FIX: Multiple token environment variable names for backwards compatibility
        oanda_token = (os.getenv("OANDA_ACCESS_TOKEN") or 
                      os.getenv("OANDA_TOKEN") or 
                      os.getenv("OANDA_API_TOKEN") or
                      os.getenv("ACCESS_TOKEN"))
        if oanda_token:
            settings_dict["oanda_access_token"] = oanda_token

        if os.getenv("OANDA_ENVIRONMENT"):
            settings_dict["oanda_environment"] = os.getenv("OANDA_ENVIRONMENT")

        # Enhanced validation
        if settings_dict.get("oanda_access_token"):
            self.oanda.access_token = settings_dict["oanda_access_token"]
        if settings_dict.get("oanda_account_id"):
            self.oanda.account_id = settings_dict["oanda_account_id"]
        if settings_dict.get("oanda_environment"):
            self.oanda.environment = settings_dict["oanda_environment"]
            
        # Database URL
        if os.getenv("DATABASE_URL"):
            self.database.url = os.getenv("DATABASE_URL")
            
        # Debug mode
        if os.getenv("DEBUG"):
            self.debug = os.getenv("DEBUG").lower() in ("true", "1", "yes")
            
        # Weekend position management settings
        if os.getenv("ENABLE_WEEKEND_POSITION_LIMITS"):
            self.trading.enable_weekend_position_limits = os.getenv("ENABLE_WEEKEND_POSITION_LIMITS").lower() in ("true", "1", "yes")
        if os.getenv("WEEKEND_POSITION_MAX_AGE_HOURS"):
            self.trading.weekend_position_max_age_hours = int(os.getenv("WEEKEND_POSITION_MAX_AGE_HOURS"))
        if os.getenv("WEEKEND_AUTO_CLOSE_BUFFER_HOURS"):
            self.trading.weekend_auto_close_buffer_hours = int(os.getenv("WEEKEND_AUTO_CLOSE_BUFFER_HOURS"))
        if os.getenv("WEEKEND_POSITION_CHECK_INTERVAL"):
            self.trading.weekend_position_check_interval = int(os.getenv("WEEKEND_POSITION_CHECK_INTERVAL"))
            
        logger.info("✅ Environment variables loaded successfully")
    
    def _validate_critical_settings(self):
        """Validate critical configuration settings"""
        errors = []
        
        # OANDA validation
        if not self.oanda.access_token:
            errors.append("OANDA access token is required")
        if not self.oanda.account_id:
            errors.append("OANDA account ID is required")
        if self.oanda.environment not in ["practice", "live"]:
            errors.append("OANDA environment must be 'practice' or 'live'")
            
        # Database validation
        if not self.database.url:
            errors.append("Database URL is required")
            
        if errors:
            error_msg = "Critical configuration errors:\n" + "\n".join(f"  - {error}" for error in errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.info("✅ Configuration validation passed")
    
    def get_oanda_base_url(self) -> str:
        """Get OANDA API base URL based on environment"""
        if self.oanda.environment == "live":
            return "https://api-fxtrade.oanda.com"
        else:
            return "https://api-fxpractice.oanda.com"
    
    def get_oanda_stream_url(self) -> str:
        """Get OANDA streaming API base URL based on environment"""
        if self.oanda.environment == "live":
            return "https://stream-fxtrade.oanda.com"
        else:
            return "https://stream-fxpractice.oanda.com"
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment.lower() == "production"
    
    def get_log_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                },
                "detailed": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": self.system.log_level,
                    "formatter": "standard"
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": self.system.log_level,
                    "formatter": "detailed",
                    "filename": "logs/trading_bot.log",
                    "maxBytes": 10485760,  # 10MB
                    "backupCount": 5
                }
            },
            "loggers": {
                "": {
                    "handlers": ["console", "file"],
                    "level": self.system.log_level,
                    "propagate": False
                }
            }
        }


# Global settings instance
settings = Settings()

class ConfigWrapper:
    """Wrapper to provide backward compatibility for flat config attributes"""
    
    def __init__(self, settings_instance):
        self._settings = settings_instance
    
    def __getattr__(self, name):
        """Provide backward compatibility for flat config attributes"""
        if name == 'database_url':
            return self._settings.database.url
        elif name == 'db_min_connections':
            return self._settings.database.pool_size
        elif name == 'db_max_connections': 
            return self._settings.database.max_overflow
        elif name == 'oanda_access_token':
            return self._settings.oanda.access_token
        elif name == 'oanda_account_id':
            return self._settings.oanda.account_id
        elif name == 'oanda_environment':
            return self._settings.oanda.environment
        # Correlation settings access
        elif name == 'enable_correlation_limits':
            return self._settings.trading.enable_correlation_limits
        elif name == 'correlation_threshold_high':
            return self._settings.trading.correlation_threshold_high
        elif name == 'correlation_threshold_medium':
            return self._settings.trading.correlation_threshold_medium
        elif name == 'max_correlated_positions':
            return self._settings.trading.max_correlated_positions
        elif name == 'max_positions_per_symbol':
            return self._settings.trading.max_positions_per_symbol
        elif name == 'atr_stop_loss_multiplier':
            return self._settings.trading.atr_stop_loss_multiplier
        elif name == 'atr_take_profit_multiplier':
            return self._settings.trading.atr_take_profit_multiplier
        # Weekend position management settings
        elif name == 'enable_weekend_position_limits':
            return self._settings.trading.enable_weekend_position_limits
        elif name == 'weekend_position_max_age_hours':
            return self._settings.trading.weekend_position_max_age_hours
        elif name == 'weekend_auto_close_buffer_hours':
            return self._settings.trading.weekend_auto_close_buffer_hours
        elif name == 'weekend_position_check_interval':
            return self._settings.trading.weekend_position_check_interval
        elif hasattr(self._settings, name):
            return getattr(self._settings, name)
        else:
            raise AttributeError(f"'{type(self._settings).__name__}' object has no attribute '{name}'")

# Create config object for backward compatibility - this is what other modules import
config = ConfigWrapper(settings)

# Convenience functions for backward compatibility
def get_oanda_config() -> OANDAConfig:
    """Get OANDA configuration"""
    return settings.oanda

def get_trading_config() -> TradingConfig:
    """Get trading configuration"""
    return settings.trading

def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return settings.database
