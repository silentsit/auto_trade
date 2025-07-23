"""
INSTITUTIONAL TRADING BOT CONFIGURATION
Enhanced with robust error handling and multi-environment support
"""

import os
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Emergency fallback for missing DATABASE_URL
if not os.getenv("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "sqlite:///trading_bot.db"

# Load environment variables from file
load_dotenv("environment.env")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Database configuration with connection pooling"""
    url: str = Field(alias="DATABASE_URL")
    pool_size: int = Field(default=10, alias="DB_MIN_CONNECTIONS")
    max_overflow: int = Field(default=20, alias="DB_MAX_CONNECTIONS")
    pool_timeout: int = Field(default=30)
    pool_recycle: int = Field(default=3600)
    
    class Config:
        populate_by_name = True

settings = DatabaseConfig()


class OANDAConfig(BaseModel):
    """OANDA API configuration with enhanced settings"""
    # Core Authentication
    access_token: str = Field(default="", alias="OANDA_ACCESS_TOKEN")
    account_id: str = Field(default="", alias="OANDA_ACCOUNT_ID")
    environment: str = Field(default="practice", alias="OANDA_ENVIRONMENT")  # practice or live
    api_url: str = Field(default="", alias="OANDA_API_URL")
    
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
    
    class Config:
        populate_by_name = True


class TradingConfig(BaseModel):
    """Trading parameters and risk management"""
    # Risk Management
    max_risk_per_trade: float = Field(default=10.0, ge=0.1, le=20.0, alias="DEFAULT_RISK_PERCENTAGE")
    max_daily_loss: float = Field(default=50.0, ge=1.0, le=100.0, alias="MAX_DAILY_LOSS")
    max_positions: int = Field(default=10, ge=1, le=50)
    default_position_size: float = Field(default=1.0, ge=0.1, le=10.0)
    
    # Position Management
    enable_stop_loss: bool = Field(default=True)
    enable_take_profit: bool = Field(default=True)
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
    enable_weekend_position_limits: bool = Field(default=True, alias="ENABLE_WEEKEND_POSITION_LIMITS")
    weekend_position_max_age_hours: int = Field(default=48, ge=24, le=72, alias="WEEKEND_POSITION_MAX_AGE_HOURS")
    weekend_auto_close_buffer_hours: int = Field(default=6, ge=1, le=12, alias="WEEKEND_AUTO_CLOSE_BUFFER_HOURS")
    weekend_position_check_interval: int = Field(default=3600, ge=300, le=7200, alias="WEEKEND_POSITION_CHECK_INTERVAL")
    
    class Config:
        populate_by_name = True


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
    debug_mode: bool = Field(default=False, alias="DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file_retention_days: int = Field(default=30)
    
    # Performance
    max_concurrent_requests: int = Field(default=10)
    cache_timeout_seconds: int = Field(default=300)
    
    # Health checks
    health_check_interval: int = Field(default=60)
    enable_metrics_collection: bool = Field(default=True)
    
    class Config:
        populate_by_name = True


class Settings(BaseSettings):
    """Main configuration class with environment variable support"""
    
    # Environment
    environment: str = Field(default="development")
    debug: bool = Field(default=False)
    
    # OANDA Configuration - Direct field mapping
    oanda_access_token: str = Field(default="", alias="OANDA_ACCESS_TOKEN")
    oanda_account_id: str = Field(default="", alias="OANDA_ACCOUNT_ID")
    oanda_environment: str = Field(default="practice", alias="OANDA_ENVIRONMENT")
    oanda_api_url: str = Field(default="", alias="OANDA_API_URL")
    
    # Database
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    
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
    
    # Additional fields for backward compatibility
    active_exchange: str = Field(default="OANDA")
    api_key: str = Field(default="")
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")
    alert_webhook_secret: str = Field(default="")
    
    class Config:
        env_file = "environment.env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Allow extra fields from environment
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_environment_variables()
        self._validate_critical_settings()
    
    def _load_environment_variables(self):
        """Load environment variables manually if needed"""
        logger.info("✅ Environment variables loaded successfully")
    
    def _validate_critical_settings(self):
        """Validate critical configuration settings"""
        errors = []
        
        # OANDA validation using direct fields
        if not self.oanda_access_token:
            errors.append("OANDA access token is required")
        if not self.oanda_account_id:
            errors.append("OANDA account ID is required")
        if self.oanda_environment not in ["practice", "live"]:
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
        if self.oanda_environment == "live":
            return "https://api-fxtrade.oanda.com"
        else:
            return "https://api-fxpractice.oanda.com"
    
    def get_oanda_stream_url(self) -> str:
        """Get OANDA streaming API base URL based on environment"""
        if self.oanda_environment == "live":
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
    
    @property
    def database_url(self) -> str:
        """Get database URL with fallback to SQLite"""
        db_url = os.getenv("DATABASE_URL", "")
        if not db_url or "localhost" in db_url:
            # Fallback to SQLite for development/testing
            return "sqlite:///trading_bot.db"
        return db_url


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
            return self._settings.oanda_access_token
        elif name == 'oanda_account_id':
            return self._settings.oanda_account_id
        elif name == 'oanda_environment':
            return self._settings.oanda_environment
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
    return OANDAConfig(
        access_token=settings.oanda_access_token,
        account_id=settings.oanda_account_id,
        environment=settings.oanda_environment,
        api_url=settings.oanda_api_url
    )

def get_trading_config() -> TradingConfig:
    """Get trading configuration"""
    return settings.trading

def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return settings.database
