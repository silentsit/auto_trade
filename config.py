"""
INSTITUTIONAL TRADING BOT CONFIGURATION
Enhanced with robust error handling and multi-environment support
"""

import os
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings

# Load environment variables from env.env file
try:
    from dotenv import load_dotenv
    load_dotenv("env.env")
    logger = logging.getLogger(__name__)
    logger.info("✅ Environment variables loaded from env.env")
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("python-dotenv not available, using system environment variables")
except Exception as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Failed to load env.env: {e}")

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
    max_risk_per_trade: float = Field(default=10.0, ge=0.1, le=20.0)  # Per-trade risk limit
    max_daily_loss: float = Field(default=20.0, ge=1.0, le=100.0)  # Reduced from 50% to 20% for better risk management
    max_positions: int = Field(default=10, ge=1, le=50)
    default_position_size: float = Field(default=1.0, ge=0.1, le=10.0)
    
    # Position Management
    enable_stop_loss: bool = Field(default=True)
    enable_take_profit: bool = Field(default=True)
    max_positions_per_symbol: int = Field(default=3, ge=1, le=10)
    
    # Margin and Leverage Settings
    margin_utilization_percentage: float = Field(default=85.0, ge=50.0, le=95.0)
    max_leverage_utilization: float = Field(default=80.0, ge=50.0, le=90.0)
    
    # 🚨 NEW: Conservative Position Sizing Settings
    enable_conservative_sizing: bool = Field(default=True, description="Enable conservative position sizing to prevent oversized trades")
    min_stop_loss_pips: float = Field(default=0.0005, ge=0.0001, le=0.01, description="Minimum stop loss distance in pips (0.0005 = 5 pips)")
    max_position_size_multiplier: float = Field(default=0.5, ge=0.1, le=2.0, description="Maximum position size as multiplier of calculated size (0.5 = 50% of calculated)")
    
    # ATR Settings
    atr_stop_loss_multiplier: float = Field(default=2.5, ge=0.5, le=5.0)  # Increased from 1.5 to 2.5 for wider stops
    atr_take_profit_multiplier: float = Field(default=3.0, ge=1.0, le=10.0)
    
    # Trading Hours
    trading_enabled: bool = Field(default=True)
    trading_start_hour: int = Field(default=0, ge=0, le=23)
    trading_end_hour: int = Field(default=23, ge=0, le=23)
    
    # Correlation Management
    enable_correlation_limits: bool = Field(default=True)
    correlation_threshold_high: float = Field(default=0.70, ge=0.0, le=1.0)  # ≥70%
    correlation_threshold_medium: float = Field(default=0.60, ge=0.0, le=1.0)  # 60-70%
    max_correlated_positions: int = Field(default=3, ge=1, le=10)
    
    # Slippage and Execution
    max_slippage_pips: float = Field(default=2.0)
    execution_timeout: int = Field(default=30)
    
    # Weekend Position Management
    enable_weekend_position_limits: bool = Field(default=True)
    weekend_position_max_age_hours: int = Field(default=48, ge=24, le=72)
    weekend_auto_close_buffer_hours: int = Field(default=6, ge=1, le=12)
    weekend_position_check_interval: int = Field(default=3600, ge=300, le=7200)

    # Phase 1: Deterministic taper & liquidity gate (ProfitRideOverride)
    enable_profit_ride_taper: bool = Field(default=True, description="Enable scale-out tapering with SLride and slippage gate")
    taper_target_initial_atr: float = Field(default=3.0, ge=1.0, le=6.0, description="Initial ATR target to start tapering")
    taper_m1_fraction: float = Field(default=0.20, ge=0.05, le=0.5, description="Fraction to close at M1 milestone")
    taper_m2_fraction: float = Field(default=0.10, ge=0.05, le=0.5, description="Fraction to close at M2 milestone")
    taper_mn_fraction: float = Field(default=0.20, ge=0.05, le=0.5, description="Fraction to close when regime confidence decays")
    taper_max_legs: int = Field(default=4, ge=1, le=8, description="Maximum number of taper legs per position")
    taper_min_clip_fraction: float = Field(default=0.10, ge=0.01, le=0.5, description="Minimum fraction of position size for a clip")
    regime_confidence_min: float = Field(default=0.70, ge=0.5, le=0.9, description="Minimum regime confidence to keep full size")
    slippage_budget_bps: float = Field(default=5.0, ge=1.0, le=50.0, description="Max acceptable implied slippage in basis points for taper legs")
    slride_ema_period: int = Field(default=21, ge=10, le=55, description="EMA period for SLride anchor")
    slride_buffer_atr_mult: float = Field(default=0.5, ge=0.1, le=2.0, description="ATR multiplier buffer applied to SLride anchor")
    
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
    debug_mode: bool = Field(default=False)
    log_level: str = Field(default="INFO")
    log_file_retention_days: int = Field(default=30)
    
    # Performance
    max_concurrent_requests: int = Field(default=10)
    cache_timeout_seconds: int = Field(default=300)
    
    # Health checks
    health_check_interval: int = Field(default=60)
    enable_metrics_collection: bool = Field(default=True)
    
    class Config:
        populate_by_name = True


class EnvironmentConfig(BaseModel):
    """Environment-specific configuration"""
    name: str
    debug: bool
    log_level: str
    database_url: str
    oanda_environment: str
    trading_enabled: bool
    risk_multiplier: float
    max_positions: int
    api_host: str
    api_port: int
    cors_origins: List[str]
    enable_metrics: bool
    enable_notifications: bool
    backup_enabled: bool
    monitoring_enabled: bool

class Settings(BaseSettings):
    """Main configuration class with comprehensive multi-environment support"""
    environment: str = Field(default="development")
    debug: bool = Field(default=False)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    oanda: OANDAConfig = Field(default_factory=OANDAConfig)
    trading: TradingConfig = Field(default_factory=TradingConfig)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    system: SystemConfig = Field(default_factory=SystemConfig)
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=1)
    secret_key: str = Field(default="your-secret-key-change-in-production")
    allowed_hosts: List[str] = Field(default=["*"])
    
    # Environment-specific configurations
    environments: Dict[str, EnvironmentConfig] = Field(default_factory=dict)
    
    # Server configuration
    server: Dict[str, Any] = Field(default_factory=lambda: {
        "host": "0.0.0.0",
        "port": 8000,
        "reload": False,
        "workers": 1,
        "access_log": True
    })
    
    # Additional fields to handle extra environment variables
    position_size_safety_factor: str = Field(default="0.85")
    slippage_tolerance_pips: str = Field(default="10.0")
    default_risk_percentage: str = Field(default="10.0")  # Default risk percentage
    max_daily_loss: str = Field(default="50.0")
    max_daily_trades: str = Field(default="50")
    max_portfolio_heat: str = Field(default="70.0")
    max_risk_percentage: str = Field(default="20.0")
    port: str = Field(default="8000")
    pythonpath: str = Field(default="/opt/render/project/src")
    pythonunbuffered: str = Field(default="1")
    read_timeout: str = Field(default="30")
    active_exchange: str = Field(default="OANDA")
    allowed_origins: str = Field(default="*")
    api_key: str = Field(default="389!")
    backup_dir: str = Field(default="/backups")
    backup_interval_hours: str = Field(default="24")
    connect_timeout: str = Field(default="10")
    host: str = Field(default="0.0.0.0")
    max_execution_latency_ms: str = Field(default="5000")
    enable_broker_reconciliation: str = Field(default="True")
    db_max_connections: str = Field(default="20")
    db_min_connections: str = Field(default="5")
    alert_webhook_secret: str = Field(default="your_secret_here")
    telegram_bot_token: str = Field(default="7918438800:AAH8EQEfHUiJ8gM847DNHzLTsLTmjD880F6U")
    telegram_chat_id: str = Field(default="164149601")
    class Config:
        env_file = "env.env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_environment_variables()
        self._setup_environment_configs()
        self._apply_environment_config()
        self._validate_critical_settings()
        
    def _setup_environment_configs(self):
        """Setup environment-specific configurations"""
        self.environments = {
            "development": EnvironmentConfig(
                name="development",
                debug=True,
                log_level="DEBUG",
                database_url="sqlite:///trading_bot_dev_v2.db",
                oanda_environment="practice",
                trading_enabled=False,  # Disabled for safety in dev
                risk_multiplier=0.5,  # 50% of normal risk
                max_positions=3,
                api_host="127.0.0.1",
                api_port=8000,
                cors_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
                enable_metrics=True,
                enable_notifications=False,
                backup_enabled=False,
                monitoring_enabled=True
            ),
            "staging": EnvironmentConfig(
                name="staging",
                debug=False,
                log_level="INFO",
                database_url=os.getenv("DATABASE_URL", "postgresql://localhost/trading_bot_staging"),
                oanda_environment="practice",
                trading_enabled=True,
                risk_multiplier=0.3,  # 30% of normal risk
                max_positions=5,
                api_host="0.0.0.0",
                api_port=8000,
                cors_origins=["https://staging.yourdomain.com"],
                enable_metrics=True,
                enable_notifications=True,
                backup_enabled=True,
                monitoring_enabled=True
            ),
            "production": EnvironmentConfig(
                name="production",
                debug=False,
                log_level="WARNING",
                database_url=os.getenv("DATABASE_URL", "postgresql://localhost/trading_bot_prod"),
                oanda_environment="live",
                trading_enabled=True,
                risk_multiplier=1.0,  # Full risk
                max_positions=10,
                api_host="0.0.0.0",
                api_port=8000,
                cors_origins=["https://yourdomain.com"],
                enable_metrics=True,
                enable_notifications=True,
                backup_enabled=True,
                monitoring_enabled=True
            )
        }
        
    def _apply_environment_config(self):
        """Apply environment-specific configuration"""
        env_config = self.environments.get(self.environment, self.environments["development"])
        
        # Apply environment-specific settings
        self.debug = env_config.debug
        self.system.log_level = env_config.log_level
        self.database.url = env_config.database_url
        self.oanda.environment = env_config.oanda_environment
        self.trading.trading_enabled = env_config.trading_enabled
        self.trading.max_positions = env_config.max_positions
        self.api_host = env_config.api_host
        self.api_port = env_config.api_port
        self.system.enable_metrics_collection = env_config.enable_metrics
        self.notifications.enabled = env_config.enable_notifications
        
        # Apply risk multiplier
        if env_config.risk_multiplier != 1.0:
            self.trading.max_risk_per_trade *= env_config.risk_multiplier
            self.trading.max_daily_loss *= env_config.risk_multiplier
            
        logger.info(f"🔧 Applied {self.environment} environment configuration")
        logger.info(f"   Debug: {self.debug}")
        logger.info(f"   Log Level: {self.system.log_level}")
        logger.info(f"   Trading Enabled: {self.trading.trading_enabled}")
        logger.info(f"   Risk Multiplier: {env_config.risk_multiplier}")
        logger.info(f"   Max Positions: {self.trading.max_positions}")
        
    def get_environment_info(self) -> Dict[str, Any]:
        """Get current environment information"""
        env_config = self.environments.get(self.environment, self.environments["development"])
        return {
            "current_environment": self.environment,
            "config": env_config.dict(),
            "database_url_masked": self._mask_database_url(self.database.url),
            "oanda_environment": self.oanda.environment,
            "trading_enabled": self.trading.trading_enabled,
            "debug_mode": self.debug,
            "log_level": self.system.log_level
        }
        
    def _mask_database_url(self, url: str) -> str:
        """Mask sensitive information in database URL"""
        if "@" in url:
            parts = url.split("://")
            if len(parts) == 2:
                protocol = parts[0]
                rest = parts[1]
                if "@" in rest:
                    creds, host_db = rest.split("@", 1)
                    if ":" in creds:
                        user, _ = creds.split(":", 1)
                        return f"{protocol}://{user}:***@{host_db}"
        return url
        
    def _load_environment_variables(self):
        """Enhanced environment variable loading with multiple fallbacks"""
        settings_dict = {}
        if os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT"):
            settings_dict["oanda_account_id"] = os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT")
        oanda_token = (os.getenv("OANDA_ACCESS_TOKEN") or 
                      os.getenv("OANDA_TOKEN") or 
                      os.getenv("OANDA_API_TOKEN") or
                      os.getenv("ACCESS_TOKEN"))
        if oanda_token:
            settings_dict["oanda_access_token"] = oanda_token
        if os.getenv("OANDA_ENVIRONMENT"):
            settings_dict["oanda_environment"] = os.getenv("OANDA_ENVIRONMENT")
        if settings_dict.get("oanda_access_token"):
            self.oanda.access_token = settings_dict["oanda_access_token"]
        if settings_dict.get("oanda_account_id"):
            self.oanda.account_id = settings_dict["oanda_account_id"]
        if settings_dict.get("oanda_environment"):
            self.oanda.environment = settings_dict["oanda_environment"]
        if os.getenv("DATABASE_URL"):
            self.database.url = os.getenv("DATABASE_URL")
        if os.getenv("DEBUG"):
            self.debug = os.getenv("DEBUG").lower() in ("true", "1", "yes")
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
        if not self.oanda.access_token:
            errors.append("OANDA access token is required")
        if not self.oanda.account_id:
            errors.append("OANDA account ID is required")
        if self.oanda.environment not in ["practice", "live"]:
            errors.append("OANDA environment must be 'practice' or 'live'")
        if not self.database.url:
            errors.append("Database URL is required")
        if errors:
            error_msg = "Critical configuration errors:\n" + "\n".join(f"  - {error}" for error in errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.info("✅ Configuration validation passed")
    def get_oanda_base_url(self) -> str:
        if self.oanda.environment == "live":
            return "https://api-fxtrade.oanda.com"
        else:
            return "https://api-fxpractice.oanda.com"
    def get_oanda_stream_url(self) -> str:
        if self.oanda.environment == "live":
            return "https://stream-fxtrade.oanda.com"
        else:
            return "https://stream-fxpractice.oanda.com"
    def is_production(self) -> bool:
        return self.environment.lower() == "production"
    def get_log_config(self) -> Dict[str, Any]:
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

settings = Settings()

class ConfigWrapper:
    """Wrapper to provide backward compatibility for flat config attributes"""
    def __init__(self, settings_instance):
        self._settings = settings_instance
    def __getattr__(self, name):
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

config = ConfigWrapper(settings)

def get_oanda_config() -> OANDAConfig:
    return settings.oanda

def get_trading_config() -> TradingConfig:
    return settings.trading

def get_database_config() -> DatabaseConfig:
    return settings.database
