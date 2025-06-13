import os
import configparser
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic import SecretStr

class Settings(BaseSettings):
    # OANDA Settings
    oanda_account_id: str = Field(default="")
    oanda_access_token: SecretStr = Field(default="")
    oanda_environment: str = Field(default="practice")
    
    # Database Settings
    database_url: str = Field(default="")
    db_min_connections: int = Field(default=5)
    db_max_connections: int = Field(default=20)
    
    # System Settings
    backup_dir: str = Field(default="./backups")
    
    # Risk Management Settings
    max_risk_percentage: float = Field(default=15.0)
    max_portfolio_heat: float = Field(default=70.0)
    max_daily_loss: float = Field(default=50.0)
    max_positions_per_symbol: int = Field(default=10)
    
    # Features
    enable_broker_reconciliation: bool = Field(default=True)

    min_trade_size: int = 1000  # For FX
    max_trade_size: int = 100000000  # Or whatever is reasonable for your account
    min_sl_distance: float = 0.005  # 50 pips for FX, or whatever fits your strategy
    min_risk_percent: float = 1.0
    max_risk_percent: float = 15.0
    min_atr: float = 0.0001
    
    # Notification Settings
    slack_webhook_url: str = Field(default="")
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")
    
    atr_stop_loss_multiplier: float = 2.0  # Centralized ATR multiplier for stop loss
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

def load_config():
    """Load configuration from environment variables or config.ini"""
    try:
        # First try environment variables
        settings_dict = {}
        
        # OANDA settings
        if os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT"):
            settings_dict["oanda_account_id"] = os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT")
        
        if os.getenv("OANDA_ACCESS_TOKEN") or os.getenv("OANDA_TOKEN"):
            settings_dict["oanda_access_token"] = os.getenv("OANDA_ACCESS_TOKEN") or os.getenv("OANDA_TOKEN")
            
        if os.getenv("OANDA_ENVIRONMENT"):
            settings_dict["oanda_environment"] = os.getenv("OANDA_ENVIRONMENT")
        
        # Database settings
        if os.getenv("DATABASE_URL"):
            settings_dict["database_url"] = os.getenv("DATABASE_URL")
            
        # Risk settings
        if os.getenv("MAX_RISK_PERCENTAGE"):
            settings_dict["max_risk_percentage"] = float(os.getenv("MAX_RISK_PERCENTAGE"))
            
        if os.getenv("MAX_PORTFOLIO_HEAT"):
            settings_dict["max_portfolio_heat"] = float(os.getenv("MAX_PORTFOLIO_HEAT"))
            
        if os.getenv("MAX_DAILY_LOSS"):
            settings_dict["max_daily_loss"] = float(os.getenv("MAX_DAILY_LOSS"))
        
        # Create settings with environment variables
        if settings_dict:
            return Settings(**settings_dict)
        else:
            return Settings()
            
    except Exception as e:
        # Fallback to config.ini if environment setup fails
        config_file = "config.ini"
        if os.path.exists(config_file):
            parser = configparser.ConfigParser()
            parser.read(config_file)
            
            config_dict = {}
            
            if parser.has_section('oanda'):
                config_dict.update({
                    'oanda_account_id': parser.get('oanda', 'account_id', fallback=''),
                    'oanda_access_token': parser.get('oanda', 'access_token', fallback=''),
                    'oanda_environment': parser.get('oanda', 'environment', fallback='practice'),
                })
            
            if parser.has_section('database'):
                config_dict.update({
                    'database_url': parser.get('database', 'url', fallback=''),
                    'db_min_connections': parser.getint('database', 'min_connections', fallback=5),
                    'db_max_connections': parser.getint('database', 'max_connections', fallback=20),
                })
            
            if parser.has_section('risk'):
                config_dict.update({
                    'max_risk_percentage': parser.getfloat('risk', 'max_risk_percentage', fallback=20.0),
                    'max_portfolio_heat': parser.getfloat('risk', 'max_portfolio_heat', fallback=70.0),
                    'max_daily_loss': parser.getfloat('risk', 'max_daily_loss', fallback=50.0),
                })
            
            return Settings(**config_dict)
        else:
            print(f"Warning: No config found in env or config.ini. Using defaults. Error: {e}")
            return Settings()

# Create global config instance
config = load_config()

# Validate critical settings
if not config.oanda_account_id:
    print("WARNING: OANDA_ACCOUNT_ID not set. Trading will not work.")
    
if not config.oanda_access_token or str(config.oanda_access_token) == "":
    print("WARNING: OANDA_ACCESS_TOKEN not set. Trading will not work.")

if not config.database_url:
    print("WARNING: DATABASE_URL not set. Database persistence will not work.")
