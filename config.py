import os
import configparser
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic import SecretStr

class Settings(BaseSettings):
    # Add all your config fields here
    oanda_account_id: str = Field(default=os.environ.get("OANDA_ACCOUNT_ID", ""))
    oanda_access_token: SecretStr = Field(default=os.environ.get("OANDA_ACCESS_TOKEN", ""))
    oanda_environment: str = Field(default=os.environ.get("OANDA_ENVIRONMENT", "practice"))
    database_url: str = Field(default=os.environ.get("DATABASE_URL", ""))
    db_min_connections: int = Field(default=int(os.environ.get("DB_MIN_CONNECTIONS", "5")))
    db_max_connections: int = Field(default=int(os.environ.get("DB_MAX_CONNECTIONS", "20")))
    backup_dir: str = Field(default=os.environ.get("BACKUP_DIR", "./backups"))
    max_risk_percentage: float = Field(default=float(os.environ.get("MAX_RISK_PERCENTAGE", "20.0")))
    max_portfolio_heat: float = Field(default=float(os.environ.get("MAX_PORTFOLIO_HEAT", "70.0")))
    max_daily_loss: float = Field(default=float(os.environ.get("MAX_DAILY_LOSS", "50.0")))
    max_positions_per_symbol: int = Field(default=int(os.environ.get("MAX_POSITIONS_PER_SYMBOL", "5")))
    enable_broker_reconciliation: bool = Field(default=bool(os.environ.get("ENABLE_BROKER_RECONCILIATION", "true").lower() == "true"))
    # Slack and Telegram notification settings
    slack_webhook_url: str = Field(default=os.environ.get("SLACK_WEBHOOK_URL", ""))
    telegram_bot_token: str = Field(default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = Field(default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

def load_config():
    try:
        return Settings()
    except Exception as e:
        # Fallback to config.ini if environment setup fails
        config_file = "config.ini"
        if os.path.exists(config_file):
            parser = configparser.ConfigParser()
            parser.read(config_file)
            # Parse config.ini and create Settings manually
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
            raise RuntimeError(f"No config found in env or config.ini. Error: {e}")

config = load_config()
