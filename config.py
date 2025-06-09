import os
import configparser
from pydantic import BaseSettings, Field, SecretStr

class Settings(BaseSettings):
    # Add all your config fields here
    oanda_account_id: str = Field(default=os.environ.get("OANDA_ACCOUNT_ID", ""))
    oanda_access_token: SecretStr = Field(default=os.environ.get("OANDA_ACCESS_TOKEN", ""))
    oanda_environment: str = Field(default=os.environ.get("OANDA_ENVIRONMENT", "practice"))
    database_url: str = Field(default=os.environ.get("DATABASE_URL", ""))
    db_min_connections: int = Field(default=int(os.environ.get("DB_MIN_CONNECTIONS", 5)))
    db_max_connections: int = Field(default=int(os.environ.get("DB_MAX_CONNECTIONS", 20)))
    backup_dir: str = Field(default=os.environ.get("BACKUP_DIR", "./backups"))
    max_risk_percentage: float = Field(default=float(os.environ.get("MAX_RISK_PERCENTAGE", 20.0)))
    max_portfolio_heat: float = Field(default=float(os.environ.get("MAX_PORTFOLIO_HEAT", 70.0)))
    max_daily_loss: float = Field(default=float(os.environ.get("MAX_DAILY_LOSS", 50.0)))
    # ...add all other fields as needed...

def load_config():
    try:
        return Settings()
    except Exception:
        config_file = "config.ini"
        if os.path.exists(config_file):
            parser = configparser.ConfigParser()
            parser.read(config_file)
            # You can parse and map config.ini fields to Settings here if needed
            # For now, fallback to defaults if not found
            return Settings()
        else:
            raise RuntimeError("No config found in env or config.ini")

config = load_config()
