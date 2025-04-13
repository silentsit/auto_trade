"""
Configuration settings for the FX Trading Bridge application.
"""
import os
from typing import Optional
from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables with defaults.
    Pydantic's BaseSettings handles environment variables automatically.
    """
    # API settings
    host: str = "0.0.0.0"
    port: int = int(os.environ.get("PORT", 8000))  # Use Render PORT env variable or default to 8000
    environment: str = "production"  # production, development
    allowed_origins: str = "*"
    
    # OANDA settings
    oanda_account_id: str = ""
    oanda_api_token: str = ""
    oanda_api_url: str = "https://api-fxtrade.oanda.com/v3"
    oanda_environment: str = "practice"  # practice, live
    
    # Risk management settings
    default_risk_percentage: float = 2.0
    max_risk_percentage: float = 5.0
    max_daily_loss: float = 20.0  # 20% max daily loss
    max_portfolio_heat: float = 15.0  # 15% max portfolio risk
    
    # Connection settings
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    max_requests_per_minute: int = 100
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


# Initialize settings
config = Settings()

# Export HTTP timeout settings for reuse
HTTP_TIMEOUT_SETTINGS = {
    "total": config.total_timeout,
    "connect": config.connect_timeout,
    "read": config.read_timeout
} 