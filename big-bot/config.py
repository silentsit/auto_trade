import os
import configparser
from pydantic import Field, validator
from pydantic_settings import BaseSettings
from pydantic import SecretStr

class Settings(BaseSettings):
    # OANDA Settings
    oanda_account_id: str = Field(default="", env='OANDA_ACCOUNT_ID')
    oanda_access_token: SecretStr = Field(default="", env='OANDA_ACCESS_TOKEN') 
    oanda_environment: str = Field(default="practice", env='OANDA_ENVIRONMENT')
    
    # Database Settings
    database_url: str = Field(default="", env='DATABASE_URL')
    db_min_connections: int = Field(default=5, env='DB_MIN_CONNECTIONS')
    db_max_connections: int = Field(default=20, env='DB_MAX_CONNECTIONS')
    
    # System Settings
    backup_dir: str = Field(default="./backups")
    
    # Risk Management Settings
    max_risk_percentage: float = Field(default=2.0, env='MAX_RISK_PERCENTAGE')
    max_portfolio_heat: float = Field(default=70.0, env='MAX_PORTFOLIO_HEAT')
    max_daily_loss: float = Field(default=10.0, env='MAX_DAILY_LOSS')
    max_positions_per_symbol: int = Field(default=5)
    
    # Features
    enable_broker_reconciliation: bool = Field(default=True)
    
    # Notification Settings
    slack_webhook_url: SecretStr = Field(default="", env='SLACK_WEBHOOK_URL')
    telegram_bot_token: SecretStr = Field(default="", env='TELEGRAM_BOT_TOKEN')
    telegram_chat_id: str = Field(default="", env='TELEGRAM_CHAT_ID')
    
    @validator('database_url')
    def validate_database_url(cls, v):
        if v and ('<' in v or '>' in v):
            raise ValueError("DATABASE_URL contains placeholder values - check environment configuration")
        return v
    
    @validator('oanda_account_id')
    def validate_oanda_account_id(cls, v):
        if v and ('<' in v or '>' in v):
            raise ValueError("OANDA_ACCOUNT_ID contains placeholder values")
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

def load_config():
    """Load configuration from environment variables or config.ini with validation"""
    try:
        # Debug: Print some environment variables to see what's available
        print("=== Environment Variable Debug ===")
        env_vars_to_check = [
            'DATABASE_URL', 'OANDA_ACCOUNT_ID', 'OANDA_ACCESS_TOKEN', 
            'OANDA_ENVIRONMENT', 'MAX_RISK_PERCENTAGE'
        ]
        
        for var in env_vars_to_check:
            value = os.getenv(var, 'NOT_SET')
            # Don't print sensitive values, just show if they're set
            if 'TOKEN' in var or 'URL' in var:
                print(f"{var}: {'SET' if value != 'NOT_SET' and value else 'NOT_SET'}")
            else:
                print(f"{var}: {value}")
        print("=" * 35)
        
        # Create settings instance - this will automatically load from environment
        settings = Settings()
        
        # Additional validation
        validate_critical_settings(settings)
        
        return settings
        
    except Exception as e:
        print(f"Error loading configuration: {e}")
        print("Attempting fallback to environment variables directly...")
        
        # Direct environment variable loading as fallback
        try:
            config_dict = {
                'database_url': os.getenv('DATABASE_URL', ''),
                'oanda_account_id': os.getenv('OANDA_ACCOUNT_ID', ''),
                'oanda_access_token': os.getenv('OANDA_ACCESS_TOKEN', ''),
                'oanda_environment': os.getenv('OANDA_ENVIRONMENT', 'practice'),
                'max_risk_percentage': float(os.getenv('MAX_RISK_PERCENTAGE', '2.0')),
                'max_portfolio_heat': float(os.getenv('MAX_PORTFOLIO_HEAT', '70.0')),
                'max_daily_loss': float(os.getenv('MAX_DAILY_LOSS', '10.0')),
                'db_min_connections': int(os.getenv('DB_MIN_CONNECTIONS', '5')),
                'db_max_connections': int(os.getenv('DB_MAX_CONNECTIONS', '20')),
            }
            
            settings = Settings(**config_dict)
            validate_critical_settings(settings)
            return settings
            
        except Exception as fallback_error:
            print(f"Fallback loading also failed: {fallback_error}")
            # Try config.ini as last resort
            return load_config_from_file()

def load_config_from_file():
    """Fallback configuration loading from config.ini"""
    config_file = "config.ini"
    if os.path.exists(config_file):
        try:
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
                    'max_risk_percentage': parser.getfloat('risk', 'max_risk_percentage', fallback=2.0),
                    'max_portfolio_heat': parser.getfloat('risk', 'max_portfolio_heat', fallback=70.0),
                    'max_daily_loss': parser.getfloat('risk', 'max_daily_loss', fallback=10.0),
                })
            
            return Settings(**config_dict)
        except Exception as e:
            print(f"Error loading config.ini: {e}")
            return Settings()  # Return defaults
    else:
        print("No config.ini found, using defaults")
        return Settings()

def validate_critical_settings(settings: Settings):
    """Validate critical configuration settings"""
    errors = []
    
    # Check for placeholder values
    if not settings.database_url:
        errors.append("DATABASE_URL is not set")
    elif '<' in settings.database_url and '>' in settings.database_url:
        errors.append("DATABASE_URL contains placeholder values")
    
    if not settings.oanda_account_id:
        errors.append("OANDA_ACCOUNT_ID is not set")
    elif '<' in settings.oanda_account_id:
        errors.append("OANDA_ACCOUNT_ID contains placeholder values")
    
    if not settings.oanda_access_token or str(settings.oanda_access_token) == "":
        errors.append("OANDA_ACCESS_TOKEN is not set")
    
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
        error_msg += "\n\nPlease check your environment variables in the Render dashboard."
        raise ValueError(error_msg)

def get_database_url_info(database_url: str) -> dict:
    """Parse database URL and return connection info"""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(database_url)
        
        return {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "database": parsed.path.lstrip('/').split('?')[0] if parsed.path else '',
            "username": parsed.username,
            "password": parsed.password,
            "scheme": parsed.scheme
        }
    except Exception as e:
        print(f"Error parsing DATABASE_URL: {e}")
        return {}

# Create global config instance
try:
    config = load_config()
    print("Configuration loaded successfully")
    
    # Log configuration status (without sensitive data)
    print(f"OANDA Environment: {config.oanda_environment}")
    print(f"Database configured: {'Yes' if config.database_url else 'No'}")
    print(f"OANDA Account configured: {'Yes' if config.oanda_account_id else 'No'}")
    print(f"Risk Management - Max Risk: {config.max_risk_percentage}%")
    
except Exception as e:
    print(f"CRITICAL: Failed to load configuration: {e}")
    # Create minimal config for startup
    config = Settings()
    print("Using minimal default configuration - some features may not work")

# Additional validation warnings
if config.database_url and not config.database_url.startswith('postgresql'):
    print("WARNING: DATABASE_URL should start with 'postgresql://' for PostgreSQL")

if config.oanda_environment not in ['practice', 'live']:
    print(f"WARNING: OANDA_ENVIRONMENT '{config.oanda_environment}' should be 'practice' or 'live'")
