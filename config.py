from fastapi import Header, Request, HTTPException, Depends, APIRouter
from fastapi.responses import JSONResponse
import os
import configparser
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic import SecretStr
from typing import Set, Optional
import asyncio
import hmac
import hashlib
import jwt
import time
import logging
from datetime import datetime, timezone

# Set up logger
logger = logging.getLogger("config")

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
    max_risk_percentage: float = Field(default=10.0)
    max_portfolio_heat: float = Field(default=70.0)
    max_daily_loss: float = Field(default=50.0)
    max_positions_per_symbol: int = Field(default=10)
    default_risk_percentage: float = Field(default=10.0)

    # Features
    enable_broker_reconciliation: bool = Field(default=True)

    min_trade_size: int = 1000  # For FX
    max_trade_size: int = 100000000
    min_sl_distance: float = 0.005
    min_risk_percent: float = 5.0
    max_risk_percent: float = 10.0
    min_atr: float = 0.0001

    # Notification Settings
    slack_webhook_url: str = Field(default="")
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")

    atr_stop_loss_multiplier: float = 2.0

    # Position Sizing Mode
    position_sizing_mode: str = Field(default="risk")
    allocation_includes_leverage: bool = Field(default=True)
    allocation_percent: float = Field(default=10.0)

    # Security Settings
    webhook_secret: str = Field(default="")
    webhook_token: str = Field(default="")
    jwt_secret: str = Field(default="")
    rate_limit_requests: int = Field(default=60)
    rate_limit_window: int = Field(default=60)
    allowed_ips: str = Field(default="")
    require_https: bool = Field(default=True)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Rate limiting storage (in production, use Redis)
request_history: dict = {}
blocked_ips: Set[str] = set()

# Security utilities
def verify_hmac_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify HMAC-SHA256 signature"""
    if not signature or not secret:
        return False

    try:
        if signature.startswith("sha256="):
            signature = signature[7:]

        expected_signature = hmac.new(
            secret.encode("utf-8"), payload, hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected_signature)
    except Exception:
        return False

def verify_jwt_token(token: str, secret: str) -> Optional[dict]:
    """Verify JWT token"""
    if not token or not secret:
        return None

    try:
        if token.startswith("Bearer "):
            token = token[7:]

        payload = jwt.decode(token, secret, algorithms=["HS256"])

        if "exp" in payload and payload["exp"] < time.time():
            return None

        return payload
    except Exception:
        return None

def check_rate_limit(client_ip: str, limit: int = 60, window: int = 60) -> bool:
    """Check if IP is within rate limits"""
    current_time = time.time()

    if client_ip in request_history:
        request_history[client_ip] = [
            timestamp
            for timestamp in request_history[client_ip]
            if current_time - timestamp < window
        ]
    else:
        request_history[client_ip] = []

    if len(request_history[client_ip]) >= limit:
        return False

    request_history[client_ip].append(current_time)
    return True

def is_ip_allowed(client_ip: str, allowed_ips: str) -> bool:
    """Check if IP is in allowed list"""
    if not allowed_ips:
        return True

    allowed_list = [ip.strip() for ip in allowed_ips.split(",")]
    return client_ip in allowed_list

async def verify_webhook_security(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_signature: Optional[str] = Header(None),
    x_tradingview_signature: Optional[str] = Header(None),
) -> dict:
    """Comprehensive webhook security verification"""
    client_ip = request.client.host if request.client else "unknown"

    if client_ip in blocked_ips:
        logger.warning(f"Blocked IP attempted access: {client_ip}")
        raise HTTPException(status_code=403, detail="IP blocked")

    if not check_rate_limit(client_ip, config.rate_limit_requests, config.rate_limit_window):
        logger.warning(f"Rate limit exceeded for IP: {client_ip}")
        blocked_ips.add(client_ip)
        asyncio.create_task(unblock_ip_after_delay(client_ip, 300))
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    if not is_ip_allowed(client_ip, config.allowed_ips):
        logger.warning(f"Unauthorized IP attempted access: {client_ip}")
        raise HTTPException(status_code=403, detail="IP not allowed")

    if config.require_https and request.url.scheme != "https":
        logger.warning(f"Non-HTTPS request from {client_ip}")
        raise HTTPException(status_code=403, detail="HTTPS required")

    body = await request.body()
    signature_verified = False
    auth_method = "none"

    if x_signature or x_tradingview_signature:
        signature = x_signature or x_tradingview_signature
        if verify_hmac_signature(body, signature, config.webhook_secret):
            signature_verified = True
            auth_method = "hmac"
        else:
            logger.warning(f"Invalid HMAC signature from {client_ip}")

    elif authorization and config.jwt_secret:
        token_payload = verify_jwt_token(authorization, config.jwt_secret)
        if token_payload:
            signature_verified = True
            auth_method = "jwt"
        else:
            logger.warning(f"Invalid JWT token from {client_ip}")

    elif authorization and config.webhook_token:
        token = (
            authorization.replace("Bearer ", "")
            if authorization.startswith("Bearer ")
            else authorization
        )
        if hmac.compare_digest(token, config.webhook_token):
            signature_verified = True
            auth_method = "token"
        else:
            logger.warning(f"Invalid token from {client_ip}")

    if not signature_verified:
        logger.error(f"Authentication failed for {client_ip}")
        raise HTTPException(status_code=401, detail="Authentication failed")

    logger.info(f"Webhook authenticated via {auth_method} from {client_ip}")

    return {
        "client_ip": client_ip,
        "auth_method": auth_method,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

async def unblock_ip_after_delay(ip: str, delay_seconds: int):
    """Unblock IP after delay"""
    await asyncio.sleep(delay_seconds)
    blocked_ips.discard(ip)
    logger.info(f"Unblocked IP: {ip}")

def load_config():
    """Load configuration from environment variables or config.ini"""
    try:
        settings_dict = {}

        if os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT"):
            settings_dict["oanda_account_id"] = os.getenv("OANDA_ACCOUNT_ID") or os.getenv("OANDA_ACCOUNT")

        if os.getenv("OANDA_ACCESS_TOKEN") or os.getenv("OANDA_TOKEN"):
            settings_dict["oanda_access_token"] = os.getenv("OANDA_ACCESS_TOKEN") or os.getenv("OANDA_TOKEN")

        if os.getenv("OANDA_ENVIRONMENT"):
            settings_dict["oanda_environment"] = os.getenv("OANDA_ENVIRONMENT")

        if os.getenv("DATABASE_URL"):
            settings_dict["database_url"] = os.getenv("DATABASE_URL")

        if os.getenv("MAX_RISK_PERCENTAGE"):
            settings_dict["max_risk_percentage"] = float(os.getenv("MAX_RISK_PERCENTAGE"))

        if os.getenv("MAX_PORTFOLIO_HEAT"):
            settings_dict["max_portfolio_heat"] = float(os.getenv("MAX_PORTFOLIO_HEAT"))

        if os.getenv("MAX_DAILY_LOSS"):
            settings_dict["max_daily_loss"] = float(os.getenv("MAX_DAILY_LOSS"))

        if settings_dict:
            return Settings(**settings_dict)
        else:
            return Settings()

    except Exception as e:
        config_file = "config.ini"
        if os.path.exists(config_file):
            parser = configparser.ConfigParser()
            parser.read(config_file)

            config_dict = {}

            if parser.has_section("oanda"):
                config_dict.update({
                    "oanda_account_id": parser.get("oanda", "account_id", fallback=""),
                    "oanda_access_token": parser.get("oanda", "access_token", fallback=""),
                    "oanda_environment": parser.get("oanda", "environment", fallback="practice"),
                })

            if parser.has_section("database"):
                config_dict.update({
                    "database_url": parser.get("database", "url", fallback=""),
                    "db_min_connections": parser.getint("database", "min_connections", fallback=5),
                    "db_max_connections": parser.getint("database", "max_connections", fallback=20),
                })

            if parser.has_section("risk"):
                config_dict.update({
                    "max_risk_percentage": parser.getfloat("risk", "max_risk_percentage", fallback=20.0),
                    "max_portfolio_heat": parser.getfloat("risk", "max_portfolio_heat", fallback=70.0),
                    "max_daily_loss": parser.getfloat("risk", "max_daily_loss", fallback=50.0),
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
