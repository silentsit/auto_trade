import os
import configparser
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic import SecretStr
from typing import Set


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
    default_risk_percentage: float = Field(
        default=10.0
    )  # Default risk percentage per trade

    # Features
    enable_broker_reconciliation: bool = Field(default=True)

    min_trade_size: int = 1000  # For FX
    max_trade_size: int = 100000000  # Or whatever is reasonable for your account
    min_sl_distance: float = 0.005  # 50 pips for FX, or whatever fits your strategy
    min_risk_percent: float = 5.0
    max_risk_percent: float = 10.0
    min_atr: float = 0.0001

    # Notification Settings
    slack_webhook_url: str = Field(default="")
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")

    atr_stop_loss_multiplier: float = 2.0  # Centralized ATR multiplier for stop loss

    # Position Sizing Mode
    position_sizing_mode: str = Field(default="risk")  # Options: "risk" or "allocation"
    allocation_includes_leverage: bool = Field(
        default=True
    )  # If True, allocation is multiplied by leverage
    allocation_percent: float = Field(
        default=10.0
    )  # % of account to allocate per trade (used if mode is 'allocation')

    # Security Settings
    webhook_secret: str = Field(
        default=""
    )  # HMAC secret - set via WEBHOOK_SECRET env var
    webhook_token: str = Field(
        default=""
    )  # Simple token - set via WEBHOOK_TOKEN env var
    jwt_secret: str = Field(default="")  # JWT secret - set via JWT_SECRET env var
    rate_limit_requests: int = Field(default=60)  # Max requests per minute
    rate_limit_window: int = Field(default=60)  # Rate limit window in seconds
    allowed_ips: str = Field(default="")  # Comma-separated allowed IPs
    require_https: bool = Field(default=True)
    webhook_secret: str = Field(default="")  # HMAC secret
    webhook_token: str = Field(default="")  # Simple token auth
    jwt_secret: str = Field(default="")  # JWT secret
    rate_limit_requests: int = Field(default=60)  # Requests per minute
    rate_limit_window: int = Field(default=60)  # Window in seconds
    allowed_ips: str = Field(default="")  # Comma-separated IPs
    require_https: bool = Field(default=True)


# Rate limiting storage (in production, use Redis)
request_history: dict = {}  # IP -> list of timestamps
blocked_ips: Set[str] = set()


# Security utilities
def verify_hmac_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify HMAC-SHA256 signature"""
    if not signature or not secret:
        return False

    try:
        # Handle different signature formats
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
        # Remove 'Bearer ' prefix if present
        if token.startswith("Bearer "):
            token = token[7:]

        payload = jwt.decode(token, secret, algorithms=["HS256"])

        # Check expiration
        if "exp" in payload and payload["exp"] < time.time():
            return None

        return payload
    except Exception:
        return None


def check_rate_limit(client_ip: str, limit: int = 60, window: int = 60) -> bool:
    """Check if IP is within rate limits"""
    current_time = time.time()

    # Clean old entries
    if client_ip in request_history:
        request_history[client_ip] = [
            timestamp
            for timestamp in request_history[client_ip]
            if current_time - timestamp < window
        ]
    else:
        request_history[client_ip] = []

    # Check rate limit
    if len(request_history[client_ip]) >= limit:
        return False

    # Add current request
    request_history[client_ip].append(current_time)
    return True


def is_ip_allowed(client_ip: str, allowed_ips: str) -> bool:
    """Check if IP is in allowed list"""
    if not allowed_ips:
        return True  # If no restriction, allow all

    allowed_list = [ip.strip() for ip in allowed_ips.split(",")]
    return client_ip in allowed_list


# Security dependency for FastAPI
async def verify_webhook_security(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_signature: Optional[str] = Header(None),
    x_tradingview_signature: Optional[str] = Header(None),
) -> dict:
    """
    Comprehensive webhook security verification
    """
    client_ip = request.client.host if request.client else "unknown"

    # 1. IP blocking check
    if client_ip in blocked_ips:
        logger.warning(f"Blocked IP attempted access: {client_ip}")
        raise HTTPException(status_code=403, detail="IP blocked")

    # 2. Rate limiting
    if not check_rate_limit(
        client_ip, config.rate_limit_requests, config.rate_limit_window
    ):
        logger.warning(f"Rate limit exceeded for IP: {client_ip}")
        # Block IP temporarily
        blocked_ips.add(client_ip)
        # Schedule unblock (in production, use proper task scheduler)
        asyncio.create_task(unblock_ip_after_delay(client_ip, 300))  # 5 minutes
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # 3. IP allowlist check
    if not is_ip_allowed(client_ip, config.allowed_ips):
        logger.warning(f"Unauthorized IP attempted access: {client_ip}")
        raise HTTPException(status_code=403, detail="IP not allowed")

    # 4. HTTPS requirement (in production)
    if config.require_https and request.url.scheme != "https":
        logger.warning(f"Non-HTTPS request from {client_ip}")
        raise HTTPException(status_code=403, detail="HTTPS required")

    # 5. Get request body for signature verification
    body = await request.body()

    # 6. Signature verification (multiple methods)
    signature_verified = False
    auth_method = "none"

    # Method A: HMAC signature (most secure)
    if x_signature or x_tradingview_signature:
        signature = x_signature or x_tradingview_signature
        if verify_hmac_signature(body, signature, config.webhook_secret):
            signature_verified = True
            auth_method = "hmac"
        else:
            logger.warning(f"Invalid HMAC signature from {client_ip}")

    # Method B: JWT token
    elif authorization and config.jwt_secret:
        token_payload = verify_jwt_token(authorization, config.jwt_secret)
        if token_payload:
            signature_verified = True
            auth_method = "jwt"
        else:
            logger.warning(f"Invalid JWT token from {client_ip}")

    # Method C: Simple token (least secure, but better than nothing)
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

    # 7. Check if any authentication method succeeded
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


# Enhanced webhook endpoint
@router.post("/tradingview", tags=["webhook"])
async def tradingview_webhook_secure(
    request: Request, auth_info: dict = Depends(verify_webhook_security)
):
    """
    Secure TradingView webhook endpoint with multiple authentication methods
    """
    try:
        client_ip = auth_info["client_ip"]
        auth_method = auth_info["auth_method"]

        logger.info(f"=== SECURE WEBHOOK RECEIVED ===")
        logger.info(f"Client IP: {client_ip}")
        logger.info(f"Auth Method: {auth_method}")

        # Get the JSON data (body already read in security verification)
        data = await request.json()
        logger.info(f"Payload keys: {list(data.keys())}")

        # Log webhook data (sanitized)
        sanitized_data = {
            k: v for k, v in data.items() if k not in ["secret", "token", "password"]
        }
        logger.info(f"Webhook data: {sanitized_data}")

        # Process the alert
        handler = get_alert_handler()
        if not handler:
            logger.error("Alert handler not available")
            raise HTTPException(status_code=503, detail="Alert handler not available")

        # Add security context to the alert data
        data["_security"] = auth_info

        result = await handler.process_alert(data)

        logger.info(f"=== WEBHOOK RESULT ===")
        logger.info(f"Result: {result}")

        return {"status": "ok", "result": result, "security": auth_method}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"=== WEBHOOK ERROR ===")
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Webhook testing endpoint (for development)
@router.post("/webhook/test", tags=["webhook"])
async def test_webhook_security():
    """Test webhook security configuration"""
    return {
        "hmac_configured": bool(config.webhook_secret),
        "jwt_configured": bool(config.jwt_secret),
        "token_configured": bool(config.webhook_token),
        "ip_restrictions": bool(config.allowed_ips),
        "rate_limit": f"{config.rate_limit_requests}/{config.rate_limit_window}s",
        "https_required": config.require_https,
    }


# Security monitoring endpoint
@router.get("/security/status", tags=["admin"])
async def get_security_status():
    """Get security monitoring status"""
    current_time = time.time()

    # Clean old rate limit data
    active_ips = {}
    for ip, timestamps in request_history.items():
        recent_requests = [
            t for t in timestamps if current_time - t < 300
        ]  # Last 5 minutes
        if recent_requests:
            active_ips[ip] = len(recent_requests)

    return {
        "blocked_ips": list(blocked_ips),
        "active_ips": active_ips,
        "total_requests_last_5min": sum(active_ips.values()),
        "rate_limit_config": f"{config.rate_limit_requests}/{config.rate_limit_window}s",
    }

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
            settings_dict["oanda_account_id"] = os.getenv(
                "OANDA_ACCOUNT_ID"
            ) or os.getenv("OANDA_ACCOUNT")

        if os.getenv("OANDA_ACCESS_TOKEN") or os.getenv("OANDA_TOKEN"):
            settings_dict["oanda_access_token"] = os.getenv(
                "OANDA_ACCESS_TOKEN"
            ) or os.getenv("OANDA_TOKEN")

        if os.getenv("OANDA_ENVIRONMENT"):
            settings_dict["oanda_environment"] = os.getenv("OANDA_ENVIRONMENT")

        # Database settings
        if os.getenv("DATABASE_URL"):
            settings_dict["database_url"] = os.getenv("DATABASE_URL")

        # Risk settings
        if os.getenv("MAX_RISK_PERCENTAGE"):
            settings_dict["max_risk_percentage"] = float(
                os.getenv("MAX_RISK_PERCENTAGE")
            )

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

            if parser.has_section("oanda"):
                config_dict.update(
                    {
                        "oanda_account_id": parser.get(
                            "oanda", "account_id", fallback=""
                        ),
                        "oanda_access_token": parser.get(
                            "oanda", "access_token", fallback=""
                        ),
                        "oanda_environment": parser.get(
                            "oanda", "environment", fallback="practice"
                        ),
                    }
                )

            if parser.has_section("database"):
                config_dict.update(
                    {
                        "database_url": parser.get("database", "url", fallback=""),
                        "db_min_connections": parser.getint(
                            "database", "min_connections", fallback=5
                        ),
                        "db_max_connections": parser.getint(
                            "database", "max_connections", fallback=20
                        ),
                    }
                )

            if parser.has_section("risk"):
                config_dict.update(
                    {
                        "max_risk_percentage": parser.getfloat(
                            "risk", "max_risk_percentage", fallback=20.0
                        ),
                        "max_portfolio_heat": parser.getfloat(
                            "risk", "max_portfolio_heat", fallback=70.0
                        ),
                        "max_daily_loss": parser.getfloat(
                            "risk", "max_daily_loss", fallback=50.0
                        ),
                    }
                )

            return Settings(**config_dict)
        else:
            print(
                f"Warning: No config found in env or config.ini. Using defaults. Error: {e}"
            )
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
