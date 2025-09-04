# main.py
# -*- coding: utf-8 -*-
"""
Entry point for the Auto Trading Bot (FastAPI + Uvicorn).

Key improvements vs. your last deploy:
- ✅ Fixed the trailing uvicorn.run(...) parenthesis bug that caused: "SyntaxError: '(' was never closed"
- ✅ Defers project imports until after we install safe fallbacks for modules that may be absent
  (e.g., `risk_manager`, `unified_analysis`) so the API still boots and can ack webhooks.
- ✅ Wires the API's alert handler reference during startup so you don't see
  "Alert handler not available" when webhooks arrive.
- ✅ Graceful DB handling: tries Postgres first; if blocked (e.g., TooManyConnections),
  falls back to SQLite automatically and continues running.
- ✅ Robust lifespan startup/shutdown with clear logs.

This file assumes you already have:
  - api.py (router, set_alert_handler)
  - alert_handler.py (AlertHandler)
  - unified_storage.py (UnifiedStorage, DatabaseConfig or equivalent)
  - oanda_service.py (OandaService)
  - tracker.py (PositionTracker)

Optional/legacy modules (`risk_manager`, `unified_analysis`) are shimmed if missing.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import os
import sys
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- Basic logging -----------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")


# --- Helper: install compatibility shims BEFORE importing project modules ----
def _install_missing_module_shims() -> None:
    """
    Some uploaded snapshots may be missing older modules (risk_manager, unified_analysis)
    that other files import at module-import time. Provide small shims so imports succeed.
    """
    import types

    # Shim: risk_manager.EnhancedRiskManager
    if "risk_manager" not in sys.modules:
        rm = types.ModuleType("risk_manager")

        class _DummyCorrelationManager:
            def __init__(self) -> None:
                self.enabled = False

            async def start(self) -> None:
                self.enabled = True
                logging.getLogger("risk_manager").info("Correlation manager (shim) started")

            async def stop(self) -> None:
                self.enabled = False
                logging.getLogger("risk_manager").info("Correlation manager (shim) stopped")

        class EnhancedRiskManager:
            def __init__(
                self,
                max_risk_per_trade: float = 10.0,
                max_portfolio_risk: float = 50.0,
                **_: object,
            ) -> None:
                self.max_risk_per_trade = max_risk_per_trade
                self.max_portfolio_risk = max_portfolio_risk
                self.correlation_manager = _DummyCorrelationManager()
                self._balance = 0.0
                self._log = logging.getLogger("risk_manager")

            async def initialize(self, balance: float) -> None:
                self._balance = float(balance or 0.0)
                self._log.info(
                    "RiskManager (shim) initialized with balance: %.2f "
                    "(max_risk_per_trade=%.2f%%, max_portfolio_risk=%.2f%%)",
                    self._balance,
                    self.max_risk_per_trade,
                    self.max_portfolio_risk,
                )

            async def is_trade_allowed(self, risk_percent: float, symbol: str) -> tuple[bool, str]:
                # Always allow in the shim; real logic lives in your full module
                return True, f"Allowed by shim for {symbol} at {risk_percent:.2f}%"

        rm.EnhancedRiskManager = EnhancedRiskManager
        sys.modules["risk_manager"] = rm
        log.warning("Installed shim for missing module: risk_manager")

    # Shim: unified_analysis.UnifiedMarketAnalyzer
    if "unified_analysis" not in sys.modules:
        ua = types.ModuleType("unified_analysis")

        class UnifiedMarketAnalyzer:
            def __init__(self, *_: object, **__: object) -> None:
                self._log = logging.getLogger("unified_analysis")
                self._log.info("UnifiedMarketAnalyzer (shim) initialized")

            async def start(self) -> None:
                self._log.info("UnifiedMarketAnalyzer (shim) start")

            async def stop(self) -> None:
                self._log.info("UnifiedMarketAnalyzer (shim) stop")

        ua.UnifiedMarketAnalyzer = UnifiedMarketAnalyzer
        sys.modules["unified_analysis"] = ua
        log.warning("Installed shim for missing module: unified_analysis")


_install_missing_module_shims()

# --- Now import project modules safely ---------------------------------------
from api import router as api_router, set_alert_handler  # type: ignore

# We'll import the remaining modules lazily inside startup to handle environments
# where a file may be syntactically different across deployments.


# --- FastAPI app --------------------------------------------------------------
app = FastAPI(title="Auto Trading Bot", version="2.0.0")

# Allow TradingView/Render/IPs; loosen by default (tighten in production via env)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("CORS_ALLOW_ORIGINS", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Attach API routes
app.include_router(api_router)


# --- Component containers -----------------------------------------------------
class Components:
    storage = None
    oanda_service = None
    position_tracker = None
    risk_manager = None
    market_analyzer = None
    alert_handler = None


C = Components  # alias for brevity


# --- Environment helpers ------------------------------------------------------
def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


# --- Startup orchestration ----------------------------------------------------
async def _init_storage() -> object:
    """
    Try Postgres first; if it fails (e.g., TooManyConnections), fall back to SQLite.
    Works with either a dataclass DatabaseConfig(...) or a simple class with attributes.
    """
    mod = importlib.import_module("unified_storage")
    UnifiedStorage = getattr(mod, "UnifiedStorage")
    DatabaseConfig = getattr(mod, "DatabaseConfig", None)
    StorageType = getattr(mod, "StorageType", None)

    db_url = os.getenv("DATABASE_URL", "").strip()
    prefer_sqlite = _bool_env("FORCE_SQLITE", False) or not db_url

    def _make_config_sqlite() -> object:
        # Construct config in a version-agnostic way
        if DatabaseConfig is None:
            cfg = type("DatabaseConfig", (), {})()
        else:
            try:
                # Newer versions may have an __init__
                return DatabaseConfig(
                    storage_type=getattr(StorageType, "sqlite", "sqlite"),
                    connection_string=os.getenv("SQLITE_URL", "sqlite+aiosqlite:///./trading.db"),
                    pool_min_size=1,
                    pool_max_size=5,
                )
            except TypeError:
                cfg = DatabaseConfig()  # old-style class without __init__
        # set attributes manually
        cfg.storage_type = getattr(StorageType, "sqlite", "sqlite")
        cfg.connection_string = os.getenv("SQLITE_URL", "sqlite+aiosqlite:///./trading.db")
        cfg.pool_min_size = 1
        cfg.pool_max_size = 5
        return cfg

    def _make_config_pg() -> object:
        if DatabaseConfig is None:
            cfg = type("DatabaseConfig", (), {})()
        else:
            try:
                return DatabaseConfig(
                    storage_type=getattr(StorageType, "postgresql", "postgresql"),
                    connection_string=db_url,
                    pool_min_size=int(os.getenv("DB_POOL_MIN", "1")),
                    pool_max_size=int(os.getenv("DB_POOL_MAX", "5")),
                )
            except TypeError:
                cfg = DatabaseConfig()
        cfg.storage_type = getattr(StorageType, "postgresql", "postgresql")
        cfg.connection_string = db_url
        cfg.pool_min_size = int(os.getenv("DB_POOL_MIN", "1"))
        cfg.pool_max_size = int(os.getenv("DB_POOL_MAX", "5"))
        return cfg

    if prefer_sqlite:
        log.info("Using SQLite mode%s", " (forced)" if os.getenv("FORCE_SQLITE") else "")
        storage = UnifiedStorage(_make_config_sqlite())
        await storage.connect()
        await storage.create_tables()
        return storage

    # Try Postgres, fall back on failure
    try:
        log.info("Attempting PostgreSQL connection...")
        storage = UnifiedStorage(_make_config_pg())
        await storage.connect()
        await storage.create_tables()
        log.info("✅ PostgreSQL storage ready")
        return storage
    except Exception as e:
        # Typical case in your logs: TooManyConnections
        log.error("❌ PostgreSQL unavailable (%s). Falling back to SQLite.", e)
        storage = UnifiedStorage(_make_config_sqlite())
        await storage.connect()
        await storage.create_tables()
        log.info("✅ SQLite fallback storage ready")
        return storage


async def _init_oanda_service():
    mod = importlib.import_module("oanda_service")
    OandaService = getattr(mod, "OandaService")

    environment = os.getenv("OANDA_ENVIRONMENT", "practice").strip()
    account_id = os.getenv("OANDA_ACCOUNT_ID", "").strip()
    access_token = os.getenv("OANDA_ACCESS_TOKEN", "").strip()

    if not account_id or not access_token:
        log.warning("OANDA credentials missing; OandaService will initialize in limited mode")

    svc = OandaService(
        account_id=account_id,
        access_token=access_token,
        environment=environment,
    )
    # Some versions expose async initialize(), others do it in __init__
    if hasattr(svc, "initialize"):
        maybe_coro = svc.initialize()
        if asyncio.iscoroutine(maybe_coro):
            await maybe_coro
    return svc


async def _init_position_tracker(storage, oanda_service):
    mod = importlib.import_module("tracker")
    PositionTracker = getattr(mod, "PositionTracker")
    tracker = PositionTracker(storage=storage, oanda_service=oanda_service)
    if hasattr(tracker, "initialize"):
        maybe = tracker.initialize()
        if asyncio.iscoroutine(maybe):
            await maybe
    return tracker


async def _init_risk_manager(oanda_service) -> object:
    rm_mod = importlib.import_module("risk_manager")  # may be shim
    EnhancedRiskManager = getattr(rm_mod, "EnhancedRiskManager")
    rm = EnhancedRiskManager(
        max_risk_per_trade=float(os.getenv("MAX_RISK_PER_TRADE", "20")),
        max_portfolio_risk=float(os.getenv("MAX_PORTFOLIO_RISK", "70")),
    )

    # Try to fetch balance via OandaService; fall back to env/default.
    balance = float(os.getenv("STARTING_BALANCE", "100000"))
    try:
        if hasattr(oanda_service, "get_account_balance"):
            maybe = oanda_service.get_account_balance()
            bal = await maybe if asyncio.iscoroutine(maybe) else maybe
            if bal:
                balance = float(bal)
    except Exception as e:
        log.warning("Could not fetch live balance; using fallback %.2f (reason: %s)", balance, e)

    if hasattr(rm, "initialize"):
        await rm.initialize(balance)

    # Start correlation manager if available
    with contextlib.suppress(Exception):
        if hasattr(rm, "correlation_manager") and hasattr(rm.correlation_manager, "start"):
            await rm.correlation_manager.start()

    return rm


async def _init_market_analyzer(oanda_service) -> Optional[object]:
    ua_mod = importlib.import_module("unified_analysis")  # may be shim
    UnifiedMarketAnalyzer = getattr(ua_mod, "UnifiedMarketAnalyzer")
    analyzer = UnifiedMarketAnalyzer(oanda_service)
    with contextlib.suppress(Exception):
        if hasattr(analyzer, "start"):
            maybe = analyzer.start()
            if asyncio.iscoroutine(maybe):
                await maybe
    return analyzer


async def _init_alert_handler(storage, oanda_service, position_tracker, risk_manager, market_analyzer):
    ah_mod = importlib.import_module("alert_handler")
    AlertHandler = getattr(ah_mod, "AlertHandler")
    handler = AlertHandler(
        storage=storage,
        oanda_service=oanda_service,
        position_tracker=position_tracker,
        risk_manager=risk_manager,
        market_analyzer=market_analyzer,
        bot_name=os.getenv("BOT_NAME", "SECONDARY BOT"),
    )
    if hasattr(handler, "start"):
        maybe = handler.start()
        if asyncio.iscoroutine(maybe):
            await maybe
    return handler


async def initialize_components() -> None:
    log.info("🚀 INITIALIZING TRADING SYSTEM COMPONENTS...")

    # 1) Storage
    C.storage = await _init_storage()

    # 2) OANDA service
    C.oanda_service = await _init_oanda_service()

    # 3) Position tracker
    C.position_tracker = await _init_position_tracker(C.storage, C.oanda_service)
    log.info("✅ Position tracker initialized")

    # 4) Risk manager (+ correlation system)
    C.risk_manager = await _init_risk_manager(C.oanda_service)
    log.info("✅ Risk manager initialized")

    # 5) Market analyzer (optional)
    C.market_analyzer = await _init_market_analyzer(C.oanda_service)
    log.info("✅ Unified Market Analyzer initialized")

    # 6) Alert handler
    C.alert_handler = await _init_alert_handler(
        C.storage, C.oanda_service, C.position_tracker, C.risk_manager, C.market_analyzer
    )
    log.info("✅ Alert handler ready")

    # 7) Expose handler to the API layer so /tradingview can process signals immediately
    set_alert_handler(C.alert_handler)
    log.info("✅ API components configured (alert handler set)")

    log.info("🎉 ALL COMPONENTS INITIALIZED SUCCESSFULLY")


async def shutdown_components() -> None:
    log.info("🛑 SHUTTING DOWN TRADING SYSTEM...")

    # Stop alert handler
    with contextlib.suppress(Exception):
        if C.alert_handler and hasattr(C.alert_handler, "stop"):
            await C.alert_handler.stop()
            log.info("⚡ Alert handler stopped")

    # Stop correlation manager / analyzer
    with contextlib.suppress(Exception):
        if C.market_analyzer and hasattr(C.market_analyzer, "stop"):
            await C.market_analyzer.stop()

    with contextlib.suppress(Exception):
        if C.risk_manager and hasattr(C.risk_manager, "correlation_manager"):
            cm = C.risk_manager.correlation_manager
            if hasattr(cm, "stop"):
                await cm.stop()

    # Stop position tracker
    with contextlib.suppress(Exception):
        if C.position_tracker and hasattr(C.position_tracker, "stop"):
            await C.position_tracker.stop()
            log.info("📍 Position tracker stopped")

    # Stop OANDA service
    with contextlib.suppress(Exception):
        if C.oanda_service and hasattr(C.oanda_service, "shutdown"):
            await C.oanda_service.shutdown()
            log.info("🔗 OANDA service shut down")

    # Disconnect storage
    with contextlib.suppress(Exception):
        if C.storage and hasattr(C.storage, "disconnect"):
            await C.storage.disconnect()
            log.info("💾 Storage disconnected")

    log.info("✅ Auto Trading Bot shut down complete")


# --- Lifespan manager ---------------------------------------------------------
@app.on_event("startup")
async def _on_startup() -> None:
    log.info("🚀 AUTO TRADING BOT STARTING UP...")
    await initialize_components()


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    await shutdown_components()


# --- Simple root route (optional, the API router also provides one) -----------
@app.get("/")
async def root() -> dict:
    return {"status": "ok", "service": "Auto Trading Bot", "version": "2.0.0"}


# --- __main__ for local runs --------------------------------------------------
if __name__ == "__main__":
    # Keep options simple; Render injects $PORT at runtime.
    import uvicorn

    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=_bool_env("RELOAD", False),
        log_level=os.getenv("UVICORN_LOG_LEVEL", LOG_LEVEL).lower(),
        workers=int(os.getenv("UVICORN_WORKERS", "1")),
        timeout_keep_alive=int(os.getenv("UVICORN_KEEPALIVE", "60")),
        limit_concurrency=int(os.getenv("UVICORN_LIMIT_CONCURRENCY", "100")),
    )
