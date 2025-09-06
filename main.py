import os
import sys
import asyncio
import inspect
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Response

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")

# -----------------------------------------------------------------------------
# Optional imports & shims
# -----------------------------------------------------------------------------
def _install_shim(module_name: str, obj_name: str, obj):
    """Register a tiny shim module with the given object if import fails."""
    import types
    m = types.ModuleType(module_name)
    setattr(m, obj_name, obj)
    sys.modules[module_name] = m
    log.warning("Installed shim for missing module: %s", module_name)

# Unified analysis is optional in some deployments
try:
    # If the real package is present, great.
    from unified_analysis import UnifiedAnalysis, LorentzianDistanceClassifier, VolatilityMonitor  # type: ignore
except Exception:
    # Provide a drop-in shim exporting the names other modules import.
    import types
    shim = types.ModuleType("unified_analysis")

    class UnifiedAnalysis:  # minimal no-op
        def __init__(self, *_, **__): pass
        async def start(self): pass
        async def stop(self): pass

    class LorentzianDistanceClassifier:
        def __init__(self, *_, **__): pass
        async def start(self): pass
        async def stop(self): pass
        def predict(self, *_, **__):
            # Return a neutral/default value; adjust if your code expects something else
            return 0.0
        def score(self, *_, **__): return 0.0
        def classify(self, *_, **__): return "neutral"

    class VolatilityMonitor:
        def __init__(self, *_, **__): 
            self.latest_vol = 0.0
        async def start(self): pass
        async def stop(self): pass
        def get_current(self, *_, **__):
            return None
        def get_current_volatility(self): 
            return self.latest_vol

    shim.UnifiedAnalysis = UnifiedAnalysis
    shim.LorentzianDistanceClassifier = LorentzianDistanceClassifier
    shim.VolatilityMonitor = VolatilityMonitor

    import sys as _sys
    _sys.modules["unified_analysis"] = shim
    log.warning("Installed shim for missing module: unified_analysis (UnifiedAnalysis, LorentzianDistanceClassifier, VolatilityMonitor)")

# Risk manager is optional; provide a tiny placeholder
try:
    from risk_manager import EnhancedRiskManager  # type: ignore
except Exception:
    class EnhancedRiskManager:
        def __init__(self, *_, **__):
            self.max_risk_per_trade = 0.2
            self.max_portfolio_risk = 0.7
        async def start(self): pass
        async def stop(self): pass
    _install_shim("risk_manager", "EnhancedRiskManager", EnhancedRiskManager)

# Import the rest (these files are present in this repo)
from unified_storage import UnifiedStorage
# DatabaseConfig/StorageType shape varies across versions -> import guarded below
try:
    from unified_storage import DatabaseConfig, StorageType  # type: ignore
except Exception as e:
    # If import fails entirely, surface a clear error at startup
    log.error("❌ Failed to import DatabaseConfig/StorageType from unified_storage: %s", e)
    DatabaseConfig = None  # type: ignore
    StorageType = None     # type: ignore

from oanda_service import OandaService
from unified_exit_manager import UnifiedExitManager
from order_queue import OrderQueue
import api  # FastAPI routes
import config
try:
    from tracker import PositionTracker  # our local tracker module
except Exception:
    # Provide a tiny shim that satisfies the alert handler
    class PositionTracker:
        def __init__(self, *_, **__): pass
        async def start(self): pass
        async def stop(self): pass
    _install_shim("tracker", "PositionTracker", PositionTracker)

try:
    from alert_handler import AlertHandler
except Exception:
    # Provide a stub so the app still answers liveness checks gracefully
    class AlertHandler:
        def __init__(self, *_, **__): pass
        async def start(self): pass
        async def stop(self): pass
        async def process_alert(self, *_, **__):
            return {"status": "error", "message": "Alert handler unavailable"}
    _install_shim("alert_handler", "AlertHandler", AlertHandler)

# Health checker is optional
try:
    from health_checker import UnifiedMonitor  # type: ignore
except Exception:
    class UnifiedMonitor:
        def __init__(self, *_, **__): pass
        async def start(self): pass
        async def stop(self): pass
    _install_shim("health_checker", "UnifiedMonitor", UnifiedMonitor)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

def _storage_is_sqlite(dsn: str) -> bool:
    return dsn.strip().lower().startswith("sqlite")

def _sqlite_path_from_dsn(dsn: str) -> Optional[str]:
    # Accept "sqlite:///relative.db" or "sqlite:////abs/path.db"
    if not _storage_is_sqlite(dsn):
        return None
    path = dsn.split("sqlite:///", 1)[-1]
    return path or "auto_trade.db"

def _resolve_storage_type(kind: str):
    """
    Return a StorageType enum member if available, otherwise the lowercase string.
    kind: 'sqlite' or 'postgresql'
    """
    if StorageType is None:
        return kind.lower()
    # Common enum names
    candidates = [kind.upper()]
    if kind.lower() == "postgresql":
        candidates.append("POSTGRES")  # some repos use POSTGRES instead
    for name in candidates:
        if hasattr(StorageType, name):
            return getattr(StorageType, name)
    return kind.lower()

def _build_db_config_direct(kind: str, dsn: str):
    """
    Build DatabaseConfig using the plain constructor (Option A),
    forgiving differences in signatures across repo versions.
    Tries keyword, then positional, then no-arg with attribute assignment.
    """
    if DatabaseConfig is None:
        raise RuntimeError("DatabaseConfig class not available")

    storage_type_value = _resolve_storage_type(kind)

    # 1) keyword args
    try:
        return DatabaseConfig(storage_type=storage_type_value, connection_string=dsn)
    except TypeError:
        pass

    # 2) positional args
    try:
        return DatabaseConfig(storage_type_value, dsn)
    except TypeError:
        pass

    # 3) no-arg then set attributes
    cfg = DatabaseConfig()
    if hasattr(cfg, "storage_type"):
        setattr(cfg, "storage_type", storage_type_value)
    if hasattr(cfg, "connection_string"):
        setattr(cfg, "connection_string", dsn)
    return cfg

def _call_with_supported_kwargs(factory, **kwargs):
    """
    Safely call a factory/classmethod but only pass kwargs it actually accepts.
    Falls back to single positional argument if needed.
    """
    try:
        sig = inspect.signature(factory)
        allowed = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return factory(**allowed)
    except Exception:
        # last-ditch: try calling with a single primary arg if present
        for key in ("dsn", "path", "connection_string"):
            if key in kwargs:
                try:
                    return factory(kwargs[key])  # as positional
                except Exception:
                    pass
        raise

def _make_config_sqlite_from_env():
    """
    Prefer Option B (classmethod for_sqlite), else Option A (direct ctor).
    """
    # Build a DSN form (works for our migration helper and some UnifiedStorage impls)
    dsn = os.getenv("SQLITE_URL", "sqlite:///auto_trade.db")
    # For the classmethod we want just the file path:
    path = _sqlite_path_from_dsn(dsn) or "auto_trade.db"

    if DatabaseConfig and hasattr(DatabaseConfig, "for_sqlite"):
        try:
            return _call_with_supported_kwargs(DatabaseConfig.for_sqlite, path=path)
        except Exception as e:
            log.warning("SQLite classmethod failed (%s), falling back to direct ctor.", e)

    return _build_db_config_direct("sqlite", dsn)

def _make_config_postgres_from_env():
    """
    Prefer Option B (classmethod for_postgres), else Option A (direct ctor).
    Keeps pool tiny for Render if classmethod supports those kwargs.
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")

    if DatabaseConfig and hasattr(DatabaseConfig, "for_postgres"):
        try:
            # Pass only supported kwargs
            return _call_with_supported_kwargs(
                DatabaseConfig.for_postgres,
                dsn=dsn,
                pool_min_size=1,
                pool_max_size=3,        # keep this small on Render
                command_timeout=60,
                ssl="require",
                app_name="auto-trade-bot",
            )
        except Exception as e:
            log.warning("Postgres classmethod failed (%s), falling back to direct ctor.", e)

    return _build_db_config_direct("postgresql", dsn)

async def _maybe_migrate_sqlite(dsn: str):
    """
    Ensure critical columns exist on older SQLite files.
    Specifically fixes 'no such column: entry_time' seen in historical DBs.
    """
    if not _storage_is_sqlite(dsn):
        return

    import sqlite3
    path = _sqlite_path_from_dsn(dsn)
    if not path:
        return

    try:
        conn = sqlite3.connect(path)
        cur  = conn.cursor()

        # Check columns of positions
        cur.execute("PRAGMA table_info(positions)")
        cols = {row[1] for row in cur.fetchall()}  # (cid, name, type, ...)
        missing = []
        if "entry_time" not in cols:
            missing.append(("entry_time", "TEXT"))
        if "updated_at" not in cols:
            missing.append(("updated_at", "TEXT"))

        for col, typ in missing:
            log.warning("⚙️ Migrating SQLite: adding positions.%s %s", col, typ)
            cur.execute(f"ALTER TABLE positions ADD COLUMN {col} {typ}")

        conn.commit()
        conn.close()
        if missing:
            log.info("✅ SQLite schema migration applied (%s)", ", ".join(c for c, _ in missing))
    except Exception as e:
        log.error("❌ SQLite migration step failed (non-fatal): %s", e)

# -----------------------------------------------------------------------------
# Component container
# -----------------------------------------------------------------------------
class C:
    storage: Optional[UnifiedStorage] = None
    oanda: Optional[OandaService] = None
    risk: Optional[EnhancedRiskManager] = None
    tracker: Optional[PositionTracker] = None
    exit_mgr: Optional[UnifiedExitManager] = None
    monitor: Optional[UnifiedMonitor] = None
    alerts: Optional[AlertHandler] = None
    analysis: Optional[UnifiedAnalysis] = None
    db_backend: Optional[str] = None  # 'sqlite' or 'postgresql'
    db_dsn: Optional[str] = None

# -----------------------------------------------------------------------------
# Initialization
# -----------------------------------------------------------------------------
async def _init_storage(*, force_sqlite: bool = False) -> UnifiedStorage:
    """
    Initialize storage using:
      - SQLite by default, unless POSTGRES_ENABLED=true
      - PostgreSQL when enabled; on failure, fall back to SQLite (once).
    The function supports both DatabaseConfig Option B (classmethods) and Option A (direct ctor).
    """
    use_pg = _bool_env("POSTGRES_ENABLED", False) and not force_sqlite

    if use_pg:
        try:
            cfg = _make_config_postgres_from_env()
            # Record for diagnostics
            C.db_backend = "postgresql"
            # we don't always know which key the DSN lives under, so grab from env
            C.db_dsn = os.getenv("DATABASE_URL")
            storage = UnifiedStorage(cfg)
            await storage.connect()
            log.info("✅ Unified storage initialized (PostgreSQL)")
            return storage
        except Exception as e:
            log.error("❌ PostgreSQL unavailable (%s). Falling back to SQLite.", e)
            # fall through to SQLite

    # SQLite path
    cfg = _make_config_sqlite_from_env()
    # For diagnostics and migration, reconstruct the DSN we used to build cfg
    sqlite_dsn = os.getenv("SQLITE_URL", "sqlite:///auto_trade.db")
    C.db_backend = "sqlite"
    C.db_dsn = sqlite_dsn

    storage = UnifiedStorage(cfg)
    await storage.connect()
    await _maybe_migrate_sqlite(sqlite_dsn)
    log.info("✅ Unified storage initialized (SQLite)")
    return storage

async def initialize_components():
    """Initialize components using robust initialization system"""
    try:
        from component_initialization_fix import robust_initialize_components
        system_status = await robust_initialize_components()
        
        if not system_status["system_operational"]:
            log.error("❌ CRITICAL COMPONENTS FAILED TO INITIALIZE")
            log.error("System will run in degraded mode")
        else:
            log.info("✅ ALL CRITICAL COMPONENTS INITIALIZED SUCCESSFULLY")
            
        return system_status
        
    except Exception as e:
        log.critical(f"❌ ROBUST INITIALIZATION FAILED: {e}")
        log.error("Falling back to basic initialization...")
        
        # Fallback to basic initialization
        return await _basic_initialize_components()

async def _basic_initialize_components():
    """Basic fallback initialization"""
    log.info("🚀 BASIC COMPONENT INITIALIZATION (FALLBACK)...")

    # Storage
    try:
        C.storage = await _init_storage()
        log.info("✅ Storage initialized")
    except Exception as e:
        log.error(f"❌ Storage failed: {e}")
        C.storage = None

    # OANDA service
    try:
        C.oanda = OandaService()
        log.info("✅ OANDA service initialized")
    except Exception as e:
        log.error(f"❌ OANDA service failed: {e}")
        C.oanda = None

    # Risk manager
    try:
        C.risk = EnhancedRiskManager()
        log.info("✅ Risk manager initialized")
    except Exception as e:
        log.error(f"❌ Risk manager failed: {e}")
        C.risk = None

    # Position tracker
    try:
        if C.storage and C.oanda:
            C.tracker = PositionTracker(db_manager=C.storage, oanda_service=C.oanda)
        else:
            C.tracker = PositionTracker()
        log.info("✅ Position tracker initialized")
    except Exception as e:
        log.error(f"❌ Position tracker failed: {e}")
        C.tracker = None

    # Unified analysis (optional)
    try:
        C.analysis = UnifiedAnalysis()
        log.info("✅ Unified analysis initialized")
    except Exception as e:
        log.warning(f"⚠️ Unified analysis failed (optional): {e}")
        C.analysis = None

    # Unified exit manager
    try:
        if C.tracker and C.oanda:
            C.exit_mgr = UnifiedExitManager(
                position_tracker=C.tracker,
                oanda_service=C.oanda,
                unified_analysis=C.analysis
            )
            log.info("✅ Unified exit manager initialized")
        else:
            log.warning("⚠️ Unified exit manager skipped - missing dependencies")
            C.exit_mgr = None
    except Exception as e:
        log.warning(f"⚠️ Unified exit manager failed: {e}")
        C.exit_mgr = None

    # Alert handler - CRITICAL
    try:
        if C.oanda and C.tracker and C.risk:
            C.alerts = AlertHandler(
                oanda_service=C.oanda,
                position_tracker=C.tracker,
                risk_manager=C.risk,
                unified_analysis=C.analysis,
                order_queue=OrderQueue(),
                config=config,
                db_manager=C.storage,
                unified_exit_manager=C.exit_mgr
            )
            api.set_alert_handler(C.alerts)
            log.info("✅ Alert handler initialized and exported to API")
        else:
            log.error("❌ Alert handler failed - missing critical dependencies")
            C.alerts = None
    except Exception as e:
        log.error(f"❌ Alert handler failed: {e}")
        C.alerts = None

    # Health monitor (optional)
    try:
        C.monitor = UnifiedMonitor()
        log.info("✅ Health monitor initialized")
    except Exception as e:
        log.warning(f"⚠️ Health monitor failed (optional): {e}")
        C.monitor = None

    # Check critical components
    critical_ready = all([C.oanda, C.tracker, C.risk, C.alerts])
    
    if critical_ready:
        log.info("🎉 ALL CRITICAL COMPONENTS READY")
    else:
        log.error("❌ CRITICAL COMPONENTS MISSING - SYSTEM DEGRADED")
    
    return {
        "system_operational": critical_ready,
        "components": {
            "storage": C.storage is not None,
            "oanda_service": C.oanda is not None,
            "risk_manager": C.risk is not None,
            "position_tracker": C.tracker is not None,
            "unified_analysis": C.analysis is not None,
            "unified_exit_manager": C.exit_mgr is not None,
            "alert_handler": C.alerts is not None,
            "health_monitor": C.monitor is not None
        }
    }

async def shutdown_components():
    log.info("🛑 SHUTTING DOWN TRADING SYSTEM...")
    for name, comp, stops in [
        ("health checker", C.monitor, ("stop", "stop_weekend_monitoring")),
        ("alert handler", C.alerts, ("stop",)),
        ("position tracker", C.tracker, ("stop",)),
        ("unified exit manager", C.exit_mgr, ("stop", "stop_monitoring")),
        ("OANDA service", C.oanda, ("shutdown", "stop")),
        ("unified storage", C.storage, ("disconnect", "close")),
    ]:
        if not comp:
            continue
        for meth in stops:
            if hasattr(comp, meth):
                try:
                    res = getattr(comp, meth)()
                    if inspect.isawaitable(res):
                        await res
                    log.info("✅ Stopped %s", name)
                    break
                except Exception as e:
                    log.warning("⚠️ %s %s() failed: %s", name, meth, e)

    log.info("✅ Auto Trading Bot shut down complete")

# -----------------------------------------------------------------------------
# FastAPI app with lifespan
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await initialize_components()
        yield
    except Exception as e:
        log.critical("❌ STARTUP FAILED: %s", e)
        # Ensure API still responds with a graceful message; handler may be None
        yield
    finally:
        try:
            await shutdown_components()
        except Exception as e:
            log.error("❌ Error during shutdown: %s", e)

app = FastAPI(lifespan=lifespan)

# Attach API routes
app.include_router(api.router)

# Root route
@app.get("/")
async def root():
    status = "ready" if C.alerts else "degraded"
    backend = C.db_backend or "unknown"
    dsn = C.db_dsn or "n/a"
    return {
        "service": "Auto Trading Bot",
        "status": status,
        "storage": backend,
        "dsn": dsn,
    }

@app.head("/")
async def root_head():
    """Handle HEAD requests to silence 405 warnings"""
    return Response(status_code=200)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level=LOG_LEVEL.lower())
