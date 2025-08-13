"""
Unified Monitoring Service
Consolidates all monitoring functionality into a single, efficient service:
- Health monitoring
- Connection monitoring  
- System monitoring
- Volatility monitoring
"""

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class MonitorType(Enum):
    """Types of monitoring services"""
    HEALTH = "health"
    CONNECTION = "connection"
    SYSTEM = "system"
    VOLATILITY = "volatility"

@dataclass
class MonitorConfig:
    """Configuration for monitoring services"""
    health_check_interval: int = 300  # 5 minutes
    connection_check_interval: int = 60  # 1 minute
    system_check_interval: int = 600  # 10 minutes
    volatility_check_interval: int = 900  # 15 minutes
    max_retries: int = 3
    retry_delay: int = 30

@dataclass
class HealthStatus:
    """Health status information"""
    timestamp: datetime
    status: str
    details: Dict[str, Any]
    last_check: datetime
    check_count: int
    error_count: int

@dataclass
class ConnectionStatus:
    """Connection status information"""
    timestamp: datetime
    oanda_connected: bool
    database_connected: bool
    last_oanda_check: datetime
    last_database_check: datetime
    oanda_latency_ms: Optional[float]
    database_latency_ms: Optional[float]

@dataclass
class SystemStatus:
    """System resource status"""
    timestamp: datetime
    cpu_usage: Optional[float]
    memory_usage: Optional[float]
    disk_usage: Optional[float]
    active_connections: int
    uptime_hours: float

@dataclass
class VolatilityState:
    """Volatility state information"""
    symbol: str
    timestamp: datetime
    atr_value: float
    volatility_ratio: float
    volatility_state: str
    position_size_modifier: float
    last_update: datetime

class UnifiedMonitor:
    """
    Unified monitoring service that consolidates all monitoring functionality
    """
    
    def __init__(self, 
                 oanda_service=None,
                 db_manager=None,
                 alert_handler=None,
                 config: MonitorConfig = None):
        
        self.oanda_service = oanda_service
        self.db_manager = db_manager
        self.alert_handler = alert_handler
        self.config = config or MonitorConfig()
        
        # Monitoring state
        self.monitoring = False
        self._monitor_tasks = {}
        self._lock = asyncio.Lock()
        
        # Status storage
        self.health_status = HealthStatus(
            timestamp=datetime.now(timezone.utc),
            status="unknown",
            details={},
            last_check=datetime.now(timezone.utc),
            check_count=0,
            error_count=0
        )
        
        self.connection_status = ConnectionStatus(
            timestamp=datetime.now(timezone.utc),
            oanda_connected=False,
            database_connected=False,
            last_oanda_check=datetime.now(timezone.utc),
            last_database_check=datetime.now(timezone.utc),
            oanda_latency_ms=None,
            database_latency_ms=None
        )
        
        self.system_status = SystemStatus(
            timestamp=datetime.now(timezone.utc),
            cpu_usage=None,
            memory_usage=None,
            disk_usage=None,
            active_connections=0,
            uptime_hours=0.0
        )
        
        self.volatility_states: Dict[str, VolatilityState] = {}
        self.start_time = datetime.now(timezone.utc)
        
        logger.info("🔍 Unified Monitor initialized")
    
    # ============================================================================
    # === HEALTH MONITORING ===
    # ============================================================================
    
    async def check_health(self) -> HealthStatus:
        """Comprehensive health check of all system components"""
        try:
            start_time = time.time()
            details = {}
            
            # Check OANDA service
            if self.oanda_service:
                try:
                    oanda_healthy = await self.oanda_service.is_healthy()
                    details['oanda_service'] = {
                        'status': 'healthy' if oanda_healthy else 'unhealthy',
                        'balance': await self.oanda_service.get_account_balance() if oanda_healthy else None
                    }
                except Exception as e:
                    details['oanda_service'] = {'status': 'error', 'error': str(e)}
            else:
                details['oanda_service'] = {'status': 'not_available'}
            
            # Check database
            if self.db_manager:
                try:
                    db_healthy = await self.db_manager.is_healthy()
                    details['database'] = {
                        'status': 'healthy' if db_healthy else 'unhealthy',
                        'connection_pool_size': getattr(self.db_manager, 'pool_size', 'unknown')
                    }
                except Exception as e:
                    details['database'] = {'status': 'error', 'error': str(e)}
            else:
                details['database'] = {'status': 'not_available'}
            
            # Check alert handler
            if self.alert_handler:
                try:
                    alert_healthy = getattr(self.alert_handler, '_started', False)
                    details['alert_handler'] = {
                        'status': 'healthy' if alert_healthy else 'unhealthy',
                        'started': alert_healthy
                    }
                except Exception as e:
                    details['alert_handler'] = {'status': 'error', 'error': str(e)}
            else:
                details['alert_handler'] = {'status': 'not_available'}
            
            # Determine overall health
            overall_healthy = all(
                detail.get('status') == 'healthy' 
                for detail in details.values() 
                if detail.get('status') != 'not_available'
            )
            
            # Update health status
            async with self._lock:
                self.health_status = HealthStatus(
                    timestamp=datetime.now(timezone.utc),
                    status="healthy" if overall_healthy else "unhealthy",
                    details=details,
                    last_check=datetime.now(timezone.utc),
                    check_count=self.health_status.check_count + 1,
                    error_count=self.health_status.error_count + (0 if overall_healthy else 1)
                )
            
            check_duration = (time.time() - start_time) * 1000
            logger.info(f"🏥 Health check completed in {check_duration:.1f}ms - Status: {self.health_status.status}")
            
            return self.health_status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            async with self._lock:
                self.health_status.error_count += 1
                self.health_status.status = "error"
                self.health_status.details = {"error": str(e)}
            return self.health_status
    
    # ============================================================================
    # === CONNECTION MONITORING ===
    # ============================================================================
    
    async def check_connections(self) -> ConnectionStatus:
        """Monitor OANDA and database connections"""
        try:
            start_time = time.time()
            
            # Check OANDA connection
            oanda_connected = False
            oanda_latency = None
            if self.oanda_service:
                try:
                    oanda_start = time.time()
                    # Simple API call to test connection
                    await self.oanda_service.get_account_balance()
                    oanda_connected = True
                    oanda_latency = (time.time() - oanda_start) * 1000
                except Exception as e:
                    logger.warning(f"OANDA connection check failed: {e}")
                    oanda_connected = False
            
            # Check database connection
            db_connected = False
            db_latency = None
            if self.db_manager:
                try:
                    db_start = time.time()
                    await self.db_manager.is_healthy()
                    db_connected = True
                    db_latency = (time.time() - db_start) * 1000
                except Exception as e:
                    logger.warning(f"Database connection check failed: {e}")
                    db_connected = False
            
            # Update connection status
            async with self._lock:
                self.connection_status = ConnectionStatus(
                    timestamp=datetime.now(timezone.utc),
                    oanda_connected=oanda_connected,
                    database_connected=db_connected,
                    last_oanda_check=datetime.now(timezone.utc) if oanda_connected else self.connection_status.last_oanda_check,
                    last_database_check=datetime.now(timezone.utc) if db_connected else self.connection_status.last_database_check,
                    oanda_latency_ms=oanda_latency,
                    database_latency_ms=db_latency
                )
            
            check_duration = (time.time() - start_time) * 1000
            logger.info(f"🔗 Connection check completed in {check_duration:.1f}ms - OANDA: {oanda_connected}, DB: {db_connected}")
            
            return self.connection_status
            
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return self.connection_status
    
    # ============================================================================
    # === SYSTEM MONITORING ===
    # ============================================================================
    
    async def check_system_resources(self) -> SystemStatus:
        """Monitor system resource usage"""
        try:
            start_time = time.time()
            
            # Calculate uptime
            uptime = datetime.now(timezone.utc) - self.start_time
            uptime_hours = uptime.total_seconds() / 3600
            
            # Try to get system metrics (psutil not always available)
            cpu_usage = None
            memory_usage = None
            disk_usage = None
            
            try:
                import psutil
                cpu_usage = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                memory_usage = memory.percent
                disk = psutil.disk_usage('/')
                disk_usage = disk.percent
            except ImportError:
                logger.debug("psutil not available - skipping system metrics")
            
            # Count active connections (estimate)
            active_connections = 0
            if self.db_manager:
                try:
                    # Try to get connection pool info
                    pool_size = getattr(self.db_manager, 'pool_size', 0)
                    active_connections = min(pool_size, 10)  # Estimate
                except:
                    pass
            
            # Update system status
            async with self._lock:
                self.system_status = SystemStatus(
                    timestamp=datetime.now(timezone.utc),
                    cpu_usage=cpu_usage,
                    memory_usage=memory_usage,
                    disk_usage=disk_usage,
                    active_connections=active_connections,
                    uptime_hours=uptime_hours
                )
            
            check_duration = (time.time() - start_time) * 1000
            logger.info(f"💻 System check completed in {check_duration:.1f}ms - Uptime: {uptime_hours:.1f}h")
            
            return self.system_status
            
        except Exception as e:
            logger.error(f"System check failed: {e}")
            return self.system_status
    
    # ============================================================================
    # === VOLATILITY MONITORING ===
    # ============================================================================
    
    async def get_volatility_state(self, symbol: str) -> Optional[VolatilityState]:
        """Get current volatility state for a symbol"""
        try:
            if not self.oanda_service:
                return None
            
            # Get current ATR value
            from utils import get_atr
            atr_value = await get_atr(symbol, "15")  # Default to 15M timeframe
            if not atr_value:
                return None
            
            # Calculate volatility ratio (simplified)
            # In a real implementation, you'd compare to historical volatility
            volatility_ratio = 1.0  # Default neutral
            
            # Determine volatility state
            if volatility_ratio > 1.5:
                volatility_state = "high"
            elif volatility_ratio < 0.5:
                volatility_state = "low"
            else:
                volatility_state = "normal"
            
            # Calculate position size modifier
            if volatility_state == "high":
                position_size_modifier = 0.7  # Reduce position size
            elif volatility_state == "low":
                position_size_modifier = 1.2  # Increase position size
            else:
                position_size_modifier = 1.0  # Normal position size
            
            # Create volatility state
            vol_state = VolatilityState(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                atr_value=atr_value,
                volatility_ratio=volatility_ratio,
                volatility_state=volatility_state,
                position_size_modifier=position_size_modifier,
                last_update=datetime.now(timezone.utc)
            )
            
            # Store in cache
            async with self._lock:
                self.volatility_states[symbol] = vol_state
            
            return vol_state
            
        except Exception as e:
            logger.error(f"Failed to get volatility state for {symbol}: {e}")
            return None
    
    async def get_all_volatility_states(self) -> Dict[str, VolatilityState]:
        """Get volatility states for all monitored symbols"""
        try:
            # Get list of symbols to monitor
            symbols = ["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF"]  # Default major pairs
            
            # Update volatility states for all symbols
            for symbol in symbols:
                await self.get_volatility_state(symbol)
            
            async with self._lock:
                return self.volatility_states.copy()
                
        except Exception as e:
            logger.error(f"Failed to get all volatility states: {e}")
            return {}
    
    async def get_position_size_modifier(self, symbol: str) -> float:
        """Get position size modifier based on volatility"""
        try:
            vol_state = await self.get_volatility_state(symbol)
            if vol_state:
                return vol_state.position_size_modifier
            return 1.0  # Default neutral
        except Exception as e:
            logger.error(f"Failed to get position size modifier for {symbol}: {e}")
            return 1.0
    
    # ============================================================================
    # === MONITORING CONTROL ===
    # ============================================================================
    
    async def start_monitoring(self):
        """Start all monitoring services"""
        if self.monitoring:
            logger.warning("Unified monitor is already running")
            return
        
        self.monitoring = True
        logger.info("🔍 Starting unified monitoring service...")
        
        # Start health monitoring
        self._monitor_tasks[MonitorType.HEALTH] = asyncio.create_task(
            self._health_monitor_loop()
        )
        
        # Start connection monitoring
        self._monitor_tasks[MonitorType.CONNECTION] = asyncio.create_task(
            self._connection_monitor_loop()
        )
        
        # Start system monitoring
        self._monitor_tasks[MonitorType.SYSTEM] = asyncio.create_task(
            self._system_monitor_loop()
        )
        
        # Start volatility monitoring
        self._monitor_tasks[MonitorType.VOLATILITY] = asyncio.create_task(
            self._volatility_monitor_loop()
        )
        
        logger.info("✅ Unified monitoring service started")
    
    async def stop_monitoring(self):
        """Stop all monitoring services"""
        if not self.monitoring:
            return
        
        self.monitoring = False
        logger.info("🛑 Stopping unified monitoring service...")
        
        # Cancel all monitoring tasks
        for task in self._monitor_tasks.values():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._monitor_tasks.clear()
        logger.info("✅ Unified monitoring service stopped")
    
    # ============================================================================
    # === MONITORING LOOPS ===
    # ============================================================================
    
    async def _health_monitor_loop(self):
        """Health monitoring loop"""
        while self.monitoring:
            try:
                await self.check_health()
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitoring loop error: {e}")
                await asyncio.sleep(self.config.retry_delay)
    
    async def _connection_monitor_loop(self):
        """Connection monitoring loop"""
        while self.monitoring:
            try:
                await self.check_connections()
                await asyncio.sleep(self.config.connection_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Connection monitoring loop error: {e}")
                await asyncio.sleep(self.config.retry_delay)
    
    async def _system_monitor_loop(self):
        """System monitoring loop"""
        while self.monitoring:
            try:
                await self.check_system_resources()
                await asyncio.sleep(self.config.system_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"System monitoring loop error: {e}")
                await asyncio.sleep(self.config.retry_delay)
    
    async def _volatility_monitor_loop(self):
        """Volatility monitoring loop"""
        while self.monitoring:
            try:
                await self.get_all_volatility_states()
                await asyncio.sleep(self.config.volatility_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Volatility monitoring loop error: {e}")
                await asyncio.sleep(self.config.retry_delay)
    
    # ============================================================================
    # === STATUS REPORTING ===
    # ============================================================================
    
    async def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all monitoring services"""
        try:
            return {
                "health": {
                    "status": self.health_status.status,
                    "last_check": self.health_status.last_check.isoformat(),
                    "check_count": self.health_status.check_count,
                    "error_count": self.health_status.error_count,
                    "details": self.health_status.details
                },
                "connections": {
                    "oanda_connected": self.connection_status.oanda_connected,
                    "database_connected": self.connection_status.database_connected,
                    "oanda_latency_ms": self.connection_status.oanda_latency_ms,
                    "database_latency_ms": self.connection_status.database_latency_ms,
                    "last_oanda_check": self.connection_status.last_oanda_check.isoformat(),
                    "last_database_check": self.connection_status.last_database_check.isoformat()
                },
                "system": {
                    "uptime_hours": self.system_status.uptime_hours,
                    "cpu_usage": self.system_status.cpu_usage,
                    "memory_usage": self.system_status.memory_usage,
                    "disk_usage": self.system_status.disk_usage,
                    "active_connections": self.system_status.active_connections
                },
                "volatility": {
                    "monitored_symbols": list(self.volatility_states.keys()),
                    "last_update": datetime.now(timezone.utc).isoformat()
                },
                "monitoring": {
                    "status": "running" if self.monitoring else "stopped",
                    "start_time": self.start_time.isoformat(),
                    "active_tasks": len(self._monitor_tasks)
                }
            }
        except Exception as e:
            logger.error(f"Failed to get comprehensive status: {e}")
            return {"error": str(e)}

# Factory function for creating unified monitor
def create_unified_monitor(oanda_service=None, db_manager=None, alert_handler=None, config=None):
    """Create and return a unified monitor instance"""
    return UnifiedMonitor(
        oanda_service=oanda_service,
        db_manager=db_manager,
        alert_handler=alert_handler,
        config=config
    )
