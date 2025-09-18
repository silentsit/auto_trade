"""
INSTITUTIONAL-GRADE LOGGING AND MONITORING SYSTEM
Comprehensive logging, metrics collection, and monitoring capabilities
"""

import logging
import json
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
import traceback
import os
from pathlib import Path
import threading
import time

class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class LogCategory(Enum):
    TRADING = "trading"
    RISK = "risk"
    EXECUTION = "execution"
    SYSTEM = "system"
    PERFORMANCE = "performance"
    ERROR = "error"
    AUDIT = "audit"

@dataclass
class LogEntry:
    """Structured log entry with metadata"""
    timestamp: str
    level: str
    category: str
    component: str
    message: str
    data: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None

@dataclass
class PerformanceMetric:
    """Performance metric for monitoring"""
    name: str
    value: float
    unit: str
    timestamp: str
    tags: Dict[str, str] = None

class AdvancedLogger:
    """Institutional-grade logger with structured logging and monitoring"""
    
    def __init__(self, 
                 log_dir: str = "logs",
                 max_file_size: int = 100 * 1024 * 1024,  # 100MB
                 backup_count: int = 10,
                 enable_metrics: bool = True,
                 enable_audit: bool = True):
        """Initialize advanced logger"""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.enable_metrics = enable_metrics
        self.enable_audit = enable_audit
        
        # Initialize loggers for different categories
        self.loggers = {}
        self.metrics_buffer = []
        self.audit_buffer = []
        self._lock = asyncio.Lock()
        
        # Setup loggers
        self._setup_loggers()
        
        # Start background tasks only if event loop is running
        try:
            loop = asyncio.get_running_loop()
            self._start_background_tasks()
        except RuntimeError:
            # No event loop running, skip background tasks for now
            pass
        
    def _setup_loggers(self):
        """Setup category-specific loggers"""
        categories = [cat.value for cat in LogCategory]
        
        for category in categories:
            logger = logging.getLogger(f"trading_bot.{category}")
            logger.setLevel(logging.DEBUG)
            
            # Remove existing handlers
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            
            # File handler
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_dir / f"{category}.log",
                maxBytes=self.max_file_size,
                backupCount=self.backup_count
            )
            
            # Console handler for critical errors
            if category == "error":
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.ERROR)
                console_handler.setFormatter(
                    logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
                )
                logger.addHandler(console_handler)
            
            # JSON formatter for structured logging
            file_handler.setFormatter(JSONFormatter())
            logger.addHandler(file_handler)
            
            self.loggers[category] = logger
    
    def _start_background_tasks(self):
        """Start background tasks for metrics and audit processing"""
        if self.enable_metrics:
            asyncio.create_task(self._process_metrics())
        if self.enable_audit:
            asyncio.create_task(self._process_audit())
    
    async def log(self, 
                  level: LogLevel,
                  category: LogCategory,
                  component: str,
                  message: str,
                  data: Optional[Dict[str, Any]] = None,
                  trace_id: Optional[str] = None,
                  user_id: Optional[str] = None,
                  session_id: Optional[str] = None):
        """Log structured message"""
        log_entry = LogEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            level=level.value,
            category=category.value,
            component=component,
            message=message,
            data=data,
            trace_id=trace_id,
            user_id=user_id,
            session_id=session_id
        )
        
        # Get appropriate logger
        logger = self.loggers.get(category.value, self.loggers["system"])
        
        # Log with appropriate level
        log_level = getattr(logging, level.value)
        logger.log(log_level, json.dumps(asdict(log_entry)))
        
        # Add to audit trail if needed
        if category == LogCategory.AUDIT or level in [LogLevel.ERROR, LogLevel.CRITICAL]:
            await self._add_to_audit(log_entry)
    
    async def log_trade(self, 
                       component: str,
                       action: str,
                       symbol: str,
                       price: float,
                       size: float,
                       pnl: float = 0.0,
                       trace_id: Optional[str] = None):
        """Log trading activity"""
        await self.log(
            LogLevel.INFO,
            LogCategory.TRADING,
            component,
            f"Trade {action}: {symbol} @ {price} size={size} PnL={pnl}",
            data={
                "action": action,
                "symbol": symbol,
                "price": price,
                "size": size,
                "pnl": pnl
            },
            trace_id=trace_id
        )
    
    async def log_risk_event(self,
                            component: str,
                            event_type: str,
                            risk_level: str,
                            details: Dict[str, Any],
                            trace_id: Optional[str] = None):
        """Log risk management events"""
        await self.log(
            LogLevel.WARNING if risk_level == "high" else LogLevel.INFO,
            LogCategory.RISK,
            component,
            f"Risk event: {event_type} (level: {risk_level})",
            data={
                "event_type": event_type,
                "risk_level": risk_level,
                "details": details
            },
            trace_id=trace_id
        )
    
    async def log_performance_metric(self,
                                   component: str,
                                   metric_name: str,
                                   value: float,
                                   unit: str = "",
                                   tags: Optional[Dict[str, str]] = None):
        """Log performance metric"""
        if not self.enable_metrics:
            return
            
        metric = PerformanceMetric(
            name=metric_name,
            value=value,
            unit=unit,
            timestamp=datetime.now(timezone.utc).isoformat(),
            tags=tags or {}
        )
        
        async with self._lock:
            self.metrics_buffer.append(metric)
    
    async def log_error(self,
                       component: str,
                       error: Exception,
                       context: Optional[Dict[str, Any]] = None,
                       trace_id: Optional[str] = None):
        """Log error with full context"""
        await self.log(
            LogLevel.ERROR,
            LogCategory.ERROR,
            component,
            f"Error: {str(error)}",
            data={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": traceback.format_exc(),
                "context": context or {}
            },
            trace_id=trace_id
        )
    
    async def log_audit(self,
                       component: str,
                       action: str,
                       user_id: Optional[str] = None,
                       details: Optional[Dict[str, Any]] = None,
                       trace_id: Optional[str] = None):
        """Log audit trail entry"""
        await self.log(
            LogLevel.INFO,
            LogCategory.AUDIT,
            component,
            f"Audit: {action}",
            data={
                "action": action,
                "details": details or {}
            },
            trace_id=trace_id,
            user_id=user_id
        )
    
    async def _add_to_audit(self, log_entry: LogEntry):
        """Add entry to audit buffer"""
        async with self._lock:
            self.audit_buffer.append(log_entry)
    
    async def _process_metrics(self):
        """Process metrics buffer periodically"""
        while True:
            try:
                await asyncio.sleep(60)  # Process every minute
                
                async with self._lock:
                    if not self.metrics_buffer:
                        continue
                    
                    # Write metrics to file
                    metrics_file = self.log_dir / "metrics.jsonl"
                    with open(metrics_file, "a") as f:
                        for metric in self.metrics_buffer:
                            f.write(json.dumps(asdict(metric)) + "\n")
                    
                    # Clear buffer
                    self.metrics_buffer.clear()
                    
            except Exception as e:
                print(f"Error processing metrics: {e}")
    
    async def _process_audit(self):
        """Process audit buffer periodically"""
        while True:
            try:
                await asyncio.sleep(30)  # Process every 30 seconds
                
                async with self._lock:
                    if not self.audit_buffer:
                        continue
                    
                    # Write audit to file
                    audit_file = self.log_dir / "audit.jsonl"
                    with open(audit_file, "a") as f:
                        for entry in self.audit_buffer:
                            f.write(json.dumps(asdict(entry)) + "\n")
                    
                    # Clear buffer
                    self.audit_buffer.clear()
                    
            except Exception as e:
                print(f"Error processing audit: {e}")
    
    def get_logger(self, category: LogCategory) -> logging.Logger:
        """Get logger for specific category"""
        return self.loggers.get(category.value, self.loggers["system"])

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)

class MetricsCollector:
    """Collect and aggregate performance metrics"""
    
    def __init__(self, logger: AdvancedLogger):
        self.logger = logger
        self.metrics = {}
        self._lock = asyncio.Lock()
    
    async def increment_counter(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric"""
        async with self._lock:
            if name not in self.metrics:
                self.metrics[name] = {"type": "counter", "value": 0.0, "tags": tags or {}}
            self.metrics[name]["value"] += value
        
        await self.logger.log_performance_metric(
            "metrics_collector",
            name,
            self.metrics[name]["value"],
            "count",
            tags
        )
    
    async def set_gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Set a gauge metric"""
        async with self._lock:
            self.metrics[name] = {"type": "gauge", "value": value, "tags": tags or {}}
        
        await self.logger.log_performance_metric(
            "metrics_collector",
            name,
            value,
            "",
            tags
        )
    
    async def record_histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a histogram value"""
        await self.logger.log_performance_metric(
            "metrics_collector",
            name,
            value,
            "",
            tags
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.metrics.copy()

class SystemMonitor:
    """System monitoring and health checks"""
    
    def __init__(self, logger: AdvancedLogger, metrics: MetricsCollector):
        self.logger = logger
        self.metrics = metrics
        self.health_checks = {}
        self._start_monitoring()
    
    def _start_monitoring(self):
        """Start background monitoring"""
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self._monitor_system())
        except RuntimeError:
            # No event loop running, skip monitoring for now
            pass
    
    async def _monitor_system(self):
        """Monitor system health continuously"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Monitor memory usage
                import psutil
                memory = psutil.virtual_memory()
                await self.metrics.set_gauge("system.memory.usage_percent", memory.percent)
                await self.metrics.set_gauge("system.memory.available_mb", memory.available / 1024 / 1024)
                
                # Monitor CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                await self.metrics.set_gauge("system.cpu.usage_percent", cpu_percent)
                
                # Monitor disk usage
                disk = psutil.disk_usage('/')
                await self.metrics.set_gauge("system.disk.usage_percent", disk.percent)
                
                # Log system health
                if memory.percent > 90:
                    await self.logger.log_risk_event(
                        "system_monitor",
                        "high_memory_usage",
                        "high",
                        {"memory_percent": memory.percent}
                    )
                
                if cpu_percent > 90:
                    await self.logger.log_risk_event(
                        "system_monitor",
                        "high_cpu_usage",
                        "high",
                        {"cpu_percent": cpu_percent}
                    )
                
            except Exception as e:
                await self.logger.log_error("system_monitor", e)

# Global logger instance
advanced_logger = AdvancedLogger()
metrics_collector = MetricsCollector(advanced_logger)
system_monitor = SystemMonitor(advanced_logger, metrics_collector)

# Convenience functions
async def log_trade(component: str, action: str, symbol: str, price: float, size: float, pnl: float = 0.0):
    """Log trading activity"""
    await advanced_logger.log_trade(component, action, symbol, price, size, pnl)

async def log_risk(component: str, event_type: str, risk_level: str, details: Dict[str, Any]):
    """Log risk event"""
    await advanced_logger.log_risk_event(component, event_type, risk_level, details)

async def log_error(component: str, error: Exception, context: Optional[Dict[str, Any]] = None):
    """Log error"""
    await advanced_logger.log_error(component, error, context)

async def log_audit(component: str, action: str, user_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
    """Log audit entry"""
    await advanced_logger.log_audit(component, action, user_id, details)

def get_logger(category: LogCategory) -> logging.Logger:
    """Get logger for category"""
    return advanced_logger.get_logger(category)
