import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from utils import logger
from config import config

class SystemMonitor:
    """
    Monitors system health, component status, and performance metrics.
    """
    def __init__(self):
        """Initialize system monitor"""
        self.component_status = {}  # component_name -> status
        self.performance_metrics = {}  # metric_name -> value
        self.error_counts = {}  # component_name -> error count
        self.last_update = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()
        self.task_manager = None  # Add task manager reference

    async def register_component(self, component_name: str, initial_status: str = "initializing") -> bool:
        """Register a component for monitoring"""
        async with self._lock:
            self.component_status[component_name] = {
                "status": initial_status,
                "last_update": datetime.now(timezone.utc).isoformat(),
                "message": "",
                "error_count": 0
            }
            self.error_counts[component_name] = 0
            logger.info(f"Registered component {component_name} for monitoring")
            return True

    async def update_component_status(self, component_name: str, status: str, message: str = "") -> bool:
        """Update status for a monitored component"""
        async with self._lock:
            if component_name not in self.component_status:
                await self.register_component(component_name)
            old_status = self.component_status[component_name]["status"]
            self.component_status[component_name] = {
                "status": status,
                "last_update": datetime.now(timezone.utc).isoformat(),
                "message": message,
                "error_count": self.error_counts.get(component_name, 0)
            }
            # Update error count if status is error
            if status == "error":
                self.error_counts[component_name] = self.error_counts.get(component_name, 0) + 1
                self.component_status[component_name]["error_count"] = self.error_counts[component_name]
            # Log status change if significant
            if old_status != status:
                if status == "error":
                    logger.error(f"Component {component_name} status changed to {status}: {message}")
                elif status == "warning":
                    logger.warning(f"Component {component_name} status changed to {status}: {message}")
                else:
                    logger.info(f"Component {component_name} status changed to {status}")
            self.last_update = datetime.now(timezone.utc)
            return True

    async def record_metric(self, metric_name: str, value: Any) -> bool:
        """Record a performance metric"""
        async with self._lock:
            self.performance_metrics[metric_name] = {
                "value": value,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            self.last_update = datetime.now(timezone.utc)
            return True

    async def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        async with self._lock:
            # Determine overall status
            status_counts = {
                "ok": 0,
                "warning": 0,
                "error": 0,
                "initializing": 0
            }
            for component in self.component_status.values():
                component_status = component["status"]
                status_counts[component_status] = status_counts.get(component_status, 0) + 1
            if status_counts["error"] > 0:
                overall_status = "error"
            elif status_counts["warning"] > 0:
                overall_status = "warning"
            elif status_counts["initializing"] > 0:
                overall_status = "initializing"
            else:
                overall_status = "ok"
            # Calculate error rate
            total_components = len(self.component_status)
            error_rate = 0.0
            if total_components > 0:
                error_rate = status_counts["error"] / total_components * 100
            return {
                "status": overall_status,
                "component_count": total_components,
                "status_counts": status_counts,
                "error_rate": error_rate,
                "last_update": self.last_update.isoformat(),
                "uptime": (datetime.now(timezone.utc) - self.last_update).total_seconds(),
                "components": self.component_status,
                "metrics": self.performance_metrics
            }

    async def reset_error_counts(self) -> bool:
        """Reset error counts for all components"""
        async with self._lock:
            for component in self.error_counts:
                self.error_counts[component] = 0
                if component in self.component_status:
                    self.component_status[component]["error_count"] = 0
            logger.info("Reset error counts for all components")
            return True

    async def get_component_status(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Get status for a specific component"""
        async with self._lock:
            if component_name not in self.component_status:
                return None
            return self.component_status[component_name]

    # Add task manager methods
    async def start_task_manager(self):
        """Start the centralized task manager"""
        if self.task_manager is None:
            self.task_manager = asyncio.create_task(self._run_task_manager())
            logger.info("Centralized task manager started.")

    async def stop_task_manager(self):
        """Stop the centralized task manager"""
        if self.task_manager and not self.task_manager.done():
            self.task_manager.cancel()
            try:
                await self.task_manager
            except asyncio.CancelledError:
                pass  # Expected on cancellation
            logger.info("Centralized task manager stopped.")

    async def _run_task_manager(self):
        """The core loop for the task manager"""
        logger.info("Task manager loop running...")
        while True:
            try:
                # Add any periodic system-wide tasks here
                # Example:
                # await self.check_system_resources()
                # await self.run_self_diagnostics()
                
                # Wait for the next cycle
                await asyncio.sleep(config.system_monitor_interval)
            except asyncio.CancelledError:
                logger.info("Task manager loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in task manager loop: {e}", exc_info=True)
                # Avoid rapid-fire loops on persistent errors
                await asyncio.sleep(config.system_monitor_interval * 2)
