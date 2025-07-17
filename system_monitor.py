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
        """Initialize system monitor with task management"""
        self.component_status = {}  # component_name -> status
        self.performance_metrics = {}  # metric_name -> value
        self.error_counts = {}  # component_name -> error count
        self.last_update = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()
        
        # Task management (displaced from alert_handler)
        self.scheduled_tasks = {}  # task_name -> task
        self.task_manager_running = False
        self._task_manager_task = None

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

    async def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health status"""
        async with self._lock:
            healthy_components = sum(1 for comp in self.component_status.values() if comp["status"] == "ok")
            total_components = len(self.component_status)
            
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "healthy_components": healthy_components,
                "total_components": total_components,
                "health_percentage": (healthy_components / total_components * 100) if total_components > 0 else 0,
                "components": self.component_status.copy(),
                "error_counts": self.error_counts.copy(),
                "performance_metrics": self.performance_metrics.copy()
            }

    # ===== DISPLACED FUNCTIONALITY FROM ALERT_HANDLER =====
    
    async def start_task_manager(self):
        """Start the centralized task manager"""
        if self.task_manager_running:
            logger.info("Task manager already running")
            return
            
        self.task_manager_running = True
        self._task_manager_task = asyncio.create_task(self._handle_scheduled_tasks())
        logger.info("✅ System task manager started")

    async def stop_task_manager(self):
        """Stop the centralized task manager"""
        if not self.task_manager_running:
            return
            
        self.task_manager_running = False
        
        # Cancel all scheduled tasks
        for task_name, task in self.scheduled_tasks.items():
            if not task.done():
                task.cancel()
                logger.info(f"Cancelled scheduled task: {task_name}")
        
        # Cancel main task manager
        if self._task_manager_task and not self._task_manager_task.done():
            self._task_manager_task.cancel()
            
        self.scheduled_tasks.clear()
        logger.info("✅ System task manager stopped")

    async def add_scheduled_task(self, task_name: str, coro, interval_seconds: int = 300):
        """Add a scheduled task to run periodically"""
        if task_name in self.scheduled_tasks:
            logger.warning(f"Task {task_name} already exists, replacing...")
            old_task = self.scheduled_tasks[task_name]
            if not old_task.done():
                old_task.cancel()
        
        async def task_wrapper():
            while self.task_manager_running:
                try:
                    await coro()
                    await self.record_metric(f"task_{task_name}_last_run", datetime.now(timezone.utc).isoformat())
                except Exception as e:
                    logger.error(f"Error in scheduled task {task_name}: {e}")
                    await self.record_error(f"scheduled_task_{task_name}")
                
                await asyncio.sleep(interval_seconds)
        
        self.scheduled_tasks[task_name] = asyncio.create_task(task_wrapper())
        logger.info(f"Added scheduled task: {task_name} (interval: {interval_seconds}s)")

    async def _handle_scheduled_tasks(self):
        """Main scheduled tasks handler (displaced from alert_handler)"""
        logger.info("System task manager started - handling scheduled tasks")
        
        # Add default system maintenance tasks
        await self.add_scheduled_task("system_health_check", self._system_health_check, 300)  # 5 min
        await self.add_scheduled_task("component_status_update", self._update_component_status, 60)  # 1 min
        await self.add_scheduled_task("performance_metrics_cleanup", self._cleanup_old_metrics, 3600)  # 1 hour
        
        try:
            while self.task_manager_running:
                # Main task manager heartbeat
                await self.record_metric("task_manager_heartbeat", datetime.now(timezone.utc).isoformat())
                await asyncio.sleep(30)  # Heartbeat every 30 seconds
                
        except asyncio.CancelledError:
            logger.info("Task manager main loop cancelled")
        except Exception as e:
            logger.error(f"Error in main task manager loop: {e}")

    async def _system_health_check(self):
        """Perform system-wide health check"""
        try:
            health_data = await self.get_system_health()
            await self.record_metric("system_health_percentage", health_data["health_percentage"])
            
            if health_data["health_percentage"] < 80:
                logger.warning(f"System health below 80%: {health_data['health_percentage']:.1f}%")
            
        except Exception as e:
            logger.error(f"Error in system health check: {e}")

    async def _update_component_status(self):
        """Update component status information"""
        try:
            current_time = datetime.now(timezone.utc)
            stale_threshold = 300  # 5 minutes
            
            async with self._lock:
                for component_name, status_info in self.component_status.items():
                    last_update_str = status_info.get("last_update", "")
                    if last_update_str:
                        try:
                            last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
                            if (current_time - last_update).total_seconds() > stale_threshold:
                                status_info["status"] = "stale"
                                status_info["message"] = f"No update for {(current_time - last_update).total_seconds():.0f}s"
                        except ValueError:
                            pass
                            
        except Exception as e:
            logger.error(f"Error updating component status: {e}")

    async def _cleanup_old_metrics(self):
        """Clean up old performance metrics"""
        try:
            # Keep only last 100 metrics to prevent memory growth
            if len(self.performance_metrics) > 100:
                # Sort by timestamp and keep newest 100
                items = list(self.performance_metrics.items())
                # Simple cleanup - remove oldest 25%
                to_remove = len(items) // 4
                for i in range(to_remove):
                    key = items[i][0]
                    del self.performance_metrics[key]
                
                logger.debug(f"Cleaned up {to_remove} old performance metrics")
                
        except Exception as e:
            logger.error(f"Error cleaning up metrics: {e}")
