"""
Exit Signal Monitor
Tracks the effectiveness of exit signals and provides alerts when exits fail
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass
from config import config

logger = logging.getLogger("exit_monitor")

@dataclass
class ExitSignalRecord:
    """Record of an exit signal and its processing"""
    signal_time: datetime
    symbol: str
    position_id: Optional[str]
    alert_data: Dict[str, Any]
    status: str  # 'pending', 'success', 'failed', 'timeout'
    processing_time: Optional[float] = None
    error_message: Optional[str] = None
    retry_count: int = 0

class ExitSignalMonitor:
    """
    Monitor exit signals and track their effectiveness.
    Provides alerts when exits are consistently failing.
    """
    
    def __init__(self):
        self.pending_exits: Dict[str, ExitSignalRecord] = {}
        self.completed_exits: List[ExitSignalRecord] = []
        self.failed_exits: List[ExitSignalRecord] = []
        self._lock = asyncio.Lock()
        self._monitoring = False
        self._monitor_task = None
        
        # Statistics
        self.stats = {
            "total_signals": 0,
            "successful_exits": 0,
            "failed_exits": 0,
            "timed_out_exits": 0,
            "average_processing_time": 0.0,
            "success_rate": 0.0
        }
    
    async def start_monitoring(self):
        """Start the exit signal monitoring"""
        if self._monitoring:
            return
            
        self._monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Exit signal monitoring started")
    
    async def stop_monitoring(self):
        """Stop the exit signal monitoring"""
        self._monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Exit signal monitoring stopped")
    
    async def record_exit_signal(self, alert_data: Dict[str, Any]) -> str:
        """Record a new exit signal for monitoring"""
        async with self._lock:
            signal_id = f"{alert_data.get('symbol', 'UNKNOWN')}_{int(time.time())}"
            
            record = ExitSignalRecord(
                signal_time=datetime.now(timezone.utc),
                symbol=alert_data.get('symbol', 'UNKNOWN'),
                position_id=alert_data.get('position_id') or alert_data.get('alert_id'),
                alert_data=alert_data.copy(),
                status='pending'
            )
            
            self.pending_exits[signal_id] = record
            self.stats["total_signals"] += 1
            
            logger.info(f"[EXIT MONITOR] Recorded exit signal: {signal_id}")
            return signal_id
    
    async def record_exit_success(self, signal_id: str, processing_time: float):
        """Record successful exit processing"""
        async with self._lock:
            if signal_id in self.pending_exits:
                record = self.pending_exits[signal_id]
                record.status = 'success'
                record.processing_time = processing_time
                
                self.completed_exits.append(record)
                del self.pending_exits[signal_id]
                
                self.stats["successful_exits"] += 1
                self._update_stats()
                
                logger.info(f"[EXIT MONITOR] Exit success: {signal_id} (processed in {processing_time:.2f}s)")
    
    async def record_exit_failure(self, signal_id: str, error_message: str, processing_time: Optional[float] = None):
        """Record failed exit processing"""
        async with self._lock:
            if signal_id in self.pending_exits:
                record = self.pending_exits[signal_id]
                record.status = 'failed'
                record.error_message = error_message
                record.processing_time = processing_time
                
                self.failed_exits.append(record)
                del self.pending_exits[signal_id]
                
                self.stats["failed_exits"] += 1
                self._update_stats()
                
                logger.error(f"[EXIT MONITOR] Exit failed: {signal_id} - {error_message}")
    
    async def record_exit_retry(self, signal_id: str):
        """Record an exit retry attempt"""
        async with self._lock:
            if signal_id in self.pending_exits:
                record = self.pending_exits[signal_id]
                record.retry_count += 1
                logger.info(f"[EXIT MONITOR] Exit retry #{record.retry_count}: {signal_id}")
    
    async def _monitor_loop(self):
        """Main monitoring loop to check for timeouts and issues"""
        while self._monitoring:
            try:
                await self._check_timeouts()
                await self._analyze_patterns()
                await asyncio.sleep(30)  # Check every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in exit monitor loop: {e}")
                await asyncio.sleep(30)
    
    async def _check_timeouts(self):
        """Check for exit signals that have timed out"""
        timeout_seconds = config.exit_signal_timeout_minutes * 60
        current_time = datetime.now(timezone.utc)
        
        async with self._lock:
            timed_out = []
            
            for signal_id, record in self.pending_exits.items():
                time_elapsed = (current_time - record.signal_time).total_seconds()
                
                if time_elapsed > timeout_seconds:
                    record.status = 'timeout'
                    record.error_message = f"Exit signal timed out after {time_elapsed:.1f} seconds"
                    
                    timed_out.append((signal_id, record))
                    self.stats["timed_out_exits"] += 1
            
            # Move timed out records to failed exits
            for signal_id, record in timed_out:
                self.failed_exits.append(record)
                del self.pending_exits[signal_id]
                
                logger.error(f"[EXIT MONITOR] Exit timed out: {signal_id} (symbol: {record.symbol})")
            
            if timed_out:
                self._update_stats()
    
    async def _analyze_patterns(self):
        """Analyze exit failure patterns and alert if needed"""
        if len(self.completed_exits) + len(self.failed_exits) < 10:
            return  # Need more data
        
        recent_failures = 0
        recent_total = 0
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
        
        # Check recent exits (last hour)
        for record in self.completed_exits[-20:]:  # Last 20 exits
            if record.signal_time > cutoff_time:
                recent_total += 1
        
        for record in self.failed_exits[-20:]:  # Last 20 failures
            if record.signal_time > cutoff_time:
                recent_total += 1
                recent_failures += 1
        
        if recent_total >= 5:  # Minimum data for analysis
            failure_rate = recent_failures / recent_total
            
            if failure_rate > 0.5:  # More than 50% failure rate
                logger.error(f"[EXIT MONITOR] HIGH FAILURE RATE ALERT: {failure_rate:.1%} of recent exits failed!")
                await self._send_failure_alert(failure_rate, recent_failures, recent_total)
    
    async def _send_failure_alert(self, failure_rate: float, failed_count: int, total_count: int):
        """Send alert about high exit failure rate"""
        alert_message = (
            f"🚨 EXIT FAILURE ALERT 🚨\n\n"
            f"High exit signal failure rate detected:\n"
            f"📊 Failure Rate: {failure_rate:.1%}\n"
            f"❌ Failed: {failed_count}/{total_count} recent exits\n"
            f"⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"Please check:\n"
            f"• Pine Script alert configuration\n"
            f"• Position ID matching logic\n"
            f"• Network connectivity\n"
            f"• System logs for detailed errors"
        )
        
        logger.critical(alert_message.replace('\n', ' | '))
        
        # If notification system is available, send alert
        # This would be integrated with your notification system
    
    def _update_stats(self):
        """Update monitoring statistics"""
        total_processed = self.stats["successful_exits"] + self.stats["failed_exits"] + self.stats["timed_out_exits"]
        
        if total_processed > 0:
            self.stats["success_rate"] = self.stats["successful_exits"] / total_processed
        
        # Calculate average processing time
        processing_times = [
            record.processing_time for record in self.completed_exits 
            if record.processing_time is not None
        ]
        
        if processing_times:
            self.stats["average_processing_time"] = sum(processing_times) / len(processing_times)
    
    async def get_monitoring_report(self) -> Dict[str, Any]:
        """Get comprehensive monitoring report"""
        async with self._lock:
            pending_count = len(self.pending_exits)
            
            # Recent failures analysis
            recent_failures = []
            for record in self.failed_exits[-5:]:  # Last 5 failures
                recent_failures.append({
                    "time": record.signal_time.isoformat(),
                    "symbol": record.symbol,
                    "position_id": record.position_id,
                    "error": record.error_message,
                    "retry_count": record.retry_count
                })
            
            return {
                "monitoring_active": self._monitoring,
                "statistics": self.stats.copy(),
                "pending_exits": pending_count,
                "recent_failures": recent_failures,
                "recommendations": self._get_recommendations()
            }
    
    def _get_recommendations(self) -> List[str]:
        """Get recommendations based on current metrics"""
        recommendations = []
        
        if self.stats["success_rate"] < 0.8:
            recommendations.append("Exit success rate is low - check Pine Script alert configuration")
        
        if self.stats["average_processing_time"] > 10.0:
            recommendations.append("Exit processing is slow - check system performance")
        
        if self.stats["timed_out_exits"] > 0:
            recommendations.append("Some exits are timing out - increase timeout or check connectivity")
        
        if len(self.pending_exits) > 5:
            recommendations.append("Many exits are pending - check alert processing")
        
        return recommendations

# Global exit monitor instance
exit_monitor = ExitSignalMonitor() 