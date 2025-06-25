from datetime import datetime, timezone

class MetricsCollector:
    def __init__(self):
        self.metrics = {
            "signals_received": 0,
            "signals_executed": 0,
            "execution_latency_ms": [],
            "slippage_pips": [],
            "positions_opened": 0,
            "positions_closed": 0,
            "total_pnl": 0.0,
            "last_signal_time": None,
            "system_uptime": datetime.now(timezone.utc)
        }
    
    def record_signal_received(self):
        self.metrics["signals_received"] += 1
        self.metrics["last_signal_time"] = datetime.now(timezone.utc)
    
    def record_execution(self, latency_ms: float, slippage_pips: float):
        self.metrics["signals_executed"] += 1
        self.metrics["execution_latency_ms"].append(latency_ms)
        self.metrics["slippage_pips"].append(slippage_pips)
        
        # Keep only last 100 measurements
        if len(self.metrics["execution_latency_ms"]) > 100:
            self.metrics["execution_latency_ms"] = self.metrics["execution_latency_ms"][-100:]
        if len(self.metrics["slippage_pips"]) > 100:
            self.metrics["slippage_pips"] = self.metrics["slippage_pips"][-100:]
    
    def get_summary(self):
        latencies = self.metrics["execution_latency_ms"]
        slippages = self.metrics["slippage_pips"]
        
        return {
            "signals_received": self.metrics["signals_received"],
            "execution_rate": self.metrics["signals_executed"] / max(1, self.metrics["signals_received"]) * 100,
            "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
            "avg_slippage_pips": sum(slippages) / len(slippages) if slippages else 0,
            "uptime_hours": (datetime.now(timezone.utc) - self.metrics["system_uptime"]).total_seconds() / 3600
        } 