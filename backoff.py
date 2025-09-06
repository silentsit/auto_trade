"""
Exponential backoff helper for handling OANDA maintenance and connection issues.
"""
import random
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger("Backoff")


@dataclass
class ExponentialBackoff:
    """Exponential backoff with jitter for handling maintenance and connection issues."""
    
    base_seconds: float = 30.0   # First maintenance retry after 30s
    factor: float = 2.0          # Exponential factor
    max_seconds: float = 900.0   # Cap at 15 minutes
    jitter: float = 0.3          # ±30% jitter
    
    _current: float = 0.0
    _last_reset: Optional[datetime] = None

    def next(self) -> float:
        """Get next backoff delay in seconds."""
        if self._current <= 0:
            self._current = self.base_seconds
        else:
            self._current = min(self._current * self.factor, self.max_seconds)
        
        # Add jitter to prevent thundering herd
        jitter_range = self._current * self.jitter
        delay = max(1.0, self._current + random.uniform(-jitter_range, jitter_range))
        
        return delay

    def reset(self) -> None:
        """Reset backoff to initial state."""
        self._current = 0.0
        self._last_reset = datetime.now(timezone.utc)

    def get_reset_age_seconds(self) -> float:
        """Get age of last reset in seconds."""
        if self._last_reset is None:
            return 0.0
        return (datetime.now(timezone.utc) - self._last_reset).total_seconds()


class ConnectionState:
    """Manages OANDA connection state and backoff logic."""
    
    def __init__(self, success_probe_seconds: int = 600):
        self.state = "DEGRADED"  # OK | MAINTENANCE | DEGRADED
        self.health = 0
        self._backoff = ExponentialBackoff()
        self._next_probe_at = datetime.now(timezone.utc)
        self._success_probe_seconds = success_probe_seconds
        self._last_maint_msg_logged_at = None
        self._consecutive_503_count = 0
        self._last_503_time = None

    def _should_probe(self) -> bool:
        """Check if we should probe the connection now."""
        return datetime.now(timezone.utc) >= self._next_probe_at

    def _schedule_next_probe(self, seconds: float):
        """Schedule the next connection probe."""
        self._next_probe_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)

    def handle_503_maintenance(self) -> bool:
        """Handle 503 maintenance error with appropriate backoff."""
        now = datetime.now(timezone.utc)
        
        # Track consecutive 503s
        if self._last_503_time and (now - self._last_503_time).total_seconds() < 300:  # Within 5 minutes
            self._consecutive_503_count += 1
        else:
            self._consecutive_503_count = 1
        
        self._last_503_time = now
        
        # Set state to maintenance
        self.state = "MAINTENANCE"
        self.health = 0
        
        # Calculate backoff delay
        delay = self._backoff.next()
        self._schedule_next_probe(delay)
        
        # Log maintenance status (reduce spam)
        should_log_error = (
            not self._last_maint_msg_logged_at or 
            (now - self._last_maint_msg_logged_at).total_seconds() > 600  # Every 10 minutes
        )
        
        if should_log_error:
            logger.error(f"OANDA under maintenance (503). Pausing probes for ~{delay:.0f}s. "
                        f"Consecutive 503s: {self._consecutive_503_count}")
            self._last_maint_msg_logged_at = now
        else:
            logger.warning(f"OANDA still in maintenance (503). Next probe in ~{delay:.0f}s. "
                          f"Consecutive 503s: {self._consecutive_503_count}")
        
        return False

    def handle_other_error(self, error: Exception) -> bool:
        """Handle non-503 errors with shorter backoff."""
        self.state = "DEGRADED"
        self.health = 0
        
        # Shorter backoff for non-maintenance errors
        delay = min(self._backoff.next(), 120.0)  # Cap at 2 minutes
        self._schedule_next_probe(delay)
        
        logger.warning(f"OANDA probe failed. Retrying in ~{delay:.0f}s. Error: {error}")
        return False

    def handle_success(self) -> bool:
        """Handle successful connection."""
        self.state = "OK"
        self.health = 100
        self._backoff.reset()
        self._consecutive_503_count = 0
        self._last_503_time = None
        
        # Schedule next probe much later for healthy connections
        self._schedule_next_probe(self._success_probe_seconds)
        
        logger.info(f"✅ OANDA healthy. Next probe in {self._success_probe_seconds}s.")
        return True

    def can_trade(self) -> bool:
        """Check if trading is allowed in current state."""
        return self.state == "OK" and self.health >= 50

    def get_status(self) -> dict:
        """Get current connection status."""
        return {
            "state": self.state,
            "health": self.health,
            "next_probe_at": self._next_probe_at.isoformat(),
            "consecutive_503s": self._consecutive_503_count,
            "backoff_current": self._backoff._current,
            "backoff_reset_age_seconds": self._backoff.get_reset_age_seconds()
        }
