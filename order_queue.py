"""
Order queue system for handling trades during broker outages.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
import json

logger = logging.getLogger("OrderQueue")


@dataclass
class QueuedOrder:
    """Represents a queued order waiting for broker availability."""
    order_id: str
    payload: Dict[str, Any]
    received_at: datetime
    retry_count: int = 0
    max_retries: int = 3
    priority: int = 1  # 1=normal, 2=high, 3=urgent
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "order_id": self.order_id,
            "payload": self.payload,
            "received_at": self.received_at.isoformat(),
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "priority": self.priority
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueuedOrder':
        """Create from dictionary."""
        return cls(
            order_id=data["order_id"],
            payload=data["payload"],
            received_at=datetime.fromisoformat(data["received_at"]),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            priority=data.get("priority", 1)
        )


class OrderQueue:
    """Thread-safe order queue for handling trades during broker outages."""
    
    def __init__(self, max_queue_size: int = 100):
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._orders: Dict[str, QueuedOrder] = {}
        self._lock = asyncio.Lock()
        self.max_queue_size = max_queue_size
        self.total_queued = 0
        self.total_processed = 0
        self.total_failed = 0
    
    async def enqueue(self, order_id: str, payload: Dict[str, Any], priority: int = 1) -> bool:
        """Add an order to the queue."""
        try:
            async with self._lock:
                if len(self._orders) >= self.max_queue_size:
                    logger.warning(f"Order queue full ({self.max_queue_size}), rejecting order {order_id}")
                    return False
                
                queued_order = QueuedOrder(
                    order_id=order_id,
                    payload=payload,
                    received_at=datetime.now(timezone.utc),
                    priority=priority
                )
                
                self._orders[order_id] = queued_order
                await self._queue.put(queued_order)
                self.total_queued += 1
                
                logger.info(f"Order {order_id} queued (priority: {priority}, queue size: {len(self._orders)})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to enqueue order {order_id}: {e}")
            return False
    
    async def dequeue(self) -> Optional[QueuedOrder]:
        """Get the next order from the queue."""
        try:
            # Get order with timeout to avoid blocking
            order = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            return order
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Failed to dequeue order: {e}")
            return None
    
    async def mark_processed(self, order_id: str, success: bool = True) -> bool:
        """Mark an order as processed."""
        try:
            async with self._lock:
                if order_id in self._orders:
                    del self._orders[order_id]
                    
                    if success:
                        self.total_processed += 1
                        logger.info(f"Order {order_id} processed successfully")
                    else:
                        self.total_failed += 1
                        logger.warning(f"Order {order_id} processing failed")
                    
                    return True
                else:
                    logger.warning(f"Order {order_id} not found in queue")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to mark order {order_id} as processed: {e}")
            return False
    
    async def retry_order(self, order: QueuedOrder) -> bool:
        """Retry a failed order."""
        try:
            async with self._lock:
                order.retry_count += 1
                
                if order.retry_count > order.max_retries:
                    logger.error(f"Order {order.order_id} exceeded max retries ({order.max_retries})")
                    await self.mark_processed(order.order_id, success=False)
                    return False
                
                # Re-queue the order
                await self._queue.put(order)
                logger.info(f"Order {order.order_id} retried ({order.retry_count}/{order.max_retries})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to retry order {order.order_id}: {e}")
            return False
    
    async def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status."""
        async with self._lock:
            return {
                "queue_size": len(self._orders),
                "max_queue_size": self.max_queue_size,
                "total_queued": self.total_queued,
                "total_processed": self.total_processed,
                "total_failed": self.total_failed,
                "orders": [order.to_dict() for order in self._orders.values()]
            }
    
    async def clear_queue(self) -> int:
        """Clear all orders from the queue."""
        async with self._lock:
            cleared_count = len(self._orders)
            self._orders.clear()
            
            # Clear the asyncio queue
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            
            logger.info(f"Cleared {cleared_count} orders from queue")
            return cleared_count
    
    async def get_oldest_order_age_seconds(self) -> Optional[float]:
        """Get age of oldest order in queue."""
        if not self._orders:
            return None
        
        now = datetime.now(timezone.utc)
        oldest = min(order.received_at for order in self._orders.values())
        return (now - oldest).total_seconds()
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._orders) == 0
    
    def is_full(self) -> bool:
        """Check if queue is full."""
        return len(self._orders) >= self.max_queue_size
