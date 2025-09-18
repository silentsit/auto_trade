"""
INSTITUTIONAL ORDER MANAGEMENT SYSTEM (OMS)
Comprehensive order lifecycle management with advanced features
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Callable
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"

class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    OCO = "oco"  # One-Cancels-Other
    BRACKET = "bracket"

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

class TimeInForce(Enum):
    GTC = "gtc"  # Good Till Cancelled
    IOC = "ioc"  # Immediate or Cancel
    FOK = "fok"  # Fill or Kill
    GTD = "gtd"  # Good Till Date

@dataclass
class Order:
    """Order representation with full lifecycle tracking"""
    order_id: str
    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    remaining_quantity: float = 0.0
    average_price: Optional[float] = None
    created_at: str = ""
    updated_at: str = ""
    filled_at: Optional[str] = None
    cancelled_at: Optional[str] = None
    rejection_reason: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at
        if not self.metadata:
            self.metadata = {}
        if not self.remaining_quantity:
            self.remaining_quantity = self.quantity

@dataclass
class Fill:
    """Order fill representation"""
    fill_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    commission: float = 0.0
    timestamp: str = ""
    venue: str = "OANDA"
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

@dataclass
class OrderBook:
    """Order book representation"""
    symbol: str
    bids: List[Tuple[float, float]]  # (price, quantity)
    asks: List[Tuple[float, float]]  # (price, quantity)
    timestamp: str = ""
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

class OrderManager:
    """Institutional Order Management System"""
    
    def __init__(self, oanda_service=None, risk_manager=None):
        self.oanda_service = oanda_service
        self.risk_manager = risk_manager
        
        # Order storage
        self.orders = {}  # order_id -> Order
        self.fills = {}   # fill_id -> Fill
        self.order_book = {}  # symbol -> OrderBook
        
        # Order tracking
        self.pending_orders = deque()
        self.active_orders = {}
        self.completed_orders = {}
        
        # Performance tracking
        self.execution_stats = {
            "total_orders": 0,
            "filled_orders": 0,
            "cancelled_orders": 0,
            "rejected_orders": 0,
            "average_fill_time": 0.0,
            "fill_rate": 0.0
        }
        
        # Risk controls
        self.max_orders_per_second = 10
        self.max_pending_orders = 100
        self.order_timeout = 300  # 5 minutes
        
        # Event handlers
        self.event_handlers = {
            "order_created": [],
            "order_filled": [],
            "order_cancelled": [],
            "order_rejected": [],
            "order_updated": []
        }
        
        self._lock = asyncio.Lock()
        self._running = False
        
    async def start(self):
        """Start the order management system"""
        self._running = True
        asyncio.create_task(self._process_orders())
        asyncio.create_task(self._monitor_order_timeouts())
        logger.info("✅ Order Management System started")
    
    async def stop(self):
        """Stop the order management system"""
        self._running = False
        logger.info("🛑 Order Management System stopped")
    
    async def create_order(self, 
                          symbol: str,
                          side: OrderSide,
                          order_type: OrderType,
                          quantity: float,
                          price: Optional[float] = None,
                          stop_price: Optional[float] = None,
                          time_in_force: TimeInForce = TimeInForce.GTC,
                          client_order_id: Optional[str] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> Order:
        """Create a new order"""
        
        # Generate order IDs
        order_id = str(uuid.uuid4())
        if not client_order_id:
            client_order_id = f"CLIENT_{int(time.time() * 1000)}"
        
        # Create order
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            metadata=metadata or {}
        )
        
        # Risk validation
        if self.risk_manager:
            risk_allowed, risk_reason = await self.risk_manager.is_trade_allowed(
                quantity * (price or 0) / 100000,  # Convert to risk percentage
                symbol,
                side.value
            )
            if not risk_allowed:
                order.status = OrderStatus.REJECTED
                order.rejection_reason = f"Risk validation failed: {risk_reason}"
                await self._handle_order_rejected(order)
                return order
        
        # Store order
        async with self._lock:
            self.orders[order_id] = order
            self.pending_orders.append(order_id)
            self.execution_stats["total_orders"] += 1
        
        # Emit event
        await self._emit_event("order_created", order)
        
        logger.info(f"📝 Order created: {order_id} {side.value} {quantity} {symbol} @ {price}")
        return order
    
    async def submit_order(self, order_id: str) -> bool:
        """Submit order to broker"""
        try:
            order = self.orders.get(order_id)
            if not order:
                logger.error(f"Order not found: {order_id}")
                return False
            
            if order.status != OrderStatus.PENDING:
                logger.warning(f"Order {order_id} is not pending, status: {order.status}")
                return False
            
            # Submit to OANDA
            if self.oanda_service:
                success = await self._submit_to_oanda(order)
                if success:
                    order.status = OrderStatus.SUBMITTED
                    order.updated_at = datetime.now(timezone.utc).isoformat()
                    self.active_orders[order_id] = order
                    await self._emit_event("order_updated", order)
                    logger.info(f"✅ Order submitted: {order_id}")
                    return True
                else:
                    order.status = OrderStatus.REJECTED
                    order.rejection_reason = "Failed to submit to broker"
                    await self._handle_order_rejected(order)
                    return False
            else:
                # Simulate successful submission
                order.status = OrderStatus.SUBMITTED
                order.updated_at = datetime.now(timezone.utc).isoformat()
                self.active_orders[order_id] = order
                await self._emit_event("order_updated", order)
                return True
                
        except Exception as e:
            logger.error(f"Error submitting order {order_id}: {e}")
            order.status = OrderStatus.REJECTED
            order.rejection_reason = f"Submission error: {str(e)}"
            await self._handle_order_rejected(order)
            return False
    
    async def cancel_order(self, order_id: str, reason: str = "Manual cancellation") -> bool:
        """Cancel an order"""
        try:
            order = self.orders.get(order_id)
            if not order:
                logger.error(f"Order not found: {order_id}")
                return False
            
            if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                logger.warning(f"Cannot cancel order {order_id}, status: {order.status}")
                return False
            
            # Cancel with broker
            if self.oanda_service and order.status == OrderStatus.SUBMITTED:
                success = await self._cancel_with_oanda(order)
                if not success:
                    logger.error(f"Failed to cancel order {order_id} with broker")
                    return False
            
            # Update order status
            order.status = OrderStatus.CANCELLED
            order.cancelled_at = datetime.now(timezone.utc).isoformat()
            order.updated_at = order.cancelled_at
            order.metadata["cancellation_reason"] = reason
            
            # Move to completed orders
            if order_id in self.active_orders:
                del self.active_orders[order_id]
            self.completed_orders[order_id] = order
            
            await self._emit_event("order_cancelled", order)
            logger.info(f"❌ Order cancelled: {order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return False
    
    async def update_order(self, order_id: str, updates: Dict[str, Any]) -> bool:
        """Update order parameters"""
        try:
            order = self.orders.get(order_id)
            if not order:
                return False
            
            if order.status not in [OrderStatus.PENDING, OrderStatus.SUBMITTED]:
                logger.warning(f"Cannot update order {order_id}, status: {order.status}")
                return False
            
            # Update allowed fields
            allowed_fields = ["price", "stop_price", "quantity", "time_in_force"]
            for field, value in updates.items():
                if field in allowed_fields and hasattr(order, field):
                    setattr(order, field, value)
            
            order.updated_at = datetime.now(timezone.utc).isoformat()
            await self._emit_event("order_updated", order)
            logger.info(f"🔄 Order updated: {order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating order {order_id}: {e}")
            return False
    
    async def process_fill(self, fill_data: Dict[str, Any]) -> bool:
        """Process order fill"""
        try:
            order_id = fill_data.get("order_id")
            order = self.orders.get(order_id)
            if not order:
                logger.error(f"Order not found for fill: {order_id}")
                return False
            
            # Create fill record
            fill = Fill(
                fill_id=str(uuid.uuid4()),
                order_id=order_id,
                symbol=fill_data["symbol"],
                side=OrderSide(fill_data["side"]),
                quantity=fill_data["quantity"],
                price=fill_data["price"],
                commission=fill_data.get("commission", 0.0),
                venue=fill_data.get("venue", "OANDA")
            )
            
            # Store fill
            self.fills[fill.fill_id] = fill
            
            # Update order
            order.filled_quantity += fill.quantity
            order.remaining_quantity -= fill.quantity
            
            # Calculate average price
            if order.average_price is None:
                order.average_price = fill.price
            else:
                total_value = (order.average_price * (order.filled_quantity - fill.quantity)) + (fill.price * fill.quantity)
                order.average_price = total_value / order.filled_quantity
            
            # Update order status
            if order.remaining_quantity <= 0:
                order.status = OrderStatus.FILLED
                order.filled_at = fill.timestamp
                if order_id in self.active_orders:
                    del self.active_orders[order_id]
                self.completed_orders[order_id] = order
            else:
                order.status = OrderStatus.PARTIALLY_FILLED
            
            order.updated_at = datetime.now(timezone.utc).isoformat()
            
            # Update statistics
            self.execution_stats["filled_orders"] += 1
            
            await self._emit_event("order_filled", {"order": order, "fill": fill})
            logger.info(f"💰 Order filled: {order_id} {fill.quantity} @ {fill.price}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing fill: {e}")
            return False
    
    async def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        return self.orders.get(order_id)
    
    async def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        """Get all orders for a symbol"""
        return [order for order in self.orders.values() if order.symbol == symbol]
    
    async def get_active_orders(self) -> List[Order]:
        """Get all active orders"""
        return list(self.active_orders.values())
    
    async def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        """Get order status"""
        order = self.orders.get(order_id)
        return order.status if order else None
    
    async def get_execution_statistics(self) -> Dict[str, Any]:
        """Get execution statistics"""
        total_orders = self.execution_stats["total_orders"]
        if total_orders > 0:
            self.execution_stats["fill_rate"] = self.execution_stats["filled_orders"] / total_orders
        
        return {
            **self.execution_stats,
            "pending_orders": len(self.pending_orders),
            "active_orders": len(self.active_orders),
            "completed_orders": len(self.completed_orders)
        }
    
    async def _process_orders(self):
        """Process pending orders"""
        while self._running:
            try:
                if self.pending_orders:
                    order_id = self.pending_orders.popleft()
                    await self.submit_order(order_id)
                
                await asyncio.sleep(0.1)  # Small delay to prevent overwhelming
            except Exception as e:
                logger.error(f"Error in order processing: {e}")
                await asyncio.sleep(1)
    
    async def _monitor_order_timeouts(self):
        """Monitor order timeouts"""
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                timeout_orders = []
                
                for order_id, order in self.active_orders.items():
                    order_time = datetime.fromisoformat(order.created_at)
                    if (current_time - order_time).total_seconds() > self.order_timeout:
                        timeout_orders.append(order_id)
                
                for order_id in timeout_orders:
                    await self.cancel_order(order_id, "Order timeout")
                
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in timeout monitoring: {e}")
                await asyncio.sleep(30)
    
    async def _submit_to_oanda(self, order: Order) -> bool:
        """Submit order to OANDA"""
        try:
            # This would integrate with actual OANDA API
            # For now, simulate successful submission
            return True
        except Exception as e:
            logger.error(f"Error submitting to OANDA: {e}")
            return False
    
    async def _cancel_with_oanda(self, order: Order) -> bool:
        """Cancel order with OANDA"""
        try:
            # This would integrate with actual OANDA API
            return True
        except Exception as e:
            logger.error(f"Error cancelling with OANDA: {e}")
            return False
    
    async def _handle_order_rejected(self, order: Order):
        """Handle order rejection"""
        order.updated_at = datetime.now(timezone.utc).isoformat()
        self.completed_orders[order.order_id] = order
        self.execution_stats["rejected_orders"] += 1
        await self._emit_event("order_rejected", order)
    
    async def _emit_event(self, event_type: str, data: Any):
        """Emit event to registered handlers"""
        handlers = self.event_handlers.get(event_type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    handler(data)
            except Exception as e:
                logger.error(f"Error in event handler for {event_type}: {e}")
    
    def add_event_handler(self, event_type: str, handler: Callable):
        """Add event handler"""
        if event_type in self.event_handlers:
            self.event_handlers[event_type].append(handler)

# Global order manager instance
order_manager = OrderManager()

# Convenience functions
async def create_order(symbol: str, side: OrderSide, order_type: OrderType, 
                      quantity: float, **kwargs) -> Order:
    """Create a new order"""
    return await order_manager.create_order(symbol, side, order_type, quantity, **kwargs)

async def cancel_order(order_id: str, reason: str = "Manual cancellation") -> bool:
    """Cancel an order"""
    return await order_manager.cancel_order(order_id, reason)

async def get_order_status(order_id: str) -> Optional[OrderStatus]:
    """Get order status"""
    return await order_manager.get_order_status(order_id)

async def get_execution_stats() -> Dict[str, Any]:
    """Get execution statistics"""
    return await order_manager.get_execution_statistics()
