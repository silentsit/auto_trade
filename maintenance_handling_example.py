#!/usr/bin/env python3
"""
Example showing how to integrate the new maintenance handling into your existing code.
"""
import asyncio
import logging
from datetime import datetime, timezone
from oanda_service import OandaService
from order_queue import OrderQueue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MaintenanceHandlingExample")


class TradingBotWithMaintenanceHandling:
    """Example trading bot with maintenance handling."""
    
    def __init__(self):
        self.oanda_service = OandaService(success_probe_seconds=600)  # 10 minutes between healthy probes
        self.order_queue = OrderQueue(max_queue_size=50)
        self.is_running = False
    
    async def initialize(self):
        """Initialize the bot."""
        try:
            await self.oanda_service.initialize()
            await self.oanda_service.start_connection_monitor()
            logger.info("✅ Trading bot initialized with maintenance handling")
        except Exception as e:
            logger.error(f"❌ Failed to initialize bot: {e}")
            raise
    
    async def handle_trading_signal(self, signal_data: dict):
        """Handle incoming trading signals with maintenance awareness."""
        order_id = f"order_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        # Check if we can trade
        if self.oanda_service.can_trade():
            # Try to execute immediately
            success, result = await self._execute_trade_immediately(signal_data)
            if success:
                logger.info(f"✅ Trade executed immediately: {order_id}")
                return {"status": "executed", "order_id": order_id, "result": result}
            else:
                logger.warning(f"⚠️ Immediate execution failed, queuing order: {order_id}")
        
        # Queue the order for later processing
        queued = await self.order_queue.enqueue(order_id, signal_data, priority=1)
        if queued:
            logger.info(f"📋 Order queued during {self.oanda_service.connection_state.state}: {order_id}")
            return {"status": "queued", "order_id": order_id, "broker_state": self.oanda_service.connection_state.state}
        else:
            logger.error(f"❌ Failed to queue order (queue full): {order_id}")
            return {"status": "rejected", "order_id": order_id, "reason": "queue_full"}
    
    async def _execute_trade_immediately(self, signal_data: dict):
        """Execute trade immediately if broker is available."""
        try:
            # Convert signal to OANDA format
            payload = {
                "symbol": signal_data.get("symbol"),
                "action": signal_data.get("action"),
                "units": signal_data.get("units"),
                "stop_loss": signal_data.get("stop_loss"),
                "take_profit": signal_data.get("take_profit")
            }
            
            return await self.oanda_service.execute_trade(payload)
        except Exception as e:
            logger.error(f"Immediate trade execution failed: {e}")
            return False, {"error": str(e)}
    
    async def process_queued_orders(self):
        """Process queued orders when broker becomes available."""
        while not self.order_queue.is_empty():
            order = await self.order_queue.dequeue()
            if not order:
                break
            
            logger.info(f"🔄 Processing queued order: {order.order_id}")
            
            try:
                success, result = await self._execute_trade_immediately(order.payload)
                await self.order_queue.mark_processed(order.order_id, success)
                
                if success:
                    logger.info(f"✅ Queued order executed: {order.order_id}")
                else:
                    logger.warning(f"⚠️ Queued order failed: {order.order_id}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing queued order {order.order_id}: {e}")
                await self.order_queue.mark_processed(order.order_id, success=False)
    
    async def monitor_connection_and_process_queue(self):
        """Background task to monitor connection and process queued orders."""
        while self.is_running:
            try:
                # Check if broker became available
                if self.oanda_service.can_trade() and not self.order_queue.is_empty():
                    logger.info("🔄 Broker available, processing queued orders...")
                    await self.process_queued_orders()
                
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in connection monitor: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def get_status(self):
        """Get comprehensive bot status."""
        connection_status = await self.oanda_service.get_connection_status()
        queue_status = await self.order_queue.get_queue_status()
        
        return {
            "bot_running": self.is_running,
            "connection": connection_status,
            "queue": queue_status,
            "can_trade": self.oanda_service.can_trade(),
            "broker_state": self.oanda_service.connection_state.state
        }
    
    async def start(self):
        """Start the bot."""
        self.is_running = True
        await self.initialize()
        
        # Start background tasks
        asyncio.create_task(self.monitor_connection_and_process_queue())
        
        logger.info("🚀 Trading bot started with maintenance handling")
    
    async def stop(self):
        """Stop the bot."""
        self.is_running = False
        await self.oanda_service.stop()
        logger.info("🛑 Trading bot stopped")


async def example_usage():
    """Example of how to use the bot."""
    bot = TradingBotWithMaintenanceHandling()
    
    try:
        await bot.start()
        
        # Simulate some trading signals
        signals = [
            {"symbol": "EUR_USD", "action": "BUY", "units": 1000, "stop_loss": 1.0800, "take_profit": 1.0900},
            {"symbol": "GBP_USD", "action": "SELL", "units": 2000, "stop_loss": 1.2700, "take_profit": 1.2600},
            {"symbol": "USD_JPY", "action": "BUY", "units": 1500, "stop_loss": 150.00, "take_profit": 151.00}
        ]
        
        for i, signal in enumerate(signals):
            logger.info(f"📊 Processing signal {i+1}: {signal['symbol']} {signal['action']}")
            result = await bot.handle_trading_signal(signal)
            logger.info(f"Result: {result}")
            
            # Wait a bit between signals
            await asyncio.sleep(2)
        
        # Show final status
        status = await bot.get_status()
        logger.info(f"📊 Final status: {status}")
        
        # Wait a bit to see background processing
        await asyncio.sleep(5)
        
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(example_usage())
