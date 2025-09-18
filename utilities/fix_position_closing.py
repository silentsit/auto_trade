import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from oanda_service import OandaService
from position_journal import PositionJournal
from tracker import PositionTracker

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PositionClosingFix:
    def __init__(self, oanda_service: OandaService, position_tracker: PositionTracker, position_journal: PositionJournal):
        self.oanda_service = oanda_service
        self.position_tracker = position_tracker
        self.position_journal = position_journal
        self.close_attempts = {}

    async def apply_position_closing_fix(self):
        """
        Apply fixes to position closing logic
        """
        try:
            # 1. Verify and fix position tracking synchronization
            await self._sync_position_tracking()
            
            # 2. Update close signal handling
            await self._update_close_signal_handling()
            
            # 3. Verify all positions have proper stop-loss orders
            await self._verify_stop_loss_orders()
            
            return {"status": "success", "message": "Position closing fixes applied successfully"}
            
        except Exception as e:
            logger.error(f"Failed to apply position closing fixes: {str(e)}")
            return {"status": "error", "error": str(e)}

    async def _sync_position_tracking(self):
        """
        Synchronize internal position tracking with OANDA
        """
        # Get positions from both systems
        oanda_positions = await self.oanda_service.get_positions()
        tracked_positions = await self.position_tracker.get_all_positions()
        
        # Create maps for easy lookup
        oanda_map = {p["id"]: p for p in oanda_positions}
        tracked_map = {p["position_id"]: p for p in tracked_positions}
        
        # Fix ghost tracked positions
        for pos_id in tracked_map:
            if pos_id not in oanda_map:
                logger.warning(f"Removing ghost tracked position: {pos_id}")
                await self.position_tracker.force_close_position(pos_id, "sync_fix")
        
        # Add missing tracked positions
        for pos_id, oanda_pos in oanda_map.items():
            if pos_id not in tracked_map:
                logger.warning(f"Adding missing tracked position: {pos_id}")
                await self.position_tracker.add_position_from_oanda(oanda_pos)

    async def _update_close_signal_handling(self):
        """
        Update close signal handling logic
        """
        # Implement proper close signal sequence
        async def handle_close_signal(position_id: str, reason: str) -> Dict:
            try:
                # 1. Verify position exists in OANDA
                oanda_position = await self.oanda_service.get_position(position_id)
                if not oanda_position:
                    return {"status": "error", "error": "Position not found in OANDA"}
                
                # 2. Close position in OANDA
                close_payload = {
                    "symbol": oanda_position["instrument"],
                    "action": "CLOSE",
                    "units": str(abs(float(oanda_position["units"])))
                }
                
                success, result = await self.oanda_service.execute_trade(close_payload)
                
                if not success:
                    return {"status": "error", "error": result.get("error", "Unknown error")}
                
                # 3. Update internal tracking ONLY after successful OANDA close
                await self.position_tracker.close_position(
                    position_id,
                    float(result["price"]),
                    reason
                )
                
                # 4. Record in journal
                await self.position_journal.record_exit(
                    position_id=position_id,
                    exit_price=float(result["price"]),
                    exit_reason=reason
                )
                
                return {"status": "success", "close_price": result["price"]}
                
            except Exception as e:
                logger.error(f"Close signal handling failed: {str(e)}")
                return {"status": "error", "error": str(e)}
        
        # Update the close signal handler in your system
        self.handle_close_signal = handle_close_signal

    async def _verify_stop_loss_orders(self):
        """
        Verify and fix stop-loss orders for all positions
        """
        positions = await self.oanda_service.get_positions()
        
        for position in positions:
            try:
                # Check if position has stop-loss
                if "stopLossOrder" not in position:
                    # Calculate appropriate stop-loss based on your risk management
                    stop_loss = await self._calculate_stop_loss(position)
                    
                    # Add stop-loss order
                    await self.oanda_service.modify_position(
                        position["id"],
                        stop_loss=stop_loss
                    )
                    
                    logger.info(f"Added missing stop-loss for position {position['id']}")
                    
            except Exception as e:
                logger.error(f"Failed to verify stop-loss for position {position['id']}: {str(e)}")

    async def _calculate_stop_loss(self, position: Dict) -> float:
        """
        Calculate appropriate stop-loss level based on risk management rules
        """
        # Implement your stop-loss calculation logic here
        # This is a placeholder implementation
        current_price = float(position.get("averagePrice", 0))
        atr = await self._get_atr(position["instrument"])
        
        # Default to 2 ATR for stop-loss
        stop_distance = atr * 2
        
        return current_price - stop_distance if float(position["units"]) > 0 else current_price + stop_distance

    async def _get_atr(self, symbol: str, period: int = 14) -> float:
        """
        Get ATR value for a symbol
        """
        # Implement your ATR calculation logic here
        # This is a placeholder implementation
        return 0.001  # Default to 0.1% of price

async def main():
    """
    Main function to apply fixes
    """
    # Initialize services
    oanda_service = OandaService()
    position_tracker = PositionTracker()
    position_journal = PositionJournal()
    
    fixer = PositionClosingFix(oanda_service, position_tracker, position_journal)
    
    # Apply fixes
    result = await fixer.apply_position_closing_fix()
    logger.info(f"Fix application result: {result}")

if __name__ == "__main__":
    asyncio.run(main())