import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from oanda_service import OandaService
from position_journal import PositionJournal
from tracker import PositionTracker

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PositionValidator:
    def __init__(self, oanda_service: OandaService, position_tracker: PositionTracker, position_journal: PositionJournal):
        self.oanda_service = oanda_service
        self.position_tracker = position_tracker
        self.position_journal = position_journal
        self.last_check_time = datetime.now(timezone.utc)
        self.discrepancy_count = 0
        self.failed_closes = []

    async def validate_positions(self) -> Dict:
        """
        Comprehensive position validation between OANDA and internal tracking
        """
        try:
            # Get positions from both systems
            oanda_positions = await self.oanda_service.get_positions()
            tracked_positions = await self.position_tracker.get_all_positions()
            
            # Convert to comparable format
            oanda_map = {p["id"]: p for p in oanda_positions}
            tracked_map = {p["position_id"]: p for p in tracked_positions}
            
            discrepancies = {
                "orphaned_oanda": [],  # In OANDA but not tracked
                "ghost_tracked": [],   # Tracked but not in OANDA
                "mismatched": []       # Different details between systems
            }
            
            # Find orphaned OANDA positions
            for pos_id, oanda_pos in oanda_map.items():
                if pos_id not in tracked_map:
                    discrepancies["orphaned_oanda"].append({
                        "position_id": pos_id,
                        "symbol": oanda_pos["instrument"],
                        "units": oanda_pos["units"],
                        "open_time": oanda_pos.get("openTime")
                    })
            
            # Find ghost tracked positions
            for pos_id, tracked_pos in tracked_map.items():
                if pos_id not in oanda_map:
                    discrepancies["ghost_tracked"].append({
                        "position_id": pos_id,
                        "symbol": tracked_pos["symbol"],
                        "units": tracked_pos["units"],
                        "open_time": tracked_pos.get("open_time")
                    })
            
            # Check for mismatches in common positions
            for pos_id in set(oanda_map.keys()) & set(tracked_map.keys()):
                oanda_pos = oanda_map[pos_id]
                tracked_pos = tracked_map[pos_id]
                
                if (abs(float(oanda_pos["units"])) != abs(float(tracked_pos["units"])) or
                    oanda_pos["instrument"] != tracked_pos["symbol"]):
                    discrepancies["mismatched"].append({
                        "position_id": pos_id,
                        "oanda_details": {
                            "units": oanda_pos["units"],
                            "symbol": oanda_pos["instrument"]
                        },
                        "tracked_details": {
                            "units": tracked_pos["units"],
                            "symbol": tracked_pos["symbol"]
                        }
                    })
            
            # Update metrics
            self.discrepancy_count = (len(discrepancies["orphaned_oanda"]) + 
                                    len(discrepancies["ghost_tracked"]) + 
                                    len(discrepancies["mismatched"]))
            
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "success" if self.discrepancy_count == 0 else "discrepancies_found",
                "discrepancy_count": self.discrepancy_count,
                "details": discrepancies
            }
            
        except Exception as e:
            logger.error(f"Position validation failed: {str(e)}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "error",
                "error": str(e)
            }

    async def monitor_close_signals(self) -> Dict:
        """
        Monitor and validate position close operations
        """
        try:
            current_time = datetime.now(timezone.utc)
            
            # Get recent close signals from journal
            recent_closes = await self.position_journal.get_recent_exits(
                since=self.last_check_time
            )
            
            close_status = {
                "successful_closes": [],
                "failed_closes": [],
                "pending_closes": []
            }
            
            for close in recent_closes:
                position_id = close["position_id"]
                
                # Check if position actually closed in OANDA
                oanda_position = await self.oanda_service.get_position(position_id)
                
                if oanda_position is None:
                    # Successfully closed
                    close_status["successful_closes"].append({
                        "position_id": position_id,
                        "close_time": close.get("exit_time"),
                        "reason": close.get("exit_reason")
                    })
                else:
                    # Failed to close
                    close_status["failed_closes"].append({
                        "position_id": position_id,
                        "attempted_close_time": close.get("exit_time"),
                        "reason": close.get("exit_reason"),
                        "current_status": "still_open"
                    })
                    
                    # Add to failed closes list for retry
                    self.failed_closes.append({
                        "position_id": position_id,
                        "symbol": close.get("symbol"),
                        "first_attempt": close.get("exit_time")
                    })
            
            self.last_check_time = current_time
            
            return {
                "timestamp": current_time.isoformat(),
                "status": "success" if not close_status["failed_closes"] else "failures_detected",
                "details": close_status
            }
            
        except Exception as e:
            logger.error(f"Close signal monitoring failed: {str(e)}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "error",
                "error": str(e)
            }

    async def emergency_close_position(self, position_id: str) -> Dict:
        """
        Emergency close for a specific position
        """
        try:
            # Get position details from OANDA
            position = await self.oanda_service.get_position(position_id)
            if not position:
                return {
                    "status": "error",
                    "error": f"Position {position_id} not found in OANDA"
                }
            
            # Prepare close payload
            close_payload = {
                "symbol": position["instrument"],
                "action": "CLOSE",
                "units": str(abs(float(position["units"])))  # Ensure positive units for close
            }
            
            # Execute close
            success, result = await self.oanda_service.execute_trade(close_payload)
            
            if success:
                # Update internal tracking
                await self.position_tracker.close_position(
                    position_id,
                    float(result["price"]),
                    "emergency_close"
                )
                
                # Record in journal
                await self.position_journal.record_exit(
                    position_id=position_id,
                    exit_price=float(result["price"]),
                    exit_reason="emergency_close"
                )
                
                return {
                    "status": "success",
                    "position_id": position_id,
                    "close_price": result["price"]
                }
            else:
                return {
                    "status": "error",
                    "error": result.get("error", "Unknown error"),
                    "position_id": position_id
                }
                
        except Exception as e:
            logger.error(f"Emergency close failed for position {position_id}: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "position_id": position_id
            }

    async def emergency_close_all(self) -> List[Dict]:
        """
        Emergency close all positions
        """
        results = []
        
        try:
            # Get all OANDA positions
            positions = await self.oanda_service.get_positions()
            
            for position in positions:
                result = await self.emergency_close_position(position["id"])
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Emergency close all failed: {str(e)}")
            return [{"status": "error", "error": str(e)}]

async def main():
    """
    Main validation and monitoring loop
    """
    # Initialize services
    oanda_service = OandaService()
    position_tracker = PositionTracker()
    position_journal = PositionJournal()
    
    validator = PositionValidator(oanda_service, position_tracker, position_journal)
    
    while True:
        try:
            # Validate positions
            validation_result = await validator.validate_positions()
            logger.info(f"Position validation result: {validation_result}")
            
            # Monitor close signals
            monitoring_result = await validator.monitor_close_signals()
            logger.info(f"Close signal monitoring result: {monitoring_result}")
            
            # Handle any failed closes
            if validator.failed_closes:
                logger.warning(f"Attempting to resolve {len(validator.failed_closes)} failed closes")
                for failed_close in validator.failed_closes:
                    result = await validator.emergency_close_position(failed_close["position_id"])
                    logger.info(f"Retry close result: {result}")
            
            # Wait before next check
            await asyncio.sleep(60)  # Check every minute
            
        except Exception as e:
            logger.error(f"Validation loop error: {str(e)}")
            await asyncio.sleep(5)  # Short delay on error

if __name__ == "__main__":
    asyncio.run(main())