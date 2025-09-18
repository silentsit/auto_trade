import asyncio
import argparse
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from oanda_service import OandaService
from position_journal import PositionJournal
from tracker import PositionTracker

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ManualPositionClose:
    def __init__(self):
        self.oanda_service = OandaService()
        self.position_tracker = PositionTracker()
        self.position_journal = PositionJournal()

    async def list_positions(self) -> List[Dict]:
        """
        List all open positions with details
        """
        try:
            oanda_positions = await self.oanda_service.get_positions()
            tracked_positions = await self.position_tracker.get_all_positions()
            
            positions = []
            for pos in oanda_positions:
                tracked_info = next(
                    (tp for tp in tracked_positions if tp["position_id"] == pos["id"]),
                    {}
                )
                
                positions.append({
                    "position_id": pos["id"],
                    "symbol": pos["instrument"],
                    "units": pos["units"],
                    "entry_price": pos.get("averagePrice"),
                    "current_price": await self.oanda_service.get_current_price(pos["instrument"]),
                    "pnl": pos.get("unrealizedPL"),
                    "open_time": tracked_info.get("open_time", "Unknown"),
                    "tracking_status": "Tracked" if tracked_info else "Untracked"
                })
            
            return positions
            
        except Exception as e:
            logger.error(f"Failed to list positions: {str(e)}")
            return []

    async def close_position(self, position_id: str, force: bool = False) -> Dict:
        """
        Close a specific position
        """
        try:
            # Get position details
            position = await self.oanda_service.get_position(position_id)
            if not position:
                return {"status": "error", "error": f"Position {position_id} not found"}
            
            # Close in OANDA
            close_payload = {
                "symbol": position["instrument"],
                "action": "CLOSE",
                "units": str(abs(float(position["units"])))
            }
            
            success, result = await self.oanda_service.execute_trade(close_payload)
            
            if success:
                # Update tracking if force close
                if force:
                    await self.position_tracker.force_close_position(
                        position_id,
                        "manual_force_close"
                    )
                else:
                    await self.position_tracker.close_position(
                        position_id,
                        float(result["price"]),
                        "manual_close"
                    )
                
                # Record in journal
                await self.position_journal.record_exit(
                    position_id=position_id,
                    exit_price=float(result["price"]),
                    exit_reason="manual_close" if not force else "manual_force_close"
                )
                
                return {
                    "status": "success",
                    "position_id": position_id,
                    "close_price": result["price"]
                }
            else:
                return {
                    "status": "error",
                    "error": result.get("error", "Unknown error")
                }
                
        except Exception as e:
            logger.error(f"Failed to close position {position_id}: {str(e)}")
            return {"status": "error", "error": str(e)}

    async def close_all_positions(self, force: bool = False) -> List[Dict]:
        """
        Close all open positions
        """
        results = []
        positions = await self.list_positions()
        
        for position in positions:
            result = await self.close_position(position["position_id"], force)
            results.append(result)
        
        return results

def print_positions(positions: List[Dict]):
    """
    Pretty print position information
    """
    if not positions:
        print("No open positions found.")
        return
    
    print("\nOpen Positions:")
    print("-" * 100)
    print(f"{'ID':<12} {'Symbol':<10} {'Units':<10} {'Entry':<10} {'Current':<10} {'P/L':<10} {'Status':<10}")
    print("-" * 100)
    
    for pos in positions:
        print(
            f"{pos['position_id']:<12} "
            f"{pos['symbol']:<10} "
            f"{pos['units']:<10} "
            f"{pos.get('entry_price', 'N/A'):<10} "
            f"{pos.get('current_price', 'N/A'):<10} "
            f"{pos.get('pnl', 'N/A'):<10} "
            f"{pos['tracking_status']:<10}"
        )
    print("-" * 100)

async def main():
    parser = argparse.ArgumentParser(description='Manual Position Close Utility')
    parser.add_argument('--list', action='store_true', help='List all open positions')
    parser.add_argument('--close', type=str, help='Close specific position by ID')
    parser.add_argument('--close-all', action='store_true', help='Close all open positions')
    parser.add_argument('--force', action='store_true', help='Force close (ignores tracking state)')
    
    args = parser.parse_args()
    
    manager = ManualPositionClose()
    
    if args.list:
        positions = await manager.list_positions()
        print_positions(positions)
        
    elif args.close:
        result = await manager.close_position(args.close, args.force)
        print(f"\nClose result for position {args.close}:")
        print(result)
        
    elif args.close_all:
        results = await manager.close_all_positions(args.force)
        print("\nClose results for all positions:")
        for result in results:
            print(result)
            
    else:
        parser.print_help()

if __name__ == "__main__":
    asyncio.run(main())