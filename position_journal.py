"""
Position Journal Module
Manages position journaling and logging for trading strategies
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class PositionJournalEntry:
    """Represents a single position journal entry"""
    position_id: str
    symbol: str
    action: str
    entry_time: datetime
    entry_price: float
    size: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    notes: str = ""
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class PositionJournal:
    """
    Position journal for tracking and logging trading activity
    """
    
    def __init__(self):
        self.entries: Dict[str, PositionJournalEntry] = {}
        self._lock = asyncio.Lock()
        
    async def log_position_entry(self, 
                                position_id: str,
                                symbol: str, 
                                action: str,
                                entry_price: float,
                                size: float,
                                stop_loss: Optional[float] = None,
                                take_profit: Optional[float] = None,
                                notes: str = "",
                                metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Log a new position entry"""
        try:
            async with self._lock:
                entry = PositionJournalEntry(
                    position_id=position_id,
                    symbol=symbol,
                    action=action.upper(),
                    entry_time=datetime.now(timezone.utc),
                    entry_price=entry_price,
                    size=size,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    notes=notes,
                    metadata=metadata or {}
                )
                
                self.entries[position_id] = entry
                logger.info(f"📝 Position journal entry created: {position_id} - {action} {size} {symbol} @ {entry_price}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to log position entry {position_id}: {e}")
            return False
    
    async def log_position_exit(self,
                               position_id: str,
                               exit_price: float,
                               pnl: float,
                               notes: str = "") -> bool:
        """Log position exit"""
        try:
            async with self._lock:
                if position_id not in self.entries:
                    logger.warning(f"Position {position_id} not found in journal for exit logging")
                    return False
                
                entry = self.entries[position_id]
                entry.exit_time = datetime.now(timezone.utc)
                entry.exit_price = exit_price
                entry.pnl = pnl
                if notes:
                    entry.notes += f" | Exit: {notes}"
                
                logger.info(f"📝 Position exit logged: {position_id} - Exit @ {exit_price}, P&L: {pnl}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to log position exit {position_id}: {e}")
            return False
    
    async def get_position_entry(self, position_id: str) -> Optional[PositionJournalEntry]:
        """Get position journal entry"""
        async with self._lock:
            return self.entries.get(position_id)
    
    async def get_all_entries(self) -> List[PositionJournalEntry]:
        """Get all journal entries"""
        async with self._lock:
            return list(self.entries.values())
    
    async def get_open_positions(self) -> List[PositionJournalEntry]:
        """Get entries for open positions"""
        async with self._lock:
            return [entry for entry in self.entries.values() if entry.exit_time is None]
    
    async def get_closed_positions(self) -> List[PositionJournalEntry]:
        """Get entries for closed positions"""
        async with self._lock:
            return [entry for entry in self.entries.values() if entry.exit_time is not None]
    
    async def update_position_metadata(self, position_id: str, metadata: Dict[str, Any]) -> bool:
        """Update position metadata"""
        try:
            async with self._lock:
                if position_id not in self.entries:
                    logger.warning(f"Position {position_id} not found in journal for metadata update")
                    return False
                
                self.entries[position_id].metadata.update(metadata)
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to update position metadata {position_id}: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get journal statistics"""
        try:
            all_entries = list(self.entries.values())
            closed_entries = [e for e in all_entries if e.exit_time is not None]
            
            if not closed_entries:
                return {
                    "total_positions": len(all_entries),
                    "open_positions": len(all_entries),
                    "closed_positions": 0,
                    "total_pnl": 0.0,
                    "win_rate": 0.0,
                    "avg_pnl": 0.0
                }
            
            total_pnl = sum(e.pnl for e in closed_entries if e.pnl is not None)
            winning_trades = [e for e in closed_entries if e.pnl and e.pnl > 0]
            
            return {
                "total_positions": len(all_entries),
                "open_positions": len(all_entries) - len(closed_entries),
                "closed_positions": len(closed_entries),
                "total_pnl": total_pnl,
                "win_rate": len(winning_trades) / len(closed_entries) * 100 if closed_entries else 0,
                "avg_pnl": total_pnl / len(closed_entries) if closed_entries else 0,
                "winning_trades": len(winning_trades),
                "losing_trades": len(closed_entries) - len(winning_trades)
            }
            
        except Exception as e:
            logger.error(f"❌ Error calculating journal statistics: {e}")
            return {}

# Global position journal instance
position_journal = PositionJournal()

# Convenience functions for compatibility
async def log_position_entry(*args, **kwargs):
    """Convenience function for logging position entry"""
    return await position_journal.log_position_entry(*args, **kwargs)

async def log_position_exit(*args, **kwargs):
    """Convenience function for logging position exit"""
    return await position_journal.log_position_exit(*args, **kwargs)
