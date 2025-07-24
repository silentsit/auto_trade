#!/usr/bin/env python3
"""
Test script for override position migration logic
"""

import sys
import os
import asyncio
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.migrate_override_positions import OverridePositionMigrator

async def test_migration_logic():
    """Test the migration logic with sample data"""
    
    migrator = OverridePositionMigrator()
    
    # Sample position with trigger data (best practice case)
    position_with_trigger = {
        'position_id': 'test_001',
        'symbol': 'EUR_USD',
        'action': 'BUY',
        'timeframe': '15',
        'metadata': {
            'profit_ride_override_fired': True,
            'transaction_id': '12345',
            'override_trigger_data': {
                'trigger_price': 1.0850,
                'trigger_atr': 0.0020,
                'trigger_timestamp': datetime.utcnow().isoformat(),
                'trigger_rr_ratio': 1.5,
                'trigger_timeframe': '15'
            }
        }
    }
    
    # Sample position without trigger data (fallback case)
    position_without_trigger = {
        'position_id': 'test_002',
        'symbol': 'GBP_USD',
        'action': 'SELL',
        'timeframe': '60',
        'metadata': {
            'profit_ride_override_fired': True,
            'transaction_id': '67890'
        }
    }
    
    print("=== Testing Migration Logic ===\n")
    
    # Test with trigger data
    print("1. Testing position WITH trigger data:")
    target_sl = migrator.calculate_target_sl(
        position_with_trigger['metadata']['override_trigger_data']['trigger_price'],
        position_with_trigger['metadata']['override_trigger_data']['trigger_atr'],
        position_with_trigger['action']
    )
    target_tp = migrator.calculate_target_tp(
        position_with_trigger['metadata']['override_trigger_data']['trigger_price'],
        position_with_trigger['metadata']['override_trigger_data']['trigger_atr'],
        position_with_trigger['metadata']['override_trigger_data']['trigger_rr_ratio'],
        position_with_trigger['action']
    )
    
    print(f"   Original trigger price: {position_with_trigger['metadata']['override_trigger_data']['trigger_price']}")
    print(f"   Original trigger ATR: {position_with_trigger['metadata']['override_trigger_data']['trigger_atr']}")
    print(f"   RR ratio: {position_with_trigger['metadata']['override_trigger_data']['trigger_rr_ratio']}")
    print(f"   Calculated SL: {target_sl:.5f}")
    print(f"   Calculated TP: {target_tp:.5f}")
    print(f"   SL distance: {abs(position_with_trigger['metadata']['override_trigger_data']['trigger_price'] - target_sl):.5f}")
    print(f"   TP distance: {abs(target_tp - position_with_trigger['metadata']['override_trigger_data']['trigger_price']):.5f}")
    print(f"   Actual RR ratio: {abs(target_tp - position_with_trigger['metadata']['override_trigger_data']['trigger_price']) / abs(position_with_trigger['metadata']['override_trigger_data']['trigger_price'] - target_sl):.2f}:1")
    
    print("\n2. Testing position WITHOUT trigger data:")
    print("   This would use fallback approach with current ATR/price")
    print("   (Requires live market data to test)")
    
    print("\n=== Migration Logic Test Complete ===")

if __name__ == "__main__":
    asyncio.run(test_migration_logic()) 