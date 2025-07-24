#!/usr/bin/env python3
"""
Weekend Position Management Demo

This script demonstrates how the weekend position age limit system works.
It shows the key features and configuration options.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from config import config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demo_weekend_detection():
    """Demonstrate weekend detection logic"""
    print("=" * 50)
    print("WEEKEND DETECTION DEMO")
    print("=" * 50)
    
    # Import the Position class to test weekend detection
    from services.position_journal import Position
    
    # Create a test position
    test_position = Position(
        position_id="DEMO_EUR_USD_LONG_123",
        symbol="EUR_USD", 
        action="BUY",
        timeframe="H1",
        entry_price=1.0950,
        size=10000
    )
    
    # Test different times
    import pytz
    est = pytz.timezone('US/Eastern')
    
    test_times = [
        ("Thursday 16:00 EST", datetime(2024, 1, 18, 21, 0, 0, tzinfo=timezone.utc)),  # Thu 4pm EST = 9pm UTC
        ("Friday 17:30 EST", datetime(2024, 1, 19, 22, 30, 0, tzinfo=timezone.utc)),   # Fri 5:30pm EST = 10:30pm UTC  
        ("Saturday 10:00 EST", datetime(2024, 1, 20, 15, 0, 0, tzinfo=timezone.utc)),  # Sat 10am EST = 3pm UTC
        ("Sunday 16:00 EST", datetime(2024, 1, 21, 21, 0, 0, tzinfo=timezone.utc)),    # Sun 4pm EST = 9pm UTC
        ("Sunday 18:00 EST", datetime(2024, 1, 21, 23, 0, 0, tzinfo=timezone.utc)),    # Sun 6pm EST = 11pm UTC
        ("Monday 09:00 EST", datetime(2024, 1, 22, 14, 0, 0, tzinfo=timezone.utc)),    # Mon 9am EST = 2pm UTC
    ]
    
    for description, test_time in test_times:
        is_weekend = test_position._is_weekend(test_time)
        print(f"{description:<20} → {'WEEKEND' if is_weekend else 'WEEKDAY'}")
    
    print(f"\nMarket Hours:")
    print(f"  • Closes: Friday 17:00 EST")
    print(f"  • Reopens: Sunday 17:05 EST")

def demo_configuration():
    """Show current weekend position configuration"""
    print("\n" + "=" * 50)
    print("WEEKEND POSITION CONFIGURATION")
    print("=" * 50)
    
    print(f"Weekend Position Limits Enabled: {config.enable_weekend_position_limits}")
    print(f"Maximum Weekend Age: {config.weekend_position_max_age_hours} hours")
    print(f"Check Interval: {config.weekend_position_check_interval} seconds ({config.weekend_position_check_interval/3600:.1f} hours)")
    print(f"Auto-Close Buffer: {config.weekend_auto_close_buffer_hours} hours")
    
    print(f"\nExample Timeline:")
    print(f"  • Position opened: Friday 16:00 EST")
    print(f"  • Weekend starts: Friday 17:00 EST")
    print(f"  • Warning threshold: {config.weekend_position_max_age_hours - config.weekend_auto_close_buffer_hours} hours ({config.weekend_position_max_age_hours - config.weekend_auto_close_buffer_hours} hours after weekend start)")
    print(f"  • Auto-close limit: {config.weekend_position_max_age_hours} hours")
    print(f"  • If not closed by Pine Script, system closes at: Sunday {17 + (config.weekend_position_max_age_hours - 48):.0f}:00 EST")

def demo_api_endpoints():
    """Show available API endpoints for weekend monitoring"""
    print("\n" + "=" * 50)  
    print("WEEKEND POSITION API ENDPOINTS")
    print("=" * 50)
    
    endpoints = [
        ("GET /api/weekend-positions", "Get summary of all weekend positions"),
        ("GET /api/weekend-positions/config", "Get weekend position configuration"),
        ("POST /api/weekend-positions/close-all", "Manually close all weekend positions"),
        ("GET /api/health", "Health check including weekend position alerts")
    ]
    
    for endpoint, description in endpoints:
        print(f"  {endpoint:<35} → {description}")

def demo_position_lifecycle():
    """Demonstrate how weekend position tracking works"""
    print("\n" + "=" * 50)
    print("WEEKEND POSITION LIFECYCLE DEMO")
    print("=" * 50)
    
    print("1. NORMAL WEEKDAY POSITION:")
    print("   • Position opened: Tuesday 10:00 EST")
    print("   • Pine Script signals exit: Tuesday 14:00 EST")
    print("   • Result: Position closed normally by Pine Script")
    print("   • Weekend age limit: NOT APPLIED ✓")
    
    print("\n2. WEEKEND POSITION SCENARIO:")
    print("   • Position opened: Friday 15:00 EST")
    print("   • Market closes: Friday 17:00 EST → Weekend tracking STARTS")
    print("   • Saturday 10:00 EST: Position age = 17 hours")
    print("   • Sunday 15:00 EST: Position age = 46 hours")
    print(f"   • Sunday 17:00 EST: Position age = 48 hours")
    print(f"   • Warning at: {config.weekend_position_max_age_hours - config.weekend_auto_close_buffer_hours} hours")
    print(f"   • Auto-close at: {config.weekend_position_max_age_hours} hours")
    print("   • Result: Position auto-closed if Pine Script hasn't closed it")
    
    print("\n3. KEY FEATURES:")
    print("   ✓ Only applies to positions open during weekends")
    print("   ✓ Weekday positions run their full Pine Script course")
    print("   ✓ Automatic monitoring every hour")
    print("   ✓ Warning notifications before auto-close")
    print("   ✓ Manual override endpoints available")
    print("   ✓ Comprehensive logging and notifications")

async def demo_weekend_position_tracking():
    """Simulate weekend position tracking"""
    print("\n" + "=" * 50)
    print("WEEKEND POSITION TRACKING SIMULATION")
    print("=" * 50)
    
    from services.position_journal import Position
    
    # Create a test position that would be opened before weekend
    position = Position(
        position_id="DEMO_GBP_USD_LONG_456",
        symbol="GBP_USD",
        action="BUY", 
        timeframe="H4",
        entry_price=1.2650,
        size=15000
    )
    
    # Simulate the position being opened on Friday
    position.open_time = datetime(2024, 1, 19, 20, 0, 0, tzinfo=timezone.utc)  # Friday 3pm EST
    
    print(f"Position: {position.position_id}")
    print(f"Symbol: {position.symbol} {position.action}")
    print(f"Opened: {position.open_time}")
    
    # Simulate weekend detection
    weekend_times = [
        datetime(2024, 1, 19, 22, 0, 0, tzinfo=timezone.utc),   # Friday 5pm EST - market close
        datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc),   # Saturday 5am EST  
        datetime(2024, 1, 21, 15, 0, 0, tzinfo=timezone.utc),   # Sunday 10am EST
        datetime(2024, 1, 21, 22, 0, 0, tzinfo=timezone.utc),   # Sunday 5pm EST - market reopen
    ]
    
    for check_time in weekend_times:
        # Manually set current time for simulation
        position.weekend_start_time = datetime(2024, 1, 19, 22, 0, 0, tzinfo=timezone.utc)  # Market close
        position.was_open_during_weekend = True
        
        # Calculate weekend age manually for demo
        if position.weekend_start_time:
            weekend_duration = (check_time - position.weekend_start_time).total_seconds() / 3600
            position.weekend_age_hours = weekend_duration
        
        status = "WEEKEND" if position._is_weekend(check_time) else "WEEKDAY"
        age_hours = position.get_weekend_age_hours()
        
        print(f"  Check time: {check_time.strftime('%a %Y-%m-%d %H:%M UTC')} → {status}")
        print(f"    Weekend age: {age_hours:.1f} hours")
        
        if age_hours >= config.weekend_position_max_age_hours:
            print(f"    🚨 WOULD AUTO-CLOSE (exceeded {config.weekend_position_max_age_hours}h limit)")
        elif age_hours >= (config.weekend_position_max_age_hours - config.weekend_auto_close_buffer_hours):
            remaining = config.weekend_position_max_age_hours - age_hours
            print(f"    ⚠️  WARNING (approaching limit, {remaining:.1f}h remaining)")
        else:
            print(f"    ✅ OK (within limits)")
        print()

def main():
    """Run all demos"""
    print("🏢 INSTITUTIONAL FX TRADING BOT")
    print("Weekend Position Age Limit System Demo")
    print("=====================================")
    
    demo_weekend_detection()
    demo_configuration()
    demo_position_lifecycle()
    
    # Run async demo
    asyncio.run(demo_weekend_position_tracking())
    
    demo_api_endpoints()
    
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print("✅ Weekend position age limits implemented")
    print("✅ Only applies to positions left open over weekends") 
    print("✅ Weekday positions unaffected")
    print("✅ Automatic monitoring and closure")
    print("✅ API endpoints for manual control")
    print("✅ Comprehensive logging and notifications")
    print("\nThe system is now protected against weekend gap risk!")

if __name__ == "__main__":
    main() 