#!/usr/bin/env python3
"""
Quick Fixes for Trading Bot Issues
Automatically resolves common problems that prevent trade execution
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class TradingBotQuickFix:
    def __init__(self):
        self.fixes_applied = []

    async def apply_all_fixes(self):
        """Apply all quick fixes"""
        print("🔧 TRADING BOT QUICK FIXES")
        print("=" * 50)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        await self.fix_environment_variables()
        await self.fix_market_hours_check()
        await self.fix_connection_issues()
        await self.fix_position_sizing()
        await self.create_missing_directories()
        
        self.print_summary()

    async def fix_environment_variables(self):
        """Check and fix common environment variable issues"""
        print("🌍 ENVIRONMENT VARIABLES FIX")
        print("-" * 30)
        
        # Check if .env file exists
        if not os.path.exists('.env'):
            print("   ⚠️  .env file not found - creating template")
            await self.create_env_template()
            self.fixes_applied.append("Created .env template file")
        else:
            print("   ✅ .env file exists")
        
        # Check critical environment variables
        critical_vars = [
            'OANDA_ACCOUNT_ID',
            'OANDA_ACCESS_TOKEN', 
            'OANDA_ENVIRONMENT'
        ]
        
        missing_vars = []
        for var in critical_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"   ⚠️  Missing variables: {', '.join(missing_vars)}")
            print("   💡 Update your .env file with these values")
        else:
            print("   ✅ All critical variables present")
        
        print()

    async def create_env_template(self):
        """Create a template .env file"""
        template = """# OANDA Configuration
OANDA_ACCOUNT_ID=your_account_id_here
OANDA_ACCESS_TOKEN=your_access_token_here
OANDA_ENVIRONMENT=practice

# Multi-Account Settings (comma-separated)
MULTI_ACCOUNTS=101-003-26651494-006,101-003-26651494-011
ENABLE_MULTI_ACCOUNT_TRADING=true

# Risk Management
DEFAULT_RISK_PERCENTAGE=15.0
MAX_RISK_PERCENTAGE=20.0
ALLOCATION_PERCENT=15.0

# Database (optional)
DATABASE_URL=

# Notifications (optional)
SLACK_WEBHOOK_URL=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Security (optional)
WEBHOOK_SECRET=
JWT_SECRET=
"""
        
        with open('.env', 'w') as f:
            f.write(template)

    async def fix_market_hours_check(self):
        """Fix market hours validation to be less restrictive"""
        print("🕐 MARKET HOURS FIX")
        print("-" * 30)
        
        try:
            # Read current utils.py
            with open('utils.py', 'r') as f:
                content = f.read()
            
            # Check if we need to add a bypass for testing
            if 'TRADING_BYPASS_MARKET_HOURS' not in content:
                print("   🔧 Adding market hours bypass option")
                
                bypass_code = '''
# Emergency bypass for market hours (for testing)
TRADING_BYPASS_MARKET_HOURS = os.getenv('TRADING_BYPASS_MARKET_HOURS', 'false').lower() == 'true'

def is_instrument_tradeable_original(symbol: str) -> Tuple[bool, str]:
    """Original market hours check - renamed"""'''
                
                # Insert the bypass at the beginning of is_instrument_tradeable function
                content = content.replace(
                    'def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:',
                    bypass_code + '\n    pass\n\ndef is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:\n    """Check if instrument is tradeable with optional bypass"""\n    \n    # Emergency bypass for testing\n    if TRADING_BYPASS_MARKET_HOURS:\n        logger.warning(f"BYPASS: Market hours check bypassed for {symbol}")\n        return True, "Market hours bypassed for testing"\n    \n    return is_instrument_tradeable_original(symbol)'
                )
                
                with open('utils.py', 'w') as f:
                    f.write(content)
                
                self.fixes_applied.append("Added market hours bypass option")
                print("   ✅ Market hours bypass added")
                print("   💡 Set TRADING_BYPASS_MARKET_HOURS=true in .env to bypass market hours")
            else:
                print("   ✅ Market hours bypass already available")
        
        except Exception as e:
            print(f"   ❌ Failed to add market hours bypass: {str(e)}")
        
        print()

    async def fix_connection_issues(self):
        """Fix common OANDA connection issues"""
        print("🔌 CONNECTION FIXES")
        print("-" * 30)
        
        try:
            # Test connection reinitializer
            sys.path.append('.')
            
            # Check if we can import required modules
            try:
                from main import initialize_oanda_client, connection_manager
                
                # Force reinitialize OANDA client
                print("   🔄 Reinitializing OANDA client...")
                success = initialize_oanda_client()
                
                if success:
                    print("   ✅ OANDA client reinitialized successfully")
                    self.fixes_applied.append("Reinitialized OANDA client")
                else:
                    print("   ❌ OANDA client initialization failed")
                    
            except ImportError as e:
                print(f"   ⚠️  Cannot reinitialize (system not running): {str(e)}")
                
        except Exception as e:
            print(f"   ❌ Connection fix failed: {str(e)}")
        
        print()

    async def fix_position_sizing(self):
        """Fix position sizing configuration issues"""
        print("📏 POSITION SIZING FIX")
        print("-" * 30)
        
        try:
            # Check if USD_THB is in leverage configuration
            with open('utils.py', 'r') as f:
                content = f.read()
            
            if '"USD_THB"' not in content:
                print("   🔧 Adding missing currency pairs to leverage config")
                
                # Add missing pairs to INSTRUMENT_LEVERAGES
                missing_pairs = [
                    '"USD_THB": 30,',
                    '"CHF_JPY": 30,',
                    '"NZD_JPY": 30,',
                    '"CAD_JPY": 30,'
                ]
                
                # Find the INSTRUMENT_LEVERAGES dictionary and add missing pairs
                lines = content.split('\n')
                new_lines = []
                in_leverages = False
                
                for line in lines:
                    if 'INSTRUMENT_LEVERAGES = {' in line:
                        in_leverages = True
                        new_lines.append(line)
                    elif in_leverages and '}' in line and 'default' in line:
                        # Insert missing pairs before the default entry
                        for pair in missing_pairs:
                            if pair.split(':')[0].strip().strip('"') not in content:
                                new_lines.append(f'    {pair}')
                        new_lines.append(line)
                        in_leverages = False
                    else:
                        new_lines.append(line)
                
                with open('utils.py', 'w') as f:
                    f.write('\n'.join(new_lines))
                
                self.fixes_applied.append("Added missing currency pairs to leverage config")
                print("   ✅ Missing currency pairs added")
            else:
                print("   ✅ Currency pairs configuration is complete")
                
        except Exception as e:
            print(f"   ❌ Position sizing fix failed: {str(e)}")
        
        print()

    async def create_missing_directories(self):
        """Create missing directories that the system needs"""
        print("📁 DIRECTORY STRUCTURE FIX")
        print("-" * 30)
        
        required_dirs = [
            'logs',
            'backups', 
            'data',
            'static'
        ]
        
        created_dirs = []
        for directory in required_dirs:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                created_dirs.append(directory)
                print(f"   ✅ Created directory: {directory}")
        
        if created_dirs:
            self.fixes_applied.append(f"Created missing directories: {', '.join(created_dirs)}")
        else:
            print("   ✅ All required directories exist")
        
        print()

    def print_summary(self):
        """Print summary of fixes applied"""
        print("📋 QUICK FIXES SUMMARY")
        print("=" * 50)
        
        if not self.fixes_applied:
            print("✅ No fixes needed - system appears to be configured correctly")
            print()
            print("If you're still having issues:")
            print("1. Run the diagnostic tool: python diagnostic_tool.py")
            print("2. Check if the trading system is running: python main.py")
            print("3. Verify TradingView is sending webhooks to your endpoint")
            return
        
        print(f"Applied {len(self.fixes_applied)} fixes:")
        print()
        
        for i, fix in enumerate(self.fixes_applied, 1):
            print(f"{i}. {fix}")
        
        print()
        print("🔄 NEXT STEPS:")
        print("1. Update your .env file with real OANDA credentials")
        print("2. Restart the trading system: python main.py")
        print("3. Run diagnostic: python diagnostic_tool.py")
        print("4. Test with a TradingView webhook")

async def main():
    """Run the quick fix tool"""
    fixer = TradingBotQuickFix()
    await fixer.apply_all_fixes()

if __name__ == "__main__":
    asyncio.run(main()) 