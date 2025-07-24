#!/usr/bin/env python3
"""
Enhanced Trading Bot Startup Script
Ensures proper logging, diagnostics, and system health before starting
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime
import subprocess

# Configure enhanced logging
def setup_logging():
    """Setup comprehensive logging system"""
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create timestamped log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/trading_bot_{timestamp}.log'
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode='w'),
            logging.FileHandler('logs/trading_bot_latest.log', mode='w')  # Always overwrite latest
        ],
    )
    
    logger = logging.getLogger("startup")
    logger.info(f"Logging initialized - Log file: {log_file}")
    return logger

async def run_diagnostics():
    """Run pre-startup diagnostics"""
    print("🔍 Running pre-startup diagnostics...")
    
    try:
        # Import and run diagnostic tool
        from diagnostic_tool import TradingBotDiagnostic
        
        diagnostic = TradingBotDiagnostic()
        await diagnostic.diagnose_all()
        
        # Check if critical issues were found
        critical_issues = [
            issue for issue in diagnostic.issues_found 
            if any(keyword in issue.lower() for keyword in ['offline', 'configuration', 'oanda'])
        ]
        
        if critical_issues:
            print("\n❌ CRITICAL ISSUES FOUND:")
            for issue in critical_issues:
                print(f"   - {issue}")
            print("\n🔧 Run 'python quick_fixes.py' to attempt automatic fixes")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Diagnostic failed: {str(e)}")
        return False

async def check_dependencies():
    """Check that all required dependencies are available"""
    print("📦 Checking dependencies...")
    
    required_modules = [
        'fastapi',
        'oandapyV20', 
        'aiohttp',
        'pydantic',
        'asyncio'
    ]
    
    missing_modules = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"❌ Missing modules: {', '.join(missing_modules)}")
        print("💡 Run: pip install -r requirements.txt")
        return False
    
    print("✅ All dependencies available")
    return True

async def check_environment():
    """Check environment configuration"""
    print("🌍 Checking environment...")
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("❌ .env file not found")
        print("💡 Run 'python quick_fixes.py' to create template")
        return False
    
    # Check critical environment variables
    critical_vars = ['OANDA_ACCOUNT_ID', 'OANDA_ACCESS_TOKEN']
    missing_vars = []
    
    # Load .env manually
    with open('.env', 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                if key in critical_vars and value and value != 'your_account_id_here':
                    critical_vars.remove(key)
    
    if critical_vars:
        print(f"❌ Missing/invalid environment variables: {', '.join(critical_vars)}")
        print("💡 Update your .env file with real OANDA credentials")
        return False
    
    print("✅ Environment configuration valid")
    return True

def start_trading_system():
    """Start the trading system with proper monitoring"""
    print("🚀 Starting trading system...")
    
    try:
        # Import main system
        import main
        
        # Start the system
        import uvicorn
        
        print("✅ Trading system starting on http://localhost:8000")
        print("📊 API documentation: http://localhost:8000/api/docs")
        print("🔍 Real-time logs: tail -f logs/trading_bot_latest.log")
        print()
        print("Press Ctrl+C to stop the system")
        
        # Start with uvicorn
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=False,  # Disable reload in production
            log_level="info",
            access_log=True
        )
        
    except KeyboardInterrupt:
        print("\n👋 Trading system stopped by user")
    except Exception as e:
        print(f"❌ Failed to start trading system: {str(e)}")
        return False
    
    return True

async def main():
    """Main startup sequence"""
    print("=" * 60)
    print("🤖 ENHANCED TRADING BOT STARTUP")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Setup logging first
    logger = setup_logging()
    logger.info("Enhanced startup sequence initiated")
    
    # Run checks in sequence
    checks = [
        ("Dependencies", check_dependencies()),
        ("Environment", check_environment()),
        ("System Diagnostics", run_diagnostics())
    ]
    
    for check_name, check_coro in checks:
        print(f"Running {check_name} check...")
        try:
            result = await check_coro
            if not result:
                print(f"\n❌ {check_name} check failed - startup aborted")
                print("🔧 Run fixes and try again:")
                print("   python quick_fixes.py")
                print("   python diagnostic_tool.py")
                return False
        except Exception as e:
            print(f"❌ {check_name} check error: {str(e)}")
            return False
        
        print(f"✅ {check_name} check passed")
        print()
    
    print("🎉 All checks passed - starting trading system!")
    print()
    
    # Start the trading system
    success = start_trading_system()
    
    if success:
        logger.info("Trading system startup completed successfully")
    else:
        logger.error("Trading system startup failed")
        return False

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Startup cancelled by user")
    except Exception as e:
        print(f"\n❌ Startup failed: {str(e)}")
        sys.exit(1) 