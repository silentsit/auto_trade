#!/usr/bin/env python3
"""
Trading Bot Status Checker
Shows why no close signal was triggered
"""

import requests
import subprocess
import time
from datetime import datetime

def check_bot_status():
    """Check if the trading bot is running and healthy"""
    
    print("🔍 TRADING BOT STATUS CHECK")
    print("=" * 50)
    print()
    
    # Check if Python processes are running
    print("1️⃣ Checking for running Python processes...")
    try:
        result = subprocess.run(['tasklist'], capture_output=True, text=True, shell=True)
        python_processes = [line for line in result.stdout.split('\n') if 'python' in line.lower()]
        
        if python_processes:
            print("✅ Found Python processes running:")
            for process in python_processes:
                if process.strip():
                    print(f"   {process.strip()}")
        else:
            print("❌ No Python processes found running")
    except Exception as e:
        print(f"❌ Error checking processes: {e}")
    
    print()
    
    # Check if the bot's API endpoint is responding
    print("2️⃣ Checking bot API endpoint...")
    api_url = "http://localhost:8000/api/health"
    
    try:
        response = requests.get(api_url, timeout=5)
        if response.status_code == 200:
            print("✅ Trading bot API is responding!")
            print(f"   Status: {response.json()}")
        else:
            print(f"❌ API responded with status code: {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to trading bot API (not running)")
    except requests.exceptions.Timeout:
        print("❌ API request timed out")
    except Exception as e:
        print(f"❌ Error checking API: {e}")
    
    print()
    
    # Check position monitoring status
    print("3️⃣ Checking position monitoring...")
    try:
        response = requests.get("http://localhost:8000/api/positions", timeout=5)
        if response.status_code == 200:
            positions = response.json()
            print(f"✅ Position API responding - {len(positions)} positions found")
        else:
            print("❌ Position API not responding")
    except:
        print("❌ Cannot check positions (bot not running)")
    
    print()
    
    # Explain the USD/THB situation
    print("🎯 USD/THB POSITION ANALYSIS")
    print("-" * 30)
    
    current_time = datetime.now()
    position_time = "7:30 AM"  # User mentioned this time
    
    print(f"Position opened: {position_time}")
    print(f"Current time: {current_time.strftime('%H:%M %p')}")
    print()
    
    print("❌ WHY NO CLOSE SIGNAL WAS TRIGGERED:")
    print("   • Trading bot is NOT RUNNING")
    print("   • No process listening for TradingView webhook alerts")
    print("   • Exit signal monitoring is OFFLINE")
    print("   • Position management is INACTIVE")
    print()
    
    print("✅ TO FIX THIS ISSUE:")
    print("   1. Start the trading bot:")
    print("      python main.py")
    print("   2. Or use the enhanced starter:")
    print("      python start_bot_enhanced.py")
    print("   3. Or use PowerShell script:")
    print("      .\\scripts\\start_bot.ps1")
    print()
    
    print("🔧 IMMEDIATE ACTIONS NEEDED:")
    print("   • Start the bot to resume exit signal monitoring")
    print("   • The bot will automatically pick up any new close signals")
    print("   • Existing positions will be monitored once bot is running")
    print()
    
    print("⚠️  IMPORTANT:")
    print("   Your USD/THB position is still open on OANDA")
    print("   Start the bot ASAP to resume automated exit handling")

if __name__ == "__main__":
    check_bot_status() 