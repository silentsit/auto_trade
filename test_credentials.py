#!/usr/bin/env python3
"""
Test script to verify OANDA and Telegram credentials
"""

import os
import requests
import oandapyV20
from oandapyV20.endpoints.accounts import AccountDetails
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_oanda_credentials():
    """Test OANDA API connection"""
    print("🏦 Testing OANDA credentials...")
    
    account_id = os.getenv('OANDA_ACCOUNT_ID')
    access_token = os.getenv('OANDA_ACCESS_TOKEN') 
    environment = os.getenv('OANDA_ENVIRONMENT', 'practice')
    
    if not account_id or not access_token:
        print("❌ OANDA credentials not found in environment variables")
        return False
    
    try:
        # Initialize OANDA client
        client = oandapyV20.API(
            access_token=access_token,
            environment=environment
        )
        
        # Test account details request
        request = AccountDetails(accountID=account_id)
        response = client.request(request)
        
        if 'account' in response:
            balance = response['account']['balance']
            currency = response['account']['currency']
            print(f"✅ OANDA connection successful!")
            print(f"   Account ID: {account_id}")
            print(f"   Environment: {environment}")
            print(f"   Balance: {balance} {currency}")
            return True
        else:
            print("❌ OANDA API returned unexpected response")
            return False
            
    except Exception as e:
        print(f"❌ OANDA connection failed: {e}")
        return False

def test_telegram_credentials():
    """Test Telegram bot connection"""
    print("\n🤖 Testing Telegram credentials...")
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ Telegram credentials not found in environment variables")
        return False
    
    try:
        # Test bot info
        url = f"https://api.telegram.org/bot{bot_token}/getMe"
        response = requests.get(url)
        
        if response.status_code == 200:
            bot_info = response.json()
            if bot_info['ok']:
                bot_name = bot_info['result']['first_name']
                bot_username = bot_info['result']['username']
                print(f"✅ Telegram bot connection successful!")
                print(f"   Bot Name: {bot_name}")
                print(f"   Bot Username: @{bot_username}")
                
                # Test sending a message
                message_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                test_message = {
                    "chat_id": chat_id,
                    "text": "🧪 Test message from your trading bot - credentials are working!"
                }
                
                msg_response = requests.post(message_url, json=test_message)
                if msg_response.status_code == 200:
                    print(f"✅ Test message sent successfully to chat ID: {chat_id}")
                    return True
                else:
                    print(f"❌ Failed to send test message: {msg_response.status_code}")
                    print(f"   Response: {msg_response.text}")
                    return False
            else:
                print(f"❌ Telegram API error: {bot_info}")
                return False
        else:
            print(f"❌ Telegram API request failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Telegram connection failed: {e}")
        return False

def test_database_connection():
    """Test database connection"""
    print("\n🗄️ Testing database connection...")
    
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("❌ DATABASE_URL not found in environment variables")
        return False
    
    try:
        import asyncpg
        import asyncio
        
        async def test_db():
            conn = await asyncpg.connect(database_url)
            result = await conn.fetchval('SELECT version()')
            await conn.close()
            return result
        
        version = asyncio.run(test_db())
        print(f"✅ Database connection successful!")
        print(f"   PostgreSQL version: {version}")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def main():
    """Run all credential tests"""
    print("🧪 Testing Trading Bot Credentials")
    print("=" * 40)
    
    results = []
    results.append(test_oanda_credentials())
    results.append(test_telegram_credentials())
    results.append(test_database_connection())
    
    print("\n" + "=" * 40)
    print("📊 Test Results Summary:")
    
    if all(results):
        print("✅ All credentials are working correctly!")
        print("🚀 Your trading bot is ready to deploy!")
    else:
        print("❌ Some credentials need to be fixed:")
        if not results[0]:
            print("   - Fix OANDA credentials")
        if not results[1]:
            print("   - Fix Telegram credentials")
        if not results[2]:
            print("   - Fix database connection")

if __name__ == "__main__":
    main() 