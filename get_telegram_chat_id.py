#!/usr/bin/env python3
"""
Helper script to get your Telegram Chat ID
"""

import requests
import json

def get_chat_id():
    """Get Telegram Chat ID from bot token"""
    print("🤖 Telegram Chat ID Helper")
    print("=" * 30)
    
    # Get bot token from user
    bot_token = input("Enter your Telegram Bot Token: ").strip()
    
    if not bot_token:
        print("❌ No bot token provided")
        return
    
    try:
        # Get bot info first
        url = f"https://api.telegram.org/bot{bot_token}/getMe"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"❌ Invalid bot token. Status: {response.status_code}")
            return
        
        bot_info = response.json()
        if not bot_info['ok']:
            print(f"❌ Bot token error: {bot_info}")
            return
        
        bot_name = bot_info['result']['first_name']
        bot_username = bot_info['result']['username']
        
        print(f"✅ Connected to bot: {bot_name} (@{bot_username})")
        print()
        print("📝 Next steps:")
        print("1. Send ANY message to your bot in Telegram")
        print("2. Press Enter here to get your Chat ID")
        
        input("Press Enter after you've sent a message to your bot...")
        
        # Get updates to find chat ID
        updates_url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        updates_response = requests.get(updates_url)
        
        if updates_response.status_code != 200:
            print(f"❌ Failed to get updates: {updates_response.status_code}")
            return
        
        updates = updates_response.json()
        
        if not updates['ok'] or not updates['result']:
            print("❌ No messages found. Make sure you sent a message to your bot first!")
            return
        
        # Get the most recent message
        latest_message = updates['result'][-1]
        chat_id = latest_message['message']['chat']['id']
        user_name = latest_message['message']['from'].get('first_name', 'Unknown')
        
        print(f"✅ Found your Chat ID: {chat_id}")
        print(f"   User: {user_name}")
        print()
        print("🔧 Add this to your .env file:")
        print(f"TELEGRAM_CHAT_ID={chat_id}")
        print()
        
        # Test sending a message
        test_message = {
            "chat_id": chat_id,
            "text": "🎉 Success! Your Telegram bot is configured correctly!"
        }
        
        test_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        test_response = requests.post(test_url, json=test_message)
        
        if test_response.status_code == 200:
            print("✅ Test message sent successfully!")
            print("   Check your Telegram - you should see a success message!")
        else:
            print(f"⚠️ Test message failed: {test_response.status_code}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    get_chat_id() 