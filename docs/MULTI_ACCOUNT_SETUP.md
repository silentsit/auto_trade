# Multi-Account Trading Setup Guide

## Overview

Your trading system now supports targeting multiple OANDA accounts from a single TradingView signal. This allows you to run 2 bots on different demo accounts while sharing the same signal source.

## ✅ What's Already Working

Your system already has complete multi-account support implemented. Here's what's been done:

### 1. **Multi-Account Signal Processing**
- ✅ Detects multiple account configurations in signals
- ✅ Routes signals to appropriate account execution methods
- ✅ Handles both single and multi-account scenarios seamlessly

### 2. **Account-Specific Operations**
- ✅ Account-specific balance fetching
- ✅ Account-specific order placement
- ✅ Account-specific position tracking
- ✅ Unique position IDs per account

### 3. **Configuration Support**
- ✅ Multi-account configuration in `config.py`
- ✅ Broadcast mode for all configured accounts
- ✅ Flexible account targeting

## 🎯 How to Use Multi-Account Signals

### Option 1: Accounts Array (Recommended)

Use an array of account IDs to target specific accounts:

```json
{
  "symbol": "{{ticker}}",
  "action": "BUY",
  "alert_id": "{{ticker}}_{{interval}}_{{time}}",
  "position_id": "{{ticker}}_{{interval}}_{{time}}",
  "exchange": "OANDA",
  "accounts": [
    "101-003-26651494-006",
    "101-003-26651494-011"
  ],
  "percentage": 10,
  "risk_percent": 10,
  "orderType": "MARKET",
  "timeInForce": "FOK",
  "comment": "Open Long Signal",
  "strategy": "Lorentzian_Classification",
  "timestamp": "{{time}}",
  "timeframe": "{{interval}}"
}
```

### Option 2: Broadcast Mode

Use broadcast mode to target all configured accounts:

```json
{
  "symbol": "{{ticker}}",
  "action": "BUY",
  "alert_id": "{{ticker}}_{{interval}}_{{time}}",
  "position_id": "{{ticker}}_{{interval}}_{{time}}",
  "exchange": "OANDA",
  "account": "BROADCAST",
  "percentage": 10,
  "risk_percent": 10,
  "orderType": "MARKET",
  "timeInForce": "FOK",
  "comment": "Open Long Signal",
  "strategy": "Lorentzian_Classification",
  "timestamp": "{{time}}",
  "timeframe": "{{interval}}"
}
```

### Option 3: Single Account (Original)

Your original format still works for single accounts:

```json
{
  "symbol": "{{ticker}}",
  "action": "BUY",
  "alert_id": "{{ticker}}_{{interval}}_{{time}}",
  "position_id": "{{ticker}}_{{interval}}_{{time}}",
  "exchange": "OANDA",
  "account": "101-003-26651494-011",
  "percentage": 10,
  "risk_percent": 10,
  "orderType": "MARKET",
  "timeInForce": "FOK",
  "comment": "Open Long Signal",
  "strategy": "Lorentzian_Classification",
  "timestamp": "{{time}}",
  "timeframe": "{{interval}}"
}
```

## 🔧 Configuration

### Multi-Account Settings in `config.py`

```python
# Multi-Account Configuration
multi_accounts: list = Field(default=[
    "101-003-26651494-006",
    "101-003-26651494-011"
])
enable_multi_account_trading: bool = Field(default=True)
```

### Environment Variables

Add your additional account IDs to your `.env` file:

```env
# Primary account (existing)
OANDA_ACCOUNT_ID=101-003-26651494-011

# Additional accounts for multi-account trading
MULTI_ACCOUNTS=101-003-26651494-006,101-003-26651494-011
```

## 🧪 Testing Multi-Account Functionality

### Run the Test Suite

```bash
python utilities/test_multi_account_signals.py
```

This will test:
- ✅ Accounts array method
- ✅ Broadcast method  
- ✅ Single account method (control)
- ✅ Multi-account close signals

### Manual Testing

You can also test via the webhook endpoint:

```bash
curl -X POST http://localhost:8000/tradingview \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "EUR_USD",
    "action": "BUY",
    "accounts": ["101-003-26651494-006", "101-003-26651494-011"],
    "percentage": 10,
    "risk_percent": 10,
    "orderType": "MARKET",
    "timeInForce": "FOK",
    "comment": "Multi-Account Test"
  }'
```

## 📊 Multi-Account Response Format

When using multi-account signals, the response includes detailed execution information:

```json
{
  "status": "ok",
  "result": {
    "multi_account_execution": true,
    "total_accounts": 2,
    "successful_accounts": 2,
    "success_rate": 1.0,
    "successful_account_ids": [
      "101-003-26651494-006",
      "101-003-26651494-011"
    ],
    "detailed_results": {
      "101-003-26651494-006": {
        "success": true,
        "result": {
          "success": true,
          "fill_price": 1.0845,
          "units": 9654,
          "transaction_id": "12345",
          "account_id": "101-003-26651494-006",
          "position_id": "EUR_USD_H1_20241201_120000_101-003-26651494-006"
        }
      },
      "101-003-26651494-011": {
        "success": true,
        "result": {
          "success": true,
          "fill_price": 1.0845,
          "units": 9654,
          "transaction_id": "12346",
          "account_id": "101-003-26651494-011",
          "position_id": "EUR_USD_H1_20241201_120000_101-003-26651494-011"
        }
      }
    }
  }
}
```

## 🔍 Position Tracking

### Account-Specific Position IDs

Each account gets unique position IDs:
- Account 006: `EUR_USD_H1_20241201_120000_101-003-26651494-006`
- Account 011: `EUR_USD_H1_20241201_120000_101-003-26651494-011`

### Viewing Multi-Account Positions

```bash
# View all positions across all accounts
curl http://localhost:8000/api/positions

# View positions for specific account
curl http://localhost:8000/api/positions?account=101-003-26651494-006
```

## 🚨 Close Signals

Multi-account close signals work the same way:

```json
{
  "symbol": "EUR_USD",
  "action": "CLOSE",
  "accounts": [
    "101-003-26651494-006",
    "101-003-26651494-011"
  ],
  "comment": "Close All Positions"
}
```

## 🛠️ Troubleshooting

### Common Issues

1. **Signal not detected as multi-account**
   - Ensure you're using `"accounts"` (plural) for the array
   - Check that the array contains more than one account

2. **Account authentication errors**
   - Verify all account IDs are correct
   - Ensure your OANDA token has access to all accounts

3. **Position tracking issues**
   - Check logs for account-specific position IDs
   - Verify positions are being created with unique IDs per account

### Debug Endpoints

```bash
# Test multi-account signal processing
curl -X POST http://localhost:8000/debug/test-multi-account \
  -H "Content-Type: application/json" \
  -d '{"accounts": ["101-003-26651494-006", "101-003-26651494-011"]}'

# View current account configurations
curl http://localhost:8000/api/config/accounts
```

## 📋 Migration Checklist

- [x] ✅ **Multi-account signal processing** - Already implemented
- [x] ✅ **Account-specific balance fetching** - Already implemented  
- [x] ✅ **Account-specific order placement** - Already implemented
- [x] ✅ **Position tracking per account** - Already implemented
- [x] ✅ **Configuration support** - Already implemented
- [x] ✅ **Test suite** - Created and ready
- [x] ✅ **Documentation** - This guide

## 🎉 Ready to Use!

Your multi-account system is **fully operational**. Simply update your TradingView alert JSON to use one of the multi-account formats above, and your signals will automatically be executed on both accounts.

### Quick Start

1. **Update your TradingView alert JSON** to use the `accounts` array format
2. **Test with the provided test script** to verify functionality
3. **Monitor execution** through the API endpoints and logs

Your 2 bots can now share the same TradingView signal source! 🚀 