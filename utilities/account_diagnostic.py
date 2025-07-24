#!/usr/bin/env python3
"""
Account Diagnostic Tool - Compare OANDA accounts to identify margin issues
"""

import asyncio
import logging
import oandapyV20
from oandapyV20.endpoints.accounts import AccountDetails, AccountInstruments
from oandapyV20.endpoints.positions import OpenPositions
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AccountDiagnostic:
    def __init__(self):
        access_token = config.oanda_access_token
        if hasattr(access_token, 'get_secret_value'):
            access_token = access_token.get_secret_value()
        
        self.oanda = oandapyV20.API(
            access_token=access_token,
            environment=config.oanda_environment
        )
        
        self.accounts = [
            "101-003-26651494-011",
            "101-003-26651494-006"
        ]

    def make_request(self, request):
        """Make OANDA API request with error handling"""
        try:
            return self.oanda.request(request)
        except Exception as e:
            logger.error(f"OANDA request failed: {e}")
            return None

    def analyze_account(self, account_id):
        """Analyze individual account"""
        print(f"\n{'='*50}")
        print(f"ACCOUNT: {account_id}")
        print(f"{'='*50}")
        
        # Get account details
        account_request = AccountDetails(accountID=account_id)
        account_data = self.make_request(account_request)
        
        if not account_data:
            print("❌ Failed to fetch account data")
            return
            
        account = account_data.get('account', {})
        
        # Basic account info
        balance = float(account.get('balance', 0))
        margin_used = float(account.get('marginUsed', 0))
        margin_available = float(account.get('marginAvailable', 0))
        margin_rate = float(account.get('marginRate', 0))
        
        print(f"💰 Balance: ${balance:,.2f}")
        print(f"📊 Margin Used: ${margin_used:,.2f}")
        print(f"✅ Margin Available: ${margin_available:,.2f}")
        print(f"📈 Margin Rate: {margin_rate:.4f}")
        print(f"🎯 Margin Utilization: {(margin_used/balance*100):.1f}%" if balance > 0 else "N/A")
        
        # Get open positions
        positions_request = OpenPositions(accountID=account_id)
        positions_data = self.make_request(positions_request)
        
        if positions_data:
            positions = positions_data.get('positions', [])
            print(f"\n📈 Open Positions: {len(positions)}")
            
            total_position_value = 0
            for pos in positions:
                instrument = pos.get('instrument')
                long_units = float(pos.get('long', {}).get('units', 0))
                short_units = float(pos.get('short', {}).get('units', 0))
                
                if long_units != 0 or short_units != 0:
                    print(f"   • {instrument}: Long={long_units}, Short={short_units}")
                    
                    # Estimate position value (simplified)
                    net_units = long_units + short_units
                    if net_units != 0:
                        avg_price = 1.0  # Simplified - would need current price
                        position_value = abs(net_units) * avg_price
                        total_position_value += position_value
            
            print(f"📊 Estimated Total Position Value: ${total_position_value:,.2f}")
        
        # Check recent transactions (limit to last 5)
        # Note: Would need TransactionRange endpoint for this
        
        return {
            'account_id': account_id,
            'balance': balance,
            'margin_used': margin_used,
            'margin_available': margin_available,
            'margin_rate': margin_rate,
            'open_positions_count': len(positions) if positions_data else 0,
            'margin_utilization': (margin_used/balance*100) if balance > 0 else 0
        }

    def compare_accounts(self):
        """Compare both accounts and identify issues"""
        print(f"\n🔍 ACCOUNT COMPARISON ANALYSIS")
        print(f"{'='*60}")
        
        results = []
        for account_id in self.accounts:
            result = self.analyze_account(account_id)
            if result:
                results.append(result)
        
        if len(results) == 2:
            print(f"\n📊 COMPARISON SUMMARY:")
            print(f"{'Metric':<20} {'Account 011':<15} {'Account 006':<15} {'Difference':<15}")
            print(f"{'-'*65}")
            
            acc_011 = results[0] if results[0]['account_id'].endswith('011') else results[1]
            acc_006 = results[1] if results[1]['account_id'].endswith('006') else results[0]
            
            # Compare key metrics
            balance_diff = acc_011['balance'] - acc_006['balance']
            margin_diff = acc_011['margin_available'] - acc_006['margin_available']
            util_diff = acc_011['margin_utilization'] - acc_006['margin_utilization']
            
            print(f"{'Balance':<20} ${acc_011['balance']:>10,.2f} ${acc_006['balance']:>10,.2f} ${balance_diff:>10,.2f}")
            print(f"{'Margin Available':<20} ${acc_011['margin_available']:>10,.2f} ${acc_006['margin_available']:>10,.2f} ${margin_diff:>10,.2f}")
            print(f"{'Margin Used %':<20} {acc_011['margin_utilization']:>10.1f}% {acc_006['margin_utilization']:>10.1f}% {util_diff:>10.1f}%")
            print(f"{'Open Positions':<20} {acc_011['open_positions_count']:>14} {acc_006['open_positions_count']:>14} {acc_011['open_positions_count']-acc_006['open_positions_count']:>14}")
            
            # Analysis
            print(f"\n🔍 ANALYSIS:")
            if margin_diff < -1000:  # Account 011 has significantly less available margin
                print(f"⚠️  Account 011 has ${abs(margin_diff):,.2f} LESS available margin than 006")
                print(f"   This explains why trades fail on 011 but succeed on 006")
                
            if acc_011['margin_utilization'] > acc_006['margin_utilization'] + 10:
                print(f"⚠️  Account 011 has {util_diff:.1f}% higher margin utilization")
                print(f"   This suggests 011 has more open positions consuming margin")
                
            if acc_011['open_positions_count'] > acc_006['open_positions_count']:
                pos_diff = acc_011['open_positions_count'] - acc_006['open_positions_count']
                print(f"⚠️  Account 011 has {pos_diff} more open positions than 006")

    def get_margin_recommendations(self):
        """Provide recommendations to fix margin issues"""
        print(f"\n💡 RECOMMENDATIONS:")
        print(f"{'='*50}")
        
        print(f"1. 🎯 Reduce Position Sizes:")
        print(f"   - Lower risk_percent from 10% to 5% for account 011")
        print(f"   - This will halve the position sizes and margin requirements")
        
        print(f"\n2. 📊 Close Some Positions:")
        print(f"   - Account 011 may have too many open positions")
        print(f"   - Consider closing losing or marginal positions")
        
        print(f"\n3. 💰 Add Funds:")
        print(f"   - Deposit additional funds to account 011")
        print(f"   - This will increase available margin")
        
        print(f"\n4. ⚙️  Adjust Risk Management:")
        print(f"   - Implement position size scaling based on available margin")
        print(f"   - Add pre-trade margin checks")

async def main():
    diagnostic = AccountDiagnostic()
    diagnostic.compare_accounts()
    diagnostic.get_margin_recommendations()

if __name__ == "__main__":
    asyncio.run(main()) 