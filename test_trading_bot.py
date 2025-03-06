# test_trading_bot.py

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from pytz import timezone
import json
import time
from trading_bot import (
    app, RetryableAlert, check_spread_warning, is_market_open, 
    calculate_next_market_open, failed_alerts_queue, process_single_alert,
    SPREAD_THRESHOLD_FOREX, SPREAD_THRESHOLD_CRYPTO
)

class TestTradingBot(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.bangkok_tz = timezone('Asia/Bangkok')
        # Clear queue before each test
        while failed_alerts_queue:
            failed_alerts_queue.popleft()
    
    def test_is_market_open(self):
        # Test Friday near market close (2 AM to 4 AM Bangkok time)
        with patch('trading_bot.datetime') as mock_datetime:
            mock_dt = datetime(2025, 1, 10, 3, 30, tzinfo=self.bangkok_tz)  # Friday 3:30 AM
            mock_datetime.now.return_value = mock_dt
            is_open, reason = is_market_open()
            self.assertTrue(is_open)
            self.assertEqual(reason, "Regular trading hours")

        # Test Friday after market close (after 4 AM Bangkok time)
        with patch('trading_bot.datetime') as mock_datetime:
            mock_dt = datetime(2025, 1, 10, 4, 1, tzinfo=self.bangkok_tz)  # Friday 4:01 AM
            mock_datetime.now.return_value = mock_dt
            is_open, reason = is_market_open()
            self.assertTrue(is_open)  # Should still be open after 4 AM
            self.assertEqual(reason, "Regular trading hours")

        # Test Sunday approaching market open (3:59 AM Bangkok time)
        with patch('trading_bot.datetime') as mock_datetime:
            mock_dt = datetime(2025, 1, 12, 3, 59, tzinfo=self.bangkok_tz)  # Sunday 3:59 AM
            mock_datetime.now.return_value = mock_dt
            is_open, reason = is_market_open()
            self.assertFalse(is_open)
            self.assertEqual(reason, "Weekend market closure")

        # Test Sunday just after market opens (4:01 AM Bangkok time)
        with patch('trading_bot.datetime') as mock_datetime:
            mock_dt = datetime(2025, 1, 12, 4, 1, tzinfo=self.bangkok_tz)  # Sunday 4:01 AM
            mock_datetime.now.return_value = mock_dt
            is_open, reason = is_market_open()
            self.assertTrue(is_open)
            self.assertEqual(reason, "Market open after weekend")

    def test_spread_warning_forex(self):
        # Test forex spread warning
        forex_data = {
            'prices': [{
                'bids': [{'price': '1.2000'}],
                'asks': [{'price': '1.2015'}]  # 0.125% spread
            }]
        }
        has_warning, _ = check_spread_warning(forex_data, 'EUR_USD')
        self.assertTrue(has_warning)  # Should trigger warning as > 0.1%

        # Test acceptable forex spread
        forex_data = {
            'prices': [{
                'bids': [{'price': '1.2000'}],
                'asks': [{'price': '1.2010'}]  # 0.083% spread
            }]
        }
        has_warning, _ = check_spread_warning(forex_data, 'EUR_USD')
        self.assertFalse(has_warning)  # Should not trigger warning

    def test_spread_warning_crypto(self):
        # Test crypto spread warning
        crypto_data = {
            'prices': [{
                'bids': [{'price': '40000.00'}],
                'asks': [{'price': '40400.00'}]  # 1% spread
            }]
        }
        has_warning, _ = check_spread_warning(crypto_data, 'BTC_USD')
        self.assertTrue(has_warning)  # Should trigger warning as > 0.8%

        # Test acceptable crypto spread
        crypto_data = {
            'prices': [{
                'bids': [{'price': '40000.00'}],
                'asks': [{'price': '40200.00'}]  # 0.5% spread
            }]
        }
        has_warning, _ = check_spread_warning(crypto_data, 'BTC_USD')
        self.assertFalse(has_warning)  # Should not trigger warning

    def test_retryable_alert(self):
        alert = RetryableAlert({'test': 'data'})
        self.assertTrue(alert.should_retry())
        
        # Test retry count limit
        next_retry = None
        for i in range(3):
            next_retry = alert.schedule_next_retry()
            self.assertIsNotNone(next_retry)
            self.assertTrue(next_retry > time.time())
        
        # Should not allow more retries
        self.assertFalse(alert.should_retry())
        self.assertIsNone(alert.schedule_next_retry())

    def test_health_check(self):
        response = self.app.get('/health')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        
        # Verify structure
        self.assertIn('scheduler', data)
        self.assertIn('queue', data)
        self.assertIn('config', data)
        
        # Verify config values
        self.assertEqual(data['config']['spread_thresholds']['forex'], SPREAD_THRESHOLD_FOREX)
        self.assertEqual(data['config']['spread_thresholds']['crypto'], SPREAD_THRESHOLD_CRYPTO)

    def test_process_single_alert(self):
        alert_data = {
            'symbol': 'EURUSD',
            'action': 'BUY',
            'price': '1.2000'
        }
        alert = RetryableAlert(alert_data)
        
        # Test with market closed
        with patch('trading_bot.check_market_status') as mock_check:
            mock_check.return_value = (False, "Market closed")
            success, message = process_single_alert(alert)
            self.assertFalse(success)
            self.assertEqual(message, "Market closed")
            self.assertEqual(alert.retry_count, 1)

    def test_tradingview_webhook(self):
        # Test invalid request
        response = self.app.post('/tradingview', json={})
        self.assertEqual(response.status_code, 400)
        
        # Test valid request structure
        valid_data = {
            'symbol': 'EURUSD',
            'action': 'BUY',
            'price': '1.2000'
        }
        with patch('trading_bot.check_market_status') as mock_check:
            mock_check.return_value = (True, "Market available")
            response = self.app.post('/tradingview', json=valid_data)
            self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main()
