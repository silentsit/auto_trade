import unittest
from trading_bot import is_market_open, calculate_next_market_open

class TestTradingBot(unittest.TestCase):
    def test_is_market_open(self):
        is_open, reason = is_market_open()
        self.assertIn(is_open, [True, False])

    def test_calculate_next_market_open(self):
        next_open = calculate_next_market_open()
        self.assertIsInstance(next_open, datetime)

if __name__ == '__main__':
    unittest.main()
