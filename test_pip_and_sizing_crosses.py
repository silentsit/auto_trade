import asyncio
import logging
from utils import get_pip_value, calculate_position_size

logging.basicConfig(level=logging.INFO)

# Simple smoke tests for cross pairs using live OANDA legs when available

def test_gbp_nzd_pip_value_and_size():
    symbol = 'GBP_NZD'
    # Use a reasonable current price for smoke (if live fetch fails, function has fallbacks)
    current_price = 2.34
    pip_val = get_pip_value(symbol, current_price)
    assert pip_val > 0, 'Pip value should be positive'
    # With a hypothetical account and risk, ensure size is within limits
    size = calculate_position_size(
        account_balance=100000.0,
        risk_percent=5.0,
        stop_loss_pips=50.0,
        symbol=symbol,
        current_price=current_price,
    )
    assert 1.0 <= size <= 100000.0


def test_eur_gbp_pip_value_and_size():
    symbol = 'EUR_GBP'
    current_price = 0.85
    pip_val = get_pip_value(symbol, current_price)
    assert pip_val > 0
    size = calculate_position_size(
        account_balance=100000.0,
        risk_percent=5.0,
        stop_loss_pips=40.0,
        symbol=symbol,
        current_price=current_price,
    )
    assert 1.0 <= size <= 100000.0


def test_aud_nzd_pip_value_and_size():
    symbol = 'AUD_NZD'
    current_price = 1.09
    pip_val = get_pip_value(symbol, current_price)
    assert pip_val > 0
    size = calculate_position_size(
        account_balance=100000.0,
        risk_percent=5.0,
        stop_loss_pips=35.0,
        symbol=symbol,
        current_price=current_price,
    )
    assert 1.0 <= size <= 100000.0
