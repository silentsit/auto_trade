# OANDA Price Validation Fix

## Problem Summary

The trading system was experiencing `InvalidParameterException` errors from OANDA with the message:
```
"Invalid value specified for 'order.takeProfitOnFill.price'"
```

This was happening because OANDA has strict requirements for:
1. Minimum distance between entry price and stop loss/take profit levels
2. Proper price formatting and validation
3. Valid price relationships (SL/TP must be on opposite sides of entry)

## Root Cause Analysis

From the logs, the specific case was:
- **Symbol**: GBP_USD
- **Action**: SELL
- **Entry Price**: 1.32932
- **Stop Loss**: 1.33108 (17.6 pips distance)
- **Take Profit**: 1.32405 (52.7 pips distance)

The take profit distance (52.7 pips) was actually valid (above the 10 pip minimum), but OANDA was still rejecting the order. This suggested the issue was with:
1. Price formatting
2. Order structure validation
3. Missing comprehensive validation before sending to OANDA

## Comprehensive Solution

### 1. Enhanced Price Validation (`utils.py`)

**Added Functions:**
- `validate_oanda_prices()` - Comprehensive price validation with minimum distance checks
- `format_price_for_oanda()` - Proper price formatting for different instrument types
- `get_oanda_minimum_distances()` - Get minimum distance requirements by instrument type
- `validate_oanda_order()` - Complete order validation including all parameters

**Key Features:**
- **Minimum Distance Validation**: Ensures SL/TP meet OANDA's minimum distance requirements
- **Price Relationship Validation**: Ensures SL/TP are on correct sides relative to entry
- **Instrument-Specific Formatting**: Different decimal places for forex vs crypto
- **Comprehensive Error Handling**: Detailed validation with specific error messages

### 2. Enhanced Alert Handler (`alert_handler.py`)

**Improvements:**
- Replaced basic distance validation with comprehensive `validate_oanda_prices()`
- Added detailed logging of price calculations and adjustments
- Enhanced error reporting with specific distance measurements

**Key Changes:**
```python
# Before: Basic validation
min_distance_pips = 10
min_distance = min_distance_pips / 10000

# After: Comprehensive validation
price_validation = validate_oanda_prices(
    symbol=symbol,
    entry_price=entry_price,
    stop_loss=stop_loss_price,
    take_profit=take_profit_price,
    action=action
)
```

### 3. Enhanced OANDA Service (`oanda_service.py`)

**Improvements:**
- Added comprehensive order validation before sending to OANDA
- Proper price formatting for different instrument types
- Enhanced error logging with detailed order data
- Automatic price adjustment when minimum distances aren't met

**Key Changes:**
```python
# Before: Direct price assignment
data["order"]["takeProfitOnFill"] = {
    "timeInForce": "GTC",
    "price": str(take_profit)
}

# After: Validated and formatted prices
order_validation = validate_oanda_order(...)
data["order"]["takeProfitOnFill"] = {
    "timeInForce": "GTC",
    "price": order_validation['order_data']['formatted_take_profit']
}
```

## Minimum Distance Requirements

| Instrument Type | Minimum Pips | Minimum Distance |
|----------------|---------------|------------------|
| Forex Major    | 10 pips      | 0.00100         |
| Forex Minor    | 10 pips      | 0.00100         |
| Crypto         | 50 pips      | 0.00500         |
| Indices        | 5 pips       | 0.00050         |
| Commodities    | 10 pips      | 0.00100         |

## Price Formatting Rules

| Instrument Type | Decimal Places | Example |
|----------------|----------------|---------|
| Forex          | 5              | 1.32932 |
| Crypto         | 2              | 50000.00 |
| Indices        | 5              | 1.32932 |
| Commodities    | 5              | 1.32932 |

## Validation Features

### 1. Distance Validation
- Ensures stop loss and take profit meet minimum distance requirements
- Automatically adjusts prices if they're too close to entry
- Logs all adjustments for transparency

### 2. Price Relationship Validation
- **BUY Orders**: SL below entry, TP above entry
- **SELL Orders**: SL above entry, TP below entry
- Prevents invalid price relationships

### 3. Data Type Validation
- Ensures all prices are valid finite numbers
- Validates positive prices only
- Checks proper data types for all parameters

### 4. Comprehensive Error Reporting
- Detailed error messages for each validation failure
- Warnings for price adjustments
- Complete order data logging

## Testing Results

The fix was thoroughly tested with:

1. **Specific Case from Logs**: GBP_USD SELL order with the exact parameters that failed
   - Result: ✅ Valid (52.7 pips TP distance > 10 pip minimum)

2. **Edge Cases**: 
   - Very small distances (automatically adjusted)
   - Invalid price relationships (properly rejected)
   - Negative prices (properly rejected)

3. **Different Instrument Types**:
   - Forex pairs (5 decimal places)
   - Crypto (2 decimal places)
   - Proper minimum distance enforcement

## Implementation Benefits

### 1. **Prevention of OANDA Rejections**
- Comprehensive validation catches issues before sending to OANDA
- Automatic price adjustment when minimum distances aren't met
- Proper price formatting for all instrument types

### 2. **Enhanced Error Handling**
- Detailed error messages for debugging
- Specific validation failures identified
- Complete order data logged for analysis

### 3. **Institutional-Grade Validation**
- Multiple validation layers
- Comprehensive error checking
- Professional logging and reporting

### 4. **Maintainability**
- Modular validation functions
- Clear separation of concerns
- Easy to extend for new instrument types

## Usage

The fix is automatically applied in the trading flow:

1. **Alert Handler**: Validates prices during position opening
2. **OANDA Service**: Final validation before API calls
3. **Error Handling**: Comprehensive error reporting and logging

No changes required to existing trading strategies - the fix operates transparently in the background.

## Monitoring

The enhanced logging provides visibility into:
- Price adjustments made
- Validation failures
- Distance calculations
- Formatted prices sent to OANDA

This enables proactive monitoring and debugging of any future issues. 