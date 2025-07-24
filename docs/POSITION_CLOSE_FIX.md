# Position Close Fix - CLOSEOUT_POSITION_DOESNT_EXIST Error

## Issue Description

The trading system was experiencing repeated failures when trying to close positions, with the following error pattern:

```
OANDA request failed after 5 attempts: {
  "longOrderRejectTransaction": {
    "rejectReason": "CLOSEOUT_POSITION_REJECT",
    "units": "-142"
  },
  "shortOrderRejectTransaction": {
    "rejectReason": "CLOSEOUT_POSITION_DOESNT_EXIST"
  },
  "errorMessage": "The Position requested to be closed out does not exist",
  "errorCode": "CLOSEOUT_POSITION_DOESNT_EXIST"
}
```

## Root Cause

The original `_close_position` function in `main.py` was using a "close all" approach:

```python
data={"longUnits": "ALL", "shortUnits": "ALL"}
```

This attempted to close both long and short positions simultaneously, but OANDA rejected this because:
1. A long position existed but couldn't be closed properly 
2. A short position didn't exist, causing `CLOSEOUT_POSITION_DOESNT_EXIST`

## Solution Implemented

### 1. Smart Position Checking
The function now first queries OANDA to check which positions actually exist:

```python
from oandapyV20.endpoints.positions import OpenPositions
positions_request = OpenPositions(accountID=config.oanda_account_id)
positions_response = await robust_oanda_request(positions_request)
```

### 2. Selective Position Closing
Only attempts to close positions that actually exist:

```python
close_data = {}
if long_units != 0:
    close_data["longUnits"] = "ALL"
if short_units != 0:
    close_data["shortUnits"] = "ALL"
```

### 3. Enhanced Error Handling
Gracefully handles specific OANDA error codes:

- `CLOSEOUT_POSITION_DOESNT_EXIST` → Treated as success (goal achieved)
- `CLOSEOUT_POSITION_REJECT` → Logged as error with details
- Network connectivity issues → Fallback to original approach

### 4. Fallback Mechanism
If position checking fails due to connectivity, falls back to the original approach but with improved error handling.

## Benefits

1. **Eliminates Retry Loops**: No more 5-attempt failures for non-existent positions
2. **Improved Logging**: Clear visibility into what positions are being closed
3. **Better Error Handling**: Specific handling for different error types
4. **Backward Compatibility**: Fallback ensures existing functionality is preserved

## Testing

Added comprehensive tests in `test_close_signals.py`:
- Direct function testing with various symbols
- End-to-end close signal processing
- Error condition handling verification

## Files Modified

- `main.py`: Enhanced `_close_position` function
- `test_close_signals.py`: Added comprehensive tests
- `POSITION_CLOSE_FIX.md`: This documentation

## Monitoring

The fix includes enhanced logging to monitor:
- Position existence checks
- Actual positions found vs. requested
- Fallback usage
- Specific error types encountered

Look for log messages with `[CLOSE]` prefix for position closing operations. 