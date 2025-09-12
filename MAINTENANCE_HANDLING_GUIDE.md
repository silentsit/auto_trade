# OANDA Maintenance Handling Solution

## Problem Solved

Your trading bot was stuck in a reinitialization loop during OANDA maintenance (503 errors), constantly trying to reconnect every minute and generating log spam. This solution implements:

1. **Exponential backoff** for 503 maintenance errors
2. **State management** (OK | MAINTENANCE | DEGRADED)
3. **Order queuing** during broker outages
4. **Intelligent probing** that respects backoff intervals

## Key Files Added

- `backoff.py` - Exponential backoff and state management
- `order_queue.py` - Thread-safe order queue for outages
- `maintenance_handling_example.py` - Integration example

## Key Changes to Existing Files

- `oanda_service.py` - Updated with state-aware health checks and maintenance handling

## How It Works

### 1. State Management

The system now tracks three states:
- **OK**: Broker is healthy, can trade
- **MAINTENANCE**: 503 errors detected, aggressive backoff
- **DEGRADED**: Other errors, shorter backoff

### 2. Exponential Backoff

When 503 maintenance is detected:
- First retry: ~30 seconds
- Each subsequent retry: doubles (60s, 120s, 240s, etc.)
- Maximum: 15 minutes
- Includes jitter to prevent thundering herd

### 3. Order Queuing

During maintenance:
- Orders are queued instead of rejected
- Queue processes automatically when broker recovers
- Configurable queue size and retry limits

## Integration Steps

### 1. Update Your Alert Handler

```python
from oanda_service import OandaService
from order_queue import OrderQueue

# Initialize with maintenance handling
oanda_service = OandaService(success_probe_seconds=600)  # 10 min between healthy probes
order_queue = OrderQueue(max_queue_size=100)

async def handle_alert(alert_data):
    if oanda_service.can_trade():
        # Execute immediately
        success, result = await oanda_service.execute_trade(alert_data)
        return {"status": "executed", "result": result}
    else:
        # Queue for later
        order_id = f"order_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        queued = await order_queue.enqueue(order_id, alert_data)
        return {"status": "queued", "order_id": order_id, "broker_state": oanda_service.connection_state.state}
```

### 2. Add Background Queue Processing

```python
async def process_queued_orders():
    """Process queued orders when broker becomes available."""
    while not order_queue.is_empty():
        order = await order_queue.dequeue()
        if order:
            success, result = await oanda_service.execute_trade(order.payload)
            await order_queue.mark_processed(order.order_id, success)

# Start background task
asyncio.create_task(process_queued_orders())
```

### 3. Update Status Endpoints

```python
async def get_status():
    connection_status = await oanda_service.get_connection_status()
    queue_status = await order_queue.get_queue_status()
    
    return {
        "broker_state": connection_status["connection_state"],
        "can_trade": connection_status["can_trade"],
        "queue_size": queue_status["queue_size"],
        "consecutive_503s": connection_status["consecutive_503s"],
        "next_probe_at": connection_status["next_probe_at"]
    }
```

## Configuration Options

### OandaService Parameters

```python
oanda_service = OandaService(
    success_probe_seconds=600  # How often to probe when healthy (default: 10 min)
)
```

### OrderQueue Parameters

```pythondfi
order_queue = OrderQueue(
    max_queue_size=100  # Maximum orders to queue (default: 100)
)
```

### Backoff Parameters (in backoff.py)

```python
ExponentialBackoff(
    base_seconds=30.0,    # First retry delay
    factor=2.0,           # Exponential factor
    max_seconds=900.0,    # Maximum delay (15 min)
    jitter=0.3            # ±30% jitter
)
```

## Benefits

1. **No More Log Spam**: 503 errors logged once every 10 minutes instead of every minute
2. **Order Preservation**: Orders are queued during maintenance instead of lost
3. **Automatic Recovery**: System resumes trading automatically when broker recovers
4. **Intelligent Backoff**: Respects maintenance windows and reduces API load
5. **Better Monitoring**: Clear status reporting on broker state and queue status

## Testing

Run the test suite to verify everything works:

```bash
python test_maintenance_handling.py
```

## Monitoring

Check the new status fields:
- `connection_state`: Current broker state
- `can_trade`: Whether trading is allowed
- `consecutive_503s`: Number of consecutive maintenance errors
- `next_probe_at`: When next health check will occur
- `queue_size`: Number of orders waiting to be processed

## Example Log Output

**Before (spam):**
```
2025-09-06 09:00:02,490 [ERROR] OandaService: ❌ Failed to reinitialize OANDA client: OANDA connection test failed
2025-09-06 09:00:02,490 [ERROR] OandaService: ❌ OANDA connection test failed: {"errorMessage":"System under maintenance, please try again later."}
```

**After (clean):**
```
2025-09-06 09:00:02,490 [ERROR] Backoff: OANDA under maintenance (503). Pausing probes for ~32s. Consecutive 503s: 1
2025-09-06 09:10:15,123 [INFO] Backoff: ✅ OANDA healthy. Next probe in 600s.
```

The system now handles OANDA maintenance gracefully without overwhelming your logs or losing trading opportunities.
