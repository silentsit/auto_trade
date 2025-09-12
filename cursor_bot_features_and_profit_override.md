# Bot features and profit override explanation
_Exported on 9/11/2025 at 07:43:13 GMT+7 from Cursor (1.5.11)_

---

**User**

tell me all the features and functions of the current bot. then explain clearly to me how the profit override function works - especially the methods and calculations involved.

---

**Cursor**

I'll analyze the current trading bot to understand all its features and functions, then explain the profit override functionality in detail. Let me start by examining the main components of the system.


Now let me examine more components to understand the complete system:


Now let me search for the profit override functionality specifically:


Let me search for more specific profit override implementation details:


Now let me create a comprehensive overview of the trading bot's features and explain the profit override function in detail:


# Auto Trading Bot - Complete Feature Overview & Profit Override Analysis

## 🚀 **Complete Bot Features & Functions**

### **Core Architecture**
- **FastAPI-based REST API** with webhook endpoints for TradingView integration
- **Modular component system** with robust error handling and fallback mechanisms
- **Multi-environment support** (development, production) with configuration management
- **Database persistence** with SQLite/PostgreSQL support and automatic migrations

### **Trading Engine Components**

#### **1. Alert Handler (`alert_handler.py`)**
- **Webhook processing** for TradingView signals
- **Alert standardization** and validation
- **Duplicate detection** with idempotency controls
- **Position lifecycle management** (open/close operations)
- **Risk validation** before trade execution
- **ATR-based position sizing** with leverage calculations

#### **2. OANDA Service (`oanda_service.py`)**
- **Institutional-grade connection management** with circuit breakers
- **Smart retry logic** with exponential backoff
- **Connection health monitoring** and automatic reconnection
- **Rate limiting** and request optimization
- **Crypto support** with symbol formatting
- **Account balance management** and leverage detection
- **Historical data fetching** for technical analysis

#### **3. Risk Manager (`risk_manager.py`)**
- **Portfolio risk management** with correlation limits
- **Position sizing calculations** with volatility adjustments
- **Daily loss limits** and drawdown protection
- **Correlation analysis** to prevent over-concentration
- **Streak-based adjustments** (win/loss streaks)
- **Timeframe-based risk weighting**

#### **4. Position Tracker (`tracker.py`)**
- **Real-time position monitoring** with P&L calculations
- **Database persistence** with automatic recovery
- **Weekend position tracking** and age management
- **Position history** and performance statistics
- **Duplicate position cleanup**
- **Partial position closing** support

#### **5. Unified Exit Manager (`unified_exit_manager.py`)**
- **Priority-based exit management** (SL/TP first, then close signals)
- **Profit override system** with trailing stops
- **Breakeven management** at 1R profit
- **Dynamic exit conditions** based on market regime
- **Exit signal monitoring** and effectiveness tracking

#### **6. Market Analysis (`unified_analysis.py`)**
- **Regime classification** using Lorentzian Distance Classification
- **Technical indicators** (RSI, MACD, Bollinger Bands, ATR)
- **Volatility monitoring** with dynamic adjustments
- **Trend analysis** with support/resistance identification
- **Crypto signal generation** with confidence scoring

#### **7. Unified Storage (`unified_storage.py`)**
- **Database abstraction layer** supporting multiple backends
- **Position persistence** with entry/exit tracking
- **Performance metrics** storage and retrieval
- **Automatic schema migrations**

### **API Endpoints**
- **`/tradingview`** - Main webhook for TradingView signals
- **`/api/positions`** - Position management and monitoring
- **`/api/status`** - System health and component status
- **`/api/emergency/close-all`** - Emergency position closure
- **`/correlation/*`** - Correlation analysis and monitoring
- **`/debug/*`** - Debugging and testing endpoints

---

## 🎯 **Profit Override Function - Detailed Analysis**

The **Profit Override** is the bot's most sophisticated feature, designed to maximize profitable trades by overriding close signals when conditions are favorable.

### **Core Concept**
When a close signal is received, instead of immediately closing the position, the system analyzes whether the position should be "ridden" for additional profit using trailing stops.

### **1. Override Eligibility Analysis (`_analyze_override_eligibility`)**

The system evaluates multiple criteria to determine if a close signal should be overridden:

#### **A. Basic Checks**
```python
# Check if override already activated
if strategy.override_fired:
    return OverrideDecision(should_override=False, reason="Override already activated")

# Check account balance
account_balance = await self._get_account_balance()
if account_balance <= 0:
    return OverrideDecision(should_override=False, reason="Cannot check account balance")

# Check position age (max 48 hours)
position_age = datetime.now() - strategy.created_at
if position_age.total_seconds() > (self.max_ride_time_hours * 3600):
    return OverrideDecision(should_override=False, reason="Position too old")
```

#### **B. Profit Threshold Check**
```python
# Calculate current profit vs initial risk
current_profit = abs(strategy.current_price - strategy.entry_price) * strategy.current_risk_amount / strategy.initial_risk_amount

# Must have at least 1x initial risk in profit
if current_profit < strategy.initial_risk_amount:
    return OverrideDecision(should_override=False, reason="Insufficient profit")
```

#### **C. Market Regime Analysis**
```python
# Get current market regime
regime_data = self.regime.get_regime_data(strategy.symbol)
current_regime = regime_data.get('regime', 'unknown')
regime_strength = regime_data.get('regime_strength', 0.0)

# Get volatility state
vol_data = self.vol.get_volatility_state(strategy.symbol)
volatility_ratio = vol_data.get('volatility_ratio', 1.0)
```

#### **D. Scoring System (0-10 points)**
The system uses a sophisticated scoring mechanism:

**Profit Factor (0-3 points):**
- `profit_factor >= 2.0`: 3 points ("High profit factor")
- `profit_factor >= 1.5`: 2 points ("Good profit factor") 
- `profit_factor >= 1.0`: 1 point ("Minimum profit threshold met")

**Market Regime (0-2 points):**
- Strong trending regime (`trending_up`/`trending_down` with >70% strength): 2 points
- Momentum regime (`momentum_up`/`momentum_down` with >60% strength): 1.5 points

**Volatility (0-2 points):**
- Optimal volatility (0.8-1.5 ratio): 2 points
- Acceptable volatility (0.6-2.0 ratio): 1 point

**Position Age (0-1 point):**
- Fresh position (<24 hours): 1 point

**Decision Threshold:** 6+ points required to override

### **2. Profit Override Activation (`_activate_profit_override`)**

When override is approved:

```python
def _activate_profit_override(self, strategy: ExitStrategy):
    # Mark override as active
    strategy.override_fired = True
    strategy.trailing_stop_price = strategy.current_price
    
    # Get trailing stop configuration based on timeframe
    timeframe = "1H"  # Default timeframe
    config = self._get_trailing_stop_config(timeframe)
    
    # Calculate initial trailing stop
    if strategy.current_price > strategy.entry_price:  # Long position
        strategy.trailing_stop_price = strategy.current_price - (config.min_distance * config.atr_multiplier)
    else:  # Short position
        strategy.trailing_stop_price = strategy.current_price + (config.min_distance * config.atr_multiplier)
    
    # CRITICAL: Remove initial SL/TP from broker
    asyncio.create_task(self._remove_broker_sl_tp(strategy.position_id))
```

### **3. Trailing Stop Configuration**

Different timeframes have different trailing stop parameters:

```python
def _get_trailing_stop_config(self, timeframe: str) -> TrailingStopConfig:
    if timeframe == "15M":
        return TrailingStopConfig(
            atr_multiplier=1.5,
            min_distance=0.0002,
            breakeven_threshold=1.0,
            max_trail_distance=0.0030
        )
    elif timeframe == "1H":
        return TrailingStopConfig(
            atr_multiplier=2.0,
            min_distance=0.0003,
            breakeven_threshold=1.0,
            max_trail_distance=0.0040
        )
    else:  # 4H+
        return TrailingStopConfig(
            atr_multiplier=2.5,
            min_distance=0.0005,
            breakeven_threshold=1.0,
            max_trail_distance=0.0060
        )
```

### **4. Trailing Stop Updates (`_update_trailing_stops`)**

The system continuously updates trailing stops:

```python
def _update_trailing_stops(self):
    for position_id, strategy in list(self.exit_strategies.items()):
        if not strategy.override_fired or not strategy.trailing_stop_price:
            continue
            
        # Check breakeven activation
        if not strategy.breakeven_enabled and strategy.current_price >= strategy.entry_price + strategy.initial_risk_amount:
            strategy.breakeven_enabled = True
            strategy.breakeven_price = strategy.entry_price
        
        # Update trailing stop
        if strategy.current_price > strategy.entry_price:  # Long position
            new_trailing_stop = strategy.current_price - (config.min_distance * config.atr_multiplier)
            
            # Apply breakeven if enabled
            if strategy.breakeven_enabled and strategy.breakeven_price > new_trailing_stop:
                new_trailing_stop = strategy.breakeven_price
            
            # Only move trailing stop up
            if new_trailing_stop > strategy.trailing_stop_price:
                strategy.trailing_stop_price = new_trailing_stop
```

### **5. Key Calculations**

#### **A. Profit Factor Calculation**
```python
current_profit = abs(strategy.current_price - strategy.entry_price) * strategy.current_risk_amount / strategy.initial_risk_amount
profit_factor = current_profit / strategy.initial_risk_amount
```

#### **B. Initial Risk Amount**
```python
initial_risk_amount = abs(entry_price - stop_loss_price) * position_size
```

#### **C. Trailing Stop Distance**
```python
trailing_distance = atr_value * atr_multiplier
new_trailing_stop = current_price - trailing_distance  # For long positions
```

### **6. Breakeven Management**

When profit reaches 1x initial risk:
- **Breakeven is activated** at entry price
- **Trailing stop cannot go below breakeven** for long positions
- **Protects against giving back all profits**

### **7. Exit Conditions**

The position exits when:
- **Trailing stop is hit** (price moves against position)
- **Maximum ride time exceeded** (48 hours default)
- **Account drawdown limit reached** (5% default)
- **Market regime changes** to unfavorable conditions

### **8. Risk Management Integration**

The profit override system integrates with:
- **Correlation limits** to prevent over-concentration
- **Volatility adjustments** for position sizing
- **Portfolio heat limits** to control overall risk
- **Daily loss limits** for account protection

---

## 🔧 **Configuration Parameters**

### **Profit Override Settings**
- `max_ride_time_hours`: 48 (maximum time to ride a position)
- `breakeven_threshold`: 1.0 (1x initial risk for breakeven)
- `account_drawdown_limit`: 0.05 (5% account drawdown limit)

### **Trailing Stop Settings**
- `atr_multiplier`: 1.5-2.5 (based on timeframe)
- `min_distance`: 0.0002-0.0005 (minimum trailing distance)
- `max_trail_distance`: 0.0030-0.0060 (maximum trailing distance)

### **Override Scoring Thresholds**
- `profit_factor >= 2.0`: High confidence override
- `profit_factor >= 1.5`: Good confidence override
- `profit_factor >= 1.0`: Minimum threshold
- `total_score >= 6.0`: Required for override activation

This sophisticated system allows the bot to maximize profitable trades while maintaining strict risk controls, making it particularly effective in trending markets where positions can continue moving in the favorable direction.


```python
# Check if override already activated
if strategy.override_fired:
    return OverrideDecision(should_override=False, reason="Override already activated")

# Check account balance
account_balance = await self._get_account_balance()
if account_balance <= 0:
    return OverrideDecision(should_override=False, reason="Cannot check account balance")

# Check position age (max 48 hours)
position_age = datetime.now() - strategy.created_at
if position_age.total_seconds() > (self.max_ride_time_hours * 3600):
    return OverrideDecision(should_override=False, reason="Position too old")
```

```python
# Calculate current profit vs initial risk
current_profit = abs(strategy.current_price - strategy.entry_price) * strategy.current_risk_amount / strategy.initial_risk_amount

# Must have at least 1x initial risk in profit
if current_profit < strategy.initial_risk_amount:
    return OverrideDecision(should_override=False, reason="Insufficient profit")
```

```python
# Get current market regime
regime_data = self.regime.get_regime_data(strategy.symbol)
current_regime = regime_data.get('regime', 'unknown')
regime_strength = regime_data.get('regime_strength', 0.0)

# Get volatility state
vol_data = self.vol.get_volatility_state(strategy.symbol)
volatility_ratio = vol_data.get('volatility_ratio', 1.0)
```

```python
def _activate_profit_override(self, strategy: ExitStrategy):
    # Mark override as active
    strategy.override_fired = True
    strategy.trailing_stop_price = strategy.current_price
    
    # Get trailing stop configuration based on timeframe
    timeframe = "1H"  # Default timeframe
    config = self._get_trailing_stop_config(timeframe)
    
    # Calculate initial trailing stop
    if strategy.current_price > strategy.entry_price:  # Long position
        strategy.trailing_stop_price = strategy.current_price - (config.min_distance * config.atr_multiplier)
    else:  # Short position
        strategy.trailing_stop_price = strategy.current_price + (config.min_distance * config.atr_multiplier)
    
    # CRITICAL: Remove initial SL/TP from broker
    asyncio.create_task(self._remove_broker_sl_tp(strategy.position_id))
```

```python
def _get_trailing_stop_config(self, timeframe: str) -> TrailingStopConfig:
    if timeframe == "15M":
        return TrailingStopConfig(
            atr_multiplier=1.5,
            min_distance=0.0002,
            breakeven_threshold=1.0,
            max_trail_distance=0.0030
        )
    elif timeframe == "1H":
        return TrailingStopConfig(
            atr_multiplier=2.0,
            min_distance=0.0003,
            breakeven_threshold=1.0,
            max_trail_distance=0.0040
        )
    else:  # 4H+
        return TrailingStopConfig(
            atr_multiplier=2.5,
            min_distance=0.0005,
            breakeven_threshold=1.0,
            max_trail_distance=0.0060
        )
```

```python
def _update_trailing_stops(self):
    for position_id, strategy in list(self.exit_strategies.items()):
        if not strategy.override_fired or not strategy.trailing_stop_price:
            continue
            
        # Check breakeven activation
        if not strategy.breakeven_enabled and strategy.current_price >= strategy.entry_price + strategy.initial_risk_amount:
            strategy.breakeven_enabled = True
            strategy.breakeven_price = strategy.entry_price
        
        # Update trailing stop
        if strategy.current_price > strategy.entry_price:  # Long position
            new_trailing_stop = strategy.current_price - (config.min_distance * config.atr_multiplier)
            
            # Apply breakeven if enabled
            if strategy.breakeven_enabled and strategy.breakeven_price > new_trailing_stop:
                new_trailing_stop = strategy.breakeven_price
            
            # Only move trailing stop up
            if new_trailing_stop > strategy.trailing_stop_price:
                strategy.trailing_stop_price = new_trailing_stop
```

```python
current_profit = abs(strategy.current_price - strategy.entry_price) * strategy.current_risk_amount / strategy.initial_risk_amount
profit_factor = current_profit / strategy.initial_risk_amount
```

```python
initial_risk_amount = abs(entry_price - stop_loss_price) * position_size
```

```python
trailing_distance = atr_value * atr_multiplier
new_trailing_stop = current_price - trailing_distance  # For long positions
```

---

**User**

- explain risk validation before trade execution
- explain atr-based position sizing with leverage calculations
- remove 'historical data fetching for technical analysis'
- explain position sizing calculations 'with volatility adjsutments'
- what are streak based adjustments?

---

**Cursor**

I'll analyze the codebase to provide detailed explanations of these specific features. Let me examine the relevant components to understand the risk validation, ATR-based position sizing, and other calculations.



Now let me examine the unified analysis component to understand the technical analysis features:


Now let me create a comprehensive explanation of the requested features:

# **Auto Trading Bot - Detailed Feature Analysis**

## **1. Risk Validation Before Trade Execution**

The bot implements a comprehensive **multi-layered risk validation system** that runs before every trade execution:

### **Pre-Trade Risk Checks (`risk_manager.py` - `is_trade_allowed` method):**

```python
async def is_trade_allowed(self, risk_percentage: float, symbol: Optional[str] = None, 
                         action: Optional[str] = None) -> Tuple[bool, str]:
```

**Validation Layers:**

1. **Daily Loss Circuit Breaker**
   - Checks if daily losses exceed `MAX_DAILY_LOSS` (50% of account balance)
   - Prevents catastrophic account destruction
   - Formula: `daily_loss >= account_balance * 0.50`

2. **Per-Trade Risk Limits**
   - Validates individual trade risk doesn't exceed `max_risk_per_trade` (default 10%)
   - Prevents oversized individual positions
   - Formula: `risk_percentage <= max_risk_per_trade`

3. **Portfolio Risk Limits**
   - Ensures total portfolio exposure doesn't exceed `max_portfolio_risk` (default 70%)
   - Prevents over-leveraging across all positions
   - Formula: `current_risk + risk_percentage <= max_portfolio_risk`

4. **Symbol Concentration Limits**
   - Limits exposure to any single symbol (default 20%)
   - Prevents over-concentration in one instrument
   - Formula: `symbol_exposure + risk_percentage <= portfolio_concentration_limit`

5. **Correlation Risk Management**
   - Checks for highly correlated positions using `CorrelationManager`
   - Prevents correlated risk accumulation
   - Blocks trades that would create excessive correlation exposure

6. **Account Balance Validation**
   - Refreshes real-time balance from OANDA before validation
   - Ensures sufficient margin for new positions
   - Validates account is not in margin call state

## **2. ATR-Based Position Sizing with Leverage Calculations**

The bot uses **institutional-grade ATR-based position sizing** with sophisticated leverage calculations:

### **Core Position Sizing Logic (`utils.py` - `calculate_position_size`):**

```python
async def calculate_position_size(
    symbol: str,
    entry_price: float,
    risk_percent: float,
    account_balance: float,
    leverage: float = 50.0,
    max_position_value: float = 100000.0,
    stop_loss_price: Optional[float] = None,
    timeframe: str = "H1"
) -> Tuple[float, Dict[str, Any]]:
```

**ATR-Based Stop Loss Calculation:**
- **ATR Multipliers by Timeframe:**
  - M1: 1.5x ATR
  - M5: 1.2x ATR  
  - M15: 1.0x ATR
  - M30: 0.8x ATR
  - H1: 0.6x ATR
  - H4: 0.4x ATR
  - D1: 0.3x ATR

- **Minimum Stop Distances** (prevents oversized positions):
  - M1: 0.0001 (1 pip)
  - M5: 0.0002 (2 pips)
  - M15: 0.0003 (3 pips)
  - M30: 0.0005 (5 pips)
  - H1: 0.0008 (8 pips)
  - H4: 0.0015 (15 pips)
  - D1: 0.0025 (25 pips)

**Position Size Calculation:**
```python
# Risk amount in account currency
risk_amount = account_balance * (risk_percent / 100.0)

# Position size based on stop loss distance
if stop_loss_distance and stop_loss_distance > 0:
    raw_size = risk_amount / stop_loss_distance
else:
    raw_size = 0

# Apply maximum position value cap
max_size_by_value = max_position_value / entry_price
final_position_size = min(raw_size, max_size_by_value)

# Calculate position value and margin requirements
position_value = final_position_size * entry_price
required_margin = position_value / leverage
margin_utilization_pct = (required_margin / account_balance) * 100
```

## **3. Position Sizing Calculations with Volatility Adjustments**

The bot implements **sophisticated volatility-adjusted position sizing**:

### **Volatility Adjustment System (`risk_manager.py` - `calculate_volatility_adjusted_size`):**

```python
async def calculate_volatility_adjusted_size(self, 
                                          base_size: float, 
                                          symbol: str, 
                                          current_atr: float,
                                          historical_atr: float,
                                          market_regime: str = "normal") -> float:
```

**Volatility Adjustment Factors:**

1. **Volatility Ratio Calculation:**
   ```python
   volatility_ratio = current_atr / historical_atr
   ```

2. **Market Regime Multipliers:**
   - **Normal Market:** 1.0x (no adjustment)
   - **Volatile Market:** 0.7x (reduce position size by 30%)
   - **Quiet Market:** 1.2x (increase position size by 20%)

3. **Instrument-Specific Volatility Caps:**
   - **EUR_USD:** 1.5x maximum volatility multiplier
   - **GBP_USD:** 1.3x maximum volatility multiplier
   - **USD_JPY:** 1.2x maximum volatility multiplier
   - **AUD_USD:** 1.4x maximum volatility multiplier

4. **Correlation Penalties:**
   - **High Correlation (≥2 correlated positions):** 15% penalty
   - **Moderate Correlation (1 correlated position):** 5% penalty
   - **No Correlation:** 0% penalty

**Final Calculation:**
```python
volatility_factor = min(volatility_ratio, volatility_cap)
adjusted_size = base_size * volatility_factor * regime_multiplier
adjusted_size *= (1 - correlation_penalty)
```

## **4. Streak-Based Adjustments**

The bot implements **dynamic position sizing based on trading performance streaks**:

### **Streak Factor Calculation (`risk_manager.py` - `_calculate_streak_factor`):**

```python
def _calculate_streak_factor(self) -> float:
    if self.win_streak >= 3:
        return min(1.5, 1.0 + (self.win_streak - 2) * 0.1)
    elif self.loss_streak >= 2:
        return max(0.5, 1.0 - (self.loss_streak - 1) * 0.2)
    else:
        return 1.0
```

**Streak Adjustment Logic:**

1. **Winning Streak Adjustments:**
   - **3+ consecutive wins:** Gradually increase position size
   - **Formula:** `min(1.5, 1.0 + (win_streak - 2) * 0.1)`
   - **Maximum multiplier:** 1.5x (50% increase)
   - **Example:** 5 wins = 1.3x multiplier (30% increase)

2. **Losing Streak Adjustments:**
   - **2+ consecutive losses:** Gradually decrease position size
   - **Formula:** `max(0.5, 1.0 - (loss_streak - 1) * 0.2)`
   - **Minimum multiplier:** 0.5x (50% reduction)
   - **Example:** 4 losses = 0.4x multiplier (60% reduction)

3. **Streak Tracking:**
   ```python
   async def update_win_loss_streak(self, is_win: bool):
       if is_win:
           self.win_streak += 1
           self.loss_streak = 0
       else:
           self.loss_streak += 1
           self.win_streak = 0
   ```

**Integration with Position Sizing:**
- Streak factors are applied to **all position size calculations**
- Combined with volatility adjustments and correlation penalties
- Final formula: `final_size = base_size * volatility_factor * regime_multiplier * streak_factor * (1 - correlation_penalty)`

## **5. Additional Risk Management Features**

### **Timeframe Risk Weights:**
```python
timeframe_risk_weights = {
    "M1": 1.2,    # 20% higher risk for scalping
    "M5": 1.1,    # 10% higher risk for short-term
    "M15": 1.0,   # Base risk
    "M30": 0.9,   # 10% lower risk
    "H1": 0.8,    # 20% lower risk
    "H4": 0.7,    # 30% lower risk
    "D1": 0.6     # 40% lower risk for swing trading
}
```

### **Real-Time Risk Monitoring:**
- **Portfolio Heat Tracking:** Monitors total portfolio risk exposure
- **Drawdown Monitoring:** Tracks maximum drawdown from peak
- **Daily Loss Tracking:** Prevents catastrophic daily losses
- **Margin Utilization:** Monitors margin usage across all positions

This comprehensive risk management system ensures **consistent risk per trade** while adapting to market conditions, trading performance, and portfolio constraints.

```python
async def is_trade_allowed(self, risk_percentage: float, symbol: Optional[str] = None, 
                         action: Optional[str] = None) -> Tuple[bool, str]:
```

```python
async def calculate_position_size(
    symbol: str,
    entry_price: float,
    risk_percent: float,
    account_balance: float,
    leverage: float = 50.0,
    max_position_value: float = 100000.0,
    stop_loss_price: Optional[float] = None,
    timeframe: str = "H1"
) -> Tuple[float, Dict[str, Any]]:
```

```python
# Risk amount in account currency
risk_amount = account_balance * (risk_percent / 100.0)

# Position size based on stop loss distance
if stop_loss_distance and stop_loss_distance > 0:
    raw_size = risk_amount / stop_loss_distance
else:
    raw_size = 0

# Apply maximum position value cap
max_size_by_value = max_position_value / entry_price
final_position_size = min(raw_size, max_size_by_value)

# Calculate position value and margin requirements
position_value = final_position_size * entry_price
required_margin = position_value / leverage
margin_utilization_pct = (required_margin / account_balance) * 100
```

```python
async def calculate_volatility_adjusted_size(self, 
                                          base_size: float, 
                                          symbol: str, 
                                          current_atr: float,
                                          historical_atr: float,
                                          market_regime: str = "normal") -> float:
```

```python
   volatility_ratio = current_atr / historical_atr
```

```python
volatility_factor = min(volatility_ratio, volatility_cap)
adjusted_size = base_size * volatility_factor * regime_multiplier
adjusted_size *= (1 - correlation_penalty)
```

```python
def _calculate_streak_factor(self) -> float:
    if self.win_streak >= 3:
        return min(1.5, 1.0 + (self.win_streak - 2) * 0.1)
    elif self.loss_streak >= 2:
        return max(0.5, 1.0 - (self.loss_streak - 1) * 0.2)
    else:
        return 1.0
```

```python
   async def update_win_loss_streak(self, is_win: bool):
       if is_win:
           self.win_streak += 1
           self.loss_streak = 0
       else:
           self.loss_streak += 1
           self.win_streak = 0
```

```python
timeframe_risk_weights = {
    "M1": 1.2,    # 20% higher risk for scalping
    "M5": 1.1,    # 10% higher risk for short-term
    "M15": 1.0,   # Base risk
    "M30": 0.9,   # 10% lower risk
    "H1": 0.8,    # 20% lower risk
    "H4": 0.7,    # 30% lower risk
    "D1": 0.6     # 40% lower risk for swing trading
}
```

