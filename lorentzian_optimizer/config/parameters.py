"""
Parameter Configuration for Lorentzian Classification Optimizer
Defines all parameter ranges, constraints, and optimization settings
"""

# Currency pairs to optimize
CURRENCY_PAIRS = [
    "AUDUSD",
    "GBPUSD", 
    "USDCHF",
    "AUDCAD",
    "NZDJPY",
    "BTCUSD",
    "ETHUSD"
]

# Parameter ranges based on user specifications
PARAMETER_RANGES = {
    # General Settings
    'neighbours_count': (4, 12),
    'max_bars_back': [2000, 2500, 3000],
    'color_compression': 1,
    'show_default_exits': True,
    'use_dynamic_exits': False,
    'show_trade_stats': True,
    'use_worst_case_estimates': False,
    
    # Feature Engineering
    'feature_count': 5,
    'f1_param_a': (7, 24),  # RSI Parameter A
    'f1_param_b': 1,        # RSI Parameter B
    'f2_param_a': (7, 17),  # WT Parameter A
    'f2_param_b': (7, 17),  # WT Parameter B
    'f3_param_a': (11, 24), # CCI Parameter A
    'f3_param_b': 1,        # CCI Parameter B
    'f4_param_a': (13, 24), # ADX Parameter A
    'f4_param_b': 2,        # ADX Parameter B
    'f5_param_a': (4, 11),  # RSI Parameter A
    'f5_param_b': (1, 9),   # RSI Parameter B
    
    # Filters
    'use_volatility_filter': True,
    'regime_threshold': (-0.3, 0.3),
    'use_regime_filter': True,
    'adx_threshold': (11, 24),
    'use_adx_filter': True,
    'ema_period': [9, 15, 20, 40],
    'use_ema_filter': True,
    'sma_period': [10, 20, 30, 50, 80],
    'use_sma_filter': True,
    
    # Kernel Settings
    'trade_with_kernel': True,
    'show_kernel_estimate': True,
    'kernel_lookback': [6, 8],
    'relative_weighting': 8,
    'kernel_regression': [18, 20, 25],
    'enhance_kernel_smoothing': False,
    
    # Display Settings
    'show_bar_colors': True,
    'show_bar_predictions': True,
    'use_atr_offset': False,
    'bar_prediction_offset': 0,
    'inputs_in_status_line': True,
    
    # Fixed Parameters
    'source': 'hlc3',
    'timeframe': '15'
}

# Preferred EMA/SMA combinations (user's preferred combinations)
PREFERRED_EMA_SMA_COMBINATIONS = [
    (9, 20),   # 9 EMA / 20 SMA
    (15, 30),  # 15 EMA / 30 SMA
    (20, 50),  # 20 EMA / 50 SMA
    (40, 80)   # 40 EMA / 80 SMA
]

# Performance criteria for optimization
PERFORMANCE_CRITERIA = {
    'min_sharpe_ratio': 1.5,
    'min_profit_factor': 1.3,
    'min_pnl_dd_ratio': 1.5,
    'min_total_trades': 30,
    'min_win_rate': 0.0  # No minimum win rate requirement
}

# Optimization settings
OPTIMIZATION_SETTINGS = {
    'max_generations': 50,
    'population_size': 20,
    'crossover_probability': 0.7,
    'mutation_probability': 0.2,
    'tournament_size': 3,
    'elite_size': 2
}

# Walk-forward validation settings
WALK_FORWARD_SETTINGS = {
    'enabled': True,
    'training_period_months': 6,
    'validation_period_months': 1,
    'step_size_months': 1
}

# Risk management settings
RISK_MANAGEMENT = {
    'max_drawdown_threshold': 0.15,  # 15% max drawdown
    'min_risk_reward_ratio': 1.0,
    'max_position_size': 0.02,  # 2% of account per trade
    'correlation_threshold': 0.7  # Max correlation between pairs
}

# Logging and monitoring
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'optimization.log',
    'max_file_size': 10485760,  # 10MB
    'backup_count': 5
}
