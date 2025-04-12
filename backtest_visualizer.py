"""
Backtest Visualizer for FX Trading Bridge

This module provides functions for creating interactive visualizations of backtest
results, including equity curves, trade performance charts, and statistical analysis.
"""

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from typing import Dict, List, Optional, Tuple, Any, Union
import datetime
from pathlib import Path

# Use a modern style for plots
plt.style.use('seaborn-v0_8-darkgrid')

def plot_equity_curve(equity_data: List[Tuple[str, float]], 
                      title: str = "Equity Curve",
                      output_path: Optional[str] = None,
                      show_drawdown: bool = True,
                      initial_capital: float = 100000.0):
    """
    Plot the equity curve and optionally drawdown from backtest results
    
    Args:
        equity_data: List of (datetime_str, equity) tuples
        title: Plot title
        output_path: Path to save the plot (if None, display plot)
        show_drawdown: Whether to show drawdown subplot
        initial_capital: Initial account capital
    """
    # Convert data to DataFrame
    df = pd.DataFrame(equity_data, columns=['date', 'equity'])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Calculate returns and drawdown
    df['returns'] = df['equity'].pct_change()
    df['cum_returns'] = (1 + df['returns']).cumprod() - 1
    
    # Calculate drawdown
    df['peak'] = df['equity'].cummax()
    df['drawdown'] = (df['equity'] - df['peak']) / df['peak']
    
    # Create figure
    if show_drawdown:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
    else:
        fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot equity curve
    ax1.plot(df.index, df['equity'], linewidth=2)
    ax1.set_title(title, fontsize=16)
    ax1.set_ylabel('Equity ($)', fontsize=12)
    ax1.grid(True)
    
    # Format y-axis as currency
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'${x:,.0f}'))
    
    # Format x-axis dates
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    
    # Add initial capital reference line
    ax1.axhline(y=initial_capital, color='r', linestyle='--', alpha=0.7)
    ax1.text(df.index[0], initial_capital, f'Initial Capital: ${initial_capital:,.0f}', 
             va='bottom', ha='left', color='r')
    
    # Add annotations for final equity
    final_equity = df['equity'].iloc[-1]
    ax1.text(df.index[-1], final_equity, f'Final Equity: ${final_equity:,.0f}', 
             va='bottom', ha='right', color='green' if final_equity > initial_capital else 'red')
    
    # Add return info
    total_return_pct = (final_equity / initial_capital - 1) * 100
    ax1.text(0.02, 0.95, f'Total Return: {total_return_pct:.2f}%', 
             transform=ax1.transAxes, fontsize=12, 
             bbox=dict(facecolor='white', alpha=0.7))
    
    # Plot drawdown if requested
    if show_drawdown:
        ax2.fill_between(df.index, 0, df['drawdown']*100, color='red', alpha=0.3)
        ax2.plot(df.index, df['drawdown']*100, color='red', linewidth=1)
        ax2.set_ylabel('Drawdown (%)', fontsize=12)
        ax2.set_ylim(df['drawdown'].min()*110*100, 5)  # Add 10% margin to min drawdown
        ax2.grid(True)
        
        # Annotate maximum drawdown
        max_dd_idx = df['drawdown'].idxmin()
        max_dd = df.loc[max_dd_idx, 'drawdown']
        ax2.text(max_dd_idx, max_dd*100, f'Max DD: {max_dd*100:.2f}%', 
                va='top', ha='center', color='darkred', fontweight='bold')
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
    
    return fig

def plot_monthly_returns(equity_data: List[Tuple[str, float]],
                        title: str = "Monthly Returns",
                        output_path: Optional[str] = None):
    """
    Plot monthly returns heatmap from backtest results
    
    Args:
        equity_data: List of (datetime_str, equity) tuples
        title: Plot title
        output_path: Path to save the plot (if None, display plot)
    """
    # Convert data to DataFrame
    df = pd.DataFrame(equity_data, columns=['date', 'equity'])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Calculate returns
    df['returns'] = df['equity'].pct_change()
    
    # Resample to get monthly returns
    monthly_returns = df['returns'].resample('M').apply(lambda x: (1 + x).prod() - 1)
    
    # Create a new DataFrame with years as rows and months as columns
    returns_by_month = pd.DataFrame({
        'Year': monthly_returns.index.year,
        'Month': monthly_returns.index.month,
        'Return': monthly_returns.values
    })
    
    # Pivot the data
    pivot_table = returns_by_month.pivot('Year', 'Month', 'Return')
    
    # Plot heatmap
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create heatmap with custom colormap
    cmap = plt.cm.RdYlGn  # Red for negative, yellow for neutral, green for positive
    im = ax.imshow(pivot_table, cmap=cmap, aspect='auto', vmin=-0.1, vmax=0.1)
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax, pad=0.01)
    cbar.set_label('Return (%)', rotation=270, labelpad=20)
    
    # Set ticks and labels
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    ax.set_xticks(np.arange(len(month_labels)))
    ax.set_xticklabels(month_labels)
    
    ax.set_yticks(np.arange(len(pivot_table.index)))
    ax.set_yticklabels(pivot_table.index)
    
    # Rotate month labels
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
    
    # Add text annotations
    for i in range(len(pivot_table.index)):
        for j in range(len(pivot_table.columns)):
            value = pivot_table.iloc[i, j]
            if not np.isnan(value):
                text_color = 'white' if abs(value) > 0.05 else 'black'
                ax.text(j, i, f'{value*100:.1f}%', 
                       ha="center", va="center", color=text_color, fontweight='bold')
    
    ax.set_title(title, fontsize=16)
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
    
    return fig

def plot_trade_distribution(trades: List[Dict],
                           title: str = "Trade Distribution",
                           output_path: Optional[str] = None):
    """
    Plot trade distribution metrics including P&L distribution, 
    duration distribution, and win/loss by instrument
    
    Args:
        trades: List of trade dictionaries
        title: Plot title
        output_path: Path to save the plot (if None, display plot)
    """
    # Convert to DataFrame
    df = pd.DataFrame(trades)
    
    # Calculate trade durations
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    df['exit_time'] = pd.to_datetime(df['exit_time'])
    df['duration'] = (df['exit_time'] - df['entry_time']).dt.total_seconds() / 3600  # in hours
    
    # Create figure with subplots
    fig = plt.figure(figsize=(15, 12))
    gs = fig.add_gridspec(3, 2)
    
    # 1. P&L Distribution histogram
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.hist(df['pnl'], bins=20, color='skyblue', edgecolor='black')
    ax1.axvline(x=0, color='red', linestyle='--')
    ax1.set_title('P&L Distribution')
    ax1.set_xlabel('Profit/Loss ($)')
    ax1.set_ylabel('Number of Trades')
    
    # Add mean and median annotations
    mean_pnl = df['pnl'].mean()
    median_pnl = df['pnl'].median()
    ax1.text(0.05, 0.95, f'Mean P&L: ${mean_pnl:.2f}\nMedian P&L: ${median_pnl:.2f}',
             transform=ax1.transAxes, fontsize=10,
             bbox=dict(facecolor='white', alpha=0.7))
    
    # 2. Trade Duration histogram
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.hist(df['duration'], bins=20, color='lightgreen', edgecolor='black')
    ax2.set_title('Trade Duration Distribution')
    ax2.set_xlabel('Duration (hours)')
    ax2.set_ylabel('Number of Trades')
    
    # Add mean and median annotations
    mean_duration = df['duration'].mean()
    median_duration = df['duration'].median()
    ax2.text(0.05, 0.95, f'Mean Duration: {mean_duration:.1f} hours\nMedian Duration: {median_duration:.1f} hours',
             transform=ax2.transAxes, fontsize=10,
             bbox=dict(facecolor='white', alpha=0.7))
    
    # 3. Win/Loss by Symbol
    ax3 = fig.add_subplot(gs[1, :])
    
    # Group by symbol and calculate win count and loss count
    symbol_performance = df.groupby('symbol').apply(
        lambda x: pd.Series({
            'win_count': (x['pnl'] > 0).sum(),
            'loss_count': (x['pnl'] < 0).sum(),
            'breakeven_count': (x['pnl'] == 0).sum(),
            'total_profit': x[x['pnl'] > 0]['pnl'].sum(),
            'total_loss': x[x['pnl'] < 0]['pnl'].sum(),
            'net_pnl': x['pnl'].sum()
        })
    )
    
    # Calculate win rate
    symbol_performance['win_rate'] = symbol_performance['win_count'] / (
        symbol_performance['win_count'] + symbol_performance['loss_count']
    )
    
    # Sort by net P&L
    symbol_performance = symbol_performance.sort_values('net_pnl', ascending=False)
    
    # Create stacked bar chart
    symbols = symbol_performance.index
    win_counts = symbol_performance['win_count']
    loss_counts = symbol_performance['loss_count']
    
    ax3.bar(symbols, win_counts, label='Wins', color='green', alpha=0.7)
    ax3.bar(symbols, loss_counts, bottom=win_counts, label='Losses', color='red', alpha=0.7)
    
    # Add win rate as text
    for i, symbol in enumerate(symbols):
        win_rate = symbol_performance.loc[symbol, 'win_rate'] * 100
        ax3.text(i, win_counts[i] + loss_counts[i] + 0.5, f'{win_rate:.1f}%',
                ha='center', va='bottom', fontweight='bold')
    
    ax3.set_title('Trade Performance by Symbol')
    ax3.set_xlabel('Symbol')
    ax3.set_ylabel('Number of Trades')
    ax3.legend()
    
    # 4. P&L by Month
    ax4 = fig.add_subplot(gs[2, :])
    
    # Extract month and year
    df['month'] = df['exit_time'].dt.to_period('M')
    
    # Group by month and calculate P&L
    monthly_pnl = df.groupby('month')['pnl'].sum()
    
    # Plot as bar chart
    months = monthly_pnl.index.astype(str)
    ax4.bar(months, monthly_pnl, color=['green' if x > 0 else 'red' for x in monthly_pnl])
    
    # Rotate x-axis labels
    ax4.set_xticklabels(months, rotation=45, ha='right')
    
    ax4.set_title('P&L by Month')
    ax4.set_xlabel('Month')
    ax4.set_ylabel('P&L ($)')
    
    # Add grid lines
    ax4.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Add cumulative P&L line
    ax_twin = ax4.twinx()
    cumulative_pnl = monthly_pnl.cumsum()
    ax_twin.plot(months, cumulative_pnl, color='blue', marker='o', linestyle='-', linewidth=2)
    ax_twin.set_ylabel('Cumulative P&L ($)', color='blue')
    
    # Set title for entire figure
    fig.suptitle(title, fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.97])  # Adjust to make room for title
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
    
    return fig

def generate_performance_report(results_file: str, output_dir: str = "reports"):
    """
    Generate a complete performance report from backtest results
    
    Args:
        results_file: Path to JSON results file
        output_dir: Directory to save reports and charts
    
    Returns:
        Dict with report metrics and paths to generated files
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Load results
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    # Extract components
    config = results.get('config', {})
    trades = results.get('trades', [])
    equity_curve = results.get('equity_curve', [])
    metrics = results.get('metrics', {})
    
    # Generate timestamp for report files
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    base_filename = f"{config.get('symbols', ['UNKNOWN'])[0]}_{config.get('timeframe', 'UNKNOWN')}_{timestamp}"
    
    # Plot equity curve
    equity_plot_path = os.path.join(output_dir, f"{base_filename}_equity.png")
    plot_equity_curve(equity_curve, 
                     title=f"Equity Curve - {config.get('symbols', [''])[0]} {config.get('timeframe', '')}",
                     output_path=equity_plot_path,
                     initial_capital=config.get('initial_capital', 100000.0))
    
    # Plot monthly returns
    monthly_returns_path = os.path.join(output_dir, f"{base_filename}_monthly.png")
    plot_monthly_returns(equity_curve,
                        title=f"Monthly Returns - {config.get('symbols', [''])[0]} {config.get('timeframe', '')}",
                        output_path=monthly_returns_path)
    
    # Plot trade distribution
    trade_dist_path = os.path.join(output_dir, f"{base_filename}_trades.png")
    plot_trade_distribution(trades,
                           title=f"Trade Analysis - {config.get('symbols', [''])[0]} {config.get('timeframe', '')}",
                           output_path=trade_dist_path)
    
    # Generate report summary
    report_summary = {
        "backtest_info": {
            "symbols": config.get('symbols', []),
            "timeframe": config.get('timeframe', ''),
            "start_date": config.get('start_date', ''),
            "end_date": config.get('end_date', ''),
            "initial_capital": config.get('initial_capital', 0)
        },
        "metrics": metrics,
        "plots": {
            "equity_curve": equity_plot_path,
            "monthly_returns": monthly_returns_path,
            "trade_distribution": trade_dist_path
        }
    }
    
    # Save report summary
    report_path = os.path.join(output_dir, f"{base_filename}_report.json")
    with open(report_path, 'w') as f:
        json.dump(report_summary, f, indent=4)
    
    return report_summary

def compare_strategies(results_files: List[str], 
                      labels: List[str],
                      output_path: Optional[str] = None):
    """
    Compare multiple strategy backtest results
    
    Args:
        results_files: List of paths to JSON results files
        labels: List of strategy labels (same length as results_files)
        output_path: Path to save the comparison plot (if None, display plot)
    
    Returns:
        Matplotlib figure
    """
    if len(results_files) != len(labels):
        raise ValueError("Number of results files must match number of labels")
    
    # Load results
    equity_curves = []
    for result_file in results_files:
        with open(result_file, 'r') as f:
            results = json.load(f)
            equity_curves.append(results.get('equity_curve', []))
    
    # Convert to DataFrames and normalize
    dfs = []
    for i, equity_data in enumerate(equity_curves):
        df = pd.DataFrame(equity_data, columns=['date', 'equity'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # Normalize to 100 at start
        start_equity = df['equity'].iloc[0]
        df['normalized'] = df['equity'] * 100 / start_equity
        
        # Label the data
        df['strategy'] = labels[i]
        dfs.append(df)
    
    # Combine into a single DataFrame
    combined = pd.concat(dfs)
    
    # Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    for label in labels:
        strategy_data = combined[combined['strategy'] == label]
        ax.plot(strategy_data.index, strategy_data['normalized'], label=label, linewidth=2)
    
    ax.set_title('Strategy Comparison (Normalized to 100)', fontsize=16)
    ax.set_ylabel('Normalized Equity (%)', fontsize=12)
    ax.grid(True)
    ax.legend()
    
    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    
    # Add baseline reference
    ax.axhline(y=100, color='black', linestyle='--', alpha=0.5)
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
    
    return fig

def create_metrics_table(results_files: List[str], labels: List[str]):
    """
    Create a comparison table of metrics from multiple backtest results
    
    Args:
        results_files: List of paths to JSON results files
        labels: List of strategy labels (same length as results_files)
    
    Returns:
        DataFrame with metrics comparison
    """
    if len(results_files) != len(labels):
        raise ValueError("Number of results files must match number of labels")
    
    # Key metrics to extract
    key_metrics = [
        'win_rate', 'profit_factor', 'max_drawdown', 'sharpe_ratio', 
        'total_trades', 'net_profit', 'final_equity'
    ]
    
    # Load metrics from each file
    all_metrics = []
    for i, result_file in enumerate(results_files):
        with open(result_file, 'r') as f:
            results = json.load(f)
            metrics = results.get('metrics', {})
            
            metrics_dict = {
                'strategy': labels[i]
            }
            
            for metric in key_metrics:
                metrics_dict[metric] = metrics.get(metric, None)
            
            all_metrics.append(metrics_dict)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_metrics)
    
    # Format metrics
    if 'win_rate' in df:
        df['win_rate'] = df['win_rate'].apply(lambda x: f'{x*100:.2f}%' if x is not None else 'N/A')
    
    if 'max_drawdown' in df:
        df['max_drawdown'] = df['max_drawdown'].apply(lambda x: f'{x*100:.2f}%' if x is not None else 'N/A')
    
    if 'net_profit' in df:
        df['net_profit'] = df['net_profit'].apply(lambda x: f'${x:,.2f}' if x is not None else 'N/A')
    
    if 'final_equity' in df:
        df['final_equity'] = df['final_equity'].apply(lambda x: f'${x:,.2f}' if x is not None else 'N/A')
    
    # Set strategy as index
    df.set_index('strategy', inplace=True)
    
    return df

# Helper function to create dashboard HTML
def create_html_dashboard(report_summary: Dict, output_file: str):
    """
    Create an HTML dashboard for viewing backtest results
    
    Args:
        report_summary: Report summary dictionary
        output_file: Path to save HTML file
    """
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Backtest Results Dashboard</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 20px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }
            h1, h2, h3 {
                color: #333;
            }
            .header {
                background-color: #2c3e50;
                color: white;
                padding: 20px;
                margin-bottom: 20px;
            }
            .metric-box {
                background-color: #f9f9f9;
                border: 1px solid #ddd;
                padding: 15px;
                margin-bottom: 10px;
                border-radius: 5px;
            }
            .metric-title {
                font-weight: bold;
                margin-bottom: 5px;
            }
            .metric-value {
                font-size: 24px;
                color: #2c3e50;
            }
            .positive {
                color: green;
            }
            .negative {
                color: red;
            }
            .plot-container {
                margin: 20px 0;
            }
            .plot-container img {
                max-width: 100%;
                height: auto;
                border: 1px solid #ddd;
            }
            .flex-container {
                display: flex;
                flex-wrap: wrap;
                gap: 20px;
            }
            .flex-item {
                flex: 1;
                min-width: 200px;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Backtest Results Dashboard</h1>
                <p>Strategy: {strategy_name} | Timeframe: {timeframe} | Period: {start_date} to {end_date}</p>
            </div>
            
            <h2>Performance Summary</h2>
            <div class="flex-container">
                <div class="flex-item metric-box">
                    <div class="metric-title">Net Profit</div>
                    <div class="metric-value {profit_class}">${net_profit}</div>
                </div>
                <div class="flex-item metric-box">
                    <div class="metric-title">Final Equity</div>
                    <div class="metric-value">${final_equity}</div>
                </div>
                <div class="flex-item metric-box">
                    <div class="metric-title">Win Rate</div>
                    <div class="metric-value">{win_rate}%</div>
                </div>
                <div class="flex-item metric-box">
                    <div class="metric-title">Profit Factor</div>
                    <div class="metric-value">{profit_factor}</div>
                </div>
            </div>
            
            <div class="flex-container">
                <div class="flex-item metric-box">
                    <div class="metric-title">Max Drawdown</div>
                    <div class="metric-value negative">{max_drawdown}%</div>
                </div>
                <div class="flex-item metric-box">
                    <div class="metric-title">Sharpe Ratio</div>
                    <div class="metric-value">{sharpe_ratio}</div>
                </div>
                <div class="flex-item metric-box">
                    <div class="metric-title">Total Trades</div>
                    <div class="metric-value">{total_trades}</div>
                </div>
                <div class="flex-item metric-box">
                    <div class="metric-title">Avg Profit Per Trade</div>
                    <div class="metric-value {avg_profit_class}">${avg_profit}</div>
                </div>
            </div>
            
            <h2>Equity Curve</h2>
            <div class="plot-container">
                <img src="{equity_curve_path}" alt="Equity Curve">
            </div>
            
            <h2>Monthly Returns</h2>
            <div class="plot-container">
                <img src="{monthly_returns_path}" alt="Monthly Returns">
            </div>
            
            <h2>Trade Analysis</h2>
            <div class="plot-container">
                <img src="{trade_distribution_path}" alt="Trade Distribution">
            </div>
            
            <h2>Detailed Metrics</h2>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                {metrics_table_rows}
            </table>
            
            <div style="text-align: center; margin-top: 50px; color: #777;">
                <p>Generated on {generation_date} | FX Trading Bridge</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Extract data from report summary
    backtest_info = report_summary.get('backtest_info', {})
    metrics = report_summary.get('metrics', {})
    plots = report_summary.get('plots', {})
    
    # Format metrics for display
    net_profit = metrics.get('net_profit', 0)
    profit_class = "positive" if net_profit > 0 else "negative"
    
    avg_profit = 0
    if metrics.get('total_trades', 0) > 0:
        avg_profit = net_profit / metrics.get('total_trades', 1)
    avg_profit_class = "positive" if avg_profit > 0 else "negative"
    
    # Build metrics table rows
    metrics_table_rows = ""
    for key, value in sorted(metrics.items()):
        # Format value based on type
        formatted_value = value
        
        if isinstance(value, float):
            if key in ['win_rate', 'max_drawdown']:
                formatted_value = f"{value*100:.2f}%"
            elif key in ['net_profit', 'total_profit', 'total_loss', 'avg_profit', 'avg_loss', 'final_equity']:
                formatted_value = f"${value:.2f}"
            else:
                formatted_value = f"{value:.4f}"
        
        metrics_table_rows += f"""
        <tr>
            <td>{key.replace('_', ' ').title()}</td>
            <td>{formatted_value}</td>
        </tr>
        """
    
    # Generate HTML
    html_content = html_template.format(
        strategy_name=", ".join(backtest_info.get('symbols', ['Unknown'])),
        timeframe=backtest_info.get('timeframe', 'Unknown'),
        start_date=backtest_info.get('start_date', 'Unknown'),
        end_date=backtest_info.get('end_date', 'Unknown'),
        net_profit=f"{metrics.get('net_profit', 0):.2f}",
        profit_class=profit_class,
        final_equity=f"{metrics.get('final_equity', 0):.2f}",
        win_rate=f"{metrics.get('win_rate', 0)*100:.2f}",
        profit_factor=f"{metrics.get('profit_factor', 0):.2f}",
        max_drawdown=f"{metrics.get('max_drawdown', 0)*100:.2f}",
        sharpe_ratio=f"{metrics.get('sharpe_ratio', 0):.2f}",
        total_trades=metrics.get('total_trades', 0),
        avg_profit=f"{avg_profit:.2f}",
        avg_profit_class=avg_profit_class,
        equity_curve_path=os.path.basename(plots.get('equity_curve', '')),
        monthly_returns_path=os.path.basename(plots.get('monthly_returns', '')),
        trade_distribution_path=os.path.basename(plots.get('trade_distribution', '')),
        metrics_table_rows=metrics_table_rows,
        generation_date=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )
    
    # Write HTML file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    return output_file 