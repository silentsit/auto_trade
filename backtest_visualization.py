import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import os
import json
from typing import Dict, List, Optional, Union, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BacktestVisualization")

class BacktestVisualizer:
    """
    Class for visualizing backtest results using Plotly and Dash.
    Provides interactive charts and performance metrics dashboards.
    """
    
    def __init__(self, results_dir: str = "data/backtest_results"):
        """
        Initialize the BacktestVisualizer
        
        Args:
            results_dir: Directory where backtest results are stored
        """
        self.results_dir = results_dir
        self.current_results = None
        self.app = None
        
        # Create results directory if it doesn't exist
        os.makedirs(self.results_dir, exist_ok=True)
        
    def load_results(self, backtest_id: str = None, result_data: Dict = None) -> Dict:
        """
        Load backtest results either from a file or directly from data
        
        Args:
            backtest_id: ID of the backtest to load
            result_data: Direct result data to use
            
        Returns:
            Dictionary containing the backtest results
        """
        if result_data:
            self.current_results = result_data
            return self.current_results
            
        if backtest_id:
            file_path = os.path.join(self.results_dir, f"{backtest_id}.json")
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        self.current_results = json.load(f)
                    return self.current_results
                except Exception as e:
                    logger.error(f"Error loading backtest results: {str(e)}")
                    raise
            else:
                logger.error(f"Backtest results file not found: {file_path}")
                raise FileNotFoundError(f"Backtest results file not found: {file_path}")
        
        # If no specific results requested, return the most recent
        result_files = [f for f in os.listdir(self.results_dir) if f.endswith('.json')]
        if not result_files:
            logger.error("No backtest results found")
            raise FileNotFoundError("No backtest results found")
            
        most_recent = max(result_files, key=lambda f: os.path.getmtime(os.path.join(self.results_dir, f)))
        try:
            with open(os.path.join(self.results_dir, most_recent), 'r') as f:
                self.current_results = json.load(f)
            return self.current_results
        except Exception as e:
            logger.error(f"Error loading most recent backtest results: {str(e)}")
            raise
    
    def generate_equity_curve(self, 
                             include_drawdowns: bool = True) -> go.Figure:
        """
        Generate an equity curve visualization
        
        Args:
            include_drawdowns: Whether to include drawdown visualization
            
        Returns:
            Plotly figure object
        """
        if not self.current_results:
            logger.error("No backtest results loaded")
            raise ValueError("No backtest results loaded")
            
        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Extract equity curve data
        equity_data = self.current_results.get("equity_curve", [])
        if not equity_data:
            logger.error("Equity curve data not found in results")
            raise ValueError("Equity curve data not found in results")
            
        dates = [item.get("timestamp") for item in equity_data]
        equity = [item.get("equity") for item in equity_data]
        
        # Add equity curve
        fig.add_trace(
            go.Scatter(x=dates, y=equity, name="Equity Curve", line=dict(color="green", width=2)),
            secondary_y=False,
        )
        
        # Add drawdowns if requested
        if include_drawdowns:
            drawdowns = self._calculate_drawdowns(equity)
            fig.add_trace(
                go.Scatter(x=dates, y=drawdowns, name="Drawdown", 
                          fill='tozeroy', line=dict(color="red", width=1)),
                secondary_y=True,
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Equity", secondary_y=False)
            fig.update_yaxes(title_text="Drawdown %", secondary_y=True)
        
        # Add trade markers
        trades = self.current_results.get("trades", [])
        buy_dates = [trade.get("entry_time") for trade in trades if trade.get("direction") == "BUY"]
        buy_values = [equity[dates.index(date)] if date in dates else None for date in buy_dates]
        
        sell_dates = [trade.get("entry_time") for trade in trades if trade.get("direction") == "SELL"]
        sell_values = [equity[dates.index(date)] if date in dates else None for date in sell_dates]
        
        exit_dates = [trade.get("exit_time") for trade in trades if trade.get("exit_time")]
        exit_values = [equity[dates.index(date)] if date in dates else None for date in exit_dates]
        
        # Add markers for trades
        fig.add_trace(
            go.Scatter(x=buy_dates, y=buy_values, mode="markers", name="Buy Entry",
                      marker=dict(color="blue", size=8, symbol="triangle-up")),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(x=sell_dates, y=sell_values, mode="markers", name="Sell Entry",
                      marker=dict(color="purple", size=8, symbol="triangle-down")),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(x=exit_dates, y=exit_values, mode="markers", name="Trade Exit",
                      marker=dict(color="black", size=8, symbol="circle")),
            secondary_y=False,
        )
        
        # Set title and layout
        strategy_name = self.current_results.get("strategy", "Unknown")
        start_date = dates[0] if dates else "Unknown"
        end_date = dates[-1] if dates else "Unknown"
        
        fig.update_layout(
            title_text=f"Equity Curve: {strategy_name} ({start_date} to {end_date})",
            xaxis_title="Date",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white",
            hovermode="x unified"
        )
        
        return fig
    
    def _calculate_drawdowns(self, equity: List[float]) -> List[float]:
        """
        Calculate drawdowns as percentage from peak
        
        Args:
            equity: List of equity values
            
        Returns:
            List of drawdown percentages
        """
        drawdowns = []
        peak = equity[0]
        
        for value in equity:
            peak = max(peak, value)
            drawdown = (value - peak) / peak * 100  # Percentage
            drawdowns.append(drawdown)
            
        return drawdowns
    
    def generate_performance_metrics(self) -> go.Figure:
        """
        Generate a visual display of key performance metrics
        
        Returns:
            Plotly figure for performance metrics
        """
        if not self.current_results:
            logger.error("No backtest results loaded")
            raise ValueError("No backtest results loaded")
            
        metrics = self.current_results.get("metrics", {})
        if not metrics:
            logger.error("Performance metrics not found in results")
            raise ValueError("Performance metrics not found in results")
        
        # Extract key metrics
        key_metrics = {
            "Total Return (%)": metrics.get("total_return_pct", 0),
            "Win Rate (%)": metrics.get("win_rate", 0) * 100,
            "Profit Factor": metrics.get("profit_factor", 0),
            "Sharpe Ratio": metrics.get("sharpe_ratio", 0),
            "Max Drawdown (%)": abs(metrics.get("max_drawdown_pct", 0)),
            "Recovery Factor": metrics.get("recovery_factor", 0),
            "Avg Win/Loss Ratio": metrics.get("avg_win_loss_ratio", 0),
            "Trades Per Month": metrics.get("trades_per_month", 0)
        }
        
        # Create a figure for the metrics
        fig = go.Figure()
        
        # Add a gauge for total return
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = key_metrics["Total Return (%)"],
            title = {'text': "Total Return (%)"},
            gauge = {
                'axis': {'range': [None, max(50, key_metrics["Total Return (%)"] * 1.2)]},
                'bar': {'color': "green" if key_metrics["Total Return (%)"] > 0 else "red"},
                'steps': [
                    {'range': [0, 10], 'color': "lightgray"},
                    {'range': [10, 20], 'color': "gray"},
                    {'range': [20, 50], 'color': "darkgray"}
                ],
            },
            domain = {'row': 0, 'column': 0}
        ))
        
        # Add a gauge for win rate
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = key_metrics["Win Rate (%)"],
            title = {'text': "Win Rate (%)"},
            gauge = {
                'axis': {'range': [0, 100]},
                'bar': {'color': "blue"},
                'steps': [
                    {'range': [0, 40], 'color': "lightgray"},
                    {'range': [40, 60], 'color': "gray"},
                    {'range': [60, 100], 'color': "darkgray"}
                ],
            },
            domain = {'row': 0, 'column': 1}
        ))
        
        # Add a gauge for Sharpe Ratio
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = key_metrics["Sharpe Ratio"],
            title = {'text': "Sharpe Ratio"},
            gauge = {
                'axis': {'range': [0, max(3, key_metrics["Sharpe Ratio"] * 1.2)]},
                'bar': {'color': "purple"},
                'steps': [
                    {'range': [0, 1], 'color': "lightgray"},
                    {'range': [1, 2], 'color': "gray"},
                    {'range': [2, 3], 'color': "darkgray"}
                ],
            },
            domain = {'row': 1, 'column': 0}
        ))
        
        # Add a gauge for max drawdown
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = key_metrics["Max Drawdown (%)"],
            title = {'text': "Max Drawdown (%)"},
            gauge = {
                'axis': {'range': [0, max(20, key_metrics["Max Drawdown (%)"] * 1.2)]},
                'bar': {'color': "red"},
                'steps': [
                    {'range': [0, 5], 'color': "darkgray"},
                    {'range': [5, 10], 'color': "gray"},
                    {'range': [10, 20], 'color': "lightgray"}
                ],
            },
            domain = {'row': 1, 'column': 1}
        ))
        
        # Update layout
        strategy_name = self.current_results.get("strategy", "Unknown")
        fig.update_layout(
            title_text=f"Performance Metrics: {strategy_name}",
            grid = {'rows': 2, 'columns': 2, 'pattern': "independent"},
            template = "plotly_white"
        )
        
        return fig
    
    def generate_trade_distribution(self) -> go.Figure:
        """
        Generate a visualization of trade distribution
        
        Returns:
            Plotly figure for trade distribution
        """
        if not self.current_results:
            logger.error("No backtest results loaded")
            raise ValueError("No backtest results loaded")
            
        trades = self.current_results.get("trades", [])
        if not trades:
            logger.error("Trade data not found in results")
            raise ValueError("Trade data not found in results")
            
        # Extract profit/loss values
        pnl_values = [trade.get("pnl", 0) for trade in trades]
        
        # Create figure with 2 subplots
        fig = make_subplots(rows=1, cols=2, 
                           subplot_titles=("PnL Distribution", "Cumulative PnL"),
                           specs=[[{"type": "histogram"}, {"type": "scatter"}]])
        
        # Add histogram of PnL
        fig.add_trace(
            go.Histogram(
                x=pnl_values,
                name="PnL Distribution",
                marker_color=['green' if x >= 0 else 'red' for x in pnl_values],
                nbinsx=20
            ),
            row=1, col=1
        )
        
        # Add cumulative PnL
        cumulative_pnl = np.cumsum(pnl_values)
        trade_indices = list(range(1, len(pnl_values) + 1))
        
        fig.add_trace(
            go.Scatter(
                x=trade_indices,
                y=cumulative_pnl,
                mode='lines',
                name='Cumulative PnL',
                line=dict(color='blue', width=2)
            ),
            row=1, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="Trade Analysis",
            showlegend=False,
            template="plotly_white"
        )
        
        # Update x and y axis labels
        fig.update_xaxes(title_text="PnL Value", row=1, col=1)
        fig.update_yaxes(title_text="Frequency", row=1, col=1)
        
        fig.update_xaxes(title_text="Trade Number", row=1, col=2)
        fig.update_yaxes(title_text="Cumulative PnL", row=1, col=2)
        
        return fig
    
    def save_results_to_file(self, file_name: str = None) -> str:
        """
        Save current backtest results to a file
        
        Args:
            file_name: Optional custom filename (without extension)
            
        Returns:
            Path to the saved file
        """
        if not self.current_results:
            logger.error("No backtest results loaded")
            raise ValueError("No backtest results loaded")
            
        if not file_name:
            strategy = self.current_results.get("strategy", "unknown")
            timestamp = self.current_results.get("timestamp", pd.Timestamp.now().strftime("%Y%m%d_%H%M%S"))
            file_name = f"backtest_{strategy}_{timestamp}"
            
        file_path = os.path.join(self.results_dir, f"{file_name}.json")
        
        try:
            with open(file_path, 'w') as f:
                json.dump(self.current_results, f, indent=2)
            logger.info(f"Backtest results saved to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error saving backtest results: {str(e)}")
            raise
    
    def create_dashboard(self, host: str = "127.0.0.1", port: int = 8050, debug: bool = False) -> dash.Dash:
        """
        Create and run a Dash web application to display backtest results
        
        Args:
            host: Host to serve the dashboard on
            port: Port to serve the dashboard on
            debug: Whether to run in debug mode
            
        Returns:
            Dash application instance
        """
        app = dash.Dash(__name__)
        
        # Define layout
        app.layout = html.Div([
            html.H1("Forex Backtest Results Dashboard", style={'textAlign': 'center'}),
            
            html.Div([
                html.Label("Select Backtest:"),
                dcc.Dropdown(
                    id='backtest-dropdown',
                    options=[{'label': f.replace('.json', ''), 'value': f.replace('.json', '')} 
                             for f in os.listdir(self.results_dir) if f.endswith('.json')],
                    value=None,
                    placeholder="Select a backtest"
                )
            ], style={'padding': '20px', 'width': '50%'}),
            
            dcc.Tabs([
                dcc.Tab(label='Equity Curve', children=[
                    dcc.Graph(id='equity-curve')
                ]),
                dcc.Tab(label='Performance Metrics', children=[
                    dcc.Graph(id='performance-metrics')
                ]),
                dcc.Tab(label='Trade Distribution', children=[
                    dcc.Graph(id='trade-distribution')
                ]),
                dcc.Tab(label='Trade List', children=[
                    html.Div(id='trade-table')
                ])
            ])
        ])
        
        # Define callbacks
        @app.callback(
            [Output('equity-curve', 'figure'),
             Output('performance-metrics', 'figure'),
             Output('trade-distribution', 'figure'),
             Output('trade-table', 'children')],
            [Input('backtest-dropdown', 'value')]
        )
        def update_dashboard(selected_backtest):
            if not selected_backtest:
                # Use default/current results if available
                if not self.current_results:
                    try:
                        self.load_results()
                    except:
                        # Return empty figures if no results available
                        empty_fig = go.Figure()
                        empty_fig.update_layout(title="No backtest results selected")
                        return empty_fig, empty_fig, empty_fig, html.P("No backtest results selected")
                
            else:
                # Load selected backtest
                try:
                    self.load_results(backtest_id=selected_backtest)
                except:
                    empty_fig = go.Figure()
                    empty_fig.update_layout(title="Error loading backtest results")
                    return empty_fig, empty_fig, empty_fig, html.P("Error loading backtest results")
            
            # Generate figures
            equity_fig = self.generate_equity_curve()
            metrics_fig = self.generate_performance_metrics()
            trade_fig = self.generate_trade_distribution()
            
            # Generate trade table
            trades = self.current_results.get("trades", [])
            
            trade_table = html.Table([
                html.Thead(
                    html.Tr([
                        html.Th("Symbol"), html.Th("Direction"), 
                        html.Th("Entry Time"), html.Th("Exit Time"),
                        html.Th("Entry Price"), html.Th("Exit Price"),
                        html.Th("Size"), html.Th("PnL"), html.Th("Return (%)")
                    ])
                ),
                html.Tbody([
                    html.Tr([
                        html.Td(trade.get("symbol", "")),
                        html.Td(trade.get("direction", "")),
                        html.Td(trade.get("entry_time", "")),
                        html.Td(trade.get("exit_time", "")),
                        html.Td(f"{trade.get('entry_price', 0):.5f}"),
                        html.Td(f"{trade.get('exit_price', 0):.5f}"),
                        html.Td(f"{trade.get('size', 0):.2f}"),
                        html.Td(f"{trade.get('pnl', 0):.2f}", 
                                style={'color': 'green' if trade.get("pnl", 0) >= 0 else 'red'}),
                        html.Td(f"{trade.get('return_pct', 0):.2f}%",
                                style={'color': 'green' if trade.get("return_pct", 0) >= 0 else 'red'})
                    ]) for trade in trades
                ])
            ], style={'width': '100%', 'border': '1px solid black', 'border-collapse': 'collapse'})
            
            return equity_fig, metrics_fig, trade_fig, trade_table
        
        # Store app instance
        self.app = app
        
        # Run the app if needed
        if host and port:
            app.run_server(host=host, port=port, debug=debug)
            
        return app

# Example usage
def create_sample_results():
    """Create sample backtest results for testing"""
    np.random.seed(42)
    
    # Create sample equity curve
    days = 30
    initial_equity = 10000
    daily_returns = np.random.normal(0.001, 0.01, days)
    equity = initial_equity * (1 + np.cumsum(daily_returns))
    
    dates = [(pd.Timestamp.now() - pd.Timedelta(days=days-i)).strftime("%Y-%m-%d") 
             for i in range(days)]
    
    equity_curve = [{"timestamp": date, "equity": float(eq)} for date, eq in zip(dates, equity)]
    
    # Create sample trades
    trades = []
    for i in range(10):
        direction = "BUY" if np.random.random() > 0.5 else "SELL"
        entry_day = np.random.randint(0, days-5)
        exit_day = entry_day + np.random.randint(1, 5)
        
        if exit_day >= days:
            exit_day = days - 1
            
        pnl = np.random.normal(0, 100)
        
        trades.append({
            "id": i,
            "symbol": "EUR_USD",
            "direction": direction,
            "entry_time": dates[entry_day],
            "exit_time": dates[exit_day],
            "entry_price": 1.1000 + np.random.normal(0, 0.01),
            "exit_price": 1.1000 + np.random.normal(0, 0.01),
            "size": 10000,
            "pnl": float(pnl),
            "return_pct": float(pnl / 10000 * 100)
        })
    
    # Create sample metrics
    metrics = {
        "total_return_pct": float((equity[-1] - equity[0]) / equity[0] * 100),
        "win_rate": 0.65,
        "profit_factor": 1.75,
        "sharpe_ratio": 1.2,
        "max_drawdown_pct": -5.3,
        "recovery_factor": 1.9,
        "avg_win_loss_ratio": 1.5,
        "trades_per_month": 22
    }
    
    # Combine everything
    sample_results = {
        "strategy": "SMA_Crossover",
        "timestamp": pd.Timestamp.now().strftime("%Y%m%d_%H%M%S"),
        "equity_curve": equity_curve,
        "trades": trades,
        "metrics": metrics
    }
    
    return sample_results

if __name__ == "__main__":
    # Example usage
    visualizer = BacktestVisualizer()
    
    # Create and save sample results
    sample_results = create_sample_results()
    visualizer.load_results(result_data=sample_results)
    visualizer.save_results_to_file("sample_backtest")
    
    # Create and run dashboard
    visualizer.create_dashboard(debug=True) 