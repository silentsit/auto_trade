from flask import Flask, render_template, request, jsonify, send_from_directory
import os
import json
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Configuration
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'reports')
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')

# Create template directory if it doesn't exist
os.makedirs(TEMPLATE_DIR, exist_ok=True)

@app.route('/')
def dashboard():
    """Home page showing list of available backtest results"""
    backtest_files = []
    
    # Get all JSON result files
    if os.path.exists(RESULTS_DIR):
        for file in os.listdir(RESULTS_DIR):
            if file.endswith('.json'):
                result_path = os.path.join(RESULTS_DIR, file)
                try:
                    with open(result_path, 'r') as f:
                        data = json.load(f)
                        
                    # Extract key information
                    strategy_name = data.get('strategy_name', 'Unknown')
                    run_date = data.get('run_date', 'Unknown')
                    total_return = data.get('metrics', {}).get('total_return', 0)
                    
                    backtest_files.append({
                        'filename': file,
                        'strategy': strategy_name,
                        'date': run_date,
                        'return': f"{total_return:.2f}%"
                    })
                except Exception as e:
                    print(f"Error loading {file}: {e}")
    
    return render_template('dashboard.html', backtests=backtest_files)

@app.route('/backtest/<filename>')
def backtest_detail(filename):
    """Detail page for a specific backtest result"""
    result_path = os.path.join(RESULTS_DIR, filename)
    
    if not os.path.exists(result_path):
        return "Backtest not found", 404
    
    with open(result_path, 'r') as f:
        data = json.load(f)
    
    # Prepare data for templates
    metrics = data.get('metrics', {})
    trades = data.get('trades', [])
    equity_curve = data.get('equity_curve', [])
    
    return render_template('backtest_detail.html', 
                           filename=filename,
                           strategy=data.get('strategy_name', 'Unknown'),
                           metrics=metrics,
                           trades=trades,
                           equity_data=equity_curve)

@app.route('/compare', methods=['GET', 'POST'])
def compare_backtests():
    """Compare multiple backtest results"""
    if request.method == 'POST':
        selected_files = request.form.getlist('selected_backtests')
        results = []
        
        for filename in selected_files:
            result_path = os.path.join(RESULTS_DIR, filename)
            if os.path.exists(result_path):
                with open(result_path, 'r') as f:
                    data = json.load(f)
                results.append({
                    'filename': filename,
                    'strategy': data.get('strategy_name', 'Unknown'),
                    'metrics': data.get('metrics', {}),
                    'equity_curve': data.get('equity_curve', [])
                })
        
        return render_template('comparison.html', results=results)
    
    # GET request - show selection form
    backtest_files = []
    if os.path.exists(RESULTS_DIR):
        backtest_files = [f for f in os.listdir(RESULTS_DIR) if f.endswith('.json')]
    
    return render_template('compare_form.html', backtest_files=backtest_files)

@app.route('/api/results')
def api_results():
    """API endpoint to get all backtest results as JSON"""
    results = []
    
    if os.path.exists(RESULTS_DIR):
        for file in os.listdir(RESULTS_DIR):
            if file.endswith('.json'):
                result_path = os.path.join(RESULTS_DIR, file)
                try:
                    with open(result_path, 'r') as f:
                        data = json.load(f)
                    results.append({
                        'filename': file,
                        'strategy': data.get('strategy_name', 'Unknown'),
                        'metrics': data.get('metrics', {})
                    })
                except Exception as e:
                    print(f"Error loading {file}: {e}")
    
    return jsonify(results)

@app.route('/static/<path:path>')
def serve_static(path):
    """Serve static files"""
    return send_from_directory('static', path)

def create_templates():
    """Create the HTML templates needed for the application"""
    templates = {
        'dashboard.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Backtest Results Dashboard</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container mt-4">
        <h1>Backtest Results Dashboard</h1>
        
        <div class="card mt-4">
            <div class="card-header">
                Available Backtests
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Strategy</th>
                                <th>Run Date</th>
                                <th>Total Return</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for backtest in backtests %}
                            <tr>
                                <td>{{ backtest.strategy }}</td>
                                <td>{{ backtest.date }}</td>
                                <td>{{ backtest.return }}</td>
                                <td>
                                    <a href="/backtest/{{ backtest.filename }}" class="btn btn-primary btn-sm">View</a>
                                </td>
                            </tr>
                            {% else %}
                            <tr>
                                <td colspan="4" class="text-center">No backtest results found</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
                
                <div class="mt-3">
                    <a href="/compare" class="btn btn-success">Compare Backtests</a>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
''',
        'backtest_detail.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Backtest Details</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container mt-4">
        <a href="/" class="btn btn-outline-secondary mb-3">← Back to Dashboard</a>
        
        <h1>{{ strategy }} Backtest Results</h1>
        
        <!-- Equity Curve -->
        <div class="card mt-4">
            <div class="card-header">
                Equity Curve
            </div>
            <div class="card-body">
                <canvas id="equityChart" height="300"></canvas>
            </div>
        </div>
        
        <!-- Metrics Summary -->
        <div class="card mt-4">
            <div class="card-header">
                Performance Metrics
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-3">
                        <div class="card bg-light">
                            <div class="card-body text-center">
                                <h5>Total Return</h5>
                                <h3>{{ metrics.total_return|round(2) }}%</h3>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card bg-light">
                            <div class="card-body text-center">
                                <h5>Sharpe Ratio</h5>
                                <h3>{{ metrics.sharpe_ratio|round(2) }}</h3>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card bg-light">
                            <div class="card-body text-center">
                                <h5>Max Drawdown</h5>
                                <h3>{{ metrics.max_drawdown|round(2) }}%</h3>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card bg-light">
                            <div class="card-body text-center">
                                <h5>Win Rate</h5>
                                <h3>{{ metrics.win_rate|round(2) }}%</h3>
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="table-responsive mt-4">
                    <table class="table table-sm">
                        <thead>
                            <tr>
                                <th>Metric</th>
                                <th>Value</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for key, value in metrics.items() %}
                            <tr>
                                <td>{{ key|replace('_', ' ')|title }}</td>
                                <td>{{ value|round(4) if value is number else value }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <!-- Trades List -->
        <div class="card mt-4">
            <div class="card-header">
                Trades
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-sm">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Symbol</th>
                                <th>Direction</th>
                                <th>Entry Time</th>
                                <th>Entry Price</th>
                                <th>Exit Time</th>
                                <th>Exit Price</th>
                                <th>P/L</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for trade in trades %}
                            <tr class="{{ 'text-success' if trade.pnl > 0 else 'text-danger' if trade.pnl < 0 else '' }}">
                                <td>{{ loop.index }}</td>
                                <td>{{ trade.symbol }}</td>
                                <td>{{ trade.direction }}</td>
                                <td>{{ trade.entry_time }}</td>
                                <td>{{ trade.entry_price }}</td>
                                <td>{{ trade.exit_time }}</td>
                                <td>{{ trade.exit_price }}</td>
                                <td>{{ trade.pnl|round(2) }}%</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Parse equity curve data
        const equityData = {{ equity_data|tojson }};
        const dates = equityData.map(point => point[0]);
        const values = equityData.map(point => point[1]);
        
        // Create equity chart
        const ctx = document.getElementById('equityChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: dates,
                datasets: [{
                    label: 'Equity Curve',
                    data: values,
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension: 0.1,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return `Equity: $${context.raw.toFixed(2)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        title: {
                            display: true,
                            text: 'Equity ($)'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Date'
                        }
                    }
                }
            }
        });
    </script>
</body>
</html>
''',
        'compare_form.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Compare Backtests</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css">
</head>
<body>
    <div class="container mt-4">
        <a href="/" class="btn btn-outline-secondary mb-3">← Back to Dashboard</a>
        
        <h1>Compare Backtest Results</h1>
        
        <div class="card mt-4">
            <div class="card-header">
                Select Backtests to Compare
            </div>
            <div class="card-body">
                <form method="post" action="/compare">
                    <div class="form-group">
                        {% for file in backtest_files %}
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" name="selected_backtests" value="{{ file }}" id="check_{{ loop.index }}">
                            <label class="form-check-label" for="check_{{ loop.index }}">
                                {{ file }}
                            </label>
                        </div>
                        {% endfor %}
                    </div>
                    
                    <button type="submit" class="btn btn-primary mt-3">Compare Selected</button>
                </form>
            </div>
        </div>
    </div>
</body>
</html>
''',
        'comparison.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Backtest Comparison</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container mt-4">
        <a href="/" class="btn btn-outline-secondary mb-3">← Back to Dashboard</a>
        
        <h1>Backtest Comparison</h1>
        
        <!-- Equity Curves Comparison -->
        <div class="card mt-4">
            <div class="card-header">
                Equity Curves Comparison
            </div>
            <div class="card-body">
                <canvas id="comparisonChart" height="400"></canvas>
            </div>
        </div>
        
        <!-- Metrics Comparison -->
        <div class="card mt-4">
            <div class="card-header">
                Performance Metrics Comparison
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Metric</th>
                                {% for result in results %}
                                <th>{{ result.strategy }}</th>
                                {% endfor %}
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>Total Return</td>
                                {% for result in results %}
                                <td>{{ result.metrics.total_return|round(2) }}%</td>
                                {% endfor %}
                            </tr>
                            <tr>
                                <td>Sharpe Ratio</td>
                                {% for result in results %}
                                <td>{{ result.metrics.sharpe_ratio|round(2) }}</td>
                                {% endfor %}
                            </tr>
                            <tr>
                                <td>Max Drawdown</td>
                                {% for result in results %}
                                <td>{{ result.metrics.max_drawdown|round(2) }}%</td>
                                {% endfor %}
                            </tr>
                            <tr>
                                <td>Win Rate</td>
                                {% for result in results %}
                                <td>{{ result.metrics.win_rate|round(2) }}%</td>
                                {% endfor %}
                            </tr>
                            <!-- Add more metrics as needed -->
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Prepare data for comparison chart
        const datasets = [];
        const colors = [
            'rgba(75, 192, 192, 1)',
            'rgba(255, 99, 132, 1)',
            'rgba(54, 162, 235, 1)',
            'rgba(255, 206, 86, 1)',
            'rgba(153, 102, 255, 1)'
        ];
        
        {% for result in results %}
        datasets.push({
            label: '{{ result.strategy }}',
            data: {{ result.equity_curve|tojson }}.map(point => ({x: point[0], y: point[1]})),
            borderColor: colors[{{ loop.index0 }} % colors.length],
            backgroundColor: colors[{{ loop.index0 }} % colors.length].replace('1)', '0.2)'),
            tension: 0.1,
            fill: false
        });
        {% endfor %}
        
        // Create comparison chart
        const ctx = document.getElementById('comparisonChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'day'
                        },
                        title: {
                            display: true,
                            text: 'Date'
                        }
                    },
                    y: {
                        beginAtZero: false,
                        title: {
                            display: true,
                            text: 'Equity ($)'
                        }
                    }
                }
            }
        });
    </script>
</body>
</html>
'''
    }
    
    # Create template files
    for filename, content in templates.items():
        template_path = os.path.join(TEMPLATE_DIR, filename)
        os.makedirs(os.path.dirname(template_path), exist_ok=True)
        
        with open(template_path, 'w') as f:
            f.write(content)

def main():
    """Create templates and start the Flask server"""
    create_templates()
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == '__main__':
    main() 