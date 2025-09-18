"""
Main GUI Window for Lorentzian Classification Optimizer
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, messagebox
import threading
import logging
from typing import Dict, List, Optional
import time

from core.optimization_engine import GeneticOptimizer
from core.backtest_engine import TradingViewBacktestEngine
from core.results_analyzer import ResultsAnalyzer
from core.pine_script_generator import PineScriptGenerator
from config.parameters import PARAMETER_RANGES, CURRENCY_PAIRS

logger = logging.getLogger(__name__)

class LorentzianOptimizerGUI:
    """Main GUI application for the Lorentzian Classification Optimizer"""
    
    def __init__(self):
        self.root = ctk.CTk()
        self.root.title("Lorentzian Classification Optimizer v1.0")
        self.root.geometry("1200x800")
        
        # Initialize components
        self.optimizer = None
        self.backtest_engine = None
        self.results_analyzer = ResultsAnalyzer()
        self.pine_script_generator = PineScriptGenerator()
        
        # GUI state
        self.stop_requested = False
        self.optimization_running = False
        
        # Create GUI elements
        self._create_widgets()
        self._setup_logging()
        
    def _create_widgets(self):
        """Create and layout GUI widgets"""
        # Main frame
        main_frame = ctk.CTkFrame(self.root)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Title
        title_label = ctk.CTkLabel(
            main_frame, 
            text="Lorentzian Classification Optimizer",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        title_label.pack(pady=(10, 20))
        
        # Currency pairs selection
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(pairs_frame, text="Currency Pairs:", font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        self.pair_vars = {}
        pairs_grid = ctk.CTkFrame(pairs_frame)
        pairs_grid.pack(fill="x", padx=10, pady=5)
        
        for i, pair in enumerate(CURRENCY_PAIRS):
            var = ctk.BooleanVar(value=True)
            self.pair_vars[pair] = var
            
            checkbox = ctk.CTkCheckBox(
                pairs_grid, 
                text=pair, 
                variable=var,
                font=ctk.CTkFont(size=12)
            )
            checkbox.grid(row=i//4, column=i%4, sticky="w", padx=5, pady=2)
        
        # Control buttons
        control_frame = ctk.CTkFrame(main_frame)
        control_frame.pack(fill="x", padx=10, pady=10)
        
        self.start_button = ctk.CTkButton(
            control_frame,
            text="Start Optimization",
            command=self._start_optimization,
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40
        )
        self.start_button.pack(side="left", padx=10, pady=10)
        
        self.stop_button = ctk.CTkButton(
            control_frame,
            text="Stop Optimization",
            command=self._stop_optimization,
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            state="disabled"
        )
        self.stop_button.pack(side="left", padx=10, pady=10)
        
        # Progress frame
        progress_frame = ctk.CTkFrame(main_frame)
        progress_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(progress_frame, text="Progress:", font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        self.progress_var = ctk.StringVar(value="Ready to start optimization")
        self.progress_label = ctk.CTkLabel(
            progress_frame, 
            textvariable=self.progress_var,
            font=ctk.CTkFont(size=12)
        )
        self.progress_label.pack(anchor="w", padx=10, pady=5)
        
        self.progress_bar = ctk.CTkProgressBar(progress_frame)
        self.progress_bar.pack(fill="x", padx=10, pady=5)
        self.progress_bar.set(0)
        
        # Results frame
        results_frame = ctk.CTkFrame(main_frame)
        results_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        ctk.CTkLabel(results_frame, text="Optimization Results:", font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Results text area
        self.results_text = ctk.CTkTextbox(
            results_frame,
            height=300,
            font=ctk.CTkFont(size=11)
        )
        self.results_text.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Status bar
        self.status_var = ctk.StringVar(value="Ready")
        status_bar = ctk.CTkLabel(
            self.root,
            textvariable=self.status_var,
            font=ctk.CTkFont(size=10)
        )
        status_bar.pack(side="bottom", fill="x", padx=5, pady=2)
    
    def _setup_logging(self):
        """Setup logging to display in GUI"""
        # Create a custom log handler that updates the GUI
        class GUILogHandler(logging.Handler):
            def __init__(self, text_widget):
                super().__init__()
                self.text_widget = text_widget
            
            def emit(self, record):
                msg = self.format(record)
                self.text_widget.insert("end", f"{msg}\n")
                self.text_widget.see("end")
        
        # Add handler to logger
        gui_handler = GUILogHandler(self.results_text)
        gui_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        gui_handler.setFormatter(formatter)
        logger.addHandler(gui_handler)
    
    def _start_optimization(self):
        """Start optimization process in separate thread"""
        if self.optimization_running:
            return
        
        # Get selected currency pairs
        selected_pairs = [pair for pair, var in self.pair_vars.items() if var.get()]
        
        if not selected_pairs:
            messagebox.showwarning("Warning", "Please select at least one currency pair")
            return
        
        # Update UI
        self.optimization_running = True
        self.stop_requested = False
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.progress_var.set("Initializing optimization...")
        self.progress_bar.set(0)
        self.results_text.delete("1.0", "end")
        
        # Start optimization in separate thread
        optimization_thread = threading.Thread(
            target=self._run_optimization,
            args=(selected_pairs,),
            daemon=True
        )
        optimization_thread.start()
    
    def _stop_optimization(self):
        """Stop optimization process"""
        self.stop_requested = True
        self.status_var.set("Stopping optimization...")
        logger.info("Stop requested by user")
    
    def _run_optimization(self, currency_pairs: List[str]):
        """Run optimization process"""
        try:
            logger.info(f"Starting optimization for pairs: {currency_pairs}")
            
            # Show Gmail login information
            self.progress_var.set("🔐 Chrome will open - please login with Gmail")
            self.results_text.insert("end", "🔐 IMPORTANT: Chrome will open for TradingView access\n")
            self.results_text.insert("end", "📧 Please login with your Gmail account when prompted\n")
            self.results_text.insert("end", "📱 If Google sends a notification to your phone, please approve it\n")
            self.results_text.insert("end", "⏳ The system will wait up to 10 minutes for login completion\n")
            self.results_text.insert("end", "✅ Once logged in, optimization will continue automatically\n\n")
            self.results_text.see("end")
            
            # Initialize backtest engine
            self.backtest_engine = TradingViewBacktestEngine(headless=False)
            
            # Initialize optimizer
            self.optimizer = GeneticOptimizer(
                backtest_engine=self.backtest_engine,
                results_analyzer=self.results_analyzer,
                pine_script_generator=self.pine_script_generator
            )
            
            # Run optimization
            results = self.optimizer.optimize(
                currency_pairs=currency_pairs,
                max_generations=50,
                population_size=20,
                stop_callback=lambda: self.stop_requested
            )
            
            # Display results
            self._display_results(results)
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            messagebox.showerror("Error", f"Optimization failed: {e}")
        finally:
            # Reset UI
            self.optimization_running = False
            self.start_button.configure(state="normal")
            self.stop_button.configure(state="disabled")
            self.progress_var.set("Optimization completed")
            self.status_var.set("Ready")
    
    def _display_results(self, results: Dict):
        """Display optimization results"""
        try:
            self.results_text.insert("end", "\n" + "="*80 + "\n")
            self.results_text.insert("end", "OPTIMIZATION RESULTS\n")
            self.results_text.insert("end", "="*80 + "\n\n")
            
            if not results:
                self.results_text.insert("end", "No results to display\n")
                return
            
            # Display results for each currency pair
            for pair, result in results.items():
                self.results_text.insert("end", f"Currency Pair: {pair}\n")
                self.results_text.insert("end", "-" * 40 + "\n")
                
                if 'best_parameters' in result:
                    self.results_text.insert("end", f"Best Parameters: {result['best_parameters']}\n")
                
                if 'best_score' in result:
                    self.results_text.insert("end", f"Best Score: {result['best_score']:.4f}\n")
                
                if 'metrics' in result:
                    metrics = result['metrics']
                    self.results_text.insert("end", f"Sharpe Ratio: {metrics.get('sharpe_ratio', 'N/A')}\n")
                    self.results_text.insert("end", f"Profit Factor: {metrics.get('profit_factor', 'N/A')}\n")
                    self.results_text.insert("end", f"P&L/DD Ratio: {metrics.get('pnl_dd_ratio', 'N/A')}\n")
                    self.results_text.insert("end", f"Total Trades: {metrics.get('total_trades', 'N/A')}\n")
                
                self.results_text.insert("end", "\n")
            
            self.results_text.see("end")
            
        except Exception as e:
            logger.error(f"Failed to display results: {e}")
    
    def run(self):
        """Start the GUI application"""
        self.root.mainloop()

def main():
    """Main entry point for GUI application"""
    app = LorentzianOptimizerGUI()
    app.run()

if __name__ == "__main__":
    main()
