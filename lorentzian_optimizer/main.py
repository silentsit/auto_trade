"""
Main entry point for Lorentzian Classification Optimizer
"""

import logging
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from gui.main_window import LorentzianOptimizerGUI

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('optimization.log')
        ]
    )

def main():
    """Main entry point"""
    print("=" * 80)
    print("LORENTZIAN CLASSIFICATION OPTIMIZER v1.0")
    print("Institutional-Grade Parameter Optimization System")
    print("=" * 80)
    print(f"Started at: {logging.Formatter().formatTime(logging.LogRecord('', 0, '', 0, '', (), None))}")
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting desktop application...")
        
        # Create and run GUI
        app = LorentzianOptimizerGUI()
        app.run()
        
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
