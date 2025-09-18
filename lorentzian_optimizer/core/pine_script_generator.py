"""
Pine Script Generator for Lorentzian Classification
Generates optimized Pine Scripts with parameter injection
"""

import logging
from typing import Dict, Any
import os

logger = logging.getLogger(__name__)

class PineScriptGenerator:
    """Generates Pine Script code with optimized parameters"""
    
    def __init__(self):
        self.template_path = os.path.join(os.path.dirname(__file__), "..", "templates", "lorentzian_template.pine")
        self.backtest_template_path = os.path.join(os.path.dirname(__file__), "..", "templates", "backtest_template.pine")
    
    def generate_script(self, parameters: Dict[str, Any]) -> str:
        """Generate Pine Script with given parameters"""
        try:
            # Load template
            with open(self.template_path, 'r') as f:
                template = f.read()
            
            # Replace parameters in template
            script = template.format(**parameters)
            
            logger.info(f"Generated Pine Script with parameters: {parameters}")
            return script
            
        except Exception as e:
            logger.error(f"Failed to generate Pine Script: {e}")
            return ""
    
    def generate_backtest_script(self, parameters: Dict[str, Any]) -> str:
        """Generate backtest strategy script"""
        try:
            # Load backtest template
            with open(self.backtest_template_path, 'r') as f:
                template = f.read()
            
            # Replace parameters in template
            script = template.format(**parameters)
            
            logger.info(f"Generated backtest script with parameters: {parameters}")
            return script
            
        except Exception as e:
            logger.error(f"Failed to generate backtest script: {e}")
            return ""
