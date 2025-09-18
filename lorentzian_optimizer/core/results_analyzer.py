"""
Results Analyzer for Lorentzian Classification Optimization
Analyzes backtest results and calculates optimization scores
"""

import logging
from typing import Dict, List, Any
import numpy as np

logger = logging.getLogger(__name__)

class ResultsAnalyzer:
    """Analyzes backtest results and calculates optimization scores"""
    
    def __init__(self):
        self.performance_criteria = {
            'min_sharpe': 1.5,
            'min_profit_factor': 1.3,
            'min_pnl_dd_ratio': 1.5,
            'min_trades': 30
        }
    
    def calculate_score(self, results: Dict[str, Any]) -> float:
        """Calculate optimization score for given results"""
        try:
            # Extract metrics
            sharpe = results.get('sharpe_ratio', 0)
            profit_factor = results.get('profit_factor', 0)
            pnl_dd_ratio = results.get('pnl_dd_ratio', 0)
            total_trades = results.get('total_trades', 0)
            win_rate = results.get('win_rate', 0)
            
            # Check minimum requirements
            if not self._meets_criteria(results):
                return 0.0
            
            # Calculate weighted score
            score = (
                sharpe * 0.3 +
                profit_factor * 0.25 +
                pnl_dd_ratio * 0.25 +
                (total_trades / 100) * 0.1 +
                win_rate * 0.1
            )
            
            logger.info(f"Calculated score: {score:.4f} for results: {results}")
            return max(0.0, score)
            
        except Exception as e:
            logger.error(f"Failed to calculate score: {e}")
            return 0.0
    
    def _meets_criteria(self, results: Dict[str, Any]) -> bool:
        """Check if results meet minimum performance criteria"""
        try:
            sharpe = results.get('sharpe_ratio', 0)
            profit_factor = results.get('profit_factor', 0)
            pnl_dd_ratio = results.get('pnl_dd_ratio', 0)
            total_trades = results.get('total_trades', 0)
            
            return (
                sharpe >= self.performance_criteria['min_sharpe'] and
                profit_factor >= self.performance_criteria['min_profit_factor'] and
                pnl_dd_ratio >= self.performance_criteria['min_pnl_dd_ratio'] and
                total_trades >= self.performance_criteria['min_trades']
            )
            
        except Exception as e:
            logger.error(f"Failed to check criteria: {e}")
            return False
    
    def analyze_results(self, results_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze list of backtest results"""
        try:
            if not results_list:
                return {}
            
            # Calculate scores for all results
            scored_results = []
            for result in results_list:
                score = self.calculate_score(result)
                result['optimization_score'] = score
                scored_results.append(result)
            
            # Sort by score
            scored_results.sort(key=lambda x: x['optimization_score'], reverse=True)
            
            # Find best result
            best_result = scored_results[0] if scored_results else {}
            
            # Calculate statistics
            scores = [r['optimization_score'] for r in scored_results]
            
            analysis = {
                'best_result': best_result,
                'total_results': len(scored_results),
                'qualified_results': len([r for r in scored_results if r['optimization_score'] > 0]),
                'average_score': np.mean(scores) if scores else 0,
                'max_score': max(scores) if scores else 0,
                'all_results': scored_results
            }
            
            logger.info(f"Analyzed {len(results_list)} results, best score: {analysis['max_score']:.4f}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze results: {e}")
            return {}
    
    def rank_currency_pairs(self, results_by_pair: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Rank currency pairs by their best optimization results"""
        try:
            pair_rankings = {}
            
            for pair, results in results_by_pair.items():
                analysis = self.analyze_results(results)
                pair_rankings[pair] = analysis
            
            # Sort pairs by best score
            sorted_pairs = sorted(
                pair_rankings.items(),
                key=lambda x: x[1].get('max_score', 0),
                reverse=True
            )
            
            logger.info(f"Ranked {len(pair_rankings)} currency pairs")
            return dict(sorted_pairs)
            
        except Exception as e:
            logger.error(f"Failed to rank currency pairs: {e}")
            return {}
