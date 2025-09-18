"""
Genetic Optimization Engine for Lorentzian Classification
Implements genetic algorithm for parameter optimization
"""

import logging
import random
import numpy as np
from typing import Dict, List, Any, Callable, Optional
from deap import base, creator, tools, algorithms

logger = logging.getLogger(__name__)

class GeneticOptimizer:
    """Genetic algorithm optimizer for Lorentzian Classification parameters"""
    
    def __init__(self, backtest_engine, results_analyzer, pine_script_generator):
        self.backtest_engine = backtest_engine
        self.results_analyzer = results_analyzer
        self.pine_script_generator = pine_script_generator
        
        # Setup DEAP framework
        self._setup_deap()
    
    def _setup_deap(self):
        """Setup DEAP genetic algorithm framework"""
        # Create fitness class
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMax)
        
        # Create toolbox
        self.toolbox = base.Toolbox()
        
        # Register parameter generators
        self._register_parameter_generators()
        
        # Register genetic operators
        self.toolbox.register("individual", tools.initIterate, creator.Individual, self._generate_individual)
        self.toolbox.register("population", tools.initRepeat, list, self.toolbox.individual)
        self.toolbox.register("evaluate", self._evaluate_individual)
        self.toolbox.register("mate", tools.cxTwoPoint)
        self.toolbox.register("mutate", self._mutate_individual, indpb=0.1)
        self.toolbox.register("select", tools.selTournament, tournsize=3)
    
    def _register_parameter_generators(self):
        """Register parameter generation functions"""
        # This would be populated with actual parameter ranges
        # For now, using placeholder ranges
        self.toolbox.register("neighbours_count", random.randint, 4, 12)
        self.toolbox.register("max_bars_back", random.choice, [2000, 2500, 3000])
        self.toolbox.register("f1_param_a", random.randint, 7, 24)
        self.toolbox.register("f2_param_a", random.randint, 7, 17)
        self.toolbox.register("f2_param_b", random.randint, 7, 17)
        self.toolbox.register("f3_param_a", random.randint, 11, 24)
        self.toolbox.register("f4_param_a", random.randint, 13, 24)
        self.toolbox.register("f5_param_a", random.randint, 4, 11)
        self.toolbox.register("f5_param_b", random.randint, 1, 9)
        self.toolbox.register("regime_threshold", random.uniform, -0.3, 0.3)
        self.toolbox.register("adx_threshold", random.randint, 11, 24)
        self.toolbox.register("ema_period", random.choice, [9, 15, 20, 40])
        self.toolbox.register("sma_period", random.choice, [10, 20, 30, 50, 80])
        self.toolbox.register("kernel_lookback", random.choice, [6, 8])
        self.toolbox.register("kernel_regression", random.choice, [18, 20, 25])
    
    def _generate_individual(self) -> List:
        """Generate a random individual (parameter set)"""
        return [
            self.toolbox.neighbours_count(),
            self.toolbox.max_bars_back(),
            self.toolbox.f1_param_a(),
            self.toolbox.f2_param_a(),
            self.toolbox.f2_param_b(),
            self.toolbox.f3_param_a(),
            self.toolbox.f4_param_a(),
            self.toolbox.f5_param_a(),
            self.toolbox.f5_param_b(),
            self.toolbox.regime_threshold(),
            self.toolbox.adx_threshold(),
            self.toolbox.ema_period(),
            self.toolbox.sma_period(),
            self.toolbox.kernel_lookback(),
            self.toolbox.kernel_regression()
        ]
    
    def _mutate_individual(self, individual, indpb):
        """Mutate an individual with given probability"""
        for i in range(len(individual)):
            if random.random() < indpb:
                # Regenerate this parameter
                if i == 0:  # neighbours_count
                    individual[i] = self.toolbox.neighbours_count()
                elif i == 1:  # max_bars_back
                    individual[i] = self.toolbox.max_bars_back()
                elif i == 2:  # f1_param_a
                    individual[i] = self.toolbox.f1_param_a()
                elif i == 3:  # f2_param_a
                    individual[i] = self.toolbox.f2_param_a()
                elif i == 4:  # f2_param_b
                    individual[i] = self.toolbox.f2_param_b()
                elif i == 5:  # f3_param_a
                    individual[i] = self.toolbox.f3_param_a()
                elif i == 6:  # f4_param_a
                    individual[i] = self.toolbox.f4_param_a()
                elif i == 7:  # f5_param_a
                    individual[i] = self.toolbox.f5_param_a()
                elif i == 8:  # f5_param_b
                    individual[i] = self.toolbox.f5_param_b()
                elif i == 9:  # regime_threshold
                    individual[i] = self.toolbox.regime_threshold()
                elif i == 10:  # adx_threshold
                    individual[i] = self.toolbox.adx_threshold()
                elif i == 11:  # ema_period
                    individual[i] = self.toolbox.ema_period()
                elif i == 12:  # sma_period
                    individual[i] = self.toolbox.sma_period()
                elif i == 13:  # kernel_lookback
                    individual[i] = self.toolbox.kernel_lookback()
                elif i == 14:  # kernel_regression
                    individual[i] = self.toolbox.kernel_regression()
        
        return individual,
    
    def _evaluate_individual(self, individual, currency_pairs: List[str]) -> tuple:
        """Evaluate an individual (parameter set) across currency pairs"""
        try:
            # Convert individual to parameter dictionary
            params = self._individual_to_parameters(individual)
            
            total_score = 0
            valid_pairs = 0
            
            for pair in currency_pairs:
                try:
                    # Generate Pine Script
                    script_content = self.pine_script_generator.generate_script(params)
                    if not script_content:
                        continue
                    
                    # Run backtest
                    results = self.backtest_engine.run_backtest(pair, script_content, params)
                    if not results:
                        continue
                    
                    # Calculate score
                    score = self.results_analyzer.calculate_score(results)
                    total_score += score
                    valid_pairs += 1
                    
                    logger.info(f"Pair {pair}: Score {score:.4f}, Sharpe: {results.get('sharpe_ratio', 0):.2f}, "
                              f"PF: {results.get('profit_factor', 0):.2f}, P&L/DD: {results.get('pnl_dd_ratio', 0):.2f}")
                    
                except Exception as e:
                    logger.error(f"Failed to evaluate {pair}: {e}")
                    continue
            
            if valid_pairs == 0:
                return (0.0,)
            
            average_score = total_score / valid_pairs
            logger.info(f"Individual evaluation: {average_score:.4f} across {valid_pairs} pairs")
            return (average_score,)
            
        except Exception as e:
            logger.error(f"Failed to evaluate individual: {e}")
            return (0.0,)
    
    def _individual_to_parameters(self, individual: List) -> Dict[str, Any]:
        """Convert individual to parameter dictionary"""
        return {
            'neighbours_count': int(individual[0]),
            'max_bars_back': int(individual[1]),
            'f1_param_a': int(individual[2]),
            'f2_param_a': int(individual[3]),
            'f2_param_b': int(individual[4]),
            'f3_param_a': int(individual[5]),
            'f4_param_a': int(individual[6]),
            'f5_param_a': int(individual[7]),
            'f5_param_b': int(individual[8]),
            'regime_threshold': float(individual[9]),
            'adx_threshold': int(individual[10]),
            'ema_period': int(individual[11]),
            'sma_period': int(individual[12]),
            'kernel_lookback': int(individual[13]),
            'kernel_regression': int(individual[14]),
            'source': 'hlc3',
            'feature_count': 5,
            'show_default_exits': True,
            'use_dynamic_exits': False,
            'show_trade_stats': True,
            'use_worst_case_estimates': False,
            'use_volatility_filter': True,
            'use_regime_filter': True,
            'use_adx_filter': True,
            'use_ema_filter': True,
            'use_sma_filter': True,
            'trade_with_kernel': True,
            'show_kernel_estimate': True,
            'relative_weighting': 8,
            'enhance_kernel_smoothing': False,
            'show_bar_colors': True,
            'show_bar_predictions': True,
            'use_atr_offset': False,
            'bar_prediction_offset': 0,
            'inputs_in_status_line': True
        }
    
    def optimize(self, currency_pairs: List[str], max_generations: int = 50, 
                 population_size: int = 20, stop_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Run genetic optimization"""
        try:
            logger.info(f"Starting optimization for {len(currency_pairs)} currency pairs")
            logger.info(f"Population size: {population_size}, Max generations: {max_generations}")
            
            # Create initial population
            population = self.toolbox.population(n=population_size)
            
            # Evaluate initial population
            fitnesses = []
            for individual in population:
                fitness = self._evaluate_individual(individual, currency_pairs)
                individual.fitness.values = fitness
                fitnesses.append(fitness[0])
            
            logger.info(f"Initial population evaluated. Best fitness: {max(fitnesses):.4f}")
            
            # Evolution loop
            for generation in range(max_generations):
                if stop_callback and stop_callback():
                    logger.info("Optimization stopped by user")
                    break
                
                # Select parents
                offspring = self.toolbox.select(population, len(population))
                offspring = list(map(self.toolbox.clone, offspring))
                
                # Apply crossover
                for child1, child2 in zip(offspring[::2], offspring[1::2]):
                    if random.random() < 0.5:
                        self.toolbox.mate(child1, child2)
                        del child1.fitness.values
                        del child2.fitness.values
                
                # Apply mutation
                for mutant in offspring:
                    if random.random() < 0.2:
                        self.toolbox.mutate(mutant)
                        del mutant.fitness.values
                
                # Evaluate invalid individuals
                invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
                fitnesses = []
                for individual in invalid_ind:
                    fitness = self._evaluate_individual(individual, currency_pairs)
                    individual.fitness.values = fitness
                    fitnesses.append(fitness[0])
                
                # Replace population
                population[:] = offspring
                
                # Log progress
                current_best = max([ind.fitness.values[0] for ind in population])
                logger.info(f"Generation {generation + 1}: Best fitness = {current_best:.4f}")
            
            # Find best individual
            best_individual = tools.selBest(population, 1)[0]
            best_parameters = self._individual_to_parameters(best_individual)
            best_score = best_individual.fitness.values[0]
            
            logger.info(f"Optimization completed. Best score: {best_score:.4f}")
            logger.info(f"Best parameters: {best_parameters}")
            
            return {
                'best_parameters': best_parameters,
                'best_score': best_score,
                'total_generations': generation + 1
            }
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            return {}
