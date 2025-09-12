"""
MACHINE LEARNING INTEGRATION FRAMEWORK
Institutional-grade ML integration for trading optimization
"""

import asyncio
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import pickle
import joblib
from pathlib import Path

logger = logging.getLogger(__name__)

class MLModelType(Enum):
    SIGNAL_VALIDATION = "signal_validation"        # Validate TradingView signals
    SIGNAL_STRENGTH_SCORING = "signal_strength"    # Score signal quality
    POSITION_SIZING = "position_sizing"           # Optimize position sizing
    EXIT_OVERRIDE_DECISION = "exit_override"       # ML-powered profit ride decisions
    RISK_ASSESSMENT = "risk_assessment"           # Risk scoring for signals
    MARKET_REGIME_DETECTION = "regime_detection"   # Market context for decisions
    VOLATILITY_FORECASTING = "volatility_forecasting"  # Volatility prediction
    ANOMALY_DETECTION = "anomaly_detection"       # Detect signal anomalies

class ModelStatus(Enum):
    TRAINING = "training"
    READY = "ready"
    DEPLOYED = "deployed"
    RETIRED = "retired"
    ERROR = "error"

@dataclass
class MLModel:
    """Machine Learning model representation"""
    model_id: str
    name: str
    model_type: MLModelType
    version: str
    status: ModelStatus
    created_at: str
    last_trained: str
    performance_metrics: Dict[str, float]
    features: List[str]
    target: str
    model_path: str
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if not self.metadata:
            self.metadata = {}

@dataclass
class Prediction:
    """ML model prediction result"""
    model_id: str
    symbol: str
    prediction: float
    confidence: float
    timestamp: str
    features_used: List[str]
    metadata: Dict[str, Any] = None

class MLModelManager:
    """Machine Learning model management system"""
    
    def __init__(self, models_dir: str = "ml_models"):
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(exist_ok=True)
        
        self.models = {}  # model_id -> MLModel
        self.loaded_models = {}  # model_id -> actual model object
        self.prediction_cache = {}  # Cache for predictions
        
        # Model performance tracking
        self.model_performance = {}
        self.prediction_history = []
        
        # Feature engineering
        self.feature_engineers = {}
        self.data_preprocessors = {}
        
    async def register_model(self, model: MLModel) -> bool:
        """Register a new ML model"""
        try:
            self.models[model.model_id] = model
            logger.info(f"✅ Model registered: {model.name} ({model.model_id})")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to register model: {e}")
            return False
    
    async def load_model(self, model_id: str) -> bool:
        """Load model into memory"""
        try:
            model = self.models.get(model_id)
            if not model:
                logger.error(f"Model not found: {model_id}")
                return False
            
            # Load model from disk
            model_path = self.models_dir / model.model_path
            if model_path.exists():
                if model_path.suffix == '.pkl':
                    with open(model_path, 'rb') as f:
                        loaded_model = pickle.load(f)
                elif model_path.suffix == '.joblib':
                    loaded_model = joblib.load(model_path)
                else:
                    logger.error(f"Unsupported model format: {model_path.suffix}")
                    return False
                
                self.loaded_models[model_id] = loaded_model
                model.status = ModelStatus.READY
                logger.info(f"✅ Model loaded: {model.name}")
                return True
            else:
                logger.error(f"Model file not found: {model_path}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to load model {model_id}: {e}")
            model.status = ModelStatus.ERROR
            return False
    
    async def predict(self, model_id: str, features: Dict[str, float], 
                     symbol: str = None) -> Optional[Prediction]:
        """Make prediction using loaded model"""
        try:
            if model_id not in self.loaded_models:
                await self.load_model(model_id)
            
            if model_id not in self.loaded_models:
                logger.error(f"Model not loaded: {model_id}")
                return None
            
            model = self.models[model_id]
            loaded_model = self.loaded_models[model_id]
            
            # Prepare features
            feature_vector = self._prepare_features(features, model.features)
            if feature_vector is None:
                return None
            
            # Make prediction
            prediction_value = loaded_model.predict([feature_vector])[0]
            
            # Calculate confidence (simplified)
            confidence = self._calculate_confidence(loaded_model, feature_vector)
            
            # Create prediction object
            prediction = Prediction(
                model_id=model_id,
                symbol=symbol or "unknown",
                prediction=float(prediction_value),
                confidence=confidence,
                timestamp=datetime.now(timezone.utc).isoformat(),
                features_used=model.features,
                metadata={"feature_vector": feature_vector.tolist()}
            )
            
            # Cache prediction
            cache_key = f"{model_id}:{symbol}:{hash(str(features))}"
            self.prediction_cache[cache_key] = prediction
            
            # Store in history
            self.prediction_history.append(prediction)
            if len(self.prediction_history) > 10000:  # Keep last 10k predictions
                self.prediction_history = self.prediction_history[-10000:]
            
            return prediction
            
        except Exception as e:
            logger.error(f"❌ Prediction failed for model {model_id}: {e}")
            return None
    
    async def batch_predict(self, model_id: str, features_list: List[Dict[str, float]], 
                           symbols: List[str] = None) -> List[Prediction]:
        """Make batch predictions"""
        predictions = []
        
        for i, features in enumerate(features_list):
            symbol = symbols[i] if symbols and i < len(symbols) else None
            prediction = await self.predict(model_id, features, symbol)
            if prediction:
                predictions.append(prediction)
        
        return predictions
    
    def _prepare_features(self, features: Dict[str, float], required_features: List[str]) -> Optional[np.ndarray]:
        """Prepare feature vector for model prediction"""
        try:
            # Check if all required features are present
            missing_features = set(required_features) - set(features.keys())
            if missing_features:
                logger.warning(f"Missing features: {missing_features}")
                # Fill missing features with zeros
                for feature in missing_features:
                    features[feature] = 0.0
            
            # Create feature vector in correct order
            feature_vector = np.array([features.get(feature, 0.0) for feature in required_features])
            return feature_vector
            
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return None
    
    def _calculate_confidence(self, model, feature_vector: np.ndarray) -> float:
        """Calculate prediction confidence"""
        try:
            # For tree-based models, use prediction probability if available
            if hasattr(model, 'predict_proba'):
                probabilities = model.predict_proba([feature_vector])[0]
                confidence = float(np.max(probabilities))
            elif hasattr(model, 'decision_function'):
                # For SVM models
                decision_score = model.decision_function([feature_vector])[0]
                confidence = float(1 / (1 + np.exp(-decision_score)))
            else:
                # Default confidence based on feature variance
                confidence = float(1.0 - np.var(feature_vector) / np.mean(np.abs(feature_vector) + 1e-8))
            
            return max(0.0, min(1.0, confidence))
            
        except Exception as e:
            logger.warning(f"Error calculating confidence: {e}")
            return 0.5  # Default confidence
    
    async def update_model_performance(self, model_id: str, actual_values: List[float], 
                                     predictions: List[float]) -> Dict[str, float]:
        """Update model performance metrics"""
        try:
            if len(actual_values) != len(predictions):
                logger.error("Actual values and predictions length mismatch")
                return {}
            
            # Calculate performance metrics
            actual = np.array(actual_values)
            pred = np.array(predictions)
            
            # Mean Absolute Error
            mae = float(np.mean(np.abs(actual - pred)))
            
            # Root Mean Square Error
            rmse = float(np.sqrt(np.mean((actual - pred) ** 2)))
            
            # Mean Absolute Percentage Error
            mape = float(np.mean(np.abs((actual - pred) / (actual + 1e-8))) * 100)
            
            # R-squared
            ss_res = np.sum((actual - pred) ** 2)
            ss_tot = np.sum((actual - np.mean(actual)) ** 2)
            r2 = float(1 - (ss_res / (ss_tot + 1e-8)))
            
            # Directional accuracy
            actual_direction = np.diff(actual) > 0
            pred_direction = np.diff(pred) > 0
            directional_accuracy = float(np.mean(actual_direction == pred_direction))
            
            metrics = {
                "mae": mae,
                "rmse": rmse,
                "mape": mape,
                "r2": r2,
                "directional_accuracy": directional_accuracy,
                "sample_size": len(actual)
            }
            
            # Update model performance
            self.model_performance[model_id] = metrics
            
            # Update model record
            if model_id in self.models:
                self.models[model_id].performance_metrics.update(metrics)
                self.models[model_id].last_trained = datetime.now(timezone.utc).isoformat()
            
            logger.info(f"✅ Model performance updated: {model_id}")
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Failed to update model performance: {e}")
            return {}
    
    async def get_model_performance(self, model_id: str) -> Dict[str, float]:
        """Get model performance metrics"""
        return self.model_performance.get(model_id, {})
    
    async def get_prediction_history(self, model_id: str = None, 
                                   symbol: str = None, 
                                   limit: int = 100) -> List[Prediction]:
        """Get prediction history"""
        history = self.prediction_history
        
        if model_id:
            history = [p for p in history if p.model_id == model_id]
        
        if symbol:
            history = [p for p in history if p.symbol == symbol]
        
        return history[-limit:] if limit else history

class FeatureEngineer:
    """Feature engineering for ML models"""
    
    @staticmethod
    def create_technical_features(price_data: pd.DataFrame) -> pd.DataFrame:
        """Create technical analysis features"""
        df = price_data.copy()
        
        # Price-based features
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
        df['price_change'] = df['close'] - df['open']
        df['price_range'] = df['high'] - df['low']
        df['body_size'] = abs(df['close'] - df['open'])
        df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
        df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
        
        # Moving averages
        for period in [5, 10, 20, 50]:
            df[f'sma_{period}'] = df['close'].rolling(period).mean()
            df[f'ema_{period}'] = df['close'].ewm(span=period).mean()
            df[f'price_vs_sma_{period}'] = df['close'] / df[f'sma_{period}'] - 1
            df[f'price_vs_ema_{period}'] = df['close'] / df[f'ema_{period}'] - 1
        
        # Volatility features
        df['volatility_5'] = df['returns'].rolling(5).std()
        df['volatility_20'] = df['returns'].rolling(20).std()
        df['atr'] = FeatureEngineer._calculate_atr(df)
        
        # Momentum indicators
        df['rsi'] = FeatureEngineer._calculate_rsi(df['close'])
        df['macd'] = FeatureEngineer._calculate_macd(df['close'])
        df['macd_signal'] = FeatureEngineer._calculate_macd_signal(df['close'])
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # Trend features
        df['trend_5'] = df['close'].rolling(5).apply(lambda x: 1 if x.iloc[-1] > x.iloc[0] else -1)
        df['trend_20'] = df['close'].rolling(20).apply(lambda x: 1 if x.iloc[-1] > x.iloc[0] else -1)
        
        # Volume features (if available)
        if 'volume' in df.columns:
            df['volume_sma_10'] = df['volume'].rolling(10).mean()
            df['volume_ratio'] = df['volume'] / df['volume_sma_10']
        
        return df
    
    @staticmethod
    def _calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        return true_range.rolling(period).mean()
    
    @staticmethod
    def _calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def _calculate_macd(prices: pd.Series, fast: int = 12, slow: int = 26) -> pd.Series:
        """Calculate MACD"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        return ema_fast - ema_slow
    
    @staticmethod
    def _calculate_macd_signal(prices: pd.Series, signal_period: int = 9) -> pd.Series:
        """Calculate MACD signal line"""
        macd = FeatureEngineer._calculate_macd(prices)
        return macd.ewm(span=signal_period).mean()

class MLSignalEnhancer:
    """Enhance TradingView signals with ML validation and scoring"""
    
    def __init__(self, model_manager: MLModelManager):
        self.model_manager = model_manager
        
    async def validate_tradingview_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and enhance TradingView signal"""
        try:
            # Extract signal features
            features = self._extract_signal_features(signal)
            
            # Get ML validation scores
            validation_score = await self._get_validation_score(features)
            strength_score = await self._get_strength_score(features)
            risk_score = await self._get_risk_score(features)
            
            # Enhance signal with ML insights
            enhanced_signal = {
                **signal,  # Original TradingView signal
                "ml_validation_score": validation_score,
                "ml_strength_score": strength_score,
                "ml_risk_score": risk_score,
                "ml_confidence": (validation_score + strength_score) / 2,
                "ml_recommendation": self._get_ml_recommendation(validation_score, strength_score, risk_score)
            }
            
            return enhanced_signal
            
        except Exception as e:
            logger.error(f"Signal enhancement failed: {e}")
            return signal  # Return original signal if enhancement fails
    
    def _extract_signal_features(self, signal: Dict[str, Any]) -> Dict[str, float]:
        """Extract features from TradingView signal"""
        features = {
            # Signal quality features
            "signal_strength": signal.get("strength", 0.5),
            "signal_confidence": signal.get("confidence", 0.5),
            "signal_age": self._calculate_signal_age(signal),
            
            # Market context features
            "market_volatility": self._get_current_volatility(signal.get("symbol", "EUR_USD")),
            "market_regime": self._get_market_regime(signal.get("symbol", "EUR_USD")),
            "trend_strength": self._get_trend_strength(signal.get("symbol", "EUR_USD")),
            
            # Technical features
            "rsi": self._get_rsi(signal.get("symbol", "EUR_USD")),
            "macd": self._get_macd(signal.get("symbol", "EUR_USD")),
            "bollinger_position": self._get_bollinger_position(signal.get("symbol", "EUR_USD")),
            
            # Time-based features
            "hour_of_day": datetime.now().hour,
            "day_of_week": datetime.now().weekday(),
            "is_market_open": self._is_market_open(signal.get("symbol", "EUR_USD")),
            
            # Historical performance features
            "recent_signal_success_rate": self._get_recent_success_rate(signal.get("symbol", "EUR_USD")),
            "avg_profit_per_signal": self._get_avg_profit(signal.get("symbol", "EUR_USD")),
            "avg_loss_per_signal": self._get_avg_loss(signal.get("symbol", "EUR_USD"))
        }
        
        return features
    
    async def _get_validation_score(self, features: Dict[str, float]) -> float:
        """Get ML validation score for signal"""
        # Find validation model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.SIGNAL_VALIDATION:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 0.5
        
        # Default validation score based on features
        return min(1.0, max(0.0, features.get("signal_confidence", 0.5)))
    
    async def _get_strength_score(self, features: Dict[str, float]) -> float:
        """Get ML strength score for signal"""
        # Find strength scoring model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.SIGNAL_STRENGTH_SCORING:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 0.5
        
        # Default strength score
        return min(1.0, max(0.0, features.get("signal_strength", 0.5)))
    
    async def _get_risk_score(self, features: Dict[str, float]) -> float:
        """Get ML risk score for signal"""
        # Find risk assessment model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.RISK_ASSESSMENT:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 0.5
        
        # Default risk score based on volatility
        volatility = features.get("market_volatility", 0.02)
        return min(1.0, max(0.0, volatility * 10))  # Scale volatility to 0-1
    
    def _get_ml_recommendation(self, validation: float, strength: float, risk: float) -> str:
        """Get ML recommendation for signal"""
        if validation < 0.3 or risk > 0.8:
            return "REJECT"  # High risk or low validation
        elif validation > 0.7 and strength > 0.6 and risk < 0.4:
            return "EXECUTE"  # High confidence signal
        else:
            return "EXECUTE_WITH_CAUTION"  # Proceed but with reduced size
    
    def _calculate_signal_age(self, signal: Dict[str, Any]) -> float:
        """Calculate signal age in minutes"""
        try:
            signal_time = datetime.fromisoformat(signal.get("timestamp", datetime.now().isoformat()))
            age_minutes = (datetime.now() - signal_time).total_seconds() / 60
            return min(age_minutes, 60)  # Cap at 60 minutes
        except:
            return 0.0
    
    def _get_current_volatility(self, symbol: str) -> float:
        """Get current market volatility"""
        # Placeholder - would integrate with your market data
        return 0.015  # 1.5% daily volatility
    
    def _get_market_regime(self, symbol: str) -> float:
        """Get market regime (0=trending, 1=ranging)"""
        # Placeholder - would integrate with your regime detection
        return 0.5
    
    def _get_trend_strength(self, symbol: str) -> float:
        """Get trend strength (0-1)"""
        # Placeholder - would integrate with your technical analysis
        return 0.6
    
    def _get_rsi(self, symbol: str) -> float:
        """Get current RSI"""
        # Placeholder - would integrate with your technical analysis
        return 50.0
    
    def _get_macd(self, symbol: str) -> float:
        """Get current MACD"""
        # Placeholder - would integrate with your technical analysis
        return 0.0
    
    def _get_bollinger_position(self, symbol: str) -> float:
        """Get position within Bollinger Bands (0-1)"""
        # Placeholder - would integrate with your technical analysis
        return 0.5
    
    def _is_market_open(self, symbol: str) -> float:
        """Check if market is open (0 or 1)"""
        # Placeholder - would integrate with your market hours check
        return 1.0
    
    def _get_recent_success_rate(self, symbol: str) -> float:
        """Get recent signal success rate"""
        # Placeholder - would integrate with your position tracking
        return 0.6
    
    def _get_avg_profit(self, symbol: str) -> float:
        """Get average profit per signal"""
        # Placeholder - would integrate with your position tracking
        return 0.02
    
    def _get_avg_loss(self, symbol: str) -> float:
        """Get average loss per signal"""
        # Placeholder - would integrate with your position tracking
        return 0.01

class MLExitOverrideOptimizer:
    """ML-powered profit ride override decisions"""
    
    def __init__(self, model_manager: MLModelManager):
        self.model_manager = model_manager
    
    async def should_override_exit(self, position: Dict[str, Any], 
                                 exit_signal: Dict[str, Any]) -> Dict[str, Any]:
        """Determine if exit signal should be overridden"""
        try:
            # Extract position and market features
            features = self._extract_override_features(position, exit_signal)
            
            # Get ML predictions
            override_probability = await self._get_override_probability(features)
            profit_potential = await self._get_profit_potential(features)
            risk_increase = await self._get_risk_increase(features)
            
            # ML decision logic
            should_override = (
                override_probability > 0.6 and 
                profit_potential > 0.02 and  # 2% additional profit potential
                risk_increase < 0.1  # Risk increase < 10%
            )
            
            return {
                "should_override": should_override,
                "override_probability": override_probability,
                "profit_potential": profit_potential,
                "risk_increase": risk_increase,
                "confidence": self._calculate_confidence(override_probability, profit_potential, risk_increase),
                "ml_reasoning": self._get_ml_reasoning(override_probability, profit_potential, risk_increase)
            }
            
        except Exception as e:
            logger.error(f"Exit override decision failed: {e}")
            return {
                "should_override": False,
                "override_probability": 0.0,
                "profit_potential": 0.0,
                "risk_increase": 0.0,
                "confidence": 0.0,
                "ml_reasoning": f"Error: {str(e)}"
            }
    
    def _extract_override_features(self, position: Dict[str, Any], exit_signal: Dict[str, Any]) -> Dict[str, float]:
        """Extract features for override decision"""
        features = {
            # Position features
            "current_pnl": position.get("unrealized_pnl", 0.0),
            "current_pnl_pct": position.get("unrealized_pnl_pct", 0.0),
            "time_in_position": position.get("time_in_position", 0.0),
            "max_favorable_excursion": position.get("mfe", 0.0),
            "max_adverse_excursion": position.get("mae", 0.0),
            "current_drawdown": position.get("current_drawdown", 0.0),
            
            # Market features
            "market_volatility": self._get_current_volatility(position.get("symbol", "EUR_USD")),
            "trend_strength": self._get_trend_strength(position.get("symbol", "EUR_USD")),
            "momentum": self._get_momentum(position.get("symbol", "EUR_USD")),
            
            # Exit signal features
            "exit_signal_strength": exit_signal.get("strength", 0.5),
            "exit_signal_confidence": exit_signal.get("confidence", 0.5),
            "exit_reason": self._encode_exit_reason(exit_signal.get("reason", "unknown")),
            
            # Time features
            "hour_of_day": datetime.now().hour,
            "day_of_week": datetime.now().weekday(),
            "is_market_open": self._is_market_open(position.get("symbol", "EUR_USD")),
            
            # Historical features
            "avg_profit_extension": self._get_avg_profit_extension(position.get("symbol", "EUR_USD")),
            "override_success_rate": self._get_override_success_rate(position.get("symbol", "EUR_USD"))
        }
        
        return features
    
    async def _get_override_probability(self, features: Dict[str, float]) -> float:
        """Get ML override probability"""
        # Find exit override model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.EXIT_OVERRIDE_DECISION:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 0.5
        
        # Default logic based on features
        pnl_pct = features.get("current_pnl_pct", 0.0)
        time_in_position = features.get("time_in_position", 0.0)
        trend_strength = features.get("trend_strength", 0.5)
        
        # Simple heuristic: higher PnL + longer time + strong trend = higher override probability
        base_prob = min(0.8, pnl_pct * 2)  # Scale PnL to probability
        time_bonus = min(0.2, time_in_position / 100)  # Time bonus
        trend_bonus = trend_strength * 0.1  # Trend bonus
        
        return min(1.0, base_prob + time_bonus + trend_bonus)
    
    async def _get_profit_potential(self, features: Dict[str, float]) -> float:
        """Get ML profit potential estimate"""
        # Find profit potential model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.VOLATILITY_FORECASTING:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 0.02
        
        # Default based on trend strength and volatility
        trend_strength = features.get("trend_strength", 0.5)
        volatility = features.get("market_volatility", 0.015)
        
        return min(0.05, trend_strength * volatility * 2)  # Max 5% potential
    
    async def _get_risk_increase(self, features: Dict[str, float]) -> float:
        """Get ML risk increase estimate"""
        # Find risk assessment model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.RISK_ASSESSMENT:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 0.05
        
        # Default based on time in position and volatility
        time_in_position = features.get("time_in_position", 0.0)
        volatility = features.get("market_volatility", 0.015)
        
        return min(0.2, time_in_position * volatility / 100)  # Max 20% risk increase
    
    def _calculate_confidence(self, override_prob: float, profit_potential: float, risk_increase: float) -> float:
        """Calculate overall confidence in override decision"""
        # Weighted confidence based on probability, profit potential, and risk
        confidence = (override_prob * 0.4 + profit_potential * 0.3 + (1 - risk_increase) * 0.3)
        return min(1.0, max(0.0, confidence))
    
    def _get_ml_reasoning(self, override_prob: float, profit_potential: float, risk_increase: float) -> str:
        """Get human-readable ML reasoning"""
        if override_prob > 0.7:
            return f"High override probability ({override_prob:.2f}) with {profit_potential:.1%} profit potential"
        elif profit_potential > 0.03:
            return f"Strong profit potential ({profit_potential:.1%}) outweighs risk ({risk_increase:.1%})"
        elif risk_increase < 0.05:
            return f"Low risk increase ({risk_increase:.1%}) allows for profit extension"
        else:
            return f"Balanced decision: {override_prob:.2f} probability, {profit_potential:.1%} potential, {risk_increase:.1%} risk"
    
    def _get_current_volatility(self, symbol: str) -> float:
        """Get current market volatility"""
        return 0.015  # Placeholder
    
    def _get_trend_strength(self, symbol: str) -> float:
        """Get trend strength"""
        return 0.6  # Placeholder
    
    def _get_momentum(self, symbol: str) -> float:
        """Get market momentum"""
        return 0.5  # Placeholder
    
    def _encode_exit_reason(self, reason: str) -> float:
        """Encode exit reason as numeric feature"""
        reason_map = {
            "take_profit": 0.8,
            "stop_loss": 0.2,
            "time_exit": 0.5,
            "signal_exit": 0.6,
            "unknown": 0.5
        }
        return reason_map.get(reason.lower(), 0.5)
    
    def _is_market_open(self, symbol: str) -> float:
        """Check if market is open"""
        return 1.0  # Placeholder
    
    def _get_avg_profit_extension(self, symbol: str) -> float:
        """Get average profit from extending positions"""
        return 0.02  # Placeholder
    
    def _get_override_success_rate(self, symbol: str) -> float:
        """Get historical override success rate"""
        return 0.6  # Placeholder

class MLPositionSizer:
    """ML-enhanced position sizing for TradingView signals"""
    
    def __init__(self, model_manager: MLModelManager):
        self.model_manager = model_manager
    
    async def calculate_ml_position_size(self, signal: Dict[str, Any], 
                                       base_size: float) -> Dict[str, Any]:
        """Calculate ML-optimized position size"""
        try:
            # Extract signal features
            features = self._extract_sizing_features(signal)
            
            # Get ML predictions
            size_multiplier = await self._get_size_multiplier(features)
            risk_adjustment = await self._get_risk_adjustment(features)
            volatility_factor = await self._get_volatility_factor(features)
            
            # Calculate ML-optimized size
            ml_size = base_size * size_multiplier * risk_adjustment * volatility_factor
            
            # Apply bounds
            ml_size = max(base_size * 0.5, min(base_size * 2.0, ml_size))
            
            return {
                "ml_position_size": ml_size,
                "size_multiplier": size_multiplier,
                "risk_adjustment": risk_adjustment,
                "volatility_factor": volatility_factor,
                "confidence": self._calculate_sizing_confidence(features),
                "reasoning": self._get_sizing_reasoning(size_multiplier, risk_adjustment, volatility_factor)
            }
            
        except Exception as e:
            logger.error(f"ML position sizing failed: {e}")
            return {
                "ml_position_size": base_size,
                "size_multiplier": 1.0,
                "risk_adjustment": 1.0,
                "volatility_factor": 1.0,
                "confidence": 0.0,
                "reasoning": f"Error: {str(e)}"
            }
    
    def _extract_sizing_features(self, signal: Dict[str, Any]) -> Dict[str, float]:
        """Extract features for position sizing"""
        features = {
            # Signal features
            "signal_strength": signal.get("strength", 0.5),
            "signal_confidence": signal.get("confidence", 0.5),
            "signal_type": self._encode_signal_type(signal.get("type", "unknown")),
            
            # Market features
            "market_volatility": self._get_current_volatility(signal.get("symbol", "EUR_USD")),
            "market_regime": self._get_market_regime(signal.get("symbol", "EUR_USD")),
            "trend_strength": self._get_trend_strength(signal.get("symbol", "EUR_USD")),
            "liquidity": self._get_liquidity(signal.get("symbol", "EUR_USD")),
            
            # Risk features
            "correlation_risk": self._get_correlation_risk(signal.get("symbol", "EUR_USD")),
            "concentration_risk": self._get_concentration_risk(signal.get("symbol", "EUR_USD")),
            "drawdown_risk": self._get_drawdown_risk(signal.get("symbol", "EUR_USD")),
            
            # Time features
            "hour_of_day": datetime.now().hour,
            "day_of_week": datetime.now().weekday(),
            "is_market_open": self._is_market_open(signal.get("symbol", "EUR_USD")),
            
            # Historical features
            "recent_success_rate": self._get_recent_success_rate(signal.get("symbol", "EUR_USD")),
            "avg_win_size": self._get_avg_win_size(signal.get("symbol", "EUR_USD")),
            "avg_loss_size": self._get_avg_loss_size(signal.get("symbol", "EUR_USD"))
        }
        
        return features
    
    async def _get_size_multiplier(self, features: Dict[str, float]) -> float:
        """Get ML size multiplier"""
        # Find position sizing model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.POSITION_SIZING:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 1.0
        
        # Default logic based on signal strength and confidence
        signal_strength = features.get("signal_strength", 0.5)
        signal_confidence = features.get("signal_confidence", 0.5)
        
        # Higher strength and confidence = larger size
        multiplier = 0.5 + (signal_strength + signal_confidence) / 2
        return min(2.0, max(0.5, multiplier))
    
    async def _get_risk_adjustment(self, features: Dict[str, float]) -> float:
        """Get ML risk adjustment factor"""
        # Find risk assessment model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.RISK_ASSESSMENT:
                prediction = await self.model_manager.predict(model_id, features)
                return 1.0 - prediction.prediction if prediction else 1.0
        
        # Default based on risk features
        correlation_risk = features.get("correlation_risk", 0.5)
        concentration_risk = features.get("concentration_risk", 0.5)
        drawdown_risk = features.get("drawdown_risk", 0.5)
        
        # Higher risk = smaller size
        avg_risk = (correlation_risk + concentration_risk + drawdown_risk) / 3
        return max(0.5, 1.0 - avg_risk)
    
    async def _get_volatility_factor(self, features: Dict[str, float]) -> float:
        """Get ML volatility adjustment factor"""
        # Find volatility forecasting model
        for model_id, model in self.model_manager.models.items():
            if model.model_type == MLModelType.VOLATILITY_FORECASTING:
                prediction = await self.model_manager.predict(model_id, features)
                return prediction.prediction if prediction else 1.0
        
        # Default based on current volatility
        volatility = features.get("market_volatility", 0.015)
        
        # Higher volatility = smaller size
        if volatility > 0.02:
            return 0.8
        elif volatility < 0.01:
            return 1.2
        else:
            return 1.0
    
    def _calculate_sizing_confidence(self, features: Dict[str, float]) -> float:
        """Calculate confidence in sizing decision"""
        signal_confidence = features.get("signal_confidence", 0.5)
        recent_success = features.get("recent_success_rate", 0.5)
        
        return (signal_confidence + recent_success) / 2
    
    def _get_sizing_reasoning(self, size_mult: float, risk_adj: float, vol_factor: float) -> str:
        """Get human-readable sizing reasoning"""
        return f"Size: {size_mult:.2f}x, Risk: {risk_adj:.2f}x, Vol: {vol_factor:.2f}x"
    
    def _encode_signal_type(self, signal_type: str) -> float:
        """Encode signal type as numeric feature"""
        type_map = {
            "buy": 1.0,
            "sell": -1.0,
            "long": 1.0,
            "short": -1.0,
            "unknown": 0.0
        }
        return type_map.get(signal_type.lower(), 0.0)
    
    def _get_current_volatility(self, symbol: str) -> float:
        """Get current market volatility"""
        return 0.015  # Placeholder
    
    def _get_market_regime(self, symbol: str) -> float:
        """Get market regime"""
        return 0.5  # Placeholder
    
    def _get_trend_strength(self, symbol: str) -> float:
        """Get trend strength"""
        return 0.6  # Placeholder
    
    def _get_liquidity(self, symbol: str) -> float:
        """Get market liquidity"""
        return 0.8  # Placeholder
    
    def _get_correlation_risk(self, symbol: str) -> float:
        """Get correlation risk"""
        return 0.3  # Placeholder
    
    def _get_concentration_risk(self, symbol: str) -> float:
        """Get concentration risk"""
        return 0.4  # Placeholder
    
    def _get_drawdown_risk(self, symbol: str) -> float:
        """Get drawdown risk"""
        return 0.2  # Placeholder
    
    def _is_market_open(self, symbol: str) -> float:
        """Check if market is open"""
        return 1.0  # Placeholder
    
    def _get_recent_success_rate(self, symbol: str) -> float:
        """Get recent success rate"""
        return 0.6  # Placeholder
    
    def _get_avg_win_size(self, symbol: str) -> float:
        """Get average win size"""
        return 0.02  # Placeholder
    
    def _get_avg_loss_size(self, symbol: str) -> float:
        """Get average loss size"""
        return 0.01  # Placeholder

class MLTradingOptimizer:
    """ML-powered trading optimization"""
    
    def __init__(self, model_manager: MLModelManager):
        self.model_manager = model_manager
        self.optimization_rules = {}
        
    async def optimize_position_size(self, symbol: str, base_size: float, 
                                   market_data: Dict[str, float]) -> float:
        """Optimize position size using ML models"""
        try:
            # Get position sizing model
            position_model = None
            for model_id, model in self.model_manager.models.items():
                if model.model_type == MLModelType.POSITION_SIZING:
                    position_model = model_id
                    break
            
            if not position_model:
                return base_size
            
            # Prepare features
            features = self._prepare_position_features(symbol, market_data)
            
            # Get ML prediction
            prediction = await self.model_manager.predict(position_model, features, symbol)
            if not prediction:
                return base_size
            
            # Apply ML optimization
            optimization_factor = prediction.prediction
            confidence_weight = prediction.confidence
            
            # Weighted optimization
            optimized_size = base_size * (1 + (optimization_factor - 0.5) * confidence_weight)
            
            # Apply bounds
            optimized_size = max(base_size * 0.5, min(base_size * 2.0, optimized_size))
            
            logger.info(f"ML position size optimization: {symbol} {base_size:.2f} -> {optimized_size:.2f}")
            return optimized_size
            
        except Exception as e:
            logger.error(f"ML position size optimization failed: {e}")
            return base_size
    
    async def generate_trading_signals(self, symbol: str, market_data: Dict[str, float]) -> Dict[str, Any]:
        """Generate ML-powered trading signals"""
        try:
            signals = {}
            
            # Get signal generation models
            for model_id, model in self.model_manager.models.items():
                if model.model_type == MLModelType.SIGNAL_GENERATION:
                    features = self._prepare_signal_features(symbol, market_data)
                    prediction = await self.model_manager.predict(model_id, features, symbol)
                    
                    if prediction:
                        signals[f"{model.name}_signal"] = {
                            "strength": prediction.prediction,
                            "confidence": prediction.confidence,
                            "direction": "buy" if prediction.prediction > 0.5 else "sell"
                        }
            
            return signals
            
        except Exception as e:
            logger.error(f"ML signal generation failed: {e}")
            return {}
    
    async def assess_risk(self, symbol: str, position_data: Dict[str, Any]) -> Dict[str, float]:
        """ML-powered risk assessment"""
        try:
            risk_metrics = {}
            
            # Get risk assessment models
            for model_id, model in self.model_manager.models.items():
                if model.model_type == MLModelType.RISK_ASSESSMENT:
                    features = self._prepare_risk_features(symbol, position_data)
                    prediction = await self.model_manager.predict(model_id, features, symbol)
                    
                    if prediction:
                        risk_metrics[f"{model.name}_risk"] = {
                            "score": prediction.prediction,
                            "confidence": prediction.confidence,
                            "level": "high" if prediction.prediction > 0.7 else "medium" if prediction.prediction > 0.4 else "low"
                        }
            
            return risk_metrics
            
        except Exception as e:
            logger.error(f"ML risk assessment failed: {e}")
            return {}
    
    def _prepare_position_features(self, symbol: str, market_data: Dict[str, float]) -> Dict[str, float]:
        """Prepare features for position sizing model"""
        return {
            "symbol_volatility": market_data.get("volatility", 0.0),
            "market_regime": market_data.get("regime", 0.5),
            "correlation_risk": market_data.get("correlation", 0.0),
            "momentum": market_data.get("momentum", 0.0),
            "trend_strength": market_data.get("trend", 0.0),
            "volume_ratio": market_data.get("volume_ratio", 1.0)
        }
    
    def _prepare_signal_features(self, symbol: str, market_data: Dict[str, float]) -> Dict[str, float]:
        """Prepare features for signal generation model"""
        return {
            "price_change": market_data.get("price_change", 0.0),
            "volatility": market_data.get("volatility", 0.0),
            "rsi": market_data.get("rsi", 50.0),
            "macd": market_data.get("macd", 0.0),
            "trend": market_data.get("trend", 0.0),
            "volume": market_data.get("volume", 0.0)
        }
    
    def _prepare_risk_features(self, symbol: str, position_data: Dict[str, Any]) -> Dict[str, float]:
        """Prepare features for risk assessment model"""
        return {
            "position_size": position_data.get("size", 0.0),
            "leverage": position_data.get("leverage", 1.0),
            "time_in_position": position_data.get("time_in_position", 0.0),
            "drawdown": position_data.get("drawdown", 0.0),
            "correlation_exposure": position_data.get("correlation", 0.0),
            "volatility_exposure": position_data.get("volatility", 0.0)
        }

# Global ML components
ml_model_manager = MLModelManager()
ml_signal_enhancer = MLSignalEnhancer(ml_model_manager)
ml_exit_override_optimizer = MLExitOverrideOptimizer(ml_model_manager)
ml_position_sizer = MLPositionSizer(ml_model_manager)
ml_optimizer = MLTradingOptimizer(ml_model_manager)

# Convenience functions for signal execution bot
async def enhance_tradingview_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Enhance TradingView signal with ML validation"""
    return await ml_signal_enhancer.validate_tradingview_signal(signal)

async def should_override_exit(position: Dict[str, Any], exit_signal: Dict[str, Any]) -> Dict[str, Any]:
    """Get ML recommendation for exit override"""
    return await ml_exit_override_optimizer.should_override_exit(position, exit_signal)

async def calculate_ml_position_size(signal: Dict[str, Any], base_size: float) -> Dict[str, Any]:
    """Calculate ML-optimized position size for TradingView signal"""
    return await ml_position_sizer.calculate_ml_position_size(signal, base_size)
