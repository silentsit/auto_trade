#!/usr/bin/env python
"""
Market Regime Classifier Demo

This script demonstrates the MarketRegimeClassifier functionality by:
1. Fetching real market data from OANDA
2. Classifying the current market regime
3. Generating trading insights and recommendations
4. Displaying the results

Usage:
    python market_regime_demo.py [symbol] [timeframe]
    
Example:
    python market_regime_demo.py EURUSD H1
    python market_regime_demo.py XAUUSD H4
"""

import os
import sys
import asyncio
import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
# Import from python_bridge.py instead of market_regime
# from market_regime import MarketRegimeClassifier, MarketRegime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MarketRegimeDemo")

# Load environment variables
load_dotenv()

# Import from python_bridge.py (assuming it's in the same directory)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import required components
try:
    # Import MarketRegimeClassifier directly from python_bridge
    from python_bridge import (
        OandaAPI, standardize_symbol, get_atr,
        MarketRegimeClassifier, MarketRegime  # Import directly from python_bridge
    )
    # Try to import optional components
    try:
        from python_bridge import LorentzianClassifier, VolatilityMonitor
        HAS_ADVANCED_CLASSIFIERS = True
        logger.info("Advanced market classifiers available")
    except (ImportError, AttributeError):
        HAS_ADVANCED_CLASSIFIERS = False
        logger.warning("Advanced market classifiers not available")
except ImportError as e:
    logger.error(f"Error importing required components: {str(e)}")
    sys.exit(1)

async def fetch_ohlc_data(symbol, timeframe="H1", count=100):
    """
    Fetch OHLC data from OANDA API
    """
    try:
        # Get OANDA credentials from environment variables
        account_id = os.getenv("OANDA_ACCOUNT_ID")
        api_token = os.getenv("OANDA_API_TOKEN")
        
        if not account_id or not api_token:
            logger.error("OANDA_ACCOUNT_ID and OANDA_API_TOKEN environment variables must be set")
            return None
        
        # Create OANDA API client
        oanda_api = OandaAPI(api_token, account_id)
        
        # Map timeframe to OANDA granularity
        granularity_map = {
            "M5": "M5",
            "M15": "M15",
            "M30": "M30",
            "H1": "H1",
            "H4": "H4",
            "D": "D"
        }
        granularity = granularity_map.get(timeframe, "H1")
        
        # Fetch candles
        logger.info(f"Fetching {count} {granularity} candles for {symbol}")
        candles = await oanda_api.get_candles(symbol, granularity=granularity, count=count)
        
        if not candles or "candles" not in candles or len(candles["candles"]) < 10:
            logger.error(f"Insufficient data received for {symbol}")
            return None
        
        # Process candles into OHLC data
        ohlc_data = {
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
            "timestamp": []
        }
        
        for candle in candles["candles"]:
            if candle["complete"]:
                ohlc_data["open"].append(float(candle["mid"]["o"]))
                ohlc_data["high"].append(float(candle["mid"]["h"]))
                ohlc_data["low"].append(float(candle["mid"]["l"]))
                ohlc_data["close"].append(float(candle["mid"]["c"]))
                ohlc_data["volume"].append(int(candle["volume"]))
                ohlc_data["timestamp"].append(candle["time"])
        
        return ohlc_data
    
    except Exception as e:
        logger.error(f"Error fetching OHLC data: {str(e)}")
        return None

async def get_lorentzian_classification(ohlc_data):
    """
    Get market classification using LorentzianClassifier
    """
    if not HAS_ADVANCED_CLASSIFIERS or not ohlc_data:
        return None
    
    try:
        # Initialize classifier
        classifier = LorentzianClassifier(lookback_period=20)
        
        # Add price data
        for price in ohlc_data["close"]:
            classifier.add_price(price)
        
        # Get classification
        regime, signal_strength = classifier.get_regime()
        risk_adjustment = classifier.get_risk_adjustment()
        
        return {
            "regime": regime,
            "signal_strength": signal_strength,
            "risk_adjustment": risk_adjustment
        }
    except Exception as e:
        logger.error(f"Error in Lorentzian classification: {str(e)}")
        return None

async def get_volatility_analysis(ohlc_data):
    """
    Get volatility analysis using VolatilityMonitor
    """
    if not HAS_ADVANCED_CLASSIFIERS or not ohlc_data:
        return None
    
    try:
        # Initialize volatility monitor
        monitor = VolatilityMonitor(lookback_period=14)
        
        # Add candle data
        for i in range(len(ohlc_data["close"])):
            monitor.add_candle(
                ohlc_data["high"][i],
                ohlc_data["low"][i],
                ohlc_data["close"][i]
            )
        
        # Get volatility state
        volatility_state = monitor.get_volatility_state()
        
        return volatility_state
    except Exception as e:
        logger.error(f"Error in volatility analysis: {str(e)}")
        return None

async def analyze_market_regime(symbol, timeframe="H1"):
    """
    Analyze market regime for the specified symbol and timeframe
    """
    try:
        # Standardize symbol
        std_symbol = standardize_symbol(symbol)
        
        # Fetch OHLC data
        ohlc_data = await fetch_ohlc_data(std_symbol, timeframe)
        if not ohlc_data:
            logger.error(f"Could not retrieve data for {symbol}")
            return
        
        # Create classifier
        classifier = MarketRegimeClassifier()
        
        # Add data to classifier
        classifier.add_price_data(std_symbol, timeframe, ohlc_data)
        
        # Classify regime
        regime = classifier.classify_regime(std_symbol, timeframe)
        
        # Generate trading insights
        insights = classifier.generate_trading_insights(std_symbol, timeframe)
        
        # Enhanced analysis with other classifiers if available
        lorentzian_classification = None
        volatility_analysis = None
        
        if HAS_ADVANCED_CLASSIFIERS:
            # Get Lorentzian classification
            lorentzian_classification = await get_lorentzian_classification(ohlc_data)
            
            # Get volatility analysis
            volatility_analysis = await get_volatility_analysis(ohlc_data)
            
            # Integrate with other classifiers if data is available
            if lorentzian_classification or volatility_analysis:
                lorentzian_signal = lorentzian_classification.get("signal_strength") if lorentzian_classification else None
                volatility_index = volatility_analysis.get("volatility_percentile") if volatility_analysis else None
                
                # Update insights with integrated analysis
                insights = classifier.integrate_with_other_classifiers(
                    std_symbol,
                    timeframe,
                    lorentzian_signal=lorentzian_signal,
                    volatility_index=volatility_index
                )
        
        # Calculate ATR (if available)
        try:
            atr_value = get_atr(std_symbol, timeframe)
            insights["atr"] = atr_value
        except Exception as e:
            logger.warning(f"Could not calculate ATR: {str(e)}")
        
        # Add price statistics
        if len(ohlc_data["close"]) > 0:
            insights["current_price"] = ohlc_data["close"][-1]
            insights["price_change_pct"] = (ohlc_data["close"][-1] / ohlc_data["close"][0] - 1) * 100
        
        # Print results in a formatted way
        print("\n" + "="*80)
        print(f" MARKET REGIME ANALYSIS: {symbol} {timeframe}")
        print("="*80)
        
        print(f"\nRegime: {insights['regime']}")
        print(f"Confidence: {insights['confidence']}%")
        print(f"Volatility: {insights['volatility']}")
        print(f"Trend Direction: {insights['trend_direction']}")
        
        print("\nRisk Adjustments:")
        print(f"  Position Size Modifier: {insights['position_size_modifier']:.2f}x")
        print(f"  Stop Loss Modifier: {insights['stop_loss_modifier']:.2f}x")
        print(f"  Risk/Reward Modifier: {insights['risk_reward_modifier']:.2f}x")
        
        if "atr" in insights:
            print(f"\nATR: {insights['atr']:.5f}")
            
        if "current_price" in insights:
            print(f"Current Price: {insights['current_price']:.5f}")
            
        if "price_change_pct" in insights:
            print(f"Price Change: {insights['price_change_pct']:.2f}%")
        
        # Print Lorentzian analysis if available
        if lorentzian_classification:
            print("\nLorentzian Classification:")
            print(f"  Regime: {lorentzian_classification['regime']}")
            print(f"  Signal Strength: {lorentzian_classification['signal_strength']:.2f}")
            print(f"  Risk Adjustment: {lorentzian_classification['risk_adjustment']:.2f}x")
        
        # Print volatility analysis if available
        if volatility_analysis:
            print("\nVolatility Analysis:")
            print(f"  ATR: {volatility_analysis.get('current_atr', 0):.5f}")
            print(f"  Historical Volatility: {volatility_analysis.get('historical_volatility', 0):.2f}%")
            print(f"  Volatility Percentile: {volatility_analysis.get('volatility_percentile', 0):.2f}")
            print(f"  Volatility State: {volatility_analysis.get('volatility_state', 'Unknown')}")
        
        print("\nTrading Recommendation:")
        print(f"  {insights['recommendation']}")
        print(f"  {insights['explanation']}")
        
        if "integration_note" in insights:
            print(f"\nNote: {insights['integration_note']}")
        
        print("\n" + "-"*80)
        
        # Return insights
        return {
            "market_regime": insights,
            "lorentzian_classification": lorentzian_classification,
            "volatility_analysis": volatility_analysis,
            "price_data": {
                "current_price": ohlc_data["close"][-1] if ohlc_data["close"] else None,
                "atr": insights.get("atr")
            }
        }
    
    except Exception as e:
        logger.error(f"Error in market regime analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

async def main():
    """
    Main function
    """
    # Get symbol and timeframe from command line args or use defaults
    symbol = "EURUSD"
    timeframe = "H1"
    
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    if len(sys.argv) > 2:
        timeframe = sys.argv[2]
    
    # Analyze market regime
    await analyze_market_regime(symbol, timeframe)

if __name__ == "__main__":
    asyncio.run(main()) 