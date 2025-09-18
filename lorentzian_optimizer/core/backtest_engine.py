"""
TradingView Backtest Engine
Handles automated backtesting via Selenium WebDriver
"""

import time
import logging
from typing import Dict, List, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import os
import tempfile

logger = logging.getLogger(__name__)

class TradingViewBacktestEngine:
    """Handles automated backtesting through TradingView interface"""
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.driver = None
        self.wait = None
        
    def _initialize_driver(self) -> bool:
        """Initialize Chrome WebDriver with clean profile"""
        try:
            chrome_options = Options()
            
            # Use clean, temporary profile for Selenium
            temp_dir = tempfile.mkdtemp()
            chrome_options.add_argument(f"--user-data-dir={temp_dir}")
            
            # Stealth options to avoid detection
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Additional options for stability
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Enable JavaScript and images for TradingView
            # chrome_options.add_argument("--disable-javascript")  # Commented out
            # chrome_options.add_argument("--disable-images")      # Commented out
            
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Set window size for consistent behavior
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Initialize ChromeDriver
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                self.wait = WebDriverWait(self.driver, 20)
                
                # Additional stealth measures
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                    "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                })
                
                logger.info("Chrome WebDriver initialized successfully")
                return True
            except Exception as e:
                logger.error(f"ChromeDriver initialization failed: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            return False
    
    def _navigate_to_tradingview(self, symbol: str, timeframe: str = "15") -> bool:
        """Navigate to TradingView chart with Gmail login support"""
        try:
            # First, go to TradingView login page
            login_url = "https://www.tradingview.com/accounts/signin/"
            logger.info(f"Navigating to TradingView login: {login_url}")
            
            self.driver.get(login_url)
            time.sleep(5)
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Check if we need to handle Google security/2FA
            page_title = self.driver.title
            logger.info(f"Page title: {page_title}")
            
            # Handle Gmail login and Google security/2FA
            if "security" in page_title.lower() or "verify" in page_title.lower() or "google" in page_title.lower() or "signin" in page_title.lower():
                logger.info("🔐 Detected login/security page - Gmail login may be required")
                logger.info("📧 Please login with your Gmail account in the browser window...")
                logger.info("📱 If Google sends a notification to your phone, please approve it...")
                logger.info("⏳ Waiting up to 10 minutes for login completion...")
                
                # Wait for user to complete login/2FA (up to 10 minutes for phone notifications)
                max_wait_time = 600  # 10 minutes
                wait_interval = 15   # Check every 15 seconds
                elapsed_time = 0
                
                while elapsed_time < max_wait_time:
                    try:
                        # Get current page information
                        current_title = self.driver.title
                        current_url = self.driver.current_url
                        
                        logger.info(f"🔍 Checking page: {current_title[:50]}...")
                        logger.info(f"🔍 Current URL: {current_url[:100]}...")
                        
                        # Check if we've moved past the login/security page
                        if "tradingview" in current_url.lower() and "signin" not in current_url.lower():
                            logger.info("✅ Login completed successfully!")
                            break
                        
                        # Check if we're still on login/security page
                        if "google" in current_title.lower() or "security" in current_title.lower() or "signin" in current_title.lower():
                            # Provide more specific guidance based on elapsed time
                            if elapsed_time < 60:
                                logger.info(f"⏳ Waiting for login... ({elapsed_time}s elapsed)")
                            elif elapsed_time < 120:
                                logger.info(f"📱 If you received a phone notification, please approve it... ({elapsed_time}s elapsed)")
                            elif elapsed_time < 180:
                                logger.info(f"📱 Still waiting for phone notification approval... ({elapsed_time}s elapsed)")
                            else:
                                logger.info(f"⏳ Still waiting for login completion... ({elapsed_time}s elapsed)")
                                logger.info("💡 TIP: Check your phone for Google notifications and approve them")
                            
                            # Try to click "Advanced" or "Proceed" if available
                            try:
                                advanced_button = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Advanced') or contains(text(), 'Proceed') or contains(text(), 'Continue')]")
                                if advanced_button and advanced_button.is_displayed():
                                    advanced_button.click()
                                    logger.info("🔄 Clicked Advanced/Proceed button")
                                    time.sleep(3)
                            except:
                                pass
                        else:
                            # We might have moved past the login page
                            logger.info(f"🔄 Page changed to: {current_title}")
                            logger.info(f"🔄 Current URL: {current_url}")
                        
                        # Wait before next check
                        logger.info(f"⏳ Waiting {wait_interval}s before next check...")
                        time.sleep(wait_interval)
                        elapsed_time += wait_interval
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Error during login wait: {e}")
                        logger.info(f"⏳ Continuing to wait... ({elapsed_time}s elapsed)")
                        time.sleep(wait_interval)
                        elapsed_time += wait_interval
                
                if elapsed_time >= max_wait_time:
                    logger.warning("⏰ Login timeout reached - continuing anyway")
            
            # Now navigate to the specific chart
            chart_url = f"https://www.tradingview.com/chart/?symbol={symbol}&interval={timeframe}"
            logger.info(f"Navigating to chart: {chart_url}")
            
            self.driver.get(chart_url)
            time.sleep(5)
            
            # Wait for chart to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Final check - ensure we're on TradingView
            final_url = self.driver.current_url
            if "tradingview" not in final_url.lower():
                logger.warning(f"⚠️ May not be on TradingView page. Current URL: {final_url}")
                logger.info("🔄 Attempting to navigate directly to TradingView...")
                
                # Try direct navigation again
                self.driver.get(chart_url)
                time.sleep(5)
            
            logger.info(f"✅ Successfully navigated to TradingView for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to navigate to TradingView: {e}")
            return False
    
    def _load_pine_script(self, script_content: str) -> bool:
        """Load Pine Script into TradingView editor"""
        try:
            # Click on Pine Editor button
            pine_editor_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='pine-editor']"))
            )
            pine_editor_button.click()
            time.sleep(2)
            
            # Clear existing script
            editor = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".view-lines"))
            )
            editor.click()
            
            # Select all and delete
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.CONTROL + "a")
            time.sleep(0.5)
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.DELETE)
            time.sleep(0.5)
            
            # Paste new script
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.CONTROL + "v")
            time.sleep(2)
            
            # Click Apply button
            apply_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='apply']"))
            )
            apply_button.click()
            time.sleep(3)
            
            logger.info("Pine Script loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load Pine Script: {e}")
            # Take screenshot for debugging
            try:
                self.driver.save_screenshot("pine_script_load_error.png")
                logger.info("Screenshot saved as pine_script_load_error.png")
            except:
                pass
            return False
    
    def _set_parameters(self, parameters: Dict) -> bool:
        """Set indicator parameters in TradingView"""
        try:
            # Click on settings/parameters button
            settings_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='indicator-properties']"))
            )
            settings_button.click()
            time.sleep(2)
            
            # Set each parameter
            for param_name, value in parameters.items():
                try:
                    # Find parameter input field
                    param_input = self.driver.find_element(
                        By.CSS_SELECTOR, f"input[name='{param_name}']"
                    )
                    param_input.clear()
                    param_input.send_keys(str(value))
                    time.sleep(0.5)
                except:
                    logger.warning(f"Could not set parameter {param_name}")
                    continue
            
            # Save settings
            save_button = self.driver.find_element(
                By.CSS_SELECTOR, "[data-name='save']"
            )
            save_button.click()
            time.sleep(2)
            
            logger.info("Parameters set successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set parameters: {e}")
            return False
    
    def _run_backtest(self) -> bool:
        """Run backtest and wait for results"""
        try:
            # Click on strategy tester
            strategy_tester = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='strategy-tester']"))
            )
            strategy_tester.click()
            time.sleep(3)
            
            # Wait for backtest to complete
            time.sleep(10)  # Allow time for backtest to run
            
            logger.info("Backtest completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to run backtest: {e}")
            return False
    
    def _extract_results(self) -> Dict:
        """Extract backtest results from TradingView"""
        try:
            results = {}
            
            # Extract key metrics
            metrics_selectors = {
                'total_trades': "[data-name='total-trades']",
                'win_rate': "[data-name='win-rate']",
                'profit_factor': "[data-name='profit-factor']",
                'sharpe_ratio': "[data-name='sharpe-ratio']",
                'max_drawdown': "[data-name='max-drawdown']",
                'net_profit': "[data-name='net-profit']",
                'gross_profit': "[data-name='gross-profit']",
                'gross_loss': "[data-name='gross-loss']"
            }
            
            for metric, selector in metrics_selectors.items():
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    value = element.text.strip()
                    results[metric] = float(value) if value.replace('.', '').replace('-', '').isdigit() else 0
                except:
                    results[metric] = 0
            
            # Calculate P&L/DD ratio
            if results.get('max_drawdown', 0) > 0:
                results['pnl_dd_ratio'] = abs(results.get('net_profit', 0)) / results['max_drawdown']
            else:
                results['pnl_dd_ratio'] = 0
            
            logger.info(f"Extracted results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to extract results: {e}")
            return {}
    
    def run_backtest(self, symbol: str, script_content: str, parameters: Dict) -> Dict:
        """Run complete backtest process"""
        try:
            # Initialize driver
            if not self._initialize_driver():
                return {}
            
            # Navigate to TradingView
            if not self._navigate_to_tradingview(symbol):
                return {}
            
            # Load Pine Script
            if not self._load_pine_script(script_content):
                return {}
            
            # Set parameters
            if not self._set_parameters(parameters):
                return {}
            
            # Run backtest
            if not self._run_backtest():
                return {}
            
            # Extract results
            results = self._extract_results()
            results['symbol'] = symbol
            results['parameters'] = parameters
            
            return results
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            return {}
        finally:
            if self.driver:
                self.driver.quit()
    
    def run_batch_backtests(self, backtests: List[Tuple[str, str, Dict]]) -> List[Dict]:
        """Run multiple backtests efficiently"""
        results = []
        
        for symbol, script_content, parameters in backtests:
            logger.info(f"Starting backtest for {symbol}")
            result = self.run_backtest(symbol, script_content, parameters)
            if result:
                results.append(result)
                logger.info(f"Completed backtest for {symbol}")
            else:
                logger.error(f"Failed backtest for {symbol}")
        
        return results
