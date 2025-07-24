#!/usr/bin/env python3
"""
OANDA Connection Issue Diagnostic and Fix Tool
Specifically designed to handle "Remote end closed connection" errors
"""

import asyncio
import logging
import sys
import os
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config
from oanda_service import OandaService
from connection_monitor import EnhancedConnectionMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("connection_fix")

class OandaConnectionFixer:
    def __init__(self):
        self.oanda_service = None
        self.connection_monitor = None
        self.fix_attempts = 0
        
    async def initialize_services(self):
        """Initialize OANDA service and connection monitor"""
        try:
            logger.info("🔧 Initializing OANDA service...")
            self.oanda_service = OandaService()
            await self.oanda_service.initialize()
            
            logger.info("🔍 Initializing connection monitor...")
            self.connection_monitor = EnhancedConnectionMonitor(self.oanda_service)
            
            logger.info("✅ Services initialized successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize services: {e}")
            return False

    async def diagnose_connection_issue(self) -> Dict:
        """Comprehensive diagnosis of connection issues"""
        logger.info("🔍 Starting comprehensive connection diagnosis...")
        
        diagnosis = {
            "timestamp": datetime.now().isoformat(),
            "issues_found": [],
            "recommendations": [],
            "tests": {}
        }
        
        # Test 1: Basic connectivity
        logger.info("📡 Testing basic OANDA connectivity...")
        try:
            results = await self.connection_monitor.run_diagnostic_suite()
            diagnosis["tests"]["basic_connectivity"] = results
            
            if not results["summary"]["all_tests_passed"]:
                diagnosis["issues_found"].append("Basic connectivity tests failed")
                diagnosis["recommendations"].append("Check internet connection and OANDA service status")
        except Exception as e:
            logger.error(f"❌ Basic connectivity test failed: {e}")
            diagnosis["issues_found"].append(f"Connectivity test error: {e}")
        
        # Test 2: Configuration validation
        logger.info("⚙️ Validating OANDA configuration...")
        config_issues = await self._validate_configuration()
        if config_issues:
            diagnosis["issues_found"].extend(config_issues)
            diagnosis["recommendations"].append("Fix configuration issues before proceeding")
        
        # Test 3: Network stability test
        logger.info("🌐 Testing network stability...")
        stability_results = await self._test_network_stability()
        diagnosis["tests"]["network_stability"] = stability_results
        
        if stability_results["consecutive_failures"] > 0:
            diagnosis["issues_found"].append("Network instability detected")
            diagnosis["recommendations"].append("Consider implementing additional retry mechanisms")
        
        # Test 4: Session persistence test
        logger.info("🔒 Testing session persistence...")
        session_results = await self._test_session_persistence()
        diagnosis["tests"]["session_persistence"] = session_results
        
        if not session_results["persistent"]:
            diagnosis["issues_found"].append("Session persistence issues detected")
            diagnosis["recommendations"].append("Implement session warming and keep-alive mechanisms")
        
        return diagnosis

    async def _validate_configuration(self) -> List[str]:
        """Validate OANDA configuration"""
        issues = []
        
        try:
            # Check access token
            if not config.oanda_access_token:
                issues.append("OANDA access token not configured")
            elif len(str(config.oanda_access_token)) < 20:
                issues.append("OANDA access token appears to be invalid (too short)")
            
            # Check account ID
            if not config.oanda_account_id:
                issues.append("OANDA account ID not configured")
            elif not str(config.oanda_account_id).startswith(('101-', '001-')):
                issues.append("OANDA account ID format appears invalid")
            
            # Check environment
            if config.oanda_environment not in ["practice", "live"]:
                issues.append(f"Invalid OANDA environment: {config.oanda_environment}")
            
        except Exception as e:
            issues.append(f"Configuration validation error: {e}")
        
        return issues

    async def _test_network_stability(self, test_duration: int = 60) -> Dict:
        """Test network stability over a period"""
        logger.info(f"Testing network stability for {test_duration} seconds...")
        
        results = {
            "test_duration": test_duration,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "consecutive_failures": 0,
            "max_consecutive_failures": 0,
            "average_response_time": 0,
            "errors": []
        }
        
        start_time = time.time()
        consecutive_failures = 0
        response_times = []
        
        while time.time() - start_time < test_duration:
            try:
                start_request = time.time()
                success, response_time, message = await self.connection_monitor.test_account_connection()
                end_request = time.time()
                
                results["total_requests"] += 1
                
                if success:
                    results["successful_requests"] += 1
                    consecutive_failures = 0
                    response_times.append(response_time)
                else:
                    results["failed_requests"] += 1
                    consecutive_failures += 1
                    results["max_consecutive_failures"] = max(
                        results["max_consecutive_failures"], 
                        consecutive_failures
                    )
                    results["errors"].append(f"{datetime.now().strftime('%H:%M:%S')}: {message}")
                
                # Wait 5 seconds between requests
                await asyncio.sleep(5)
                
            except Exception as e:
                results["failed_requests"] += 1
                consecutive_failures += 1
                results["errors"].append(f"{datetime.now().strftime('%H:%M:%S')}: {e}")
        
        results["consecutive_failures"] = consecutive_failures
        if response_times:
            results["average_response_time"] = sum(response_times) / len(response_times)
        
        return results

    async def _test_session_persistence(self) -> Dict:
        """Test if sessions persist over time"""
        logger.info("Testing session persistence...")
        
        results = {
            "persistent": False,
            "initial_success": False,
            "final_success": False,
            "session_duration": 300,  # 5 minutes
            "error_message": None
        }
        
        try:
            # Initial test
            success, _, message = await self.connection_monitor.test_account_connection()
            results["initial_success"] = success
            
            if success:
                # Wait 5 minutes
                logger.info("Waiting 5 minutes to test session persistence...")
                await asyncio.sleep(300)
                
                # Final test
                success, _, message = await self.connection_monitor.test_account_connection()
                results["final_success"] = success
                results["persistent"] = success
            else:
                results["error_message"] = message
                
        except Exception as e:
            results["error_message"] = str(e)
        
        return results

    async def apply_connection_fixes(self) -> Dict:
        """Apply various fixes for connection issues"""
        logger.info("🔧 Applying connection fixes...")
        
        fix_results = {
            "fixes_applied": [],
            "successful_fixes": [],
            "failed_fixes": [],
            "final_status": "unknown"
        }
        
        self.fix_attempts += 1
        
        # Fix 1: Reinitialize OANDA client
        logger.info("🔄 Fix 1: Reinitializing OANDA client...")
        try:
            self.oanda_service._init_oanda_client()
            await asyncio.sleep(2)  # Wait for initialization
            
            # Test connection
            success, _, _ = await self.connection_monitor.test_account_connection()
            if success:
                fix_results["successful_fixes"].append("OANDA client reinitialization")
                logger.info("✅ Fix 1 successful: Client reinitialized")
            else:
                fix_results["failed_fixes"].append("OANDA client reinitialization")
                logger.warning("⚠️ Fix 1 failed: Client reinitialization didn't help")
        except Exception as e:
            fix_results["failed_fixes"].append(f"OANDA client reinitialization: {e}")
            logger.error(f"❌ Fix 1 error: {e}")
        
        fix_results["fixes_applied"].append("OANDA client reinitialization")
        
        # Fix 2: Connection warming
        logger.info("🔥 Fix 2: Warming connection...")
        try:
            await self.oanda_service._warm_connection()
            await asyncio.sleep(1)
            
            success, _, _ = await self.connection_monitor.test_account_connection()
            if success:
                fix_results["successful_fixes"].append("Connection warming")
                logger.info("✅ Fix 2 successful: Connection warmed")
            else:
                fix_results["failed_fixes"].append("Connection warming")
                logger.warning("⚠️ Fix 2 failed: Connection warming didn't help")
        except Exception as e:
            fix_results["failed_fixes"].append(f"Connection warming: {e}")
            logger.error(f"❌ Fix 2 error: {e}")
        
        fix_results["fixes_applied"].append("Connection warming")
        
        # Fix 3: Multiple retry with backoff
        logger.info("🔁 Fix 3: Multiple retry with exponential backoff...")
        try:
            max_retries = 5
            for attempt in range(max_retries):
                success, response_time, message = await self.connection_monitor.test_account_connection()
                if success:
                    fix_results["successful_fixes"].append("Multiple retry with backoff")
                    logger.info(f"✅ Fix 3 successful: Connected on attempt {attempt + 1}")
                    break
                else:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + 1
                        logger.info(f"Retry attempt {attempt + 1} failed, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
            else:
                fix_results["failed_fixes"].append("Multiple retry with backoff")
                logger.warning("⚠️ Fix 3 failed: All retry attempts failed")
        except Exception as e:
            fix_results["failed_fixes"].append(f"Multiple retry with backoff: {e}")
            logger.error(f"❌ Fix 3 error: {e}")
        
        fix_results["fixes_applied"].append("Multiple retry with backoff")
        
        # Final status check
        try:
            success, _, _ = await self.connection_monitor.test_account_connection()
            fix_results["final_status"] = "healthy" if success else "unhealthy"
        except:
            fix_results["final_status"] = "error"
        
        return fix_results

    async def run_complete_fix_sequence(self) -> Dict:
        """Run complete diagnostic and fix sequence"""
        logger.info("🚀 Starting complete OANDA connection fix sequence...")
        
        sequence_results = {
            "started_at": datetime.now().isoformat(),
            "diagnosis": {},
            "fixes": {},
            "final_health_check": {},
            "success": False,
            "recommendations": []
        }
        
        # Step 1: Initialize services
        if not await self.initialize_services():
            sequence_results["error"] = "Failed to initialize services"
            return sequence_results
        
        # Step 2: Diagnose issues
        logger.info("📋 Step 1: Diagnosing connection issues...")
        sequence_results["diagnosis"] = await self.diagnose_connection_issue()
        
        # Step 3: Apply fixes
        logger.info("🔧 Step 2: Applying connection fixes...")
        sequence_results["fixes"] = await self.apply_connection_fixes()
        
        # Step 4: Final health check
        logger.info("🏥 Step 3: Final health check...")
        try:
            health_results = await self.connection_monitor.run_diagnostic_suite()
            sequence_results["final_health_check"] = health_results
            
            if health_results["summary"]["all_tests_passed"]:
                sequence_results["success"] = True
                logger.info("🎉 SUCCESS: All connection issues resolved!")
            else:
                logger.warning("⚠️ PARTIAL SUCCESS: Some issues may remain")
        except Exception as e:
            sequence_results["final_health_check"] = {"error": str(e)}
            logger.error(f"❌ Final health check failed: {e}")
        
        # Generate recommendations
        if not sequence_results["success"]:
            sequence_results["recommendations"].extend([
                "Consider checking OANDA service status at status.oanda.com",
                "Verify network connectivity and firewall settings",
                "Check if your IP address is blocked by OANDA",
                "Consider using a different network connection",
                "Contact OANDA support if issues persist"
            ])
        
        sequence_results["completed_at"] = datetime.now().isoformat()
        return sequence_results

    def print_results_summary(self, results: Dict):
        """Print a formatted summary of results"""
        print("\n" + "="*70)
        print("🏥 OANDA CONNECTION FIX RESULTS SUMMARY")
        print("="*70)
        
        print(f"Started: {results.get('started_at', 'Unknown')}")
        print(f"Success: {'✅ YES' if results.get('success') else '❌ NO'}")
        
        # Diagnosis summary
        if 'diagnosis' in results:
            issues_found = len(results['diagnosis'].get('issues_found', []))
            print(f"Issues Found: {issues_found}")
            
            for issue in results['diagnosis'].get('issues_found', [])[:3]:
                print(f"  • {issue}")
        
        # Fix summary
        if 'fixes' in results:
            successful_fixes = len(results['fixes'].get('successful_fixes', []))
            failed_fixes = len(results['fixes'].get('failed_fixes', []))
            print(f"Successful Fixes: {successful_fixes}")
            print(f"Failed Fixes: {failed_fixes}")
            
            for fix in results['fixes'].get('successful_fixes', []):
                print(f"  ✅ {fix}")
        
        # Final status
        if 'final_health_check' in results:
            final_status = results['final_health_check'].get('summary', {}).get('health_status', 'Unknown')
            print(f"Final Status: {final_status}")
        
        # Recommendations
        if results.get('recommendations'):
            print("\n💡 Recommendations:")
            for rec in results['recommendations'][:3]:
                print(f"  • {rec}")
        
        print("="*70)

async def main():
    """Main function for standalone execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="OANDA Connection Issue Diagnostic and Fix Tool")
    parser.add_argument("--diagnose-only", action="store_true", help="Run diagnosis only, no fixes")
    parser.add_argument("--quick-fix", action="store_true", help="Run quick fixes only")
    parser.add_argument("--full-sequence", action="store_true", help="Run complete diagnostic and fix sequence (default)")
    
    args = parser.parse_args()
    
    fixer = OandaConnectionFixer()
    
    try:
        if args.diagnose_only:
            await fixer.initialize_services()
            results = await fixer.diagnose_connection_issue()
            print(json.dumps(results, indent=2))
        elif args.quick_fix:
            await fixer.initialize_services()
            results = await fixer.apply_connection_fixes()
            print(json.dumps(results, indent=2))
        else:
            # Default: full sequence
            results = await fixer.run_complete_fix_sequence()
            fixer.print_results_summary(results)
            
    except KeyboardInterrupt:
        logger.info("🛑 Fix sequence interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fix sequence failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 