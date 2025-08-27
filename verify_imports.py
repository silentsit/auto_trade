#!/usr/bin/env python3
"""
Import Verification Script
Verifies that all modules can be imported without errors
"""

import sys
import traceback

def test_import(module_name):
    """Test importing a module"""
    try:
        __import__(module_name)
        print(f"✅ {module_name}")
        return True
    except Exception as e:
        print(f"❌ {module_name}: {e}")
        traceback.print_exc()
        return False

def main():
    """Run import verification tests"""
    print("🧪 IMPORT VERIFICATION TEST")
    print("=" * 50)
    
    # List of modules to test
    modules_to_test = [
        'tracker',
        'technical_analysis', 
        'position_journal',
        'crypto_signal_handler',
        'oanda_service',
        'risk_manager',
        'alert_handler',
        'unified_storage',
        'unified_analysis',
        'unified_exit_manager',
        'health_checker',
        'utils',
        'config'
    ]
    
    results = []
    for module in modules_to_test:
        success = test_import(module)
        results.append((module, success))
    
    print("\n" + "=" * 50)
    print("📊 IMPORT VERIFICATION RESULTS:")
    
    successful = [name for name, success in results if success]
    failed = [name for name, success in results if not success]
    
    print(f"✅ Successful: {len(successful)}/{len(modules_to_test)}")
    print(f"❌ Failed: {len(failed)}/{len(modules_to_test)}")
    
    if failed:
        print(f"\n❌ Failed modules: {', '.join(failed)}")
        return False
    else:
        print("\n🎉 ALL IMPORTS SUCCESSFUL!")
        return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
