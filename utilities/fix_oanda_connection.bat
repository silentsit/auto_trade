@echo off
echo ==========================================
echo OANDA Connection Fix Tool
echo ==========================================
echo.
echo This tool will diagnose and fix OANDA connection issues
echo like "Remote end closed connection" errors.
echo.

cd /d "%~dp0\.."

echo Running OANDA Connection Diagnostic and Fix...
echo.

python utilities/oanda_connection_fix.py --full-sequence

echo.
echo ==========================================
echo Fix sequence completed!
echo ==========================================
echo.
echo If issues persist, try:
echo 1. Check your internet connection
echo 2. Verify OANDA service status at status.oanda.com
echo 3. Run the connection monitor: python connection_monitor.py
echo 4. Test profit override integration: python utilities/test_override_integration.py
echo.
pause 