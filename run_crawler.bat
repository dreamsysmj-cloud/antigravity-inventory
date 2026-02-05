@echo off
cd /d "%~dp0"
echo 🚀 Running Ecount Inventory/Sales Crawler...
python crawler.py
echo.
echo ✅ Job Finished. You can close this window.
pause
