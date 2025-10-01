@echo off
REM run_early_shift.bat - Easy launcher for Early Shift

echo =========================================
echo        EARLY SHIFT - $99/month
echo    Roblox Game Trend Alerts for Studios
echo =========================================
echo.

cd /d "%~dp0"

REM Check if Python is installed
call python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if dependencies are installed
call python -c "import aiohttp" 2>nul
if errorlevel 1 (
    echo Installing dependencies...
    call python -m pip install --upgrade pip >nul 2>&1
    call python -m pip install -r requirements.txt
    echo.
)

:menu
echo What would you like to do?
echo.
echo 1. Run test (verify everything works)
echo 2. Add a studio ($99/month)
echo 3. Start monitoring (production mode)
echo 4. Run one monitoring cycle
echo 5. Exit
echo.

set /p choice="Enter your choice (1-5): "

echo.
if "%choice%"=="1" goto test
if "%choice%"=="2" goto addstudio
if "%choice%"=="3" goto production
if "%choice%"=="4" goto onecycle
if "%choice%"=="5" goto quit

echo Invalid choice. Please try again.
echo.
goto menu

:test
echo Running Early Shift test...
call python test_early_shift.py
pause
goto menu

:addstudio
echo Add a new studio
set /p name="Studio name: "
set /p token="Notion API token: "
set /p database="Notion database ID: "
set /p ntfy="ntfy.sh topic (optional): "

if "%ntfy%"=="" (
    call python add_studio.py --name "%name%" --token "%token%" --database "%database%"
) else (
    call python add_studio.py --name "%name%" --token "%token%" --database "%database%" --ntfy-topic "%ntfy%"
)

pause
goto menu

:production
echo Starting Early Shift in production mode...
echo Will check for trending games every 4 hours
call python -c "import asyncio; from main import EarlyShift; asyncio.run(EarlyShift().run_forever())"
pause
goto menu

:onecycle
echo Running one monitoring cycle...
call python main.py
pause
goto menu

:quit
exit /b 0