@echo off
echo ============================================================
echo WAL Reader Test Setup
echo ============================================================
echo.
echo This will open two windows:
echo   1. WAL Reader (Main.py) - processes WAL changes
echo   2. Test Data Generator - creates database changes
echo.
echo Make sure Docker containers are running first!
echo   (run 'docker compose up -d' in Docker_Connections folder)
echo.
pause

echo.
echo Starting WAL Reader...
start "WAL Reader" cmd /k "python Main.py"

timeout /t 2 /nobreak > nul

echo Starting Test Data Generator...
start "Test Data Generator" cmd /k "python Test_Data_Generator.py"

echo.
echo ============================================================
echo Both processes started in separate windows
echo Close those windows or press Ctrl+C in them to stop
echo ============================================================

