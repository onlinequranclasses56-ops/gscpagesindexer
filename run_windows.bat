@echo off
REM Double-click to install + start the dashboard.
cd /d "%~dp0"
echo Installing requirements (first run only)...
python -m pip install -q -r requirements.txt
if errorlevel 1 (
  echo ERROR: could not install requirements. Is Python installed?
  echo Get it from https://www.python.org/downloads/  (tick "Add python.exe to PATH")
  pause
  exit /b 1
)
echo Starting GSC Indexer...
python app.py
pause
