@echo off
cd /d "%~dp0"
where git >nul 2>nul && if exist .git (
  echo Checking for updates...
  git pull --ff-only --quiet || echo Could not update - starting the current version.
)
if not exist .venv (
  echo First run: setting up...
  python -m venv .venv || (echo Python 3.10+ is required: https://www.python.org/downloads/ & pause & exit /b 1)
)
.venv\Scripts\python -m pip install -q -r requirements.txt
.venv\Scripts\python app.py
pause
