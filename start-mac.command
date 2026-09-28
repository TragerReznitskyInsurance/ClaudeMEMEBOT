#!/bin/bash
cd "$(dirname "$0")"
if command -v git >/dev/null && [ -d .git ]; then
  echo "Checking for updates..."
  git pull --ff-only --quiet || echo "Could not update - starting the current version."
fi
if [ ! -d .venv ]; then
  echo "First run: setting up..."
  python3 -m venv .venv || { echo "Python 3.10+ is required: https://www.python.org/downloads/"; read; exit 1; }
fi
.venv/bin/python -m pip install -q -r requirements.txt
.venv/bin/python app.py
