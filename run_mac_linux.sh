#!/bin/bash
# Double-click (or: bash run_mac_linux.sh) to install + start the dashboard.
cd "$(dirname "$0")"
echo "Installing requirements (first run only)..."
python3 -m pip install -q -r requirements.txt || {
  echo "ERROR: could not install requirements. Is Python 3 installed?"
  echo "Get it from https://www.python.org/downloads/"
  read -p "Press Enter to close..."
  exit 1
}
echo "Starting GSC Indexer..."
python3 app.py
read -p "Press Enter to close..."
