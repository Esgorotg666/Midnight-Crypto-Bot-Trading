#!/bin/bash
# Start Midnight Crypto Bot Trading

set -e

# 1) Load local env vars if you're running on your own machine (never commit .env)
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# 2) Install dependencies (noop if already installed)
pip install -r requirements.txt

# 3) Run the bot (polling) + tiny web server for hosts like Render
python app.py
