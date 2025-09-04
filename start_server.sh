#!/bin/bash
# Database MCP Server startup script

# Get the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to the script directory
cd "$SCRIPT_DIR"

# Run the server with all passed arguments
uv run python src/main.py "$@"
