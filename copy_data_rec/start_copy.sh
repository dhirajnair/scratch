#!/bin/bash

# Start SharePoint to Azure Blob Copy Script in Background
# Usage: ./start_copy.sh

set -e

SCRIPT_NAME="recursive_sharept_to_blob.sh"
PID_FILE="copy_process.pid"
LOG_DIR="logs"

# Create logs directory
mkdir -p "$LOG_DIR"

# Check if script is already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "❌ Script is already running with PID: $PID"
        echo "   Use './stop_copy.sh' to stop it first"
        exit 1
    else
        echo "⚠️  Stale PID file found, removing..."
        rm -f "$PID_FILE"
    fi
fi

# Check if main script exists
if [ ! -f "$SCRIPT_NAME" ]; then
    echo "❌ Error: $SCRIPT_NAME not found in current directory"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found"
    echo "   Please create .env file with required configuration"
    exit 1
fi

# Make script executable
chmod +x "$SCRIPT_NAME"

echo "🚀 Starting SharePoint to Azure Blob copy in background..."

# Start script in background with nohup
nohup ./"$SCRIPT_NAME" > "$LOG_DIR/script_output.log" 2>&1 &
PID=$!

# Save PID
echo "$PID" > "$PID_FILE"

echo "✅ Script started successfully!"
echo "   PID: $PID"
echo "   Output log: $LOG_DIR/script_output.log"
echo "   Process log: sharepoint_copy_*.log"
echo "   Success tracking: successfully_uploaded.txt"
echo ""
echo "📊 Monitor progress with:"
echo "   tail -f $LOG_DIR/script_output.log"
echo "   tail -f sharepoint_copy_*.log"
echo "   wc -l successfully_uploaded.txt  # Count uploaded files"
echo ""
echo "🛑 Stop script with:"
echo "   ./stop_copy.sh"
