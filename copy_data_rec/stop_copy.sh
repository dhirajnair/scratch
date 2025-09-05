#!/bin/bash

# Stop SharePoint to Azure Blob Copy Script
# Usage: ./stop_copy.sh

set -e

PID_FILE="copy_process.pid"
SCRIPT_NAME="recursive_sharept_to_blob.sh"

echo "🛑 Stopping SharePoint to Azure Blob copy process..."

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  No PID file found. Checking for running processes..."
    
    # Look for running script processes
    PIDS=$(pgrep -f "$SCRIPT_NAME" || true)
    
    if [ -z "$PIDS" ]; then
        echo "✅ No running copy processes found"
        exit 0
    else
        echo "🔍 Found running processes: $PIDS"
        echo "   Killing processes..."
        echo "$PIDS" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        
        # Force kill if still running
        REMAINING=$(pgrep -f "$SCRIPT_NAME" || true)
        if [ -n "$REMAINING" ]; then
            echo "⚠️  Processes still running, force killing..."
            echo "$REMAINING" | xargs kill -KILL 2>/dev/null || true
        fi
        
        echo "✅ All processes stopped"
        exit 0
    fi
fi

# Read PID from file
PID=$(cat "$PID_FILE")

# Check if process is still running
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "⚠️  Process with PID $PID is not running"
    echo "   Cleaning up PID file..."
    rm -f "$PID_FILE"
    exit 0
fi

echo "🔍 Found process with PID: $PID"

# Try graceful termination first
echo "   Sending TERM signal..."
kill -TERM "$PID" 2>/dev/null || true

# Wait for graceful shutdown
echo "   Waiting for graceful shutdown (10 seconds)..."
for i in {1..10}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Process stopped gracefully"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
    echo -n "."
done
echo ""

# Force kill if still running
if ps -p "$PID" > /dev/null 2>&1; then
    echo "⚠️  Process still running, force killing..."
    kill -KILL "$PID" 2>/dev/null || true
    sleep 1
    
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "❌ Failed to stop process $PID"
        exit 1
    else
        echo "✅ Process force stopped"
    fi
fi

# Clean up PID file
rm -f "$PID_FILE"

echo "✅ Copy process stopped successfully"
echo ""
echo "📊 Check final status in logs:"
echo "   tail sharepoint_copy_*.log"
