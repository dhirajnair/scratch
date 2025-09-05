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
    
    # Also look for parallel processes
    PARALLEL_PIDS=$(pgrep -f "parallel" || true)
    
    if [ -z "$PIDS" ] && [ -z "$PARALLEL_PIDS" ]; then
        echo "✅ No running copy processes found"
        exit 0
    else
        echo "🔍 Found running processes:"
        if [ -n "$PIDS" ]; then
            echo "   Script processes: $PIDS"
        fi
        if [ -n "$PARALLEL_PIDS" ]; then
            echo "   Parallel processes: $PARALLEL_PIDS"
        fi
        
        echo "   Killing all processes..."
        
        # Kill script processes
        if [ -n "$PIDS" ]; then
            echo "$PIDS" | xargs kill -TERM 2>/dev/null || true
        fi
        
        # Kill parallel processes
        if [ -n "$PARALLEL_PIDS" ]; then
            echo "$PARALLEL_PIDS" | xargs kill -TERM 2>/dev/null || true
        fi
        
        sleep 3
        
        # Force kill if still running
        REMAINING_SCRIPT=$(pgrep -f "$SCRIPT_NAME" || true)
        REMAINING_PARALLEL=$(pgrep -f "parallel" || true)
        
        if [ -n "$REMAINING_SCRIPT" ] || [ -n "$REMAINING_PARALLEL" ]; then
            echo "⚠️  Some processes still running, force killing..."
            if [ -n "$REMAINING_SCRIPT" ]; then
                echo "$REMAINING_SCRIPT" | xargs kill -KILL 2>/dev/null || true
            fi
            if [ -n "$REMAINING_PARALLEL" ]; then
                echo "$REMAINING_PARALLEL" | xargs kill -KILL 2>/dev/null || true
            fi
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
    
    # Also kill any parallel processes that might be related
    PARALLEL_PIDS=$(pgrep -f "parallel" || true)
    if [ -n "$PARALLEL_PIDS" ]; then
        echo "   Also killing parallel processes: $PARALLEL_PIDS"
        echo "$PARALLEL_PIDS" | xargs kill -KILL 2>/dev/null || true
    fi
    
    sleep 2
    
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
