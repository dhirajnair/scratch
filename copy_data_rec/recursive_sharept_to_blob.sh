#!/bin/bash

# Recursive SharePoint to Azure Blob Storage Copy Script
# Usage: ./recursive_sharept_to_blob.sh
#
# Configuration is read from .env file:
# SHAREPOINT_SITE_URL, SHAREPOINT_PATH, ALLOWED_EXTENSIONS, MAX_PARALLEL_JOBS

set -e

# Default values
TEMP_DIR="temp_sharepoint_download"
LOG_FILE="sharepoint_copy_$(date +%Y%m%d_%H%M%S).log"

# Performance optimizations for large file transfers
export PYTHONUNBUFFERED=1
export AZURE_CORE_DISABLE_APPLICATION_INSIGHTS=1

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to show usage
usage() {
    echo "Usage: $0"
    echo ""
    echo "Configuration is read from .env file with the following variables:"
    echo "  SHAREPOINT_SITE_URL: SharePoint site URL (e.g., https://contexq.sharepoint.com/sites/Development)"
    echo "  SHAREPOINT_PATH: SharePoint path to copy from (e.g., Data/4.ESG/SNP/30-JUN-2025)"
    echo "  ALLOWED_EXTENSIONS: Comma-separated file extensions (e.g., .xls,.xlsx)"
    echo "  MAX_PARALLEL_JOBS: Maximum parallel download jobs (optional, default: 10)"
    echo ""
    echo "Authentication variables required:"
    echo "  GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET"
    echo "  AZURE_STORAGE_CONNECTION_STRING"
    exit 1
}

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | sed 's/#.*//g' | xargs)
fi

# Set default for MAX_PARALLEL_JOBS if not provided
# Optimized for 8-core 32GB VM: 40-60 parallel jobs for 40k files
MAX_PARALLEL_JOBS=${MAX_PARALLEL_JOBS:-50}

# Validate required environment variables
if [ -z "$GRAPH_TENANT_ID" ] || [ -z "$GRAPH_CLIENT_ID" ] || [ -z "$GRAPH_CLIENT_SECRET" ] || [ -z "$AZURE_STORAGE_CONNECTION_STRING" ]; then
    log "ERROR: Missing required authentication environment variables"
    usage
fi

if [ -z "$SHAREPOINT_SITE_URL" ] || [ -z "$SHAREPOINT_PATH" ] || [ -z "$ALLOWED_EXTENSIONS" ] || [ -z "$AZURE_STORAGE_CONTAINER_NAME" ]; then
    log "ERROR: Missing required SharePoint configuration environment variables"
    usage
fi

# Create temp directory
mkdir -p "$TEMP_DIR"

log "Starting recursive SharePoint to Azure copy"
log "Site: $SHAREPOINT_SITE_URL"
log "Path: $SHAREPOINT_PATH"
log "Extensions: $ALLOWED_EXTENSIONS"
log "Max parallel jobs: $MAX_PARALLEL_JOBS"

# --- 1. Get Microsoft Graph API Access Token ---
log "Requesting Access Token from Microsoft Graph API..."
TOKEN_RESPONSE=$(curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded" \
    "https://login.microsoftonline.com/$GRAPH_TENANT_ID/oauth2/v2.0/token" \
    -d "client_id=$GRAPH_CLIENT_ID" \
    -d "client_secret=$GRAPH_CLIENT_SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=https://graph.microsoft.com/.default")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r .access_token)

if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" == "null" ]; then
    log "ERROR: Failed to get access token. Response:"
    echo "$TOKEN_RESPONSE"
    exit 1
fi
log "Successfully obtained access token."

# --- 2. Extract site information ---
# Extract hostname and site path from URL
HOSTNAME=$(echo "$SHAREPOINT_SITE_URL" | sed 's|https://||' | sed 's|/.*||')
SITE_PATH=$(echo "$SHAREPOINT_SITE_URL" | sed "s|https://$HOSTNAME||")

log "Hostname: $HOSTNAME"
log "Site path: $SITE_PATH"

# --- 3. Get site ID ---
log "Getting site ID..."
SITE_INFO=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
    "https://graph.microsoft.com/v1.0/sites/$HOSTNAME:$SITE_PATH")

SITE_ID=$(echo "$SITE_INFO" | jq -r .id)
if [ -z "$SITE_ID" ] || [ "$SITE_ID" == "null" ]; then
    log "ERROR: Failed to get site ID. Response:"
    echo "$SITE_INFO"
    exit 1
fi
log "Site ID: $SITE_ID"

# --- 4. Function to recursively get all files with pagination ---
get_files_recursive() {
    local drive_id="$1"
    local folder_path="$2"
    local output_file="$3"
    
    # URL encode the folder path
    local encoded_path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$folder_path', safe='/'))")
    
    log "Scanning folder: $folder_path"
    
    # Get folder contents with pagination
    local api_url="https://graph.microsoft.com/v1.0/sites/$SITE_ID/drive/root:/$encoded_path:/children?\$top=5000"
    local page_count=0
    
    # For large folders, use parallel pagination if enabled
    if [ "$PARALLEL_PAGINATION" = "true" ]; then
        log "Using parallel pagination for: $folder_path"
        echo "[Discovery Job $job_id] Using parallel pagination for: $folder_path" >> "$LOG_FILE"
        
        # Collect first few page URLs quickly
        local page_urls=()
        local temp_url="$api_url"
        local url_count=0
        
        log "Collecting page URLs for parallel processing..."
        while [ -n "$temp_url" ] && [ $url_count -lt 10 ]; do  # Collect up to 10 page URLs
            page_urls+=("$temp_url")
            url_count=$((url_count + 1))
            
            # Get full response to find next page URL properly
            local quick_response=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" "$temp_url")
            
            # Check if we got a valid response
            if echo "$quick_response" | jq -e .value > /dev/null 2>&1; then
                local item_count=$(echo "$quick_response" | jq '.value | length')
                local has_next=$(echo "$quick_response" | jq -r 'has("@odata.nextLink")')
                temp_url=$(echo "$quick_response" | jq -r '."@odata.nextLink" // empty')
                
                log "  Page $url_count: Found $item_count items, has_next: $has_next"
                if [ -n "$temp_url" ] && [ "$temp_url" != "null" ]; then
                    log "  Next URL: ${temp_url:0:100}..."
                else
                    log "  No more pages available"
                    break
                fi
            else
                log "  Error getting page $url_count, stopping URL collection"
                log "  Response preview: $(echo "$quick_response" | head -c 200)"
                break
            fi
            
            if [ $((url_count % 3)) -eq 0 ]; then
                log "  Collected $url_count page URLs so far..."
            fi
        done
        
        log "Found ${#page_urls[@]} pages to process in parallel for: $folder_path"
        echo "[Discovery Job $job_id] Processing ${#page_urls[@]} pages in parallel for: $folder_path" >> "$LOG_FILE"
        
        # Process pages in parallel batches
        local batch_size=5
        local page_index=0
        local total_files=0
        local total_folders=0
        
        while [ $page_index -lt ${#page_urls[@]} ]; do
            local batch_pids=()
            local batch_start=$((page_index + 1))
            local batch_end=$((page_index + batch_size))
            if [ $batch_end -gt ${#page_urls[@]} ]; then
                batch_end=${#page_urls[@]}
            fi
            
            log "Processing pages $batch_start-$batch_end of ${#page_urls[@]} in parallel..."
            
            # Start batch of parallel page requests
            for ((i=0; i<batch_size && page_index+i<${#page_urls[@]}; i++)); do
                local page_url="${page_urls[$((page_index + i))]}"
                local batch_page=$((page_index + i + 1))
                
                (
                    local response=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" "$page_url")
                    local page_files=0
                    local page_folders=0
                    
                    if ! echo "$response" | jq -e .error > /dev/null 2>&1; then
                        # Process items efficiently
                        echo "$response" | jq -r '.value[]? | 
                            if has("folder") then
                                "FOLDER|" + .name
                            else
                                if (.name | ascii_downcase | test("\\.(xls|xlsx)$")) then
                                    "FILE|" + .name + "|" + (."@microsoft.graph.downloadUrl" // "")
                                else
                                    empty
                                end
                            end' | while IFS='|' read -r type name download_url; do
                            
                            if [ "$type" = "FOLDER" ]; then
                                # Use full path to avoid variable issues in subshells
                                echo "$folder_path/$name" >> "$TEMP_DIR/folder_queue.txt"
                                page_folders=$((page_folders + 1))
                            elif [ "$type" = "FILE" ]; then
                                local azure_path=$(echo "$folder_path" | sed "s|^[^/]*/||")
                                echo "$folder_path/$name|$download_url|$azure_path/$name" >> "$output_file"
                                page_files=$((page_files + 1))
                            fi
                        done
                        
                        echo "[Discovery Job $job_id] Page $batch_page: $page_files files, $page_folders folders" >> "$LOG_FILE"
                    else
                        echo "[Discovery Job $job_id] ERROR on page $batch_page: $(echo "$response" | jq -r .error.message)" >> "$LOG_FILE"
                    fi
                ) &
                batch_pids+=($!)
            done
            
            # Wait for current batch to complete
            for pid in "${batch_pids[@]}"; do
                wait $pid
            done
            
            page_index=$((page_index + batch_size))
            
            # Progress update
            log "Completed batch $batch_start-$batch_end of ${#page_urls[@]} pages"
            
            # Small delay between batches to avoid rate limiting
            sleep 0.2
        done
        
        echo "[Discovery Job $job_id] Completed parallel pagination for: $folder_path" >> "$LOG_FILE"
        return  # Skip the sequential pagination below
    fi
    
    # Sequential pagination (original method)
    log "Using sequential pagination for: $folder_path"
    local total_files=0
    local total_folders=0
    
    while [ -n "$api_url" ]; do
        page_count=$((page_count + 1))
        log "  Processing page $page_count for folder: $folder_path"
        
        local response=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" "$api_url")
        
        # Check if request was successful
        if echo "$response" | jq -e .error > /dev/null 2>&1; then
            log "WARNING: Error accessing folder $folder_path: $(echo "$response" | jq -r .error.message)"
            return
        fi
        
        # Process each item in this page
        local page_files=0
        local page_folders=0
        echo "$response" | jq -r '.value[]? | @json' | while read -r item; do
            local name=$(echo "$item" | jq -r .name)
            local item_type=$(echo "$item" | jq -r 'if has("folder") then "folder" else "file" end')
            local download_url=$(echo "$item" | jq -r '."@microsoft.graph.downloadUrl" // empty')
            
            if [ "$item_type" == "folder" ]; then
                # Add subfolder to queue for parallel processing
                echo "$folder_path/$name" >> "$TEMP_DIR/folder_queue.txt"
                page_folders=$((page_folders + 1))
            else
                # Check if file extension is allowed
                local extension=$(echo "$name" | sed 's/.*\(\.[^.]*\)$/\1/' | tr '[:upper:]' '[:lower:]')
                if echo "$ALLOWED_EXTENSIONS" | grep -q "$extension"; then
                    # Calculate Azure path (one level up from the specified path)
                    local azure_path=$(echo "$folder_path" | sed "s|^[^/]*/||")
                    echo "$folder_path/$name|$download_url|$azure_path/$name" >> "$output_file"
                    page_files=$((page_files + 1))
                fi
            fi
        done
        
        total_files=$((total_files + page_files))
        total_folders=$((total_folders + page_folders))
        log "    Page $page_count: Found $page_files files, $page_folders folders (Total: $total_files files, $total_folders folders)"
        
        # Check for next page
        api_url=$(echo "$response" | jq -r '."@odata.nextLink" // empty')
    done
}

# --- 5. Get default drive ID ---
log "Getting default drive ID..."
DRIVE_INFO=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
    "https://graph.microsoft.com/v1.0/sites/$SITE_ID/drive")

DRIVE_ID=$(echo "$DRIVE_INFO" | jq -r .id)
if [ -z "$DRIVE_ID" ] || [ "$DRIVE_ID" == "null" ]; then
    log "ERROR: Failed to get drive ID. Response:"
    echo "$DRIVE_INFO"
    exit 1
fi
log "Drive ID: $DRIVE_ID"

# --- 6. Discover all files ---
FILES_LIST="$TEMP_DIR/files_to_copy.txt"
FOLDER_QUEUE="$TEMP_DIR/folder_queue.txt"

log "Discovering files recursively..."

# Process root folder first
get_files_recursive "$DRIVE_ID" "$SHAREPOINT_PATH" "$FILES_LIST"

# Process all discovered subfolders from queue
if [ -f "$FOLDER_QUEUE" ]; then
    while [ -s "$FOLDER_QUEUE" ]; do
        # Get next folder from queue
        current_folder=$(head -n 1 "$FOLDER_QUEUE")
        tail -n +2 "$FOLDER_QUEUE" > "$TEMP_DIR/temp_queue.txt"
        mv "$TEMP_DIR/temp_queue.txt" "$FOLDER_QUEUE"
        
        if [ -n "$current_folder" ]; then
            log "Processing discovered subfolder: $current_folder"
            get_files_recursive "$DRIVE_ID" "$current_folder" "$FILES_LIST"
        fi
    done
    
    log "Completed processing all subfolders"
else
    log "No subfolder queue found - only root folder processed"
fi

if [ ! -f "$FILES_LIST" ] || [ ! -s "$FILES_LIST" ]; then
    log "ERROR: No files found or failed to create file list"
    exit 1
fi

TOTAL_FILES=$(wc -l < "$FILES_LIST")
log "Found $TOTAL_FILES files to copy"
log "Files will be copied to Azure container: $AZURE_STORAGE_CONTAINER_NAME"

# --- 7. Function to download and upload a single file ---
process_file() {
    local file_info="$1"
    local job_id="$2"
    
    IFS='|' read -r sharepoint_path download_url azure_path <<< "$file_info"
    
    local temp_file="$TEMP_DIR/job_${job_id}_$(basename "$azure_path")"
    local log_prefix="[Job $job_id]"
    
    echo "$log_prefix Processing: $sharepoint_path -> $azure_path" >> "$LOG_FILE"
    
    # Progress indicator every 100 files
    if [ $((job_id % 100)) -eq 0 ]; then
        local completed=$(grep -c "SUCCESS:" "$LOG_FILE" 2>/dev/null || echo "0")
        echo "$log_prefix Progress: $completed/$TOTAL_FILES files completed" >> "$LOG_FILE"
    fi
    
    # Get fresh download URL for this file (URLs expire quickly)
    local encoded_path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$sharepoint_path', safe='/'))")
    local fresh_download_url=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "https://graph.microsoft.com/v1.0/sites/$SITE_ID/drive/root:/$encoded_path" | \
        jq -r '."@microsoft.graph.downloadUrl" // empty')
    
    if [ -z "$fresh_download_url" ] || [ "$fresh_download_url" == "null" ]; then
        echo "$log_prefix ERROR: Failed to get fresh download URL for $sharepoint_path" >> "$LOG_FILE"
        return 1
    fi
    
    # Download file with fresh URL (optimized for speed)
    if ! curl -L -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Accept: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*" \
        -H "User-Agent: Mozilla/5.0" \
        --connect-timeout 30 \
        --max-time 300 \
        --retry 3 \
        --retry-delay 1 \
        --compressed \
        "$fresh_download_url" -o "$temp_file" --fail-with-body; then
        echo "$log_prefix ERROR: Failed to download $sharepoint_path" >> "$LOG_FILE"
        return 1
    fi
    
    # Check if file was actually downloaded
    if [ ! -f "$temp_file" ] || [ ! -s "$temp_file" ]; then
        echo "$log_prefix ERROR: Downloaded file is empty: $sharepoint_path" >> "$LOG_FILE"
        return 1
    fi
    
    # Upload to Azure (optimized for speed)
    if az storage blob upload \
        --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
        --container-name "$AZURE_STORAGE_CONTAINER_NAME" \
        --name "$azure_path" \
        --file "$temp_file" \
        --overwrite \
        --max-connections 4 \
        --output none 2>/dev/null; then
        echo "$log_prefix SUCCESS: $azure_path" >> "$LOG_FILE"
    else
        echo "$log_prefix ERROR: Failed to upload $azure_path" >> "$LOG_FILE"
        rm -f "$temp_file"
        return 1
    fi
    
    # Cleanup
    rm -f "$temp_file"
    return 0
}

export -f process_file
export ACCESS_TOKEN AZURE_STORAGE_CONNECTION_STRING TEMP_DIR LOG_FILE FOLDER_QUEUE SITE_ID

# --- 8. Process files in parallel ---
log "Starting parallel processing with $MAX_PARALLEL_JOBS jobs..."
log "Estimated time: ~$((TOTAL_FILES / MAX_PARALLEL_JOBS / 60)) minutes for $TOTAL_FILES files"

# Use GNU parallel if available, otherwise use a background job approach
if command -v parallel >/dev/null 2>&1; then
    log "Using GNU parallel for processing"
    cat "$FILES_LIST" | parallel -j "$MAX_PARALLEL_JOBS" --line-buffer process_file {} {#}
else
    log "Using background jobs for processing (install GNU parallel for better performance)"
    
    # Process files with background jobs
    job_count=0
    start_time=$(date +%s)
    
    while IFS= read -r file_info; do
        # Wait if we have too many background jobs
        while [ $(jobs -r | wc -l) -ge "$MAX_PARALLEL_JOBS" ]; do
            sleep 0.1
        done
        
        # Start background job
        job_count=$((job_count + 1))
        process_file "$file_info" "$job_count" &
        
        # Progress update every 500 files
        if [ $((job_count % 500)) -eq 0 ]; then
            elapsed=$(($(date +%s) - start_time))
            rate=$((job_count / (elapsed + 1)))
            eta=$(((TOTAL_FILES - job_count) / (rate + 1)))
            log "Progress: Started $job_count/$TOTAL_FILES jobs (${rate}/sec, ETA: ${eta}s)"
        fi
    done < "$FILES_LIST"
    
    # Wait for all background jobs to complete
    wait
fi

# --- 9. Summary ---
log "Processing completed! Generating summary..."

SUCCESSFUL=$(grep -c "SUCCESS:" "$LOG_FILE" 2>/dev/null || echo "0")
FAILED=$(grep -c "ERROR:" "$LOG_FILE" 2>/dev/null || echo "0")
DOWNLOAD_ERRORS=$(grep -c "Failed to download" "$LOG_FILE" 2>/dev/null || echo "0")
UPLOAD_ERRORS=$(grep -c "Failed to upload" "$LOG_FILE" 2>/dev/null || echo "0")

# Calculate completion percentage
COMPLETION_PERCENT=$((SUCCESSFUL * 100 / (TOTAL_FILES > 0 ? TOTAL_FILES : 1)))

log "==================== COPY OPERATION SUMMARY ===================="
log "Total files discovered: $TOTAL_FILES"
log "Successfully copied: $SUCCESSFUL ($COMPLETION_PERCENT%)"
log "Failed: $FAILED"
log "  - Download failures: $DOWNLOAD_ERRORS"
log "  - Upload failures: $UPLOAD_ERRORS"
log "Container: $AZURE_STORAGE_CONTAINER_NAME"
log "Log file: $LOG_FILE"

if [ $FAILED -gt 0 ]; then
    log "============== ERROR DETAILS =============="
    log "Recent errors from log:"
    tail -20 "$LOG_FILE" | grep "ERROR:" | head -5 | while read -r line; do
        log "  $line"
    done
    log "Check full log file for complete error details: $LOG_FILE"
    log "============================================="
    exit 1
else
    log "🎉 All files copied successfully!"
fi

# --- 10. Cleanup ---
rm -rf "$TEMP_DIR"
log "Cleanup complete. Log saved to: $LOG_FILE"
