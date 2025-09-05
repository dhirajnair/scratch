#!/bin/bash

# Simple File Reconciliation - Basic Version
set -e
source .env

# Set default values if not provided in .env
INCLUDE_EXTENSIONS=${INCLUDE_EXTENSIONS:-""}
EXCLUDE_EXTENSIONS=${EXCLUDE_EXTENSIONS:-""}
INCLUDE_FOLDERS=${INCLUDE_FOLDERS:-""}
EXCLUDE_FOLDERS=${EXCLUDE_FOLDERS:-""}
OUTPUT_DIR=${OUTPUT_DIR:-"reconciliation_output"}
GENERATE_EXCEL=${GENERATE_EXCEL:-"true"}

# Convert comma-separated extensions to array and build regex patterns
IFS=',' read -ra INCLUDE_EXT_ARRAY <<< "$INCLUDE_EXTENSIONS"
IFS=',' read -ra EXCLUDE_EXT_ARRAY <<< "$EXCLUDE_EXTENSIONS"
IFS=',' read -ra INCLUDE_FOLDER_ARRAY <<< "$INCLUDE_FOLDERS"
IFS=',' read -ra EXCLUDE_FOLDER_ARRAY <<< "$EXCLUDE_FOLDERS"

EXTENSION_PATTERN=""
EXCLUDE_EXTENSION_PATTERN=""
SEARCH_FILETYPES=""

# Build include extension patterns
for ext in "${INCLUDE_EXT_ARRAY[@]}"; do
    # Remove leading dot and spaces
    clean_ext=$(echo "$ext" | sed 's/^[[:space:]]*\.*//' | sed 's/[[:space:]]*$//')
    
    if [ -n "$clean_ext" ]; then
        # Build regex pattern for file matching
        if [ -z "$EXTENSION_PATTERN" ]; then
            EXTENSION_PATTERN="\\.$clean_ext"
        else
            EXTENSION_PATTERN="$EXTENSION_PATTERN|\\.$clean_ext"
        fi
        
        # Build SharePoint search filetype query
        if [ -z "$SEARCH_FILETYPES" ]; then
            SEARCH_FILETYPES="filetype:$clean_ext"
        else
            SEARCH_FILETYPES="$SEARCH_FILETYPES OR filetype:$clean_ext"
        fi
    fi
done

# Build exclude extension patterns
for ext in "${EXCLUDE_EXT_ARRAY[@]}"; do
    # Remove leading dot and spaces
    clean_ext=$(echo "$ext" | sed 's/^[[:space:]]*\.*//' | sed 's/[[:space:]]*$//')
    
    if [ -n "$clean_ext" ]; then
        if [ -z "$EXCLUDE_EXTENSION_PATTERN" ]; then
            EXCLUDE_EXTENSION_PATTERN="\\.$clean_ext"
        else
            EXCLUDE_EXTENSION_PATTERN="$EXCLUDE_EXTENSION_PATTERN|\\.$clean_ext"
        fi
    fi
done

# Simple function to normalize filenames by replacing problematic characters
normalize_filenames() {
    local input_file="$1"
    local output_file="$2"
    
    # Use sed to replace smart quotes and other problematic characters with regular ones
    # Using hex codes to avoid shell interpretation issues
    sed 's/\xe2\x80\x99/'"'"'/g; s/\xe2\x80\x98/'"'"'/g; s/\xe2\x80\x9d/"/g; s/\xe2\x80\x9c/"/g; s/\xe2\x80\x93/-/g; s/\xe2\x80\x94/-/g' "$input_file" > "$output_file"
}

# Function to check if file should be included based on extension filters
should_include_file() {
    local filename="$1"
    
    # Check if file matches include pattern
    if [ -n "$EXTENSION_PATTERN" ] && [[ ! "$filename" =~ ($EXTENSION_PATTERN)$ ]]; then
        return 1
    fi
    
    # Check if file matches exclude pattern
    if [ -n "$EXCLUDE_EXTENSION_PATTERN" ] && [[ "$filename" =~ ($EXCLUDE_EXTENSION_PATTERN)$ ]]; then
        return 1
    fi
    
    return 0
}

# Function to check if folder should be included based on folder filters
should_include_folder() {
    local folder_path="$1"
    
    # If include folders are specified, check if path matches any
    if [ -n "$INCLUDE_FOLDERS" ]; then
        local found_match=false
        for pattern in "${INCLUDE_FOLDER_ARRAY[@]}"; do
            pattern=$(echo "$pattern" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
            if [ -n "$pattern" ] && [[ "$folder_path" =~ $pattern ]]; then
                found_match=true
                break
            fi
        done
        if [ "$found_match" = false ]; then
            return 1
        fi
    fi
    
    # Check if folder matches exclude pattern
    if [ -n "$EXCLUDE_FOLDERS" ]; then
        for pattern in "${EXCLUDE_FOLDER_ARRAY[@]}"; do
            pattern=$(echo "$pattern" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
            if [ -n "$pattern" ] && [[ "$folder_path" =~ $pattern ]]; then
                return 1
            fi
        done
    fi
    
    return 0
}

echo "Using include extensions: $INCLUDE_EXTENSIONS"
if [ -n "$EXCLUDE_EXTENSIONS" ]; then
    echo "Excluding extensions: $EXCLUDE_EXTENSIONS"
fi
if [ -n "$INCLUDE_FOLDERS" ]; then
    echo "Including folders matching: $INCLUDE_FOLDERS"
fi
if [ -n "$EXCLUDE_FOLDERS" ]; then
    echo "Excluding folders matching: $EXCLUDE_FOLDERS"
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="$OUTPUT_DIR/$TIMESTAMP"
mkdir -p "$OUTPUT_DIR"

echo "=== Simple File Reconciliation ==="
echo "Output directory: $OUTPUT_DIR"

# 1. Get SharePoint files (just first page for now)
echo ""
echo "=== Step 1: Getting SharePoint files ==="

TOKEN_RESPONSE=$(curl -s --max-time 30 --connect-timeout 10 -X POST -H "Content-Type: application/x-www-form-urlencoded" \
    "https://login.microsoftonline.com/$GRAPH_TENANT_ID/oauth2/v2.0/token" \
    -d "client_id=$GRAPH_CLIENT_ID" \
    -d "client_secret=$GRAPH_CLIENT_SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=https://graph.microsoft.com/.default")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r .access_token)
echo "✓ Got access token"

SITE_DOMAIN=$(echo "$SHAREPOINT_SITE_URL" | sed -E 's|https?://([^/]+)/.*|\1|')
SITE_PATH=$(echo "$SHAREPOINT_SITE_URL" | sed -E 's|https?://[^/]+/(.*)|\1|')
SITE_RESPONSE=$(curl -s --max-time 30 --connect-timeout 10 -H "Authorization: Bearer $ACCESS_TOKEN" \
    "https://graph.microsoft.com/v1.0/sites/$SITE_DOMAIN:/$SITE_PATH")
SITE_ID=$(echo "$SITE_RESPONSE" | jq -r .id)
DRIVES_RESPONSE=$(curl -s --max-time 30 --connect-timeout 10 -H "Authorization: Bearer $ACCESS_TOKEN" \
    "https://graph.microsoft.com/v1.0/sites/$SITE_ID/drives")
DRIVE_ID=$(echo "$DRIVES_RESPONSE" | jq -r '.value[0].id')
echo "✓ Got site and drive IDs"

# Use optimized parallel pagination for SharePoint discovery
echo "Using optimized parallel pagination for SharePoint discovery..."

# Function to process folder with parallel pagination
get_sharepoint_files() {
    local folder_path="$1"
    local output_file="$2"
    
    # URL encode the folder path
    local encoded_path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$folder_path', safe='/'))")
    
    echo "  Scanning folder: $folder_path"
    
    # Get folder contents with pagination
    local api_url="https://graph.microsoft.com/v1.0/sites/$SITE_ID/drive/root:/$encoded_path:/children?\$top=5000"
    
    # Collect page URLs for parallel processing
    local page_urls=()
    local temp_url="$api_url"
    local url_count=0
    
    echo "    Collecting page URLs..."
    while [ -n "$temp_url" ] && [ $url_count -lt 20 ]; do  # Up to 20 pages for large folders
        page_urls+=("$temp_url")
        url_count=$((url_count + 1))
        
        # Get response to find next page
        local response=$(curl -s --max-time 30 -H "Authorization: Bearer $ACCESS_TOKEN" "$temp_url")
        
        if echo "$response" | jq -e .value > /dev/null 2>&1; then
            local item_count=$(echo "$response" | jq '.value | length')
            echo "      Page $url_count: Found $item_count items"
            temp_url=$(echo "$response" | jq -r '."@odata.nextLink" // empty')
            
            if [ -z "$temp_url" ] || [ "$temp_url" = "null" ]; then
                break
            fi
        else
            echo "      Error on page $url_count, stopping"
            break
        fi
    done
    
    echo "    Processing ${#page_urls[@]} pages in parallel..."
    
    # Process pages in parallel batches of 3
    local batch_size=3
    local page_index=0
    
    while [ $page_index -lt ${#page_urls[@]} ]; do
        local batch_pids=()
        
        # Start batch of parallel requests
        for ((i=0; i<batch_size && page_index+i<${#page_urls[@]}; i++)); do
            local page_url="${page_urls[$((page_index + i))]}"
            local batch_page=$((page_index + i + 1))
            
            (
                local response=$(curl -s --max-time 30 -H "Authorization: Bearer $ACCESS_TOKEN" "$page_url")
                
                if ! echo "$response" | jq -e .error > /dev/null 2>&1; then
                    # Process files and folders efficiently
                    echo "$response" | jq -r '.value[]? | 
                        if has("folder") then
                            "FOLDER|" + .name
                        else
                            "FILE|" + .name
                        end' | while IFS='|' read -r type name; do
                        
                        if [ "$type" = "FOLDER" ]; then
                            # Add to folder queue for recursive processing
                            echo "$folder_path/$name" >> "$OUTPUT_DIR/folder_queue.txt"
                        elif [ "$type" = "FILE" ] && should_include_file "$name"; then
                            # Calculate relative path
                            if [ "$folder_path" = "$SHAREPOINT_FOLDER" ]; then
                                echo "$name" >> "$output_file"
                            else
                                relative_subfolder="${folder_path#$SHAREPOINT_FOLDER}"
                                relative_subfolder="${relative_subfolder#/}"
                                echo "$relative_subfolder/$name" >> "$output_file"
                            fi
                        fi
                    done
                fi
            ) &
            batch_pids+=($!)
        done
        
        # Wait for batch to complete
        for pid in "${batch_pids[@]}"; do
            wait $pid
        done
        
        page_index=$((page_index + batch_size))
        sleep 0.1  # Small delay to avoid rate limiting
    done
}

# Initialize
> "$OUTPUT_DIR/sharepoint_files_raw.txt"
> "$OUTPUT_DIR/folder_queue.txt"
echo "$SHAREPOINT_FOLDER" > "$OUTPUT_DIR/folder_queue.txt"

# Process folders recursively with parallel pagination
while [ -s "$OUTPUT_DIR/folder_queue.txt" ]; do
    # Get next folder from queue
    current_folder=$(head -n 1 "$OUTPUT_DIR/folder_queue.txt")
    tail -n +2 "$OUTPUT_DIR/folder_queue.txt" > "$OUTPUT_DIR/folder_queue_temp.txt"
    mv "$OUTPUT_DIR/folder_queue_temp.txt" "$OUTPUT_DIR/folder_queue.txt"
    
    if [ -n "$current_folder" ] && should_include_folder "$current_folder"; then
        get_sharepoint_files "$current_folder" "$OUTPUT_DIR/sharepoint_files_raw.txt"
    fi
done
# Normalize SharePoint filenames
touch "$OUTPUT_DIR/sharepoint_files_raw.txt"
RAW_SP_COUNT=$(wc -l < "$OUTPUT_DIR/sharepoint_files_raw.txt" 2>/dev/null || echo "0")
echo "DEBUG: Raw SharePoint files collected: $RAW_SP_COUNT"

normalize_filenames "$OUTPUT_DIR/sharepoint_files_raw.txt" "$OUTPUT_DIR/sharepoint_files.txt"

SP_COUNT=$(wc -l < "$OUTPUT_DIR/sharepoint_files.txt" 2>/dev/null || echo "0")
echo "DEBUG: Normalized SharePoint files: $SP_COUNT"
echo "✓ Found $SP_COUNT SharePoint files using search API ($PAGE_NUM pages)"

# 2. Get ALL Azure Blob files (no limits)
echo ""
echo "=== Step 2: Getting Azure Blob files ==="

az storage blob list \
    --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
    --container-name "$AZURE_STORAGE_CONTAINER_NAME" \
    --prefix "$AZURE_BLOB_PREFIX" \
    --query "[].name" \
    --output tsv \
    --num-results "*" > "$OUTPUT_DIR/azure_blob_all.txt" 2>/dev/null &

AZ_PID=$!
echo "Azure CLI running (PID: $AZ_PID)..."

# Wait with longer timeout for full results
COUNT=0
while kill -0 $AZ_PID 2>/dev/null && [ $COUNT -lt 120 ]; do
    sleep 5
    COUNT=$((COUNT + 5))
    echo "  Waiting... ($COUNT/120 seconds)"
done

if kill -0 $AZ_PID 2>/dev/null; then
    echo "  Timeout reached after 2 minutes, killing Azure CLI process"
    kill $AZ_PID 2>/dev/null || true
    echo "  Using partial results..."
fi

wait $AZ_PID 2>/dev/null || true

# Process Azure files - keep full relative paths for matching
if [ -f "$OUTPUT_DIR/azure_blob_all.txt" ]; then
    while IFS= read -r blob_name; do
        if [ -n "$blob_name" ]; then
            # Remove prefix to get relative path
            relative_path="$blob_name"
            if [ -n "$AZURE_BLOB_PREFIX" ] && [[ "$blob_name" == "$AZURE_BLOB_PREFIX"* ]]; then
                relative_path="${blob_name#$AZURE_BLOB_PREFIX}"
                relative_path="${relative_path#/}"
            fi
            
            # Check both file and folder filters
            filename=$(basename "$relative_path")
            folder_path=$(dirname "$relative_path")
            
            if should_include_file "$filename" && should_include_folder "$folder_path"; then
                echo "$relative_path" >> "$OUTPUT_DIR/azure_blob_files_raw.txt"  # Use full relative path
            fi
        fi
    done < "$OUTPUT_DIR/azure_blob_all.txt"
fi

# Normalize Azure blob filenames
touch "$OUTPUT_DIR/azure_blob_files_raw.txt"
RAW_BLOB_COUNT=$(wc -l < "$OUTPUT_DIR/azure_blob_files_raw.txt" 2>/dev/null || echo "0")
echo "DEBUG: Raw Azure files collected: $RAW_BLOB_COUNT"

normalize_filenames "$OUTPUT_DIR/azure_blob_files_raw.txt" "$OUTPUT_DIR/azure_blob_files.txt"

BLOB_COUNT=$(wc -l < "$OUTPUT_DIR/azure_blob_files.txt" 2>/dev/null || echo "0")
echo "DEBUG: Normalized Azure files: $BLOB_COUNT"
echo "✓ Found $BLOB_COUNT Azure Blob files"

# 3. Perform reconciliation
echo ""
echo "=== Step 3: Reconciliation ==="

# Files should already exist from normalization step

# Sort files for comparison
sort "$OUTPUT_DIR/sharepoint_files.txt" > "$OUTPUT_DIR/sharepoint_sorted.txt"
sort "$OUTPUT_DIR/azure_blob_files.txt" > "$OUTPUT_DIR/azure_blob_sorted.txt"

# Find differences
comm -23 "$OUTPUT_DIR/sharepoint_sorted.txt" "$OUTPUT_DIR/azure_blob_sorted.txt" > "$OUTPUT_DIR/missing_in_blob.txt"
comm -13 "$OUTPUT_DIR/sharepoint_sorted.txt" "$OUTPUT_DIR/azure_blob_sorted.txt" > "$OUTPUT_DIR/missing_in_sharepoint.txt"
comm -12 "$OUTPUT_DIR/sharepoint_sorted.txt" "$OUTPUT_DIR/azure_blob_sorted.txt" > "$OUTPUT_DIR/common_files.txt"

MISSING_IN_BLOB=$(wc -l < "$OUTPUT_DIR/missing_in_blob.txt")
MISSING_IN_SP=$(wc -l < "$OUTPUT_DIR/missing_in_sharepoint.txt")
COMMON=$(wc -l < "$OUTPUT_DIR/common_files.txt")

# Create summary
cat > "$OUTPUT_DIR/summary.txt" << EOF
File Reconciliation Summary
Generated: $(date)
Timestamp: $TIMESTAMP

Configuration:
- SharePoint: $SHAREPOINT_SITE_URL/$SHAREPOINT_FOLDER
- Azure Blob: $AZURE_STORAGE_CONTAINER_NAME/$AZURE_BLOB_PREFIX
- Include Extensions: $INCLUDE_EXTENSIONS$([ -n "$EXCLUDE_EXTENSIONS" ] && echo "
- Exclude Extensions: $EXCLUDE_EXTENSIONS")$([ -n "$INCLUDE_FOLDERS" ] && echo "
- Include Folders: $INCLUDE_FOLDERS")$([ -n "$EXCLUDE_FOLDERS" ] && echo "
- Exclude Folders: $EXCLUDE_FOLDERS")

Results:
- SharePoint Files: $SP_COUNT (using search API - includes subfolders)
- Azure Blob Files: $BLOB_COUNT
- Common Files: $COMMON
- Missing in Azure Blob: $MISSING_IN_BLOB
- Missing in SharePoint: $MISSING_IN_SP

Files:
- sharepoint_files.txt: All SharePoint files found
- azure_blob_files.txt: All Azure Blob files found
- missing_in_blob.txt: Files in SharePoint but not in Blob
- missing_in_sharepoint.txt: Files in Blob but not in SharePoint
- common_files.txt: Files in both locations

Note: This analysis uses SharePoint Search API to find ALL files recursively.
Matching is done by full relative path (including subfolders).
EOF

# Display results
echo ""
echo "=== RESULTS ==="
echo "SharePoint files: $SP_COUNT"
echo "Azure Blob files: $BLOB_COUNT"
echo "Common files: $COMMON"
echo "Missing in Azure Blob: $MISSING_IN_BLOB"
echo "Missing in SharePoint: $MISSING_IN_SP"
echo ""
echo "Reports saved in: $OUTPUT_DIR/"

if [ $MISSING_IN_BLOB -gt 0 ] && [ $MISSING_IN_BLOB -le 10 ]; then
    echo ""
    echo "Files missing in Azure Blob:"
    cat "$OUTPUT_DIR/missing_in_blob.txt"
fi

if [ $MISSING_IN_SP -gt 0 ] && [ $MISSING_IN_SP -le 10 ]; then
    echo ""
    echo "Files missing in SharePoint:"
    cat "$OUTPUT_DIR/missing_in_sharepoint.txt"
fi

echo ""
echo "✓ Simple reconciliation completed successfully!"

# Generate Excel report if enabled
if [ "$GENERATE_EXCEL" = "true" ]; then
    echo ""
    echo "=== Step 4: Generating Excel Report ==="
    
    if [ -f "./generate_excel_report.sh" ]; then
        echo "Running Excel report generator..."
        if ./generate_excel_report.sh "$OUTPUT_DIR"; then
            echo "✓ Excel report generated successfully!"
        else
            echo "⚠ Excel report generation failed, but reconciliation completed"
        fi
    else
        echo "⚠ Excel report generator not found (./generate_excel_report.sh)"
        echo "  Text reports are available in: $OUTPUT_DIR/"
    fi
else
    echo ""
    echo "Excel report generation disabled (GENERATE_EXCEL=false)"
fi
