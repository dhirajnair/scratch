#!/bin/bash

# Generate Excel Report from Reconciliation Output
# Usage: ./generate_excel_report.sh [output_directory]

set -e

# Function to setup Python environment
setup_python_env() {
    echo "Setting up Python environment..."
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv_excel" ]; then
        echo "Creating Python virtual environment..."
        python3 -m venv venv_excel
    fi
    
    # Activate virtual environment
    source venv_excel/bin/activate
    
    # Install required packages
    echo "Installing required Python packages..."
    pip install --quiet pandas openpyxl
    
    echo "✓ Python environment ready"
}

# Setup Python environment
setup_python_env

# Get the most recent reconciliation output directory if not specified
if [ -z "$1" ]; then
    OUTPUT_DIR=$(find reconciliation_output -maxdepth 1 -type d -name "20*" | sort | tail -1)
    if [ -z "$OUTPUT_DIR" ]; then
        echo "Error: No reconciliation output directory found"
        echo "Usage: $0 [output_directory]"
        exit 1
    fi
else
    OUTPUT_DIR="$1"
fi

if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: Directory $OUTPUT_DIR does not exist"
    exit 1
fi

echo "Generating Excel report from: $OUTPUT_DIR"

# Check if required files exist
REQUIRED_FILES=(
    "$OUTPUT_DIR/sharepoint_files.txt"
    "$OUTPUT_DIR/azure_blob_files.txt"
    "$OUTPUT_DIR/common_files.txt"
    "$OUTPUT_DIR/missing_in_blob.txt"
    "$OUTPUT_DIR/missing_in_sharepoint.txt"
    "$OUTPUT_DIR/summary.txt"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Error: Required file $file not found"
        exit 1
    fi
done

# Activate virtual environment and generate Excel file using Python
source venv_excel/bin/activate
python3 << EOF
import pandas as pd
import os
from datetime import datetime

# Read the text files
output_dir = "$OUTPUT_DIR"

# Read summary information
summary_file = os.path.join(output_dir, "summary.txt")
with open(summary_file, 'r', encoding='utf-8') as f:
    summary_content = f.read()

# Extract timestamp from directory name
timestamp = os.path.basename(output_dir)

# Read file lists
def read_file_list(filename):
    filepath = os.path.join(output_dir, filename)
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    return []

sharepoint_files = read_file_list("sharepoint_files.txt")
azure_files = read_file_list("azure_blob_files.txt")
common_files = read_file_list("common_files.txt")
missing_in_blob = read_file_list("missing_in_blob.txt")
missing_in_sharepoint = read_file_list("missing_in_sharepoint.txt")

# Create Excel file
excel_filename = os.path.join(output_dir, f"reconciliation_report_{timestamp}.xlsx")

with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    
    # Summary sheet
    summary_data = {
        'Metric': [
            'Total SharePoint Files',
            'Total Azure Blob Files', 
            'Common Files (in both locations)',
            'Missing in Azure Blob',
            'Missing in SharePoint',
            'Report Generated',
            'Timestamp'
        ],
        'Count': [
            len(sharepoint_files),
            len(azure_files),
            len(common_files),
            len(missing_in_blob),
            len(missing_in_sharepoint),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            timestamp
        ]
    }
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    # All SharePoint files
    if sharepoint_files:
        sp_df = pd.DataFrame({'SharePoint Files': sharepoint_files})
        sp_df.to_excel(writer, sheet_name='SharePoint Files', index=False)
    
    # All Azure files  
    if azure_files:
        azure_df = pd.DataFrame({'Azure Blob Files': azure_files})
        azure_df.to_excel(writer, sheet_name='Azure Blob Files', index=False)
    
    # Common files (in both locations)
    if common_files:
        common_df = pd.DataFrame({'Common Files': common_files})
        common_df.to_excel(writer, sheet_name='Common Files', index=False)
    
    # Missing in Azure Blob
    if missing_in_blob:
        missing_blob_df = pd.DataFrame({'Files Missing in Azure Blob': missing_in_blob})
        missing_blob_df.to_excel(writer, sheet_name='Missing in Azure', index=False)
    
    # Missing in SharePoint
    if missing_in_sharepoint:
        missing_sp_df = pd.DataFrame({'Files Missing in SharePoint': missing_in_sharepoint})
        missing_sp_df.to_excel(writer, sheet_name='Missing in SharePoint', index=False)
    
    # Raw summary text
    summary_lines = summary_content.split('\n')
    summary_text_df = pd.DataFrame({'Summary Report': summary_lines})
    summary_text_df.to_excel(writer, sheet_name='Raw Summary', index=False)

print(f"✓ Excel report generated: {excel_filename}")

# Get file counts for display
sp_count = len(sharepoint_files)
azure_count = len(azure_files)
common_count = len(common_files)
missing_blob_count = len(missing_in_blob)
missing_sp_count = len(missing_in_sharepoint)

print(f"")
print(f"Report Summary:")
print(f"- SharePoint Files: {sp_count}")
print(f"- Azure Blob Files: {azure_count}")
print(f"- Common Files: {common_count}")
print(f"- Missing in Azure: {missing_blob_count}")
print(f"- Missing in SharePoint: {missing_sp_count}")
print(f"")
print(f"Excel file saved as: {os.path.basename(excel_filename)}")

EOF

echo "✓ Excel report generation completed!"
