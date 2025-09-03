#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | sed 's/#.*//g' | xargs)
fi

# --- 1. Get Microsoft Graph API Access Token ---
echo "Requesting Access Token from Microsoft Graph API..."
TOKEN_RESPONSE=$(curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded" \
    "https://login.microsoftonline.com/$GRAPH_TENANT_ID/oauth2/v2.0/token" \
    -d "client_id=$GRAPH_CLIENT_ID" \
    -d "client_secret=$GRAPH_CLIENT_SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=https://graph.microsoft.com/.default")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r .access_token)

if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" == "null" ]; then
    echo "Failed to get access token. Response:"
    echo "$TOKEN_RESPONSE"
    exit 1
fi
echo "Successfully obtained access token."

# --- 2. Convert SharePoint URL to Graph API format ---
# The sharing URL needs to be base64 encoded in a specific way for the API
ENCODED_URL=$(python -c "import base64; print('u!' + base64.b64encode('$SHAREPOINT_URL'.encode()).decode().rstrip('=').replace('/', '_').replace('+', '-'))")

# --- 3. Get File Metadata from Graph API ---
echo "Fetching file metadata from SharePoint..."
FILE_METADATA=$(curl -s -L -H "Authorization: Bearer $ACCESS_TOKEN" \
    "https://graph.microsoft.com/v1.0/shares/$ENCODED_URL/driveItem")

DOWNLOAD_URL=$(echo "$FILE_METADATA" | jq -r '."@microsoft.graph.downloadUrl"')

if [ -z "$DOWNLOAD_URL" ] || [ "$DOWNLOAD_URL" == "null" ]; then
    echo "Failed to get file download URL. Response:"
    echo "$FILE_METADATA"
    exit 1
fi
echo "Successfully fetched file download URL."

# --- 4. Download the file content ---
echo "Downloading file..."
TEMP_FILE="temp_downloaded_file"
curl -s -L -H "Authorization: Bearer $ACCESS_TOKEN" "$DOWNLOAD_URL" -o "$TEMP_FILE"
echo "File downloaded to $TEMP_FILE."

# --- 5. Upload to Azure Blob Storage ---
echo "Uploading file to Azure Blob Storage..."
az storage blob upload \
    --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
    --container-name "$AZURE_STORAGE_CONTAINER_NAME" \
        --name "$AZURE_STORAGE_BLOB_PATH" \
    --file "$TEMP_FILE" \
    --overwrite

echo "File successfully uploaded to Azure Blob Storage."

# --- 6. Cleanup ---
rm "$TEMP_FILE"
echo "Cleanup complete."
