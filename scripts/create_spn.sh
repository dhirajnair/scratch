# Variables
SUBSCRIPTION_ID=<id>
RESOURCE_GROUP=data-pipeline-rg  
STORAGE_ACCOUNT=dataingestiondl
SP_NAME=data-ingestion-spn

# Login
az login
az account set --subscription $SUBSCRIPTION_ID

# Create Service Principal
az ad sp create-for-rbac \
  --name $SP_NAME \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT \
  --sdk-auth