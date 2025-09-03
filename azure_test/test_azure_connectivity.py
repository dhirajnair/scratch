#!/usr/bin/env python3
"""
Azure Spark Connectivity Test

This script tests your local environment's ability to connect to Azure Spark resources.
"""

import sys
import os

def test_azure_cli():
    """Test Azure CLI authentication"""
    print("🔐 Testing Azure CLI Authentication...")
    
    try:
        import subprocess
        result = subprocess.run(['az', 'account', 'show'], capture_output=True, text=True)
        
        if result.returncode == 0:
            import json
            account_info = json.loads(result.stdout)
            print(f"✅ Azure CLI authenticated as: {account_info.get('user', {}).get('name', 'Unknown')}")
            print(f"✅ Subscription: {account_info.get('name', 'Unknown')}")
            return True
        else:
            print(f"❌ Azure CLI not authenticated: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Azure CLI test failed: {e}")
        return False

def test_pyspark_local():
    """Test if PySpark can run locally"""
    print("\n⚡ Testing Local PySpark...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("ConnectivityTest") \
            .master("local[2]") \
            .getOrCreate()
        
        # Simple test
        df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Local PySpark working: Created DataFrame with {count} rows")
        return True
        
    except Exception as e:
        print(f"❌ Local PySpark failed: {e}")
        return False

def test_databricks_connect():
    """Test Databricks Connect if configured"""
    print("\n🧱 Testing Databricks Connect...")
    
    try:
        # Check if databricks-connect is installed
        import subprocess
        result = subprocess.run(['databricks-connect', 'test'], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Databricks Connect configured and working")
            return True
        else:
            print(f"⚠️  Databricks Connect not configured or failed: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("⚠️  Databricks Connect not installed")
        print("   Install with: pip install databricks-connect")
        return False
    except Exception as e:
        print(f"⚠️  Databricks Connect test error: {e}")
        return False

def test_azure_storage_access():
    """Test Azure Storage access"""
    print("\n💾 Testing Azure Storage Access...")
    
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
        
        # Try to create a client using default credentials
        credential = DefaultAzureCredential()
        print("✅ Azure credentials available")
        
        # Note: This doesn't actually connect without a storage account URL
        print("⚠️  To test storage access, you need to specify your storage account")
        return True
        
    except ImportError:
        print("⚠️  Azure SDK not installed")
        print("   Install with: pip install azure-identity azure-storage-blob")
        return False
    except Exception as e:
        print(f"⚠️  Azure Storage test error: {e}")
        return False

def check_environment_variables():
    """Check for relevant environment variables"""
    print("\n🔧 Checking Environment Variables...")
    
    env_vars = [
        'DATABRICKS_HOST',
        'DATABRICKS_TOKEN', 
        'AZURE_CLIENT_ID',
        'AZURE_CLIENT_SECRET',
        'AZURE_TENANT_ID'
    ]
    
    found_vars = []
    for var in env_vars:
        if os.getenv(var):
            found_vars.append(var)
            print(f"✅ {var}: {'*' * 10} (set)")
        else:
            print(f"⚪ {var}: Not set")
    
    if found_vars:
        print(f"✅ Found {len(found_vars)} Azure/Databricks environment variables")
    else:
        print("⚠️  No Azure/Databricks environment variables found")
    
    return len(found_vars) > 0

def provide_recommendations():
    """Provide recommendations based on test results"""
    print("\n" + "="*60)
    print("📋 RECOMMENDATIONS")
    print("="*60)
    
    print("\n🎯 To run your Enhanced Data Processor with Azure Spark:")
    
    print("\n1️⃣  EASIEST: Use Databricks Notebook")
    print("   • Upload enhanced_data_processor.py to Databricks workspace")
    print("   • Create a notebook and import the functions")
    print("   • Run directly on the cluster")
    
    print("\n2️⃣  LOCAL DEVELOPMENT: Set up Databricks Connect")
    print("   • Install: pip install databricks-connect")
    print("   • Configure: databricks-connect configure")
    print("   • You'll need:")
    print("     - Databricks workspace URL")
    print("     - Personal access token")
    print("     - Cluster ID")
    
    print("\n3️⃣  ALTERNATIVE: Use Azure Synapse")
    print("   • Create Synapse workspace")
    print("   • Use Synapse Studio notebooks")
    print("   • Built-in Spark pools")
    
    print("\n4️⃣  QUICK TEST: Run locally first")
    print("   • python test_local_setup.py")
    print("   • Tests configuration without Spark connectivity")

def main():
    """Run all connectivity tests"""
    print("🚀 Azure Spark Connectivity Test")
    print("="*60)
    
    tests = [
        ("Azure CLI Authentication", test_azure_cli),
        ("Local PySpark", test_pyspark_local),
        ("Databricks Connect", test_databricks_connect),
        ("Azure Storage Access", test_azure_storage_access),
        ("Environment Variables", check_environment_variables)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"💥 {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    if results.get("Azure CLI Authentication") and results.get("Local PySpark"):
        print("\n🎉 Good foundation! You have Azure auth and PySpark working.")
        print("   Next: Set up Databricks Connect or use Databricks notebook.")
    elif results.get("Local PySpark"):
        print("\n⚠️  PySpark works locally, but no Azure connectivity.")
        print("   You can test the processor logic locally first.")
    else:
        print("\n❌ Missing basic requirements.")
        print("   Install PySpark: pip install pyspark")
    
    provide_recommendations()
    
    return 0 if passed >= 2 else 1

if __name__ == "__main__":
    sys.exit(main())
