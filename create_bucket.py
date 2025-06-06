import os
from supabase import create_client, Client
from dotenv import load_dotenv
import json
import httpx

# Load environment variables from .env file
load_dotenv()

# Get Supabase credentials from env
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")

print(f"URL: {supabase_url}")
print(f"Key: {supabase_key[:10]}...") if supabase_key else print("No key found")

if not supabase_url or not supabase_key:
    print("Missing Supabase credentials. Make sure SUPABASE_URL and SUPABASE_SERVICE_KEY are set.")
    exit(1)

# Create Supabase client
try:
    supabase = create_client(supabase_url, supabase_key)

    # List existing buckets
    buckets = supabase.storage.list_buckets()
    print(f"Existing buckets: {[b['name'] for b in buckets if 'name' in b]}")
    
    # Check if bucket already exists
    if not any(bucket.get('name') == 'combined-datasets' for bucket in buckets):
        # Create a private bucket for combined datasets using direct API call
        print("Creating combined-datasets bucket...")
        
        # Create using httpx directly
        bucket_url = f"{supabase_url}/storage/v1/bucket"
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "name": "combined-datasets",
            "public": False
        }
        
        with httpx.Client() as client:
            response = client.post(bucket_url, headers=headers, json=payload)
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            
            if response.status_code == 200:
                print("Bucket created successfully")
            else:
                print(f"Failed to create bucket: {response.text}")
    else:
        print("Bucket 'combined-datasets' already exists")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    # Clean up client
    if 'supabase' in locals():
        supabase.auth.close() 