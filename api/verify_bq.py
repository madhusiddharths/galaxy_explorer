import os
from dotenv import load_dotenv
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load env
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

project = os.getenv("GOOGLE_CLOUD_PROJECT")
dataset = os.getenv("BIGQUERY_DATASET")
table = os.getenv("BIGQUERY_TABLE")
creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

print(f"Loaded config:")
print(f"  Project: {project}")
print(f"  Dataset: {dataset}")
print(f"  Table: {table}")
print(f"  Creds: {creds}")

try:
    client = bigquery.Client()
    print("BigQuery Client Initialized Successfully.")
    
    # Try a simple query
    query = f"SELECT count(*) as count FROM `{table}`"
    print(f"Running query: {query}")
    
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        print(f"Connection Successful! Total stars in table: {row.count}")
        
except Exception as e:
    print(f"Connection Failed: {e}")
