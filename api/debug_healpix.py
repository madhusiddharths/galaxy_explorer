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

GCS_URI = os.getenv("GCS_URI")
client = bigquery.Client()

def verify_healpix_logic():
    print(f"Using GCS URI: {GCS_URI}")
    query_table = "gcs_stars"
    
    # Configure External Table
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [GCS_URI]
    external_config.autodetect = True
    
    job_config = bigquery.QueryJobConfig()
    job_config.table_definitions = {query_table: external_config}
    
    # Test Sector 1 (Healpix 1 -> Index 0 -> Nested 0-15)
    # Test Sector 12 (Healpix 12 -> Index 11 -> Nested 176-191)
    
    sectors = [1, 2, 6, 12]
    
    for hp in sectors:
        p0 = hp - 1
        min_hp2 = p0 * 16
        max_hp2 = (p0 + 1) * 16 - 1
        
        print(f"\nChecking Sector {hp} (Indices {min_hp2}-{max_hp2})...")
        
        query = f"""
            SELECT COUNT(*) as count
            FROM {query_table}
            WHERE healpix_2 BETWEEN {min_hp2} AND {max_hp2}
        """
        try:
            row = list(client.query(query, job_config=job_config).result())[0]
            print(f"  Count: {row.count}")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    verify_healpix_logic()
