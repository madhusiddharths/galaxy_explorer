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

def verify_data():
    if not GCS_URI:
        print("GCS_URI not found in .env, cannot debug external table.")
        return

    print(f"Using GCS URI: {GCS_URI}")
    query_table = "gcs_stars"
    
    # Configure External Table
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [GCS_URI]
    external_config.autodetect = True
    
    job_config = bigquery.QueryJobConfig()
    job_config.table_definitions = {query_table: external_config}
    
    # Check Min/Max Distance
    print("Checking Min/Max Distance...")
    query_dist = f"""
        SELECT 
            MIN(distance_ly) as min_dist,
            MAX(distance_ly) as max_dist,
            COUNT(*) as total_rows
        FROM {query_table}
    """
    try:
        row_dist = list(client.query(query_dist, job_config=job_config).result())[0]
        print(f"Distance Range: {row_dist.min_dist} - {row_dist.max_dist} ly")
        print(f"Total Rows: {row_dist.total_rows}")
    except Exception as e:
        print(f"Error checking distance: {e}")

    # Check HEALPix distribution (Sample)
    print("\nChecking HEALPix distribution...")
    query_hp = f"""
        SELECT healpix_2, COUNT(*) as count
        FROM {query_table}
        GROUP BY healpix_2
        ORDER BY count DESC
        LIMIT 10
    """
    try:
        print("\nTop 10 HEALPix indices:")
        for row in client.query(query_hp, job_config=job_config).result():
            print(f"  HP {row.healpix_2}: {row.count}")
    except Exception as e:
        print(f"Error checking HEALPix: {e}")

    # Check specific range query (10-400 ly)
    print("\nChecking 10-400 ly range...")
    query_range = f"""
        SELECT COUNT(*) as count
        FROM {query_table}
        WHERE distance_ly BETWEEN 10 AND 400
    """
    try:
        row_range = list(client.query(query_range, job_config=job_config).result())[0]
        print(f"Stars in 10-400 ly range: {row_range.count}")
    except Exception as e:
        print(f"Error checking range: {e}")

if __name__ == "__main__":
    verify_data()
