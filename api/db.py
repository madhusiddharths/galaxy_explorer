from google.cloud import bigquery
import os
import logging
from typing import List, Optional, Tuple, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize BigQuery Client
# Assumes GOOGLE_APPLICATION_CREDENTIALS or default auth is set
try:
    client = bigquery.Client()
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    client = None

# Configuration
DATASET_ID = os.getenv("BIGQUERY_DATASET", "gaia_ly")
TABLE_ID = os.getenv("BIGQUERY_TABLE", "gaia_ly.stars")
GCS_URI = os.getenv("GCS_URI")

async def query_stars(
    min_dist: float,
    max_dist: float,
    healpix: Optional[int] = None,
    limit: int = 25000,
    year: int = 2016
) -> List[Dict[str, Any]]:
    """
    Query stars from BigQuery based on distance and healpix.
    Project positions to `year`.
    
    If GCS_URI is set, queries GCS directly using an ephemeral table definition.
    Otherwise, queries the standard BigQuery table.
    """
    if not client:
        logger.error("BigQuery client not initialized.")
        return []

    logger.info(f"Querying stars: min_dist={min_dist}, max_dist={max_dist}, healpix={healpix}, limit={limit}, year={year}")

    # Projection logic
    import numpy as np
    MAS_TO_RAD_YR = (np.pi / 180.0) / 3_600_000.0
    delta_t = year - 2016.0
    
    # Determine source table
    if GCS_URI:
        # Use ephemeral table name
        query_table = "gcs_stars"
    else:
        # Use persistent table
        query_table = f"`{TABLE_ID}`"

    query = f"""
        SELECT 
            source_id,
            x, y, z,
            vx, vy, vz,
            phot_g_mean_mag,
            distance_ly
        FROM {query_table}
        WHERE distance_ly BETWEEN @min_dist AND @max_dist
        AND phot_g_mean_mag IS NOT NULL
    """
    
    params = [
        bigquery.ScalarQueryParameter("min_dist", "FLOAT64", min_dist),
        bigquery.ScalarQueryParameter("max_dist", "FLOAT64", max_dist),
    ]
    
    # Healpix Filter
    if healpix is not None:
        p0 = healpix - 1
        min_hp2 = p0 * 16
        max_hp2 = (p0 + 1) * 16 - 1
        
        query += " AND healpix_2 BETWEEN @min_hp AND @max_hp"
        params.extend([
            bigquery.ScalarQueryParameter("min_hp", "INT64", min_hp2),
            bigquery.ScalarQueryParameter("max_hp", "INT64", max_hp2),
        ])

    query += " ORDER BY RAND() LIMIT @limit"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    
    # Configure Ephemeral Table if GCS_URI is set
    if GCS_URI:
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [GCS_URI]
        external_config.autodetect = True
        job_config.table_definitions = {query_table: external_config}
    
    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        stars = []
        for row in results:
            dist = row.distance_ly
            x0 = row.x * dist
            y0 = row.y * dist
            z0 = row.z * dist
            
            v_lx = row.vx * MAS_TO_RAD_YR * dist
            v_ly = row.vy * MAS_TO_RAD_YR * dist
            v_lz = row.vz * MAS_TO_RAD_YR * dist
            
            stars.append({
                "source_id": str(row.source_id),
                "x": x0 + v_lx * delta_t,
                "y": y0 + v_ly * delta_t,
                "z": z0 + v_lz * delta_t,
                "mag": row.phot_g_mean_mag,
                "dist": dist
            })
            
        return stars
        
    except Exception as e:
        logger.error(f"BigQuery execution failed: {e}")
        return []
