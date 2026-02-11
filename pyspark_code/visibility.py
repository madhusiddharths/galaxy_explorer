import polars as pl
from pymongo import MongoClient
from pathlib import Path

# MongoDB setup
client = MongoClient("mongodb://localhost:27017")
db = client["star_db"]
collection = db["city_visibility"]

# Gaia raw data and SSD cache
GAIA_PATH = Path('/Volumes/One Touch/bigdata/data/gaia_partitioned')
SSD_CACHE = Path("/Users/madhusiddharthsuthagar/Documents/bigdata/mac_gaia/stars")
SSD_CACHE.mkdir(exist_ok=True)

# Define cities and coordinates
cities = {
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "London": (51.5074, -0.1278),
    "Delhi": (28.6139, 77.2090),
    "Tokyo": (35.6895, 139.6917),
}

def visible_dec_range(lat_deg: float):
    return lat_deg - 90, lat_deg + 90

def generate_and_cache_visible_stars(city: str, lat: float, lon: float):
    dec_min, dec_max = visible_dec_range(lat)
    city_file = SSD_CACHE / f"visible_stars_{city.replace(' ', '_').lower()}.parquet"

    print(f"Generating stars visible from {city}...")

    scan = pl.scan_parquet(str(GAIA_PATH / "*.parquet"))
    filtered = scan.filter(
        (pl.col("dec") >= dec_min) & (pl.col("dec") <= dec_max)
    ).select([
        "source_id", "ra", "dec", "phot_g_mean_mag", "phot_bp_mean_mag",
        "phot_rp_mean_mag", "bp_rp", "parallax", "pmra", "pmdec", "has_rvs", "healpix_2"
    ])

    filtered_df = filtered.collect()
    filtered_df.write_parquet(city_file)

    # Upsert (replace if exists)
    collection.replace_one(
        {"city": city},
        {
            "city": city,
            "lat": lat,
            "lon": lon,
            "parquet_path": str(city_file)
        },
        upsert=True
    )

    print(f"Done. Saved to: {city_file}")

# Example batch run for predefined cities
if __name__ == "__main__":
    for city, (lat, lon) in cities.items():
        generate_and_cache_visible_stars(city, lat, lon)
