import polars as pl
from pathlib import Path

# Set your merged parquet directory
merged_dir = Path('/Volumes/One Touch/bigdata/data/gaia_partitioned')

# Columns to keep (and required to be present to skip rewriting)
keep_cols = [
    "phot_g_mean_mag", "phot_bp_mean_mag", "phot_rp_mean_mag", "bp_rp",
    "has_rvs", "ra", "dec", "parallax", "pmra", "pmdec", "source_id", "healpix_2"
]
keep_cols_set = set(keep_cols)

# Iterate over each .parquet file
for file in merged_dir.glob("*.parquet"):
    if file.name.startswith("._"):
        print(f"Deleting macOS metadata file: {file.name}")
        try:
            file.unlink()
        except Exception as e:
            print(f"Failed to delete {file.name}: {e}")
        continue

    try:
        df = pl.read_parquet(file)
        df_cols_set = set(df.columns)

        # If the file already has exactly the desired columns, skip it
        if df_cols_set == keep_cols_set:
            print(f"Skipping {file.name} (already processed)")
            continue

        print(f"Processing {file.name}")
        selected = df.select([col for col in keep_cols if col in df.columns])
        selected.write_parquet(file, compression="zstd")
    except Exception as e:
        print(f"Error processing {file.name}: {e}")
