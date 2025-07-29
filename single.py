import polars as pl
from pathlib import Path
from tqdm import tqdm

# Path setup
input_dir = Path('/Volumes/One Touch/bigdata/data/gaia_visible_from_earth')
output_file = Path("/Users/madhusiddharthsuthagar/Documents/bigdata/mac_gaia/gaia_visible_combined.parquet")

# List all Parquet files, excluding macOS metadata files like ._filename
parquet_files = sorted(f for f in input_dir.glob("*.parquet") if not f.name.startswith("._"))

# Collect non-empty DataFrames
frames = []

for f in tqdm(parquet_files, desc="Loading Parquet files"):
    try:
        df = pl.read_parquet(f)
        if df.shape[0] > 0:
            frames.append(df)
    except Exception as e:
        print(f"⚠️ Error reading {f.name}: {e}")

# Combine and write
if frames:
    combined = pl.concat(frames)
    combined.write_parquet(output_file)
    print(f"✅ Combined {len(frames)} files into {output_file.name} with {combined.shape[0]:,} rows.")
else:
    print("⚠️ No valid data found in any file.")
