import polars as pl
import numpy as np
from pathlib import Path

# Input/output paths
input_dir = Path('/Volumes/One Touch/bigdata/data/gaia_partitioned')
output_dir = Path('/Volumes/One Touch/bigdata/data/gaia_ly')
output_dir.mkdir(parents=True, exist_ok=True)

# Constants
MAS_TO_ARCSEC = 1 / 1000
PARSEC_TO_LY = 3.26156

def compute_columns(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(df["parallax"] > 0)

    ra_rad = np.radians(df["ra"].to_numpy())
    dec_rad = np.radians(df["dec"].to_numpy())

    parallax_np = df["parallax"].to_numpy()
    low_parallax_mask = parallax_np < 0.2
    valid_mask = ~low_parallax_mask

    distance_pc = np.empty(df.height)
    distance_ly = np.empty(df.height)

    distance_pc[valid_mask] = 1000.0 / parallax_np[valid_mask]
    distance_ly[valid_mask] = distance_pc[valid_mask] * PARSEC_TO_LY

    distance_ly[low_parallax_mask] = 10000  # assign max distance for low parallax

    x = np.cos(dec_rad) * np.cos(ra_rad)
    y = np.cos(dec_rad) * np.sin(ra_rad)
    z = np.sin(dec_rad)

    pmra = df["pmra"].to_numpy()
    pmdec = df["pmdec"].to_numpy()
    vx = -pmra * np.sin(ra_rad) - pmdec * np.sin(dec_rad) * np.cos(ra_rad)
    vy = pmra * np.cos(ra_rad) - pmdec * np.sin(dec_rad) * np.sin(ra_rad)
    vz = pmdec * np.cos(dec_rad)

    bins = (distance_ly // 50).astype(int) * 50

    return df.with_columns([
        pl.Series("distance_ly", distance_ly),
        pl.Series("x", x),
        pl.Series("y", y),
        pl.Series("z", z),
        pl.Series("vx", vx),
        pl.Series("vy", vy),
        pl.Series("vz", vz),
        pl.Series("distance_bin", bins),
    ])

# Start processing
for parquet_path in input_dir.glob("*.parquet"):
    if parquet_path.name.startswith("._"):
        continue  # Skip macOS system files

    print(f"Processing: {parquet_path.name}")

    df = pl.read_parquet(parquet_path)
    df = compute_columns(df)

    if df.height == 0:
        print(f"⚠️ Skipped: {parquet_path.name} (no valid rows)")
        continue

    # Group by distance bin and write to folders
    for (bin_val,), group_df in df.group_by("distance_bin", maintain_order=False):
        bin_dir = output_dir / f"bin_{int(bin_val)}"
        bin_dir.mkdir(exist_ok=True)

        out_path = bin_dir / f"{parquet_path.stem}_bin_{int(bin_val)}.parquet"
        group_df.write_parquet(out_path)

    print(f"✅ Finished: {parquet_path.name}")

print("🎉 All files processed and partitioned by light-year bins.")
