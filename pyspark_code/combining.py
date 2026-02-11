import polars as pl
import os
import glob
import shutil

base_dir = "/Volumes/One Touch/bigdata/data/gaia_partitioned"

parquet_folders = [
    f for f in os.listdir(base_dir)
    if f.startswith("healpix_2=") and os.path.isdir(os.path.join(base_dir, f))
]

for folder in sorted(parquet_folders):
    folder_path = os.path.join(base_dir, folder)
    healpix_2_value = folder.split("=")[-1]

    files = glob.glob(os.path.join(folder_path, "*.parquet"))
    if not files:
        print(f"Skipping {folder} — no files found")
        continue

    print(f"Merging {len(files)} files in {folder}...")

    # Merge all files
    try:
        merged_df = pl.concat([pl.read_parquet(f) for f in files], how="vertical")

        # New flat file path
        flat_file_path = os.path.join(base_dir, f"healpix_2={healpix_2_value}.parquet")

        # Write single merged Parquet
        merged_df.write_parquet(flat_file_path)

        # Delete original folder and contents
        shutil.rmtree(folder_path)

        print(f"Merged into {flat_file_path} and deleted folder")

    except Exception as e:
        print(f"Failed to merge {folder}: {e}")
