import os
import pandas as pd

def merge_parquet_files_to_root(root_dir):
    for item in os.listdir(root_dir):
        subfolder_path = os.path.join(root_dir, item)
        if os.path.isdir(subfolder_path):
            parquet_files = [
                os.path.join(subfolder_path, f)
                for f in os.listdir(subfolder_path)
                if f.endswith('.parquet') and not f.startswith('._')
            ]

            if not parquet_files:
                print(f"No parquet files in {subfolder_path}, skipping.")
                continue

            print(f"Merging {len(parquet_files)} files in {subfolder_path}...")

            dfs = []
            for pf in parquet_files:
                try:
                    df = pd.read_parquet(pf)
                    dfs.append(df)
                except Exception as e:
                    print(f"Error reading {pf}: {e}")

            if not dfs:
                print(f"No readable parquet files in {subfolder_path}, skipping.")
                continue

            merged_df = pd.concat(dfs, ignore_index=True)

            # Save merged file in the root directory, named as the subfolder
            merged_file_path = os.path.join(root_dir, f"{item}.parquet")
            merged_df.to_parquet(merged_file_path)
            print(f"Saved merged file to {merged_file_path}")

            # Delete original parquet files in the subfolder
            for pf in parquet_files:
                try:
                    os.remove(pf)
                except Exception as e:
                    print(f"Failed to delete {pf}: {e}")

            print(f"Deleted original parquet files in {subfolder_path}")

if __name__ == "__main__":
    root_directory = '/Volumes/One Touch/bigdata/data/gaia_ly'  # Change this to your root directory path
    merge_parquet_files_to_root(root_directory)
