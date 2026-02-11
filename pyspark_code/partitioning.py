import polars as pl
import os

input_folder = "/Volumes/One Touch/bigdata/data/gaia_filtered"
output_base = "/Volumes/One Touch/bigdata/data/gaia_partitioned"

os.makedirs(output_base, exist_ok=True)

parquet_files = [
    f for f in os.listdir(input_folder)
    if f.endswith(".parquet") and not f.startswith("._")
]

for i, file in enumerate(parquet_files):
    try:
        file_path = os.path.join(input_folder, file)
        df = pl.read_parquet(file_path)

        # Drop unnecessary columns
        df = df.drop([col for col in ["designation", "healpix_12"] if col in df.columns])

        # Partition by healpix_2
        for hp2, subdf in df.partition_by("healpix_2", as_dict=True).items():
            partition_folder = os.path.join(output_base, f"healpix_2={hp2}")
            os.makedirs(partition_folder, exist_ok=True)
            out_file = os.path.join(partition_folder, f"{file}")
            subdf.write_parquet(out_file)

        print(f"[{i+1}/{len(parquet_files)}] Partitioned {file} ✔️")

    except Exception as e:
        print(f"[{i+1}/{len(parquet_files)}] Failed on {file}: {e}")
