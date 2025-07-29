import polars as pl
import os

folder_path = "/Volumes/One Touch/bigdata/data/gaia_dr3_col_removed"
output_folder = '/Volumes/One Touch/bigdata/data/gaia_visible_from_earth'

os.makedirs(output_folder, exist_ok=True)

parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet") and not f.startswith("._")]

for i, file in enumerate(parquet_files):
    file_path = os.path.join(folder_path, file)

    try:
        df = pl.read_parquet(file_path)

        # Drop rows with missing key columns
        df = df.drop_nulls([
            'phot_g_mean_mag',
            'phot_bp_mean_mag',
            'phot_rp_mean_mag',
            'has_rvs',
            'ra',
            'dec',
            'parallax',
            'pmra',
            'pmdec'
        ])

        # Derived color index
        df = df.with_columns([
            (pl.col("phot_bp_mean_mag") - pl.col("phot_rp_mean_mag")).alias("bp_rp"),
        ])

        # Extract source_id integer from designation string
        df = df.with_columns([
            pl.col("designation")
            .str.extract(r"(\d+)$")
            .cast(pl.UInt64)
            .alias("source_id")
        ])

        # Compute healpix_2 from healpix_12
        df = df.with_columns([
            (pl.col("source_id") // 34359738368).alias("healpix_12"),
            ((pl.col("source_id") // 34359738368) // 1048576).alias("healpix_2")
        ])

        # Basic quality + brightness + visibility filters
        mask = (
            (pl.col("phot_g_mean_mag") <= 6.5) &  # naked eye cutoff
            (pl.col("bp_rp") > -1) &
            (pl.col("duplicated_source") == False) &
            (pl.col("ruwe") < 2.0)  # relaxed RUWE threshold
        )

        df_filtered = df.filter(mask)

        # Retain only relevant columns
        columns_to_keep = [
            "ra", "dec", "parallax", "pmra", "pmdec",
            "phot_g_mean_mag", "phot_bp_mean_mag", "phot_rp_mean_mag",
            "bp_rp", "source_id", "has_rvs", "healpix_2"
        ]
        df_filtered = df_filtered.select(columns_to_keep)

        # Save
        output_file = os.path.join(output_folder, file)
        df_filtered.write_parquet(output_file)

        print(f"Processed {i+1}/{len(parquet_files)}: {file} | Stars kept: {df_filtered.shape[0]}")

    except Exception as e:
        print(f"Failed on {file}: {e}")
