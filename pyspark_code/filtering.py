import polars as pl
import os

folder_path = "/Volumes/One Touch/bigdata/data/gaia_dr3_col_removed"
output_folder = '/Volumes/One Touch/bigdata/data/gaia_filtered'

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
            'parallax',
            'pmra',
            'pmdec'
        ])

        # Derived columns
        df = df.with_columns([
            (pl.col("scan_direction_strength_k1") +
             pl.col("scan_direction_strength_k2") +
             pl.col("scan_direction_strength_k3") +
             pl.col("scan_direction_strength_k4")).alias("total_scan_strength"),
        ])

        df = df.with_columns([
            (pl.col("scan_direction_strength_k1") / pl.col("total_scan_strength")).alias("scan_diversity_ratio"),
            (pl.col("phot_bp_mean_mag") - pl.col("phot_rp_mean_mag")).alias("bp_rp"),
        ])

        # Extract source_id integer from designation string
        df = df.with_columns([
            pl.col("designation")
            .str.extract(r"(\d+)$")
            .cast(pl.UInt64)
            .alias("source_id")
        ])

        # Compute healpix_12 and healpix_2 (optional, can keep for later)
        df = df.with_columns([
            (pl.col("source_id") // 34359738368).alias("healpix_12"),
            ((pl.col("source_id") // 34359738368) // 1048576).alias("healpix_2")
        ])

        # Filter mask
        mask = (
            (pl.col("astrometric_n_good_obs_al") >= 15) &
            (pl.col("astrometric_n_bad_obs_al") <= 5) &
            (pl.col("astrometric_excess_noise") < 1) &
            (pl.col("astrometric_params_solved") != 3) &
            (pl.col("astrometric_sigma5d_max") <= 2) &
            (pl.col("ruwe") < 1.4) &
            (pl.col("ipd_frac_odd_win") < 0.1) &
            (pl.col("scan_diversity_ratio") <= 0.8) &
            (pl.col("bp_rp") > -1) &
            (pl.col("duplicated_source") == False) &
            (pl.col("phot_g_mean_flux_over_error") > 10) &
            (pl.col("phot_bp_n_contaminated_transits") <= 2) &
            (pl.col("phot_rp_n_contaminated_transits") <= 2) &
            (pl.col("phot_bp_n_blended_transits") <= 3) &
            (pl.col("phot_rp_n_blended_transits") <= 3) &
            (pl.col("phot_bp_rp_excess_factor") <
                (1.3 + 0.06 * (pl.col("bp_rp") ** 2))) &
            (pl.col("non_single_star") == 0) &
            (pl.col("in_qso_candidates") == False) &
            (pl.col("in_galaxy_candidates") == False)
        )

        df_filtered = df.filter(mask)

        # Write filtered dataframe as parquet, same filename but in output folder
        output_file = os.path.join(output_folder, file)
        df_filtered.write_parquet(output_file)

        print(f"Processed {i+1}/{len(parquet_files)}: {file} | Rows filtered: {df_filtered.shape[0]}")

    except Exception as e:
        print(f"Failed on {file}: {e}")
