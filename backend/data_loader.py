
import polars as pl
import numpy as np
from pathlib import Path
import streamlit as st

DATA_DIR = Path('/Volumes/One Touch/bigdata/data/gaia_ly')
MAS_TO_RAD_YR = (np.pi / 180.0) / 3_600_000.0

@st.cache_data(show_spinner="Scanning the cosmos...")
def load_and_process_stars(min_dist=10, max_dist=17000, healpix=None, max_stars=25000):
    """
    Load stars filtering by distance range and Healpix sector.
    min_dist, max_dist: Light Years
    healpix: Integer 1-12 (Level 0 sector), or None for All.
    max_stars: Limit on result size (random sample).
    """
    files = []
    
    if not DATA_DIR.exists():
        st.error(f"Data directory not found: {DATA_DIR}")
        return None

    # Filter files based on distance range
    # Files are named bin_{start_dist}.parquet (e.g. bin_0.parquet, bin_100.parquet)
    # Each bin covers [start_dist, start_dist + BIN_SIZE)
    # We assume BIN_SIZE is roughly 100 based on file Naming (0, 100, 200...)
    BIN_SIZE = 100 
    
    compatible_files = []
    
    # We can glob all and filter, or smarter if we know the naming convention fits.
    # Let's simple glob and filter by name to be safe against missing files.
    for f in DATA_DIR.glob("bin_*.parquet"):
        try:
            # Extract start distance of bin
            bin_start = int(f.name.split("_")[1].split(".")[0])
            bin_end = bin_start + BIN_SIZE
            
            # Check overlap with requested range [min_dist, max_dist]
            # Overlap if: start1 < end2 AND start2 < end1
            if bin_start < max_dist and min_dist < bin_end:
                compatible_files.append((bin_start, f))
        except:
            continue
            
    compatible_files.sort(key=lambda x: x[0])
    
    lazy_frames = []
    
    for _, fpath in compatible_files:
        
        try:
           lf = pl.scan_parquet(fpath)
           
           # Apply filters
           # Distance
           lf = lf.filter(
               (pl.col("distance_ly") >= min_dist) &
               (pl.col("distance_ly") <= max_dist)
           )
           
           # Healpix
           if healpix is not None:
               # Input is 1-12 (Level 0). Column is healpix_2 (Level 2, Nside=4).
               # Pixels per Level 0 pixel = 4^2 = 16.
               # Level 0 pixel 'p0' (0-11) contains Level 2 pixels [p0*16, (p0+1)*16 - 1]
               # User input is 1-based, so p0 = healpix - 1.
               p0 = healpix - 1
               min_hp2 = p0 * 16
               max_hp2 = (p0 + 1) * 16 - 1
               
               lf = lf.filter(
                   (pl.col("healpix_2") >= min_hp2) &
                   (pl.col("healpix_2") <= max_hp2)
               )
           
           lazy_frames.append(lf)
        except Exception as e:
            continue

    if not lazy_frames:
        return None

    try:
        combined_lf = pl.concat(lazy_frames)
        
        combined_lf = combined_lf.select([
            pl.col("source_id"),
            pl.col("distance_ly"),
            pl.col("x"),
            pl.col("y"),
            pl.col("z"),
            pl.col("vx"),
            pl.col("vy"),
            pl.col("vz"),
            pl.col("phot_g_mean_mag")
        ])

        combined_lf = combined_lf.with_columns([
            (pl.col("x") * pl.col("distance_ly")).alias("x0"),
            (pl.col("y") * pl.col("distance_ly")).alias("y0"),
            (pl.col("z") * pl.col("distance_ly")).alias("z0"),
            (pl.col("vx") * MAS_TO_RAD_YR * pl.col("distance_ly")).alias("v_lx"),
            (pl.col("vy") * MAS_TO_RAD_YR * pl.col("distance_ly")).alias("v_ly"),
            (pl.col("vz") * MAS_TO_RAD_YR * pl.col("distance_ly")).alias("v_lz"),
        ])
        
        combined_lf = combined_lf.filter(pl.col("phot_g_mean_mag").is_not_null())
        
        # Collect results to memory to perform random sampling
        df = combined_lf.collect()
        
        if len(df) > max_stars:
            # Random sampling as requested
            df = df.sample(n=max_stars, with_replacement=False, seed=42)
        
        return df
        
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return None
