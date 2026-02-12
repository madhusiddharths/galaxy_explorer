
import polars as pl
import numpy as np
from pathlib import Path
import streamlit as st

DATA_DIR = Path('/Volumes/One Touch/bigdata/data/gaia_ly')
MAS_TO_RAD_YR = (np.pi / 180.0) / 3_600_000.0

@st.cache_data(show_spinner="Scanning the cosmos...")
def load_and_process_stars(max_stars=25000, x_range=(-2000, 2000), y_range=(-2000, 2000), z_range=(-2000, 2000), radius=None):
    """
    Load stars, filtering by a 3D rectangular region (ranges) OR by a spherical radius.
    
    If radius is provided:
        - Filters by sphere center (0,0,0) and radius.
        - Sorts by brightness (phot_g_mean_mag) and takes top max_stars.
        - Used for the default "Earth View".
        
    If radius is None:
        - Filters by x_range, y_range, z_range box.
        - Randomly samples max_stars.
        - Used for "Custom Focus" view.
    """
    files = []
    
    if not DATA_DIR.exists():
        st.error(f"Data directory not found: {DATA_DIR}")
        return None

    # Determine distance range for file filtering
    min_dist_file = 0
    max_dist_file = 0
    
    if radius is not None:
        # Spherical logic (centered at 0,0,0 as per updated request)
        max_dist_file = radius
    else:
        # Box logic
        corners_x = [x_range[0], x_range[1]]
        corners_y = [y_range[0], y_range[1]]
        corners_z = [z_range[0], z_range[1]]
        
        for x in corners_x:
            for y in corners_y:
                for z in corners_z:
                    d = np.sqrt(x**2 + y**2 + z**2)
                    if d > max_dist_file:
                        max_dist_file = d

    # Load compatible files
    for f in DATA_DIR.glob("bin_*.parquet"):
        try:
            dist = int(f.name.split("_")[1].split(".")[0])
            if dist > max_dist_file: 
                continue
            files.append((dist, f))
        except:
            continue
            
    files.sort(key=lambda x: x[0])
    
    lazy_frames = []
    
    # Pre-calc filter bounds
    if radius is None:
        min_x, max_x = x_range
        min_y, max_y = y_range
        min_z, max_z = z_range
    else:
        # Optimization: use bounding box of sphere for initial filter
        min_x, max_x = -radius, radius
        min_y, max_y = -radius, radius
        min_z, max_z = -radius, radius
    
    for _, fpath in files:
        try:
           lf = pl.scan_parquet(fpath)
           
           # Apply geometric filter (Box or BBox of Sphere)
           lf = lf.filter(
               (pl.col("x") * pl.col("distance_ly") >= min_x) &
               (pl.col("x") * pl.col("distance_ly") <= max_x) &
               (pl.col("y") * pl.col("distance_ly") >= min_y) &
               (pl.col("y") * pl.col("distance_ly") <= max_y) &
               (pl.col("z") * pl.col("distance_ly") >= min_z) &
               (pl.col("z") * pl.col("distance_ly") <= max_z)
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
        
        # Apply strict spherical filter if needed
        if radius is not None:
             combined_lf = combined_lf.filter(
                (pl.col("x0")**2 + pl.col("y0")**2 + pl.col("z0")**2) <= radius**2
            )
        
        # Sampling Strategy
        if radius is not None:
            # ORIGINAL BEHAVIOR: Sort by brightness
            q = combined_lf.sort("phot_g_mean_mag").limit(max_stars)
            return q.collect()
        else:
            # NEW BEHAVIOR: Random sample
            # Collect to memory for uniform random sample
            df = combined_lf.collect()
            if len(df) > max_stars:
                df = df.sample(n=max_stars, with_replacement=False, seed=42)
            return df
        
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return None
