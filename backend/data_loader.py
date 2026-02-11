
import polars as pl
import numpy as np
from pathlib import Path
import streamlit as st

DATA_DIR = Path('/Volumes/One Touch/bigdata/data/gaia_ly')
MAS_TO_RAD_YR = (np.pi / 180.0) / 3_600_000.0

@st.cache_data(show_spinner="Scanning the cosmos...")
def load_and_process_stars(max_stars=25000, center=(0, 0, 0), radius=None):
    """
    Load stars, optionally filtering by a 3D region (center + radius).
    center: (x, y, z) tuple in Light Years
    radius: float in Light Years (if None, loads everything up to max_stars)
    """
    files = []
    
    if not DATA_DIR.exists():
        st.error(f"Data directory not found: {DATA_DIR}")
        return None

    # Determine distance range for file filtering
    min_dist_file = 0
    max_dist_file = float('inf')
    
    if radius is not None:
        cx, cy, cz = center
        center_dist = np.sqrt(cx**2 + cy**2 + cz**2)
        min_dist_file = max(0, center_dist - radius)
        max_dist_file = center_dist + radius
        
    for f in DATA_DIR.glob("bin_*.parquet"):
        try:
            dist = int(f.name.split("_")[1].split(".")[0])
            # Filter files based on distance shell
            # Each bin is likely 50ly or 100ly. Let's assume 50ly for safety.
            # If bin is 1000, it contains stars 1000-1050?? Or 1000-1100?
            # We'll be generous and load if ANY part of bin might overlap.
            # Let's assume bin size is approx 50ly (based on file listing: 0, 50, 100...)
            # Actually listing showed 0, 100, 1000...
            # Wait, file list showed bin_0, bin_100, bin_1000.
            # It seems bin size varies or is large.
            # Safest is to just check if bin_start <= max_dist_file.
            # And we can't easily know bin_end without opening.
            # But since they are sorted...
            
            # Optimization: 
            # If bin_dist > max_dist_file, we can skip?
            # Yes, if bins are strictly increasing distance shells.
            if dist > max_dist_file: 
                continue
                
            # If bin_dist + BIN_SIZE < min_dist_file, skip?
            # We don't know BIN_SIZE accurately. Let's assume 1000ly to be safe?
            # The file list showed 1000, 10050, 10100 -> 50ly steps?
            # Bin 0, 100, 1000 -> variable wtf.
            # Let's effectively filter by Upper Bound only for now to be safe.
            # If dist > max_dist_file, we definitely don't need it (assuming bins don't overlap backwards).
            
            files.append((dist, f))
        except:
            continue
            
    files.sort(key=lambda x: x[0])
    
    lazy_frames = []
    
    # Pre-calculate bounds for SQL filter
    if radius is not None:
        cx, cy, cz = center
        # Bounding box for fast filtering
        min_x, max_x = cx - radius, cx + radius
        min_y, max_y = cy - radius, cy + radius
        min_z, max_z = cz - radius, cz + radius
    
    for _, fpath in files:
        try:
           lf = pl.scan_parquet(fpath)
           
           # Apply geometric filter immediately if radius is set
           if radius is not None:
               lf = lf.filter(
                   (pl.col("x") * pl.col("distance_ly") >= min_x) &
                   (pl.col("x") * pl.col("distance_ly") <= max_x) &
                   (pl.col("y") * pl.col("distance_ly") >= min_y) &
                   (pl.col("y") * pl.col("distance_ly") <= max_y) &
                   (pl.col("z") * pl.col("distance_ly") >= min_z) &
                   (pl.col("z") * pl.col("distance_ly") <= max_z)
               )
           
           lazy_frames.append(lf)
           
           # Check if we have enough files?
           # If we are selecting a small region, we might need to scan MANY files if the region is dense?
           # Or specific files?
           # If radius is small, likely only few files match.
        except Exception as e:
            continue

    if not lazy_frames:
        # If no files found in range (e.g. very far out), return empty
        # Or maybe create empty DF
        return None # Will handle empty later

    try:
        combined_lf = pl.concat(lazy_frames)
        
        combined_lf = combined_lf.select([
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
        
        # If radius is precise sphere, filter here
        if radius is not None:
            cx, cy, cz = center
            # (x-cx)^2 + ... <= r^2
            # Optimization: We already did BBox. Sphere is optional but cleaner.
            # Let's stick to BBox for speed, or add sphere if needed.
            # Let's add precise sphere check.
            combined_lf = combined_lf.filter(
                (pl.col("x0") - cx)**2 + (pl.col("y0") - cy)**2 + (pl.col("z0") - cz)**2 <= radius**2
            )
        
        # Sort and limit
        q = combined_lf.sort("phot_g_mean_mag").limit(max_stars)
        return q.collect()
        
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return None
