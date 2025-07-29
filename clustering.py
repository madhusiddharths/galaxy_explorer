import polars as pl
import numpy as np
from sklearn.preprocessing import StandardScaler
import hdbscan
from pathlib import Path

# Path to your input file
input_file = Path('/Volumes/One Touch/bigdata/data/gaia_partitioned/healpix_2=(5,).parquet')

# Load the Parquet file with Polars
print("📥 Reading Parquet file...")
df = pl.read_parquet(input_file)

# Select relevant columns for visibility clustering
print("🔍 Selecting relevant features...")
features = df.select(["phot_g_mean_mag", "parallax", "ra", "dec"]).drop_nulls()

# Convert to NumPy for sklearn/hdbscan compatibility
print("📐 Converting to NumPy and standardizing...")
X = features.to_numpy()
X_scaled = StandardScaler().fit_transform(X)

# Run HDBSCAN
print("🧠 Running HDBSCAN clustering...")
clusterer = hdbscan.HDBSCAN(
    min_cluster_size=100,     # Try adjusting this value if needed
    min_samples=10,           # Optional: can improve noise robustness
    core_dist_n_jobs=1        # HDBSCAN is not GPU-accelerated on Mac
)
labels = clusterer.fit_predict(X_scaled)
probabilities = clusterer.probabilities_

# Add cluster labels and confidence back to the DataFrame
print("📝 Adding cluster labels back to dataframe...")
df_clustered = features.with_columns([
    pl.Series("cluster", labels),
    pl.Series("confidence", probabilities)
])

# Save to Parquet
output_file = Path("../clustered_visibility_partition_hdbscan.parquet")
print(f"💾 Saving clustered data to {output_file}")
df_clustered.write_parquet(output_file)

print("✅ Clustering complete.")
