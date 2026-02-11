from astroquery.simbad import Simbad
import pandas as pd
import time

custom_simbad = Simbad()
custom_simbad.add_votable_fields('main_id', 'ids')

def fetch_star_names(hip_ids):
    results = []
    batch_size = 100

    for i in range(0, len(hip_ids), batch_size):
        batch = hip_ids[i:i + batch_size]
        try:
            # Convert HIP IDs to int for safety
            batch_int = [int(x) for x in batch]

            # Query Simbad
            query_result = custom_simbad.query_objects(["HIP " + str(hip) for hip in batch_int])
            if query_result is None:
                continue

            print("Returned columns:", query_result.colnames)  # Debug print

            df_batch = query_result.to_pandas()

            # Decode bytes in 'main_id' column if necessary
            df_batch['main_id'] = df_batch['main_id'].apply(lambda x: x.decode('utf-8') if hasattr(x, 'decode') else x)

            # ✅ Add HIP back as a separate column for merging later
            df_batch['HIP'] = batch_int

            results.append(df_batch[['HIP', 'main_id']])
        except Exception as e:
            print(f"Error querying batch starting at index {i}: {e}")
            time.sleep(5)
            continue

        time.sleep(1)

    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame(columns=['HIP', 'main_id'])

# Load full dataframe with all stars (some may not have Hipparcos match)
combined_df = pd.read_parquet('../stars/visible_stars_with_hipparcos.parquet')

# ✅ Only select non-null HIPs for Simbad query — but DON'T drop from combined_df
hip_ids = combined_df['original_ext_source_id'].dropna().astype(int).unique().tolist()

# 🔍 Fetch names for those that do have HIP IDs
names_df = fetch_star_names(hip_ids)

# 🔁 Merge star names back into full dataset
combined_with_names = combined_df.merge(
    names_df,
    left_on='original_ext_source_id',
    right_on='HIP',
    how='left'
)

# ✅ Rename and clean
combined_with_names = combined_with_names.rename(columns={"main_id": "star_name"})
combined_with_names = combined_with_names.drop(columns=['HIP'])

# ✅ Now contains all 8,529 original stars
print(f"Final star count: {len(combined_with_names)}")

# 🔐 Save final output
combined_with_names.to_parquet("../stars/visible_stars_with_hipparcos_and_names.parquet", index=False)
