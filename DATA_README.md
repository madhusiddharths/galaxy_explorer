# Data Files

This directory contains various astronomical data files used by the Galaxy Explorer project. These files are **not tracked in git** due to their large size.

## Required Data Files

The following data files should be present in the root directory:

### Processed Data Files (Generated)
- `visible_stars_with_hipparcos.parquet` - Stars visible to naked eye with Hipparcos cross-match
- `gaia_visible_combined.parquet` - Combined Gaia DR3 visible stars data
- `clustered_visibility_partition_hdbscan.parquet` - Clustered star data using HDBSCAN
- `final_stars.csv` - Final processed star dataset

### Source Data Files
- `hipparcos_merged.csv` - Merged Hipparcos catalog data
- `Hipparcos2BestNeighbour.csv` - Gaia-Hipparcos cross-match table
- `Tycho2tdscMergeBestNeighbour.csv` - Tycho-2 cross-match table
- `hip2.dat` - Hipparcos 2 binary catalog file
- `ReadMe.txt` - Hipparcos catalog documentation

### Documentation
- `gaia_dr3_info.pdf` - Gaia Data Release 3 documentation

## Data Sources

### Gaia DR3
Download from: https://gea.esac.esa.int/archive/
- Use ADQL queries to filter for visible stars (magnitude < 6.5)
- Include position, proper motion, and photometry columns

### Hipparcos 2
Download from: https://cdsarc.cds.unistra.fr/ftp/cats/I/311/
- `hip2.dat` - Main catalog
- `ReadMe.txt` - Format documentation

### Cross-Match Tables
Available from Gaia Archive:
- `gaiadr3.hipparcos2_best_neighbour`
- `gaiadr3.tycho2tdsc_merge_best_neighbour`

## Processing Pipeline

1. Download raw Gaia DR3 data (filtered for magnitude)
2. Download Hipparcos 2 catalog
3. Download cross-match tables
4. Run `pyspark_code/merging.py` to merge datasets
5. Run `pyspark_code/filtering.py` to filter visible stars
6. Run `pyspark_code/clustering.py` for star clustering
7. Run `pyspark_code/fetch_star_names.py` to add common names

## File Sizes (Approximate)

Most data files are large (100MB - 2GB) and should not be committed to git. The `.gitignore` file is configured to exclude all data files with extensions:
- `.parquet`
- `.csv` 
- `.dat`
- `.pdf`

## Note

If you're setting up this project for the first time, you'll need to download or generate these data files following the processing pipeline above.

