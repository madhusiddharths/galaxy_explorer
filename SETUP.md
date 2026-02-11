# Setup Guide for Galaxy Explorer

This guide will help you set up the Galaxy Explorer project on any machine.

## Prerequisites

- Python 3.8 or higher
- Git

## Quick Setup

### 1. Clone the Repository

```bash
git clone https://github.com/madhusiddharths/galaxy_explorer.git
cd galaxy_explorer
```

### 2. Create and Activate Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
.venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Running the Applications

### Main Streamlit App (Visible Stars Finder)

```bash
# Make sure you're in the project root
streamlit run app.py
```

This will open the Galaxy Explorer app in your browser at `http://localhost:8501`

### Time Travel App (Which Stars Can See Earth Events)

```bash
# From project root
streamlit run time.py
```

## Project Structure

```
galaxy_explorer/
├── app.py                      # Main Streamlit app
├── time.py                     # Time travel event visibility app
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
├── DATA_README.md              # Data files documentation
├── SETUP.md                    # This file
├── .gitignore                  # Git ignore rules
├── .venv/                      # Virtual environment (not in git)
├── stars/                      # Data files directory
│   ├── visible_stars_with_hipparcos_and_names.parquet
│   ├── gaia_visible_combined.parquet
│   ├── clustered_visibility_partition_hdbscan.parquet
│   ├── final_stars.csv
│   └── ... (other data files)
└── pyspark_code/               # Data processing scripts
    ├── clustering.py
    ├── combining.py
    ├── filtering.py
    ├── merging.py
    ├── visibility.py
    └── ... (other processing scripts)
```

## Data Files

The `stars/` directory contains all data files needed to run the applications. These files are **not** included in the git repository due to their size.

### Required Data Files

For the main app (`app.py`):
- `stars/visible_stars_with_hipparcos_and_names.parquet` (required)

For the time travel app (`time.py`):
- Requires external data source on external drive (see DATA_README.md)

### Obtaining Data Files

1. **Option 1**: Copy from existing installation
   - If you have access to an existing installation, copy the `stars/` directory

2. **Option 2**: Generate from raw data
   - Download Gaia DR3 and Hipparcos data (see DATA_README.md)
   - Run the data processing pipeline in `pyspark_code/`
   - Follow the processing steps in DATA_README.md

## Data Processing Scripts

The `pyspark_code/` directory contains scripts for processing raw astronomical data:

### Processing Pipeline

1. **filtering.py** - Filter raw Gaia data by quality criteria
2. **new_filtering.py** - Filter for visible stars (magnitude < 6.5)
3. **partitioning.py** - Partition data by HEALPix
4. **combining.py** - Combine partitioned files
5. **single.py** - Create single combined parquet file
6. **fetch_star_names.py** - Add star names from SIMBAD
7. **clustering.py** - Cluster stars using HDBSCAN
8. **visibility.py** - Generate city-specific visibility data

### Running Data Processing Scripts

```bash
cd pyspark_code

# Example: Fetch star names
python fetch_star_names.py

# Example: Run clustering
python clustering.py
```

**Note**: Most processing scripts require external data sources on external drives. Update paths as needed for your setup.

## Configuration

### Updating Paths

If your data files are in a different location:

1. **For app.py**: Data files should be in `stars/` relative to project root
2. **For time.py**: Update `DATA_DIR` variable if your external drive path differs
3. **For processing scripts**: Update input/output paths as needed

## Troubleshooting

### Import Errors

If you get import errors, make sure:
1. Virtual environment is activated
2. All dependencies are installed: `pip install -r requirements.txt`

### Data File Not Found

If the app can't find data files:
1. Check that `stars/` directory exists in project root
2. Verify the required .parquet files are present
3. Check file permissions

### MongoDB Connection Error (visibility.py)

If running `visibility.py` and MongoDB is not installed:
1. Install MongoDB: `brew install mongodb-community` (macOS)
2. Start MongoDB: `brew services start mongodb-community`
3. Or comment out MongoDB sections if not needed

## Development

### Adding New Features

1. Create a new branch: `git checkout -b feature-name`
2. Make your changes
3. Test thoroughly
4. Commit: `git commit -am "Description of changes"`
5. Push: `git push origin feature-name`

### Code Style

- Follow PEP 8 for Python code
- Use meaningful variable names
- Add comments for complex logic
- Update documentation when adding features

## Getting Help

- Check README.md for project overview
- Check DATA_README.md for data information
- Review code comments in individual scripts
- Open an issue on GitHub for bugs or questions

## License

[Add License Info Here]

