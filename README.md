# Galaxy Explorer

A Gaia DR3 big-data project in two parts:

1. **Night Sky Viewer** (`app.py`, Streamlit) — a scientifically accurate, interactive
   **planetarium**: the real sky from your location at any moment, as a dome projection
   with true star colours, constellations, the Sun/Moon/planets, the Milky Way and
   day/night twilight. Drag the time slider to watch the sky turn.
2. **Galaxy Explorer** (`api/` + `web/`, Cloud Run) — a 3D/analytical explorer of the
   solar neighbourhood backed by ~77 GB of Gaia data in BigQuery.

## Night Sky Viewer — quickstart

```bash
pip install -r requirements.txt
python scripts/fetch_planetarium_assets.py   # one-time: names, constellation lines, Milky Way
streamlit run app.py
```

The star field is your processed Gaia visible-star catalogue
(`stars/visible_stars_with_hipparcos_and_names.parquet`, naked-eye mag ≤ 6.5). Overlays
come from the HYG database (names/Bayer/constellation) and d3-celestial (constellation
lines, Milky Way outline). Sky maths (Alt/Az, sidereal time, Sun/Moon/planets, twilight,
refraction, blackbody star colour) live in the `skyview/` package.

## Galaxy Explorer — two-page big-data app

Backed by an **external BigQuery table over ~77 GB / 557 M Gaia stars** in GCS
(`aicouncelling.gaia_ly.stars`). Two pages (`web/src/pages/`):

- **Structure** — stellar **density map** (3D, aggregated per HEALPix sector × distance
  shell) + a live **HR / colour–magnitude diagram**. Pure big-data aggregation.
- **Families** — open clusters & co-moving groups found by **HDBSCAN in 6D** (position +
  velocity), coloured in 3D with a clickable catalogue (Hyades, Pleiades, …).

### One-time BigQuery precompute (run in your GCP project — costs query $)

```bash
python bq_jobs/build.py --job all --dry-run     # FREE cost estimate
python bq_jobs/build.py --job materialize --run # native CLUSTERED table (~77 GB scan, ~$0.38)
python bq_jobs/build.py --job density --run     # density_voxels (cheap, off native)
python bq_jobs/build.py --job hr --run          # hr_bins (cheap)
python bq_jobs/clustering.py --run --max-dist 600   # cluster_catalog + cluster_stars
```

The native clustered table replaces the old `ORDER BY RAND()` full scan, so interactive
queries prune by `healpix_2` + `distance_bin` instead of reading all 77 GB.

### Run the explorer

```bash
# API (mock data works with no GCP — great for frontend dev):
MOCK_DATA=1 uvicorn api.main:app --port 8000     # or omit MOCK_DATA to hit BigQuery
cd web && npm install && npm run dev              # Vite dev server, proxies /stars,/density,/hr,/clusters
```

Production is one container (`Dockerfile`): the React build is served by FastAPI on Cloud
Run, same origin as the API. Endpoints: `/stars`, `/density`, `/hr`, `/clusters`. If a
precomputed table is missing, the API serves synthetic mock data so the UI still renders.

## Project Structure

```
mac_gaia/
├── pyspark_code/          # Main application and PySpark processing scripts
│   ├── app.py            # Streamlit web application
│   ├── clustering.py     # Star clustering algorithms
│   ├── combining.py      # Data combination utilities
│   ├── filtering.py      # Data filtering operations
│   ├── merging.py        # Data merging operations
│   ├── visibility.py     # Visibility calculation logic
│   ├── partitioning.py   # Data partitioning utilities
│   ├── fetch_star_names.py # Star name retrieval
│   └── requirements.txt  # Python dependencies
├── stars/                # Additional star data (if any)
└── data files/           # Various data files (*.parquet, *.csv, *.dat)
```

## Features

- **Real-time Visibility**: Calculates which stars are above the horizon based on your location and time
- **Proper Motion Correction**: Adjusts star positions using Hipparcos data to account for movement since J2016.0
- **Interactive Sky Map**: Accurate visualization of the sky using Plotly
- **Filtering**: Filter stars by brightness (magnitude)
- **Timezone Support**: Select from various global timezones to check visibility
- **Big Data Processing**: Uses PySpark for efficient processing of large astronomical datasets

## Data Sources

The project integrates data from:
- **Gaia DR3**: ESA's Gaia Data Release 3 for star positions and magnitudes
- **Hipparcos**: For proper motion data and star names
- **Tycho-2**: Additional cross-matching data

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/madhusiddharths/galaxy_explorer.git
   cd galaxy_explorer
   ```

2. **Create a virtual environment (optional but recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   cd pyspark_code
   pip install -r requirements.txt
   ```

## Usage

### Running the Streamlit App

1. **Start the application:**
   ```bash
   cd pyspark_code
   streamlit run app.py
   ```

2. **Interact with the interface:**
   - Enter your **Latitude** and **Longitude**
   - Select your **Timezone**
   - Adjust the **Brightness cutoff** slider (lower values = brighter stars)
   - Click **Find Visible Stars**

3. **View Results:**
   - Number of visible stars
   - Data table with star details
   - Interactive sky map

### Data Processing Scripts

The `pyspark_code` directory contains various scripts for processing astronomical data:
- Run individual scripts with PySpark for data preparation
- Process raw Gaia and Hipparcos data
- Create merged and filtered datasets

## Requirements

- Python 3.8+
- PySpark
- Streamlit
- Pandas
- Additional dependencies in `pyspark_code/requirements.txt`

## License

[Add License Info Here]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- ESA Gaia mission for providing comprehensive star catalog data
- Hipparcos mission for proper motion data
- Tycho-2 catalog for additional stellar information

