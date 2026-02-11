# Galaxy Explorer: Visible Stars Finder

A PySpark-based data processing pipeline and Streamlit application for identifying stars visible to the naked eye, with proper motion correction and interactive visualization.

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

