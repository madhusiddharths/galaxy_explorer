# Galaxy Explorer: Visible Stars Finder

This Streamlit application helps users identify stars currently visible to the naked eye from their location, accounting for proper motion adjustments over time.

## Features

- **Real-time Visibility**: Calculates which stars are above the horizon based on your location and time.
- **Proper Motion Correction**: Adjusts star positions using Hipparcos data to account for movement since J2016.0.
- **Interactive Sky Map**: accurate visualization of the sky using Plotly.
- **Filtering**: Filter stars by brightness (magnitude).
- **Timezone Support**: Select from various global timezones to check visibility.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_folder>
    ```

2.  **Create a virtual environment (optional but recommended):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  **Run the Streamlit app:**
    ```bash
    streamlit run app.py
    ```

2.  **Interact with the Sidebar:**
    -   Enter your **Latitude** and **Longitude**.
    -   Select your **Timezone**.
    -   Adjust the **Brightness cutoff** slider (lower values = brighter stars).

3.  **View Results:**
    -   Click **Find Visible Stars**.
    -   The app will display the number of visible stars, a data table, and an interactive sky map.

## Data Source

The application uses `visible_stars_with_hipparcos_and_names.parquet`, which contains star data including:
-   RA/Dec coordinates
-   Proper motion (pmra, pmdec)
-   Magnitude
-   Star names

## License

[Add License Info Here]