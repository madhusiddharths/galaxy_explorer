
import streamlit as st
import sys
from pathlib import Path

# Add project root to path if needed (it is by default in cwd)
# But we just import from backend package
try:
    from backend import data_loader, visualizer
except ImportError as e:
    st.error(f"Failed to import backend modules: {e}")
    st.stop()

st.set_page_config(layout="wide", page_title="Universe Eye")

def main():
    # Custom CSS for Space Theme
    st.markdown("""
        <style>
        .stApp {
            background-color: #0E1117;
        }
        /* Sidebar styling */
        section[data-testid="stSidebar"] {
            background-color: #161B22;
        }
        /* Headings */
        h1, h2, h3 {
            color: #E6E6E6 !important;
        }
        /* Metrics */
        div[data-testid="metric-container"] {
            background-color: #262730;
            border: 1px solid #444;
            padding: 10px;
            border-radius: 5px;
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("Universe Eye")

    # Time Slider
    current_year = st.slider("Year", min_value=-100000, max_value=100000, value=2016, step=100)

    # Filter Controls
    st.sidebar.header("Filters")
    
    # Distance Range
    dist_range = st.sidebar.slider(
        "Distance Range (ly)",
        min_value=10.0,
        max_value=17000.0,
        value=(10.0, 400.0),
        step=10.0
    )
    
    # Healpix Sector
    region_options = ["All"] + list(range(1, 13))
    selected_region = st.sidebar.selectbox("Healpix Sector (1-12)", region_options)
    
    healpix_arg = None
    max_stars_arg = 25000 # Default for 'All'
    
    if selected_region != "All":
        healpix_arg = int(selected_region)
        max_stars_arg = 1000 # Reduced for single sector as requested
    
    # Load Data
    with st.spinner(f"Loading stars (Dist: {dist_range}, Sector: {selected_region})..."):
        df = data_loader.load_and_process_stars(
            min_dist=dist_range[0],
            max_dist=dist_range[1],
            healpix=healpix_arg,
            max_stars=max_stars_arg
        )
    
    # Visualize
    # Pass focus info purely for debug/UI feedback if needed, 
    # but the visualizer just renders the DF it gets.
    visualizer.render_universe(df, current_year)

if __name__ == "__main__":
    main()
