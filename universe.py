
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
    # Increased range and step size to make proper motion more visible
    current_year = st.slider("Year", min_value=-500000, max_value=500000, value=2016, step=1000)

    # Camera / Focus Control
    st.sidebar.header("Focus Region")
    
    # Camera / Focus Control
    st.sidebar.header("Focus Region")
    
    # Range limit
    RANGE_LIMIT = 2000.0 # Hard limit as requested
    
    # Helper for synchronized inputs
    def make_axis_control(label_suffix, key_prefix, default_range):
        st.sidebar.subheader(f"{label_suffix} Range (ly)")
        c1, c2, c3 = st.sidebar.columns([0.3, 0.4, 0.3])
        
        min_key = f"{key_prefix}_min"
        max_key = f"{key_prefix}_max"
        slider_key = f"{key_prefix}_slider"
        
        # Initialize state if needed
        if slider_key not in st.session_state:
            st.session_state[slider_key] = default_range
        
        # Callbacks
        def update_from_slider():
            # State is already updated by slider
            # Just ensure keys are synced if we needed to, but slider is master here
            pass 
            
        def update_from_num():
            # Get values from number inputs (which are already in state)
            s = st.session_state[min_key]
            e = st.session_state[max_key]
            if s > e: s, e = e, s # Swap if inverted
            # Clamp to range limit
            s = max(-RANGE_LIMIT, min(RANGE_LIMIT, s))
            e = max(-RANGE_LIMIT, min(RANGE_LIMIT, e))
            st.session_state[slider_key] = (s, e)

        # Get current values from slider state (master)
        current_range = st.session_state[slider_key]
        
        # NOTE: We do NOT pass 'value' to slider if key is present and state is initialized
        # Streamlit warns/errors if both are provided when key exists in session state
        
        with c1:
            st.number_input(f"{label_suffix} Min", value=current_range[0], min_value=-RANGE_LIMIT, max_value=RANGE_LIMIT, step=100.0, key=min_key, on_change=update_from_num, label_visibility="collapsed")
        with c2:
            st.slider(f"{label_suffix} Slider", min_value=-RANGE_LIMIT, max_value=RANGE_LIMIT, key=slider_key, step=100.0, on_change=update_from_slider, label_visibility="collapsed")
        with c3:
            st.number_input(f"{label_suffix} Max", value=current_range[1], min_value=-RANGE_LIMIT, max_value=RANGE_LIMIT, step=100.0, key=max_key, on_change=update_from_num, label_visibility="collapsed")
            
        return st.session_state[slider_key]

    x_range = make_axis_control("X", "x", (-2000.0, 2000.0))
    y_range = make_axis_control("Y", "y", (-2000.0, 2000.0))
    z_range = make_axis_control("Z", "z", (-2000.0, 2000.0))
    
    # Hybrid Logic: 
    # If ranges are exactly default (-2000 to 2000), show "Original Sphere" (25k stars, brightest)
    # Else, show "Custom Box" (2.5k stars, random)
    
    is_default_view = (
        x_range == (-2000.0, 2000.0) and
        y_range == (-2000.0, 2000.0) and
        z_range == (-2000.0, 2000.0)
    )
    
    if is_default_view:
        load_msg = "Loading Earth view (2000ly radius, 25k brightest stars)..."
        df = data_loader.load_and_process_stars(
            max_stars=25000,
            radius=2000.0
        )
    else:
        load_msg = f"Loading Custom Box ({x_range}, {y_range}, {z_range}) - 2.5k random stars..."
        df = data_loader.load_and_process_stars(
            max_stars=2500,
            x_range=x_range,
            y_range=y_range,
            z_range=z_range,
            radius=None
        )

    with st.spinner(load_msg):
        # Result is already loaded above
        pass
    
    # Visualize
    # Pass focus info purely for debug/UI feedback if needed, 
    # but the visualizer just renders the DF it gets.
    visualizer.render_universe(df, current_year)

if __name__ == "__main__":
    main()
