
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
    
    # Defaults to Earth (0,0,0)
    col1, col2, col3 = st.sidebar.columns(3)
    cx = col1.number_input("X (ly)", value=0.0, step=100.0)
    cy = col2.number_input("Y (ly)", value=0.0, step=100.0)
    cz = col3.number_input("Z (ly)", value=0.0, step=100.0)
    
    # Radius Control: Synchronized Slider and Number Input
    st.sidebar.subheader("Field of View")
    
    # Default radius
    if 'focus_radius' not in st.session_state:
        st.session_state.focus_radius = 2000.0

    def update_radius_slider():
        st.session_state.focus_radius = st.session_state.radius_slider

    def update_radius_number():
        st.session_state.focus_radius = st.session_state.radius_number

    # Input Widgets
    # We want max 5000 as requested
    MAX_RADIUS = 5000.0
    MIN_RADIUS = 10.0
    
    # Ensure session state is within bounds (in case it was set higher before)
    if st.session_state.focus_radius > MAX_RADIUS:
         st.session_state.focus_radius = MAX_RADIUS
         
    r_col1, r_col2 = st.sidebar.columns([0.4, 0.6])
    
    with r_col1:
        radius_num = st.number_input(
            "Radius (ly)", 
            min_value=MIN_RADIUS, 
            max_value=MAX_RADIUS, 
            value=st.session_state.focus_radius, 
            step=10.0,
            key="radius_number",
            on_change=update_radius_number,
            label_visibility="collapsed" # Hide label to save space
        )
        
    with r_col2:
        radius_slider = st.slider(
            "Radius Slider", 
            min_value=MIN_RADIUS, 
            max_value=MAX_RADIUS, 
            value=st.session_state.focus_radius, 
            step=10.0,
            key="radius_slider",
            on_change=update_radius_slider,
            label_visibility="collapsed"
        )
    
    # Use the session state value which is synced
    focus_radius = st.session_state.focus_radius

    # Load Data with Focus
    # Note: caching in data_loader handles (max_stars, center, radius) as key.
    with st.spinner(f"Loading stars around ({cx},{cy},{cz}) radius {focus_radius}ly..."):
        # Increased star count significantly for better "Zoom" experience
        df = data_loader.load_and_process_stars(
            max_stars=25000,
            center=(cx, cy, cz),
            radius=focus_radius
        )
    
    # Visualize
    # Pass focus info purely for debug/UI feedback if needed, 
    # but the visualizer just renders the DF it gets.
    visualizer.render_universe(df, current_year)

if __name__ == "__main__":
    main()
