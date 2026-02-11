import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
import plotly.graph_objs as go
from pathlib import Path
import polars as pl
from astropy.time import Time
from astropy.coordinates import EarthLocation, SkyCoord, AltAz
import astropy.units as u
import numpy as np
from matplotlib import cm
import os

# Load once
PARQUET_PATH = Path("stars/visible_stars_with_hipparcos_and_names.parquet")

st.title("🌌 Visible Stars Finder with Proper Motion")

st.markdown("This tool shows stars currently visible to the naked eye from your location.")

# --- Default Location: Chicago ---
default_lat = 41.83310031992272
default_lon = -87.62416090340078

# Timezone dropdown options
timezone_options = [
    "Now (System Time)",
    "UTC",
    "America/Chicago",
    "America/New_York",
    "America/Los_Angeles",
    "Europe/London",
    "Asia/Kolkata",
    "Asia/Tokyo",
]

# --- User Inputs ---
if "lat" not in st.session_state:
    st.session_state.lat = default_lat
if "lon" not in st.session_state:
    st.session_state.lon = default_lon

lat = st.number_input("Latitude", value=st.session_state.lat, format="%.6f", key="lat_input")
lon = st.number_input("Longitude", value=st.session_state.lon, format="%.6f", key="lon_input")
timezone = st.selectbox("Select Timezone", timezone_options, index=timezone_options.index("America/Chicago"))
# Brightness filter
gmag_cutoff = st.slider("Select brightness cutoff (G magnitude)", min_value=0.0, max_value=6.5, value=5.0, step=0.1)

# Time selection
if timezone == "Now (System Time)":
    local_time = datetime.now().astimezone()
else:
    if "date_input" not in st.session_state:
        st.session_state.date_input = datetime.now().date()

    date = st.date_input("Pick a date", value=st.session_state.date_input, key="date_input")
    # Initialize only once
    if "time_input" not in st.session_state:
        st.session_state.time_input = datetime.now().time()

    # Widget with persistent key
    time_input = st.time_input("Pick a time", value=st.session_state.time_input, key="time_input")
    local_time = datetime.combine(date, time_input).replace(tzinfo=ZoneInfo(timezone))

utc_time = local_time.astimezone(ZoneInfo("UTC"))
jyear = Time(utc_time).jyear
delta_years = jyear - 2016.0

st.markdown("---")
st.subheader("📍 Observation Info")
st.write(f"**Latitude:** {lat}")
st.write(f"**Longitude:** {lon}")
st.write(f"**Local Time:** {local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
st.write(f"**UTC Time:** {utc_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
st.write(f"**Years since J2016.0:** {delta_years:.3f}")

# --- Plotting function ---
def plot_sky_map(df):
    fixed_size = 5
    mag_max = df['phot_g_mean_mag'].max()
    mag_min = df['phot_g_mean_mag'].min()
    if mag_max == mag_min:
        norm_mag = np.ones(len(df))
    else:
        norm_mag = (mag_max - df['phot_g_mean_mag']) / (mag_max - mag_min)

    colors = [f'rgba({int(255 * c)}, {int(255 * c)}, {int(255 * c)}, 1)' for c in norm_mag]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['azimuth_deg'],
        y=df['altitude_deg'],
        mode='markers',
        marker=dict(size=fixed_size, color=colors, symbol='star', line=dict(width=0), opacity=1),
        text=[f"Mag: {mag:.2f}, Source ID: {sid}" for mag, sid in zip(df['phot_g_mean_mag'], df['source_id'])],
        hoverinfo='text+x+y'
    ))

    fig.update_layout(
        title='Sky Map: Altitude vs Azimuth',
        xaxis=dict(
            title='Azimuth (degrees)',
            range=[0, 360],   # fixed from 0 to 360
            dtick=30,
            showgrid=False
        ),
        yaxis=dict(
            title='Altitude (degrees)',
            range=[0, 90],    # fixed from 0 to 90
            showgrid=False
        ),
        height=600,
        width=800,
        template='plotly_dark'
    )

    fig.update_xaxes(ticks='outside')
    fig.update_yaxes(ticks='outside')

    return fig


# --- AltAz calculation with proper motion ---
def calculate_altaz(df, lat, lon, time_utc):
    ra = df["ra"].to_numpy()
    dec = df["dec"].to_numpy()
    pmra = df["pmra"].to_numpy()  # mas/yr
    pmdec = df["pmdec"].to_numpy()  # mas/yr

    ra_corr = ra + (pmra / 3_600_000.0) * delta_years  # mas → deg
    dec_corr = dec + (pmdec / 3_600_000.0) * delta_years

    coords = SkyCoord(ra=ra_corr * u.deg, dec=dec_corr * u.deg)

    obs_time = Time(time_utc.replace(tzinfo=None), format="datetime")

    location = EarthLocation(lat=lat * u.deg, lon=lon * u.deg, height=0 * u.m)
    altaz = AltAz(obstime=obs_time, location=location)

    star_altaz = coords.transform_to(altaz)

    return star_altaz.alt.deg, star_altaz.az.deg

# Initialize session state variable for visible stars DataFrame
if "visible_stars" not in st.session_state:
    st.session_state.visible_stars = None

# --- Find Stars Button ---
# --- Find Stars Button ---
if st.button("🔭 Find Visible Stars"):
    try:
        df = pl.read_parquet(PARQUET_PATH)
        df = df.filter(pl.col("phot_g_mean_mag") <= gmag_cutoff)

        altitudes, azimuths = calculate_altaz(df, lat, lon, utc_time)
        df = df.with_columns([
            pl.Series("altitude_deg", altitudes),
            pl.Series("azimuth_deg", azimuths)
        ])
        visible = df.filter(pl.col("altitude_deg") > 0)

        # Convert to pandas for display
        visible_df = visible.to_pandas()
        if "star_name" not in visible_df.columns:
            visible_df["star_name"] = "None"
        else:
            visible_df["star_name"] = visible_df["star_name"].fillna("None")

        if "famous_star_name" not in visible_df.columns:
            visible_df["famous_star_name"] = "None"
        else:
            visible_df["famous_star_name"] = visible_df["famous_star_name"].fillna("None")

        st.session_state.visible_stars = visible_df  # store full pd.DataFrame

        st.success(f"✅ {len(visible_df)} stars are visible above the horizon.")

        st.dataframe(
            visible_df[
                [
                    "phot_g_mean_mag",
                    "source_id",
                    "star_name",
                    "famous_star_name",
                    "altitude_deg",
                    "azimuth_deg",
                    "ra",
                    "dec",
                    "parallax",
                    "pmra",
                    "pmdec",
                ]
            ].head(10)
        )

        # Use the plot_sky_map function to generate figure with dynamic axis ranges
        fig = plot_sky_map(visible_df)
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Error: {e}")
