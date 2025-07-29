import streamlit as st
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# Constants
DATA_DIR = Path('/Volumes/One Touch/bigdata/data/gaia_ly')
DEFAULT_LAT = 41.8781
DEFAULT_LON = -87.6298
EARTH_TILT_DEG = 23.44
C_DEG2RAD = np.pi / 180.0


def geo_to_cartesian(lat, lon):
    lat_rad = lat * C_DEG2RAD
    lon_rad = lon * C_DEG2RAD
    x = np.cos(lat_rad) * np.cos(lon_rad)
    y = np.cos(lat_rad) * np.sin(lon_rad)
    z = np.sin(lat_rad)
    return np.array([x, y, z])


def star_can_see_event(star_pos, star_vel, event_vector, event_time, now, duration_days):
    max_years = 1000
    step_years = 0.5
    cos_threshold = np.cos(np.radians(0.6))  # ~0.6 degrees

    duration_seconds = duration_days * 86400

    for years in np.arange(0, max_years, step_years):
        star_future_pos = star_pos + star_vel * years
        distance_ly = np.linalg.norm(star_future_pos)

        try:
            # Time light needs to reach the star (guard against overflow)
            max_days = (datetime.max - event_time).days
            light_days = distance_ly * 365.25
            if light_days > max_days:
                continue  # Skip this star; light would arrive too far in the future

            light_travel_time = timedelta(days=light_days)

        except:
            return False, None, None

        arrival_time = event_time + light_travel_time

        if arrival_time < now:
            continue
        elif (arrival_time - now).total_seconds() > duration_seconds:
            continue

        vec_to_earth = -star_future_pos
        vec_to_earth /= np.linalg.norm(vec_to_earth)

        alignment = np.dot(vec_to_earth, event_vector)
        if alignment > cos_threshold:
            wait_time = arrival_time - now
            return True, wait_time, distance_ly

    return False, None, None

def main():
    st.title("Which Stars Can See Your Earth Event?")

    lat = st.number_input("Latitude", value=DEFAULT_LAT, format="%.6f")
    lon = st.number_input("Longitude", value=DEFAULT_LON, format="%.6f")
    event_vector = geo_to_cartesian(lat, lon)

    # Default event time string for Oct 8, 1871 at midnight UTC
    default_event_time_str = "1871-10-08 00:00:00"
    event_time_str = st.text_input("Event time (UTC, YYYY-MM-DD HH:MM:SS)", default_event_time_str)
    try:
        event_time = datetime.strptime(event_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        st.error("Invalid date format. Use YYYY-MM-DD HH:MM:SS")
        return

    # Duration input with default 2 days 5 hours
    col1, col2, col3 = st.columns(3)
    with col1:
        duration_days = st.number_input("Days", min_value=0, step=1, value=2)
    with col2:
        duration_hours = st.number_input("Hours", min_value=0, max_value=23, step=1, value=5)
    with col3:
        duration_minutes = st.number_input("Minutes", min_value=0, max_value=59, step=1, value=0)

    # Total duration in days as float
    total_duration_days = duration_days + duration_hours / 24 + duration_minutes / (24 * 60)

    # (Rest of your code...)

    submit = st.button("Find Visible Stars")

    if submit:
        with st.spinner("Searching stars that can see the event..."):
            now = datetime.utcnow()
            light_years_ago = (now - event_time).total_seconds() / (365.25 * 24 * 3600)
            min_required_bin = int(np.ceil(light_years_ago / 50.0) * 50)

            result = []
            found = False

            parquet_files = sorted(
                DATA_DIR.glob("bin_*.parquet"),
                key=lambda p: int(p.name.split("_")[1].split(".")[0])
            )

            for file in parquet_files:
                bin_distance = int(file.name.split("_")[1].split(".")[0])
                if bin_distance < min_required_bin:
                    continue

                df = pl.read_parquet(file).select([
                    "source_id", "ra", "dec", "distance_ly",
                    "x", "y", "z", "vx", "vy", "vz"
                ])

                for row in df.iter_rows(named=True):
                    try:
                        star_pos = np.array([row["x"], row["y"], row["z"]], dtype=float)
                        star_vel = np.array([row["vx"], row["vy"], row["vz"]], dtype=float)
                        if np.any(np.isnan(star_pos)) or np.any(np.isnan(star_vel)):
                            continue
                    except:
                        continue

                    visible, wait_time, distance = star_can_see_event(
                        star_pos, star_vel, event_vector, event_time, now, total_duration_days
                    )

                    if visible:
                        total_minutes = int(wait_time.total_seconds() // 60)
                        y, rem_days = divmod(total_minutes, 60 * 24 * 365)
                        d, rem_minutes = divmod(rem_days, 60 * 24)
                        h, m = divmod(rem_minutes, 60)

                        result.append((wait_time, {
                            "source_id": row["source_id"],
                            "distance_ly": distance,
                            "wait_detail": (y, d, h, m)
                        }))

                    if len(result) >= 10:
                        found = True
                        break

                if found:
                    break

            result.sort(key=lambda x: x[0])
            top_stars = result[:10]

            if not top_stars:
                st.warning("No stars found that can see this event. Try increasing duration or going further back.")
            else:
                st.success("Top 10 stars that can see the event:")
                for i, (wait_time, star) in enumerate(top_stars):
                    y, d, h, m = star["wait_detail"]
                    precise_time = f"{y}y {d}d {h}h {m}m"
                    st.markdown(
                        f"**#{i + 1}** — ID: `{star['source_id']}` | Distance: `{star['distance_ly']:.2f}` ly | Wait time: `{precise_time}`"
                    )


if __name__ == "__main__":
    main()
