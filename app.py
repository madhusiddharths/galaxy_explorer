"""
Night Sky Viewer — a scientifically accurate, interactive planetarium.

Shows the real sky from your location at any moment: a dome (stereographic)
projection with true star colours, constellations, the Sun/Moon/planets, the
Milky Way and day/night twilight. Drag the time slider to watch the sky turn.

Star field: your processed Gaia DR3 visible-star catalogue (mag <= 6.5).
Overlays:   HYG database (names), d3-celestial (constellation lines, Milky Way).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import streamlit as st
from astropy.time import Time

from skyview import catalog, ephem, render

STARS = Path(__file__).resolve().parent / "stars"

st.set_page_config(layout="wide", page_title="Night Sky Viewer", page_icon="🌌")

CITY_PRESETS = {
    "Custom / GPS": None,
    "Chicago": (41.8331, -87.6242, "America/Chicago"),
    "New York": (40.7128, -74.0060, "America/New_York"),
    "Los Angeles": (34.0522, -118.2437, "America/Los_Angeles"),
    "London": (51.5074, -0.1278, "Europe/London"),
    "Reykjavík": (64.1466, -21.9426, "Atlantic/Reykjavik"),
    "Delhi": (28.6139, 77.2090, "Asia/Kolkata"),
    "Tokyo": (35.6895, 139.6917, "Asia/Tokyo"),
    "Sydney": (-33.8688, 151.2093, "Australia/Sydney"),
    "Atacama (Chile)": (-24.6272, -70.4042, "America/Santiago"),
}

COMMON_TZS = ["UTC", "America/Chicago", "America/New_York", "America/Los_Angeles",
              "Europe/London", "Europe/Berlin", "Asia/Kolkata", "Asia/Tokyo",
              "Australia/Sydney", "America/Santiago"]


# --------------------------------------------------------------------------- #
# Data / init (cached)
# --------------------------------------------------------------------------- #
@st.cache_resource
def _warm_astropy() -> bool:
    ephem.warm_up()
    return True


@st.cache_data(show_spinner="Loading star catalogue & sky overlays…")
def load_everything():
    cat = catalog.load_catalog(
        STARS / "visible_stars_with_hipparcos_and_names.parquet",
        STARS / "hyg_bright.csv",
    )
    lines = catalog.load_constellation_lines(STARS / "constellation_lines.json")
    labels = catalog.load_constellation_labels(STARS / "constellation_names.json")
    mw = catalog.load_milkyway(STARS / "milkyway.json")
    return cat, lines, labels, mw


try:
    from streamlit_js_eval import get_geolocation
except Exception:  # noqa: BLE001
    get_geolocation = None

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:  # noqa: BLE001
    st_autorefresh = None


# --------------------------------------------------------------------------- #
# Sidebar — controls
# --------------------------------------------------------------------------- #
def sidebar():
    with st.spinner("Warming up astronomy engine…"):
        _warm_astropy()

    st.sidebar.title("🌌 Night Sky Viewer")

    # ---- Location ----
    st.sidebar.header("📍 Location")
    if "lat" not in st.session_state:
        st.session_state.lat, st.session_state.lon = 41.8331, -87.6242
        st.session_state.tz = "America/Chicago"

    city = st.sidebar.selectbox("City preset", list(CITY_PRESETS), index=1)
    if CITY_PRESETS[city] is not None:
        clat, clon, ctz = CITY_PRESETS[city]
        if st.session_state.get("_city") != city:
            st.session_state.lat, st.session_state.lon, st.session_state.tz = clat, clon, ctz
            st.session_state._city = city

    if get_geolocation is not None:
        if st.sidebar.button("Use my location (GPS)", width="stretch"):
            loc = get_geolocation()
            if loc and loc.get("coords"):
                st.session_state.lat = round(loc["coords"]["latitude"], 5)
                st.session_state.lon = round(loc["coords"]["longitude"], 5)
                st.rerun()
    else:
        st.sidebar.caption("Install `streamlit-js-eval` for one-tap GPS.")

    lat = st.sidebar.number_input("Latitude", -90.0, 90.0,
                                  value=float(st.session_state.lat), format="%.5f")
    lon = st.sidebar.number_input("Longitude", -180.0, 180.0,
                                  value=float(st.session_state.lon), format="%.5f")
    st.session_state.lat, st.session_state.lon = lat, lon

    # ---- Time ----
    st.sidebar.header("🕐 Time")
    tz_opts = ["System (auto)"] + COMMON_TZS
    default_tz = st.session_state.get("tz", "America/Chicago")
    idx = tz_opts.index(default_tz) if default_tz in tz_opts else 0
    tz_choice = st.sidebar.selectbox("Time zone", tz_opts, index=idx)
    tz = (datetime.now().astimezone().tzinfo if tz_choice == "System (auto)"
          else ZoneInfo(tz_choice))

    now_local = datetime.now(tz)
    the_date = st.sidebar.date_input("Date", value=now_local.date())

    if "tod" not in st.session_state:
        st.session_state.tod = now_local.hour + now_local.minute / 60.0

    playing = st.sidebar.toggle("▶ Play (time-lapse)", value=False,
                                help="Auto-advance time to watch the sky rotate.")
    speed = st.sidebar.select_slider("Speed (sky-hours / sec)", [0.25, 0.5, 1, 2, 4, 8],
                                     value=2) if playing else 0

    if playing and st_autorefresh is not None:
        st_autorefresh(interval=200, key="play_tick")
        st.session_state.tod = (st.session_state.tod + speed * 0.2) % 24.0
    elif playing:
        st.sidebar.caption("Install `streamlit-autorefresh` to enable Play.")

    tod = st.sidebar.slider("Time of day (hours) — drag to scrub", 0.0, 24.0,
                            value=float(st.session_state.tod), step=0.05)
    if not playing:
        st.session_state.tod = tod

    c1, c2, c3 = st.sidebar.columns(3)
    if c1.button("−1h", width="stretch"):
        st.session_state.tod = (st.session_state.tod - 1) % 24; st.rerun()
    if c2.button("Now", width="stretch"):
        st.session_state.tod = now_local.hour + now_local.minute / 60.0; st.rerun()
    if c3.button("+1h", width="stretch"):
        st.session_state.tod = (st.session_state.tod + 1) % 24; st.rerun()

    hh = int(st.session_state.tod)
    mm = int(round((st.session_state.tod - hh) * 60)) % 60
    local_dt = datetime(the_date.year, the_date.month, the_date.day, hh, mm, tzinfo=tz)

    # ---- View ----
    st.sidebar.header("🔭 View")
    mag_limit = st.sidebar.slider("Faintest magnitude shown", 1.0, 6.5, 6.0, 0.1,
                                  help="Higher = fainter stars included.")
    projection = st.sidebar.selectbox("Projection",
                                      ["Stereographic (shapes true)", "Equidistant"], index=0)
    projection = "stereographic" if projection.startswith("Stereo") else "equidistant"

    st.sidebar.caption("Layers")
    opts = dict(
        show_constellations=st.sidebar.checkbox("Constellations", True),
        show_names=st.sidebar.checkbox("Star names", True),
        show_planets=st.sidebar.checkbox("Sun · Moon · planets", True),
        show_milkyway=st.sidebar.checkbox("Milky Way", True),
        show_grid=st.sidebar.checkbox("Alt/Az grid", True),
        show_ground=True,
    )
    return local_dt, lat, lon, mag_limit, projection, opts, playing


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    cat, lines, labels, mw = load_everything()
    local_dt, lat, lon, mag_limit, projection, opts, playing = sidebar()

    utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
    dt_years = Time(utc_dt.replace(tzinfo=None)).jyear - 2016.0

    scene = render.compute_scene(cat, utc_dt, lat, lon, dt_years, mag_limit)
    fig = render.render_sky(cat, scene, projection=projection,
                            lines=lines, labels=labels, milkyway=mw, **opts)

    left, right = st.columns([3, 1], gap="medium")
    with left:
        st.plotly_chart(fig, width="stretch",
                        config={"displayModeBar": False, "scrollZoom": False})
    with right:
        info_panel(scene, local_dt, cat)

    if playing:
        st.stop()  # keep the autorefresh loop tight


def info_panel(scene, local_dt, cat):
    twi = scene.twilight
    st.subheader("Sky right now")
    st.markdown(
        f"<div style='padding:8px 12px;border-radius:8px;background:{twi.sky_color};"
        f"color:#fff;border:1px solid #333;font-weight:600'>{twi.label} · "
        f"Sun {scene.sun_alt:+.0f}°</div>", unsafe_allow_html=True)

    st.metric("Stars visible", f"{scene.n_visible:,}")
    st.caption(f"{local_dt.strftime('%A %d %b %Y · %H:%M %Z')}")
    st.caption(f"Lat {scene.lat:.3f}, Lon {scene.lon:.3f} · LST {scene.lst:.2f}h")

    up = [b for b in scene.bodies if b.up]
    if up:
        st.markdown("**Planets & Moon up**")
        for b in up:
            st.markdown(f"- {b.info} · alt {b.alt:.0f}°")

    # What's up: brightest named visible stars
    import numpy as np
    vis = scene.visible
    if vis.any():
        idx = np.where(vis)[0]
        order = idx[np.argsort(cat.mag[idx])][:12]
        named = [(cat.name[i], cat.mag[i], scene.star_alt[i], scene.star_az[i])
                 for i in order if cat.name[i]]
        if named:
            st.markdown("**Brightest stars up**")
            for nm, m, al, az in named[:8]:
                st.markdown(f"- {nm} · mag {m:.1f} · alt {al:.0f}°")

    st.caption("Data: Gaia DR3 · HYG · d3-celestial")


if __name__ == "__main__":
    main()
