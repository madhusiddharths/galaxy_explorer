"""Horizontal coordinates and the planetarium dome projection.

We convert equatorial (RA/Dec) to horizontal (altitude/azimuth) with fast
vectorised numpy given the apparent local sidereal time, then project the sky
hemisphere onto a unit disc as seen looking straight up.
"""
from __future__ import annotations

import numpy as np

DEG = np.pi / 180.0


# --------------------------------------------------------------------------- #
# Equatorial -> Horizontal
# --------------------------------------------------------------------------- #
def radec_to_altaz(ra_deg, dec_deg, lat_deg: float, lst_hours: float,
                   refraction: bool = True):
    """RA/Dec (deg) -> (altitude_deg, azimuth_deg) for one observer/time.

    lst_hours : apparent local sidereal time in hours (0-24).
    Azimuth is measured from North (0) through East (90), as is conventional.
    A simple Bennett refraction term lifts objects near the horizon.
    """
    ra = np.asarray(ra_deg, dtype=float) * DEG
    dec = np.asarray(dec_deg, dtype=float) * DEG
    lat = lat_deg * DEG
    lst = (lst_hours * 15.0) * DEG  # hours -> deg -> rad

    ha = lst - ra  # hour angle

    sin_alt = np.sin(dec) * np.sin(lat) + np.cos(dec) * np.cos(lat) * np.cos(ha)
    sin_alt = np.clip(sin_alt, -1.0, 1.0)
    alt = np.arcsin(sin_alt)

    cos_az = (np.sin(dec) - np.sin(alt) * np.sin(lat)) / (np.cos(alt) * np.cos(lat) + 1e-12)
    cos_az = np.clip(cos_az, -1.0, 1.0)
    az = np.arccos(cos_az)
    az = np.where(np.sin(ha) > 0.0, 2.0 * np.pi - az, az)

    alt_deg = alt / DEG
    az_deg = az / DEG

    if refraction:
        alt_deg = apply_refraction(alt_deg)
    return alt_deg, az_deg


def apply_refraction(alt_deg):
    """Bennett (1982) atmospheric refraction, arcmin -> deg, applied above ~-2 deg."""
    alt = np.asarray(alt_deg, dtype=float)
    a = np.clip(alt, -2.0, 90.0)
    r_arcmin = 1.0 / np.tan((a + 7.31 / (a + 4.4)) * DEG)
    r_deg = np.where(alt > -2.0, r_arcmin / 60.0, 0.0)
    return alt + r_deg


def apply_proper_motion(ra_deg, dec_deg, pmra_masyr, pmdec_masyr, dt_years: float):
    """Linear proper-motion update. pmra is mu_alpha* (already x cos dec)."""
    if abs(dt_years) < 1e-6:
        return np.asarray(ra_deg, dtype=float), np.asarray(dec_deg, dtype=float)
    dec = np.asarray(dec_deg, dtype=float)
    cosd = np.cos(dec * DEG)
    cosd = np.where(np.abs(cosd) < 1e-6, 1e-6, cosd)
    ra_new = np.asarray(ra_deg, dtype=float) + (np.asarray(pmra_masyr, dtype=float) / 3.6e6) * dt_years / cosd
    dec_new = dec + (np.asarray(pmdec_masyr, dtype=float) / 3.6e6) * dt_years
    return ra_new, dec_new


# --------------------------------------------------------------------------- #
# Dome projection: hemisphere -> unit disc (looking up; N top, E left)
# --------------------------------------------------------------------------- #
def altaz_to_xy(alt_deg, az_deg, projection: str = "stereographic"):
    """Project (alt, az) to disc coords (x, y) in [-1, 1].

    Zenith -> centre, horizon -> unit circle.  Looking up: N at top, E at left,
    S at bottom, W at right.  Points below the horizon get NaN.
    """
    alt = np.asarray(alt_deg, dtype=float)
    az = np.asarray(az_deg, dtype=float) * DEG
    zd = (90.0 - alt)  # zenith distance, degrees

    if projection == "equidistant":
        r = zd / 90.0
    elif projection == "orthographic":
        r = np.sin(zd * DEG)
    else:  # stereographic (conformal: constellation shapes preserved)
        r = np.tan(np.clip(zd, 0.0, 179.0) * DEG / 2.0)

    # N(az=0)->(0,+1), E(az=90)->(-1,0), S->(0,-1), W->(+1,0)
    x = -r * np.sin(az)
    y = r * np.cos(az)

    below = alt < 0.0
    x = np.where(below, np.nan, x)
    y = np.where(below, np.nan, y)
    return x, y


def horizon_radius(projection: str = "stereographic") -> float:
    """Disc radius corresponding to altitude = 0 for the chosen projection."""
    return 1.0  # all projections above are normalised so horizon -> r = 1
