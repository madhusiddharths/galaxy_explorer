"""Solar-system bodies and twilight, via astropy.

Provides apparent local sidereal time (for the fast star transform), Alt/Az of
the Sun, Moon and naked-eye planets at a given instant/place, the Moon's
illuminated fraction, and the twilight state that drives the sky colour.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import astropy.units as u
from astropy.coordinates import AltAz, EarthLocation, get_body, solar_system_ephemeris
from astropy.time import Time
from astropy.utils import iers

# Use the IERS-B table bundled with astropy: no network at runtime (deploy-safe)
# and plenty accurate for a planetarium. Avoids a slow first-call IERS download.
iers.conf.auto_download = False
iers.conf.auto_max_age = None
solar_system_ephemeris.set("builtin")


def warm_up() -> None:
    """Trigger astropy's one-time ERFA/ephemeris init (call behind a spinner)."""
    from datetime import datetime, timezone
    bodies_altaz(datetime.now(timezone.utc), 0.0, 0.0)

# Body -> (display colour rgb, relative marker size, label)
_BODIES = {
    "sun":     ((255, 236, 170), 26, "Sun"),
    "moon":    ((225, 225, 215), 22, "Moon"),
    "mercury": ((190, 180, 165), 7,  "Mercury"),
    "venus":   ((255, 248, 224), 12, "Venus"),
    "mars":    ((255, 110, 70),  9,  "Mars"),
    "jupiter": ((255, 220, 165), 12, "Jupiter"),
    "saturn":  ((240, 222, 170), 10, "Saturn"),
}


@dataclass
class Body:
    name: str
    alt: float
    az: float
    rgb: tuple[int, int, int]
    size: float
    up: bool
    info: str = ""


def _astropy_time(when_utc: datetime) -> Time:
    return Time(when_utc.replace(tzinfo=None), format="datetime", scale="utc")


def local_sidereal_time(when_utc: datetime, lon_deg: float) -> float:
    """Apparent local sidereal time in hours (0-24)."""
    t = _astropy_time(when_utc)
    lst = t.sidereal_time("apparent", longitude=lon_deg * u.deg)
    return float(lst.hour)


def bodies_altaz(when_utc: datetime, lat: float, lon: float) -> list[Body]:
    t = _astropy_time(when_utc)
    loc = EarthLocation(lat=lat * u.deg, lon=lon * u.deg, height=0 * u.m)
    frame = AltAz(obstime=t, location=loc)

    sun = get_body("sun", t, loc)
    moon = get_body("moon", t, loc)
    # Moon illuminated fraction from Sun-Moon elongation as seen from Earth
    elong = sun.separation(moon).radian
    illum = (1.0 - np.cos(elong)) / 2.0

    out: list[Body] = []
    for key, (rgb, size, label) in _BODIES.items():
        coord = sun if key == "sun" else moon if key == "moon" else get_body(key, t, loc)
        aa = coord.transform_to(frame)
        alt = float(aa.alt.deg)
        az = float(aa.az.deg)
        info = label
        if key == "moon":
            info = f"{label} — {illum * 100:.0f}% illuminated"
        out.append(Body(label, alt, az, rgb, size, up=alt > -2.0, info=info))
    return out


def sun_altitude(when_utc: datetime, lat: float, lon: float) -> float:
    t = _astropy_time(when_utc)
    loc = EarthLocation(lat=lat * u.deg, lon=lon * u.deg, height=0 * u.m)
    sun = get_body("sun", t, loc).transform_to(AltAz(obstime=t, location=loc))
    return float(sun.alt.deg)


@dataclass
class Twilight:
    label: str
    sky_color: str       # background hex
    horizon_glow: str    # glow colour near horizon
    star_alpha: float    # 0..1 multiplier (stars wash out by day)


def twilight_state(sun_alt: float) -> Twilight:
    if sun_alt > 0:
        return Twilight("Daytime", "#7fb2e6", "#bcd9f2", 0.04)
    if sun_alt > -6:
        return Twilight("Civil twilight", "#33557f", "#e8915a", 0.25)
    if sun_alt > -12:
        return Twilight("Nautical twilight", "#16243f", "#7a5a8a", 0.6)
    if sun_alt > -18:
        return Twilight("Astronomical twilight", "#0a1020", "#243a55", 0.85)
    return Twilight("Night", "#03050b", "#0c1a2c", 1.0)
