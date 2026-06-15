"""Physically-motivated star colours.

A star's hue is set by its surface temperature, which we estimate from a
colour index (Johnson B-V, or Gaia BP-RP as a stand-in) via the Ballesteros
relation, then convert the blackbody temperature to sRGB with the well-known
Tanner Helland approximation.  The result: hot stars blue-white, Sun-like
stars yellow-white, cool stars orange-red — as they actually appear.
"""
from __future__ import annotations

import numpy as np


def color_index_to_temp(ci: np.ndarray) -> np.ndarray:
    """Colour index (B-V-like) -> effective temperature in Kelvin (Ballesteros 2012)."""
    ci = np.clip(np.asarray(ci, dtype=float), -0.4, 2.5)
    return 4600.0 * (1.0 / (0.92 * ci + 1.7) + 1.0 / (0.92 * ci + 0.62))


def temp_to_rgb(temp: np.ndarray) -> np.ndarray:
    """Blackbody temperature (K) -> sRGB array of shape (..., 3), values 0-255."""
    t = np.clip(np.asarray(temp, dtype=float), 1000.0, 40000.0) / 100.0

    # Red
    r = np.where(t <= 66.0,
                 255.0,
                 329.698727446 * np.power(np.clip(t - 60.0, 1e-6, None), -0.1332047592))
    # Green
    g = np.where(t <= 66.0,
                 99.4708025861 * np.log(np.clip(t, 1e-6, None)) - 161.1195681661,
                 288.1221695283 * np.power(np.clip(t - 60.0, 1e-6, None), -0.0755148492))
    # Blue
    b = np.where(t >= 66.0,
                 255.0,
                 np.where(t <= 19.0,
                          0.0,
                          138.5177312231 * np.log(np.clip(t - 10.0, 1e-6, None)) - 305.0447927307))

    rgb = np.stack([r, g, b], axis=-1)
    return np.clip(rgb, 0.0, 255.0)


def star_rgb(ci: np.ndarray) -> np.ndarray:
    """Colour index -> sRGB (..., 3) 0-255 for a star."""
    return temp_to_rgb(color_index_to_temp(ci))


def rgba_strings(rgb: np.ndarray, alpha: np.ndarray | float = 1.0) -> list[str]:
    """Vectorised 'rgba(r,g,b,a)' strings for Plotly markers."""
    rgb = np.asarray(rgb)
    if np.isscalar(alpha):
        alpha = np.full(len(rgb), float(alpha))
    return [
        f"rgba({int(r)},{int(g)},{int(b)},{a:.3f})"
        for (r, g, b), a in zip(rgb, alpha)
    ]
