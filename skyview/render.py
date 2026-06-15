"""Assemble the planetarium scene and render the dome with Plotly."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import plotly.graph_objects as go

from .catalog import Catalog
from .colors import rgba_strings
from .ephem import Body, bodies_altaz, local_sidereal_time, twilight_state, Twilight
from .geometry import altaz_to_xy, apply_proper_motion, radec_to_altaz


# --------------------------------------------------------------------------- #
# Scene: everything time/place dependent, reused by the figure and the UI panel
# --------------------------------------------------------------------------- #
@dataclass
class Scene:
    when_utc: datetime
    lat: float
    lon: float
    lst: float
    star_alt: np.ndarray
    star_az: np.ndarray
    visible: np.ndarray
    bodies: list[Body]
    sun_alt: float
    twilight: Twilight = field(repr=False)
    n_visible: int = 0


def compute_scene(cat: Catalog, when_utc: datetime, lat: float, lon: float,
                  dt_years: float, mag_limit: float) -> Scene:
    ra, dec = apply_proper_motion(cat.ra, cat.dec, cat.pmra, cat.pmdec, dt_years)
    lst = local_sidereal_time(when_utc, lon)
    star_alt, star_az = radec_to_altaz(ra, dec, lat, lst, refraction=True)

    bodies = bodies_altaz(when_utc, lat, lon)
    sun_alt = next((b.alt for b in bodies if b.name == "Sun"), -90.0)
    twi = twilight_state(sun_alt)

    visible = (star_alt > 0.0) & (cat.mag <= mag_limit)
    return Scene(when_utc, lat, lon, lst, star_alt, star_az, visible,
                 bodies, sun_alt, twi, int(visible.sum()))


# --------------------------------------------------------------------------- #
# Colour helpers
# --------------------------------------------------------------------------- #
def _hex_to_rgb(h: str) -> np.ndarray:
    h = h.lstrip("#")
    return np.array([int(h[i:i + 2], 16) for i in (0, 2, 4)], dtype=float)


def _lerp_hex(c0: str, c1: str, t: float) -> str:
    a, b = _hex_to_rgb(c0), _hex_to_rgb(c1)
    r, g, bl = (a + (b - a) * t).astype(int)
    return f"rgb({r},{g},{bl})"


def _project_radec(ra, dec, lst, lat, projection):
    alt, az = radec_to_altaz(ra, dec, lat, lst, refraction=False)
    return altaz_to_xy(alt, az, projection)


def _star_sizes(mag: np.ndarray, mag_limit: float) -> np.ndarray:
    mmin = 0.0
    norm = np.clip((mag_limit - mag) / (mag_limit - mmin), 0.0, 1.0)
    size = 2.2 + 7.0 * norm ** 1.5
    size = np.where(mag < 1.5, size + 3.5, size)  # let the few brightest pop
    return size


# --------------------------------------------------------------------------- #
# Figure
# --------------------------------------------------------------------------- #
def render_sky(cat: Catalog, scene: Scene, *, projection: str = "stereographic",
               lines=None, labels=None, milkyway=None,
               show_constellations=True, show_names=True, show_planets=True,
               show_milkyway=True, show_ground=True, show_grid=True,
               name_mag_limit: float = 2.2) -> go.Figure:
    fig = go.Figure()
    twi = scene.twilight

    # --- graded sky disc (brighter toward the horizon) -------------------- #
    steps = 9
    for i in range(steps):
        r_out = 1.0 - i / steps
        t = (i / steps) ** 1.4
        fig.add_shape(type="circle", xref="x", yref="y",
                      x0=-r_out, y0=-r_out, x1=r_out, y1=r_out,
                      fillcolor=_lerp_hex(twi.horizon_glow, twi.sky_color, t),
                      line=dict(width=0), layer="below")

    # --- Milky Way: soft faint band of points ----------------------------- #
    if show_milkyway and milkyway:
        mx, my = [], []
        for ring in milkyway:
            x, y = _project_radec(ring[:, 0], ring[:, 1], scene.lst, scene.lat, projection)
            mask = np.isfinite(x)
            mx.extend(x[mask].tolist()); my.extend(y[mask].tolist())
        if mx:
            fig.add_trace(go.Scatter(
                x=mx, y=my, mode="markers", hoverinfo="skip",
                marker=dict(size=10, color="rgba(195,210,255,0.035)", line=dict(width=0)),
                showlegend=False))

    # --- constellation stick-figures -------------------------------------- #
    if show_constellations and lines:
        lx, ly = [], []
        for poly in lines:
            x, y = _project_radec(poly[:, 0], poly[:, 1], scene.lst, scene.lat, projection)
            lx.extend(x.tolist() + [np.nan]); ly.extend(y.tolist() + [np.nan])
        fig.add_trace(go.Scatter(
            x=lx, y=ly, mode="lines", hoverinfo="skip",
            line=dict(color="rgba(130,165,225,0.33)", width=1), showlegend=False))

    if show_constellations and labels:
        tx, ty, tt = [], [], []
        for ra, dec, nm in labels:
            x, y = _project_radec(np.array([ra]), np.array([dec]), scene.lst, scene.lat, projection)
            if np.isfinite(x[0]) and abs(x[0]) < 1 and abs(y[0]) < 1:
                tx.append(x[0]); ty.append(y[0]); tt.append(nm)
        if tx:
            fig.add_trace(go.Scatter(
                x=tx, y=ty, mode="text", text=tt, hoverinfo="skip",
                textfont=dict(color="rgba(150,170,210,0.45)", size=9), showlegend=False))

    # --- stars ------------------------------------------------------------ #
    vis = scene.visible
    if vis.any():
        sx, sy = altaz_to_xy(scene.star_alt[vis], scene.star_az[vis], projection)
        mag = cat.mag[vis]
        sizes = _star_sizes(mag, mag_limit=6.5)
        norm = np.clip((6.6 - mag) / 6.6, 0.05, 1.0)
        alpha = np.clip(twi.star_alpha * (0.5 + 0.5 * norm), 0.06, 1.0)
        colors = rgba_strings(cat.rgb[vis], alpha)

        names = cat.name[vis]
        cons = cat.constellation[vis]
        v_alt = scene.star_alt[vis]
        v_az = scene.star_az[vis]
        hover = [
            (f"<b>{nm or 'Star'}</b>" + (f" · {cn}" if cn else "")
             + f"<br>mag {m:.2f}<br>alt {al:.1f}° · az {az:.1f}°")
            for nm, cn, m, al, az in zip(names, cons, mag, v_alt, v_az)
        ]

        # glow halo for the brightest stars
        bright = mag < 3.0
        if bright.any() and twi.star_alpha > 0.3:
            fig.add_trace(go.Scatter(
                x=sx[bright], y=sy[bright], mode="markers", hoverinfo="skip",
                marker=dict(size=sizes[bright] * 3.0,
                            color=rgba_strings(cat.rgb[vis][bright], 0.16 * twi.star_alpha),
                            line=dict(width=0)), showlegend=False))

        fig.add_trace(go.Scatter(
            x=sx, y=sy, mode="markers",
            marker=dict(size=sizes, color=colors, line=dict(width=0)),
            text=hover, hoverinfo="text", showlegend=False))

        # star name labels (only the genuinely bright/named)
        if show_names:
            lab = vis.copy()
            lab[vis] = (mag <= name_mag_limit) & (names != "")
            if lab.any():
                lxx, lyy = altaz_to_xy(scene.star_alt[lab], scene.star_az[lab], projection)
                fig.add_trace(go.Scatter(
                    x=lxx, y=lyy + 0.022, mode="text", text=cat.name[lab],
                    textfont=dict(color="rgba(235,235,245,0.8)", size=10),
                    hoverinfo="skip", showlegend=False))

    # --- Sun / Moon / planets --------------------------------------------- #
    if show_planets:
        for b in scene.bodies:
            if not b.up:
                continue
            bx, by = altaz_to_xy(np.array([b.alt]), np.array([b.az]), projection)
            if not np.isfinite(bx[0]):
                continue
            col = f"rgb({b.rgb[0]},{b.rgb[1]},{b.rgb[2]})"
            fig.add_trace(go.Scatter(
                x=[bx[0]], y=[by[0]], mode="markers+text",
                marker=dict(size=b.size, color=col,
                            line=dict(width=0.6, color="rgba(255,255,255,0.5)")),
                text=[f"  {b.name}"], textposition="middle right",
                textfont=dict(color="rgba(255,255,255,0.85)", size=10),
                hovertext=b.info, hoverinfo="text", showlegend=False))

    _add_dome_furniture(fig, projection, twi, show_grid, show_ground)
    _layout(fig, twi)
    return fig


def _add_dome_furniture(fig, projection, twi, show_grid, show_ground):
    # ground/space outside the dome
    if show_ground:
        fig.add_shape(type="rect", x0=-1.25, y0=-1.25, x1=1.25, y1=1.25,
                      fillcolor="#05060d", line=dict(width=0), layer="below")
        fig.add_shape(type="circle", x0=-1.0, y0=-1.0, x1=1.0, y1=1.0,
                      fillcolor=twi.sky_color, line=dict(width=0), layer="below")

    # altitude rings
    if show_grid:
        for alt in (30, 60):
            x, _ = altaz_to_xy(np.array([alt]), np.array([0.0]), projection)
            r = abs(_y_for(alt, projection))
            fig.add_shape(type="circle", x0=-r, y0=-r, x1=r, y1=r,
                          line=dict(color="rgba(255,255,255,0.10)", width=1, dash="dot"))
        # azimuth spokes
        for az in range(0, 360, 45):
            x1, y1 = altaz_to_xy(np.array([0.5]), np.array([az]), projection)
            fig.add_shape(type="line", x0=0, y0=0, x1=_x_for(0.0, az, projection),
                          y1=_y_az(0.0, az, projection),
                          line=dict(color="rgba(255,255,255,0.07)", width=1))

    # horizon circle
    fig.add_shape(type="circle", x0=-1, y0=-1, x1=1, y1=1,
                  line=dict(color="rgba(255,255,255,0.45)", width=2))

    # cardinal + intercardinal labels just outside the horizon
    cards = {"N": 0, "NE": 45, "E": 90, "SE": 135, "S": 180, "SW": 225, "W": 270, "NW": 315}
    cx, cy, ct = [], [], []
    for name, az in cards.items():
        x = -1.1 * np.sin(np.radians(az))
        y = 1.1 * np.cos(np.radians(az))
        cx.append(x); cy.append(y); ct.append(name)
    fig.add_trace(go.Scatter(x=cx, y=cy, mode="text", text=ct,
                             textfont=dict(color="rgba(255,255,255,0.75)",
                                           size=[15 if len(t) == 1 else 11 for t in ct]),
                             hoverinfo="skip", showlegend=False))


def _y_for(alt, projection):
    _, y = altaz_to_xy(np.array([alt]), np.array([0.0]), projection)
    return y[0]


def _x_for(alt, az, projection):
    x, _ = altaz_to_xy(np.array([alt]), np.array([az]), projection)
    return x[0]


def _y_az(alt, az, projection):
    _, y = altaz_to_xy(np.array([alt]), np.array([az]), projection)
    return y[0]


def _layout(fig, twi):
    fig.update_layout(
        xaxis=dict(visible=False, range=[-1.2, 1.2], fixedrange=True),
        yaxis=dict(visible=False, range=[-1.2, 1.2], fixedrange=True,
                   scaleanchor="x", scaleratio=1),
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="#05060d", plot_bgcolor="#05060d",
        height=720, showlegend=False, dragmode=False,
        hoverlabel=dict(bgcolor="rgba(10,12,20,0.9)",
                        font=dict(color="white", size=12)),
    )
