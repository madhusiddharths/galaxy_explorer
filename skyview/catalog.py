"""Load the Gaia visible-star catalogue and the overlay reference data.

The star field comes from your processed Gaia parquet.  We enrich it with
common/Bayer names + constellation from the HYG database (matched on the
Hipparcos id carried in the cross-match), and load constellation stick-figure
lines, constellation label points and the Milky-Way outline (GeoJSON).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from .colors import star_rgb


@dataclass
class Catalog:
    ra: np.ndarray          # deg, epoch J2016.0
    dec: np.ndarray         # deg
    pmra: np.ndarray        # mas/yr (mu_alpha*)
    pmdec: np.ndarray       # mas/yr
    mag: np.ndarray         # Gaia G
    rgb: np.ndarray         # (N, 3) 0-255, time-independent
    ci: np.ndarray          # colour index used (B-V scale)
    name: np.ndarray        # display name or "" (object array)
    constellation: np.ndarray
    source_id: np.ndarray

    def __len__(self) -> int:
        return len(self.ra)


def _clean(col: pl.Expr) -> pl.Expr:
    return col.cast(pl.Float64, strict=False)


def load_catalog(parquet_path: str | Path, hyg_path: str | Path) -> Catalog:
    df = pl.read_parquet(parquet_path)

    # --- names from HYG, matched on Hipparcos id -------------------------- #
    name = np.full(df.height, "", dtype=object)
    con = np.full(df.height, "", dtype=object)
    hyg_path = Path(hyg_path)
    if hyg_path.exists() and "original_ext_source_id" in df.columns:
        hyg = pl.read_csv(hyg_path).with_columns(pl.col("hip").cast(pl.Int64, strict=False))
        joined = df.with_columns(
            pl.col("original_ext_source_id").cast(pl.Int64, strict=False).alias("hip")
        ).join(hyg.select(["hip", "proper", "bf", "con"]), on="hip", how="left")
        proper = joined["proper"].to_list()
        bf = joined["bf"].to_list()
        cons = joined["con"].to_list()
        star_name = df["star_name"].to_list() if "star_name" in df.columns else [None] * df.height
        for i in range(df.height):
            p = (proper[i] or "").strip()
            b = (bf[i] or "").strip()
            s = (star_name[i] or "").strip() if star_name[i] else ""
            name[i] = p or b or s
            con[i] = (cons[i] or "").strip()

    # --- colour index: B-V where measured, else BP-RP rescaled to B-V ----- #
    bp_rp = df["bp_rp"].to_numpy().astype(float) if "bp_rp" in df.columns else np.full(df.height, np.nan)
    bv = df["B_V"].to_numpy().astype(float) if "B_V" in df.columns else np.full(df.height, np.nan)

    both = np.isfinite(bv) & np.isfinite(bp_rp)
    if both.sum() > 50:
        a, b = np.polyfit(bp_rp[both], bv[both], 1)  # self-calibrate BP-RP -> B-V
    else:
        a, b = 0.85, -0.05
    ci = np.where(np.isfinite(bv), bv, a * bp_rp + b)
    ci = np.where(np.isfinite(ci), ci, 0.65)  # default Sun-like

    rgb = star_rgb(ci)

    return Catalog(
        ra=_to_np(df, "ra"),
        dec=_to_np(df, "dec"),
        pmra=_to_np(df, "pmra"),
        pmdec=_to_np(df, "pmdec"),
        mag=_to_np(df, "phot_g_mean_mag"),
        rgb=rgb,
        ci=ci,
        name=name,
        constellation=con,
        source_id=df["source_id"].to_numpy() if "source_id" in df.columns else np.arange(df.height),
    )


def _to_np(df: pl.DataFrame, col: str) -> np.ndarray:
    return df[col].cast(pl.Float64, strict=False).to_numpy().astype(float)


# --------------------------------------------------------------------------- #
# GeoJSON overlays
# --------------------------------------------------------------------------- #
def _norm_ra(ra: float) -> float:
    return ra % 360.0


def load_constellation_lines(path: str | Path) -> list[np.ndarray]:
    """Return a list of polylines; each is an (M, 2) array of [ra_deg, dec_deg]."""
    if not Path(path).exists():
        return []
    data = json.loads(Path(path).read_text())
    polylines: list[np.ndarray] = []
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") != "MultiLineString":
            continue
        for line in geom["coordinates"]:
            pts = np.array([[_norm_ra(p[0]), p[1]] for p in line], dtype=float)
            if len(pts) >= 2:
                polylines.append(pts)
    return polylines


def load_constellation_labels(path: str | Path) -> list[tuple[float, float, str]]:
    if not Path(path).exists():
        return []
    data = json.loads(Path(path).read_text())
    out: list[tuple[float, float, str]] = []
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") != "Point":
            continue
        ra, dec = geom["coordinates"][0], geom["coordinates"][1]
        nm = feat.get("properties", {}).get("name") or feat.get("id", "")
        out.append((_norm_ra(ra), dec, nm))
    return out


def load_milkyway(path: str | Path, levels: tuple[str, ...] = ("ol1", "ol2")) -> list[np.ndarray]:
    """Return polygon rings (each an (M,2) [ra,dec] array) for the chosen
    brightness levels of the Milky Way outline."""
    if not Path(path).exists():
        return []
    data = json.loads(Path(path).read_text())
    rings: list[np.ndarray] = []
    for feat in data.get("features", []):
        if feat.get("id") not in levels:
            continue
        geom = feat.get("geometry", {})
        polys = geom.get("coordinates", [])
        if geom.get("type") == "Polygon":
            polys = [polys]
        for poly in polys:                       # each poly: list of rings
            for ring in poly:
                pts = np.array([[_norm_ra(p[0]), p[1]] for p in ring], dtype=float)
                if len(pts) >= 3:
                    rings.append(pts)
    return rings
