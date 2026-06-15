"""
One-time downloader for Night Sky Viewer (planetarium) overlay assets.

Pulls small, public-domain reference data and slims it into stars/ so the
Streamlit app can load everything locally (fast + deployable):

  - stars/hyg_bright.csv          Bright-star names/Bayer/constellation/colour
                                   (from the HYG v4.1 database, mag <= 6.5)
  - stars/constellation_lines.json Constellation stick-figures (d3-celestial)
  - stars/constellation_names.json Constellation label points + full names
  - stars/milkyway.json            Milky Way outline contours (d3-celestial)

The star *field* itself still comes from your processed Gaia parquet; these
files only add the naming / line / Milky-Way overlays.

Run once:  python scripts/fetch_planetarium_assets.py
"""
from __future__ import annotations

import io
import json
import sys
import urllib.request
from pathlib import Path

import polars as pl

STARS_DIR = Path(__file__).resolve().parent.parent / "stars"

HYG_URL = "https://raw.githubusercontent.com/astronexus/HYG-Database/main/hyg/CURRENT/hygdata_v41.csv"
LINES_URL = "https://raw.githubusercontent.com/ofrohn/d3-celestial/master/data/constellations.lines.json"
NAMES_URL = "https://raw.githubusercontent.com/ofrohn/d3-celestial/master/data/constellations.json"
MW_URL = "https://raw.githubusercontent.com/ofrohn/d3-celestial/master/data/mw.json"

MAG_LIMIT = 6.5  # naked-eye limit, matches the Gaia visible-star catalogue


def _download(url: str) -> bytes:
    print(f"  GET {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "mac-gaia-planetarium/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def fetch_hyg() -> None:
    out = STARS_DIR / "hyg_bright.csv"
    raw = _download(HYG_URL)
    df = pl.read_csv(io.BytesIO(raw), infer_schema_length=2000, ignore_errors=True)

    # HYG RA is in HOURS; convert to degrees so everything downstream is in degrees.
    df = df.with_columns((pl.col("ra").cast(pl.Float64) * 15.0).alias("ra_deg"))

    keep = ["hip", "hd", "proper", "bf", "bayer", "flam", "con",
            "ra_deg", "dec", "mag", "ci", "spect", "dist"]
    keep = [c for c in keep if c in df.columns]

    slim = (
        df.select(keep)
        .filter(pl.col("mag").cast(pl.Float64, strict=False) <= MAG_LIMIT)
        .filter(pl.col("hip").is_not_null())  # drop the Sun (id 0, no HIP)
    )
    slim.write_csv(out)
    print(f"  -> {out}  ({slim.height} stars, {out.stat().st_size // 1024} KB)")


def fetch_json(url: str, name: str) -> None:
    out = STARS_DIR / name
    data = json.loads(_download(url))
    with open(out, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"  -> {out}  ({out.stat().st_size // 1024} KB)")


def main() -> int:
    STARS_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching planetarium overlay assets into stars/ ...")
    try:
        fetch_hyg()
        fetch_json(LINES_URL, "constellation_lines.json")
        fetch_json(NAMES_URL, "constellation_names.json")
        fetch_json(MW_URL, "milkyway.json")
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
