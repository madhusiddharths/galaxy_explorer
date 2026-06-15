"""BigQuery access for Galaxy Explorer.

Serves three views over the Gaia data:
  * stars    — sampled 3D point cloud (cheap, cluster-pruned, projected to a year)
  * density  — precomputed healpix x distance-shell voxels (table density_voxels)
  * hr       — precomputed BP-RP x absolute-mag histogram (table hr_bins)
  * clusters — open clusters / moving groups (tables cluster_catalog, cluster_stars)

If the BigQuery client or a precomputed table is unavailable, each function
falls back to synthetic mock data so the frontend still renders (and local dev
works without GCP).  Run bq_jobs/build.py + clustering.py to populate the tables.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

import numpy as np
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- BigQuery client (service-account JSON, ADC, or none -> mock) ----------- #
try:
    if os.getenv("GOOGLE_CREDENTIALS_JSON"):
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        json.loads(creds_json)
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
            tmp.write(creds_json)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
    client = bigquery.Client()
except Exception as exc:  # noqa: BLE001
    logger.error(f"BigQuery client unavailable, using mock data: {exc}")
    client = None

# --- Table configuration ---------------------------------------------------- #
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "aicouncelling")
DATASET = os.getenv("BIGQUERY_DATASET", "gaia_ly")
EXTERNAL_TABLE = os.getenv("BIGQUERY_TABLE", f"{PROJECT}.{DATASET}.stars")
# Prefer the native clustered table for interactive queries; override via env.
STARS_TABLE = os.getenv("STARS_TABLE", f"{PROJECT}.{DATASET}.stars_native")
DENSITY_TABLE = f"{PROJECT}.{DATASET}.density_voxels"
HR_TABLE = f"{PROJECT}.{DATASET}.hr_bins"
CLUSTER_STARS_TABLE = f"{PROJECT}.{DATASET}.cluster_stars"
CLUSTER_CATALOG_TABLE = f"{PROJECT}.{DATASET}.cluster_catalog"

MAS_TO_RAD_YR = (np.pi / 180.0) / 3_600_000.0
FORCE_MOCK = os.getenv("MOCK_DATA", "").lower() in ("1", "true", "yes")
_rng = np.random.default_rng(42)


def _healpix_range(healpix: Optional[int]):
    """Level-0 sector (1-12) -> inclusive [min, max] of level-2 healpix indices."""
    if healpix is None:
        return None
    p0 = healpix - 1
    return p0 * 16, (p0 + 1) * 16 - 1


def _run(sql: str, params: list) -> list:
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    return list(client.query(sql, job_config=cfg).result())


# --------------------------------------------------------------------------- #
# Stars — sampled 3D scatter, projected to `year`
# --------------------------------------------------------------------------- #
async def query_stars(min_dist: float, max_dist: float, healpix: Optional[int] = None,
                      limit: int = 25000, year: int = 2016) -> List[Dict[str, Any]]:
    if client is None or FORCE_MOCK:
        return _mock_stars(min_dist, max_dist, limit, year)
    dt = year - 2016.0
    where = "distance_ly BETWEEN @min_dist AND @max_dist AND phot_g_mean_mag IS NOT NULL"
    params = [bigquery.ScalarQueryParameter("min_dist", "FLOAT64", min_dist),
              bigquery.ScalarQueryParameter("max_dist", "FLOAT64", max_dist)]
    hp = _healpix_range(healpix)
    if hp:
        where += " AND healpix_2 BETWEEN @min_hp AND @max_hp"
        params += [bigquery.ScalarQueryParameter("min_hp", "INT64", hp[0]),
                   bigquery.ScalarQueryParameter("max_hp", "INT64", hp[1])]
    params.append(bigquery.ScalarQueryParameter("lim", "INT64", limit))
    # No ORDER BY RAND(): rely on cluster pruning (healpix_2, distance_bin) + LIMIT.
    # Prefer the native clustered table; fall back to the external one if it
    # hasn't been materialised yet, then to mock data.
    rows = None
    candidates = [STARS_TABLE] + ([EXTERNAL_TABLE] if EXTERNAL_TABLE != STARS_TABLE else [])
    for tbl in candidates:
        sql = f"""
            SELECT source_id, x, y, z, vx, vy, vz, phot_g_mean_mag, distance_ly
            FROM `{tbl}`
            WHERE {where}
            LIMIT @lim
        """
        try:
            rows = _run(sql, params)
            break
        except (NotFound, GoogleAPIError) as exc:
            logger.warning(f"stars query on {tbl} failed ({exc}); trying next source.")
    if rows is None:
        return _mock_stars(min_dist, max_dist, limit, year)

    out = []
    for r in rows:
        d = r.distance_ly
        vlx, vly, vlz = (r.vx * MAS_TO_RAD_YR * d, r.vy * MAS_TO_RAD_YR * d,
                         r.vz * MAS_TO_RAD_YR * d)
        out.append({"source_id": str(r.source_id),
                    "x": r.x * d + vlx * dt, "y": r.y * d + vly * dt, "z": r.z * d + vlz * dt,
                    "mag": r.phot_g_mean_mag, "dist": d})
    return out


# --------------------------------------------------------------------------- #
# Density — precomputed voxels
# --------------------------------------------------------------------------- #
async def query_density(min_dist: float, max_dist: float,
                        healpix: Optional[int] = None) -> List[Dict[str, Any]]:
    if client is None or FORCE_MOCK:
        return _mock_density(min_dist, max_dist)
    where = "distance_bin BETWEEN @min_d AND @max_d AND n > 0"
    params = [bigquery.ScalarQueryParameter("min_d", "INT64", int(min_dist)),
              bigquery.ScalarQueryParameter("max_d", "INT64", int(max_dist))]
    hp = _healpix_range(healpix)
    if hp:
        where += " AND healpix_2 BETWEEN @min_hp AND @max_hp"
        params += [bigquery.ScalarQueryParameter("min_hp", "INT64", hp[0]),
                   bigquery.ScalarQueryParameter("max_hp", "INT64", hp[1])]
    sql = f"""
        SELECT healpix_2, distance_bin, n, mean_bp_rp, cx, cy, cz
        FROM `{DENSITY_TABLE}` WHERE {where}
    """
    try:
        rows = _run(sql, params)
    except (NotFound, GoogleAPIError) as exc:
        logger.warning(f"density query failed ({exc}); mock fallback.")
        return _mock_density(min_dist, max_dist)
    return [{"hp": r.healpix_2, "bin": r.distance_bin, "n": r.n,
             "bp_rp": r.mean_bp_rp, "x": r.cx, "y": r.cy, "z": r.cz} for r in rows]


# --------------------------------------------------------------------------- #
# HR diagram — precomputed histogram
# --------------------------------------------------------------------------- #
async def query_hr() -> List[Dict[str, Any]]:
    if client is None or FORCE_MOCK:
        return _mock_hr()
    sql = f"SELECT bp_rp_bin, absmag_bin, n FROM `{HR_TABLE}`"
    try:
        rows = _run(sql, [])
    except (NotFound, GoogleAPIError) as exc:
        logger.warning(f"hr query failed ({exc}); mock fallback.")
        return _mock_hr()
    return [{"bp_rp": r.bp_rp_bin, "absmag": r.absmag_bin, "n": r.n} for r in rows]


# --------------------------------------------------------------------------- #
# Clusters — catalogue + sampled members
# --------------------------------------------------------------------------- #
async def query_clusters(max_members: int = 40000) -> Dict[str, Any]:
    if client is None or FORCE_MOCK:
        return _mock_clusters()
    try:
        cat = _run(f"SELECT * FROM `{CLUSTER_CATALOG_TABLE}` ORDER BY n DESC", [])
        mem = _run(f"""SELECT X, Y, Z, mag, cluster_id FROM `{CLUSTER_STARS_TABLE}`
                       LIMIT {int(max_members)}""", [])
    except (NotFound, GoogleAPIError) as exc:
        logger.warning(f"cluster query failed ({exc}); mock fallback.")
        return _mock_clusters()
    clusters = [{"id": c.cluster_id, "n": c.n, "name": c.name,
                 "x": c.cx, "y": c.cy, "z": c.cz,
                 "dist": c.dist_ly, "bp_rp": c.mean_bp_rp} for c in cat]
    stars = [{"x": m.X, "y": m.Y, "z": m.Z, "mag": m.mag, "cid": m.cluster_id} for m in mem]
    return {"clusters": clusters, "stars": stars}


# --------------------------------------------------------------------------- #
# Mock data (used when BigQuery / precomputed tables are unavailable)
# --------------------------------------------------------------------------- #
def _sphere_dirs(n):
    v = _rng.normal(size=(n, 3))
    return v / np.linalg.norm(v, axis=1, keepdims=True)


def _mock_stars(min_dist, max_dist, limit, year):
    n = min(limit, 20000)
    d = _rng.uniform(min_dist, max_dist, n)
    dirs = _sphere_dirs(n)
    # fake a galactic-plane concentration
    dirs[:, 2] *= 0.5
    dirs /= np.linalg.norm(dirs, axis=1, keepdims=True)
    p = dirs * d[:, None]
    mag = _rng.uniform(2, 13, n)
    return [{"source_id": str(i), "x": float(p[i, 0]), "y": float(p[i, 1]),
             "z": float(p[i, 2]), "mag": float(mag[i]), "dist": float(d[i])}
            for i in range(n)]


def _mock_density(min_dist, max_dist):
    out = []
    for hp in range(192):
        dirs = _sphere_dirs(1)[0]
        dirs[2] *= 0.35
        dirs /= np.linalg.norm(dirs)
        for b in range(int(min_dist), int(max_dist), 100):
            disk = np.exp(-abs(dirs[2]) * 3)
            n = int(_rng.poisson(800 * disk * np.exp(-b / 4000.0)) + 1)
            c = dirs * (b + 50)
            out.append({"hp": hp, "bin": b, "n": n, "bp_rp": float(_rng.uniform(0.5, 1.6)),
                        "x": float(c[0]), "y": float(c[1]), "z": float(c[2])})
    return out


def _mock_hr():
    out = []
    bp = _rng.uniform(-0.4, 3.5, 60000)
    absmag = 4.5 + 4.0 * bp + _rng.normal(0, 0.6, bp.size)          # main sequence
    g = _rng.uniform(0.9, 1.6, 8000)                                # giant branch
    bp = np.concatenate([bp, g]); absmag = np.concatenate([absmag, _rng.normal(0.5, 0.6, g.size)])
    from collections import Counter
    c = Counter((round(float(b) / 0.05) * 0.05, round(float(a) / 0.2) * 0.2)
                for b, a in zip(bp, absmag) if -6 < a < 20)
    return [{"bp_rp": k[0], "absmag": k[1], "n": v} for k, v in c.items()]


def _mock_clusters():
    names = ["Hyades", "Pleiades (M45)", "Coma Berenices", "Praesepe (M44)", ""]
    clusters, stars = [], []
    for cid, nm in enumerate(names):
        center = _sphere_dirs(1)[0] * _rng.uniform(120, 580)
        n = int(_rng.integers(80, 400))
        pts = center + _rng.normal(0, 12, (n, 3))
        for p in pts:
            stars.append({"x": float(p[0]), "y": float(p[1]), "z": float(p[2]),
                          "mag": float(_rng.uniform(3, 11)), "cid": cid})
        clusters.append({"id": cid, "n": n, "name": nm,
                         "x": float(center[0]), "y": float(center[1]), "z": float(center[2]),
                         "dist": float(np.linalg.norm(center)), "bp_rp": float(_rng.uniform(0.4, 1.4))})
    return {"clusters": clusters, "stars": stars}
