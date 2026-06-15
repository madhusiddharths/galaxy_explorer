"""
Big-data precompute jobs for Galaxy Explorer (Part 2), run in YOUR GCP project.

The source is an EXTERNAL BigQuery table over ~77 GB / 557 M Gaia stars in GCS
(`aicouncelling.gaia_ly.stars`). External tables can't be clustered, so every
query full-scans 77 GB. These jobs fix that and precompute the small aggregate
tables the API serves:

  materialize  -> gaia_ly.stars_native   (native, CLUSTERED by healpix_2,
                                           distance_bin; slimmed columns)
  density      -> gaia_ly.density_voxels  (healpix x distance-shell counts)
  hr           -> gaia_ly.hr_bins         (BP-RP x absolute-mag histogram)

Every job supports a FREE dry run that prints bytes scanned + a cost estimate
before you spend anything.  Clustering is a separate script (clustering.py).

Usage:
  python bq_jobs/build.py --job all       --dry-run     # estimate cost (free)
  python bq_jobs/build.py --job materialize --run        # ~77 GB one-time scan
  python bq_jobs/build.py --job density --run            # cheap on stars_native
  python bq_jobs/build.py --job hr --run                 # cheap on stars_native
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(Path(__file__).resolve().parent.parent / "api" / ".env")

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "aicouncelling")
DATASET = os.getenv("BIGQUERY_DATASET", "gaia_ly")
SOURCE = os.getenv("BIGQUERY_TABLE", f"{PROJECT}.{DATASET}.stars")   # external 77 GB
NATIVE = f"{PROJECT}.{DATASET}.stars_native"
DENSITY = f"{PROJECT}.{DATASET}.density_voxels"
HR = f"{PROJECT}.{DATASET}.hr_bins"

PRICE_PER_TB = 5.0          # USD, BigQuery on-demand
PC_PER_LY = 1.0 / 3.26156   # ly -> parsec
SENTINEL_LY = 9999          # low-parallax rows were parked at distance_ly = 10000

# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #
SQL = {
    # One-time: native, clustered, slimmed copy of the external table.
    "materialize": f"""
        CREATE OR REPLACE TABLE `{NATIVE}`
        CLUSTER BY healpix_2, distance_bin AS
        SELECT
            source_id, healpix_2, distance_bin, distance_ly,
            x, y, z, vx, vy, vz,
            phot_g_mean_mag, bp_rp, has_rvs
        FROM `{SOURCE}`
        WHERE distance_ly > 0
    """,

    # 3D density field: one row per (healpix sector, distance shell).
    "density": f"""
        CREATE OR REPLACE TABLE `{DENSITY}` AS
        SELECT
            healpix_2,
            distance_bin,
            COUNT(*)                       AS n,
            AVG(phot_g_mean_mag)           AS mean_g,
            AVG(bp_rp)                     AS mean_bp_rp,
            AVG(x * distance_ly)           AS cx,
            AVG(y * distance_ly)           AS cy,
            AVG(z * distance_ly)           AS cz
        FROM `{{src}}`
        WHERE distance_ly < {SENTINEL_LY}
        GROUP BY healpix_2, distance_bin
    """,

    # Hertzsprung-Russell / colour-magnitude histogram.
    "hr": f"""
        CREATE OR REPLACE TABLE `{HR}` AS
        WITH s AS (
            SELECT
                bp_rp,
                phot_g_mean_mag - 5 * LOG10(distance_ly * {PC_PER_LY}) + 5 AS absmag,
                distance_ly
            FROM `{{src}}`
            WHERE bp_rp IS NOT NULL
              AND distance_ly > 0 AND distance_ly < {SENTINEL_LY}
              AND bp_rp BETWEEN -0.6 AND 4.5
        )
        SELECT
            ROUND(bp_rp / 0.05) * 0.05  AS bp_rp_bin,
            ROUND(absmag / 0.2) * 0.2   AS absmag_bin,
            COUNT(*)                    AS n
        FROM s
        WHERE absmag BETWEEN -6 AND 20
        GROUP BY bp_rp_bin, absmag_bin
    """,
}


JOB_DEST = {"materialize": NATIVE, "density": DENSITY, "hr": HR}


def _job_source(job: str, use_native: bool) -> str:
    if job == "materialize":
        return SOURCE                       # always reads the external 77 GB table
    return NATIVE if use_native else SOURCE


def _render(job: str, use_native: bool) -> str:
    sql = SQL[job]
    return sql.replace("{src}", _job_source(job, use_native)) if "{src}" in sql else sql


def _cost(client: bigquery.Client, sql: str) -> int:
    cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=cfg)
    return job.total_bytes_processed


def run_job(client: bigquery.Client, job: str, use_native: bool, do_run: bool) -> None:
    sql = _render(job, use_native)
    src, dest = _job_source(job, use_native), JOB_DEST[job]
    print(f"\n=== {job}  ({src.split('.')[-1]} -> {dest.split('.')[-1]}) ===")
    try:
        nbytes = _cost(client, sql)
        print(f"  est. scan {nbytes/1e9:.2f} GB  ->  ${nbytes/1e12 * PRICE_PER_TB:.3f}"
              + ("  (external dry-runs report 0)" if nbytes == 0 else ""))
    except Exception as exc:  # noqa: BLE001
        print(f"  dry-run note: {exc}")
    if not do_run:
        print("  (dry run — nothing executed)")
        return
    print("  running…")
    q = client.query(sql)
    q.result()  # CREATE TABLE AS SELECT is DDL -> returns no rows
    try:
        t = client.get_table(dest)
        print(f"  done. `{dest.split('.')[-1]}` now holds {t.num_rows:,} rows "
              f"({(t.num_bytes or 0)/1e9:.2f} GB); scanned {q.total_bytes_processed/1e9:.2f} GB "
              f"(${q.total_bytes_processed/1e12 * PRICE_PER_TB:.3f}).")
    except Exception as exc:  # noqa: BLE001
        print(f"  done (couldn't read {dest}: {exc}).")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--job", choices=["materialize", "density", "hr", "all"], default="all")
    ap.add_argument("--run", action="store_true", help="actually execute (costs money)")
    ap.add_argument("--dry-run", action="store_true", help="estimate cost only (free)")
    ap.add_argument("--external", action="store_true",
                    help="aggregate straight off the 77 GB external table "
                         "(skip materialize; pricier per job)")
    args = ap.parse_args()
    do_run = args.run and not args.dry_run

    client = bigquery.Client(project=PROJECT)
    use_native = not args.external

    jobs = ["materialize", "density", "hr"] if args.job == "all" else [args.job]
    if args.external and "materialize" in jobs:
        jobs.remove("materialize")

    if do_run and "materialize" in jobs:
        print("NOTE: 'materialize' scans the full ~77 GB external table once.")

    total = 0
    for job in jobs:
        # density/hr read the native table, which only exists after materialize
        run_job(client, job, use_native=use_native, do_run=do_run)
    if not do_run:
        print("\nRe-run with --run to execute. Order: materialize → density → hr,"
              " then `python bq_jobs/clustering.py`.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
