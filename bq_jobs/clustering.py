"""
Stellar Families — find open clusters & co-moving groups in 6D phase space.

Scales up pyspark_code/clustering.py: instead of clustering one HEALPix tile in
4D, we pull the nearby solar neighbourhood (default < 600 ly, where Gaia
distances are reliable and the classic clusters live) and run HDBSCAN over the
full 6D state (position x,y,z in ly + space velocity vx,vy,vz). Co-moving stars
fall into the same cluster.

Outputs two native tables (run in YOUR GCP project; the SELECT scans data):
  gaia_ly.cluster_stars    source_id, X, Y, Z, mag, bp_rp, cluster_id   (members)
  gaia_ly.cluster_catalog  cluster_id, n, centroid, mean velocity, name  (per group)

Usage:
  python bq_jobs/clustering.py --dry-run                 # count stars + cost note
  python bq_jobs/clustering.py --run --max-dist 600      # cluster & write back
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(Path(__file__).resolve().parent.parent / "api" / ".env")

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "aicouncelling")
DATASET = os.getenv("BIGQUERY_DATASET", "gaia_ly")
NATIVE = f"{PROJECT}.{DATASET}.stars_native"
EXTERNAL = os.getenv("BIGQUERY_TABLE", f"{PROJECT}.{DATASET}.stars")
MAS_TO_RAD_YR = (np.pi / 180.0) / 3_600_000.0

# A few famous nearby clusters (name, ra°, dec°, dist ly) for labelling centroids.
KNOWN = [
    ("Hyades", 66.75, 15.87, 151),
    ("Pleiades (M45)", 56.75, 24.12, 444),
    ("Coma Berenices", 186.0, 26.0, 280),
    ("Praesepe (M44)", 130.1, 19.67, 577),
    ("Ursa Major moving group", 165.0, 56.0, 80),
    ("alpha Persei", 51.0, 49.0, 560),
]


def _known_xyz() -> list[tuple[str, np.ndarray]]:
    out = []
    for name, ra, dec, d in KNOWN:
        r, dr = np.radians(ra), np.radians(dec)
        out.append((name, np.array([d * np.cos(dr) * np.cos(r),
                                    d * np.cos(dr) * np.sin(r),
                                    d * np.sin(dr)])))
    return out


def _query(client, table, max_dist, sample_cap):
    sql = f"""
        SELECT source_id,
               x * distance_ly AS X, y * distance_ly AS Y, z * distance_ly AS Z,
               vx, vy, vz, distance_ly, phot_g_mean_mag AS mag, bp_rp
        FROM `{table}`
        WHERE distance_ly > 0 AND distance_ly < {max_dist}
    """
    if sample_cap:
        sql += f"\n        ORDER BY RAND() LIMIT {sample_cap}"
    return client.query(sql).result().to_dataframe()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-dist", type=float, default=600.0, help="ly cutoff")
    ap.add_argument("--min-cluster-size", type=int, default=30)
    ap.add_argument("--min-samples", type=int, default=5)
    ap.add_argument("--vel-weight", type=float, default=3.5,
                    help="how strongly velocity (vs position) drives clustering")
    ap.add_argument("--max-extent", type=float, default=60.0,
                    help="reject groups whose position RMS (ly) exceeds this (field, not a cluster)")
    ap.add_argument("--max-members", type=int, default=8000,
                    help="reject groups larger than this (field, not a cluster)")
    ap.add_argument("--sample-cap", type=int, default=0,
                    help="cap stars pulled (0 = all in volume)")
    ap.add_argument("--external", action="store_true", help="read external table")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client = bigquery.Client(project=PROJECT)
    table = EXTERNAL if args.external else NATIVE

    count_sql = (f"SELECT COUNT(*) n FROM `{table}` "
                 f"WHERE distance_ly > 0 AND distance_ly < {args.max_dist}")
    n = list(client.query(count_sql).result())[0].n
    print(f"{n:,} stars within {args.max_dist:.0f} ly of the Sun in `{table.split('.')[-1]}`.")
    if not args.run or args.dry_run:
        print("Dry run. Re-run with --run to cluster & write back "
              "(the SELECT scans these rows).")
        return 0

    print("Pulling stars…")
    df = _query(client, table, args.max_dist, args.sample_cap)
    print(f"Loaded {len(df):,}. Clustering in 6D (position + velocity)…")

    # Standardise position and velocity separately, then up-weight velocity:
    # open clusters / moving groups are defined by a shared space velocity
    # (dispersion ~1 km/s) sitting in a field with ~40 km/s spread.
    from sklearn.preprocessing import StandardScaler
    X = df[["X", "Y", "Z"]].to_numpy()
    v = df[["vx", "vy", "vz"]].to_numpy() * MAS_TO_RAD_YR * df["distance_ly"].to_numpy()[:, None]
    Xs = StandardScaler().fit_transform(X)
    Vs = StandardScaler().fit_transform(v)
    feats = np.hstack([Xs, Vs * args.vel_weight]).astype(np.float64)

    import hdbscan
    # 'leaf' selection returns the many small, dense knots (real clusters) instead
    # of the single giant root cluster that 'eom' picks out of the field.
    labels = hdbscan.HDBSCAN(min_cluster_size=args.min_cluster_size,
                             min_samples=args.min_samples,
                             cluster_selection_method="leaf",
                             core_dist_n_jobs=-1).fit_predict(feats)
    df["cluster_id"] = labels.astype(int)

    # Keep only compact groups; relabel rejected ones as field (-1).
    known = _known_xyz()
    rows, keep = [], {}
    for cid in sorted(set(labels)):
        if cid < 0:
            continue
        m = df[df.cluster_id == cid]
        cen = m[["X", "Y", "Z"]].mean().to_numpy()
        rms = float(np.sqrt(((m[["X", "Y", "Z"]].to_numpy() - cen) ** 2).sum(1).mean()))
        if len(m) > args.max_members or rms > args.max_extent:
            continue
        new_id = len(rows)
        keep[cid] = new_id
        name, best = "", 1e9
        for kn, kxyz in known:
            d = float(np.linalg.norm(cen - kxyz))
            if d < best and d < 0.18 * np.linalg.norm(kxyz) + 30:
                best, name = d, kn
        rows.append({
            "cluster_id": new_id, "n": int(len(m)),
            "cx": float(cen[0]), "cy": float(cen[1]), "cz": float(cen[2]),
            "dist_ly": float(np.linalg.norm(cen)), "extent_ly": round(rms, 1),
            "mean_bp_rp": float(m.bp_rp.mean()), "name": name, "_md": best,
        })

    # Each known cluster names only its single closest group; clear the rest.
    for nm in {r["name"] for r in rows if r["name"]}:
        matches = sorted((r for r in rows if r["name"] == nm), key=lambda r: r["_md"])
        for r in matches[1:]:
            r["name"] = ""
    for r in rows:
        r.pop("_md", None)

    df["cluster_id"] = df["cluster_id"].map(lambda c: keep.get(c, -1))
    n_clusters = len(rows)
    n_members = int((df.cluster_id >= 0).sum())
    print(f"Kept {n_clusters} compact groups; {n_members:,} member stars "
          f"(filtered out the diffuse field).")

    import pandas as pd
    catalog = pd.DataFrame(rows).sort_values("n", ascending=False)
    print(catalog.head(20).to_string(index=False))

    members = df[df.cluster_id >= 0][["source_id", "X", "Y", "Z", "mag", "bp_rp", "cluster_id"]]
    job = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(members, f"{PROJECT}.{DATASET}.cluster_stars", job_config=job).result()
    client.load_table_from_dataframe(catalog, f"{PROJECT}.{DATASET}.cluster_catalog", job_config=job).result()
    print(f"Wrote {len(members):,} members + {len(catalog)} clusters to BigQuery.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
