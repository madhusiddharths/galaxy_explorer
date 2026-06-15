from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Tuple, Optional
import os
from . import db

app = FastAPI(title="Universe API")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all for dev, restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class StarQuery(BaseModel):
    min_dist: float
    max_dist: float
    healpix: Optional[int] = None  # 1-12 or None
    year: int = 2016


class DensityQuery(BaseModel):
    min_dist: float = 0
    max_dist: float = 17000
    healpix: Optional[int] = None


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/stars")
async def get_stars(query: StarQuery):
    """Sampled 3D star scatter for a distance range / sector, projected to a year."""
    # Density-based limit: more stars for wider distance ranges, fewer per sector.
    scale = min(1.0, query.max_dist / 17000.0)
    limit = int(18000 + (100000 - 18000) * scale)
    if query.healpix is not None:
        limit = max(5000, limit // 4)
    stars = await db.query_stars(query.min_dist, query.max_dist,
                                 query.healpix, limit, query.year)
    return {"count": len(stars), "stars": stars}


@app.post("/density")
async def get_density(query: DensityQuery):
    """Precomputed stellar-density voxels (healpix x distance shell)."""
    voxels = await db.query_density(query.min_dist, query.max_dist, query.healpix)
    return {"count": len(voxels), "voxels": voxels}


@app.get("/hr")
async def get_hr():
    """Precomputed Hertzsprung-Russell (colour-magnitude) histogram."""
    bins = await db.query_hr()
    return {"count": len(bins), "bins": bins}


@app.get("/clusters")
async def get_clusters():
    """Open clusters / moving groups: catalogue + sampled members."""
    return await db.query_clusters()

# Serve React Frontend (SPA)
# Mount static files (JS, CSS, images)
# Check if static directory exists (for local dev vs prod)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Allow API routes to pass through if not matched above (FastAPI handles order)
        # But since this is a catch-all, we need to be careful.
        # Actually, FastAPI matches specific routes first.
        # So we just serve index.html for anything not caught.
        
        # Check if file exists in static (e.g. vite.svg)
        file_path = os.path.join(static_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
            
        return FileResponse(os.path.join(static_dir, "index.html"))
