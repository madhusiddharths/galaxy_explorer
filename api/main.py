from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Tuple, Optional
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
    healpix: Optional[int] = None # 1-12 or None
    year: int = 2016

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/stars")
async def get_stars(query: StarQuery):
    """
    Search for stars based on Distance Range and Healpix.
    """
    
    # Determine max stars based on density/range
    # User requested: ~18k for 400ly, 50k for 17000ly.
    # Linear interpolation:
    # limit = 18000 + (32000 * (max_dist / 17000))
    
    # Base limit for small ranges
    base_limit = 18000
    max_limit_cap = 100000
    
    # Calculate scale factor (0.0 to 1.0) based on max_dist
    scale = min(1.0, query.max_dist / 17000.0)
    
    # If looking at a single healpix sector (1/12th of sky), we might want to INCREASE density 
    # relative to the volume, or just keep the same "count" user experience?
    # User said: "if the range and the healpix is more then, more stars can be shown".
    # This implies Healpix="All" -> More stars than Healpix="1".
    
    scale_hp = 1.0
    if query.healpix is not None:
        # If filtering by sector, reduce limit slightly or strictly follow density?
        # If we keep global limit 50k for ALL, then 1 sector should probably have 50k/12 ~ 4k stars?
        # BUT user said "18k stars only when... all healpix... just 10-400".
        # Let's stick to the Distance scaling primarily as requested.
        # "if the range and the healpix is more then, more stars" -> 
        # Range increases -> Limit increases.
        # Healpix increases (1 -> All) -> Limit increases.
        scale_hp = 0.5 # Reduce limit if specialized sector to avoid over-cluttering small volume?
                       # Or maybe user means "Healpix=All gives 50k, Healpix=1 gives less".
                       # Let's try scaling down for single sector.
        pass

    limit = int(base_limit + (max_limit_cap - base_limit) * scale)
    
    # If single healpix, maybe we don't need 50k stars in one wedge?
    # 50k stars in one wedge is extremely dense compared to 50k in whole sky.
    # Let's cap single-sector queries to avoid explosion?
    # Actually, 10-400ly is small volume. 18k stars there.
    # If I do Sector 1, 10-400ly, 1/12th volume.
    # Should I show 18k/12 stars? Or 18k stars (super dense)?
    # User said "18k stars only when ALL 17000 ly... NO wait:
    # "is its just 10 - 400 ly with all healpix, the number of stars should be only around 18k."
    # So 10-400ly + All Sectors = 18k.
    
    # "strictly density based"
    # Logic:
    # Limit depends on Volume?
    # Let's just use the Distance Scaling as valid proxy.
    
    if query.healpix is not None:
        # Reduce limit for single sector to maintain proportional density?
        # User: "if the range and the healpix is more then, more stars".
        # So correct: Limit should be lower for single sector.
        limit = int(limit / 4) # Heuristic: 1/12 is too small, let's say 1/4.
        limit = max(5000, limit) # Min floor
        
    stars = await db.query_stars(
        min_dist=query.min_dist,
        max_dist=query.max_dist,
        healpix=query.healpix,
        limit=limit,
        year=query.year
    )
        
    return {"count": len(stars), "stars": stars}
