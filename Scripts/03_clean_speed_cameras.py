# scripts/03_clean_speed_cameras.py
import pandas as pd
from pathlib import Path
from _utils import explode_multipoints, drop_bad_coords

RAW = Path("data_raw")
OUT = Path("data_clean"); OUT.mkdir(parents=True, exist_ok=True)

infile = RAW / "speed_cameras.csv"

df = pd.read_csv(infile, low_memory=False)
# Normalize columns
df.columns = [c.strip() for c in df.columns]

# Try common names; keep flexible
possible_geom = [c for c in df.columns if c.lower() == "geometry"]
geom_col = possible_geom[0] if possible_geom else None

# Try to keep a few helpful columns if present
keep_cols = []
for name in ["Status","Ward","Location","Address","FID","Location_Code","Location Code","Status Icon"]:
    if name in df.columns:
        keep_cols.append(name)
if geom_col:
    keep_cols.append(geom_col)
df = df[keep_cols].copy()

# Add lon/lat by exploding MultiPoint
if geom_col:
    df = explode_multipoints(df, geom_col, "lon", "lat")
else:
    # If the file already has lon/lat columns (rare), keep them
    for cand in ["LONG_WGS84","Longitude","lon","LONG","X"]:
        if cand in df.columns:
            df = df.rename(columns={cand:"lon"})
            break
    for cand in ["LAT_WGS84","Latitude","lat","LAT","Y"]:
        if cand in df.columns:
            df = df.rename(columns={cand:"lat"})
            break

df = drop_bad_coords(df, "lon", "lat")

# Normalize status
if "Status" in df.columns:
    df["status_clean"] = df["Status"].str.strip().str.lower()
else:
    df["status_clean"] = None

outpath = OUT / "speed_cameras_clean.parquet"
df.to_parquet(outpath, index=False)
print(f"Saved {len(df):,} rows -> {outpath}")
