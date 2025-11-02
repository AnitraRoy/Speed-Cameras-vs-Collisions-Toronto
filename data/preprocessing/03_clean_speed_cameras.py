# scripts/03_clean_speed_cameras.py
from pathlib import Path
import pandas as pd
import numpy as np
import json
import ast
import re

RAW = Path("data_raw/speed_cameras.csv")
OUT_DIR = Path("data_clean")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def parse_geometry(val):
    """
    Accepts strings like:
      {"coordinates": [[-79.56, 43.71]], "type": "MultiPoint"}
      {"coordinates": [-79.56, 43.71],   "type": "Point"}
    Returns a list of [lon, lat] pairs (possibly length 1).
    """
    if pd.isna(val):
        return []
    s = str(val).strip()
    # Try JSON first
    try:
        obj = json.loads(s)
    except Exception:
        # Some CSVs store with single quotes; use ast.literal_eval as fallback
        try:
            obj = ast.literal_eval(s)
        except Exception:
            return []
    coords = obj.get("coordinates", [])
    # If it's a single point, wrap it
    if isinstance(coords, (list, tuple)) and len(coords) == 2 and all(isinstance(x, (int,float)) for x in coords):
        coords = [coords]
    # Validate lon/lat ordering; keep as-is (dataset uses lon, lat)
    out = []
    for c in coords:
        if isinstance(c, (list, tuple)) and len(c) >= 2:
            try:
                lon = float(c[0]); lat = float(c[1])
                out.append([lon, lat])
            except Exception:
                continue
    return out

def clean_ward(val):
    """E.g., '1 - Etobicoke North' -> 1 ; otherwise returns original/NaN."""
    if pd.isna(val):
        return np.nan
    m = re.match(r"\s*(\d+)\s*[-–]", str(val))
    return int(m.group(1)) if m else pd.NA

def main():
    if not RAW.exists():
        raise SystemExit(f"[ERROR] Missing {RAW}")

    df = pd.read_csv(RAW, low_memory=False)
    # Normalize columns (keep original names for display columns)
    lower = {c: c.strip().lower() for c in df.columns}
    df.rename(columns=lower, inplace=True)

    # Identify key columns if present
    geom_col = "geometry" if "geometry" in df.columns else None
    ward_col = "ward" if "ward" in df.columns else None
    status_col = "status" if "status" in df.columns else None
    loc_col = None
    for c in ["location", "address", "site", "site_location"]:
        if c in df.columns:
            loc_col = c; break

    if not geom_col:
        raise SystemExit("Expected a 'geometry' column with MultiPoint/Point coordinates.")

    # Parse & explode geometry → one row per [lon, lat]
    coords = df[geom_col].apply(parse_geometry)
    df = df.assign(_coords=coords)
    df = df[df["_coords"].map(len) > 0].explode("_coords", ignore_index=True)
    df[["lon", "lat"]] = pd.DataFrame(df["_coords"].tolist(), index=df.index)
    df.drop(columns=["_coords"], inplace=True)

    # Coerce numeric + drop clearly bad coords
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df = df.dropna(subset=["lon","lat"])
    # Loose GTA bbox
    df = df[(df["lat"].between(43.0, 44.5)) & (df["lon"].between(-80.0, -78.0))]

    # Normalize status & ward
    if status_col:
        df["status_clean"] = df[status_col].astype(str).str.strip().str.lower()
    else:
        df["status_clean"] = pd.NA

    if ward_col:
        df["ward_num"] = df[ward_col].apply(clean_ward)
    else:
        df["ward_num"] = pd.NA

    # Build tidy output
    out_cols = ["lon","lat","status_clean","ward_num"]
    if loc_col: out_cols.append(loc_col)
    # keep original FID if exists
    if "fid" in df.columns: out_cols.append("fid")

    out = df[out_cols].reset_index(drop=True)

    # Save
    out_csv = OUT_DIR / "speed_cameras_clean.csv"
    out_pq  = OUT_DIR / "speed_cameras_clean.parquet"
    out.to_csv(out_csv, index=False)
    try:
        out.to_parquet(out_pq, index=False)
    except Exception:
        pass

    print(f"✅ speed cameras cleaned: {len(out):,} rows")
    print(f"   wrote: {out_csv}")
    print(out.head(8).to_string(index=False))

if __name__ == "__main__":
    main()
