# scripts/04_merge_enrich.py
from pathlib import Path
import pandas as pd
import numpy as np

# config
CLEAN = Path("data_clean")
MODEL = Path("data_model"); MODEL.mkdir(parents=True, exist_ok=True)

COLLISIONS_PQ = CLEAN / "collisions_clean.parquet"
COLLISIONS_CSV = CLEAN / "collisions_clean.csv"
WEATHER_PQ    = CLEAN / "weather_clean.parquet"
WEATHER_CSV   = CLEAN / "weather_clean.csv"
CAMERAS_PQ    = CLEAN / "speed_cameras_clean.parquet"
CAMERAS_CSV   = CLEAN / "speed_cameras_clean.csv"

NEAR_THRESHOLD_M = 250  # distance for near-camera flag

#utils -
def load_any(pq: Path, csv: Path) -> pd.DataFrame:
    if pq.exists():
        return pd.read_parquet(pq)
    if csv.exists():
        return pd.read_csv(csv)
    raise FileNotFoundError(f"Missing input: {pq} or {csv}")

def to_date(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"Expected column '{col}' for merging.")
    return df.assign(**{col: pd.to_datetime(df[col], errors="coerce").dt.date})

def coerce_num(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def ensure_precip_day(wx: pd.DataFrame) -> pd.DataFrame:
    """Make a robust precip flag using any of precip/rain/snow > 0."""
    # normalize common amount columns
    for c in ["precipitation","total_precip","total_precipitation","precip",
              "rain","rain_mm","snow","snow_mm"]:
        if c not in wx.columns:
            continue
        wx[c] = pd.to_numeric(wx[c], errors="coerce").fillna(0)
    # choose best available columns
    p = wx.get("precipitation", wx.get("total_precip", wx.get("total_precipitation", wx.get("precip", 0))))
    r = wx.get("rain", wx.get("rain_mm", 0))
    s = wx.get("snow", wx.get("snow_mm", 0))
    wx["precip_day"] = ((pd.to_numeric(p, errors="coerce").fillna(0) > 0) |
                        (pd.to_numeric(r, errors="coerce").fillna(0) > 0) |
                        (pd.to_numeric(s, errors="coerce").fillna(0) > 0)).astype("int8")
    return wx

def haversine_m(lat1, lon1, lat2, lon2):
    """Vectorized Haversine distance (meters). Inputs in radians."""
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371000.0 * c  # Earth radius (m)

def attach_nearest_camera(collisions: pd.DataFrame, cams: pd.DataFrame, threshold_m: float) -> pd.DataFrame:
    """Attach nearest speed camera distance/attrs. Works with numpy only; chunks to keep memory low."""
    if collisions.empty or cams.empty:
        collisions["cam_nearest_m"] = np.nan
        collisions["cam_within_250m"] = np.int8(0)
        return collisions

    # require lon/lat columns in both
    for c in ["lat","lon"]:
        if c not in collisions.columns:
            raise ValueError(f"collisions missing '{c}'")
        if c not in cams.columns:
            raise ValueError(f"cameras missing '{c}'")

    # radians
    coll_ok = collisions[collisions[["lat","lon"]].notna().all(axis=1)].copy()
    coll_idx = coll_ok.index.to_numpy()
    lat1 = np.radians(coll_ok["lat"].to_numpy())
    lon1 = np.radians(coll_ok["lon"].to_numpy())

    lat2 = np.radians(pd.to_numeric(cams["lat"], errors="coerce").to_numpy())
    lon2 = np.radians(pd.to_numeric(cams["lon"], errors="coerce").to_numpy())

    # prepare outputs
    nearest_m = np.full(coll_ok.shape[0], np.nan, dtype=float)
    nearest_j = np.full(coll_ok.shape[0], -1, dtype=int)

    # chunk over collisions to limit (N x M) memory
    M = len(cams)
    CHUNK = 50000
    for start in range(0, coll_ok.shape[0], CHUNK):
        end = min(start + CHUNK, coll_ok.shape[0])
        # broadcast distances: (chunk, M)
        d = haversine_m(lat1[start:end, None], lon1[start:end, None], lat2[None, :], lon2[None, :])
        j = np.argmin(d, axis=1)
        nearest_j[start:end] = j
        nearest_m[start:end] = d[np.arange(end - start), j]

    # write back into full collisions df at matching indices
    collisions.loc[coll_idx, "cam_nearest_m"] = nearest_m
    collisions["cam_within_250m"] = (collisions["cam_nearest_m"] <= threshold_m).fillna(False).astype("int8")

    # attach a few camera attributes for the nearest camera (optional, if present)
    attach_cols = [c for c in ["location","status_clean","ward_num","fid"] if c in cams.columns]
    if attach_cols:
        # build small frame of nearest camera attrs
        nearest_attrs = pd.DataFrame(index=coll_idx)
        for c in attach_cols:
            vals = cams[c].to_numpy()
            nearest_attrs[f"cam_{c}"] = vals[nearest_j[np.arange(nearest_attrs.shape[0])]]
        for c in nearest_attrs.columns:
            collisions.loc[coll_idx, c] = nearest_attrs[c].values

    return collisions

# pipeline
def main():
    # Load inputs
    collisions = load_any(COLLISIONS_PQ, COLLISIONS_CSV)
    weather    = load_any(WEATHER_PQ, WEATHER_CSV)

    # Normalize merge keys
    collisions = to_date(collisions, "date")
    weather    = to_date(weather,    "date")

    # Ensure precip flag and numeric amounts
    weather = ensure_precip_day(weather)
    weather = coerce_num(weather, ["precipitation","total_precip","total_precipitation","precip","rain","snow"])

    # Build map for authoritative precip flag
    flag_by_date = dict(zip(weather["date"], weather["precip_day"]))

    # Prefix weather cols (context)
    wx_cols = [c for c in weather.columns if c != "date"]
    weather_prefixed = weather.rename(columns={c: f"wx_{c}" for c in wx_cols})

    # Merge collisions + weather
    df = collisions.merge(weather_prefixed, on="date", how="left", validate="m:1")
    # Overwrite precip flag with authoritative map (clean int8)
    df["wx_precip_day"] = pd.Series(df["date"]).map(flag_by_date).fillna(0).astype("int8")

    # Convenience: a single "any precipitation amount" column (max of available numeric amounts)
    amount_cols = [c for c in ["wx_precipitation","wx_total_precip","wx_total_precipitation","wx_precip","wx_rain","wx_snow"]
                   if c in df.columns]
    for c in amount_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["wx_precip_amount_any"] = df[amount_cols].max(axis=1) if amount_cols else 0.0

    # Optional: attach nearest speed camera if the file exists
    if CAMERAS_PQ.exists() or CAMERAS_CSV.exists():
        cams = load_any(CAMERAS_PQ, CAMERAS_CSV)
        df = attach_nearest_camera(df, cams, NEAR_THRESHOLD_M)
        print(f"Nearest camera attached (≤ {NEAR_THRESHOLD_M} m flag in 'cam_within_250m').")
    else:
        df["cam_nearest_m"] = np.nan
        df["cam_within_250m"] = np.int8(0)
        print("No camera file found; skipping camera enrichment.")

    # Basic sanity prints
    matched = df["wx_precip_day"].notna().sum()
    print(f"Collisions: {len(collisions):,} | Weather days: {weather['date'].nunique():,} | Rows with matched weather: {matched:,}")
    print("wx_precip_day counts:\n", df["wx_precip_day"].value_counts(dropna=False).to_string())

    # Save model outputs for teammates (Parquet + CSV)
    out_pq  = MODEL / "collisions_enriched.parquet"
    out_csv = MODEL / "collisions_enriched.csv"
    df.to_parquet(out_pq, index=False)
    df.to_csv(out_csv, index=False)
    print(f"Saved {len(df):,} rows -> {out_pq}")
    print(f"Saved {len(df):,} rows -> {out_csv}")

if __name__ == "__main__":
    main()
