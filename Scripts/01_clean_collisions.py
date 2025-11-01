# scripts/01_clean_collisions.py
from pathlib import Path
import pandas as pd
import numpy as np
from _utils import to_bool  # uses your helper mapping YES/NO -> True/False

RAW = Path("data_raw")
OUT = Path("data_clean"); OUT.mkdir(parents=True, exist_ok=True)

infile = RAW / "collisions.csv"  # make sure your raw collisions CSV is here

def _safe_lower(s):
    return [c.strip().lower() for c in s]

def _parse_collision_date(df: pd.DataFrame) -> pd.Series:
    """
    Create a proper calendar date column from the Toronto collisions dataset.
    Tries, in this order:
      1) 'occ_date' if exists:
         - parse as datetime string
         - if numeric: detect unix seconds vs milliseconds
      2) compose from year/month/day columns if available
    Returns a pd.Series of dtype 'date' (not datetime).
    """
    cols = set(df.columns)

    # 1) Direct OCC_DATE parsing if present
    if "occ_date" in cols:
        occ = df["occ_date"]

        # Case A: already strings like '2019-02-14' or '2014/01/03'
        if occ.dtype == "O":
            s = pd.to_datetime(occ, errors="coerce", infer_datetime_format=True)
            if s.notna().any():
                return s.dt.date

        # Case B: numeric unix timestamps (seconds or millis)
        if np.issubdtype(occ.dtype, np.number) or occ.astype(str).str.isnumeric().all():
            occ_num = pd.to_numeric(occ, errors="coerce")

            # Heuristic: milliseconds if values are >= 10^11
            unit = "ms" if occ_num.fillna(0).abs().max() >= 1e11 else "s"
            s = pd.to_datetime(occ_num, unit=unit, errors="coerce")
            if s.notna().any():
                return s.dt.date

    # 2) Compose from year/month/day if available
    ycol = next((c for c in ["occ_year", "year"] if c in cols), None)
    mcol = next((c for c in ["occ_month", "month"] if c in cols), None)
    dcol = next((c for c in ["occ_day", "day"] if c in cols), None)

    if ycol and mcol and dcol:
        # If month is a name, map to number
        m = df[mcol]
        if m.dtype == "O":
            m_num = pd.to_datetime("2020-" + m.astype(str) + "-01", errors="coerce").dt.month
        else:
            m_num = pd.to_numeric(m, errors="coerce")
        y = pd.to_numeric(df[ycol], errors="coerce")
        d = pd.to_numeric(df[dcol], errors="coerce")

        s = pd.to_datetime(dict(year=y, month=m_num, day=d), errors="coerce")
        return s.dt.date

    # 3) If we got here, everything failed → all NaT
    return pd.to_datetime(pd.Series([pd.NaT] * len(df))).dt.date

# ---------------- Load & normalize ----------------
df = pd.read_csv(infile, low_memory=False)
df.columns = _safe_lower(df.columns)

# ---- Coordinates ----
lon_col = next((c for c in ["long_wgs84", "longitude", "x"] if c in df.columns), None)
lat_col = next((c for c in ["lat_wgs84",  "latitude",  "y"] if c in df.columns), None)

if not lon_col or not lat_col:
    raise ValueError("Could not find longitude/latitude columns (expected LONG_WGS84/LAT_WGS84).")

df["lon"] = pd.to_numeric(df[lon_col], errors="coerce")
df["lat"] = pd.to_numeric(df[lat_col], errors="coerce")
# remove clearly invalid coordinates
df.loc[(df["lon"] == 0) | (df["lat"] == 0), ["lon", "lat"]] = np.nan

# ---- Severity (simple 2-bucket) ----
# Map from the typical columns: 'fatal' / 'injury' YES/NO flags.
fatal_col  = next((c for c in ["fatal", "fti_collisions", "fatal_collisions"] if c in df.columns), None)
inj_col    = next((c for c in ["injury", "injury_collisions"] if c in df.columns), None)

def _to_bool_series(s):
    if s is None or s not in df.columns:
        return pd.Series([False]*len(df))
    v = df[s]
    if v.dtype == "O":
        return v.map(lambda x: to_bool(x) or False)
    return v.astype(bool)

fatal  = _to_bool_series(fatal_col)
injury = _to_bool_series(inj_col)

severity = pd.Series(np.where(fatal, "Fatal",
                       np.where(injury, "Injury", "Property Damage Only")))
# If dataset doesn’t distinguish, collapse Fatal into Injury (2 buckets requested earlier)
severity = severity.replace({"Fatal": "Injury"})

# ---- Time fields (optional) ----
hour_col = next((c for c in ["occ_hour", "hour"] if c in df.columns), None)
dow_col  = next((c for c in ["occ_dow", "dow"] if c in df.columns), None)
hour = pd.to_numeric(df[hour_col], errors="coerce") if hour_col else pd.Series([np.nan]*len(df))
dow  = df[dow_col].astype(str) if dow_col else pd.Series([pd.NA]*len(df))

# ---- Date (this was the root cause) ----
date = _parse_collision_date(df)

# ---- Build cleaned frame ----
out_cols = pd.DataFrame({
    "date": date,
    "hour": hour,
    "dow": dow,
    "lat": df["lat"],
    "lon": df["lon"],
    "severity": severity
})

# Drop rows with no usable date or coordinates (optional but helpful)
out_cols = out_cols.dropna(subset=["date"]).reset_index(drop=True)

# ---- Save ----
out_pq  = OUT / "collisions_clean.parquet"
out_csv = OUT / "collisions_clean.csv"
out_cols.to_parquet(out_pq, index=False)
out_cols.to_csv(out_csv, index=False)
print(f"Saved {len(out_cols):,} rows -> {out_pq}")
