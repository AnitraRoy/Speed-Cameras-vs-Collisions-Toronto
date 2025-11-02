# scripts/01_clean_collisions.py
from pathlib import Path
import pandas as pd
import numpy as np

RAW = Path("data_raw/collisions.csv")
OUT_DIR = Path("data_clean")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _safe_lower(cols):
    return [c.strip().lower() for c in cols]

def _to_bool(x):
    if isinstance(x, str):
        t = x.strip().lower()
        return t in {"y","yes","true","t","1"}
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, bool):
        return x
    return False

def parse_occ_date(col: pd.Series) -> pd.Series:
    """
    Handles:
      - numeric unix timestamps in seconds or milliseconds (e.g., 1388552400000)
      - date strings
    Returns date (no time) as dtype 'object' containing datetime.date.
    """
    # strings first
    if col.dtype == "O" and not col.astype(str).str.isnumeric().all():
        s = pd.to_datetime(col, errors="coerce", infer_datetime_format=True)
        return s.dt.date

    # numeric-like: decide seconds vs milliseconds
    occ_num = pd.to_numeric(col, errors="coerce")
    # Heuristic: >= 1e11 → milliseconds
    unit = "ms" if occ_num.fillna(0).abs().max() >= 1e11 else "s"
    s = pd.to_datetime(occ_num, unit=unit, errors="coerce")
    return s.dt.date

def main():
    if not RAW.exists():
        raise SystemExit(f"[ERROR] Missing {RAW}")

    df = pd.read_csv(RAW, low_memory=False)
    df.columns = _safe_lower(df.columns)

    # ---- coordinates ----
    lon_col = next((c for c in ["long_wgs84","longitude","x","long","lon"] if c in df.columns), None)
    lat_col = next((c for c in ["lat_wgs84","latitude","y","lat"] if c in df.columns), None)
    if not lon_col or not lat_col:
        raise SystemExit("Could not find longitude/latitude columns (e.g., LONG_WGS84/LAT_WGS84).")

    df["lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    df["lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df.loc[(df["lon"] == 0) | (df["lat"] == 0), ["lon","lat"]] = np.nan

    # ---- date/hour/dow ----
    # OCC_DATE → date
    if "occ_date" in df.columns:
        date = parse_occ_date(df["occ_date"])
    else:
        # fallback: compose from year/month/day if present
        y = pd.to_numeric(df.get("occ_year"), errors="coerce")
        m = df.get("occ_month")
        if m is not None and m.dtype == "O":
            m = pd.to_datetime("2020-" + m.astype(str) + "-01", errors="coerce").dt.month
        else:
            m = pd.to_numeric(m, errors="coerce")
        d = pd.to_numeric(df.get("occ_day"), errors="coerce")
        date = pd.to_datetime(dict(year=y, month=m, day=d), errors="coerce").dt.date

    hour = pd.to_numeric(df.get("occ_hour"), errors="coerce")  # may be NaN
    dow  = df.get("occ_dow")
    if dow is not None:
        dow = dow.astype(str)
    else:
        dow = pd.Series([pd.NA]*len(df))

    # ---- severity (simple two-bucket) ----
    fatal_col = next((c for c in ["fatal","fti_collisions","fatal_collisions"] if c in df.columns), None)
    inj_col   = next((c for c in ["injury","injury_collisions"] if c in df.columns), None)

    fatal  = df[fatal_col].map(_to_bool) if fatal_col else pd.Series(False, index=df.index)
    injury = df[inj_col].map(_to_bool)   if inj_col   else pd.Series(False, index=df.index)

    severity = np.where(fatal, "Injury", np.where(injury, "Injury", "Property Damage Only"))

    # ---- build cleaned frame ----
    out = pd.DataFrame({
        "date": date,          # <-- canonical date for future joins
        "hour": hour,          # optional but useful
        "dow": dow,
        "lat": df["lat"],
        "lon": df["lon"],
        "severity": severity
    })

    # drop rows with missing date or coords
    before = len(out)
    out = out.dropna(subset=["date"]).reset_index(drop=True)
    out = out[(out["lat"].between(43.0, 44.5)) & (out["lon"].between(-80.0, -78.0))]
    after = len(out)

    # save
    out_csv = OUT_DIR / "collisions_clean.csv"
    out_pq  = OUT_DIR / "collisions_clean.parquet"
    out.to_csv(out_csv, index=False)
    try:
        out.to_parquet(out_pq, index=False)
    except Exception:
        pass  # parquet optional

    print(f"✅ collisions cleaned: kept {after:,}/{before:,}")
    print(f"   wrote: {out_csv}")
    print(out.head(8).to_string(index=False))

if __name__ == "__main__":
    main()


