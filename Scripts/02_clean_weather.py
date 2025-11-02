# scripts/02_clean_weather.py
from pathlib import Path
import pandas as pd
import numpy as np

RAW_WEATHER = Path("data_raw/weather.csv")
CLEAN_DIR = Path("data_clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

COLLISIONS_CLEAN = CLEAN_DIR / "collisions_clean.csv"

def to_date(s: pd.Series) -> pd.Series:
    """Parse a date-like column to plain date (no time)."""
    dt = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
    return dt.dt.date

def to_num(s: pd.Series) -> pd.Series:
    """Coerce to numeric, NaN on errors."""
    return pd.to_numeric(s, errors="coerce")

def main():
    if not RAW_WEATHER.exists():
        raise SystemExit(f"[ERROR] Missing {RAW_WEATHER}")

    df = pd.read_csv(RAW_WEATHER, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]

    if "date" not in df.columns:
        raise SystemExit("Weather file must contain a 'date' column.")

    # ---- parse date
    df["date"] = to_date(df["date"])

    # ---- choose useful numeric columns if present (robust to schema differences)
    maybe_numeric = [c for c in df.columns if c != "date"]
    for c in maybe_numeric:
        # try numeric, keep strings if clearly non-numeric text columns
        if df[c].dtype == "O":
            # only coerce if column looks numeric-ish
            if df[c].str.replace(r"[.\-+eE]", "", regex=True).str.isnumeric().mean() > 0.3:
                df[c] = to_num(df[c])
        else:
            df[c] = to_num(df[c])

    # ---- build precip_day = 1 if rain OR snow OR total precipitation > 0
    # Recognize common column names
    precip_cols = [c for c in df.columns if c in {"precipitation","total_precip","total_precipitation","precip"}]
    rain_cols   = [c for c in df.columns if c in {"rain","rain_mm","rainfall"}]
    snow_cols   = [c for c in df.columns if c in {"snow","snow_mm","snowfall","snow_on_ground"}]

    # numeric checks
    precip_num = pd.Series(0, index=df.index, dtype="int8")
    if precip_cols:
        p = df[precip_cols[0]]
        if p.dtype.kind in "biufc":
            precip_num = (p.fillna(0) > 0).astype("int8")
        else:
            precip_num = df[precip_cols[0]].astype(str).str.contains("rain|snow", case=False, na=False).astype("int8")

    rain_num = pd.Series(0, index=df.index, dtype="int8")
    if rain_cols:
        r = df[rain_cols[0]]
        if r.dtype.kind in "biufc":
            rain_num = (r.fillna(0) > 0).astype("int8")

    snow_num = pd.Series(0, index=df.index, dtype="int8")
    if snow_cols:
        s = df[snow_cols[0]]
        if s.dtype.kind in "biufc":
            snow_num = (s.fillna(0) > 0).astype("int8")

    df["precip_day"] = ((precip_num == 1) | (rain_num == 1) | (snow_num == 1)).astype("int8")

    # ---- trim to collisions date window if available
    if COLLISIONS_CLEAN.exists():
        c = pd.read_csv(COLLISIONS_CLEAN, usecols=["date"])
        c["date"] = to_date(c["date"])
        cmin, cmax = min(c["date"]), max(c["date"])
        before = len(df)
        df = df[(df["date"] >= cmin) & (df["date"] <= cmax)]
        after = len(df)
        print(f"Aligned to collisions window: {cmin} → {cmax} (kept {after:,}/{before:,})")
    else:
        cmin = df["date"].min(); cmax = df["date"].max()
        print(f"No collisions file found; keeping full weather range {cmin} → {cmax}")

    # ---- select tidy columns to save (keep what exists)
    preferred = [
        "date", "precip_day",
        "precipitation", "rain", "snow", "snow_on_ground",
        "max_temperature", "avg_temperature", "avg_hourly_temperature", "min_temperature",
        "max_humidex", "min_windchill",
        "max_wind_speed", "avg_hourly_wind_speed",
        "max_relative_humidity", "avg_relative_humidity"
    ]
    cols_to_save = [c for c in preferred if c in df.columns]
    if "date" not in cols_to_save:
        cols_to_save = ["date","precip_day"]

    out = df[cols_to_save].sort_values("date").reset_index(drop=True)

    # ---- write outputs
    out_csv = CLEAN_DIR / "weather_clean.csv"
    out_pq  = CLEAN_DIR / "weather_clean.parquet"
    out.to_csv(out_csv, index=False)
    try:
        out.to_parquet(out_pq, index=False)
    except Exception:
        pass

    print(f"✅ weather cleaned: {len(out):,} rows")
    print(f"   wrote: {out_csv}")
    print(out.head(8).to_string(index=False))

if __name__ == "__main__":
    main()
