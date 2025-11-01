# scripts/02_clean_weather.py
import pandas as pd
from pathlib import Path
from _utils import parse_date  # uses the helper in scripts/_utils.py

RAW = Path("data_raw")
OUT = Path("data_clean"); OUT.mkdir(parents=True, exist_ok=True)

infile = RAW / "weather.csv"

# Load & normalize column names 
df = pd.read_csv(infile, low_memory=False)
df.columns = [c.strip().lower() for c in df.columns]

# Keep a focused set of columns if present 
wanted = [
    "date",
    "precipitation", "rain", "snow", "snow_on_ground",
    "max_temperature", "min_temperature", "avg_temperature",
    "max_wind_speed", "avg_hourly_wind_speed",
]
keep = [c for c in wanted if c in df.columns]
df_small = df[keep].copy()

# Parse date 
if "date" not in df_small.columns:
    raise ValueError("Weather file must contain a 'date' column.")
df_small["date"] = parse_date(df_small["date"])

#  Coerce numerics for all non-date columns 
num_cols = [c for c in df_small.columns if c != "date"]
for c in num_cols:
    df_small[c] = pd.to_numeric(df_small[c], errors="coerce")

# Precip flag: ONLY total precipitation (> 0)
if "precipitation" not in df_small.columns:
    raise ValueError("Weather file must contain 'precipitation' for precip_day.")
df_small["precipitation"] = df_small["precipitation"].fillna(0)
df_small["precip_day"] = (df_small["precipitation"] > 0).astype("int8")

# Save cleaned daily weather 
outpath = OUT / "weather_clean.parquet"
df_small.to_parquet(outpath, index=False)
print(f"Saved {len(df_small):,} rows -> {outpath}")
