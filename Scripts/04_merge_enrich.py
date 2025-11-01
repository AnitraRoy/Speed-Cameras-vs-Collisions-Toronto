# scripts/04_merge_enrich.py
from pathlib import Path
import pandas as pd

CLEAN = Path("data_clean")
MODEL = Path("data_model"); MODEL.mkdir(parents=True, exist_ok=True)

def load_any(pq: Path, csv: Path) -> pd.DataFrame:
    if pq.exists():  return pd.read_parquet(pq)
    if csv.exists(): return pd.read_csv(csv)
    raise FileNotFoundError(f"Missing input: {pq} or {csv}")

def to_date(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"Expected column '{col}' for merging.")
    return df.assign(**{col: pd.to_datetime(df[col], errors="coerce").dt.date})

# load
collisions = load_any(CLEAN/"collisions_clean.parquet", CLEAN/"collisions_clean.csv")
weather    = load_any(CLEAN/"weather_clean.parquet",    CLEAN/"weather_clean.csv")

# normalize dates
collisions = to_date(collisions, "date")
weather    = to_date(weather,    "date")

# authoritative precip_day in weather
if "precip_day" not in weather.columns:
    for col in ["precipitation", "rain", "snow"]:
        if col not in weather.columns:
            weather[col] = 0
        weather[col] = pd.to_numeric(weather[col], errors="coerce").fillna(0)
    weather["precip_day"] = (weather["precipitation"] > 0).astype("int8")

# build map: date -> 0/1
flag_by_date = dict(zip(weather["date"], weather["precip_day"]))

# prefix weather cols (context)
wx_cols = [c for c in weather.columns if c != "date"]
weather_prefixed = weather.rename(columns={c: f"wx_{c}" for c in wx_cols})

# merge
df = collisions.merge(weather_prefixed, on="date", how="left", validate="m:1")

# set wx_precip_day from the map (this is the *only* source)
df["wx_precip_day"] = df["date"].map(flag_by_date).fillna(0).astype("int8")

# optional: convenience amount column
amount_cols = [c for c in ["wx_precipitation", "wx_rain", "wx_snow"] if c in df.columns]
for c in amount_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
df["wx_precip_amount_any"] = df[amount_cols].max(axis=1) if amount_cols else 0.0

# prints
matched = df["wx_precip_day"].notna().sum()
print(f"Collisions: {len(collisions):,} | Weather days: {weather['date'].nunique():,} | Rows with matched weather: {matched:,}")
print("wx_precip_day counts:\n", df["wx_precip_day"].value_counts(dropna=False))

# save
out = MODEL / "collisions_enriched.parquet"
df.to_parquet(out, index=False)
print(f"Saved {len(df):,} rows -> {out}")

