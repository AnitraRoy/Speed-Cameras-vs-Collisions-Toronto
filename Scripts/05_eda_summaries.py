# scripts/05_eda_summaries.py
import pandas as pd
from pathlib import Path

MODEL = Path("data_model")
EDA = Path("data_eda"); EDA.mkdir(parents=True, exist_ok=True)

# Load merged (parquet preferred, csv fallback)
df = None
pq = MODEL / "collisions_enriched.parquet"
csv = MODEL / "collisions_enriched.csv"
if pq.exists():
    df = pd.read_parquet(pq)
elif csv.exists():
    df = pd.read_csv(csv)
else:
    raise FileNotFoundError("Missing data_model/collisions_enriched.(parquet|csv)")

#  Ensure date/time helper columns exist 
if "date" in df.columns:
    dts = pd.to_datetime(df["date"], errors="coerce")
    if "month" not in df.columns:
        df["month"] = dts.dt.month
    if "dow" not in df.columns:
        df["dow"] = dts.dt.day_name()
else:
    # If no date, make placeholders (prevents KeyErrors)
    if "month" not in df.columns: df["month"] = pd.NA
    if "dow" not in df.columns:   df["dow"] = pd.NA

# 1) Basic counts 
by_severity = (
    df.groupby("severity", dropna=False)
      .size().rename("count")
      .reset_index()
      .sort_values("count", ascending=False)
)

by_hour = (
    df.groupby("hour", dropna=False)
      .size().rename("count")
      .reset_index()
      .sort_values("hour", ascending=True)
)

by_month = (
    df.groupby("month", dropna=False)
      .size().rename("count")
      .reset_index()
      .sort_values("month", ascending=True)
)

by_dow = (
    df.groupby("dow", dropna=False)
      .size().rename("count")
      .reset_index()
)

# 2) Severity vs precipitation (robust)
if "wx_precip_day" in df.columns:
    sev_precip = (
        df.groupby(["severity", "wx_precip_day"], dropna=False)
          .size().unstack(fill_value=0)
    )
    # ensure both 0 and 1 exist
    for col in [0, 1]:
        if col not in sev_precip.columns:
            sev_precip[col] = 0
    sev_precip = sev_precip[[0, 1]].reset_index()
    sev_precip.columns = ["severity", "dry", "precip"]
else:
    sev_precip = pd.DataFrame(columns=["severity", "dry", "precip"])

# 3) Weather numeric summaries (optional)
num_cols = [c for c in df.columns if c.startswith("wx_")]
num_summary = (
    df[num_cols].apply(pd.to_numeric, errors="coerce")
      .describe(include="all").T
    if num_cols else pd.DataFrame()
)

# Save CSVs 
by_severity.to_csv(EDA / "counts_by_severity.csv", index=False)
by_hour.to_csv(EDA / "counts_by_hour.csv", index=False)
by_month.to_csv(EDA / "counts_by_month.csv", index=False)
by_dow.to_csv(EDA / "counts_by_dow.csv", index=False)
if not sev_precip.empty:
    sev_precip.to_csv(EDA / "severity_by_precip.csv", index=False)
if not num_summary.empty:
    num_summary.to_csv(EDA / "weather_numeric_summary.csv")

print("Saved EDA summary tables in data_eda/")
