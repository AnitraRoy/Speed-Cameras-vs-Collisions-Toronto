# scripts/05_eda_summaries.py
from pathlib import Path
import pandas as pd
import numpy as np

MODEL = Path("data_model")
EDA   = Path("data_eda"); EDA.mkdir(parents=True, exist_ok=True)

# helpers
def load_merged() -> pd.DataFrame:
    pq  = MODEL / "collisions_enriched.parquet"
    csv = MODEL / "collisions_enriched.csv"
    if pq.exists():
        return pd.read_parquet(pq)
    if csv.exists():
        return pd.read_csv(csv)
    raise FileNotFoundError("Missing data_model/collisions_enriched.(parquet|csv)")

def ensure_time_cols(df: pd.DataFrame) -> pd.DataFrame:
    # date → pandas datetime
    if "date" in df.columns:
        dts = pd.to_datetime(df["date"], errors="coerce")
    else:
        dts = pd.NaT

    if "hour" not in df.columns:
        df["hour"] = pd.to_numeric(df.get("hour", np.nan), errors="coerce")

    # month (1..12), year, year_month (YYYY-MM), dow (ordered)
    if "month" not in df.columns:
        df["month"] = pd.to_datetime(dts).dt.month
    if "year" not in df.columns:
        df["year"]  = pd.to_datetime(dts).dt.year
    if "year_month" not in df.columns:
        df["year_month"] = pd.to_datetime(dts).dt.to_period("M").astype(str)

    if "dow" not in df.columns:
        df["dow"] = pd.to_datetime(dts).dt.day_name()

    # Order DOW for nicer charts later
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    df["dow"] = pd.Categorical(df["dow"], categories=dow_order, ordered=True)

    return df

def safe_groupcount(df, by):
    # robust groupby count (works even if keys contain NaNs)
    return (
        df.groupby(by, dropna=False)
          .size().rename("count")
          .reset_index()
    )

def to_csv(df, name):
    out = EDA / name
    df.to_csv(out, index=False)
    return out

# load & prepare
df = load_merged()
df = ensure_time_cols(df)

# bool/int versions for flags if present
if "wx_precip_day" in df.columns:
    df["wx_precip_day"] = pd.to_numeric(df["wx_precip_day"], errors="coerce").fillna(0).astype("int8")
if "cam_within_250m" in df.columns:
    df["cam_within_250m"] = pd.to_numeric(df["cam_within_250m"], errors="coerce").fillna(0).astype("int8")

# 1) Basic counts -
by_severity = safe_groupcount(df, ["severity"]).sort_values("count", ascending=False)
by_hour     = safe_groupcount(df, ["hour"]).sort_values("hour", ascending=True)
by_month    = safe_groupcount(df, ["month"]).sort_values("month", ascending=True)
by_dow      = safe_groupcount(df, ["dow"]).sort_values("dow", ascending=True)
by_year     = safe_groupcount(df, ["year"]).sort_values("year", ascending=True)
by_ym       = safe_groupcount(df, ["year_month"]).sort_values("year_month", ascending=True)

# - 2) Severity × precipitation
if "wx_precip_day" in df.columns:
    sev_precip = (
        df.groupby(["severity","wx_precip_day"], dropna=False)
          .size().unstack(fill_value=0)
          .rename(columns={0:"dry", 1:"precip"})
          .reset_index()
    )
else:
    sev_precip = pd.DataFrame(columns=["severity","dry","precip"])

#  3) Hour × precip; Month × precip 
if "wx_precip_day" in df.columns:
    hour_precip = (
        df.groupby(["hour","wx_precip_day"], dropna=False)
          .size().unstack(fill_value=0)
          .rename(columns={0:"dry", 1:"precip"})
          .reset_index()
          .sort_values("hour")
    )
    month_precip = (
        df.groupby(["year_month","wx_precip_day"], dropna=False)
          .size().unstack(fill_value=0)
          .rename(columns={0:"dry", 1:"precip"})
          .reset_index()
          .sort_values("year_month")
    )
else:
    hour_precip = pd.DataFrame(columns=["hour","dry","precip"])
    month_precip = pd.DataFrame(columns=["year_month","dry","precip"])

#  4) Camera proximity summaries 
if "cam_within_250m" in df.columns:
    cam_counts = safe_groupcount(df, ["cam_within_250m"]).sort_values("cam_within_250m")
    # cam × precip (how many near cameras on wet vs dry)
    if "wx_precip_day" in df.columns:
        cam_precip = (
            df.groupby(["cam_within_250m","wx_precip_day"], dropna=False)
              .size().unstack(fill_value=0)
              .rename(columns={0:"dry", 1:"precip"})
              .reset_index()
              .sort_values("cam_within_250m")
        )
    else:
        cam_precip = pd.DataFrame(columns=["cam_within_250m","dry","precip"])
else:
    cam_counts = pd.DataFrame(columns=["cam_within_250m","count"])
    cam_precip = pd.DataFrame(columns=["cam_within_250m","dry","precip"])

# -5) Weather numeric summaries (prefixed wx_) 
num_cols = [c for c in df.columns if c.startswith("wx_")]
if num_cols:
    num_summary = (
        df[num_cols].apply(pd.to_numeric, errors="coerce")
          .describe(percentiles=[0.05,0.25,0.5,0.75,0.95]).T
          .reset_index().rename(columns={"index":"metric"})
    )
else:
    num_summary = pd.DataFrame(columns=["metric"])

# -6) Injury-rate lift on precip days (quick KPI) 
def compute_injury_lift(frame: pd.DataFrame) -> pd.DataFrame:
    if "severity" not in frame.columns or "wx_precip_day" not in frame.columns:
        return pd.DataFrame(columns=["wx_precip_day","n","injury_rate","lift_vs_dry_pct"])
    g = frame.assign(is_injury=(frame["severity"].astype(str) == "Injury")) \
             .groupby("wx_precip_day", dropna=False)
    out = g.agg(n=("is_injury","size"),
                injury_sum=("is_injury","sum")).reset_index()
    out["injury_rate"] = out["injury_sum"] / out["n"].replace(0, np.nan)
    dry_rate = float(out.loc[out["wx_precip_day"] == 0, "injury_rate"]) if 0 in out["wx_precip_day"].values else np.nan
    out["lift_vs_dry_pct"] = (out["injury_rate"] / dry_rate - 1.0) * 100.0 if pd.notna(dry_rate) else np.nan
    return out[["wx_precip_day","n","injury_rate","lift_vs_dry_pct"]]

injury_lift = compute_injury_lift(df)

# write all outputs 
paths = []
paths += [to_csv(by_severity, "counts_by_severity.csv")]
paths += [to_csv(by_hour,     "counts_by_hour.csv")]
paths += [to_csv(by_month,    "counts_by_month.csv")]
paths += [to_csv(by_dow,      "counts_by_dow.csv")]
paths += [to_csv(by_year,     "counts_by_year.csv")]
paths += [to_csv(by_ym,       "counts_by_year_month.csv")]
if not sev_precip.empty:  paths += [to_csv(sev_precip,   "severity_by_precip.csv")]
if not hour_precip.empty: paths += [to_csv(hour_precip,  "hour_by_precip.csv")]
if not month_precip.empty:paths += [to_csv(month_precip, "year_month_by_precip.csv")]
if not cam_counts.empty:  paths += [to_csv(cam_counts,   "camera_within_250m_counts.csv")]
if not cam_precip.empty:  paths += [to_csv(cam_precip,   "camera_within_250m_by_precip.csv")]
if not num_summary.empty: paths += [to_csv(num_summary,  "weather_numeric_summary.csv")]
if not injury_lift.empty: paths += [to_csv(injury_lift,  "injury_rate_lift_precip.csv")]

print("Saved EDA summary tables in data_eda/")
for p in paths:
    print(" -", p.name)
