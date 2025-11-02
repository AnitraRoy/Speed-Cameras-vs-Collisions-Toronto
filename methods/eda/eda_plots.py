# scripts/06_eda_plots.py
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import folium
from folium.plugins import HeatMap, MarkerCluster

MODEL   = Path("data_model")
CLEAN   = Path("data_clean")
FIGURES = Path("figures"); FIGURES.mkdir(parents=True, exist_ok=True)
MAPS    = Path("maps");    MAPS.mkdir(parents=True, exist_ok=True)

def load_any(pq: Path, csv: Path) -> pd.DataFrame:
    if pq.exists():
        return pd.read_parquet(pq)
    if csv.exists():
        return pd.read_csv(csv)
    raise FileNotFoundError(f"Missing input: {pq} or {csv}")

def ensure_time(df: pd.DataFrame) -> pd.DataFrame:
    dts = pd.to_datetime(df["date"], errors="coerce") if "date" in df.columns else pd.NaT
    if "hour" not in df.columns:
        df["hour"] = pd.to_numeric(df.get("hour", np.nan), errors="coerce")
    if "dow" not in df.columns:
        df["dow"] = pd.to_datetime(dts).dt.day_name()
    if "year_month" not in df.columns:
        df["year_month"] = pd.to_datetime(dts).dt.to_period("M").astype(str)
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    df["dow"] = pd.Categorical(df["dow"], categories=dow_order, ordered=True)
    return df

def safe_counts(df, by):
    return (df.groupby(by, dropna=False)
              .size().rename("count")
              .reset_index())

def plot_save(ax, title, outpath):
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f" - wrote {outpath}")

def main():
    #load
    df = load_any(MODEL/"collisions_enriched.parquet", MODEL/"collisions_enriched.csv")
    df = ensure_time(df)

    # flags
    if "wx_precip_day" in df.columns:
        df["wx_precip_day"] = pd.to_numeric(df["wx_precip_day"], errors="coerce").fillna(0).astype("int8")
    if "cam_within_250m" in df.columns:
        df["cam_within_250m"] = pd.to_numeric(df["cam_within_250m"], errors="coerce").fillna(0).astype("int8")

    #FIGURES
    print("Creating figures/ ...")

    # 1) Hourly counts split by precip (line chart)
    if "wx_precip_day" in df.columns and "hour" in df.columns:
        hp = (df.groupby(["hour","wx_precip_day"])
                .size().unstack(fill_value=0)
                .rename(columns={0:"dry", 1:"precip"})
                .reset_index()
                .sort_values("hour"))
        fig, ax = plt.subplots(figsize=(10,5))
        ax.plot(hp["hour"], hp["dry"],   label="Dry")
        ax.plot(hp["hour"], hp["precip"],label="Precip")
        ax.set_xlabel("Hour of day")
        ax.set_ylabel("Collisions")
        ax.legend()
        plot_save(ax, "Collisions by Hour (Dry vs Precip)", FIGURES/"hour_by_precip.png")

    # 2) Day-of-week bar chart
    if "dow" in df.columns:
        dowc = safe_counts(df, ["dow"]).sort_values("dow")
        fig, ax = plt.subplots(figsize=(9,5))
        ax.bar(dowc["dow"].astype(str), dowc["count"])
        ax.set_xlabel("Day of Week")
        ax.set_ylabel("Collisions")
        plot_save(ax, "Collisions by Day of Week", FIGURES/"dow_counts.png")

    # 3) Year-Month line (seasonality over time) — cleaner x-axis
    if "year_month" in df.columns:
        ymc = safe_counts(df, ["year_month"]).copy()
        ymc["ym_date"] = pd.to_datetime(ymc["year_month"] + "-01", errors="coerce")
        ymc = ymc.sort_values("ym_date")
        fig, ax = plt.subplots(figsize=(12,5))
        ax.plot(ymc["ym_date"], ymc["count"])
        ax.set_xlabel("Year–Month")
        ax.set_ylabel("Collisions")
        # Major ticks yearly; minor ticks quarterly
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1,4,7,10]))
        plot_save(ax, "Collisions Over Time (Year–Month)", FIGURES/"year_month_counts.png")

    # 4) Camera distance histogram + near flag breakdown
    if "cam_nearest_m" in df.columns:
        d = pd.to_numeric(df["cam_nearest_m"], errors="coerce").dropna()
        d = d[d < 2000]  # clip for readability
        if len(d) > 0:
            fig, ax = plt.subplots(figsize=(9,5))
            ax.hist(d, bins=40)
            ax.set_xlabel("Nearest camera distance (m) [< 2km]")
            ax.set_ylabel("Collision count")
            plot_save(ax, "Nearest Speed Camera Distance (Histogram)", FIGURES/"camera_distance_hist.png")
    if "cam_within_250m" in df.columns:
        cc = safe_counts(df, ["cam_within_250m"]).sort_values("cam_within_250m")
        fig, ax = plt.subplots(figsize=(6,4))
        labels = cc["cam_within_250m"].map({0:">250m or none",1:"≤250m"})
        ax.bar(labels.astype(str), cc["count"])
        ax.set_xlabel("Camera proximity")
        ax.set_ylabel("Collision count")
        plot_save(ax, "Collisions by Camera Proximity", FIGURES/"camera_within_250m_counts.png")

    # -------------- MAPS --------------
    print("Creating maps/ ...")

    lat = pd.to_numeric(df.get("lat"), errors="coerce")
    lon = pd.to_numeric(df.get("lon"), errors="coerce")
    latc = float(lat.dropna().mean()) if lat.notna().any() else 43.653
    lonc = float(lon.dropna().mean()) if lon.notna().any() else -79.383

    # Map A: collisions heatmap (sample to keep file small)
    try:
        sample = df[lat.notna() & lon.notna()][["lat","lon"]].sample(n=min(50000, len(df)), random_state=42)
        m = folium.Map(location=[latc, lonc], zoom_start=11, tiles="cartodbpositron")
        HeatMap(sample[["lat","lon"]].values.tolist(), radius=8, blur=10).add_to(m)
        m.save(str(MAPS/"collisions_heatmap.html"))
        print(" - wrote maps/collisions_heatmap.html")
    except Exception as e:
        print(f" ! skipped collisions heatmap: {e}")

    # Map B: speed cameras markers (if available)
    try:
        cams = load_any(CLEAN/"speed_cameras_clean.parquet", CLEAN/"speed_cameras_clean.csv")
        cams["lat"] = pd.to_numeric(cams["lat"], errors="coerce")
        cams["lon"] = pd.to_numeric(cams["lon"], errors="coerce")
        cams = cams[cams[["lat","lon"]].notna().all(axis=1)]
        mc = folium.Map(location=[latc, lonc], zoom_start=11, tiles="cartodbpositron")
        cluster = MarkerCluster().add_to(mc)
        label_cols = [c for c in ["location","status_clean","ward_num","fid"] if c in cams.columns]
        for _, r in cams.iterrows():
            popup = "<br>".join([f"<b>{c}:</b> {r[c]}" for c in label_cols]) if label_cols else "Speed camera"
            folium.Marker([r["lat"], r["lon"]], popup=popup).add_to(cluster)
        mc.save(str(MAPS/"speed_cameras.html"))
        print(" - wrote maps/speed_cameras.html")
    except Exception as e:
        print(f" ! skipped speed cameras map: {e}")

    # Map C: collisions on precip days (small random sample)
    try:
        if "wx_precip_day" in df.columns:
            samp = df[(df["wx_precip_day"]==1) & lat.notna() & lon.notna()].sample(n=min(5000, len(df)), random_state=1)
            mp = folium.Map(location=[latc, lonc], zoom_start=11, tiles="cartodbpositron")
            cluster = MarkerCluster().add_to(mp)
            for _, r in samp.iterrows():
                folium.CircleMarker([r["lat"], r["lon"]], radius=2, fill=True).add_to(cluster)
            mp.save(str(MAPS/"collisions_precip_sample.html"))
            print(" - wrote maps/collisions_precip_sample.html")
    except Exception as e:
        print(f" ! skipped precip sample map: {e}")

    print("Done. Check the figures/ and maps/ folders.")

if __name__ == "__main__":
    main()

# python -m http.server 8080
# then browse to http://localhost:8080/maps/
