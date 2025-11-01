# scripts/06_eda_plots.py
import os
import pandas as pd
import matplotlib.pyplot as plt

# folders 
os.makedirs("figures", exist_ok=True)
os.makedirs("maps", exist_ok=True)

# helpers 
def load_any(parquet_path: str, csv_path: str) -> pd.DataFrame:
    """Load parquet if present, else csv."""
    if os.path.exists(parquet_path):
        return pd.read_parquet(parquet_path)
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"Missing: {parquet_path} or {csv_path}")

def save_bar(series: pd.Series, title: str, fname: str, xlabel: str | None = None) -> None:
    ax = series.plot(kind="bar", rot=0)
    ax.set_title(title)
    if xlabel: ax.set_xlabel(xlabel)
    ax.set_ylabel("Count")
    plt.tight_layout()
    plt.savefig(f"figures/{fname}", dpi=160)
    plt.clf()

# load data -
df = load_any("data_model/collisions_enriched.parquet", "data_model/collisions_enriched.csv")

#1) bar charts 
if "severity" in df.columns:
    save_bar(df.groupby("severity").size(), "Collisions by Severity", "counts_by_severity.png")
if "hour" in df.columns:
    save_bar(df.groupby("hour").size(), "Collisions by Hour", "counts_by_hour.png", "Hour (0–23)")
if "month" in df.columns:
    save_bar(df.groupby("month").size(), "Collisions by Month", "counts_by_month.png")
if "dow" in df.columns:
    save_bar(df.groupby("dow").size(), "Collisions by Day of Week", "counts_by_dow.png")

# 2) Precip vs Dry comparison 
if "wx_precip_day" in df.columns and "severity" in df.columns:
    ct = (
        df.groupby(["severity", "wx_precip_day"], dropna=False)
          .size()
          .unstack(fill_value=0)
    )
    # ensure both categories exist
    for col in [0, 1]:
        if col not in ct.columns:
            ct[col] = 0
    ct = ct[[0, 1]].rename(columns={0: "Dry", 1: "Precip"})
    ax = ct.plot(kind="bar", rot=0)
    ax.set_title("Severity counts: Precip vs Dry")
    ax.set_ylabel("Count")
    plt.tight_layout()
    plt.savefig("figures/severity_precip_vs_dry.png", dpi=160)
    plt.clf()

#3) Collisions heatmap
try:
    import folium
    from folium.plugins import HeatMap

    center = [43.6532, -79.3832]  # Toronto
    m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

    if {"lat", "lon"}.issubset(df.columns):
        pts = df[["lat", "lon"]].dropna().values.tolist()
        # thin for responsiveness on huge datasets
        if len(pts) > 200_000:
            pts = pts[::10]
        HeatMap(pts, radius=8, blur=12, max_zoom=15).add_to(m)
        m.save("maps/collisions_heatmap.html")
    else:
        # create a placeholder map if lat/lon missing
        folium.Map(location=center, zoom_start=11).save("maps/collisions_heatmap.html")
except Exception as e:
    print("Skipping collisions heatmap:", e)

#  4) Speed-camera point map 
try:
    import folium

    cams = load_any("data_clean/speed_cameras_clean.parquet", "data_clean/speed_cameras_clean.csv")
    mc = folium.Map(location=[43.6532, -79.3832], zoom_start=11, tiles="CartoDB positron")
    if {"lat", "lon"}.issubset(cams.columns):
        for _, r in cams.dropna(subset=["lat", "lon"]).iterrows():
            folium.CircleMarker(
                location=[r["lat"], r["lon"]],
                radius=4, fill=True, weight=0.5,
                popup=str(r.get("Status", r.get("status_clean", "camera")))
            ).add_to(mc)
    mc.save("maps/speed_cameras.html")
except Exception as e:
    print("Skipping camera map:", e)

print("Wrote outputs to figures/ and maps/")
