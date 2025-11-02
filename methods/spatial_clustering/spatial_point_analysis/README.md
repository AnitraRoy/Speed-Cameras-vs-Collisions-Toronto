## **Spatial Point Analysis – Collision Clusters & Speed Camera Coverage (Toronto)**

Uses spatial analysis and DBSCAN clustering to study the relationship between traffic collisions and Automated Speed Enforcement (ASE) camera locations across Toronto. The objective is to identify collision hotspots, evaluate camera coverage, and highlight areas that may benefit from additional enforcement.

---

### **Files Included**
- **`Spatial Point Analysis.ipynb`** — Jupyter Notebook containing spatial data processing, DBSCAN clustering, buffer creation, and interactive visualizations.  
- **`camera_coverage_map.html`** — Interactive Folium map showing collision clusters, camera locations, and buffer zones for enforcement coverage.

---

### **Key Insights**
- **Collision clusters identified via DBSCAN** reveal hotspots that are either well-covered or under-covered by speed cameras.  
- **Buffer analysis (500m radius)** indicates which collisions fall within camera coverage and which occur outside enforcement zones.  
- **Distance-based color coding** allows quick identification of collisions by proximity to cameras:  
  - **Red:** within 250m of a camera  
  - **Yellow:** 250–500m from a camera  
  - **Green:** beyond 500m from a camera  
- Sampling (15,000 collisions) is used for visualization only, ensuring the map remains responsive without affecting the underlying analysis.
