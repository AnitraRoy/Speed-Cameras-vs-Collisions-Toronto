## **K-Means Clustering Analysis – Speed Cameras vs Collisions (Toronto)**

Uses K-Means to analyze the spatial relationship between traffic collisions and Automated Speed Enforcement (ASE) camera locations across Toronto. The objective is to identify collision hotspots, evaluate current enforcement coverage, and highlight areas that may benefit from additional camera placement.

---

### **Files Included**
- **`K_Clustering.ipynb`** — Jupyter Notebook containing the complete clustering analysis, data integration, and visualizations.  
- **`toronto_collision_hotspots.html`** — Interactive Folium map displaying collision clusters (hotspots) alongside existing speed camera locations.

---

### **Key Insights**
- The **downtown cluster (blue)** contains the **highest proportion of collisions (~32%)** and the **highest collisions-per-camera ratio (~4,700)**, suggesting that existing camera coverage is insufficient relative to collision density.  
- **Suburban clusters** show **lower collisions-per-camera ratios (~2,700–3,000)**, indicating more balanced enforcement relative to traffic incidents.  
- Across all clusters, collisions most frequently occurred between **1 PM and 2 PM**, aligning with mid-day traffic congestion.  
- Weather conditions (e.g., snow on ground and temperature) were consistent across clusters, confirming that environmental bias was minimal in this analysis.
