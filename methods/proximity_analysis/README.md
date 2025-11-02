## **Proximity Analysis – Speed Cameras vs Collisions (Toronto)**

Analyzes the spatial proximity of traffic collisions to Automated Speed Enforcement (ASE) camera locations in Toronto. The objective is to determine whether collisions are more likely to occur near or far from existing cameras and to provide insights into potential coverage gaps.

---

### **Files Included**
- **`Proximity_Analysis.ipynb`** — Jupyter Notebook performing the proximity analysis, calculating distances from each collision to the nearest speed camera, categorizing collisions by distance, and visualizing results.  
- **`collisions_by_distance.png`** — Visualization showing the distribution of collisions by proximity to the nearest camera.  

---

### **Analysis**
1. **Calculate Nearest Camera Distances**  
   - Uses the **Haversine formula** to compute distance in meters between each collision and all speed cameras.  
   - Determines the minimum distance for each collision to its closest camera.  

2. **Categorize Collisions by Distance**  
   - Collisions within **500 meters** of a camera are labeled `Near`.  
   - Collisions farther than 500 meters are labeled `Far`.  

3. **Visualization**  
   - Generates a **count plot** showing the number of collisions near vs far from cameras.  

---

### **Key Insights**
- Collisions are **distributed unevenly** relative to camera locations.  
- A significant number of collisions occur **beyond 500 meters** from the nearest camera, suggesting potential areas for additional ASE deployment.  
