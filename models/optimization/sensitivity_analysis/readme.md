## Sensitivity Analysis

To evaluate the robustness of the optimization model and understand how changes in the number of cameras and their coverage radii affect overall collision coverage, a three-scenario sensitivity analysis was conducted on top of the base case configuration. 

All scenarios modify either the number of ASE cameras, the radius of influence, or both, while keeping the candidate-site grid and severity-weighted collision dataset constant.

---

### **Base Case (Optimized Model) — 150 Cameras, 500-metre Radius**
- **Coverage Risk:** **68.07%**

This serves as the reference scenario for assessing performance changes in alternative configurations.

---

### **Case A — 150 Cameras, 250-metre Radius**
- **Coverage Risk:** **34.49%**

Reducing the radius by half while keeping the camera count constant cuts risk coverage by nearly **50%**, indicating the strong influence of radius size on model performance.

---

### **Case B — 250 Cameras, 500-metre Radius**
- **Coverage Risk:** **85.09%**

Increasing the camera count while maintaining the original radius yields substantial gains, improving coverage by **17 percentage points** over the base case and demonstrating diminishing returns as hotspots become saturated.

---

### **Case C — 250 Cameras, 250-metre Radius**
- **Coverage Risk:** **46.53%**

With a reduced radius but higher camera count, coverage improves relative to Case A but remains well below the 500-m configurations. Additional cameras only partially compensate for smaller radii.

---

### **Summary of Coverage Risk Across Scenarios**

| Scenario (K = Cameras, R = Radius) | Coverage Risk (%) |
|------------------------------------|-------------------|
| **K150_R500 (Base Case)**          | **68.07** |
| **K250_R500**                      | **85.09** |
| **K150_R250**                      | **34.49** |
| **K250_R250**                      | **46.53** |

---

Across all scenarios, the model consistently relocates cameras toward high-density collision hotspots. The sensitivity results confirm that:
- Larger radii improve coverage efficiency.
- More cameras increase coverage but yield diminishing returns.
- Small radii can still achieve strong coverage when supported by higher camera counts.
