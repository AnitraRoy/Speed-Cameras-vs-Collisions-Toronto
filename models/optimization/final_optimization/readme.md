# Optimization Model

The optimization problem was formulated as a **maximum-coverage location model**, where the objective is to select camera locations that maximize the share of severity-weighted collision risk covered across the city. Each candidate site represents a feasible camera location generated from clustering analyses (K-means and DBSCAN), major road intersections, and high-density collision zones. Collision events were assigned weights based on injury severity, ensuring that the model prioritizes areas with high concentrations of serious and fatal collisions.

A camera’s coverage was modeled using a fixed-radius buffer, within which collisions are considered “covered.” The optimization selects the subset of candidate sites that maximizes the total covered collision risk while minimizing overlap between adjacent cameras.

By relocating the existing 150 cameras to the optimized sites, the model increases the share of severity-weighted collision risk captured from the baseline **27%** to **68%**, representing a **41% improvement** in network effectiveness. The optimized configuration substantially enhances coverage in downtown Toronto and other high-risk corridors while reducing redundancy in areas currently overserved by overlapping cameras.
