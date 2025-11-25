# Spatial and Statistical Analysis of Traffic Collisions and Speed Camera Coverage in Toronto

An analysis of traffic collision patterns and speed camera placement in Toronto to evaluate the effectiveness of automated speed enforcement (ASE) in reducing collision frequency and severity.

## Project Objective

This project investigates whether Toronto's speed cameras are effectively located to improve road safety. Specifically, it aims to answer:

- Are traffic collisions less severe near active speed cameras?
- Do high-collision areas have sufficient speed camera coverage?
- Where should new speed cameras be placed to maximize their impact on traffic safety?

## Data Sources

- **Toronto Open Data Portal**
  - [Police Annual Statistical Report - Traffic Collisions](https://open.toronto.ca/dataset/police-annual-statistical-report-traffic-collisions/)
  - [Automated Speed Enforcement Locations](https://open.toronto.ca/dataset/automated-speed-enforcement-locations/)
  
- **Kaggle**
  - [Toronto Daily Weather Data (1937–2025)](https://www.kaggle.com/datasets/aliranjipour/toronto-weather-stats-nov-1937-jul-2025)

## Methods

The analysis follows a structured, data-driven approach consisting of the following key methods:

1. Exploratory Data Analysis (EDA)  
2. Proximity Analysis  
3. Statistical Testing (Chi-squared, t-tests)  
4. Spatial Clustering (K-Means) and Coverage Gap Analysis (DBSCAN)  
5. Optimization Model to Recommend New Locations
