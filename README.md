# meteorology-portfolio
Portfolio of meteorology, data science, and geospatial analytics projects — featuring Python pipelines, forecast verification tools, and geospatial visualization solutions for research, energy, and transportation applications.

# David Levin – Meteorology & Data Science Portfolio

Welcome to my portfolio!  
I’m a **meteorologist and data scientist** with expertise in **Python**, **cloud-based workflows**, and **geospatial analysis**. Over the past decade with the National Weather Service, I’ve developed scalable tools for automating weather data pipelines, building verification systems, and delivering actionable insights through dashboards and visualizations.

This repository showcases **sanitized examples** of my work for clients, research, and operational forecasting.

---

## **Featured Projects**

### **1. Automated Forecast Archive**
- **Tech stack:** Python, AWS S3, Pandas, Parquet, fsspec  
- Developed a **parallelized Python pipeline** to archive **NDFD**, **HRRR**, **RRFS** and **NBM** forecasts.  
- Uses partitioned Parquet files for efficient storage and query-ready datasets.  
- **Demo:** [https://github.com/david-levin11/meteorology-portfolio/tree/main/alaska_verification_portfolio]  
- **Highlights:**  
  - 70% faster processing  
  - Scalable and cloud-friendly design  
  - Reusable class structure for multiple data sources

---

### **2. Forecast Verification Tool**
- **Tech stack:** Python, DuckDB, Pandas, Matplotlib, Seaborn  
- Modular verification system to analyze model accuracy, generate **bias/confusion matrices**, and visualize category-based performance (e.g., Beaufort bins).  
- **Demo:** [Link to notebook or repo directory]  
- **Highlights:**  
  - Flexible binning schemes  
  - Clean, publication-ready visualizations  
  - Supports large datasets with low memory footprint

---

### **3. Geospatial Snowfall Mapping**
- **Tech stack:** Arcpy, GeoPandas, Python
- Designed **automated geospatial snowfall analysis** to visualize spatial extend of sparse observations using Empirical Bayesian Krieging Regression Analysis to smartly spread out snowfall observations based on climatology.  
- **Demo:** [Screenshot or hosted link]  
- **Highlights:**  
  - Used to verify National Weather Service warning products and for public dissemenation post major events.  
  - Modular design adaptable to multiple regions.

---

## **Skills & Tools**
- **Languages:** Python, SQL, Bash  
- **Data:** GRIB, NetCDF, Parquet, REST APIs  
- **Libraries:** Pandas, xarray, DuckDB, fsspec, Matplotlib, Seaborn, Plotly  
- **Cloud & Storage:** AWS S3, GitHub Actions, Docker  
- **GIS Tools:** ArcGIS, GeoPandas, Shapely

---

## **How to Explore**
Each project folder contains:
- A `README.md` with project description and instructions  
- Example scripts or Jupyter notebooks (with dummy datasets when needed)  
- Visuals or screenshots where applicable  

---

## **Contact**
📧 **david.levin0812@gmail.com**  
🔗 **[LinkedIn](www.linkedin.com/in/david-levin-b85b8651)**  


---

*Note: This repository contains sanitized code and examples for demonstration purposes only. Some production code and datasets cannot be shared due to licensing and operational restrictions.*
