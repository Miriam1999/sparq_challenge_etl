# UK Road Safety — Data Assignment
Analysis of UK road accident data (2015–2019) using a medallion architecture built in Snowflake, with a Power BI dashboard as the final deliverable.

## Objective
Identify the conditions that correlate with fatal road accident outcomes in the UK as part of the Sparq data assignment.

## What's in this repository
```
├── dags/sql/bronze_job.sql          SQL scripts to load raw CSV data into Snowflake Bronze layer
├── dags/sql/silver_job.sql          SQL scripts for type casting, cleaning, and quality validation
├── dags/sql/gold_job.sql            SQL scripts for pre-aggregated tables powering the dashboard
│                    
└── docs/            Final Power BI dashboard (.pbix) and supporting process reports, as well as EDA.
```

## Stack
- **Snowflake** — data warehouse, all ETL layers (Bronze → Silver → Gold)
- **SQL** - ETL layers files
- **Python** — exploratory data analysis
- **Power BI** — dashboard and visualization
- **Airflow** - data orchestration

## Dashboard narrative
The dashboard is structured around a single question: **what kills on UK roads?**
- **Page 1 — How bad is it?** Scale, trend, and severity split across the full dataset
- **Page 2 — What kills?** Severity broken down by speed limit, light conditions, road type, urban/rural area, and driver age band
