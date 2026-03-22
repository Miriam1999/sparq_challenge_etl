# UK Road Safety — Data Assignment
Analysis of UK road accident data (2015–2019) using a medallion architecture built in Snowflake, with a Power BI dashboard as the final deliverable.

## Objective
Identify the conditions that correlate with fatal road accident outcomes in the UK as part of the Sparq data assignment.

## What's in this repository
```
├── bronze/          SQL scripts to load raw CSV data into Snowflake Bronze layer
├── silver/          SQL scripts for type casting, cleaning, and quality validation
├── gold/            SQL scripts for pre-aggregated tables powering the dashboard
├── eda/             Python EDA notebook (Snowflake Snowpark) covering data quality,
│                    referential integrity, temporal distribution, and risk factor analysis
└── docs/            Final Power BI dashboard (.pbix) and supporting process reports
```

## Stack
- **Snowflake** — data warehouse, all ETL layers (Bronze → Silver → Gold)
- **Python / Snowpark** — exploratory data analysis
- **Power BI** — dashboard and visualization
- **STATS19** — UK road accident dataset, 2015–2019

## Dashboard narrative
The dashboard is structured around a single question: **what kills on UK roads?**
- **Page 1 — How bad is it?** Scale, trend, and severity split across the full dataset
- **Page 2 — What kills?** Severity broken down by speed limit, light conditions, road type, urban/rural area, and driver age band
