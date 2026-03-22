# SPARQ Challenge — ETL architecture overview

## Introduction

This ETL ingests data from files staged in Snowflake into layered tables in a single database (`SPARQ_CHALLENGE`), with curated outputs in Gold for reporting (**Power BI**). **Apache Airflow** orchestrates the pipeline; **SQL** executed in Snowflake implements the Bronze, Silver, and Gold layers.

---

## Tech stack

| Component | Role |
|-----------|------|
| **Docker Compose** | Local runtime: Airflow **scheduler**, **web UI**, and **PostgreSQL** (Airflow metadata only). |
| **Apache Airflow** | Schedules tasks; DAGs under `dags/`, SQL under `dags/sql/`. |
| **Snowflake** | **Warehouse**, **database**, and **schemas** per layer; **internal stage** for source files; **COPY** for loads. |
| **SQL** | Layer logic in `.sql` files, invoked via `SnowflakeOperator`. |

---

## Architecture

This ETL was developed using the **Medallion architecture** (Bronze → Silver → Gold) within **one Snowflake database** and **separate schemas** (`BRONZE`, `SILVER`, `GOLD`).

- **Bronze** — **Landing / raw**: ingest from staged CSVs.
- **Silver** — **Conformed facts and quality**: typing, dimension lookups, deduplication where required, and validation before downstream use. Reference dimensions are maintained in Silver.  
- **Gold** — **Analytics**: aggregates, KPIs, and consumption datasets for dashboards and BI.

---

## Assumptions

- Source files are delivered to a Snowflake **internal stage**; Bronze loads use **path patterns** (regex) rather than enumerating every file.  
- Pattern definitions align with the **current** folder and file layout; new paths require **SQL or pattern updates**.  
- **One database, multiple schemas**; SQL uses explicit schema qualification or `USE SCHEMA`. A **single** Airflow Snowflake connection is used; **access control** is enforced in Snowflake (roles, grants). 
- Airflow in Docker targets **local development and demonstration**; production would use hardened images, secret management, and/or managed orchestration.

---

## Future improvements

- **Landing conventions**: stable prefixes per domain (e.g. `landing/accidents/`) to reduce pattern addings or changes.  
- **External stage + object storage** (S3 / Azure / GCS) with optional **event-driven** loads for scale and multi-source ingestion.  
- **Continuous ingest** (Snowpipe in Snowflake) for high-volume or frequent small files.  
- **Operations**: secrets outside source control, non-default credentials, monitoring (runtime, credits), idempotent and incremental load strategies where appropriate.  

## Evidence of running:
#### Airflow:
![image](images/DAG.PNG)

#### SnowFlake Database and Schemas:
![image](images/SF.PNG)