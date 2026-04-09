Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


# 🌍 WFP Food Prices Pipeline — Kenya

A end-to-end data engineering pipeline that extracts, transforms, and loads
WFP (World Food Programme) food price data for Kenya, orchestrated with Apache
Airflow, modelled with dbt, and visualized with Metabase and Grafana.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [dbt Models](#dbt-models)
- [Airflow DAG](#airflow-dag)
- [Visualization](#visualization)
- [Dashboard SQL Queries](#dashboard-sql-queries)
- [Ports Reference](#ports-reference)
- [Troubleshooting](#troubleshooting)

---

## Project Overview

This pipeline processes food market price data collected across Kenya by the
World Food Programme. It covers prices for commodities such as Maize, Beans,
Sugar, and Bread across multiple counties and markets from 2006 to present.

**Key Questions the Dashboard Answers:**
- How have food prices changed year over year across Kenyan counties?
- Which commodities have the highest and lowest price volatility?
- Which commodities are sold in the most markets?
- What are the monthly price trends for key food items?

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCES                        │
│         wfp_food_prices_ken.csv  (17,632 rows)          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                  APACHE AIRFLOW                         │
│                                                         │
│   extract ──► transform ──► load                        │
│   (CSV)       (Clean/Validate)  (PostgreSQL)            │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              POSTGRESQL (Local Windows)                 │
│              Database: wfp_foods                        │
│              Table: wfp_food_prices_clean               │
└───────────────┬─────────────────────┬───────────────────┘
                │                     │
                ▼                     ▼
┌──────────────────────┐   ┌─────────────────────────────┐
│       dbt            │   │     VISUALIZATION           │
│  Staging + Analytics │   │  Metabase  │  Grafana       │
│  Models & Views      │   │  :3001     │  :3000         │
└──────────────────────┘   └─────────────────────────────┘
```

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| **Apache Airflow** | 3.1.7 | Pipeline orchestration |
| **PostgreSQL** | 17 | Data storage |
| **dbt** | Latest | Data transformation & modelling |
| **Docker** | Latest | Containerisation |
| **Redis** | 7.2 | Celery message broker |
| **Metabase** | Latest | Business intelligence dashboards |
| **Python** | 3.11 | Scripting & DAG authoring |
| **pandas** | Latest | Data manipulation |
| **SQLAlchemy** | Latest | Database ORM |

---

## Project Structure

```
WFP_FOODS_PIPELINE/
│
├── dags/                               
│   ├── data/
│   │   └── wfp_food_prices_ken.csv
│   └── wfp_food_pipeline_dag.py
│
├── wfp_foods/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_food_prices.sql
│   │   └── marts/
│   │       └── analytics/
│   │           ├── average_price_by_county_and_year.sql
│   │           ├── average_price_per_commodity.sql
│   │           ├── commodities_sold_in_more_than_10_markets.sql
│   │           ├── highest_and_lowest_price_per_commodity.sql
│   │           └── monthly_average_prices.sql
│   ├── analyses/
│   ├── macros/
│   ├── seeds/
│   ├── snapshots/
│   ├── tests/
│   └── dbt_project.yml
│
├── metabase/
│   └── provisioning/
│       └── datasources/
│           └── datasource.yaml
│
├── logs/
├── plugins/
├── config/
├── dbt_venv/
├── .env
├── docker-compose.yaml
├── docker-compose.metabase.yaml
├── wfp_clean.ipynb
└── wfp_food_prices_ken.csv
```

---

## Prerequisites

Before starting make sure you have the following installed:

- **Docker Desktop** (Windows) — [Download](https://www.docker.com/products/docker-desktop)
- **VS Code** — [Download](https://code.visualstudio.com)
- **PostgreSQL 17** (local Windows install via pgAdmin) — [Download](https://www.postgresql.org/download/windows/)
- **Python 3.11** — [Download](https://www.python.org/downloads/)
- **dbt-core** + **dbt-postgres** — installed in `dbt_venv`

---

## Setup & Installation

### 1. Clone the Project
```powershell
git clone <your-repo-url>
cd WFP_FOODS_PIPELINE
```

### 2. Create the `.env` File
Create a `.env` file in the project root:
```env
AIRFLOW_UID=50000
WFP_PG_USER=postgres
WFP_PG_PASSWORD=sekonda
WFP_PG_DB=wfp_foods
WFP_PG_PORT=5432
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 3. Create the WFP Database in pgAdmin
Open **pgAdmin** and run:
```sql
CREATE DATABASE wfp_foods;
```

### 4. Copy the CSV into the DAGs Data Folder
```powershell
mkdir dags\data
copy wfp_food_prices_ken.csv dags\data\
```

### 5. Create Grafana Provisioning Folder
```powershell
mkdir grafana\provisioning\datasources
# Copy datasource.yaml into this folder
```

### 6. Start Airflow + Grafana
```powershell
docker-compose up airflow-init
docker-compose up -d
```

### 7. Start Metabase (separate)
```powershell
docker-compose -f docker-compose.metabase.yaml up -d
```

### 8. Activate dbt Virtual Environment
```powershell
.\dbt_venv\Scripts\Activate.ps1
dbt deps
dbt run
```

---

## Running the Pipeline

### Trigger the Airflow DAG
1. Open **http://localhost:8080**
2. Log in with `airflow` / `airflow`
3. Find `wfp_food_prices_pipeline`
4. Toggle it **ON**
5. Click **▶ Trigger DAG**

### DAG Tasks
```
extract ──► transform ──► load
```

| Task | Description |
|---|---|
| `extract` | Reads `wfp_food_prices_ken.csv` from disk |
| `transform` | Cleans columns, removes duplicates, validates dates & prices |
| `load` | Loads clean data into `wfp_food_prices_clean` table in PostgreSQL |

### Schedule
The DAG runs **daily** at midnight. To change the schedule edit this line
in `wfp_food_pipeline_dag.py`:
```python
schedule="@daily"    # Options: @hourly, @weekly, @monthly, None
```

---

## dbt Models

### Staging Model
**`stg_food_prices.sql`** — Cleans and standardises column names from the
raw PostgreSQL table.

```sql
SELECT
    commodity,
    admin2      AS county,
    market,
    price::numeric AS price,
    date,
    EXTRACT(YEAR FROM date)  AS year,
    EXTRACT(MONTH FROM date) AS month
FROM wfp_food_prices_clean
```

### Analytics Models

| Model | Description |
|---|---|
| `average_price_by_county_and_year.sql` | Average price grouped by county and year |
| `average_price_per_commodity.sql` | Average price per commodity type |
| `commodities_sold_in_more_than_10_markets.sql` | Commodities with wide market coverage |
| `highest_and_lowest_price_per_commodity.sql` | Price range per commodity |
| `monthly_average_prices.sql` | Monthly price trend over time |

### Run dbt Models
```powershell
# Activate virtual environment first
.\dbt_venv\Scripts\Activate.ps1

# Run all models
dbt run

# Run specific model
dbt run --select average_price_per_commodity

# Test models
dbt test

# Generate & serve docs
dbt docs generate
dbt docs serve
```

---

## Visualization

### Metabase — http://localhost:3001
Business intelligence dashboards built with SQL queries directly against
your `wfp_foods` PostgreSQL database.

**First-time setup:**
1. Go to **http://localhost:3001**
2. Complete the setup wizard
3. Add database connection:
   - Host: `host.docker.internal`
   - Port: `5432`
   - Database: `wfp_foods`
   - User: `postgres`
   - Password: `sekonda`


---

## Dashboard SQL Queries

Use these queries directly in **Metabase SQL editor** or **Grafana**:

### 1. Average Price by County and Year
```sql
SELECT
    admin2                         AS county,
    EXTRACT(YEAR FROM date)::INT   AS year,
    ROUND(AVG(price)::NUMERIC, 2)  AS avg_price
FROM wfp_food_prices_clean
GROUP BY admin2, EXTRACT(YEAR FROM date)
ORDER BY county, year;
```
**Chart type:** Bar chart (series breakout by county)

---

### 2. Average Price per Commodity
```sql
SELECT
    commodity,
    ROUND(AVG(price)::NUMERIC, 2) AS avg_price
FROM wfp_food_prices_clean
GROUP BY commodity
ORDER BY avg_price DESC;
```
**Chart type:** Bar chart

---

### 3. Commodities Sold in More Than 10 Markets
```sql
SELECT
    commodity,
    COUNT(DISTINCT market) AS market_count
FROM wfp_food_prices_clean
GROUP BY commodity
HAVING COUNT(DISTINCT market) > 10
ORDER BY market_count DESC;
```
**Chart type:** Row chart

---

### 4. Highest and Lowest Price per Commodity
```sql
SELECT
    commodity,
    ROUND(MAX(price)::NUMERIC, 2) AS highest_price,
    ROUND(MIN(price)::NUMERIC, 2) AS lowest_price
FROM wfp_food_prices_clean
GROUP BY commodity
ORDER BY highest_price DESC;
```
**Chart type:** Row chart (side by side — red for highest, green for lowest)

---

### 5. Monthly Average Prices (Time Series)
```sql
SELECT
    DATE_TRUNC('month', date)::DATE       AS month,
    ROUND(AVG(price)::NUMERIC, 2)         AS avg_price
FROM wfp_food_prices_clean
GROUP BY DATE_TRUNC('month', date)::DATE
ORDER BY month ASC;
```
**Chart type:** Line chart (timeseries)

---

## Ports Reference

| Service | URL | Credentials |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | airflow / airflow |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Metabase** | http://localhost:3001 | set on first login |
| **Airflow PostgreSQL** | localhost:5432 | airflow / airflow |
| **Metabase internal DB** | localhost:5434 | metabase / metabase |
| **WFP PostgreSQL (Windows)** | localhost:5432 | postgres / sekonda |

---

## Troubleshooting

### Airflow task stuck in queue
The Celery worker is not running. Check:
```powershell
docker ps | findstr worker
```
If missing, restart:
```powershell
docker-compose down
docker-compose up -d
```

### `role "airflow" does not exist`
Old volume conflict. Run:
```powershell
docker-compose down -v
docker-compose up -d
```

### `psql` not found in VS Code terminal
Add PostgreSQL to PATH:
```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\17\bin"
```

### Metabase can't connect to PostgreSQL
Make sure PostgreSQL allows Docker connections. Add to `pg_hba.conf`:
```
host    all    all    172.0.0.0/8    scram-sha-256
```
Then restart PostgreSQL.

### DAG import error — `schedule_interval`
Airflow 3.x removed `schedule_interval`. Use `schedule` instead:
```python
# ❌ Old
schedule_interval="@daily"
# ✅ New
schedule="@daily"
```

### DAG import error — `provide_context`
Airflow 3.x removed `provide_context`. Remove it from all PythonOperators:
```python
# ❌ Old
PythonOperator(task_id="extract", python_callable=extract, provide_context=True)
# ✅ New
PythonOperator(task_id="extract", python_callable=extract)
```

---

## Dataset

**Source:** World Food Programme — VAM Food Security Analysis

**Coverage:**
- **Country:** Kenya
- **Period:** January 2006 — Present
- **Records:** 17,632 rows
- **Commodities:** Maize, Beans, Bread, Sugar, and more
- **Counties:** Coast, Nairobi, Eastern, and others

---

*Built as part of the EverythingData Cohort 5 Data Engineering programme.*
