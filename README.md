# Law Enforcement Call Response Analytics — ETL Pipeline

An end-to-end data pipeline that ingests law enforcement call data, calculates response time metrics, and ranks police districts by performance. Built with **Apache Airflow** for orchestration, **dbt** for transformation, and **Snowflake** as the data warehouse — all containerized with Docker.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Key Metrics](#key-metrics)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Setup](#environment-setup)
  - [Running the Pipeline](#running-the-pipeline)
- [Airflow DAGs](#airflow-dags)
- [dbt Models](#dbt-models)
- [Snowflake Schema](#snowflake-schema)

---

## Overview

This pipeline automates the collection and analysis of law enforcement call data by:

1. **Extracting** raw call records from an external CSV source via HTTP
2. **Transforming** the data to compute response time deltas at each stage (received → dispatched → en-route → on-scene)
3. **Loading** cleaned records into Snowflake (`dev.raw.law_enforcement_calls`)
4. **Modeling** the raw data using dbt to produce district-level performance analytics
5. **Snapshotting** historical changes so trends can be tracked over time

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Apache Airflow                           │
│                                                                  │
│  DAG 1: law_enforcement_ETL  (daily @ 02:30 UTC)                │
│  ┌──────────┐    ┌─────────────┐    ┌───────────────────────┐   │
│  │ Extract  │───▶│  Transform  │───▶│  Load to Snowflake    │   │
│  │ (CSV URL)│    │ (pandas)    │    │  dev.raw.*            │   │
│  └──────────┘    └─────────────┘    └───────────────────────┘   │
│                                                                  │
│  DAG 2: BuildELT_dbt  (manual trigger)                          │
│  ┌──────────┐    ┌─────────────┐    ┌───────────────────────┐   │
│  │ dbt run  │───▶│  dbt test   │───▶│  dbt snapshot         │   │
│  └──────────┘    └─────────────┘    └───────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │           Snowflake            │
              │  dev.raw  →  dev.analytics    │
              │  law_enforcement_calls         │
              │  final_district_performance    │
              └───────────────────────────────┘
```

---

## Tech Stack

| Component        | Technology                        | Version   |
|-----------------|-----------------------------------|-----------|
| Orchestration   | Apache Airflow                    | 2.10.1    |
| Transformation  | dbt (data build tool)             | 1.6.0     |
| Data Warehouse  | Snowflake                         | —         |
| Airflow Backend | PostgreSQL                        | 13        |
| Language        | Python                            | 3.x       |
| Containerization| Docker / Docker Compose           | —         |
| Key Libraries   | pandas, requests, snowflake-connector-python | Latest |

---

## Project Structure

```
Lab2_ETL/
├── dags/
│   ├── Lab2_ETL.py               # ETL pipeline DAG (Extract → Transform → Load)
│   └── build_elt_with_dbt.py     # dbt execution DAG (run → test → snapshot)
├── dbt/
│   ├── dbt_project.yml           # dbt project configuration
│   ├── profiles.yml              # Snowflake connection profile (env-var driven)
│   ├── models/
│   │   ├── input/
│   │   │   ├── source.yml        # Raw source definitions & NOT NULL tests
│   │   │   └── law_enforcement_calls.sql  # Input staging model
│   │   └── output/
│   │       ├── Avg_response_time_pd.sql   # District performance analytics model
│   │       └── schema.yml        # Output model documentation & tests
│   └── snapshots/
│       └── snapshot_session_summary.sql  # Historical snapshot (SCD Type 2)
├── config/
│   └── airflow.cfg               # Airflow configuration
├── docker-compose.yaml           # Full Docker setup (Airflow + PostgreSQL)
├── docker-compose-min.yml        # Minimal setup with dbt integration
├── .env                          # Environment variables (AIRFLOW_UID)
├── logs/                         # Airflow task execution logs
└── plugins/                      # Custom Airflow plugins (empty)
```

---

## Data Flow

```
External CSV (HTTP)
        │
        ▼
┌───────────────────────────────────────────────────┐
│  EXTRACT                                          │
│  • Fetch CSV via HTTP GET                         │
│  • Parse into pandas DataFrame                    │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────┐
│  TRANSFORM                                        │
│  • Parse datetime columns                         │
│  • Calculate time deltas (minutes):               │
│    - dispatch_to_received_min                     │
│    - enroute_to_dispatch_min                      │
│    - onscene_to_enroute_min                       │
│  • Drop rows with missing critical values         │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────┐
│  LOAD → Snowflake: dev.raw.law_enforcement_calls  │
│  • Create table if not exists                     │
│  • Delete + insert (idempotent, transactional)    │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────┐
│  dbt RUN                                          │
│  • Stage raw data (input model)                   │
│  • Build analytics model (output model)           │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────┐
│  dbt TEST                                         │
│  • NOT NULL assertions on key columns             │
│  • Data quality checks                            │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────┐
│  dbt SNAPSHOT → dev.analytics                     │
│  • Capture changes to district_response_snapshot  │
│  • Track avg_response_time_min + total_cases      │
└───────────────────────────────────────────────────┘
```

---

## Key Metrics

The pipeline computes the following response time metrics per police district:

| Metric | Description |
|--------|-------------|
| `dispatch_to_received_min` | Minutes from call received to dispatch |
| `enroute_to_dispatch_min` | Minutes from dispatch to unit en-route |
| `onscene_to_enroute_min` | Minutes from en-route to on-scene arrival |
| `avg_response_time_min` | Average total response time per district |
| `total_cases` | Number of CAD calls handled per district |

The final output `dev.analytics.final_district_performance` ranks all districts from slowest to fastest average response time.

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- A Snowflake account with a configured warehouse, database, and role
- An Airflow Snowflake connection configured (connection ID: `snowflake_conn`)

### Environment Setup

1. **Clone the repository**

   ```bash
   git clone <repo-url>
   cd Lab2_ETL
   ```

2. **Configure environment variables**

   The `.env` file sets the Airflow container user ID. Update it if needed:

   ```env
   AIRFLOW_UID=50000
   ```

3. **Configure dbt Snowflake credentials**

   The [dbt/profiles.yml](dbt/profiles.yml) reads from environment variables. Export these before running (or add them to your Docker Compose environment block):

   ```bash
   export DBT_ACCOUNT=<your_snowflake_account>
   export DBT_USER=<your_username>
   export DBT_PASSWORD=<your_password>
   export DBT_DATABASE=dev
   export DBT_ROLE=<your_role>
   export DBT_WAREHOUSE=<your_warehouse>
   ```

4. **Configure the Airflow Snowflake connection**

   After the Airflow webserver is running, add a connection via the UI or CLI:

   ```bash
   airflow connections add snowflake_conn \
     --conn-type snowflake \
     --conn-host <account>.snowflakecomputing.com \
     --conn-login <user> \
     --conn-password <password> \
     --conn-schema raw \
     --conn-extra '{"database": "dev", "warehouse": "<warehouse>", "role": "<role>"}'
   ```

### Running the Pipeline

**Start all services (with dbt support):**

```bash
docker compose -f docker-compose-min.yml up -d
```

**Access the Airflow UI:**

```
http://localhost:8080
```

Default credentials: `airflow` / `airflow`

**Trigger the ETL DAG:**

Navigate to `law_enforcement_ETL` in the Airflow UI and enable/trigger it, or:

```bash
airflow dags trigger law_enforcement_ETL
```

**Trigger the dbt DAG:**

```bash
airflow dags trigger BuildELT_dbt
```

**Stop all services:**

```bash
docker compose -f docker-compose-min.yml down
```

---

## Airflow DAGs

### `law_enforcement_ETL`

| Property | Value |
|----------|-------|
| Schedule | Daily at `02:30 UTC` |
| Source | External CSV URL |
| Destination | `dev.raw.law_enforcement_calls` |
| Retries | 0 |

**Tasks:**

| Task | Operator | Description |
|------|----------|-------------|
| `extract_data` | `@task` (Python) | Fetches CSV from URL, returns records |
| `transform_data` | `@task` (Python) | Parses datetimes, computes response deltas |
| `load_data` | `@task` (Python) | Loads records into Snowflake with ACID transaction |

### `BuildELT_dbt`

| Property | Value |
|----------|-------|
| Schedule | Manual trigger only |
| Runs | dbt run → dbt test → dbt snapshot |

**Tasks:**

| Task | Operator | Description |
|------|----------|-------------|
| `dbt_run` | `BashOperator` | Executes dbt models |
| `dbt_test` | `BashOperator` | Runs dbt data quality tests |
| `dbt_snapshot` | `BashOperator` | Updates historical snapshot |

---

## dbt Models

### Input Layer (`models/input/`)

| Model | Source | Description |
|-------|--------|-------------|
| `law_enforcement_calls` | `dev.raw.law_enforcement_calls` | Staging pass-through of raw call data |

### Output Layer (`models/output/`)

| Model | Description |
|-------|-------------|
| `final_district_performance` | Aggregates average response time and total case count per police district, ordered slowest → fastest |

### Snapshots (`snapshots/`)

| Snapshot | Strategy | Unique Key | Tracked Columns |
|----------|----------|------------|-----------------|
| `district_response_snapshot` | `check` | `police_district` | `avg_response_time_min`, `total_cases` |

---

## Snowflake Schema

### `dev.raw.law_enforcement_calls`

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR | Record identifier |
| `cad_number` | VARCHAR | Unique CAD case number |
| `received_datetime` | VARCHAR | Timestamp call was received |
| `dispatch_datetime` | VARCHAR | Timestamp call was dispatched |
| `enroute_datetime` | VARCHAR | Timestamp unit went en-route |
| `onscene_datetime` | VARCHAR | Timestamp unit arrived on scene |
| `police_district` | VARCHAR | District handling the call |
| `dispatch_to_received_min` | FLOAT | Minutes: received → dispatch |
| `enroute_to_dispatch_min` | FLOAT | Minutes: dispatch → en-route |
| `onscene_to_enroute_min` | FLOAT | Minutes: en-route → on-scene |

### `dev.analytics.final_district_performance`

| Column | Type | Description |
|--------|------|-------------|
| `police_district` | VARCHAR | District name |
| `avg_response_time_min` | FLOAT | Average total response time (minutes) |
| `total_cases` | INTEGER | Total CAD calls handled |
