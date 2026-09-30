# Law Enforcement Call Response Analytics

An end-to-end ELT pipeline that pulls police dispatch call records from a public CSV endpoint, computes stage-by-stage response times, loads them into **Snowflake**, and uses **dbt** to rank police districts by average response time. Orchestration runs on **Apache Airflow**, and the whole stack is containerised with **Docker Compose**.

---

## Table of Contents

- [Results at a Glance](#results-at-a-glance)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Details](#pipeline-details)
  - [DAG 1 — `law_enforcement_ETL`](#dag-1--law_enforcement_etl)
  - [DAG 2 — `BuildELT_dbt`](#dag-2--buildelt_dbt)
  - [dbt Models, Tests & Snapshot](#dbt-models-tests--snapshot)
- [Metric Definitions](#metric-definitions)
- [Results](#results)
  - [Data Volume & Quality](#data-volume--quality)
  - [Response-Time Distribution](#response-time-distribution)
  - [District Performance](#district-performance)
  - [Pipeline Reliability & Performance](#pipeline-reliability--performance)
- [Known Issues & Limitations](#known-issues--limitations)
- [Getting Started](#getting-started)
- [Snowflake Schema](#snowflake-schema)

---

## Results at a Glance

All figures come from the Airflow task logs in [logs/](logs/) (runs between **2025-04-18 and 2025-04-23**). District metrics were recomputed from the row-level values printed by the `load_data` task, using the same logic as the dbt model.

| Metric | Value |
|---|---|
| Rows pulled per extract | **1,000** (identical in all 26 extract attempts) |
| Rows kept after cleaning | **507 – 567** per load (**43–49 %** dropped for missing timestamps/district) |
| Successful loads into Snowflake | **4** (row counts were logged for 3: 520, 507, 567) |
| Police districts covered | **10** |
| Latest load | **567 calls** received **2025-04-16 → 2025-04-22** |
| Avg. response time, all districts (model output, latest load) | **52.69 min** (median 24.95 min, n = 340) |
| Slowest / fastest district (model output, latest load) | **Tenderloin 80.13 min** / **Central 35.17 min** |
| dbt data tests | **2 / 2 passed** in all 6 successful dbt runs |
| dbt `run` time (2 view models) | **2.88 – 5.25 s** on successful runs |
| Snowflake load time | **5 m 42 s – 6 m 45 s** (~1.4 rows/s, row-by-row inserts) |

> ⚠️ **Caveat:** zero-minute intervals are stored as `NULL` (see [Known Issues](#known-issues--limitations)), so the model leaves out every call that was dispatched or arrived instantly. That pushes averages up. With zeros counted, the latest load's overall average falls from **52.69 min to 39.33 min**, and the district ranking changes.

---

## Architecture

```
            Public CSV endpoint (Airflow Variable: lab2_LawInforcement_url)
                                   │  HTTP GET
                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Apache Airflow 2.10.1  (LocalExecutor, Postgres 13 metadata DB)      │
│                                                                      │
│  DAG 1: law_enforcement_ETL  — cron "30 2 * * *" (02:30 UTC daily)   │
│    extract_data ──▶ transform_data ──▶ load_data                     │
│    (requests +      (pandas: parse,     (SnowflakeHook: BEGIN →      │
│     pandas)          drop NA, deltas)    DELETE → INSERT → COMMIT)   │
│                                                                      │
│  DAG 2: BuildELT_dbt  — manual trigger only                          │
│    dbt_run ──▶ dbt_test ──▶ dbt_snapshot     (BashOperator)          │
└──────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Snowflake  (database: dev)                                           │
│   raw.law_enforcement_calls          ← table written by DAG 1        │
│   analytics.law_enforcement_calls    ← dbt view (staging)            │
│   analytics.Avg_response_time_pd     ← dbt view (district ranking)   │
│   snapshots.district_response_snapshot ← dbt snapshot (SCD-2, check) │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version (from config/logs) |
|---|---|---|
| Orchestration | Apache Airflow | `apache/airflow:2.10.1` |
| Airflow metadata DB | PostgreSQL | 13 |
| Transformation | dbt-core / dbt-snowflake | 1.6.18 / 1.6.0 |
| Warehouse | Snowflake | — |
| Language / libs | Python 3.12, pandas, requests, `apache-airflow-providers-snowflake`, `snowflake-connector-python` | — |
| Containers | Docker Compose | — |

---

## Project Structure

```
Law_Enforcement_Analytics-main/
├── dags/
│   ├── Lab2_ETL.py                  # DAG 1: extract → transform → load
│   └── build_elt_with_dbt.py        # DAG 2: dbt run → test → snapshot
├── dbt/                             # dbt project "lab2_dbt"
│   ├── dbt_project.yml
│   ├── profiles.yml                 # Snowflake target, all creds via env vars
│   ├── models/
│   │   ├── input/
│   │   │   ├── source.yml           # sources + not_null tests
│   │   │   └── law_enforcement_calls.sql
│   │   └── output/
│   │       ├── Avg_response_time_pd.sql   # district performance model
│   │       └── schema.yml
│   └── snapshots/
│       └── snapshot_session_summary.sql   # district_response_snapshot
├── config/airflow.cfg
├── docker-compose-min.yml           # Airflow + Postgres + dbt mount (use this one)
├── docker-compose.yaml              # same stack without dbt mount / dbt package
├── .env                             # AIRFLOW_UID
└── logs/                            # Airflow task & scheduler logs (source of the results below)
```

---

## Pipeline Details

### DAG 1 — `law_enforcement_ETL`

[dags/Lab2_ETL.py](dags/Lab2_ETL.py) · schedule `30 2 * * *` · `catchup=False` · start date 2025-04-05

| Task | What it does |
|---|---|
| `extract_data` | `GET` on the URL stored in the Airflow Variable `lab2_LawInforcement_url`, then parses the CSV into pandas and passes the records on via XCom. |
| `transform_data` | Casts `received`, `dispatch`, `enroute` and `onscene` datetimes (`errors='coerce'`), keeps 7 columns, drops rows missing any of the 4 timestamps or `police_district`, and computes three interval columns in minutes. |
| `load_data` | Opens a transaction in Snowflake, runs `CREATE TABLE IF NOT EXISTS dev.raw.law_enforcement_calls`, `DELETE`s all rows (full refresh), `INSERT`s row by row, then `COMMIT`s. It runs `ROLLBACK` on error. |

### DAG 2 — `BuildELT_dbt`

[dags/build_elt_with_dbt.py](dags/build_elt_with_dbt.py) · manual trigger · `dbt_run >> dbt_test >> dbt_snapshot`

Snowflake credentials are read from the Airflow connection `snowflake_conn` and passed to dbt as `DBT_*` environment variables. `DBT_USER`, `DBT_PASSWORD` and `DBT_ACCOUNT` come from the connection's **extra** fields `snowflake_userid`, `snowflake_password` and `snowflake_account`.

### dbt Models, Tests & Snapshot

| Object | Type | Logic |
|---|---|---|
| `law_enforcement_calls` | view (`dev.analytics`) | `SELECT *` from source `raw.law_enforcement_calls` |
| `Avg_response_time_pd` | view (`dev.analytics`) | For each district: `AVG(dispatch_to_received_min + onscene_to_enroute_min)` over rows where both are non-null, joined to `COUNT(cad_number)`, ordered slowest → fastest |
| `district_response_snapshot` | snapshot (`dev.snapshots`) | Same aggregation, `strategy='check'` on `avg_response_time_min` and `total_cases`, `unique_key='police_district'` |
| `source_not_null_raw_law_enforcement_calls_cad_number` | test | not_null |
| `source_not_null_raw_law_enforcement_calls_police_district` | test | not_null |

---

## Metric Definitions

| Metric | Formula | Stored in |
|---|---|---|
| `dispatch_to_received_min` | `dispatch_datetime − received_datetime` (call-processing time) | raw table |
| `enroute_to_dispatch_min` | `enroute_datetime − dispatch_datetime` | raw table |
| `onscene_to_enroute_min` | `onscene_datetime − enroute_datetime` (travel time) | raw table |
| `avg_response_time_min` | mean of (`dispatch_to_received_min` + `onscene_to_enroute_min`) per district. `enroute_to_dispatch_min` is **not** included in the sum. | dbt view |
| `total_cases` | `COUNT(cad_number)` per district | dbt view |

---

## Results

### Data Volume & Quality

| Load (Airflow run) | Rows extracted | Rows loaded | Dropped | Call `received_datetime` range | Unique `cad_number` |
|---|---|---|---|---|---|
| `manual__2025-04-21T05:15` | 1,000 | **520** | 480 (48.0 %) | 2024-05-20 → 2025-04-20 | 520 |
| `scheduled__2025-04-21T02:30` | 1,000 | **507** | 493 (49.3 %) | 2025-03-29 → 2025-04-21 | 507 |
| `scheduled__2025-04-22T02:30` | 1,000 | **567** | 433 (43.3 %) | 2025-04-16 → 2025-04-22 | 567 |

- The extract always returns exactly 1,000 rows. This looks like the source API's default page limit, so every load is a sample of the dataset rather than the full dataset.
- None of the three loads had duplicate `cad_number`/`id` values or negative intervals.
- In the latest load, every row has `enroute_datetime == dispatch_datetime`, so `enroute_to_dispatch_min` is `0.0` for **567 / 567** rows.

### Response-Time Distribution

Latest load (567 rows). "Non-null" is the number of rows actually stored with a value. Zeros are stored as NULL (see Known Issues).

| Interval | Non-null rows | Mean (min) | Median (min) | Max (min) |
|---|---|---|---|---|
| Received → dispatched | 395 (69.7 %) | 43.27 | 13.77 | 479.22 |
| Dispatched → en route | 0 (all were 0.0) | — | — | — |
| En route → on scene | 361 (63.7 %) | 14.44 | 7.23 | 289.62 |
| **Total (model logic, n = 340)** | — | **52.69** | **24.95** | — |
| **Total (zeros counted, n = 567)** | — | **39.33** | **12.08** | — |

The distribution has a strong right skew: the mean is about 2–3× the median. A handful of calls with multi-hour queue times dominate the averages. Most of the delay is call-processing time (received → dispatched), not travel time.

### District Performance

**Latest load: 567 calls received 2025-04-16 → 2025-04-22.** "Model output" is what `dev.analytics.Avg_response_time_pd` returns. "Zeros counted" is the corrected figure.

| Rank (model) | District | Model avg (min) | Model median (min) | Rows in avg | `total_cases` | Avg, zeros counted (min) |
|---|---|---|---|---|---|---|
| 1 (slowest) | TENDERLOIN | 80.13 | 29.52 | 19 | 47 | 61.42 |
| 2 | RICHMOND | 71.31 | 25.23 | 30 | 33 | 71.05 |
| 3 | MISSION | 57.75 | 31.60 | 53 | 93 | 43.52 |
| 4 | PARK | 57.35 | 25.16 | 26 | 35 | 45.11 |
| 5 | SOUTHERN | 53.66 | 15.67 | 53 | **127** | 32.27 |
| 6 | TARAVAL | 47.35 | 23.28 | 33 | 46 | 34.63 |
| 7 | NORTHERN | 46.47 | 23.48 | 46 | 72 | 31.75 |
| 8 | INGLESIDE | 44.91 | 41.00 | 33 | 45 | 32.93 |
| 9 | BAYVIEW | 36.13 | 13.70 | 21 | 33 | **23.39** |
| 10 (fastest) | CENTRAL | 35.17 | 25.19 | 26 | 36 | 33.67 |

Key takeaways:

- **Spread:** the slowest district's model average is **2.3×** the fastest's (80.13 vs 35.17 min).
- **Volume:** Southern handled the most calls (**127, 22.4 %** of the load). Richmond and Bayview handled the fewest (33 each).
- **The ranking changes when zeros are counted.** Richmond becomes the slowest (71.05 min) and Bayview the fastest (23.39 min). Tenderloin's model average is based on only 19 of its 47 calls.
- **The ranking also changes between loads** because each load is a different 1,000-row sample:

  | Load | Slowest (model avg) | Fastest (model avg) | Overall model avg |
  |---|---|---|---|
  | `manual__2025-04-21T05:15` (520 rows) | PARK 89.39 | CENTRAL 51.49 | 63.77 min |
  | `scheduled__2025-04-21` (507 rows) | SOUTHERN 65.64 | TARAVAL 34.82 | 54.66 min |
  | `scheduled__2025-04-22` (567 rows) | TENDERLOIN 80.13 | CENTRAL 35.17 | 52.69 min |

### Pipeline Reliability & Performance

**`law_enforcement_ETL`**: 26 DAG runs (21 manual, 5 scheduled)

| Outcome | Count |
|---|---|
| Fully successful (extract → transform → load) | **4 / 26 (15.4 %)**. Of the last 3 attempts, 3 succeeded, including both scheduled runs on 04-21 and 04-22. |
| `extract_data` succeeded | 25 / 26 |
| Failures seen while debugging | XCom serialisation of pandas `Timestamp` (fixed by `strftime`), `KeyError` on a non-existent `agency` column, Snowflake `SQL compilation error` (column-count mismatch), `not all arguments converted during string formatting` |

| Successful load | Rows | Duration | Throughput |
|---|---|---|---|
| 2025-04-19 20:18 | not logged | 4 m 09 s | — |
| 2025-04-21 05:15 | 520 | 5 m 42 s | 1.52 rows/s |
| 2025-04-22 (sched.) | 507 | 6 m 11 s | 1.37 rows/s |
| 2025-04-23 (sched. 04-22) | 567 | 6 m 45 s | 1.40 rows/s |

**`BuildELT_dbt`**: 16 DAG runs

| Outcome | Count |
|---|---|
| Fully successful (run → test → snapshot) | **6 / 16 (37.5 %)**. All of the last 4 runs succeeded. |
| Failures seen while debugging | dbt binary not found (exit 127), missing env var / profiles path (`NoneType`), Snowflake `invalid identifier 'DISPATCH_TO_RECEIVED'` |
| `dbt run` (2 view models) | 2.88 – 5.25 s (successful runs) |
| `dbt test` (2 tests) | 1.84 – 4.24 s, **PASS = 2, ERROR = 0** in all 6 runs |
| `dbt snapshot` | Task succeeded 6 / 6, but dbt reported *"Nothing to do"* every time. **No snapshot rows were ever written** (see below). |

---

## Known Issues & Limitations

1. **Zero-minute intervals become `NULL`.** In `load_data`, `row.get(col) or None` turns `0.0` into `None`. In the latest load this nulled **all 567** `enroute_to_dispatch_min` values, **172** `dispatch_to_received_min` values and **206** `onscene_to_enroute_min` values. The dbt model then filters out those rows, so it keeps only 340 of 567 calls and overstates averages by about 34 % (52.69 vs 39.33 min). **Fix:** `row.get(col)` and map only real NaN values to `None`.
2. **The snapshot never ran.** At runtime dbt found `2 models, 2 tests` and no snapshot, so `dev.snapshots.district_response_snapshot` was never populated. Check that `dbt/snapshots/` is mounted in the container, then run `dbt snapshot` again.
3. **Model name mismatch.** The model file is `Avg_response_time_pd.sql`, but [schema.yml](dbt/models/output/schema.yml) documents `final_district_performance`. That model's `not_null` test therefore never runs, and only the 2 source tests execute. `schema.yml` also lists columns the view doesn't produce, such as `district_rank` and `total_incident_duration_min`.
4. **`enroute_datetime` is not persisted.** It is used in the transform but is not a column in the Snowflake table.
5. **1,000-row cap.** The extract gets one page from the API, so the results describe a sample, not the full dataset.
6. **Slow load.** Row-by-row `INSERT` runs at about 1.4 rows/s. `executemany` or `write_pandas` would be much faster.
7. **Credentials in connection extras.** The dbt DAG expects the password in the connection's `extra` JSON (`snowflake_password`).

---

## Getting Started

### Prerequisites

- Docker Desktop (at least 4 GB RAM, 2 CPUs and 10 GB disk, as checked by `airflow-init`)
- A Snowflake account with a warehouse, the `dev` database, and `raw`, `analytics` and `snapshots` schemas

### 1. Start the stack

Use `docker-compose-min.yml`. It is the only compose file that mounts `./dbt` and installs `dbt-snowflake==1.6.0`.

```bash
docker compose -f docker-compose-min.yml up -d
```

The Airflow UI is at **http://localhost:8081** (user `airflow` / password `airflow`).

### 2. Create the Airflow Variable

```bash
airflow variables set lab2_LawInforcement_url "<csv-endpoint-url>"
```

### 3. Create the Snowflake connection `snowflake_conn`

The ETL DAG uses the standard `SnowflakeHook` fields. The dbt DAG reads `snowflake_userid`, `snowflake_password`, `snowflake_account`, `database`, `role` and `warehouse` from **extra**, so set both:

```bash
airflow connections add snowflake_conn \
  --conn-type snowflake \
  --conn-login <user> \
  --conn-password <password> \
  --conn-schema raw \
  --conn-extra '{"account": "<account>", "database": "dev", "warehouse": "<wh>", "role": "<role>",
                 "snowflake_account": "<account>", "snowflake_userid": "<user>", "snowflake_password": "<password>"}'
```

### 4. Run

```bash
airflow dags trigger law_enforcement_ETL   # or wait for 02:30 UTC
airflow dags trigger BuildELT_dbt          # after the load completes
```

Both DAGs are paused when created (`DAGS_ARE_PAUSED_AT_CREATION=true`), so unpause them in the UI first.

### 5. Stop

```bash
docker compose -f docker-compose-min.yml down
```

---

## Snowflake Schema

### `dev.raw.law_enforcement_calls` (created by `load_data`)

| Column | Type |
|---|---|
| `id` | STRING |
| `cad_number` | STRING |
| `received_datetime` | TIMESTAMP |
| `dispatch_datetime` | TIMESTAMP |
| `onscene_datetime` | TIMESTAMP |
| `police_district` | STRING |
| `dispatch_to_received_min` | FLOAT |
| `enroute_to_dispatch_min` | FLOAT |
| `onscene_to_enroute_min` | FLOAT |

### `dev.analytics.Avg_response_time_pd` (dbt view)

| Column | Description |
|---|---|
| `police_district` | District name (10 distinct values) |
| `avg_response_time_min` | Mean of received→dispatched + en route→on scene, in minutes |
| `total_cases` | Count of `cad_number` in the district |
