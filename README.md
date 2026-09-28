# Smart Water Network Intelligence Platform

## Overview

The **Smart Water Network Intelligence Platform** is an end-to-end data engineering project for processing water-distribution sensor data for network monitoring, operational analytics, and future anomaly detection.

The platform uses historical **BattLeDIM** water-network data to simulate continuous pressure, flow, tank-level, and demand measurements. These measurements are converted into sensor events and processed through a streaming architecture built with **Apache Kafka, Apache Spark, PostgreSQL/PostGIS, Parquet, Apache Airflow, and Docker**.

The project demonstrates how raw infrastructure sensor data can move through a complete data engineering pipeline:

**simulation → ingestion → stream processing → storage → orchestration → validation → analytics-ready data**

The longer-term objective is to support investigation of abnormal network behaviour and potential leakage.

> Detected anomalies will be treated as potential incidents requiring investigation, not automatically as confirmed leaks.

---

## Architecture

```text
BattLeDIM Historical SCADA Data
              │
              ▼
     Python Sensor Simulator
              │
              ▼
         Apache Kafka
     water-sensor-events
              │
              ▼
    Spark Structured Streaming
              │
       ┌──────┴──────┐
       │             │
       ▼             ▼
    Parquet      PostgreSQL
 Historical       Staging
   Storage           │
                     ▼
                Apache Airflow
                     │
                     ▼
              PostgreSQL / PostGIS
                Serving Layer
                     │
                     ▼
          Analytics / GIS / ML
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| Python simulator | Replays historical SCADA measurements as chronological sensor events |
| Apache Kafka | Provides streaming event ingestion |
| Spark Structured Streaming | Parses, validates, aggregates, and persists streaming data |
| Parquet | Stores historical valid events and rejected-event quarantine data |
| PostgreSQL | Stores reference data, staging metrics, and serving-layer metrics |
| PostGIS | Provides spatial database capability for future network/GIS analysis |
| Apache Airflow | Schedules and orchestrates database loading and data-quality checks |
| Docker | Provides isolated and reproducible infrastructure services |

Selected components may later be extended to AWS after the local architecture has been fully developed and validated.

---

## Current Implementation

The current local platform supports:

- historical BattLeDIM sensor-data replay;
- Kafka-based streaming ingestion;
- explicit JSON schema parsing;
- structural event validation;
- Spark event-time processing;
- a 10-minute watermark;
- 15-minute sensor-level aggregation;
- Parquet historical event storage;
- rejected-event quarantine;
- persistent Spark checkpoints;
- PostgreSQL/PostGIS reference and serving storage;
- staging-to-serving metric upserts;
- Airflow workflow orchestration;
- automatic daily DAG scheduling;
- task retry handling;
- PostgreSQL connectivity checks;
- serving-layer data-quality validation;
- reference-data validation;
- shared Docker networking across infrastructure services.

Current aggregated metrics include:

- reading count;
- average value;
- minimum value;
- maximum value.

A controlled one-hour replay processes:

```text
12 timestamps × 119 sensors = 1,428 sensor events
```

Those events produce:

```text
476 sensor/window metric records
```

across four 15-minute windows.

---

## Data Source

The project uses the **BattLeDIM 2018 dataset** together with the **L-Town EPANET water-distribution network model**.

BattLeDIM provides a benchmark dataset for research and development involving water-distribution monitoring and leakage detection.

The L-Town network is a hypothetical benchmark network and should not be interpreted as a real Rwandan water network.

### Sensor Measurements

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m3/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |
| **Total** | **119** | — |

The SCADA datasets contain **105,120 timestamps** at 5-minute intervals throughout 2018.

Each timestamp produces **119 sensor events**.

### Network Reference Data

The network model contains:

```text
Sensors          119
Network nodes    785
Network links    909
```

BattLeDIM leakage ground truth is kept separate from the streaming detector inputs. It can therefore be used later to evaluate anomaly-detection performance without leaking target information into the streaming pipeline.

Raw source data is preserved unchanged and excluded from Git version control.

---

## Sensor Event Contract

Kafka messages use the following logical structure:

```text
event_id
event_time
sensor_id
sensor_type
value
unit
```

Supported sensor types and units are:

```text
pressure → m
flow     → m3/h
level    → m
demand   → L/h
```

Kafka message keys use the composite pattern:

```text
<sensor_type>:<sensor_id>
```

Example:

```text
pressure:n1
```

Using the sensor type together with the sensor identifier prevents ambiguity when identifiers are reused across different measurement categories.

Structural validation and anomaly detection are deliberately separated.

A structurally valid measurement is not automatically considered normal, and an anomalous measurement is not automatically considered a confirmed leak.

---

## Streaming Processing

Spark Structured Streaming consumes sensor events from the Kafka topic:

```text
water-sensor-events
```

The processor performs:

```text
Kafka ingestion
      ↓
JSON parsing
      ↓
Schema validation
      ↓
Structural validation
      ↓
Event-time processing
      ↓
10-minute watermark
      ↓
15-minute sensor aggregation
      ↓
Parquet + PostgreSQL persistence
```

Spark currently runs locally inside a Docker container connected to the project's shared Docker network.

The controlled execution uses an `availableNow` trigger. Spark processes all events currently available in Kafka and then terminates cleanly rather than running indefinitely.

---

## Storage Architecture

The platform separates historical storage from operational serving responsibilities.

### Parquet Historical Layer

Structurally valid sensor events are written to:

```text
data/lake/sensor_events/
```

Rejected events are quarantined separately:

```text
data/lake/rejected_sensor_events/
```

The clean controlled replay produced:

```text
Valid historical events    1,428
Rejected events                 0
```

This separation allows invalid records to be investigated without contaminating the validated historical dataset.

### PostgreSQL / PostGIS Layer

PostgreSQL stores:

- sensor metadata;
- network nodes;
- network links;
- 15-minute sensor metrics;
- metric staging data.

Validated reference-data counts are:

```text
sensors          119
network_nodes    785
network_links    909
```

The controlled replay produced:

```text
metrics_staging    476
metrics_serving    476
```

Metrics are moved from staging into the serving table through an idempotent PostgreSQL upsert procedure.

Running the upsert again with the same metric records does not create duplicate serving records.

PostGIS is enabled for future spatial analysis. Network geometry is retained without assigning an unsupported coordinate reference system until the source CRS is confirmed.

---

## Streaming Checkpoints

Spark maintains checkpoint state for its historical-event and metric queries:

```text
data/checkpoints/events_archive/
data/checkpoints/metrics/
```

Checkpoint-based restart/resume behaviour has been validated.

After processing the controlled replay, restarting Spark using the same checkpoint state did not duplicate historical Parquet records.

This demonstrates checkpoint-based recovery behaviour in the current local implementation. It is not presented as a production exactly-once guarantee.

Generated lake data and Spark checkpoint state are excluded from Git.

> When Kafka is deliberately recreated and its topic offsets are reset, old Spark checkpoints may no longer correspond to the new Kafka log. In that controlled development scenario, the relevant checkpoints must also be reset before replaying the source data.

---

## Airflow Orchestration

Apache Airflow provides workflow orchestration for the serving layer.

The current DAG is:

```text
smart_water_daily_pipeline
```

Its workflow is:

```text
check_postgresql
        ↓
upsert_sensor_metrics
        ↓
validate_metric_load
        ↓
validate_reference_data
```

### Task Responsibilities

**`check_postgresql`**

Confirms that Airflow can connect successfully to the Smart Water PostgreSQL database.

**`upsert_sensor_metrics`**

Runs the PostgreSQL procedure that merges staged Spark metrics into the serving layer.

**`validate_metric_load`**

Checks that the serving metric table contains data after the upsert.

**`validate_reference_data`**

Confirms that required sensor and network reference tables contain records.

### Scheduling

The DAG runs daily using:

```text
@daily
```

which resolves to:

```text
0 0 * * *
```

The schedule therefore executes at **00:00 UTC**.

Airflow scheduling controls the serving-layer workflow. It does not mean Kafka itself only processes data once per day.

### Retry Behaviour

Airflow tasks are configured with:

```text
retries       = 2
retry_delay   = 5 minutes
```

This allows temporary failures such as short-lived database or network interruptions to be retried before the task is marked as failed.

The DAG uses:

```text
catchup=False
```

so historical scheduled runs are not automatically backfilled.

---

## End-to-End Validation

The integrated pipeline has been validated from source simulation through the final serving layer.

```text
BattLeDIM source data
        ↓
Python sensor simulator
        ↓
1,428 sensor events
        ↓
Apache Kafka
1,428 events confirmed
        ↓
Spark Structured Streaming
        ↓
├── 1,428 valid events → Parquet
│
└── 15-minute aggregation
             ↓
PostgreSQL staging
476 metric records
             ↓
Apache Airflow
             ↓
PostgreSQL serving layer
476 metric records
```

### Validated Results

| Validation | Result |
|---|---:|
| Kafka sensor events | 1,428 |
| Parquet historical events | 1,428 |
| Rejected events | 0 |
| Sensors | 119 |
| Network nodes | 785 |
| Network links | 909 |
| PostgreSQL staging metrics | 476 |
| PostgreSQL serving metrics | 476 |

Representative 15-minute pressure metrics were also checked against the expected calculations.

A second Airflow/upsert execution left the serving-layer count at:

```text
476
```

rather than increasing it to 952.

This validates repeat-safe metric persistence for the controlled replay and demonstrates that the serving-layer upsert is idempotent for the tested data.

All four Airflow tasks completed successfully during the final end-to-end validation.

---

## Project Progress

### Phase 1 — Project Design ✅

Defined:

- business problem;
- target users;
- architecture;
- technology responsibilities;
- initial anomaly-detection concept.

### Phase 2 — Data Acquisition & Understanding ✅

Validated:

- BattLeDIM SCADA datasets;
- timestamps and measurement structure;
- sensor mappings;
- leakage ground truth;
- L-Town network topology;
- event-contract requirements.

### Phase 3 — Python Sensor Simulator ✅

Built a reusable simulator that converts historical SCADA measurements into chronological sensor events with deterministic event identifiers and standardized units.

### Phase 4 — Apache Kafka Streaming Ingestion ✅

Implemented:

- local Kafka broker;
- sensor event topic;
- JSON serialization;
- sensor-based message keys;
- simulator-to-Kafka integration;
- consumer validation.

The development environment currently uses a **single Kafka broker and one topic partition** for functional validation.

### Phase 5 — Spark Structured Streaming ✅

Implemented:

- Kafka-to-Spark streaming;
- explicit schema parsing;
- structural validation;
- rejected-event handling;
- event-time processing;
- watermarking;
- 15-minute sensor metrics.

Spark currently executes locally inside Docker.

### Phase 6 — Persistent Storage ✅

Implemented:

- PostgreSQL/PostGIS serving storage;
- sensor and network reference-data loading;
- composite sensor identity handling;
- metric staging;
- idempotent metric upsert;
- Parquet historical storage;
- rejected-event quarantine;
- persistent Spark checkpoints;
- shared Docker networking;
- persistence validation.

### Phase 7 — Workflow Orchestration & End-to-End Integration ✅

Implemented and validated:

- Apache Airflow 3.3.1;
- Airflow `LocalExecutor`;
- PostgreSQL-backed Airflow metadata;
- Smart Water PostgreSQL Airflow connection;
- daily DAG scheduling;
- automatic task retries;
- staging-to-serving upsert orchestration;
- serving-layer data-quality checks;
- reference-data validation;
- shared Airflow/Kafka/PostgreSQL/Spark networking;
- successful manual DAG execution;
- successful scheduled DAG execution;
- complete source-to-serving validation;
- repeat-safe serving-layer persistence.

The final controlled validation confirmed:

```text
1,428 Kafka events
        ↓
476 aggregated staging records
        ↓
476 serving-layer records
        ↓
Successful Airflow validation
```

---

## Technology Stack

### Implemented

| Technology | Role |
|---|---|
| Python | Simulation, validation, reference-data loading |
| Pandas | Source-data manipulation and exploration |
| Apache Kafka 4.1.1 | Streaming ingestion |
| Apache Spark 4.1.3 | Structured stream processing and aggregation |
| PostgreSQL / PostGIS | Operational and spatial serving storage |
| Parquet | Historical event storage |
| Apache Airflow 3.3.1 | Workflow orchestration and data-quality checks |
| Docker / Docker Compose | Local infrastructure and service isolation |
| Git / GitHub | Version control and project publication |

### Planned as Requirements Are Introduced

Future phases may introduce:

- anomaly detection / machine learning;
- operational analytics;
- GIS analysis;
- Power BI dashboards;
- automated testing;
- selected AWS services.

Additional technologies such as Hadoop or MongoDB will only be introduced if a later architectural requirement justifies their use.

---

## Project Structure

```text
Smart-water-network-platform/
│
├── Docs/
│   ├── project_design.md
│   ├── data_understanding.md
│   └── data_contract.md
│
├── data/
│   ├── raw/
│   ├── reference/
│   ├── lake/
│   └── checkpoints/
│
├── infrastructure/
│   ├── airflow/
│   │   ├── dags/
│   │   │   └── smart_water_daily_pipeline.py
│   │   └── docker-compose.yml
│   │
│   ├── kafka/
│   │   └── docker-compose.yml
│   │
│   ├── postgres/
│   │   ├── docker-compose.yml
│   │   └── .env.example
│   │
│   └── spark/
│       └── run_spark_stream.cmd
│
├── sql/
│   ├── 01_create_storage_schema.sql
│   └── 02_upsert_sensor_metrics.sql
│
├── src/
│   ├── data/
│   ├── exploration/
│   ├── simulator/
│   └── streaming/
│
├── .gitignore
├── requirements.txt
└── README.md
```

Runtime files such as `.env`, Python virtual environments, generated lake data, Spark checkpoints, and Python cache files are excluded from version control.

---

## Local Setup

The current implementation is developed and validated on Windows using Docker Desktop.

### Prerequisites

Install:

- Git;
- Python;
- Docker Desktop with Docker Compose.

Clone the repository and move into the project directory.

Create and activate a Python virtual environment, then install the required packages:

```cmd
pip install -r requirements.txt
```

### Environment Configuration

Use:

```text
infrastructure/postgres/.env.example
```

as the template for the local environment configuration.

Create:

```text
infrastructure/postgres/.env
```

and provide the required local values.

The real `.env` file is excluded from Git and must not be committed.

### Create the Shared Docker Network

Create the project network once:

```cmd
docker network create smart-water-network
```

Kafka, Spark, PostgreSQL, and Airflow communicate through this shared Docker network.

### Start PostgreSQL / PostGIS

```cmd
docker compose --env-file infrastructure\postgres\.env -f infrastructure\postgres\docker-compose.yml up -d
```

### Initialize the Storage Schema

```cmd
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\01_create_storage_schema.sql
```

Install the metric upsert procedure:

```cmd
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\02_upsert_sensor_metrics.sql
```

Load sensor and network reference data:

```cmd
python -m src.data.load_reference_data
```

### Start Kafka

```cmd
docker compose -f infrastructure\kafka\docker-compose.yml up -d
```

Create the development sensor topic when starting with a new Kafka broker:

```cmd
docker exec smart-water-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic water-sensor-events --partitions 1 --replication-factor 1
```

### Start Airflow

```cmd
docker compose --env-file infrastructure\postgres\.env -f infrastructure\airflow\docker-compose.yml up -d
```

The Airflow API/UI service is exposed locally through port:

```text
8081
```

The DAG:

```text
smart_water_daily_pipeline
```

is configured for daily execution.

### Replay Sensor Data

From the project root:

```cmd
python -m src.simulator.sensor_simulator
```

The current controlled replay publishes:

```text
12 timestamps × 119 sensors = 1,428 Kafka events
```

### Run Spark Structured Streaming

```cmd
infrastructure\spark\run_spark_stream.cmd
```

Spark consumes the available Kafka events, writes valid historical events to Parquet, and writes 15-minute aggregated metrics to PostgreSQL staging.

### Trigger the Airflow Pipeline Manually

For development validation:

```cmd
docker compose --env-file infrastructure\postgres\.env -f infrastructure\airflow\docker-compose.yml exec airflow-scheduler airflow dags trigger smart_water_daily_pipeline
```

Airflow then:

```text
checks PostgreSQL
        ↓
upserts staged metrics
        ↓
validates serving metrics
        ↓
validates reference data
```

The DAG also runs automatically according to its daily schedule.

---

## Reproducibility and Repository Hygiene

The repository intentionally excludes machine-specific, generated, or sensitive files.

Examples include:

```text
.env
*.env
venv/
__pycache__/
*.pyc
data/raw/
data/lake/
data/checkpoints/
```

The `.env.example` file documents required configuration variables without exposing real credentials.

Raw source datasets are also excluded from Git and must be obtained separately.

This keeps the repository focused on source code, infrastructure configuration, SQL definitions, and documentation rather than local runtime state.

---

## Documentation

Detailed design and data decisions are maintained under `Docs/`:

- `project_design.md` — business problem, users, architecture, and design decisions;
- `data_understanding.md` — SCADA data, leakage data, quality findings, and network topology;
- `data_contract.md` — sensor-event schema and validation rules.

The README provides the high-level engineering view, while detailed findings remain in the relevant technical documentation.

---

## Engineering Principles

The project follows several deliberate engineering principles:

**Separation of responsibilities**
Kafka handles ingestion, Spark handles streaming transformations, PostgreSQL serves structured operational data, Parquet retains historical events, and Airflow handles workflow orchestration.

**Raw-data preservation**
Source data is not modified in place.

**Explicit data contracts**
Sensor events have defined fields, types, sensor categories, and units.

**Separation of validation and anomaly detection**
A valid record is not automatically a normal record.

**Idempotent persistence**
Repeated serving-layer upserts should not create duplicate metrics.

**Checkpoint-based recovery**
Spark checkpoint state supports controlled restart/resume behaviour.

**Secrets management**
Credentials are provided through ignored environment files rather than committed configuration.

**Incremental architecture**
Technologies are introduced only when they have a clear responsibility in the system.

---

## Development Roadmap

```text
Understand source data                         ✅
        ↓
Define sensor event contract                   ✅
        ↓
Simulate sensor events                         ✅
        ↓
Build Kafka streaming ingestion                ✅
        ↓
Process streams with Spark                     ✅
        ↓
Persist historical and operational data        ✅
        ↓
Orchestrate and validate workflows             ✅
        ↓
Develop anomaly detection
        ↓
Add analytics and spatial context
        ↓
Build dashboards
        ↓
Extend selected components to AWS
```

---

## Next Phase

With the core streaming, persistence, and orchestration architecture validated, the next development stage can build on a stable engineering foundation.

Future work will focus incrementally on capabilities such as:

- anomaly-detection logic;
- leakage-related analytical features;
- GIS/network analysis;
- dashboards and operational visualization;
- automated testing;
- selected AWS integration.

These capabilities will be introduced only when their role in the architecture is clearly defined.

---

## Author

**GATERA Emile**
Civil & Water Resources Engineer | Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with practical data engineering, streaming architecture, workflow orchestration, database design, and future geospatial analytics.
