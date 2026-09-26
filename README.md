# Smart Water Network Intelligence Platform

## Overview

The **Smart Water Network Intelligence Platform** is an end-to-end data engineering project for processing water-distribution sensor data for network monitoring, anomaly detection, and operational analytics.

The platform uses historical **BattLeDIM** water-network data to simulate continuous pressure, flow, tank-level, and demand measurements and progressively builds a real-time streaming architecture around them.

The goal is to transform raw sensor measurements into validated, structured, and analysis-ready data that can support investigation of abnormal network behaviour and potential leakage.

> Detected anomalies are treated as potential incidents requiring investigation, not automatically confirmed leaks.

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
              │
              ▼
   Spark Structured Streaming
              │
        ┌─────┴─────┐
        ▼           ▼
     Parquet     PostgreSQL
 Historical      / PostGIS
   Storage        Serving
        │           │
        └─────┬─────┘
              ▼
     Analytics / GIS
```

Apache Airflow will later orchestrate scheduled workflows.

Selected components may be extended to AWS after the local platform has been implemented and validated.

---

## Current Implementation

The current local pipeline supports:

- replay of historical BattLeDIM measurements as sensor events;
- Kafka-based streaming ingestion;
- Spark Structured Streaming processing;
- explicit JSON schema parsing and structural validation;
- event-time processing with a 10-minute watermark;
- 15-minute sensor-level aggregations;
- Parquet historical event storage;
- Parquet rejected-event quarantine;
- PostgreSQL serving-layer persistence;
- PostgreSQL/PostGIS network and sensor reference data;
- persistent Spark checkpoints for restart/resume behaviour.

Current streaming metrics include reading count, average, minimum, and maximum values.

A controlled one-hour replay processes **12 BattLeDIM timestamps × 119 sensors = 1,428 sensor events**.

These events produce **476 sensor/window metric records** across four 15-minute windows.

---

## Data Source

The project uses the **BattLeDIM 2018 dataset** and the **L-Town EPANET water-distribution network model**.

BattLeDIM provides a benchmark water-distribution network and sensor dataset. The L-Town network is a hypothetical benchmark network and should not be interpreted as a real Rwandan water network.

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m3/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |

The SCADA datasets contain **105,120 timestamps** at 5-minute intervals throughout 2018.

Each timestamp produces **119 sensor events**.

The network model contains:

- 785 nodes;
- 909 links;
- 119 mapped sensors.

BattLeDIM leakage ground truth is kept separate from the streaming detector inputs so that it can later support anomaly-detection evaluation.

Raw source data is preserved unchanged and excluded from Git version control.

---

## Sensor Event Contract

Kafka messages use the following structure:

```text
event_id
event_time
sensor_id
sensor_type
value
unit
```

Supported sensor types and units:

```text
pressure → m
flow     → m3/h
level    → m
demand   → L/h
```

Kafka message keys use:

```text
<sensor_type>:<sensor_id>
```

Example:

```text
pressure:n1
```

Structural data validation is kept separate from anomaly detection. A structurally valid measurement is not automatically considered normal, and an anomaly is not automatically considered a confirmed leak.

---

## Storage Architecture

The platform separates storage responsibilities.

### Parquet Historical Layer

Structurally valid sensor events are written to:

```text
data/lake/sensor_events/
```

Rejected events are quarantined separately:

```text
data/lake/rejected_sensor_events/
```

The clean controlled replay produced **1,428 valid historical events** and zero rejected records.

### PostgreSQL / PostGIS Serving Layer

PostgreSQL stores:

- sensor metadata;
- network nodes;
- network links;
- 15-minute sensor metrics;
- metric staging data.

Validated reference-data counts:

```text
sensors          119
network_nodes    785
network_links    909
```

The controlled replay produced:

```text
metrics_staging  476
metrics_serving  476
```

Metrics are merged from staging into the serving table using an idempotent PostgreSQL upsert procedure.

PostGIS is enabled for future spatial analysis. Network geometry is retained without assigning an unsupported coordinate reference system until the source CRS is confirmed.

---

## Streaming Checkpoints

Spark uses persistent checkpoints for its historical-event and metric queries:

```text
data/checkpoints/events_archive/
data/checkpoints/metrics/
```

Checkpoint-based restart/resume behaviour has been validated.

After processing the controlled 1,428-event replay, restarting Spark with the same checkpoints did not duplicate the historical Parquet records.

This validation demonstrates checkpoint-based recovery behaviour in the current local implementation; it is not presented as a production exactly-once guarantee.

Generated lake data and checkpoint state are excluded from Git.

---

## Project Progress

### Phase 1 — Project Design ✅

Defined the business problem, target users, architecture, technology responsibilities, and initial anomaly-detection concept.

### Phase 2 — Data Acquisition & Understanding ✅

Validated the BattLeDIM SCADA datasets and L-Town network topology, including timestamps, measurements, sensor mappings, leakage data, and the streaming event contract.

### Phase 3 — Python Sensor Simulator ✅

Built a reusable simulator that converts historical SCADA measurements into chronological sensor events with deterministic IDs and standardized units.

### Phase 4 — Apache Kafka Streaming Ingestion ✅

Implemented local Kafka ingestion, JSON serialization, sensor-based message keys, simulator integration, and consumer validation.

The development environment currently uses a **single Kafka broker and one topic partition** for functional validation.

### Phase 5 — Spark Structured Streaming ✅

Implemented Kafka-to-Spark streaming, schema parsing, structural validation, event-time processing, watermarking, and 15-minute sensor metrics.

Spark currently runs in **local mode inside a Docker container**.

### Phase 6 — Persistent Storage ✅

Implemented:

- PostgreSQL/PostGIS serving storage;
- network and sensor reference-data loading;
- composite sensor identity handling;
- metric staging and idempotent upsert;
- Parquet historical event storage;
- rejected-event quarantine;
- persistent Spark checkpoints;
- end-to-end persistence validation;
- shared Docker networking for Kafka, PostgreSQL, and Spark.

---

## Current Technology Stack

### Implemented

- Python
- Pandas
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- PostGIS
- Parquet
- Docker

### Planned as requirements are introduced

- Apache Airflow
- anomaly detection / machine learning
- Power BI
- GIS analytics
- selected AWS services

Hadoop, MongoDB, and other technologies will be introduced only if later requirements justify their use.

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
│   ├── kafka/
│   │   └── docker-compose.yml
│   ├── postgres/
│   │   ├── docker-compose.yml
│   │   └── .env.example
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

---

## Local Setup

The current implementation requires Python, Docker, and Docker Compose.

Install Python dependencies:

```cmd
pip install -r requirements.txt
```

Create the PostgreSQL environment file from:

```text
infrastructure/postgres/.env.example
```

and provide the local database password in:

```text
infrastructure/postgres/.env
```

The real `.env` file is excluded from Git.

Create the shared Docker network once:

```cmd
docker network create smart-water-network
```

Start Kafka:

```cmd
docker compose -f infrastructure\kafka\docker-compose.yml up -d
```

Start PostgreSQL/PostGIS:

```cmd
docker compose --env-file infrastructure\postgres\.env -f infrastructure\postgres\docker-compose.yml up -d
```

Apply the storage schema:

```cmd
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\01_create_storage_schema.sql
```

Install the metric upsert procedure:

```cmd
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\02_upsert_sensor_metrics.sql
```

Load network and sensor reference data:

```cmd
python -m src.data.load_reference_data
```

Replay BattLeDIM measurements to Kafka:

```cmd
python -m src.simulator.sensor_simulator
```

Run Spark Structured Streaming:

```cmd
infrastructure\spark\run_spark_stream.cmd
```

Merge staged metrics into the serving table:

```cmd
docker exec smart-water-postgres psql -U smart_water_user -d smart_water -c "CALL upsert_sensor_metrics();"
```

Workflow orchestration will be introduced later rather than embedding orchestration responsibilities directly into the Spark processor.

---

## Validation

The controlled end-to-end pipeline has been validated with:

```text
Kafka sensor events       1,428
Parquet historical events 1,428
Sensors                     119
Network nodes               785
Network links               909
15-minute metrics           476
Rejected events               0
```

Representative pressure metrics for sensor `n1` were verified against the expected 15-minute calculations.

The PostgreSQL upsert was also rerun without increasing the serving-table row count, validating idempotent metric persistence for the controlled replay.

---

## Documentation

Detailed design and data decisions are maintained under `Docs/`:

- `project_design.md` — problem, users, architecture, and design decisions;
- `data_understanding.md` — SCADA, leakage, data-quality, and network-topology findings;
- `data_contract.md` — sensor-event schema and validation rules.

The README provides the high-level project view while detailed technical findings remain in the relevant documentation.

---

## Development Approach

```text
Understand data
      ↓
Define event contract
      ↓
Simulate sensor events
      ↓
Build streaming ingestion
      ↓
Process streaming data
      ↓
Persist operational and historical data
      ↓
Detect potential anomalies
      ↓
Add analytics and spatial context
      ↓
Extend selected components to AWS
```

Each technology is introduced only when it has a defined responsibility in the architecture.

---

## Next Phase

The next major platform capability will build on the validated streaming and storage foundation.

Upcoming work will introduce orchestration, anomaly-detection logic, analytics, GIS integration, testing, and selected cloud components incrementally rather than adding technologies without a defined requirement.

---

## Author

**GATERA Emile**  
Civil & Water Resources Engineer | Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with data engineering, streaming analytics, and geospatial analysis.