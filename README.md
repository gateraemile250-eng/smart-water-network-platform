# Smart Water Network Intelligence Platform

## Overview

The **Smart Water Network Intelligence Platform** is an end-to-end data engineering project for processing water-distribution sensor data for network monitoring, anomaly detection, and operational analytics.

The platform uses historical **BattLeDIM** water-network data to simulate continuous pressure, flow, tank-level, and demand measurements and progressively builds a real-time streaming architecture around them.

The goal is to transform raw sensor measurements into validated, structured, and analysis-ready data that can support investigation of abnormal network behaviour and potential leakage.

> Detected anomalies are treated as potential incidents requiring investigation, not automatically confirmed leaks.

---

## Architecture

Current and planned core data flow:

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
              ▼
 PostgreSQL / PostGIS + Parquet
              │
              ▼
    Analytics / Dashboard / GIS
```

Apache Airflow will later orchestrate scheduled batch workflows.

Selected components may be extended to AWS after the local platform has been implemented and validated.

---

## Current Implementation

The current implementation provides a working local streaming pipeline:

```text
BattLeDIM SCADA
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
      ├── JSON schema parsing
      ├── event-time conversion
      ├── structural validation
      ├── watermark handling
      └── 15-minute sensor metrics
```

Historical BattLeDIM measurements are replayed as individual sensor events and published to Kafka as JSON messages.

Spark consumes the Kafka stream, applies an explicit event schema, safely converts event timestamps, validates event structure and units, and calculates event-time windowed metrics.

Current streaming metrics include:

- reading count;
- average value;
- minimum value;
- maximum value.

Metrics are calculated per sensor using **15-minute tumbling windows** with a **10-minute event-time watermark**.

This processing layer provides the foundation for later storage, operational analytics, and anomaly-detection logic.

---

## Data Source

The initial implementation uses the **BattLeDIM 2018 dataset** and the **L-Town EPANET water-distribution network model**.

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m3/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |

The SCADA datasets contain **105,120 timestamps** at 5-minute intervals throughout 2018.

Each timestamp produces **119 sensor events**.

BattLeDIM also provides leakage ground truth and network-topology information for later anomaly-detection evaluation and network-aware analysis.

Raw source data is preserved unchanged and excluded from Git version control.

---

## Sensor Event Contract

Kafka messages use a consistent sensor-event structure:

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

Kafka message keys use:

```text
<sensor_type>:<sensor_id>
```

Example:

```text
pressure:n1
```

This keeps sensor identity explicit and supports sensor-level ordering when the topic is partitioned by key in future iterations.

Structural data validation is kept separate from anomaly detection. A structurally valid measurement is not automatically considered normal, and an anomaly is not automatically considered a confirmed leak.

---

## Project Progress

### Phase 1 — Project Design ✅

Defined the business problem, target users, system outputs, initial anomaly-detection concept, technology responsibilities, and core platform architecture.

### Phase 2 — Data Acquisition & Understanding ✅

Validated the BattLeDIM source data and L-Town network model.

Key findings:

- 105,120 timestamps per SCADA dataset;
- complete 5-minute measurement sequence throughout 2018;
- no missing or duplicate timestamps;
- no missing sensor measurements;
- all selected pressure, flow, level, and leakage identifiers mapped to the network topology;
- streaming sensor-event data contract defined.

### Phase 3 — Python Sensor Simulator ✅

Built a reusable simulator that converts historical SCADA measurements into chronological sensor events.

The simulator provides deterministic event IDs, standardized sensor types and units, event-contract validation, batch-level uniqueness checks, configurable replay speed, and timestamp-by-timestamp processing.

### Phase 4 — Apache Kafka Streaming Ingestion ✅

Implemented Kafka as the streaming ingestion layer between the simulator and downstream processing.

Completed:

- local Kafka deployment using Docker;
- `water-sensor-events` topic;
- reusable Python Kafka producer;
- JSON event serialization;
- sensor-based Kafka message keys;
- simulator-to-Kafka integration;
- validation consumer;
- Kafka partition and offset verification.

The development environment currently uses a **single Kafka broker and one topic partition** for local functional validation. It is not intended to represent a production fault-tolerant Kafka cluster.

### Phase 5 — Spark Structured Streaming ✅

Implemented Spark Structured Streaming as the real-time processing layer.

Completed:

- containerized Spark 4.1.3 runtime;
- Kafka-to-Spark integration;
- explicit JSON schema parsing;
- safe event-time conversion;
- structural event validation;
- separation of valid and rejected-event logic;
- event-time watermarking;
- 15-minute tumbling-window aggregation;
- per-sensor count, average, minimum, and maximum metrics;
- configurable Kafka bootstrap address;
- reusable Spark launcher;
- end-to-end validation from BattLeDIM replay through Kafka to Spark metrics.

A clean integration test replayed **12 BattLeDIM timestamps**, producing **1,428 sensor events** and successfully generating event-time windowed metrics across pressure, flow, level, and demand measurements.

Spark currently runs in **local mode inside a Docker container**, and the console output is used as a development sink. Persistent streaming storage and durable checkpointing will be introduced when the storage layer is implemented.

---

## Current Technology Stack

### Implemented

- Python
- Pandas
- Apache Kafka
- Apache Spark Structured Streaming
- Docker

### Planned as requirements are introduced

- PostgreSQL / PostGIS
- Parquet
- Apache Airflow
- Power BI
- GIS
- AWS

Machine learning, Hadoop, MongoDB, and WNTR will be introduced only if later requirements justify their use.

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
│   │   └── battledim/
│   └── reference/
│
├── infrastructure/
│   ├── kafka/
│   │   └── docker-compose.yml
│   └── spark/
│       └── run_spark_stream.cmd
│
├── src/
│   ├── data/
│   │   └── battledim_loader.py
│   │
│   ├── exploration/
│   │   ├── inspect_battledim.py
│   │   ├── analyze_sensor_measurements.py
│   │   ├── plot_sensor_timeseries.py
│   │   ├── analyze_leakages.py
│   │   ├── assess_data_quality.py
│   │   └── inspect_network_topology.py
│   │
│   ├── simulator/
│   │   └── sensor_simulator.py
│   │
│   └── streaming/
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       └── spark_stream_processor.py
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Running the Current Streaming Pipeline

The current local workflow requires Docker and the Python environment configured for the project.

Start Kafka:

```cmd
docker compose -f infrastructure\kafka\docker-compose.yml up -d
```

Replay BattLeDIM sensor measurements to Kafka:

```cmd
python -m src.simulator.sensor_simulator
```

Run the Spark streaming processor:

```cmd
infrastructure\spark\run_spark_stream.cmd
```

The Spark processor consumes the Kafka events and displays event-time windowed sensor metrics.

---

## Documentation

Detailed design and data decisions are maintained under `Docs/`:

- `project_design.md` — problem, users, architecture, and design decisions;
- `data_understanding.md` — SCADA, leakage, data-quality, and network-topology findings;
- `data_contract.md` — sensor-event schema and validation rules.

The README provides the high-level project view while detailed technical findings remain in the relevant documentation.

---

## Development Approach

The platform is being developed incrementally:

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
Store operational and historical data
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

The next phase will introduce the **storage layer** for streaming and historical data.

The objective is to persist processed measurements and analytical outputs in formats suited to their responsibilities, with PostgreSQL/PostGIS and Parquet evaluated as the primary storage components.

Storage design will be defined before implementation so that technologies are introduced based on data-access and operational requirements rather than added only for tool coverage.

---

## Author

**GATERA Emile**  
Civil & Water Resources Engineer | Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with data engineering, streaming analytics, and geospatial analysis.