# Smart Water Network Intelligence Platform

## Overview

The **Smart Water Network Intelligence Platform** is an end-to-end data engineering project for processing water-distribution sensor data for network monitoring, anomaly detection, and operational analytics.

The project uses historical **BattLeDIM** water-network data to simulate continuous pressure, flow, tank-level, and demand measurements and progressively builds a real-time streaming data platform around them.

The goal is to transform raw sensor measurements into validated, structured, and analysis-ready data that can support the investigation of abnormal network behaviour and potential leakage.

> Detected anomalies are treated as potential incidents requiring investigation, not automatically confirmed leaks.

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

## Current Implementation

The project currently has a working streaming ingestion path:

```text
BattLeDIM SCADA
      ↓
Python Sensor Simulator
      ↓
Event Validation
      ↓
Kafka Producer
      ↓
water-sensor-events
      ↓
Kafka Consumer
      ↓
Event Validation
```

The simulator converts historical measurements into individual sensor events and publishes them to Apache Kafka as JSON messages.

Kafka message keys use:

```text
<sensor_type>:<sensor_id>
```

Example:

```text
pressure:n1
```

This keeps sensor identity explicit and supports sensor-level ordering when the topic is partitioned by key in future iterations.

## Data Source

The initial implementation uses the **BattLeDIM 2018 dataset** and the **L-Town EPANET water-distribution network model**.

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m3/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |

The SCADA datasets contain **105,120 timestamps** at 5-minute intervals throughout 2018.

BattLeDIM also provides leakage ground truth and network topology information for later anomaly-detection evaluation and network-aware analysis.

Raw source data is preserved unchanged and excluded from Git version control.

## Project Progress

### Phase 1 — Project Design ✅

Defined:

- business and operational problem;
- target users and decisions;
- system outputs;
- initial anomaly-detection concept;
- technology responsibilities;
- core platform architecture.

### Phase 2 — Data Acquisition & Understanding ✅

Validated the BattLeDIM source data and L-Town network model.

Key findings:

- 105,120 timestamps per SCADA dataset;
- complete 5-minute measurement sequence throughout 2018;
- no missing or duplicate timestamps;
- no missing sensor measurements;
- 33/33 pressure sensors mapped to network nodes;
- 3/3 flow sensors mapped to network links;
- 1/1 tank-level sensor mapped to a network node;
- 14/14 leakage identifiers mapped to network links.

A streaming sensor-event data contract was also defined.

### Phase 3 — Python Sensor Simulator ✅

Built a reusable simulator that converts historical SCADA measurements into sequential sensor events.

Each timestamp produces **119 events**:

- 33 pressure;
- 3 flow;
- 1 tank-level;
- 82 demand.

The simulator provides:

- deterministic event IDs;
- standardized sensor types and units;
- event-contract validation;
- batch-level event-ID uniqueness checks;
- chronological replay;
- configurable replay speed;
- timestamp-by-timestamp processing to avoid loading the complete event history into memory.

### Phase 4 — Apache Kafka Streaming Ingestion ✅

Implemented the streaming ingestion layer between the sensor simulator and downstream processing.

Completed:

- deployed Apache Kafka locally using Docker;
- created the `water-sensor-events` topic;
- implemented a reusable Python Kafka producer;
- serialized sensor events as JSON;
- keyed messages by `sensor_type:sensor_id`;
- integrated the BattLeDIM simulator with Kafka;
- implemented a Python validation consumer;
- verified JSON deserialization and event-contract validation;
- verified Kafka partition and offset behaviour.

Integration testing successfully published a full timestamp batch of **119 BattLeDIM sensor events** to Kafka and consumed the resulting messages with valid sensor keys and event structures.

The current development environment uses a **single Kafka broker and one topic partition** for local functional validation. It is not intended to represent a production fault-tolerant Kafka cluster.

## Next Phase

### Phase 5 — Spark Structured Streaming

The next phase will introduce Spark Structured Streaming as the real-time processing layer.

Spark will consume events from Kafka and provide the foundation for:

- streaming schema enforcement;
- event-time processing;
- data-quality handling;
- stream transformations;
- operational metrics;
- later anomaly-detection logic.

This separates responsibilities clearly:

```text
Kafka → ingest and transport events
Spark → process and analyze events
```

## Core Technologies

**Implemented**

- Python
- Apache Kafka
- Docker
- Pandas

**Planned as the platform evolves**

- Apache Spark
- PostgreSQL / PostGIS
- Parquet
- Apache Airflow
- Power BI
- GIS
- AWS

Machine learning, Hadoop, MongoDB, and WNTR will be introduced only if later requirements justify their use.

## Project Structure

```text
Smart-water-network-platform/
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
│   └── kafka/
│       └── docker-compose.yml
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
│       └── kafka_consumer.py
│
├── .gitignore
├── requirements.txt
└── README.md
```

## Documentation

Detailed design and data decisions are kept under `Docs/`:

- `project_design.md` — problem, users, architecture, and design decisions.
- `data_understanding.md` — SCADA, leakage, data-quality, and network-topology findings.
- `data_contract.md` — sensor-event schema and validation rules.

The README intentionally provides only the high-level project view while detailed technical findings remain in the relevant documentation.

## Development Approach

The platform is developed incrementally:

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
Build analytics and spatial context
    ↓
Extend selected components to AWS
```

Each technology is introduced only when it has a defined responsibility in the architecture.

## Author

**GATERA Emile**  
Civil & Water Resources Engineer | Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with data engineering, streaming analytics, and geospatial analysis.