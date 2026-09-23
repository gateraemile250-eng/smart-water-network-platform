# Smart Water Network Intelligence Platform

## Overview

Smart Water Network Intelligence Platform is an end-to-end data engineering project for transforming water-distribution sensor measurements into reliable information for network monitoring, anomaly detection, and operational analysis.

The project uses historical BattLeDIM water-network data to simulate continuous pressure, flow, tank-level, and demand sensor events and progressively build a real-time streaming data platform.

The platform combines water-infrastructure domain knowledge with data engineering, streaming analytics, and geospatial analysis to identify abnormal network behaviour that may indicate potential leakage or other conditions requiring investigation.

## Problem

Water-distribution networks can generate continuous measurements from pressure, flow, tank-level, demand, and other monitoring devices.

Raw sensor readings must be validated, processed, associated with network assets, and analyzed over time before they can effectively support operational decisions.

The platform is designed to produce:

- validated sensor data;
- potential anomaly alerts;
- network and spatial context;
- historical analytical datasets;
- operational dashboards and GIS-based analysis.

Detected anomalies represent potential incidents requiring investigation rather than automatically confirmed leaks.

## Architecture

The planned core data flow is:

**Historical water-network data → Python sensor simulator → Apache Kafka → Spark Structured Streaming → PostgreSQL/PostGIS and Parquet data lake → analytics and GIS**

Apache Airflow will later orchestrate scheduled batch workflows.

Selected components may be extended to AWS after the local platform is implemented and validated.

## Core Technologies

- Python
- Apache Kafka
- Apache Spark
- PostgreSQL/PostGIS
- Apache Airflow
- Docker
- Parquet
- Power BI
- GIS
- AWS

Additional technologies such as machine learning, Hadoop, MongoDB, and WNTR will be evaluated later and included only where they provide a clear technical purpose.

## Data Source

The initial platform uses the **BattLeDIM 2018 historical dataset** and the **L-Town EPANET water-distribution network model**.

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m3/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |

Measurements are recorded at **5-minute intervals throughout 2018**, providing 105,120 timestamps per SCADA dataset.

BattLeDIM also provides leakage ground truth and network topology information for later anomaly-detection evaluation and network-aware analysis.

Raw source data is preserved unchanged and excluded from Git version control.

## Current Status

### Phase 1 — Project Design ✅

Completed:

- Defined the business and operational problem.
- Identified target users and operational decisions.
- Defined expected system outputs.
- Defined the initial anomaly-detection concept.
- Mapped system requirements to technologies.
- Defined the initial platform architecture.

### Phase 2 — Data Acquisition and Understanding ✅

Completed:

- Acquired and investigated the BattLeDIM 2018 source data.
- Analyzed pressure, flow, tank-level, and demand measurements.
- Investigated leakage ground truth and event behaviour.
- Assessed SCADA data quality and temporal consistency.
- Inspected the L-Town EPANET network topology.
- Verified sensor and leakage identifiers against network assets.
- Defined the streaming sensor-event data contract.

Key validation results:

- 105,120 timestamps per SCADA dataset;
- complete 5-minute measurement grid throughout 2018;
- zero missing or duplicate timestamps;
- zero missing measurements;
- 33/33 pressure sensors mapped to network nodes;
- 3/3 flow sensors mapped to network links;
- 1/1 tank-level sensor mapped to a network node;
- 14/14 leakage identifiers mapped to network links.

### Phase 3 — Python Sensor Simulator ✅

Implemented a reusable Python simulator that transforms historical BattLeDIM SCADA measurements into validated sensor events suitable for streaming ingestion.

Completed:

- Converted wide SCADA measurements into individual sensor events.
- Standardized pressure, flow, tank-level, and demand events using the data contract.
- Created deterministic event identifiers using sensor type, sensor ID, and timestamp.
- Added event-level structural and unit validation.
- Added batch validation and event-ID uniqueness checks.
- Preserved the original 5-minute chronological measurement sequence.
- Implemented configurable accelerated replay of historical measurements.
- Processed measurements one timestamp batch at a time instead of materializing the full event history in memory.

Each BattLeDIM timestamp currently produces **119 sensor events**:

- 33 pressure events;
- 3 flow events;
- 1 tank-level event;
- 82 demand events.

The simulator was tested across sequential timestamp batches with all generated events passing contract validation and event-ID uniqueness checks.

## Next Phase

### Phase 4 — Apache Kafka Streaming Ingestion

The next phase will connect the Python sensor simulator to Apache Kafka.

Kafka will provide the streaming ingestion layer between the simulated sensor source and downstream stream-processing components.

The phase will focus on:

- defining the Kafka event-ingestion design;
- producing simulator events to Kafka;
- serializing events for transport;
- configuring topics and partitioning based on pipeline requirements;
- consuming and validating streamed events;
- verifying ordered end-to-end event delivery.

Spark Structured Streaming will be introduced after the Kafka ingestion layer is working and validated.

## Documentation

Project documentation is maintained under `Docs/`:

- `Docs/project_design.md` — business problem, users, requirements, architecture, and design decisions.
- `Docs/data_understanding.md` — BattLeDIM data, leakage, data-quality, and network-topology findings.
- `Docs/data_contract.md` — streaming sensor-event schema and validation rules.

## Project Structure

```text
Smart-water-network-platform/
├── Docs/
│   ├── project_design.md
│   ├── data_understanding.md
│   └── data_contract.md
├── data/
│   ├── raw/
│   │   └── battledim/
│   └── reference/
├── src/
│   ├── data/
│   │   └── battledim_loader.py
│   ├── exploration/
│   │   ├── inspect_battledim.py
│   │   ├── analyze_sensor_measurements.py
│   │   ├── plot_sensor_timeseries.py
│   │   ├── analyze_leakages.py
│   │   ├── assess_data_quality.py
│   │   └── inspect_network_topology.py
│   └── simulator/
│       └── sensor_simulator.py
├── .gitignore
├── requirements.txt
└── README.md
```

The project structure expands incrementally as new platform components are implemented.

## Development Approach

The platform is being developed incrementally, with each technology introduced only when required by the architecture.

**Understand the data → define the event contract → simulate sensor events → build streaming ingestion → process and validate streams → store operational and historical data → detect anomalies → provide analytics and spatial context → extend selected components to the cloud**

This approach keeps the project focused on engineering requirements, reproducibility, and clear separation of responsibilities rather than adding technologies without a defined purpose.

---

## Author

**GATERA Emile**  
Civil & Water Resources Engineer | Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with data engineering, streaming analytics, and geospatial technologies to explore data-driven monitoring of water-distribution networks.