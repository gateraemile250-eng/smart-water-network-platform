# Smart Water Network Intelligence Platform

## Overview

Smart Water Network Intelligence Platform is an end-to-end data engineering project designed to transform water-distribution sensor measurements into reliable information for network monitoring, anomaly detection, and operational analysis.

The project uses historical BattLeDIM water-network data to simulate continuous sensor events and progressively build a streaming data platform for processing pressure, flow, tank-level, and demand measurements.

The platform combines data engineering with water-network and geospatial analysis to investigate abnormal network behaviour that may indicate potential leakage or other operational conditions requiring investigation.

## Problem

Water-distribution networks can generate continuous measurements from pressure, flow, tank-level, demand, and other monitoring devices.

Raw sensor readings alone are not sufficient for effective network monitoring. Measurements must be validated, processed, connected with network assets, and analyzed over time before they can support operational decisions.

This project aims to build a platform capable of transforming continuous sensor measurements into:

- validated sensor data;
- potential anomaly alerts;
- network and spatial context;
- historical analytical datasets;
- operational dashboards and GIS-based analysis.

Detected anomalies will represent potential incidents requiring investigation rather than automatically confirmed leaks.

## Architecture

The planned core data flow is:

**Historical water-network data → Python sensor simulator → Apache Kafka → Spark Structured Streaming → PostgreSQL/PostGIS and Parquet data lake → analytics and GIS**

Apache Airflow will later orchestrate scheduled batch workflows.

Selected components may later be extended to AWS after the local platform is implemented and validated.

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

Additional technologies such as machine learning, Hadoop, MongoDB, and WNTR will be evaluated in later phases and included only where they provide a clear technical purpose.

## Data Source

The initial platform uses the **BattLeDIM 2018 historical dataset** and the **L-Town EPANET water-distribution network model**.

The historical SCADA data contains measurements for:

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m³/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |

Measurements are recorded at **5-minute intervals throughout 2018**, producing 105,120 timestamps per SCADA dataset.

BattLeDIM also provides leakage ground truth and network topology information that will support later anomaly-detection evaluation and network-aware analysis.

Raw source data is preserved unchanged and excluded from Git version control.

## Current Status

### Phase 1 — Project Design ✅

Completed:

- Defined the business and operational problem.
- Identified the target client and system users.
- Defined the decisions the platform should support.
- Defined expected system outputs.
- Defined the initial leak-detection concept.
- Mapped core requirements to technologies.
- Defined the initial system architecture.

### Phase 2 — Data Acquisition and Understanding ✅

Completed:

- Acquired the required BattLeDIM 2018 source files.
- Investigated SCADA dataset structure and measurement frequency.
- Analyzed pressure, flow, tank-level, and demand measurements.
- Investigated leakage ground truth and leakage-event behaviour.
- Distinguished abrupt and incipient leakage events.
- Assessed source-data quality.
- Verified a complete 5-minute measurement grid throughout 2018.
- Inspected the L-Town EPANET network topology.
- Verified sensor and leakage identifiers against network assets.
- Confirmed availability of network coordinate information.
- Defined the initial streaming sensor data contract.

### Key Phase 2 Validation Results

The 2018 SCADA datasets contain:

- zero missing timestamps;
- zero duplicate timestamps;
- zero unexpected 5-minute intervals;
- zero missing measurements;
- zero negative measurements.

Network relationship validation confirmed:

- **33/33** pressure sensors mapped to network nodes;
- **3/3** flow sensors mapped to network links;
- **1/1** tank-level sensor mapped to a network node;
- **14/14** leakage identifiers mapped to network links.

These results establish a reliable historical source for the next stage of sensor-event simulation.

## Next Phase

### Phase 3 — Python Sensor Simulator

The next phase will transform the historical BattLeDIM SCADA measurements from wide CSV datasets into individual sensor events following the defined streaming data contract.

The simulator will replay historical measurements sequentially to reproduce the behaviour of continuously arriving water-network sensor data.

This simulated event stream will later become the input to Apache Kafka and the streaming processing pipeline.

## Documentation

Project documentation is maintained under `Docs/`:

- `Docs/project_design.md` — business problem, users, requirements, architecture, and design decisions.
- `Docs/data_understanding.md` — BattLeDIM data, leakage, data-quality, and network-topology findings.
- `Docs/data_contract.md` — initial streaming sensor-event schema and validation rules.

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
│   └── exploration/
│       ├── inspect_battledim.py
│       ├── analyze_sensor_measurements.py
│       ├── plot_sensor_timeseries.py
│       ├── analyze_leakages.py
│       ├── assess_data_quality.py
│       └── inspect_network_topology.py
├── .gitignore
└── README.md
```

The project structure will expand incrementally as new platform components are implemented.

## Development Approach

The project is being developed incrementally, with each technology introduced only when required by the system architecture.

The current progression is:

**Understand the data → define the event contract → simulate sensor events → build streaming ingestion → process and validate streams → store operational and historical data → detect anomalies → provide analytics and spatial context → extend selected components to the cloud**

This approach keeps the project focused on engineering requirements rather than adding technologies without a clear purpose.

---

## Author

**GATERA Emile**  
Civil & Water Resources Engineer | Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with data engineering, streaming analytics, and geospatial technologies to explore data-driven monitoring of water-distribution networks.