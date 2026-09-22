# Smart Water Network Intelligence Platform

## Overview

Smart Water Network Intelligence Platform is a data engineering project designed to process simulated water-distribution sensor data and transform it into reliable information for network monitoring, anomaly detection, and operational analysis.

The platform will focus on pressure and flow measurements and will investigate how continuous sensor data can be used to identify abnormal network behaviour that may indicate potential water leaks.

The project is being developed as an end-to-end portfolio project combining streaming data engineering, water-network analytics, geospatial analysis, and cloud technologies.

## Problem

Water utilities can generate continuous measurements from pressure, flow, reservoir, and other network sensors.

Raw sensor readings alone are not sufficient for effective monitoring. They must be validated, processed, analyzed, and connected with network and geographic information before they can support operational decisions.

This project will develop a data platform that can transform continuous sensor readings into validated data, potential anomaly alerts, and historical analytical information.

## Planned Architecture

The initial architecture is:

**Historical water-network data → Python sensor simulator → Apache Kafka → Spark Structured Streaming → PostgreSQL/PostGIS and Parquet data lake → analytics and GIS**

Apache Airflow will later orchestrate scheduled batch workflows.

Selected components will eventually be extended to AWS after the local pipeline is working and validated.

## Planned Technologies

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

Additional technologies such as MongoDB, Hadoop, and machine learning will be evaluated later where they provide a clear purpose.

## Data Sources

The project plans to use:

- BattLeDIM water-distribution network data;
- EPANET network information;
- WNTR for additional water-network simulation scenarios where required.

The datasets and network structure will be investigated during the data-understanding phase before the final event schema and detection rules are defined.

## Current Status

**Phase 1 — Project Design**

Completed:

- Defined the business and operational problem.
- Identified the target client and system users.
- Defined the decisions the platform should support.
- Defined the initial sensor-event concept.
- Defined expected system outputs.
- Defined the initial leak-detection approach.
- Mapped core requirements to technologies.
- Defined the initial system architecture.

## Next Phase

**Phase 2 — Data Acquisition and Understanding**

The next stage will focus on obtaining and exploring BattLeDIM and related water-network data before building the sensor simulator or streaming pipeline.

## Documentation

Detailed project-design decisions are available in:

`Docs/project_design.md`