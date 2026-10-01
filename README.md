# Smart Water Network Intelligence Platform

An end-to-end **data engineering, machine-learning, network analytics,
and GIS platform** for processing water-distribution sensor data,
detecting abnormal network behaviour, interpreting sensor evidence,
ranking candidate network assets, and presenting operational
intelligence through an interactive dashboard.

> **Important:** This is a simulation/benchmark project using the
> BattLeDIM 2018 / L-Town water-network dataset. It is **not a live
> monitoring system for a real Rwandan water network**, and anomaly
> results must not be interpreted as confirmed leaks.

## 1. Project Overview

The platform demonstrates a complete engineering workflow:

``` text
Historical SCADA Data
        |
        v
Python Sensor Simulator
        |
        v
Apache Kafka
        |
        v
Spark Structured Streaming
        |
        +-------------> Parquet
        |
        v
PostgreSQL / PostGIS
        |
        v
Apache Airflow
        |
        +-------------> Anomaly Detection
        |                      |
        |                      v
        |               Sensor Interpretation
        |                      |
        |                      v
        |               Network Localization
        |
        v
Dashboard Serving Views
        |
        v
Streamlit Dashboard
        |
        +-- Network Overview
        +-- Anomaly Monitoring
        +-- Network Localization
```

The architecture separates ingestion, processing, storage,
orchestration, analytics, localization, GIS, and presentation
responsibilities.

## 2. Architecture

### Overall Architecture

![Smart Water Network Intelligence Platform
Architecture](docs/images/smart_water_architecture.png)

``` text
BattLeDIM / L-Town Historical SCADA + Network Reference
                         |
                         v
                Python Sensor Simulator
                         |
                         v
                    Apache Kafka
                 water-sensor-events
                         |
                         v
             Spark Structured Streaming
               | validation / event time
               | watermark / aggregation
               +--------------------+
               |                    |
               v                    v
            Parquet            PostgreSQL
            Storage            Operational Data
                                    |
                                    v
                             Apache Airflow
                                    |
                       +------------+------------+
                       |                         |
                       v                         v
                Anomaly Detection       Network Localization
                  Baseline + ML          + Network Topology
                       |                         |
                       +------------+------------+
                                    |
                                    v
                           PostgreSQL / PostGIS
                           Analytical Serving
                               |        |
                               v        v
                           Streamlit   QGIS
                           Dashboard   Spatial View
```

### Component Responsibilities

  -----------------------------------------------------------------------
  Component                           Responsibility
  ----------------------------------- -----------------------------------
  Python                              Sensor simulation, feature
                                      engineering, anomaly analysis,
                                      localization

  Apache Kafka                        Streaming sensor-event ingestion

  Apache Spark                        Validation, event-time processing,
                                      aggregation, persistence

  Parquet                             Historical valid-event storage and
                                      rejected-event quarantine

  PostgreSQL                          Operational, analytical, serving,
                                      anomaly, and localization data

  PostGIS                             Network geometry and spatial
                                      serving

  Apache Airflow                      Scheduling, orchestration, retries,
                                      and validation

  scikit-learn                        Isolation Forest anomaly detection

  Network topology                    Sensor-to-network attribution and
                                      candidate-pipe ranking

  QGIS                                Spatial investigation of
                                      localization results

  Streamlit                           Interactive operational analytics
                                      dashboard

  Docker                              Reproducible infrastructure and
                                      execution environments

  Git / GitHub                        Version control and project
                                      publication
  -----------------------------------------------------------------------

## 3. Data Source and Simulation Model

The project uses the **BattLeDIM 2018 dataset** together with the
**L-Town EPANET water-distribution network model**.

L-Town is a benchmark network and should not be interpreted as a real
Rwandan water network.

### SCADA Measurements

  Measurement     Sensors
  ------------- ---------
  Pressure             33
  Flow                  3
  Tank level            1
  Demand               82
  **Total**       **119**

The 2018 SCADA data contains **105,120 timestamps at 5-minute
intervals**, with 119 measurements at each timestamp.

### Network Reference Data

``` text
Sensors          119
Network nodes    785
Network links    909
Pipes            905
Pump               1
Valves             3
```

All 119 sensors are mapped to network assets. The source network uses
the BattLeDIM/L-Town **local model coordinate system (SRID 0)**; no
unsupported geographic CRS is assigned.

Known leakage information is kept separate from detector and
localization inputs and is used for evaluation rather than model
training or threshold tuning.

Raw source data is preserved unchanged and excluded from Git.

## 4. Streaming Data Pipeline

The Python simulator converts historical SCADA observations into
chronological sensor events and publishes them to:

``` text
water-sensor-events
```

Spark Structured Streaming performs:

``` text
Kafka ingestion
      |
      v
JSON parsing
      |
      v
Schema validation
      |
      v
Structural validation
      |
      v
Event-time processing
      |
      v
10-minute watermark
      |
      v
15-minute sensor aggregation
      |
      +-------------> Parquet
      |
      v
PostgreSQL persistence
```

A controlled validation replay processed:

``` text
1,428 sensor events
1,428 valid historical events
0 rejected events
476 staging metrics
476 serving metrics
```

Repeated serving-layer execution remained at **476 records**,
demonstrating idempotent persistence for the validated workload.

## 5. Network Anomaly Detection

Phase 8 extended the platform into network-level anomaly intelligence.

Two complementary detectors were implemented.

### Statistical Baseline

The explainable baseline uses causal weekly-deviation statistics. A
sensor is considered abnormal when:

``` text
|z-score| >= 3
```

A network anomaly is triggered when at least:

``` text
15 of 119 sensors
```

are simultaneously abnormal.

Validated result:

``` text
Anomaly timestamps    1,128
Anomaly rate           1.12%
```

### Isolation Forest

The unsupervised model uses 357 causal features:

``` text
119 x 5-minute changes
119 x 1-hour changes
119 x weekly deviations
```

Configuration:

``` text
n_estimators = 200
contamination = "auto"
random_state = 42
```

The anomaly threshold is fixed from the **99th percentile of
training-period scores**, without using future scoring data or leakage
labels.

Validated result:

``` text
Threshold             0.521368
Anomaly timestamps       5,897
Anomaly rate               5.83%
```

### Ground-Truth Evaluation

Both detectors were evaluated against **14 known BattLeDIM leakage
events**.

  Evaluation                    Baseline   Isolation Forest
  --------------------------- ---------- ------------------
  Anomaly timestamps               1,128              5,897
  Anomaly rate                     1.12%              5.83%
  Alert within 24 h               8 / 14            13 / 14
  Alert within 72 h              12 / 14            14 / 14
  Mean first-alert delay         30.08 h             7.10 h
  Median first-alert delay       15.50 h             4.79 h
  Maximum first-alert delay     107.25 h            33.00 h

The results demonstrate an operational trade-off between earlier warning
and alert volume. They are not presented as evidence that one detector
is universally superior.

## 6. Interpretation, Localization and GIS

Phase 9 extends anomaly detection into interpretable network decision
support.

The objective is to move from:

``` text
"An anomaly exists"
```

toward:

``` text
"Which sensors explain the anomaly,
and which parts of the network should be investigated?"
```

### Sensor-Level Interpretation

Isolation Forest provides a network-level anomaly score but does not
directly identify the responsible sensor. The localization layer
therefore uses causal statistical sensor evidence to identify abnormal
measurements.

``` text
Detection
    |
    v
Interpretation
    |
    v
Localization
```

### Sensor-to-Network Attribution

The physical network is represented as a graph using its nodes and
links. The localization system implements:

-   sensor-to-node/link mapping;
-   graph construction;
-   shortest-hop distance calculation;
-   cached topology distances;
-   node/link sensor attribution.

### Candidate Pipe Localization

Candidate pipes are ranked using a transparent heuristic combining
sensor abnormality and network distance:

``` text
                 |sensor z-score|
contribution = --------------------
                1 + topology distance
```

The system persists the **Top 25 candidate pipes** for each localizable
ML anomaly timestamp.

The ranking is an investigation aid. It is **not interpreted as the
probability that a pipe is leaking**.

### Localization Evaluation

  Metric                                           Result
  ------------------------------------- -----------------
  Known leakage events                                 14
  Localization available                          13 / 14
  Exact Top-1 matches                                   0
  Top-5 matches                                         0
  Top-10 matches                                        0
  Top-25 matches                                        4
  Median true-pipe rank                               160
  Median Rank-1 distance to true pipe     25 network hops

The evaluation shows that the current topology-weighted heuristic can
produce candidate network assets, but it is not reliable enough for
exact-pipe leak localization. This limitation is explicitly documented
rather than overstating model performance.

### GIS Integration

Localization results are joined to PostGIS network geometry through the
QGIS serving view.

The QGIS project visualizes:

-   the complete pipe network;
-   anomaly-specific candidate pipes;
-   candidate rank;
-   Top-5 candidate labels.

Because the source network uses local/model coordinates, the
visualization intentionally avoids assigning an unsupported geographic
CRS.

### QGIS Map Preview

![Smart Water Network Localization Map](gis/smart_water_localization_map.png)

## 7. Operational Analytics Dashboard

Phase 10 adds an interactive **Streamlit operational analytics layer**
on top of the validated PostgreSQL serving data.

The dashboard is a decision-support interface rather than a replacement
for the analytical pipeline.

### Dashboard Structure

``` text
Smart Water Network Intelligence
|
+-- Network Overview
|   +-- 119 monitored sensors
|   +-- 785 network nodes
|   +-- 909 network links
|   +-- 905 pipes
|
+-- Anomaly Monitoring
|   +-- anomaly totals
|   +-- ML anomaly rate
|   +-- baseline anomaly activity
|   +-- anomaly timeline
|   +-- investigation filters
|   +-- selected-event evidence
|
+-- Network Localization
    +-- localized anomaly count
    +-- localization coverage
    +-- candidate-pipe activity
    +-- selected anomaly timestamp
    +-- ranked candidate network assets
```

### Dashboard Serving Layer

Dashboard-specific PostgreSQL views are created in:

``` text
sql/06_create_dashboard_views.sql
```

The views are:

``` text
dashboard_anomaly_daily
dashboard_anomaly_detail
dashboard_localization_summary
dashboard_localization_candidates
```

This creates a clean serving boundary between analytical tables and
dashboard queries.

### Validated Dashboard Results

#### Network

``` text
Sensors          119
Network nodes    785
Network links    909
Pipes            905
```

#### Anomaly Intelligence

``` text
Scored timestamps             101,088
ML anomaly timestamps           5,897
ML anomaly rate                   5.83%
Baseline anomaly timestamps      1,128
Baseline anomaly rate              1.12%
```

Analysis period:

``` text
2018-01-15 -> 2018-12-31
```

#### Localization Intelligence

``` text
ML anomaly timestamps          5,897
Localized timestamps           4,868
Unlocalized anomalies          1,029
Localization coverage         82.55%
Localization records        121,700
Distinct candidate pipes        495
Candidates per localized event   25
```

The dashboard includes daily anomaly activity. The highest ML anomaly
activity in the validated daily dataset occurred on **2018-07-19**, with
76 ML anomaly timestamps and a 26.39% daily ML anomaly rate. Other
high-activity periods occurred during May-August 2018.

These results indicate periods of elevated detected anomaly activity.
They do not by themselves establish the physical cause of the anomalies.

### Dashboard Implementation

``` text
dashboard/
├── app.py
├── app_overview.py
├── db.py
├── queries.py
├── components/
│   ├── styles.py
│   └── __init__.py
└── pages/
    ├── 1_Anomaly_Monitoring.py
    └── 2_Network_Localization.py
```

The application connects to PostgreSQL through SQLAlchemy and uses
dedicated query functions for the dashboard serving views.

### Run Locally

From the project root:

``` cmd
streamlit run dashboard\app.py
```

The dashboard normally opens at:

``` text
http://localhost:8501
```

A public deployment can be added later without changing the analytical
architecture.

### Dashboard Preview

![Smart Water Network Dashboard](docs/images/phase10/dashboard_overview.png)

The dashboard is a **simulation/benchmark demonstration**. A future
deployed version will expose analytical results generated from the
project dataset and pipeline, not live measurements from a utility
network.

## 8. Airflow Orchestration

The daily Airflow workflow orchestrates the serving, anomaly-detection,
and localization pipeline:

``` text
check_postgresql
      |
      v
upsert_sensor_metrics
      |
      v
validate_metric_load
      |
      v
validate_reference_data
      |
      v
run_anomaly_detection
      |
      v
validate_anomaly_results
      |
      v
run_localization
      |
      v
validate_localization_results
```

Configuration:

``` text
schedule      @daily
retries       2
retry delay   5 minutes
catchup       False
```

The Airflow schedule orchestrates the workflow; it does not imply that
Kafka itself only processes data once per day.

## 9. End-to-End Validation

The final analytical workflow was validated through PostgreSQL and
Airflow.

  Validation                           Result
  --------------------------------- ---------
  Anomaly rows                        101,088
  ML anomaly timestamps                 5,897
  Localization rows                   121,700
  Localized timestamps                  4,868
  Distinct candidate pipes                495
  Invalid Top-25 groups                     0
  Localization without ML anomaly           0
  Missing network links                     0
  Non-pipe candidates                       0
  Missing geometries                        0
  Duplicate timestamp/pipe pairs            0
  NULL localization scores                  0
  Negative localization scores              0
  QGIS view rows                      121,700
  Unique QGIS feature IDs             121,700

Validated chain:

``` text
Sensor Data
    |
    v
Streaming Pipeline
    |
    v
PostgreSQL
    |
    v
Anomaly Detection
    |
    v
Sensor Evidence
    |
    v
Topology Attribution
    |
    v
Candidate Pipe Ranking
    |
    +-------------> PostGIS / QGIS
    |
    v
Dashboard Serving Views
    |
    v
Streamlit Dashboard
```

## 10. Technology Stack

  -----------------------------------------------------------------------
  Technology                          Role
  ----------------------------------- -----------------------------------
  Python                              Simulation, feature engineering,
                                      anomaly detection, localization

  Pandas / NumPy                      Data transformation and analytical
                                      processing

  scikit-learn                        Isolation Forest

  Apache Kafka 4.1.1                  Streaming ingestion

  Apache Spark 4.1.3                  Structured streaming and
                                      aggregation

  PostgreSQL / PostGIS                Operational, analytical, and
                                      spatial storage

  Parquet                             Historical event storage

  Apache Airflow 3.3.1                Workflow orchestration and
                                      validation

  Streamlit                           Operational analytics dashboard

  QGIS 3.44                           Network and localization
                                      visualization

  Docker / Docker Compose             Reproducible infrastructure

  Git / GitHub                        Version control and project
                                      publication
  -----------------------------------------------------------------------

## 11. Project Structure

``` text
Smart-water-network-platform/
│
├── dashboard/
├── docs/
│   └── images/
│       ├── phase10/
│       └── smart_water_architecture.png
├── gis/
├── infrastructure/
│   ├── airflow/
│   ├── kafka/
│   ├── ml/
│   ├── postgres/
│   └── spark/
├── sql/
│   ├── 01_create_storage_schema.sql
│   ├── 02_upsert_sensor_metrics.sql
│   ├── 03_create_network_anomaly_results.sql
│   ├── 04_create_network_localization_results.sql
│   ├── 05_create_localization_qgis_view.sql
│   └── 06_create_dashboard_views.sql
├── src/
│   ├── anomaly_detection/
│   ├── data/
│   ├── exploration/
│   ├── localization/
│   ├── simulator/
│   └── streaming/
├── .gitignore
├── requirements.txt
└── README.md
```

Runtime files, credentials, generated lake data, Spark checkpoints,
virtual environments, and Python caches are excluded from version
control.

## 12. Local Setup

The project has been developed and validated on Windows using Docker
Desktop.

### Prerequisites

-   Git
-   Python
-   Docker Desktop with Docker Compose
-   QGIS for spatial visualization

### Environment Configuration

Use:

``` text
infrastructure/postgres/.env.example
```

as the template for:

``` text
infrastructure/postgres/.env
```

The real `.env` file is excluded from Git.

### Create Shared Docker Network

``` cmd
docker network create smart-water-network
```

### Start PostgreSQL/PostGIS

``` cmd
docker compose --env-file infrastructure\postgres\.env -f infrastructure\postgres\docker-compose.yml up -d
```

### Initialize Database

``` cmd
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\01_create_storage_schema.sql
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\02_upsert_sensor_metrics.sql
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\03_create_network_anomaly_results.sql
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\04_create_network_localization_results.sql
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\05_create_localization_qgis_view.sql
docker exec -i smart-water-postgres psql -U smart_water_user -d smart_water < sql\06_create_dashboard_views.sql
```

### Load Reference Data

``` cmd
python -m src.data.load_reference_data
```

### Start Kafka

``` cmd
docker compose -f infrastructure\kafka\docker-compose.yml up -d
```

### Replay Sensor Data

``` cmd
python -m src.simulator.sensor_simulator
```

### Run Spark

``` cmd
infrastructure\spark\run_spark_stream.cmd
```

### Start Airflow

``` cmd
docker compose --env-file infrastructure\postgres\.env -f infrastructure\airflow\docker-compose.yml up -d
```

### Run the Dashboard

``` cmd
streamlit run dashboard\app.py
```

## 13. Engineering Principles

-   **Separation of responsibilities:** each platform component has a
    defined role.
-   **Causal analytics:** anomaly features and sensor evidence avoid
    future observations.
-   **Ground-truth separation:** leakage labels are used for evaluation
    rather than detector/localization input or threshold tuning.
-   **Baseline before ML:** an explainable statistical detector provides
    a reference before additional ML complexity.
-   **Evidence before claims:** anomalies are not automatically
    classified as leaks, and candidate pipes are not presented as
    confirmed leak locations.
-   **Idempotent persistence:** repeated analytical execution is
    designed to avoid duplicate records.
-   **Data-quality validation:** validation is performed between major
    pipeline stages and after final persistence.
-   **Raw-data preservation:** source datasets are not modified in
    place.
-   **Secrets management:** credentials remain in ignored environment
    files.
-   **Incremental architecture:** technologies are introduced only when
    they have a clear responsibility.

## 14. Project Progress

  -----------------------------------------------------------------------
  Phase                   Scope                   Status
  ----------------------- ----------------------- -----------------------
  1                       Project design          Complete

  2                       Data acquisition and    Complete
                          understanding

  3                       Python sensor simulator Complete

  4                       Kafka streaming         Complete
                          ingestion

  5                       Spark Structured        Complete
                          Streaming

  6                       Persistent storage      Complete

  7                       Airflow orchestration   Complete
                          and integration

  8                       Network anomaly         Complete
                          detection

  9                       Anomaly interpretation, Complete
                          localization and GIS

  10                      Operational analytics   Complete
                          and Streamlit dashboard

  Future                  Public dashboard        Planned
                          deployment / selected
                          AWS services
  -----------------------------------------------------------------------

## 15. Current Capabilities

### Data Engineering

-   Event simulation
-   Kafka ingestion
-   Spark Structured Streaming
-   Parquet storage
-   PostgreSQL serving
-   Airflow orchestration

### Machine Learning & Analytics

-   Causal feature engineering
-   Statistical anomaly detection
-   Isolation Forest
-   Ground-truth evaluation
-   Sensor-level anomaly interpretation

### Network Intelligence

-   Graph construction
-   Sensor-to-network attribution
-   Shortest-hop analysis
-   Candidate-pipe ranking

### Geospatial Analytics

-   PostGIS network geometry
-   Spatial serving views
-   QGIS visualization

### Operational Analytics

-   Streamlit dashboard
-   Network KPIs
-   Anomaly investigation
-   Localization investigation
-   Temporal anomaly analysis
-   PostgreSQL dashboard serving views

### Engineering Quality

-   Dockerized execution
-   Data-quality validation
-   Idempotent persistence
-   Reproducible workflows
-   Explicit model limitations
-   Secrets exclusion

## 16. Public Dashboard

**Status:** Not yet publicly deployed.

Current local command:

``` cmd
streamlit run dashboard\app.py
```

Public URL will be added after deployment.

The eventual deployment will expose the same simulated/benchmark
analytical results described in this repository; it will not represent
live utility-network telemetry.

## 17. Reproducibility and Version Control

The repository excludes machine-specific, generated, and sensitive files
including:

``` text
.env
*.env
venv/
__pycache__/
*.pyc
data/raw/
data/lake/
data/checkpoints/
```

The repository focuses on source code, infrastructure definitions, SQL,
analytical logic, GIS configuration, dashboard code, and documentation
required to understand and reproduce the architecture.

## 18. Author

**GATERA Emile**

Civil & Water Resources Engineer \| Data Engineering & Analytics

This project combines water-infrastructure domain knowledge with
practical data engineering, streaming systems, database design, workflow
orchestration, machine learning, network analysis, geospatial analytics,
and operational dashboard development.
