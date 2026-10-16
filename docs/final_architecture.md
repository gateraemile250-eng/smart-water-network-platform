# Smart Water Network Platform — Final Architecture

## 1. Overview

The Smart Water Network Platform is an end-to-end data engineering and water-network intelligence platform.

The platform combines real-time-style sensor data ingestion, distributed processing, operational data storage, machine-learning-based anomaly detection, network localization, predictive scoring, workflow orchestration, AWS cloud analytics, Databricks lakehouse processing, GIS capabilities, and a user-facing dashboard.

The architecture was developed progressively from a local data-engineering platform into a hybrid local/cloud analytical platform.

The final architecture demonstrates how water-network sensor data can move from ingestion through processing and intelligence generation to cloud analytics and operational visualization.

---

## 2. Final Architecture

```text
                         SMART WATER NETWORK
                                  |
                                  v
                         Data Simulation
                                  |
                                  v
                           Apache Kafka
                                  |
                                  v
                       Spark Streaming / Spark
                                  |
                                  v
                       PostgreSQL / PostGIS
                                  |
                 +----------------+----------------+
                 |                |                |
                 v                v                v
          Anomaly Detection  Localization   Predictive Scoring
                 |                |                |
                 +----------------+----------------+
                                  |
                                  v
                         Predictive Fusion
                                  |
                                  v
                         Apache Airflow
                                  |
                 +----------------+----------------+
                 |                                 |
                 v                                 v
              AWS CLOUD                       DATABRICKS
                 |                                 |
                 v                                 v
              Amazon S3                      Unity Catalog
                 |                                 |
          +------+-------+                     Delta Lake
          |              |                 +-------+-------+
          v              v                 |       |       |
        Glue           Athena           Bronze  Silver   Gold
          |              |                 |       |       |
          +------+-------+                 +-------+-------+
                 |                                 |
                 |                                 v
                 |                         Databricks SQL
                 |                                 |
                 +----------------+----------------+
                                  |
                                  v
                         Streamlit Dashboard
                                  |
                                  v
                         WATER NETWORK USER

3. Architecture Layers
The platform is organized into the following major layers:
1. Data generation and ingestion
2. Distributed data processing
3. Operational database
4. Water-network intelligence
5. Workflow orchestration
6. AWS cloud data platform
7. Databricks lakehouse
8. GIS and spatial analysis
9. User-facing analytics
10. Testing and data quality
Each layer has a specific responsibility.
4. Data Generation and Ingestion Layer
Technologies
- Python-based simulator
- Apache Kafka
- Spark Structured Streaming
Purpose
This layer represents the entry point for Smart Water Network sensor information.
The project includes a sensor-data simulation component that produces structured sensor events.
Kafka provides the messaging layer for sensor-event ingestion.
Spark provides distributed processing capabilities for streaming and batch workloads.
Project implementation
src/simulator/
src/streaming/
infrastructure/kafka/
infrastructure/spark/

Responsibility
The ingestion layer is responsible for:
- Producing sensor events
- Transporting sensor events through Kafka
- Providing data to downstream Spark processing
- Supporting a realistic streaming-data architecture
5. Distributed Processing Layer
Technology
Apache Spark
Purpose
Spark provides the processing engine for sensor data.
The platform uses Spark for distributed data processing and supports the processing of sensor-event data before it reaches downstream storage and analytical workflows.
Project implementation
src/streaming/
infrastructure/spark/

Responsibility
Spark is responsible for:
- Processing sensor events
- Transforming incoming data
- Supporting scalable processing
- Producing structured datasets for downstream systems
Spark is retained as an important processing component rather than replacing it with unnecessary managed cloud services.
6. Operational Data Layer
Technologies
- PostgreSQL
- PostGIS
Purpose
PostgreSQL/PostGIS provides the operational serving and spatial data layer.
It stores structured Smart Water Network information and supports network-oriented analysis.
PostGIS provides spatial capabilities required for network and geographic analysis.
Project implementation
infrastructure/postgres/
sql/

Important SQL components include:
01_create_storage_schema.sql
02_upsert_sensor_metrics.sql
03_create_network_anomaly_results.sql
04_create_network_localization_results.sql
05_create_localization_qgis_view.sql
06_create_dashboard_views.sql

Responsibility
This layer provides:
- Persistent operational storage
- Sensor metrics
- Network information
- Anomaly results
- Localization results
- Dashboard-oriented database views
- Spatial analysis support
7. Water-Network Intelligence Layer
The platform does not stop at storing sensor measurements.
It converts sensor information into water-network intelligence.
The intelligence layer consists of:
Anomaly Detection
       |
       v
Localization
       |
       v
Predictive Scoring
       |
       v
Predictive / Anomaly Fusion

7.1 Anomaly Detection
The anomaly-detection layer identifies sensor behavior that differs from expected or baseline behavior.
Project implementation:
src/anomaly_detection/

The existing platform includes baseline-based and machine-learning-oriented anomaly detection.
7.2 Network Localization
Localization attempts to associate abnormal behavior with likely locations within the water network.
Project implementation:
src/localization/
gis/

The system produces network-localization results that can be used for investigation and operational analysis.
7.3 Predictive Intelligence
The predictive layer extends the anomaly workflow with predictive scoring.
Project implementation:
src/predictive/
infrastructure/ml/

The later pipeline integrates predictive results with anomaly intelligence.
7.4 Predictive Fusion
Predictive and anomaly information can be combined into a final analytical result.
This creates a stronger operational intelligence layer than relying on a single anomaly score.
8. Apache Airflow Orchestration Layer
Technology
Apache Airflow
Main DAG
smart_water_daily_pipeline

Project implementation
infrastructure/airflow/dags/

The main DAG is:
smart_water_daily_pipeline.py

Purpose
Airflow coordinates the major processing, validation, ML, export and publishing steps.
The workflow is scheduled daily.
The validated configuration includes:
- Daily schedule
- 00:00 UTC execution
- 02:00 Rwanda local time
- Catchup disabled
- Two retries
- Five-minute retry delay
Current workflow
The current DAG contains 18 tasks:
check_postgresql
upsert_sensor_metrics
validate_metric_load
validate_reference_data
validate_lake_quality
run_anomaly_detection
validate_anomaly_results
run_localization
validate_localization_results
run_curated_export
run_production_predictive_scoring
run_production_fusion
validate_predictive_fusion
publish_network_links
publish_network_nodes
publish_localization_results
publish_anomaly_results
publish_predictive_anomaly_fusion

Validation
A complete manual Airflow run was successfully executed.
All 18 tasks reached:
success

This demonstrates that the current orchestration workflow can execute the complete processing and publishing chain.
9. AWS Cloud Data Platform
The AWS layer provides cloud storage and serverless analytical capabilities.
The main AWS components are:
Amazon S3
    |
    +---- AWS Glue Data Catalog
    |
    +---- Amazon Athena

9.1 Amazon S3
S3 provides the cloud data-lake storage layer.
The project uses organized data zones including:
raw/
processed/
curated/

Processed datasets use Parquet storage with compression and partitioning.
The S3 bucket used by the Smart Water project is:
smart-water-lake-453914763430

S3 therefore provides durable cloud storage for analytical datasets.
9.2 AWS Glue
Glue Data Catalog provides metadata and schema organization for cloud datasets.
It allows the S3 data lake to be understood and queried using structured table definitions.
The project avoids unnecessary Glue ETL where existing transformations are sufficient.
9.3 Amazon Athena
Athena provides serverless SQL analytics directly against data stored in S3.
This avoids introducing a separate traditional data warehouse when analytical requirements can be served directly from the data lake.
The resulting architecture is:
S3
 |
 v
Glue Catalog
 |
 v
Athena
 |
 v
SQL Analytics

10. Databricks Lakehouse Layer
Databricks was introduced during Phase 10 as a complementary lakehouse layer.
It does not replace:
- PostgreSQL
- Kafka
- Spark
- Airflow
- AWS S3
- Existing machine-learning workflows
Instead, Databricks provides another analytical and lakehouse processing environment.
10.1 Unity Catalog
Unity Catalog provides organization and governance for Databricks data assets.
The project created a managed data volume:
/Volumes/workspace/default/smart_water_data

10.2 Bronze Layer
The Bronze layer stores sensor-event data in Delta format.
bronze_sensor_events

The Phase 10 validation recorded:
1,428 records

The Bronze layer preserves the incoming sensor-event structure.
10.3 Silver Layer
The Silver layer represents cleaned and validated sensor-event data.
silver_sensor_events

The Phase 10 validation recorded:
1,428 records

Structural data-quality checks confirmed:
Null event IDs: 0
Null event times: 0
Null sensor IDs: 0
Null sensor types: 0

10.4 Gold Layer
The Gold layer contains analytical datasets.
One important dataset is:
gold_daily_sensor_metrics

The Phase 10 milestone recorded:
119 aggregated records

The Gold layer contains analytical metrics such as:
- Event date
- Sensor ID
- Sensor type
- Reading count
- Average value
- Minimum value
- Maximum value
10.5 ML / Anomaly Results
Predictive anomaly fusion results were also integrated into the Databricks analytical layer.
The Phase 10 milestone recorded:
1,010,888 records

This demonstrates that Databricks can complement the existing ML pipeline rather than replacing it.
10.6 Databricks SQL
Databricks SQL provides a SQL-oriented analytical interface over the Gold data.
It supports business-oriented summaries such as:
- Sensor activity
- Reading counts
- Average values
- Minimum values
- Maximum values
- Sensor-type summaries
- Daily analytical metrics
11. GIS Layer
GIS capabilities support the spatial component of the Smart Water Network.
The project includes:
gis/

and PostGIS/QGIS-oriented SQL views.
GIS supports:
- Network visualization
- Spatial relationships
- Localization analysis
- Geographic interpretation of network issues
The GIS layer is particularly important because the platform is designed around a physical water network rather than a generic sensor-data problem.
12. Dashboard Layer
The user-facing analytical layer is implemented using Streamlit.
Project implementation:
dashboard/

Main components include:
app.py
app_overview.py
db.py
queries.py
components/
pages/

Current analytical pages include:
dashboard/pages/1_Anomaly_Monitoring.py
dashboard/pages/2_Network_Localization.py

The dashboard provides a user-facing interface for interpreting the results produced by the data and ML pipelines.
The final Phase 11 work will extend this layer into the complete Smart Water analytics product.
13. Data Quality and Testing Layer
Data quality is treated as a separate concern throughout the architecture.
Project implementation:
tests/

Current test areas include:
test_baseline_detector.py
test_cloud_build.py
test_cloud_layout.py
test_cloud_parquet.py
test_cloud_quality_gate.py
test_curated_export.py
test_feature_engineering.py
test_lake_dedup.py
test_predictive.py
test_predictive_fusion.py
test_quality_checks.py
test_simulator_contract.py

The platform therefore validates:
- Data contracts
- Cloud layout
- Parquet generation
- Data-quality gates
- Deduplication
- Feature engineering
- Predictive processing
- ML fusion
- Simulator behavior
Airflow additionally performs runtime validation between major workflow stages.
14. End-to-End Data Flow
The complete conceptual data flow is:
Sensor Simulation
       |
       v
     Kafka
       |
       v
     Spark
       |
       v
PostgreSQL/PostGIS
       |
       +--------------------+
       |                    |
       v                    v
Anomaly Detection      Localization
       |                    |
       +---------+----------+
                 |
                 v
        Predictive Scoring
                 |
                 v
        Predictive Fusion
                 |
                 v
              Airflow
                 |
       +---------+---------+
       |                   |
       v                   v
      AWS              Databricks
       |                   |
       v                   v
      S3              Delta Lake
       |                   |
    +--+--+           +----+----+
    |     |           |    |    |
   Glue Athena      Bronze Silver Gold
    |     |                    |
    +--+--+                    v
       |                 Databricks SQL
       |                       |
       +-----------+-----------+
                   |
                   v
              Dashboard
                   |
                   v
          Water Network User

15. What Happens During a Daily Airflow Run
A typical daily workflow follows this sequence:
Step 1 — Connectivity
Airflow checks PostgreSQL connectivity.
Step 2 — Sensor metrics
Sensor metrics are inserted or updated using an idempotent database operation.
Step 3 — Validation
The workflow validates:
- Metric loading
- Reference data
- Lake/data quality
Step 4 — Anomaly detection
The anomaly-detection process evaluates sensor behavior.
Step 5 — Localization
Potential network locations associated with abnormal behavior are calculated.
Step 6 — Predictive processing
Predictive scoring and predictive/anomaly fusion are executed.
Step 7 — Curated export
Analytical datasets are prepared for downstream use.
Step 8 — Cloud publication
Results are published to the cloud data platform.
Step 9 — Validation
Published and generated outputs are validated.
Step 10 — Analytics
The results become available to downstream analytical tools and the dashboard.
16. Technology Responsibilities
Technology	Main responsibility
Python	Simulation, processing and application logic
Kafka	Sensor-event messaging
Spark	Distributed processing
PostgreSQL	Operational data storage
PostGIS	Spatial/network data
ML	Anomaly and predictive intelligence
Airflow	Workflow orchestration
S3	Cloud data-lake storage
Glue	Cloud metadata/catalog
Athena	Serverless SQL analytics
Databricks	Lakehouse processing and analytics
Delta Lake	Reliable lakehouse storage
Unity Catalog	Databricks data organization
Streamlit	User-facing analytics
GIS/QGIS	Spatial visualization and analysis
Git	Version control and project history


17. Architecture Design Principles
The platform follows several important design principles.
17.1 Use each technology for a clear purpose
A technology is included because it provides a specific capability.
The architecture avoids adding services simply to increase the technology count.
17.2 Preserve working components
Existing Kafka, Spark, PostgreSQL, ML and Airflow components remain part of the platform.
Cloud and lakehouse technologies complement these components.
17.3 Separate operational and analytical workloads
PostgreSQL/PostGIS supports operational data and network intelligence.
S3, Athena and Databricks support analytical and lakehouse workloads.
17.4 Validate data between major stages
The platform uses tests and validation tasks rather than assuming that successful execution automatically means successful data processing.
17.5 Design for reproducibility
Airflow provides repeatable workflow execution.
Idempotent database operations reduce the risk of duplicate or inconsistent results during reruns.
17.6 Control cloud cost
The AWS architecture favors services appropriate for the project's scale and avoids unnecessary always-on infrastructure.
18. Current Databricks Limitation
The Phase 10 Databricks integration established the Databricks environment, Unity Catalog organization, Delta processing workflow and analytical layers.
However, Unity Catalog IAM AssumeRole validation for the AWS S3 storage credential remained outstanding during Phase 10.
This is intentionally documented rather than presented as successfully validated.
The project therefore distinguishes between:
Databricks lakehouse processing
        |
        v
Successfully demonstrated

and:
Unity Catalog → AWS IAM AssumeRole
        |
        v
Validation outstanding

This limitation does not invalidate the demonstrated Databricks Bronze/Silver/Gold workflow.
19. Repository Architecture
The major project directories are:
Smart-water-network-platform/
│
├── cloud/
│   ├── iam/
│   └── s3/
│
├── dashboard/
│   ├── components/
│   └── pages/
│
├── data/
│
├── docs/
│
├── gis/
│
├── infrastructure/
│   ├── airflow/
│   ├── kafka/
│   ├── ml/
│   ├── postgres/
│   └── spark/
│
├── notebooks/
│   └── databricks/
│
├── sql/
│
├── src/
│   ├── anomaly_detection/
│   ├── cloud/
│   ├── data/
│   ├── exploration/
│   ├── localization/
│   ├── predictive/
│   ├── simulator/
│   └── streaming/
│
└── tests/

This structure separates:
- Infrastructure
- Application/source code
- SQL
- Cloud configuration
- Dashboards
- Documentation
- Notebooks
- Testing
and therefore supports maintainability and reproducibility.
20. Final Architecture Outcome
The Smart Water Network Platform has evolved from a local data-processing project into an integrated data platform.
The final architecture demonstrates:
1. Sensor-data simulation
2. Event ingestion with Kafka
3. Distributed processing with Spark
4. PostgreSQL/PostGIS operational storage
5. Anomaly detection
6. Network localization
7. Predictive scoring
8. Predictive/anomaly fusion
9. Airflow workflow orchestration
10. Data-quality validation
11. AWS S3 data-lake storage
12. AWS Glue metadata/catalog capabilities
13. Amazon Athena serverless SQL analytics
14. Databricks lakehouse processing
15. Unity Catalog organization
16. Delta Bronze/Silver/Gold layers
17. Databricks SQL analytics
18. GIS/network analysis
19. Streamlit user-facing analytics
20. Automated testing and validation
The architecture therefore demonstrates a complete path from:
RAW SENSOR DATA
       |
       v
INGESTION
       |
       v
PROCESSING
       |
       v
OPERATIONAL STORAGE
       |
       v
WATER-NETWORK INTELLIGENCE
       |
       v
ORCHESTRATION
       |
       v
CLOUD / LAKEHOUSE
       |
       v
ANALYTICS
       |
       v
USER

21. Phase 11 Direction
The final Phase 11 work will build on this architecture.
The remaining objectives are:
- Finalize the Smart Water dashboard
- Integrate operational, anomaly, localization and predictive information
- Improve GIS/network visualization
- Add pipeline/data-quality visibility
- Perform final end-to-end validation
- Complete project documentation
- Clean and finalize the Git repository
- Produce the final project release
No additional major technology platform is required.
The objective is to turn the existing engineering architecture into a complete, understandable and demonstrable Smart Water Network analytics product.
Final Statement
The Smart Water Network Platform is designed as a hybrid data-engineering and analytics architecture.
Local technologies provide ingestion, processing, operational storage and machine-learning intelligence.
AWS provides scalable cloud storage and serverless analytics.
Databricks provides a modern lakehouse environment for Delta-based analytical processing.
Airflow connects the major processing stages into a repeatable workflow.
The dashboard provides the final interface through which water-network information can be interpreted.
The architecture therefore demonstrates not only individual technologies, but how those technologies work together to solve a realistic Smart Water Network data problem.