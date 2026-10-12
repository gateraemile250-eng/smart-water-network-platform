import os
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.standard.operators.bash import BashOperator


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

PROJECT_PATH = os.environ.get("SMART_WATER_PROJECT_PATH")

if not PROJECT_PATH:
    raise RuntimeError(
        "SMART_WATER_PROJECT_PATH is not set. Add it to "
        "infrastructure/postgres/.env and recreate Airflow containers."
    )

AWS_CONN_ID = "aws_default"
S3_BUCKET = "smart-water-lake-453914763430"
S3_CURATED_PREFIX = "curated"


# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------

with DAG(
    dag_id="smart_water_daily_pipeline",
    description=(
        "Daily Smart Water pipeline with validation, anomaly detection, "
        "localization, predictive ML, fusion, and S3 publication."
    ),
    start_date=datetime(2026, 9, 28),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=[
        "smart-water",
        "postgresql",
        "data-quality",
        "anomaly-detection",
        "localization",
        "predictive-ml",
        "aws",
        "s3",
    ],
) as dag:

    # ------------------------------------------------------------------
    # PostgreSQL
    # ------------------------------------------------------------------

    check_postgresql = SQLExecuteQueryOperator(
        task_id="check_postgresql",
        conn_id="smart_water_db",
        sql="SELECT 1;",
    )

    upsert_sensor_metrics = SQLExecuteQueryOperator(
        task_id="upsert_sensor_metrics",
        conn_id="smart_water_db",
        sql="CALL upsert_sensor_metrics();",
        autocommit=True,
    )

    validate_metric_load = SQLCheckOperator(
        task_id="validate_metric_load",
        conn_id="smart_water_db",
        sql="""
            SELECT COUNT(*) > 0
            FROM sensor_metrics_15min;
        """,
    )

    validate_reference_data = SQLCheckOperator(
        task_id="validate_reference_data",
        conn_id="smart_water_db",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM sensors) > 0
                AND (SELECT COUNT(*) FROM network_nodes) > 0
                AND (SELECT COUNT(*) FROM network_links) > 0;
        """,
    )

    # ------------------------------------------------------------------
    # Anomaly detection
    # ------------------------------------------------------------------

    run_anomaly_detection = BashOperator(
        task_id="run_anomaly_detection",
        bash_command=f"""
            docker run --rm \
            --network smart-water-network \
            -v "{PROJECT_PATH}:/opt/project" \
            -e POSTGRES_HOST=smart-water-postgres \
            -e POSTGRES_PORT=5432 \
            smart-water-ml \
            python src/anomaly_detection/anomaly_persistence.py
        """,
    )

    validate_anomaly_results = SQLCheckOperator(
        task_id="validate_anomaly_results",
        conn_id="smart_water_db",
        sql="""
            SELECT
                COUNT(*) > 0
                AND COUNT(*) = COUNT(DISTINCT timestamp)
                AND COUNT(ml_anomaly_score) = COUNT(*)
                AND COUNT(is_ml_anomaly) = COUNT(*)
                AND COUNT(is_baseline_anomaly) = COUNT(*)
            FROM network_anomaly_results;
        """,
    )

    # ------------------------------------------------------------------
    # Network localization
    # ------------------------------------------------------------------

    run_localization = BashOperator(
        task_id="run_localization",
        bash_command=f"""
            docker run --rm \
            --network smart-water-network \
            -v "{PROJECT_PATH}:/opt/project" \
            -e POSTGRES_HOST=smart-water-postgres \
            -e POSTGRES_PORT=5432 \
            smart-water-ml \
            python -m src.localization.localization_persistence
        """,
    )

    validate_localization_results = SQLCheckOperator(
        task_id="validate_localization_results",
        conn_id="smart_water_db",
        sql="""
            SELECT
                COUNT(*) > 0
                AND COUNT(*) = COUNT(
                    DISTINCT (timestamp, pipe_id)
                )
                AND COUNT(*) FILTER (
                    WHERE localization_score IS NULL
                ) = 0
                AND COUNT(*) FILTER (
                    WHERE localization_score < 0
                ) = 0
                AND COUNT(*) FILTER (
                    WHERE candidate_rank < 1
                       OR candidate_rank > 25
                ) = 0
            FROM network_localization_results;
        """,
    )

    # ------------------------------------------------------------------
    # Curated export
    # ------------------------------------------------------------------

    run_curated_export = BashOperator(
        task_id="run_curated_export",
        bash_command=f"""
            docker run --rm \
            --network smart-water-network \
            -v "{PROJECT_PATH}:/opt/project" \
            -e POSTGRES_HOST=smart-water-postgres \
            -e POSTGRES_PORT=5432 \
            smart-water-ml \
            python -m src.cloud.curated_export
        """,
    )

    # ------------------------------------------------------------------
    # 8.6.1 Production predictive scoring
    # ------------------------------------------------------------------

    run_production_predictive_scoring = BashOperator(
        task_id="run_production_predictive_scoring",
        bash_command=f"""
            docker run --rm \
            -v "{PROJECT_PATH}:/opt/project" \
            -w /opt/project \
            smart-water-ml \
            python -m src.predictive.production_scoring
        """,
    )

    # ------------------------------------------------------------------
    # 8.6.2 Production fusion
    # ------------------------------------------------------------------

    run_production_fusion = BashOperator(
        task_id="run_production_fusion",
        bash_command=f"""
            docker run --rm \
            -v "{PROJECT_PATH}:/opt/project" \
            -w /opt/project \
            smart-water-ml \
            python -m src.predictive.predictive_anomaly_fusion
        """,
    )

    # ------------------------------------------------------------------
    # 8.6.4 Production fusion validation
    # ------------------------------------------------------------------

    validate_predictive_fusion = BashOperator(
        task_id="validate_predictive_fusion",
        bash_command=f"""
            docker run --rm \
            -v "{PROJECT_PATH}:/opt/project" \
            -w /opt/project \
            smart-water-ml \
            python -c "
import pandas as pd

path = 'data/publish/curated/predictive_anomaly_fusion.parquet'
df = pd.read_parquet(path)

required = [
    'event_time',
    'predicted_probability',
    'predicted_leakage',
    'is_baseline_anomaly',
    'is_ml_anomaly',
    'intelligence_state',
]

missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(f'Missing columns: {{missing}}')

if df.empty:
    raise ValueError('Predictive fusion output is empty.')

if df['event_time'].duplicated().any():
    raise ValueError('Duplicate event timestamps detected.')

if df[required].isna().any().any():
    raise ValueError(
        'Required fusion fields contain missing values.'
    )

print('Predictive fusion validation: OK')
print('Rows:', len(df))
print(
    'Required missing values:',
    int(df[required].isna().sum().sum())
)
print(
    'Duplicate timestamps:',
    int(df['event_time'].duplicated().sum())
)
"
        """,
    )

    # ------------------------------------------------------------------
    # 8.6.5 S3 publication
    # ------------------------------------------------------------------

    publish_predictive_anomaly_fusion = LocalFilesystemToS3Operator(
        task_id="publish_predictive_anomaly_fusion",
        filename=(
            "/opt/project/data/publish/curated/"
            "predictive_anomaly_fusion.parquet"
        ),
        dest_key=(
            f"{S3_CURATED_PREFIX}/"
            "predictive_anomaly_fusion.parquet"
        ),
        dest_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    # ------------------------------------------------------------------
    # Existing S3 publication
    # ------------------------------------------------------------------

    publish_anomaly_results = LocalFilesystemToS3Operator(
        task_id="publish_anomaly_results",
        filename=(
            "/opt/project/data/publish/curated/"
            "network_anomaly_results.parquet"
        ),
        dest_key=f"{S3_CURATED_PREFIX}/network_anomaly_results.parquet",
        dest_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    publish_network_links = LocalFilesystemToS3Operator(
        task_id="publish_network_links",
        filename=(
            "/opt/project/data/publish/curated/"
            "network_links.parquet"
        ),
        dest_key=f"{S3_CURATED_PREFIX}/network_links.parquet",
        dest_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    publish_localization_results = LocalFilesystemToS3Operator(
        task_id="publish_localization_results",
        filename=(
            "/opt/project/data/publish/curated/"
            "network_localization_results.parquet"
        ),
        dest_key=(
            f"{S3_CURATED_PREFIX}/"
            "network_localization_results.parquet"
        ),
        dest_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    publish_network_nodes = LocalFilesystemToS3Operator(
        task_id="publish_network_nodes",
        filename=(
            "/opt/project/data/publish/curated/"
            "network_nodes.parquet"
        ),
        dest_key=f"{S3_CURATED_PREFIX}/network_nodes.parquet",
        dest_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    (
        check_postgresql
        >> upsert_sensor_metrics
        >> validate_metric_load
        >> validate_reference_data
        >> run_anomaly_detection
        >> validate_anomaly_results
        >> run_localization
        >> validate_localization_results
        >> run_curated_export
        >> run_production_predictive_scoring
        >> run_production_fusion
        >> validate_predictive_fusion
    )

    validate_predictive_fusion >> publish_predictive_anomaly_fusion

    run_curated_export >> [
        publish_anomaly_results,
        publish_network_links,
        publish_localization_results,
        publish_network_nodes,
    ]