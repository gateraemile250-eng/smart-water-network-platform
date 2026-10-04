import os
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.standard.operators.bash import BashOperator


# Absolute path of the repository on the Docker host.
# Set in infrastructure/postgres/.env.
PROJECT_PATH = os.environ.get("SMART_WATER_PROJECT_PATH")

if not PROJECT_PATH:
    raise RuntimeError(
        "SMART_WATER_PROJECT_PATH is not set. Add it to "
        "infrastructure/postgres/.env and recreate the Airflow containers."
    )


with DAG(
    dag_id="smart_water_daily_pipeline",
    description=(
        "Load, validate, detect anomalies, and localize "
        "suspected network areas for the Smart Water platform."
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
    ],
) as dag:

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

    (
        check_postgresql
        >> upsert_sensor_metrics
        >> validate_metric_load
        >> validate_reference_data
        >> run_anomaly_detection
        >> validate_anomaly_results
        >> run_localization
        >> validate_localization_results
    )