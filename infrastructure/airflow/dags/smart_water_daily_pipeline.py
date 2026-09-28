from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)


with DAG(
    dag_id="smart_water_daily_pipeline",
    description="Load and validate Smart Water serving-layer data.",
    start_date=datetime(2026, 9, 28),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["smart-water", "postgresql", "data-quality"],
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

    (
        check_postgresql
        >> upsert_sensor_metrics
        >> validate_metric_load
        >> validate_reference_data
    )