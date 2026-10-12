from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


with DAG(
    dag_id="smart_water_aws_test",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["smart-water", "aws", "test"],
) as dag:

    list_s3_objects = S3ListOperator(
        task_id="list_s3_objects",
        bucket="smart-water-lake-453914763430",
        prefix="",
        aws_conn_id="aws_default",
    )