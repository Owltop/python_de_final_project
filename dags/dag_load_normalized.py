#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="load_normalized_data",
        default_args=default_args,
        description="Загрузка данных из parquet в PostgreSQL",
) as dag:

    load_data = BashOperator(
        task_id="load_data_to_postgres",
        bash_command=(
            "spark-submit "
            "--packages org.postgresql:postgresql:42.5.0 "
            "/opt/airflow/scripts/load_normalized.py"
        ),
        execution_timeout=timedelta(minutes=10),
    )
