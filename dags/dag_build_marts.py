from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="build_marts",
        default_args=default_args,
        description="Расчёт витрин заказов и товаров через PySpark",
        schedule_interval=None,
) as dag:
    # wait_for_load = ExternalTaskSensor(
    #     task_id="wait_for_normalized_data",
    #     external_dag_id="load_normalized_data",
    #     external_task_id=None,
    #     timeout=3600,
    #     poke_interval=30,
    #     mode="reschedule",
    # )

    build_marts = BashOperator(
        task_id="run_pyspark_build_marts",
        bash_command=(
            "spark-submit "
            "--packages org.postgresql:postgresql:42.5.0 "
            "/opt/airflow/scripts/build_marts.py"
        ),
        execution_timeout=timedelta(minutes=10),
    )
