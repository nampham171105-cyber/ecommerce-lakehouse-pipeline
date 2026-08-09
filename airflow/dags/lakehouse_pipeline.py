from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineer",
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    # schedule_interval="@daily",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "spark"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/ingest_bronze.py"
        ),
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/transform_silver.py"
        ),
    )

    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/transform_gold.py"
        ),
    )

    ingest_bronze >> transform_silver >> transform_gold