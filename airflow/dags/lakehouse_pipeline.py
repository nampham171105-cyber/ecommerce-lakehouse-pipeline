from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_DIR = "/opt/spark/jobs"
SPARK_UTILS_DIR = "/opt/spark/utils"

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def make_spark_task(task_id: str, script: str, table: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_JOBS_DIR}/{script}",
        conn_id=SPARK_CONN_ID,
        # Không truyền packages/conf — spark-defaults.conf trong image đã lo,
        # jar cũng đã có sẵn trong /opt/spark/jars qua Dockerfile.
        py_files=f"{SPARK_UTILS_DIR}/job_control.py,{SPARK_UTILS_DIR}/validation.py",
        env_vars={"PYTHONPATH": "/opt/spark"},
        application_args=["--table", table],
        verbose=False,
    )


with DAG(
    dag_id="lakehouse_pipeline",
    description="Bronze → Silver → Gold, mỗi bảng 1 task, chạy song song khi có thể",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,   # 2 worker x 2 core = 4 core khả dụng của cluster
    tags=["lakehouse", "spark"],
) as dag:

    TABLES = ["users", "products", "sessions", "interactions", "purchases", "reviews"]

    bronze_tasks = {t: make_spark_task(f"bronze_{t}", "ingest_bronze.py", t) for t in TABLES}
    silver_tasks = {t: make_spark_task(f"silver_{t}", "transform_silver.py", t) for t in TABLES}

    for t in TABLES:
        bronze_tasks[t] >> silver_tasks[t]

    gold_dim_user    = make_spark_task("gold_dim_user", "build_gold.py", "dim_user")
    gold_dim_product = make_spark_task("gold_dim_product", "build_gold.py", "dim_product")
    gold_dim_session = make_spark_task("gold_dim_session", "build_gold.py", "dim_session")
    gold_dim_date    = make_spark_task("gold_dim_date", "build_gold.py", "dim_date")

    silver_tasks["users"]    >> gold_dim_user
    silver_tasks["products"] >> gold_dim_product
    silver_tasks["sessions"] >> gold_dim_session
    # gold_dim_date: không phụ thuộc Silver nào

    gold_fact_interaction = make_spark_task("gold_fact_interaction", "build_gold.py", "fact_interaction")
    gold_fact_purchase    = make_spark_task("gold_fact_purchase", "build_gold.py", "fact_purchase")
    gold_fact_review      = make_spark_task("gold_fact_review", "build_gold.py", "fact_review")

    [gold_dim_user, gold_dim_product, gold_dim_session, gold_dim_date] >> gold_fact_interaction
    [gold_dim_user, gold_dim_product, gold_dim_session, gold_dim_date] >> gold_fact_purchase
    [gold_dim_user, gold_dim_product, gold_dim_date] >> gold_fact_review