from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_DIR = "/opt/spark/jobs"
SPARK_UTILS_DIR = "/opt/spark/utils"

BRONZE_PRIORITY = 400
SILVER_PRIORITY = 300
GOLD_PRIORITY = 200
MART_PRIORITY = 100

BRONZE_SILVER_TIMEOUT_MIN = 20
GOLD_MART_TIMEOUT_MIN = 30

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def make_spark_task(
    task_id: str,
    script: str,
    table: str,
    priority_weight: int,
    timeout_minutes: int = BRONZE_SILVER_TIMEOUT_MIN,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_JOBS_DIR}/{script}",
        conn_id=SPARK_CONN_ID,
        priority_weight=priority_weight,
        weight_rule="absolute",
        execution_timeout=timedelta(minutes=timeout_minutes),
        conf={
            "spark.cores.max": "1",               # Ép mỗi task chỉ dùng tối đa 1 core
            "spark.executor.memory": "1g",        # Ép mỗi task dùng 1GB RAM
            "spark.driver.host": "airflow-scheduler",  # Chỉ đường cho Worker gọi về Driver
            "spark.driver.bindAddress": "0.0.0.0"
        },
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
    max_active_tasks=2,   # 2 worker x 2 core = 4 core khả dụng của cluster
    tags=["lakehouse", "spark"],
) as dag:

    TABLES = ["users", "products", "sessions", "interactions", "purchases", "reviews"]
 
    # ---------------- BRONZE ----------------
    with TaskGroup(group_id="bronze") as bronze_group:
        bronze_tasks = {
            t: make_spark_task(
                f"bronze_{t}", "ingest_bronze.py", t, BRONZE_PRIORITY,
                timeout_minutes=BRONZE_SILVER_TIMEOUT_MIN,
            )
            for t in TABLES
        }
 
    # ---------------- SILVER ----------------
    with TaskGroup(group_id="silver") as silver_group:
        silver_tasks = {
            t: make_spark_task(
                f"silver_{t}", "transform_silver.py", t, SILVER_PRIORITY,
                timeout_minutes=BRONZE_SILVER_TIMEOUT_MIN,
            )
            for t in TABLES
        }
 
    # Dependency ghép cặp theo từng bảng - độc lập giữa các bảng khác nhau,
    # nên KHÔNG dùng .expand() ở đây (expand() sẽ ép toàn bộ Bronze xong hết
    # mới tới bất kỳ Silver nào, phá vỡ tính song song theo bảng).
    for t in TABLES:
        bronze_tasks[t] >> silver_tasks[t]
 
    # ---------------- GOLD ----------------
    with TaskGroup(group_id="gold") as gold_group:
        gold_dim_user = make_spark_task(
            "gold_dim_user", "build_gold.py", "dim_user", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        gold_dim_product = make_spark_task(
            "gold_dim_product", "build_gold.py", "dim_product", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        gold_dim_session = make_spark_task(
            "gold_dim_session", "build_gold.py", "dim_session", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        gold_dim_date = make_spark_task(
            "gold_dim_date", "build_gold.py", "dim_date", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
 
        gold_fact_interaction = make_spark_task(
            "gold_fact_interaction", "build_gold.py", "fact_interaction", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        gold_fact_purchase = make_spark_task(
            "gold_fact_purchase", "build_gold.py", "fact_purchase", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        gold_fact_review = make_spark_task(
            "gold_fact_review", "build_gold.py", "fact_review", GOLD_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
 
        # Dependency nội bộ trong group gold: dim phải xong trước fact tương ứng
        [gold_dim_user, gold_dim_product, gold_dim_session, gold_dim_date] >> gold_fact_interaction
        [gold_dim_user, gold_dim_product, gold_dim_session, gold_dim_date] >> gold_fact_purchase
        [gold_dim_user, gold_dim_product, gold_dim_date] >> gold_fact_review
 
    # Dependency giữa Silver -> Gold, theo đúng bảng nguồn của từng dim
    silver_tasks["users"] >> gold_dim_user
    silver_tasks["products"] >> gold_dim_product
    silver_tasks["sessions"] >> gold_dim_session
    # gold_dim_date: không phụ thuộc Silver nào (generate từ sequence ngày)
 
    # ---------------- MART ----------------
    with TaskGroup(group_id="mart") as mart_group:
        mart_customer_360 = make_spark_task(
            "mart_customer_360", "build_mart.py", "customer_360", MART_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        mart_sales_daily = make_spark_task(
            "mart_sales_daily", "build_mart.py", "sales_daily", MART_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
        mart_product_performance = make_spark_task(
            "mart_product_performance", "build_mart.py", "product_performance", MART_PRIORITY,
            timeout_minutes=GOLD_MART_TIMEOUT_MIN,
        )
 
    [gold_dim_user, gold_fact_purchase, gold_fact_interaction, gold_fact_review] >> mart_customer_360
    [gold_dim_date, gold_fact_purchase] >> mart_sales_daily
    [gold_dim_product, gold_fact_purchase, gold_fact_interaction, gold_fact_review] >> mart_product_performance