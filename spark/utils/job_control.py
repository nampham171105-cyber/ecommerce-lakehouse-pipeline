from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, expr, lit, max as spark_max
from delta import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampNTZType, LongType
)
from datetime import datetime, date
import uuid

JOB_CONTROL_TABLE = "control.job_control"
QUALITY_CHECKS_TABLE = "control.quality_checks"
QUALITY_METRICS_TABLE = "control.quality_metrics"
FAILED_RECORDS_TABLE = "control.failed_records"
AUDIT_LOG_TABLE = "control.audit_log"


def _to_timestamp(val):
    if val is None:
        return None

    if isinstance(val, datetime):
        return val

    if isinstance(val, date):
        return datetime.combine(val, datetime.min.time())

    value = str(val)

    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unsupported watermark format: {value}")


# ============================================================
# Run ID — 1 run_id đại diện cho 1 lần chạy task (bronze_users_...)
# Dùng chung để join quality_metrics <-> failed_records <-> audit_log
# ============================================================
def generate_run_id(layer: str, table_name: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{layer}_{table_name}_{ts}_{short_uuid}"


# ============================================================
# WATERMARK (giữ nguyên logic cũ)
# ============================================================
def insert_log(
    spark: SparkSession,
    layer: str,
    table_name: str,
    watermark_column: str,
    watermark_value,
    rundate: str
) -> bool:
    try:
        schema = StructType([
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("watermark_column", StringType(), True),
            StructField("watermark_value", TimestampNTZType(), True),
            StructField("rundate", StringType(), True),
            StructField("insert_dt", TimestampNTZType(), True)
        ])

        data = [(
            layer,
            table_name,
            watermark_column,
            _to_timestamp(watermark_value),
            rundate,
            None
        )]

        df = spark.createDataFrame(data, schema)
        df = df.withColumn("insert_dt", current_timestamp().cast("timestamp_ntz"))

        df.write.format("delta").mode("append").saveAsTable(JOB_CONTROL_TABLE)
        print("[JOB_CONTROL] Insert log success")
        return True

    except Exception as e:
        print(f"[JOB_CONTROL] Insert log failed: {e}")
        raise RuntimeError(f"Failed to persist watermark for {layer}.{table_name}") from e


def get_watermark(spark: SparkSession, layer: str, table_name: str):
    try:
        df = spark.read.table(JOB_CONTROL_TABLE)
        row = (
            df.filter((df.layer == layer) & (df.table_name == table_name))
            .agg(spark_max("watermark_value").alias("watermark_value"))
            .first()
        )
        if row["watermark_value"] is not None:
            return row["watermark_value"]
        return None

    except Exception as e:
        print(f"[JOB_CONTROL] Get watermark failed: {e}")
        return None


def delete_log(spark: SparkSession, layer: str, table_name: str) -> bool:
    try:
        delta_table = DeltaTable.forName(spark, JOB_CONTROL_TABLE)
        delta_table.delete(f"layer = '{layer}' AND table_name = '{table_name}'")
        print(f"[JOB_CONTROL] Deleted logs: {layer}.{table_name}")
        return True
    except Exception as e:
        print(f"[JOB_CONTROL] Delete log failed: {e}")
        return False


def truncate_logs(spark: SparkSession) -> bool:
    try:
        delta_table = DeltaTable.forName(spark, JOB_CONTROL_TABLE)
        delta_table.delete("1 = 1")
        print("[JOB_CONTROL] All logs deleted")
        return True
    except Exception as e:
        print(f"[JOB_CONTROL] Truncate logs failed: {e}")
        return False


# ============================================================
# QUALITY_CHECKS — đọc rule động (metadata-driven), thay cho
# hard-code BRONZE_RULES/SILVER_RULES nếu bạn muốn tách config ra khỏi code.
# Nếu bảng chưa được seed dữ liệu, trả về [] và pipeline dùng fallback
# hard-code trong file .py như hiện tại.
# ============================================================
# def get_active_checks(spark: SparkSession, layer: str, table_name: str):
#     try:
#         df = spark.read.table(QUALITY_CHECKS_TABLE)
#         rows = (
#             df.filter(
#                 (col("layer") == layer)
#                 & (col("table_name") == table_name)
#                 & (col("is_active") == True)
#             )
#             .collect()
#         )
#         return [r.asDict() for r in rows]
#     except Exception as e:
#         print(f"[QUALITY_CHECKS] Get active checks failed (dùng fallback code): {e}")
#         return []


# ============================================================
# QUALITY_METRICS — kết quả đo mỗi check, luôn ghi dù pass hay fail
# ============================================================
def insert_quality_metric(
    spark: SparkSession,
    run_id: str,
    layer: str,
    table_name: str,
    check_name: str,
    metric_value,
    passed: bool,
    rundate: str
) -> bool:
    try:
        schema = StructType([
            StructField("metric_id", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("check_name", StringType(), True),
            StructField("metric_value", StringType(), True),  # string để tránh lệch kiểu số giữa các check
            StructField("passed", StringType(), True),
            StructField("rundate", StringType(), True),
            StructField("created_at", TimestampNTZType(), True),
        ])

        data = [(
            uuid.uuid4().hex,
            run_id, layer, table_name, check_name,
            str(metric_value), str(bool(passed)), rundate, None
        )]

        df = spark.createDataFrame(data, schema)
        df = df.withColumn("created_at", current_timestamp().cast("timestamp_ntz"))

        df.write.format("delta").mode("append").saveAsTable(QUALITY_METRICS_TABLE)
        print(f"[QUALITY_METRIC] {layer}.{table_name}.{check_name} = {metric_value} (passed={passed})")
        return True

    except Exception as e:
        # Không raise: ghi metric thất bại không được phép làm sập pipeline chính
        print(f"[QUALITY_METRIC] Insert failed: {e}")
        return False


# ============================================================
# FAILED_RECORDS — quarantine: log nhẹ (record_id + lý do), không lưu full row
# Dùng chung cho MỌI bảng, mọi layer (na.drop, dedupe, business rule violation...)
# ============================================================
def insert_failed_records(
    spark: SparkSession,
    run_id: str,
    layer: str,
    table_name: str,
    df_rejected: DataFrame,
    key_columns: list,
    failure_reason: str,
    rundate: str
) -> int:
    if df_rejected is None:
        return 0

    count = df_rejected.count()
    if count == 0:
        return 0

    try:
        light_df = (
            df_rejected
            .select(concat_ws("|", *[col(c) for c in key_columns]).alias("record_id"))
            .withColumn("failed_record_id", expr("uuid()"))
            .withColumn("run_id", lit(run_id))
            .withColumn("layer", lit(layer))
            .withColumn("table_name", lit(table_name))
            .withColumn("failure_reason", lit(failure_reason))
            .withColumn("rundate", lit(rundate))
            .withColumn("created_at", current_timestamp().cast("timestamp_ntz"))
        )

        # 3. Ghi dữ liệu
        light_df.write.format("delta").mode("append").saveAsTable("control.failed_records")
        print(f"[FAILED_RECORDS] {layer}.{table_name}: {count} record(s) logged ({failure_reason})")
        
        # 4. Trả về biến count đã đếm ở bước 1
        return count

    except Exception as e:
        print(f"[FAILED_RECORDS] Insert failed: {e}")
        return 0


# ============================================================
# AUDIT_LOG — 1 dòng / 1 lần chạy task (PASS | PARTIAL_FAIL | FAIL)
# ============================================================
def insert_audit(
    spark: SparkSession,
    run_id: str,
    layer: str,
    table_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    input_rows: int = None,
    output_rows: int = None,
    rejected_rows: int = None,
    error_message: str = None,
    rundate: str = None
) -> bool:
    try:
        schema = StructType([
            StructField("audit_id", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("input_rows", LongType(), True),
            StructField("output_rows", LongType(), True),
            StructField("rejected_rows", LongType(), True),
            StructField("started_at", TimestampNTZType(), True),
            StructField("finished_at", TimestampNTZType(), True),
            StructField("error_message", StringType(), True),
            StructField("rundate", StringType(), True),
        ])

        data = [(
            uuid.uuid4().hex, run_id, layer, table_name, status,
            input_rows, output_rows, rejected_rows,
            started_at, finished_at, error_message,
            rundate or str(date.today())
        )]

        df = spark.createDataFrame(data, schema)
        df.write.format("delta").mode("append").saveAsTable(AUDIT_LOG_TABLE)
        print(f"[AUDIT_LOG] {layer}.{table_name} -> {status} (run_id={run_id})")
        return True

    except Exception as e:
        print(f"[AUDIT_LOG] Insert audit failed: {e}")
        return False