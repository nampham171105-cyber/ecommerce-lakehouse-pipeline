from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, to_timestamp, lit, max as spark_max
from delta import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampNTZType
from datetime import datetime, date

JOB_CONTROL_TABLE = "control.job_control"
AUDIT_LOG_TABLE = "control.audit_log"
FAILED_RECORDS_TABLE = "control.failed_records"
QUALITY_METRICS_TABLE = "control.quality_metrics"


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

# Insert một log sau khi Bronze load thành công
def insert_log(
    spark: SparkSession,
    layer: str,
    table_name: str,
    watermark_column: str,
    watermark_value,
    rundate: str
) -> bool:
    try:
        # Định nghĩa rõ Schema để tránh Spark tự suy luận sai kiểu dữ liệu
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


# Lấy watermark gần nhất của một table
def get_watermark(
    spark: SparkSession,
    layer: str,
    table_name: str
):
    try:
        df = spark.read.table(JOB_CONTROL_TABLE)

        row = (
            df.filter(
                (df.layer == layer) &
                (df.table_name == table_name)
            )
            .agg(
                spark_max("watermark_value").alias("watermark_value")
            ).first()
        )

        if row["watermark_value"] is not None:
            return row["watermark_value"]
        # Chưa từng chạy → full load
        return None

    except Exception as e:
        print(f"[JOB_CONTROL] Get watermark failed: {e}")
        return None


#Xóa log của một table → dùng khi muốn reset và chạy full load
def delete_log(
    spark: SparkSession,
    layer: str,
    table_name: str
) -> bool:
    try:
        delta_table = DeltaTable.forName(
            spark,
            JOB_CONTROL_TABLE
        )

        delta_table.delete(
            f"schema_name = '{layer}' "
            f"AND table_name = '{table_name}'"
        )

        print(
            f"[JOB_CONTROL] Deleted logs: "
            f"{layer}.{table_name}"
        )

        return True

    except Exception as e:
        print(f"[JOB_CONTROL] Delete log failed: {e}")
        return False


# Xóa toàn bộ job control → reset toàn bộ pipeline
def truncate_logs(spark: SparkSession) -> bool:
    try:
        delta_table = DeltaTable.forName(
            spark,
            JOB_CONTROL_TABLE
        )

        delta_table.delete("1 = 1")

        print("[JOB_CONTROL] All logs deleted")

        return True

    except Exception as e:
        print(f"[JOB_CONTROL] Truncate logs failed: {e}")
        return False

# Ghi 1 dòng kết quả kiểm tra DQ (QualityMetrics)
def insert_quality_metric(
    spark: SparkSession,
    layer: str,
    table_name: str,
    check_name: str,
    metric_value: float,
    passed: bool,
    rundate: str
) -> bool:
    try:
        schema = StructType([
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("check_name", StringType(), True),
            StructField("metric_value", StringType(), True),  # để dạng string, cast khi query - tránh lỗi kiểu số khác nhau giữa các check
            StructField("passed", StringType(), True),
            StructField("rundate", StringType(), True),
            StructField("created_at", TimestampNTZType(), True),
        ])
 
        data = [(
            layer, table_name, check_name,
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
 
 
# Ghi lịch sử thực thi kèm trạng thái DQ (AuditLog)
def insert_audit(
    spark: SparkSession,
    layer: str,
    table_name: str,
    status: str,      # "PASS" | "PARTIAL_FAIL" | "FAIL"
    details: str,
    rundate: str
) -> bool:
    try:
        schema = StructType([
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("details", StringType(), True),
            StructField("rundate", StringType(), True),
            StructField("executed_at", TimestampNTZType(), True),
        ])
 
        data = [(layer, table_name, status, details, rundate, None)]
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("executed_at", current_timestamp().cast("timestamp_ntz"))
 
        df.write.format("delta").mode("append").saveAsTable(AUDIT_LOG_TABLE)
        print(f"[AUDIT_LOG] {layer}.{table_name} -> {status}")
        return True
 
    except Exception as e:
        print(f"[AUDIT_LOG] Insert audit failed: {e}")
        return False
 
 
# Ghi log nhẹ cho từng bản ghi bị loại (FailedRecords) — dùng chung cho MỌI bảng,
# bất kể bảng đó có bật quarantine (lưu full row) hay không.
def insert_failed_records(
    spark: SparkSession,
    layer: str,
    table_name: str,
    df_rejected: DataFrame,
    key_columns: list,
    failure_reason: str,
    rundate: str
) -> int:
    if df_rejected.limit(1).count() == 0:
        return 0
 
    try:
        light_df = (
            df_rejected
            .select(concat_ws("|", *[col(c) for c in key_columns]).alias("record_id"))
            .withColumn("layer", lit(layer))
            .withColumn("table_name", lit(table_name))
            .withColumn("failure_reason", lit(failure_reason))
            .withColumn("rundate", lit(rundate))
            .withColumn("logged_at", current_timestamp().cast("timestamp_ntz"))
        )
 
        light_df.write.format("delta").mode("append").saveAsTable(FAILED_RECORDS_TABLE)
        count = light_df.count()
        print(f"[FAILED_RECORDS] {layer}.{table_name}: {count} record(s) logged")
        return count
 
    except Exception as e:
        print(f"[FAILED_RECORDS] Insert failed: {e}")
        return 0