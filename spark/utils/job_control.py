from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, max as spark_max
from delta import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampNTZType
from datetime import datetime, date

JOB_CONTROL_TABLE = "control.job_control"

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
        return False


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