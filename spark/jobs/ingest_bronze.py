from pyspark.sql import SparkSession
import argparse
from validation import validate_schema, validate_minimum_row_count
from job_control import generate_run_id, get_watermark, insert_log, insert_audit
from pyspark.sql.functions import *
from datetime import datetime, date


def create_spark_session():
    return SparkSession.builder \
        .appName("IngestBronze") \
        .getOrCreate()


BRONZE_RULES = {
    "users": {
        "expected_columns": {
            "user_id": "string",
            "age": "int",
            "gender": "string",
            "country": "string",
            "city": "string",
            "signup_date": "date",
            "income_level": "string",
            "preferred_category": "string",
            "loyalty_tier": "string"
        },
        "min_rows": 1,
        "duplicate_key": ["user_id"],
        "watermark_column": "signup_date",
    },
    "products": {
        "expected_columns": {
            "product_id": "string",
            "product_name": "string",
            "product_description": "string",
            "category": "string",
            "subcategory": "string",
            "brand": "string",
            "price": "decimal(18,2)",
            "rating_avg": "double",  # Postgres float map sang Spark double
            "review_count": "int",
            "stock_quantity": "int",
            "date_added": "date"
        },
        "min_rows": 1,
        "duplicate_key": ["product_id"],
        "watermark_column": "date_added",
    },
    "sessions": {
        "expected_columns": {
            "session_id": "string",
            "user_id": "string",
            "start_time": "timestamp",
            "device_type": "string",
            "referrer_source": "string",
            "is_converted": "boolean"
        },
        "min_rows": 1,
        "duplicate_key": ["session_id"],
        "watermark_column": "start_time",
    },
    "interactions": {
        "expected_columns": {
            "interaction_id": "string",
            "user_id": "string",
            "product_id": "string",
            "session_id": "string",
            "interaction_type": "string",
            "interaction_timestamp": "timestamp",
            "dwell_time_ms": "int"
        },
        "min_rows": 1,
        "duplicate_key": ["interaction_id"],
        "watermark_column": "interaction_timestamp",
    },
    "purchases": {
        "expected_columns": {
            "purchase_id": "string",
            "order_id": "string",
            "user_id": "string",
            "product_id": "string",
            "session_id": "string",
            "interaction_id": "string",
            "quantity": "int",
            "unit_price": "decimal(18,2)",
            "total_amount": "decimal(18,2)",
            "order_date": "timestamp"
        },
        "min_rows": 1,
        "duplicate_key": ["purchase_id"],
        "watermark_column": "order_date",
    },
    "reviews": {
        "expected_columns": {
            "review_id": "string",
            "user_id": "string",
            "product_id": "string",
            "purchase_id": "string",
            "rating": "int",
            "title": "string",
            "review_text": "string",
            "review_date": "timestamp"
        },
        "min_rows": 1,
        "duplicate_key": ["review_id"],
        "watermark_column": "review_date",
    },
}


def _read_from_postgres(spark, table_name, watermark_column, last_watermark):
    if last_watermark is None:
        print("[Load] First run -> FULL LOAD")
        return (
            spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
            .option("dbtable", table_name)
            .option("user", "admin")
            .option("password", "admin123")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    print("[Load] Incremental load")
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {watermark_column} > TIMESTAMP '{last_watermark}'
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
        .option("dbtable", f"({query}) AS source")
        .option("user", "admin")
        .option("password", "admin123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def ingest_table(spark, table_name):
    layer = "bronze"
    run_id = generate_run_id(layer, table_name)
    started_at = datetime.now()
    rundate = str(date.today())

    bronze_table = f"bronze.bronze_{table_name}"
    table = BRONZE_RULES[table_name]
    watermark_column = table["watermark_column"]

    input_rows = 0
    output_rows = 0

    try:
        last_watermark = get_watermark(spark, layer, table_name)
        print(f"[Watermark] Previous: {last_watermark}")

        df = _read_from_postgres(spark, table_name, watermark_column, last_watermark)

        # ---- Data Quality: Validity (schema) + Completeness (min rows) ----
        validate_schema(
            spark, run_id, df, table["expected_columns"],
            layer=layer, table_name=table_name, rundate=rundate,
            severity="BLOCKING"
        )

        input_rows = df.count()

        validate_minimum_row_count(
            spark, run_id, df, table["min_rows"],
            layer=layer, table_name=table_name, rundate=rundate,
            severity="WARNING"  # cảnh báo, không chặn — incremental load ít dòng vẫn là hợp lệ
        )

        # thêm metadata
        ingest_time = spark.sql("SELECT current_timestamp()").first()[0]

        df = (
            df
            .withColumn("ingest_at", lit(ingest_time))
            .withColumn("source", lit("postgresql"))
        )

        if last_watermark is None:
            df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
            print(f"[BRONZE] Created {bronze_table}")
        else:
            df.write.format("delta").mode("append").saveAsTable(bronze_table)
            print(f"[BRONZE] APPEND completed: {bronze_table}")

        output_rows = df.count()
        print(f"[Bronze] Ingested table {table_name}: {output_rows} rows -> {bronze_table}")

        new_watermark = df.agg(
            max(col(watermark_column)).alias("max_value")
        ).first()["max_value"]

        print(f"[Watermark] New: {new_watermark}")

        insert_log(
            spark=spark,
            layer=layer,
            table_name=table_name,
            watermark_column=watermark_column,
            watermark_value=new_watermark,
            rundate=rundate
        )

        finished_at = datetime.now()

        insert_audit(
            spark=spark,
            run_id=run_id,
            layer=layer,
            table_name=table_name,
            status="PASS",
            started_at=started_at,
            finished_at=finished_at,
            input_rows=input_rows,
            output_rows=output_rows,
            rejected_rows=0,  # bronze chưa loại bỏ dòng nào, chỉ validate schema/min-rows
            rundate=rundate,
        )

        print(f"[AUDIT] SUCCESS run_id={run_id}")

    except Exception as e:
        finished_at = datetime.now()
        insert_audit(
            spark=spark,
            run_id=run_id,
            layer=layer,
            table_name=table_name,
            status="FAIL",
            started_at=started_at,
            finished_at=finished_at,
            input_rows=input_rows,
            output_rows=output_rows,
            rejected_rows=None,
            error_message=str(e)[:2000],  # tránh log quá dài
            rundate=rundate,
        )
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest một bảng từ PostgreSQL vào Bronze layer"
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=list(BRONZE_RULES.keys()),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    table_name = args.table

    spark = create_spark_session()

    print(f"--- BẮT ĐẦU LOAD TỪ DATABASE: {table_name} ---")

    try:
        ingest_table(spark, table_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()