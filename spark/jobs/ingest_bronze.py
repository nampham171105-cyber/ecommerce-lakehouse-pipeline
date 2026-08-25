from pyspark.sql import SparkSession
from utils.validation import validate_schema, validate_minimum_row_count
from utils.job_control import get_watermark, insert_log
from pyspark.sql.functions import *
from datetime import date


def create_spark_session():
    return SparkSession.builder \
        .appName("IngestBronze") \
        .getOrCreate()

BRONZE_RULES = {
    "users": {
        "expected_columns": {
            "user_id", "age", "gender", "country", "city", "signup_date",
            "income_level", "preferred_category", "loyalty_tier"
        },
        "min_rows": 1,
        "duplicate_key": ["user_id"],
        # "watermark_column": "updated_at",
        "watermark_column": "signup_date",
    },
    "products": {
        "expected_columns": {
            "product_id", "product_name", "product_description", "category",
            "subcategory", "brand", "price", "rating_avg", "review_count",
            "stock_quantity", "date_added"
        },
        "min_rows": 1,
        "duplicate_key": ["product_id"],
        # "watermark_column": "updated_at",
        "watermark_column": "date_added",
    },
    "sessions": {
        "expected_columns": {
            "session_id", "user_id", "start_time", "device_type",
            "referrer_source", "is_converted"
        },
        "min_rows": 1,
        "duplicate_key": ["session_id"],
        "watermark_column": "start_time",
    },
    "interactions": {
        "expected_columns": {
            "interaction_id", "user_id", "product_id", "session_id",
            "interaction_type", "interaction_timestamp", "dwell_time_ms"
        },
        "min_rows": 1,
        "duplicate_key": ["interaction_id"],
        "watermark_column": "interaction_timestamp",
    },
    "purchases": {
        "expected_columns": {
            "purchase_id", "order_id", "user_id", "product_id", "session_id",
            "interaction_id", "quantity", "unit_price", "total_amount", "order_date"
        },
        "min_rows": 1,
        "duplicate_key": ["purchase_id"],
        "watermark_column": "order_date",
    },
    "reviews": {
        "expected_columns": {
            "review_id", "user_id", "product_id", "purchase_id", "rating",
            "title", "review_text", "review_date"
        },
        "min_rows": 1,
        "duplicate_key": ["review_id"],
        "watermark_column": "review_date",
    },
}

def ingest_table(spark, table_name):

    bronze_table = f"bronze.bronze_{table_name}"
    table = BRONZE_RULES[table_name]
    watermark_column = table["watermark_column"]

    last_watermark = get_watermark(spark, "bronze",table_name)

    print(f"[Watermark] Previous: {last_watermark}")

    if last_watermark is None:
        print("[Load] First run -> FULL LOAD")

        df = spark.read \
            .format("jdbc") \
            .option(
                "url",
                "jdbc:postgresql://postgres:5432/ecommerce"
            ) \
            .option("dbtable", table_name) \
            .option("user", "admin") \
            .option("password", "admin123") \
            .option("driver", "org.postgresql.Driver") \
            .load()

    else:
        print("[Load] Incremental load")

        query = f"""
            SELECT *
            FROM {table_name}
            WHERE {watermark_column} >
                  TIMESTAMP '{last_watermark}'
        """

        df = spark.read \
            .format("jdbc") \
            .option(
                "url",
                "jdbc:postgresql://postgres:5432/ecommerce"
            ) \
            .option(
                "dbtable",
                f"({query}) AS source"
            ) \
            .option("user", "admin") \
            .option("password", "admin123") \
            .option("driver", "org.postgresql.Driver") \
            .load()

    validate_schema(df, table["expected_columns"], bronze_table)
    # validate_minimum_row_count(df, table["min_rows"], bronze_table)

    # thêm metadata
    ingest_time = spark.sql("SELECT current_timestamp()").first()[0]

    df = (
        df
        .withColumn("ingest_at", lit(ingest_time)) # viết ingest time để spark trả về ngay mà không cần phải đợi action
        .withColumn("source", lit("postgresql"))
    )

    if last_watermark is None:
        df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        print(f"[BRONZE] Created {bronze_table}")
    else:
        df.write.format("delta").mode("append").saveAsTable(bronze_table)
        print(f"[BRONZE] APPEND completed: {bronze_table}")
    # df.write.format("delta").mode("overwrite").option("path", target_path).saveAsTable(table)
    
    print(f"[Bronze] Ingested table {table_name}: {df.count()} rows -> {bronze_table}")

    new_watermark = df.agg(
        max(col(watermark_column)).alias("max_value")
    ).first()["max_value"]

    print(f"[Watermark] New: {new_watermark}")

    insert_log(
        spark=spark,
        layer="bronze",
        table_name=table_name,
        watermark_column=watermark_column,
        watermark_value=str(new_watermark),
        rundate=str(date.today())
    )

def main():
    spark = create_spark_session()

    print("--- BẮT ĐẦU LOAD TỪ DATABASE ---")
    tables = [
        "users", 
        "products", 
        "sessions", 
        "interactions", 
        "purchases", 
        "reviews"
    ]
    
    for table in tables:
        ingest_table(spark, table)

    spark.stop()

if __name__ == "__main__":
    main()