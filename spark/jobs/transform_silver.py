from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from datetime import datetime, date
from pyspark.sql.functions import col, lit, max as spark_max
from job_control import generate_run_id, get_watermark, insert_log, insert_audit
from validation import (
    validate_row_count,
    validate_business_rule,
    validate_no_duplicates,
    dedupe_by_key,
)
import argparse

SILVER_RULES = {
    "users": {
        "unique_keys": ["user_id"],
        "not_null": ["user_id", "signup_date", "country"],
        "business_rules": {
            "age_in_range": lambda df: (col("age") >= 0) & (col("age") <= 120),
        },
    },
    "products": {
        "unique_keys": ["product_id"],
        "not_null": ["product_id", "product_name", "price"],
        "business_rules": {
            "price_positive": lambda df: col("price") > 0,
            "stock_non_negative": lambda df: col("stock_quantity") >= 0,
        },
    },
    "sessions": {
        "unique_keys": ["session_id"],
        "not_null": ["session_id", "user_id"],
        "business_rules": {},
    },
    "interactions": {
        "unique_keys": ["interaction_id"],
        "not_null": ["user_id", "product_id", "session_id"],
        "business_rules": {
            "dwell_time_non_negative": lambda df: col("dwell_time_ms") >= 0,
        },
    },
    "purchases": {
        "unique_keys": ["purchase_id"],
        "not_null": ["purchase_id", "user_id", "product_id", "total_amount"],
        "business_rules": {
            "amount_positive": lambda df: col("total_amount") > 0,
            "quantity_positive": lambda df: col("quantity") > 0,
        },
    },
    "reviews": {
        "unique_keys": ["review_id"],
        "not_null": ["review_id", "user_id", "product_id", "rating"],
        "business_rules": {
            "rating_in_range": lambda df: (col("rating") >= 1) & (col("rating") <= 5),
        },
    },
}


def create_spark_session():
    return SparkSession.builder \
        .appName("TransformSilver") \
        .enableHiveSupport() \
        .getOrCreate()


def read_incremental_bronze(spark, table_name):
    bronze_table = f"bronze.bronze_{table_name}"
    last_watermark = get_watermark(spark, layer="silver", table_name=table_name)

    df = spark.read.table(bronze_table)

    if last_watermark is None:
        print(f"[Silver] {table_name}: FIRST RUN")
        return df, None

    print(f"[Silver] {table_name}: Previous watermark = {last_watermark}")
    df = df.filter(col("ingest_at") > lit(last_watermark))
    return df, last_watermark


# ---------------- CLEAN FUNCTIONS (giữ nguyên logic gốc) ----------------

def clean_users(df: DataFrame) -> DataFrame:
    df = df.select(
        col('user_id'), col('age'), col('gender'), col('country'), col('city'),
        col('signup_date'), col('income_level'), col('preferred_category'), col('loyalty_tier')
    )
    df_clean = df.na.drop(subset=["user_id"])
    return dedupe_by_key(df_clean, ["user_id"], order_column="signup_date", keep="latest")


def clean_products(df: DataFrame) -> DataFrame:
    df = df.select(
        col('product_id'), col('product_name'), col('product_description'), col('category'),
        col('subcategory'), col('brand'), col('price'), col('rating_avg'),
        col('review_count'), col('stock_quantity'), col('date_added')
    )
    # rating_avg/review_count được phép NULL — sản phẩm chưa có review
    df_clean = df.na.drop(subset=["product_id", "product_name", "price"])
    return dedupe_by_key(df_clean, ["product_id"], order_column="date_added")


def clean_sessions(df: DataFrame) -> DataFrame:
    df = df.select(
        col('session_id'), col('user_id'), col('start_time'),
        col('device_type'), col('referrer_source'), col('is_converted')
    )
    df_clean = df.na.drop(subset=["session_id", "user_id"])
    return dedupe_by_key(df_clean, ["session_id"], order_column="start_time")


def clean_interactions(df: DataFrame) -> DataFrame:
    df = df.select(
        col('interaction_id'), col('user_id'), col('product_id'), col('session_id'),
        col('interaction_type'), col('interaction_timestamp'), col('dwell_time_ms')
    )
    df_clean = df.na.drop(subset=["interaction_id", "user_id", "product_id", "session_id"])
    return dedupe_by_key(df_clean, ["interaction_id"], order_column="dwell_time_ms")


def clean_purchases(df: DataFrame) -> DataFrame:
    df = df.select(
        col("purchase_id"), col("order_id"), col('user_id'), col('product_id'), col('session_id'),
        col("interaction_id"), col("quantity"), col("unit_price"), col("total_amount"), col('order_date')
    )
    df_clean = df.na.drop(subset=["purchase_id", "order_id", "user_id", "product_id", "total_amount"])
    return dedupe_by_key(df_clean, ["purchase_id"], order_column="order_date")


def clean_reviews(df: DataFrame) -> DataFrame:
    df = df.select(
        col("review_id"), col('user_id'), col('product_id'), col("purchase_id"),
        col("rating"), col("title"), col("review_text"), col("review_date")
    )
    df_clean = df.na.drop(subset=["review_id", "user_id", "product_id", "rating"])
    return dedupe_by_key(df_clean, ["review_id"], order_column="review_date")


# ---------------- PROCESS TABLE ----------------

def process_table(
    spark,
    table_name,
    clean_fn,
    use_merge=False
):
    layer = "silver"
    run_id = generate_run_id(layer, table_name)
    started_at = datetime.now()
    rundate = str(date.today())

    silver_table = f"silver.silver_{table_name}"
    rules = SILVER_RULES[table_name]
    unique_keys = rules["unique_keys"]

    input_rows = 0
    output_rows = 0
    rejected_rows = 0

    print(f"\n========== SILVER: {table_name} ==========")

    try:
        df_raw, last_watermark = read_incremental_bronze(spark, table_name)
        print(f"[Watermark] Previous: {last_watermark}")

        if df_raw.limit(1).count() == 0:
            print(f"[Silver] {table_name}: No new data")
            finished_at = datetime.now()
            insert_audit(
                spark=spark, run_id=run_id, layer=layer, table_name=table_name,
                status="PASS", started_at=started_at, finished_at=finished_at,
                input_rows=0, output_rows=0, rejected_rows=0, rundate=rundate,
            )
            return None

        input_rows = df_raw.count()

        df_clean = clean_fn(df_raw)

        # ---- Data Quality: Completeness (row loss do na.drop/dedupe) + quarantine ----
        df_clean = validate_row_count(
            spark, run_id, df_raw, df_clean,
            layer=layer, table_name=table_name, rundate=rundate,
            max_loss_pct=10.0, key_columns=unique_keys,
            severity="BLOCKING"
        )

        # ---- Data Quality: Integrity (unique key sau dedupe) ----
        # validate_no_duplicates(
        #     spark, run_id, df_clean, unique_keys,
        #     layer=layer, table_name=table_name, rundate=rundate
        # )

        # ---- Data Quality: Validity (business rules), quarantine dòng vi phạm ----
        for rule_name, condition_fn in rules["business_rules"].items():
            validate_business_rule(
                spark, run_id, df_clean, rule_name,
                condition=condition_fn(df_clean), key_columns=unique_keys,
                layer=layer, table_name=table_name, rundate=rundate,
                max_violation_pct=1.0, severity="BLOCKING"
            )

        # thêm metadata
        ingest_time = spark.sql("SELECT current_timestamp()").first()[0]
        df_clean = (
            df_clean
            .withColumn("ingest_at", lit(ingest_time))
            .withColumn("source", lit("bronze"))
        )

        if not spark.catalog.tableExists(silver_table):
            df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
            print(f"[Silver] Created {silver_table}")

        elif use_merge:
            target = DeltaTable.forName(spark, silver_table)
            merge_condition = " AND ".join([f"tgt.{key} = src.{key}" for key in unique_keys])
            (
                target.alias("tgt")
                .merge(df_clean.alias("src"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            print(f"[Silver] MERGE completed: {silver_table}")

        else:
            df_clean.write.format("delta").mode("append").saveAsTable(silver_table)
            print(f"[Silver] APPEND completed: {silver_table}")

        output_rows = df_clean.count()
        rejected_rows = input_rows - output_rows

        new_watermark = df_raw.select(spark_max("ingest_at").alias("watermark_value")).first()["watermark_value"]
        print(f"[Watermark] New: {new_watermark}")

        insert_log(
            spark=spark, layer=layer, table_name=table_name,
            watermark_column="ingest_at", watermark_value=new_watermark, rundate=rundate,
        )

        print(f"[Silver] {table_name}: {output_rows} rows -> {silver_table}")

        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name,
            status="PASS", started_at=started_at, finished_at=finished_at,
            input_rows=input_rows, output_rows=output_rows, rejected_rows=rejected_rows,
            rundate=rundate,
        )
        print(f"[AUDIT] SUCCESS run_id={run_id}")

        return df_clean

    except Exception as e:
        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name,
            status="FAIL", started_at=started_at, finished_at=finished_at,
            input_rows=input_rows, output_rows=output_rows, rejected_rows=rejected_rows,
            error_message=str(e)[:2000], rundate=rundate,
        )
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise


def parse_args():
    parser = argparse.ArgumentParser(description="Transform một bảng từ Bronze sang Silver")
    parser.add_argument("--table", required=True, choices=list(SILVER_RULES.keys()))
    return parser.parse_args()


def main():
    table_configs = {
        "users":        {"clean_fn": clean_users,  "use_merge": True},
        "products":     {"clean_fn": clean_products,"use_merge": True},
        "sessions":     {"clean_fn": clean_sessions,"use_merge": True},
        "interactions": {"clean_fn": clean_interactions,"use_merge": True},
        "purchases":    {"clean_fn": clean_purchases, "use_merge": True},
        "reviews":      {"clean_fn": clean_reviews,"use_merge": True},
    }

    args = parse_args()
    table_name = args.table
    config = table_configs[table_name]

    spark = create_spark_session()
    print("--- BẮT ĐẦU TRANSFORM SILVER ---")

    try:
        process_table(
            spark,
            table_name,
            config["clean_fn"],
            use_merge=config["use_merge"],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()