from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from job_control import generate_run_id, get_watermark, insert_log, insert_audit
from validation import validate_business_rule
from delta.tables import DeltaTable
from datetime import datetime, date
import argparse


def create_spark_session():
    return SparkSession.builder \
        .appName("TransformGold") \
        .enableHiveSupport() \
        .getOrCreate()


def read_incremental_silver(spark, table_name, gold_table_name):
    silver_table = f"silver.silver_{table_name}"

    last_watermark = get_watermark(spark, layer="gold", table_name=gold_table_name)

    df = spark.read.table(silver_table)

    if last_watermark is None:
        print(f"[GOLD] {table_name}: FIRST RUN")
        return df, None

    print(f"[GOLD] {table_name}: Previous watermark = {last_watermark}")
    df = df.filter(col("ingest_at") > lit(last_watermark))
    return df, last_watermark


def add_surrogate_key(df: DataFrame, natural_key_cols: list, sk_name: str) -> DataFrame:
    return df.withColumn(sk_name, sha2(concat_ws("||", *[col(c) for c in natural_key_cols]), 256))


def _merge_scd1(spark, updates_df: DataFrame, target_table: str, merge_key: list):
    if not spark.catalog.tableExists(target_table):
        print(f"[Gold][SCD1] {target_table} chưa tồn tại -> full load lần đầu")
        updates_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    dim_table = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_key])

    (
        dim_table.alias("tgt")
        .merge(updates_df.alias("src"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


_ROW_HASH_SQL = """
    sha2(concat_ws('||',
        COALESCE(CAST(age AS STRING), '__NULL__'),
        COALESCE(gender, '__NULL__'),
        COALESCE(country, '__NULL__'),
        COALESCE(city, '__NULL__'),
        COALESCE(CAST(signup_date AS STRING), '__NULL__'),
        COALESCE(income_level, '__NULL__'),
        COALESCE(preferred_category, '__NULL__'),
        COALESCE(loyalty_tier, '__NULL__')
    ), 256)
"""

_INITIAL_LOAD_SQL = f"""
CREATE TABLE gold.dim_user USING DELTA AS
SELECT
    sha2(concat_ws('||', user_id, CAST(ingest_at AS STRING)), 256) AS user_sk,
    user_id, age, gender, country, city, signup_date,
    income_level, preferred_category, loyalty_tier,
    {_ROW_HASH_SQL} AS row_hash,
    CAST(signup_date AS TIMESTAMP) AS effective_start_date,
    CAST(NULL AS DATE) AS effective_end_date,
    true AS is_current
FROM stg_users_incremental
"""

_MERGE_SCD2_SQL = f"""
WITH source_prepared AS (
    SELECT
        sha2(concat_ws('||', user_id, CAST(ingest_at AS STRING)), 256) AS user_sk,
        user_id, age, gender, country, city, signup_date,
        income_level, preferred_category, loyalty_tier,
        {_ROW_HASH_SQL} AS row_hash,
        current_date() AS effective_start_date,
        CAST(NULL AS DATE) AS effective_end_date,
        true AS is_current
    FROM stg_users_incremental
)
MERGE INTO gold.dim_user AS target
USING (
    SELECT NULL AS mergeKey, src.*
    FROM source_prepared AS src
    JOIN gold.dim_user AS tgt
        ON src.user_id = tgt.user_id
        AND tgt.is_current = true
    WHERE src.row_hash <> tgt.row_hash

    UNION ALL

    SELECT user_id AS mergeKey, src.*
    FROM source_prepared AS src
) AS staged_updates
ON target.user_id = staged_updates.mergeKey
   AND target.is_current = true

WHEN MATCHED AND target.is_current = true
    AND target.row_hash <> staged_updates.row_hash THEN
    UPDATE SET
        target.is_current = false,
        target.effective_end_date = staged_updates.effective_start_date

WHEN NOT MATCHED THEN
    INSERT (
        user_sk, user_id, age, gender, country, city, signup_date,
        income_level, preferred_category, loyalty_tier,
        row_hash, effective_start_date, effective_end_date, is_current
    )
    VALUES (
        staged_updates.user_sk, staged_updates.user_id, staged_updates.age,
        staged_updates.gender, staged_updates.country, staged_updates.city,
        staged_updates.signup_date, staged_updates.income_level,
        staged_updates.preferred_category, staged_updates.loyalty_tier,
        staged_updates.row_hash, staged_updates.effective_start_date,
        staged_updates.effective_end_date, staged_updates.is_current
    )
"""


def build_dim_user_scd2(spark):
    layer = "gold"
    table_name = "users"
    gold_table = "dim_user"
    run_id = generate_run_id(layer, gold_table)
    started_at = datetime.now()
    rundate = str(date.today())
    input_rows = 0

    try:
        df_raw, last_watermark = read_incremental_silver(spark, table_name, gold_table)
        print(f"[Watermark] Previous: {last_watermark}")

        if df_raw.limit(1).count() == 0:
            print(f"[Gold] {table_name}: No new data")
            insert_audit(
                spark=spark, run_id=run_id, layer=layer, table_name=gold_table,
                status="PASS", started_at=started_at, finished_at=datetime.now(),
                input_rows=0, output_rows=0, rejected_rows=0, rundate=rundate,
            )
            return

        input_rows = df_raw.count()
        df_raw.createOrReplaceTempView("stg_users_incremental")

        if not spark.catalog.tableExists(f"gold.{gold_table}"):
            print(f"[Gold][SCD2] {gold_table} chưa tồn tại -> full load lần đầu")
            spark.sql(_INITIAL_LOAD_SQL)
        else:
            print("[Gold][SCD2] Chạy MERGE SCD2")
            spark.sql(_MERGE_SCD2_SQL)

        row_count = spark.read.table("gold.dim_user").count()
        print(f"[Gold][SCD2] gold.dim_user: {row_count} rows (tổng, gồm cả version lịch sử)")

        new_watermark = df_raw.agg(max("ingest_at")).collect()[0][0]
        print(f"[Watermark] New: {new_watermark}")

        insert_log(
            spark=spark, layer=layer, table_name=gold_table,
            watermark_column="ingest_at", watermark_value=new_watermark, rundate=rundate,
        )

        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=gold_table,
            status="PASS", started_at=started_at, finished_at=finished_at,
            input_rows=input_rows, output_rows=input_rows, rejected_rows=0, rundate=rundate,
        )
        print(f"[AUDIT] SUCCESS run_id={run_id}")

    except Exception as e:
        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=gold_table,
            status="FAIL", started_at=started_at, finished_at=finished_at,
            input_rows=input_rows, output_rows=0, rejected_rows=None,
            error_message=str(e)[:2000], rundate=rundate,
        )
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise


def build_dim_product(spark, df: DataFrame) -> DataFrame:
    df = add_surrogate_key(df, ["product_id"], "product_sk")
    return df.select(
        col('product_sk'), col('product_id'), col('product_name'), col('product_description'),
        col('category'), col('subcategory'), col('brand'), col('price'),
        col('rating_avg'), col('review_count'), col('stock_quantity'), col('date_added')
    )


def build_dim_date(spark) -> DataFrame:
    begin_date = "2023-01-01"
    end_date = "2027-01-01"
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as date"
    )
    df = add_surrogate_key(df, ["date"], "date_sk")
    return df.select(
        col("date_sk"), col("date"),
        year(col("date")).alias("year"),
        quarter(col("date")).alias("quarter"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        dayofweek(col("date")).alias("day_of_week")  # 1: Chủ nhật, 7: Thứ bảy
    )


def build_dim_session(spark, df: DataFrame) -> DataFrame:
    df = add_surrogate_key(df, ["session_id"], "session_sk")
    return df.select(
        col("session_sk"), col('session_id'), col('device_type'),
        col('referrer_source'), col('is_converted'), col('start_time')
    )


def build_fact_interaction(spark, df: DataFrame) -> DataFrame:
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True).select(col("user_sk"), col("user_id"))
    dim_product = spark.read.table("gold.dim_product").select(col("product_sk"), col("product_id"))
    dim_session = spark.read.table("gold.dim_session").select(col("session_sk"), col("session_id"))
    dim_date = spark.read.table("gold.dim_date").select(col("date_sk"), col("date"))

    fact_interaction = (
        df
        .join(dim_user, on="user_id", how="left")
        .join(dim_product, on="product_id", how="left")
        .join(dim_session, on="session_id", how="left")
        .join(broadcast(dim_date), to_date(col("interaction_timestamp")) == col("date"), how="left")
    )
    return fact_interaction.select(
        col('interaction_id'), col('user_sk'), col('product_sk'), col('session_sk'), col('date_sk'),
        col('interaction_type'), col('dwell_time_ms'), col('interaction_timestamp')
    )


def build_fact_purchase(spark, df: DataFrame) -> DataFrame:
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True).select(col("user_sk"), col("user_id"))
    dim_product = spark.read.table("gold.dim_product").select(col("product_sk"), col("product_id"))
    dim_session = spark.read.table("gold.dim_session").select(col("session_sk"), col("session_id"))
    dim_date = spark.read.table("gold.dim_date").select(col("date_sk"), col("date"))

    fact_purchase = (
        df
        .join(dim_user, on="user_id", how="left")
        .join(dim_product, on="product_id", how="left")
        .join(dim_session, on="session_id", how="left")
        .join(broadcast(dim_date), to_date(col("order_date")) == col("date"), how="left")
    )
    return fact_purchase.select(
        col("purchase_id"), col("order_id"), col('user_sk'), col('product_sk'), col('session_sk'), col('date_sk'),
        col("interaction_id"), col("quantity"), col("unit_price"), col("total_amount"), col('order_date')
    )


def build_fact_review(spark, df: DataFrame) -> DataFrame:
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True).select(col("user_sk"), col("user_id"))
    dim_product = spark.read.table("gold.dim_product").select("product_sk", "product_id")
    dim_date = spark.read.table("gold.dim_date").select("date_sk", "date")
    fact_review = (
        df
        .join(dim_user, on="user_id", how="left")
        .join(dim_product, on="product_id", how="left")
        .join(broadcast(dim_date), to_date(col("review_date")) == col("date"), how="left")
    )
    return fact_review.select(
        col("review_id"), col('user_sk'), col('product_sk'), col('date_sk'),
        col("purchase_id"), col("rating"), col('review_date')
    )


def build_table(
    spark,
    source_table_name: str,
    gold_table_name: str,
    build_fn,
    write_mode: str = "append",
    merge_key: list = None,
    orphan_check_columns: list = None,
    business_key: str = None,
):
    layer = "gold"
    run_id = generate_run_id(layer, gold_table_name)
    started_at = datetime.now()
    rundate = str(date.today())
    input_rows = 0
    output_rows = 0

    print(f"\n========== GOLD: {gold_table_name} ==========")

    try:
        df_incremental, last_watermark = read_incremental_silver(spark, source_table_name, gold_table_name)
        print(f"[Watermark] Previous: {last_watermark}")

        if df_incremental.limit(1).count() == 0:
            print(f"[Gold] {gold_table_name}: No new data")
            insert_audit(
                spark=spark, run_id=run_id, layer=layer, table_name=gold_table_name,
                status="PASS", started_at=started_at, finished_at=datetime.now(),
                input_rows=0, output_rows=0, rejected_rows=0, rundate=rundate,
            )
            return

        input_rows = df_incremental.count()

        df_built = build_fn(spark, df_incremental)
        target_table = f"gold.{gold_table_name}"

        ingest_time = spark.sql("SELECT current_timestamp()").first()[0]
        df_built = (
            df_built
            .withColumn("ingest_at", lit(ingest_time))
            .withColumn("source", lit("silver"))
        )

        # ---- Data Quality: Integrity — fact join dim bị orphan (dim_sk NULL) ----
        # KHÔNG chặn pipeline (severity=WARNING): orphan hiếm khi có nghĩa dữ liệu sai,
        # thường do dim chưa kịp cập nhật (out-of-order). Vẫn quarantine để theo dõi.
        if orphan_check_columns:
            for sk_col in orphan_check_columns:
                validate_business_rule(
                    spark, run_id, df_built,
                    rule_name=f"{sk_col}_not_null",
                    condition=col(sk_col).isNotNull(),
                    key_columns=[business_key] if business_key else None,
                    layer=layer, table_name=gold_table_name, rundate=rundate,
                    max_violation_pct=5.0, severity="WARNING"
                )

        if write_mode == "append":
            df_built.write.format("delta").mode("append").saveAsTable(target_table)
        elif write_mode == "overwrite":
            df_built.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        elif write_mode == "merge_scd1":
            _merge_scd1(spark, df_built, target_table, merge_key)
        else:
            raise ValueError(f"write_mode không hợp lệ: {write_mode}")

        output_rows = df_built.count()
        print(f"[Gold] {target_table}: +{output_rows} rows xử lý ({write_mode})")

        new_watermark = df_incremental.agg(max("ingest_at")).collect()[0][0]
        print(f"[Watermark] {gold_table_name} New: {new_watermark}")

        insert_log(
            spark=spark, layer=layer, table_name=gold_table_name,
            watermark_column="ingest_at", watermark_value=new_watermark, rundate=rundate,
        )

        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=gold_table_name,
            status="PASS", started_at=started_at, finished_at=finished_at,
            input_rows=input_rows, output_rows=output_rows, rejected_rows=0, rundate=rundate,
        )
        print(f"[AUDIT] SUCCESS run_id={run_id}")

    except Exception as e:
        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=gold_table_name,
            status="FAIL", started_at=started_at, finished_at=finished_at,
            input_rows=input_rows, output_rows=output_rows, rejected_rows=None,
            error_message=str(e)[:2000], rundate=rundate,
        )
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise


def write_gold_table(spark, df, table_name):
    layer = "gold"
    run_id = generate_run_id(layer, table_name)
    started_at = datetime.now()
    rundate = str(date.today())

    print(f"\n========== GOLD: {table_name} ==========")
    try:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        row_count = df.count()
        print(f"[Gold] {table_name}: {row_count} rows")

        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name,
            status="PASS", started_at=started_at, finished_at=datetime.now(),
            input_rows=row_count, output_rows=row_count, rejected_rows=0, rundate=rundate,
        )
    except Exception as e:
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name,
            status="FAIL", started_at=started_at, finished_at=datetime.now(),
            error_message=str(e)[:2000], rundate=rundate,
        )
        raise


GOLD_CONFIG = {
    "dim_product": {
        "source_table_name": "products",
        "build_fn": build_dim_product,
        "write_mode": "merge_scd1",
        "merge_key": ["product_id"],
        "orphan_check_columns": None,
        "business_key": "product_id",
    },
    "dim_session": {
        "source_table_name": "sessions",
        "build_fn": build_dim_session,
        "write_mode": "append",
        "merge_key": None,
        "orphan_check_columns": None,
        "business_key": "session_id",
    },
    "fact_interaction": {
        "source_table_name": "interactions",
        "build_fn": build_fact_interaction,
        "write_mode": "append",
        "merge_key": None,
        "orphan_check_columns": ["user_sk", "product_sk", "session_sk", "date_sk"],
        "business_key": "interaction_id",
    },
    "fact_purchase": {
        "source_table_name": "purchases",
        "build_fn": build_fact_purchase,
        "write_mode": "append",
        "merge_key": None,
        "orphan_check_columns": ["user_sk", "product_sk", "session_sk", "date_sk"],
        "business_key": "purchase_id",
    },
    "fact_review": {
        "source_table_name": "reviews",
        "build_fn": build_fact_review,
        "write_mode": "append",
        "merge_key": None,
        "orphan_check_columns": ["user_sk", "product_sk", "date_sk"],
        "business_key": "review_id",
    },
}

# dim_user và dim_date xử lý đặc biệt, không nằm trong dict trên
ALL_GOLD_TABLES = ["dim_user", "dim_date"] + list(GOLD_CONFIG.keys())


def parse_args():
    parser = argparse.ArgumentParser(description="Build một bảng Gold")
    parser.add_argument("--table", required=True, choices=ALL_GOLD_TABLES)
    return parser.parse_args()


def main():
    args = parse_args()
    table_name = args.table

    spark = create_spark_session()
    print(f"--- BẮT ĐẦU BUILD GOLD: {table_name} ---")

    if table_name == "dim_user":
        build_dim_user_scd2(spark)

    elif table_name == "dim_date":
        dim_date = build_dim_date(spark)
        write_gold_table(spark, dim_date, "gold.dim_date")

    else:
        config = GOLD_CONFIG[table_name]
        build_table(
            spark,
            source_table_name=config["source_table_name"],
            gold_table_name=table_name,
            build_fn=config["build_fn"],
            write_mode=config["write_mode"],
            merge_key=config["merge_key"],
            orphan_check_columns=config["orphan_check_columns"],
            business_key=config["business_key"],
        )

    spark.stop()


if __name__ == "__main__":
    main()