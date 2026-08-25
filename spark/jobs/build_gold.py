from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from utils.job_control import get_watermark, insert_log
from delta.tables import DeltaTable
from datetime import date

def create_spark_session():
    return SparkSession.builder \
        .appName("TransformGold") \
        .enableHiveSupport() \
        .getOrCreate()

def read_incremental_silver(spark, table_name, gold_table_name):
    silver_table = f"silver.silver_{table_name}"

    last_watermark = get_watermark(
        spark,
        layer="gold",
        table_name=gold_table_name
    )

    df = spark.read.table(silver_table)

    if last_watermark is None:
        print(f"[GOLD] {table_name}: FIRST RUN")
        return df, None

    print(f"[GOLD] {table_name}: Previous watermark = {last_watermark}")

    df = df.filter(
        col("ingest_at") > lit(last_watermark)
    )

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
    -- Nhánh 1 -- "ép insert": mergeKey = NULL, chỉ user ĐÃ TỒN TẠI và
    -- row_hash đổi -> luôn rơi vào WHEN NOT MATCHED -> tạo version mới.
    SELECT NULL AS mergeKey, src.*
    FROM source_prepared AS src
    JOIN gold.dim_user AS tgt
        ON src.user_id = tgt.user_id
        AND tgt.is_current = true
    WHERE src.row_hash <> tgt.row_hash
 
    UNION ALL
 
    -- Nhánh 2 -- "so khớp chuẩn": mergeKey = user_id, toàn bộ user nguồn.
    SELECT user_id AS mergeKey, src.*
    FROM source_prepared AS src
) AS staged_updates
-- "AND target.is_current = true" phải nằm trong ON (không chỉ WHEN MATCHED)
-- để chỉ so khớp version hiện tại, tránh khớp tràn qua các version lịch sử.
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

    table_name = "users"
    gold_table = "dim_user"
    
    df_raw, last_watermark = read_incremental_silver(
        spark,
        table_name,
        gold_table
    )
    print(f"[Watermark] Previous: {last_watermark}")
 
    if df_raw.limit(1).count() == 0:
        print(f"[Gold] {table_name}: No new data")
        return
 
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
        spark=spark,
        layer="gold",
        table_name=gold_table,
        watermark_column="ingest_at",
        watermark_value=str(new_watermark),
        rundate=str(date.today())
    )

def build_dim_product(spark, df: DataFrame) -> DataFrame:
    df = add_surrogate_key(df, ["product_id"], "product_sk")
    return df.select(
        col('product_sk'),
        col('product_id'),
        col('product_name'),
        col('product_description'),
        col('category'),
        col('subcategory'),
        col('brand'),
        col('price'),
        col('rating_avg'),
        col('review_count'),
        col('stock_quantity'),
        col('date_added')
    )

def build_dim_date(spark) -> DataFrame:
    begin_date = "2023-01-01"
    end_date = "2027-01-01"
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as date"
    )
    df = add_surrogate_key(df, ["date"], "date_sk")
    return df.select(
        col("date_sk"),
        col("date"),
        year(col("date")).alias("year"),
        quarter(col("date")).alias("quarter"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        dayofweek(col("date")).alias("day_of_week") # 1: Chủ nhật, 7: Thứ bảy
        )
def build_dim_session(spark, df: DataFrame) -> DataFrame:
    df = add_surrogate_key(df, ["session_id"], "session_sk")
    return df.select(
        col("session_sk"),
        col('session_id'),
        col('device_type'),
        col('referrer_source'),
        col('is_converted'),
        col('start_time')
    )

def build_fact_interaction(spark, df: DataFrame) -> DataFrame:
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True).select(col("user_sk"), col("user_id")) 
    # join với version current
    dim_product = spark.read.table("gold.dim_product").select(col("product_sk"), col("product_id"))
    dim_session = spark.read.table("gold.dim_session").select(col("session_sk"), col("session_id"))
    dim_date = spark.read.table("gold.dim_date").select(col("date_sk"), col("date"))

    fact_interaction = (
        df
        .join(dim_user, on="user_id", how="inner")
        .join(dim_product, on="product_id", how="inner")
        .join(dim_session, on="session_id", how="inner")
        .join(dim_date, to_date(col("interaction_timestamp")) == col("date"), how="inner")
    )
    return fact_interaction.select(
        col('interaction_id'),
        col('user_sk'),
        col('product_sk'),
        col('session_sk'),
        col('date_sk'),
        col('interaction_type'),
        col('dwell_time_ms'),
        col('interaction_timestamp')
    )
def build_fact_purchase(spark, df: DataFrame) -> DataFrame:
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True).select(col("user_sk"), col("user_id")) 
    # join với version current
    dim_product = spark.read.table("gold.dim_product").select(col("product_sk"), col("product_id"))
    dim_session = spark.read.table("gold.dim_session").select(col("session_sk"), col("session_id"))
    dim_date = spark.read.table("gold.dim_date").select(col("date_sk"), col("date"))

    fact_purchase = (
        df
        .join(dim_user, on="user_id", how="inner")
        .join(dim_product, on="product_id", how="inner")
        .join(dim_session, on="session_id", how="left")
        .join(dim_date, to_date(col("order_date")) == col("date"), how="inner")
    )
    return fact_purchase.select(
        col("purchase_id"), 
        col("order_id"), 
        col('user_sk'),
        col('product_sk'),
        col('session_sk'),
        col('date_sk'),
        col("interaction_id"), 
        col("quantity"), 
        col("unit_price"), 
        col("total_amount"),
        col('order_date')
    )
def build_fact_review(spark, df: DataFrame) -> DataFrame:
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True).select(col("user_sk"), col("user_id")) 
    # join với version current
    dim_product = spark.read.table("gold.dim_product").select("product_sk", "product_id")
    dim_date = spark.read.table("gold.dim_date").select("date_sk", "date")
    fact_review = (
        df
        .join(dim_user, on="user_id", how="inner")
        .join(dim_product, on="product_id", how="inner")
        .join(dim_date, to_date(col("review_date")) == col("date"), how="inner")
    )
    return fact_review.select(
        col("review_id"), 
        col('user_sk'),
        col('product_sk'),
        col('date_sk'),
        col("purchase_id"), 
        col("rating"),
        col('review_date')
    )

def build_table(
    spark,
    source_table_name: str,
    gold_table_name: str,
    build_fn,
    write_mode: str = "append",
    merge_key: list = None,
):

    print(f"\n========== GOLD: {gold_table_name} ==========")
    
    df_incremental, last_watermark = read_incremental_silver(
        spark, 
        source_table_name, 
        gold_table_name
    )
    print(f"[Watermark] Previous: {last_watermark}")
 
    if df_incremental.limit(1).count() == 0:
        print(f"[Gold] {gold_table_name}: No new data")
        return
 
    df_built = build_fn(spark, df_incremental)
    target_table = f"gold.{gold_table_name}"

    ingest_time = spark.sql("SELECT current_timestamp()").first()[0]
    
    df_built = (
            df_built
            .withColumn("ingest_at", lit(ingest_time)) # viết ingest time để spark trả về ngay mà không cần phải đợi action
            .withColumn("source", lit("silver"))
        )
 
    if write_mode == "append":

        df_built.write.format("delta") \
            .mode("append") \
            .saveAsTable(target_table)

    elif write_mode == "overwrite":

        df_built.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
    elif write_mode == "merge_scd1":

        _merge_scd1(spark, df_built, target_table, merge_key)

    else:
        raise ValueError(f"write_mode không hợp lệ: {write_mode}")
 
    print(f"[Gold] {target_table}: +{df_built.count()} rows xử lý ({write_mode})")
 
    new_watermark = df_incremental.agg(max("ingest_at")).collect()[0][0]
    print(f"[Watermark] {gold_table_name} New: {new_watermark}")
 
    insert_log(
        spark=spark,
        layer="gold",
        table_name=gold_table_name,
        watermark_column="ingest_at",
        watermark_value=str(new_watermark),
        rundate=str(date.today())
    )

def write_gold_table(df, table_name):
    print(f"\n========== GOLD: {table_name} ==========")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    print(f"[Gold] {table_name}: {df.count()} rows")

def main():
    spark = create_spark_session()
    
    print("--- BẮT ĐẦU BUILD GOLD ---")
    
    # ==== USER =====
    build_dim_user_scd2(spark)

    # ==== PRODUCT =====
    build_table(
        spark,
        source_table_name="products",
        gold_table_name="dim_product",
        build_fn=build_dim_product,
        write_mode="merge_scd1",
        merge_key=["product_id"],
    )

    # ==== DATE =====
    dim_date = build_dim_date(spark)
    write_gold_table(dim_date, "gold.dim_date")

    # ==== SESSION =====
    build_table(
        spark,
        source_table_name="sessions",
        gold_table_name="dim_session",
        build_fn=build_dim_session,
        write_mode="append",
    )     

    # ==== INTERACTION =====
    build_table(
        spark,
        source_table_name="interactions",
        gold_table_name="fact_interaction",
        build_fn=build_fact_interaction,
        write_mode="append",
    )

    # ==== PURCHASE =====
    build_table(
        spark,
        source_table_name="purchases",
        gold_table_name="fact_purchase",
        build_fn=build_fact_purchase,
        write_mode="append",
    )

    # ==== REVIEW =====
    build_table(
        spark,
        source_table_name="reviews",
        gold_table_name="fact_review",
        build_fn=build_fact_review,
        write_mode="append",
    )


    spark.stop()

if __name__ == "__main__":
    main()