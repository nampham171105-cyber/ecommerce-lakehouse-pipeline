from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

def create_spark_session():
    return SparkSession.builder \
        .appName("TransformGold") \
        .enableHiveSupport() \
        .getOrCreate()

def add_surrogate_key(df: DataFrame, natural_key_cols: list, sk_name: str) -> DataFrame:
    return df.withColumn(sk_name, sha2(concat_ws("||", *[col(c) for c in natural_key_cols]), 256))

def build_dim_user(spark) -> DataFrame:
    df = spark.read.table("silver.silver_users")
    df = add_surrogate_key(df, ["user_id"], "user_sk")
    return df.select(
        col('user_sk'),
        col('user_id'),
        col('age'),
        col('gender'),
        col('country'),
        col('city'),
        col('signup_date'),
        col('income_level'),
        col('preferred_category'),
        col('loyalty_tier')
    )

def build_dim_product(spark) -> DataFrame:
    df = spark.read.table("silver.silver_products")
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
def build_dim_session(spark) -> DataFrame:
    df = spark.read.table("silver.silver_sessions")
    df = add_surrogate_key(df, ["session_id"], "session_sk")
    return df.select(
        col("session_sk"),
        col('session_id'),
        col('device_type'),
        col('referrer_source'),
        col('is_converted')
    )

def build_fact_interaction(spark) -> DataFrame:
    df = spark.read.table("silver.silver_interactions")
    dim_user = spark.read.table("gold.dim_user").select(col("user_sk"), col("user_id"))
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
        col('dwell_time_ms')
    )
def build_fact_purchase(spark) -> DataFrame:
    df = spark.read.table("silver.silver_purchases")
    dim_user = spark.read.table("gold.dim_user").select(col("user_sk"), col("user_id"))
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
        col("total_amount")
    )
def build_fact_review(spark) -> DataFrame:
    df = spark.read.table("silver.silver_reviews")
    dim_user = spark.read.table("gold.dim_user").select("user_sk", "user_id")
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
        col("rating")
    )

def write_gold_table(df, table_name):
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
    dim_user = build_dim_user(spark)
    write_gold_table(dim_user, "gold.dim_user")

    # ==== PRODUCT =====
    dim_product = build_dim_product(spark)
    write_gold_table(dim_product, "gold.dim_product")

    # ==== DATE =====
    dim_date = build_dim_date(spark)
    write_gold_table(dim_date, "gold.dim_date")

    # ==== SESSION =====
    dim_session = build_dim_session(spark)
    write_gold_table(dim_session, "gold.dim_session")     

    # ==== INTERACTION =====
    fact_interaction = build_fact_interaction(spark)
    write_gold_table(fact_interaction, "gold.fact_interaction")

    # ==== PURCHASE =====
    fact_purchase = build_fact_purchase(spark)
    write_gold_table(fact_purchase, "gold.fact_purchase")

    # ==== REVIEW =====
    fact_review = build_fact_review(spark)
    write_gold_table(fact_review, "gold.fact_review")


    spark.stop()

if __name__ == "__main__":
    main()