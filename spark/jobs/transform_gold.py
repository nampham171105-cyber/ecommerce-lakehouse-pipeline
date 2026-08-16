from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

def create_spark_session():
    return SparkSession.builder \
        .appName("TransformGold") \
        .enableHiveSupport() \
        .getOrCreate()

def build_dim_user(spark) -> DataFrame:
    df = spark.read.table("silver.silver_users")
    return df.select(
        col('user_id'),
        col('age'),
        col('gender'),
        col('country'),
        col('signup_date'),
        col('income_level'),
        col('preferred_category'),
        col('loyalty_tier')
    )

def build_dim_product(spark) -> DataFrame:
    df = spark.read.table("silver.silver_products")
    return df.select(
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

    return spark.sql(f"SELECT explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as date") \
                    .select(
                        # sha2(col("date").cast("string"), 256).alias("date_sk"),
                        col("date"),
                        year(col("date")).alias("year"),
                        quarter(col("date")).alias("quarter"),
                        month(col("date")).alias("month"),
                        dayofmonth(col("date")).alias("day"),
                        dayofweek(col("date")).alias("day_of_week") # 1: Chủ nhật, 7: Thứ bảy
                    )
def build_dim_session(spark) -> DataFrame:
    df = spark.read.table("silver.silver_sessions")
    return df.select(
        col('session_id'),
        col('device_type'),
        col('referrer_source'),
        col('is_converted')
    )

def build_fact_interaction(spark) -> DataFrame:
    df = spark.read.table("silver.silver_interactions")
    dim_user = spark.read.table("gold.dim_user")
    dim_product = spark.read.table("gold.dim_product")
    dim_session = spark.read.table("gold.dim_session")
    dim_date = spark.read.table("gold.dim_date")

    fact_interaction = (
        df
        .join(dim_user, on="user_id",how="left") 
        .join(dim_product, on="product_id",how="left") 
        .join(dim_session, on="session_id",how="left")
        .join(dim_date, to_date("interaction_timestamp") == col("date"),how="left")
    )
    return fact_interaction.select(
        col('interaction_id'),
        col('user_id'),
        col('product_id'),
        col('session_id'),
        col('date'),
        col('interaction_type'),
        col('dwell_time_ms')
    )
def build_fact_purchase(spark) -> DataFrame:
    df = spark.read.table("silver.silver_purchases")
    dim_user = spark.read.table("gold.dim_user")
    dim_product = spark.read.table("gold.dim_product")
    dim_session = spark.read.table("gold.dim_session")
    dim_date = spark.read.table("gold.dim_date")
    fact_interaction = spark.read.table("gold.fact_interaction")

    fact_purchase = (
        df
        .join(dim_user, df["user_id"] == dim_user["user_id"], how="left")
        .join(dim_product, df["product_id"] == dim_product["product_id"], how="left")
        .join(dim_session, df["session_id"] == dim_session["session_id"], how="left")
        .join(fact_interaction, df["interaction_id"] == fact_interaction["interaction_id"], how="left")
        .join(dim_date, to_date(df["order_date"]) == dim_date["date"], how="left")
    )
    return fact_purchase.select(
        df['purchase_id'],
        df['order_id'],
        df['user_id'],
        df['product_id'],
        df['session_id'],
        df['interaction_id'],
        dim_date['date'].alias('date'), # Lấy cột ngày từ bảng dim_date
        df['quantity'],
        df['unit_price'],
        df['total_amount']
    )
def build_fact_review(spark) -> DataFrame:
    df = spark.read.table("silver.silver_reviews")
    dim_user = spark.read.table("gold.dim_user")
    dim_product = spark.read.table("gold.dim_product")
    dim_date = spark.read.table("gold.dim_date")
    fact_purchase = spark.read.table("gold.fact_purchase")
    fact_review = (
        df
        .join(dim_user, df["user_id"] == dim_user["user_id"], how="left")
        .join(dim_product, df["product_id"] == dim_product["product_id"], how="left")
        .join(fact_purchase, df["purchase_id"] == fact_purchase["purchase_id"], how="left")
        .join(dim_date, to_date(df["review_date"]) == dim_date["date"], how="left")
    )
    return fact_review.select(
        df["review_id"],
        df["user_id"],
        df["product_id"],
        df["purchase_id"],
        dim_date["date"].alias("date"), # Lấy cột ngày từ bảng dim_date
        df["rating"]
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