from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, initcap, upper, coalesce as spark_coalesce

from utils.validation import validate_row_count, dedupe_exact, dedupe_by_key, validate_business_rule

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


def clean_users(df: DataFrame) -> DataFrame:
    df_clean = df.na.drop(subset=["user_id"])
    return dedupe_by_key(df_clean, ["user_id"], order_column="signup_date", keep="latest")


def clean_products(df: DataFrame) -> DataFrame:
    # rating_avg/review_count được phép NULL — sản phẩm chưa có review, không ép na.drop toàn cột
    df_clean = df.na.drop(subset=["product_id", "product_name", "price"])
    return dedupe_by_key(df_clean, ["product_id"], order_column="date_added", keep="latest")


def clean_sessions(df: DataFrame) -> DataFrame:
    df_clean = df.na.drop(subset=["session_id", "user_id"])
    return dedupe_by_key(df_clean, ["session_id"], order_column="start_time", keep="latest")


def clean_interactions(df: DataFrame) -> DataFrame:
    df_clean = df.na.drop(subset=["interaction_id", "user_id", "product_id", "session_id"])
    return dedupe_by_key(df_clean, ["interaction_id"], order_column="dwell_time_ms", keep="latest")


def clean_purchases(df: DataFrame) -> DataFrame:
    df_clean = df.na.drop(subset=["purchase_id", "order_id", "user_id", "product_id", "total_amount"])
    return dedupe_by_key(df_clean, ["purchase_id"], order_column="order_date", keep="latest")


def clean_reviews(df: DataFrame, purchases_cleaned: DataFrame) -> DataFrame:
    df_clean = df.na.drop(subset=["review_id", "user_id", "product_id", "rating"])
    df_clean = dedupe_by_key(df_clean, ["review_id"], order_column="review_date", keep="latest")

    # purchase_id gốc bị trống nhiều -> khôi phục qua (user_id, product_id)
    # Dedupe purchases trước theo cặp (user_id, product_id) để tránh fan-out nếu
    # 1 user mua cùng sản phẩm nhiều lần (chỉ lấy purchase gần nhất để gán review)
    purchases_latest_per_pair = dedupe_by_key(
        purchases_cleaned.select("purchase_id", "user_id", "product_id", "order_date"),
        key_columns=["user_id", "product_id"],
        order_column="order_date",
        keep="latest"
    ).select(col("purchase_id").alias("recovered_purchase_id"), "user_id", "product_id")

    df_enriched = df_clean.join(purchases_latest_per_pair, on=["user_id", "product_id"], how="left")
    df_enriched = df_enriched.withColumn(
        "purchase_id", spark_coalesce(col("purchase_id"), col("recovered_purchase_id"))
    ).drop("recovered_purchase_id")

    return df_enriched


def process_table(spark, table_name, clean_fn, extra_arg=None, max_loss_pct=15.0):
    bronze_table = f"bronze.bronze_{table_name}"
    df_raw = spark.read.table(bronze_table)

    df_clean = clean_fn(df_raw, extra_arg) if extra_arg is not None else clean_fn(df_raw)

    validate_row_count(df_raw, df_clean, f"silver_{table_name}", max_loss_pct=max_loss_pct)

    for rule_name, condition in SILVER_RULES:
        validate_business_rule(
            df=df_clean, 
            rule_name=rule_name, 
            condition=condition, 
            step_name=f"silver_{table_name}", 
            max_violation_pct=1.0
        )

    silver_table = f"silver.silver_{table_name}"
    df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
    print(f"[Silver] {table_name}: {df_clean.count()} rows -> {silver_table}")
    return df_clean


def main():
    spark = create_spark_session()
    print("--- BẮT ĐẦU TRANSFORM SILVER ---")

    process_table(spark, "users", clean_users, max_loss_pct=5.0)
    process_table(spark, "products", clean_products, max_loss_pct=5.0)
    process_table(spark, "sessions", clean_sessions, max_loss_pct=5.0)
    # interactions: ngưỡng cao hơn vì có duplicate thật cần loại bỏ (đã ghi nhận ở validate_bronze)
    process_table(spark, "interactions", clean_interactions, max_loss_pct=20.0)
    purchases = process_table(spark, "purchases", clean_purchases, max_loss_pct=5.0)
    process_table(spark, "reviews", clean_reviews, extra_arg=purchases, max_loss_pct=5.0)

    spark.stop()


if __name__ == "__main__":
    main()