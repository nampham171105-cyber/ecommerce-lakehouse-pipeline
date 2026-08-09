from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, date_format

def create_spark_session():
    return SparkSession.builder \
        .appName("TransformGold") \
        .enableHiveSupport() \
        .getOrCreate()

def load_silver(spark):
    return spark.read.format("delta").load("s3a://spark-bucket/lakehouse/silver/orders_enriched")

def build_dim_customers(orders_enriched):
    return orders_enriched.select("customer_id", "customer_name", "city").dropDuplicates(["customer_id"])

def build_dim_date(orders_enriched):
    return orders_enriched.select(
        col("order_date").alias("date_key"),
        date_format(col("order_date"), "yyyy-MM-dd").alias("full_date"),
        year(col("order_date")).alias("year"),
        month(col("order_date")).alias("month"),
        dayofmonth(col("order_date")).alias("day")
    ).dropDuplicates(["date_key"])

def build_fact_orders(orders_enriched):
    return orders_enriched.select(
        col("order_id"),
        col("customer_id"),
        col("order_date").alias("date_key"),
        col("amount")
    )

def write_gold_table(df, table_name, path):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", path) \
        .saveAsTable(table_name)
    print(f"[Gold] {table_name}: {df.count()} rows")

def main():
    spark = create_spark_session()
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    orders_enriched = load_silver(spark)

    dim_customers = build_dim_customers(orders_enriched)
    dim_date = build_dim_date(orders_enriched)
    fact_orders = build_fact_orders(orders_enriched)

    write_gold_table(dim_customers, "gold.dim_customers", "s3a://spark-bucket/lakehouse/gold/dim_customers")
    write_gold_table(dim_date, "gold.dim_date", "s3a://spark-bucket/lakehouse/gold/dim_date")
    write_gold_table(fact_orders, "gold.fact_orders", "s3a://spark-bucket/lakehouse/gold/fact_orders")

    spark.stop()

if __name__ == "__main__":
    main()