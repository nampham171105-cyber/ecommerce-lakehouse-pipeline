from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap

def create_spark_session():
    return SparkSession.builder \
        .appName("TransformSilver") \
        .enableHiveSupport() \
        .getOrCreate()

def load_bronze(spark):
    orders = spark.read.format("delta").load("s3a://spark-bucket/lakehouse/bronze/orders_raw")
    customers = spark.read.format("delta").load("s3a://spark-bucket/lakehouse/bronze/customers_raw")
    return orders, customers

def clean_customers(customers):
    return customers \
        .withColumn("customer_name", trim(initcap(col("customer_name")))) \
        .withColumn("city", trim(initcap(col("city")))) \
        .dropDuplicates(["customer_id"])

def build_orders_enriched(orders, customers_cleaned):
    orders_valid = orders.filter(col("amount") > 0)

    enriched = orders_valid.join(
        customers_cleaned,
        on="customer_id",
        how="inner"
    )

    return enriched.select(
        "order_id", "customer_id", "customer_name", "city",
        "amount", "order_date"
    )

def main():
    spark = create_spark_session()

    orders, customers = load_bronze(spark)
    customers_cleaned = clean_customers(customers)
    orders_enriched = build_orders_enriched(orders, customers_cleaned)

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    orders_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "s3a://spark-bucket/lakehouse/silver/orders_enriched") \
        .saveAsTable("silver.orders_enriched")

    print(f"[Silver] orders_enriched: {orders_enriched.count()} rows")

    spark.stop()

if __name__ == "__main__":
    main()