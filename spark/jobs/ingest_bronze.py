from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("IngestBronze") \
        .getOrCreate()

def ingest_orders_csv(spark, source_path, target_path):
    df = spark.read \
        .option("header", "true") \
        .schema("order_id INT, customer_id INT, amount DOUBLE, order_date DATE") \
        .csv(source_path)

    df.write.format("delta").mode("overwrite").save(target_path)
    print(f"[Bronze] Ingested orders: {df.count()} rows -> {target_path}")

def ingest_customers_jdbc(spark, target_path):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/companydb") \
        .option("dbtable", "customers") \
        .option("user", "admin") \
        .option("password", "admin123") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df.write.format("delta").mode("overwrite").save(target_path)
    print(f"[Bronze] Ingested customers: {df.count()} rows -> {target_path}")

def main():
    spark = create_spark_session()

    ingest_orders_csv(
        spark,
        source_path="/opt/spark-data/orders.csv",
        target_path="s3a://spark-bucket/lakehouse/bronze/orders_raw"
    )

    ingest_customers_jdbc(
        spark,
        target_path="s3a://spark-bucket/lakehouse/bronze/customers_raw"
    )

    spark.stop()

if __name__ == "__main__":
    main()