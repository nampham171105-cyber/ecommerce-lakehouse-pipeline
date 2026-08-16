from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("IngestBronze") \
        .getOrCreate()

def ingest_table(spark, table_name):

    bronze_table = f"bronze.bronze_{table_name}"

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce") \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "admin123") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # df.write.format("delta").mode("overwrite").option("path", target_path).saveAsTable(table)
    df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    print(f"[Bronze] Ingested table {table_name}: {df.count()} rows -> {bronze_table}")

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