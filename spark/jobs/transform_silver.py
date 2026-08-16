from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap
from pyspark.sql import DataFrame

def create_spark_session():
    return SparkSession.builder \
        .appName("TransformSilver") \
        .enableHiveSupport() \
        .getOrCreate()

def clean_users(df: DataFrame) -> DataFrame:
    return df.na.drop()

def clean_products(df: DataFrame) -> DataFrame:
    return df.na.drop()

def clean_reviews(df: DataFrame) -> DataFrame:
    return df.na.drop()

def clean_purchases(df: DataFrame) -> DataFrame:
    return df.na.drop()

def clean_sessions(df: DataFrame) -> DataFrame:
    return df.na.drop()

def clean_interactions(df: DataFrame) -> DataFrame:
    return df.na.drop()

def main():
    spark = create_spark_session()

    tables = [
            "users", 
            "products", 
            "sessions", 
            "interactions", 
            "purchases", 
            "reviews"
        ]

    print("--- BẮT ĐẦU TRANSFORM ---")
    for table in tables:
        bronze_table = f"bronze.bronze_{table}"
        df = spark.read.table(bronze_table)
        #dedup

        silver_table = f"silver.silver_{table}"
        df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        print(f"[Silver] Cleaned table {table}: {df.count()} rows -> {silver_table}")

    spark.stop()

if __name__ == "__main__":
    main()