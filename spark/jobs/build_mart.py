from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import *
from job_control import generate_run_id, insert_quality_metric, insert_failed_records, insert_audit
from datetime import datetime, date
import argparse
import builtins


def create_spark_session():
    return SparkSession.builder.appName("BuildCustomer360").enableHiveSupport().getOrCreate()


# ---------------- BUILD FUNCTIONS (giữ nguyên logic gốc) ----------------

def build_customer_360(spark):
    print("\n========== BUILD CUSTOMER 360 ==========")

    print("[1] Reading Gold tables...")
    dim_user = spark.read.table("gold.dim_user").filter(col("is_current") == True)
    fact_purchase = spark.read.table("gold.fact_purchase")
    fact_interaction = spark.read.table("gold.fact_interaction")
    fact_review = spark.read.table("gold.fact_review")

    print("[2] Building purchase metrics...")
    purchase_metrics = fact_purchase.groupBy("user_sk").agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("total_items"),
        sum("total_amount").alias("total_spend"),
        avg("total_amount").alias("avg_order_value"),
        min("order_date").alias("first_purchase_date"),
        max("order_date").alias("last_purchase_date")
    )

    print("[3] Building interaction metrics...")
    interaction_metrics = fact_interaction.groupBy("user_sk").agg(
        count("*").alias("total_interactions"),
        sum("dwell_time_ms").alias("total_dwell_time"),
        avg("dwell_time_ms").alias("avg_dwell_time")
    )

    print("[4] Building review metrics...")
    review_metrics = fact_review.groupBy("user_sk").agg(
        count("*").alias("total_reviews"),
        avg("rating").alias("avg_rating")
    )

    print("[5] Joining metrics...")
    customer_360 = (
        dim_user.select(
            "user_sk", "user_id", "age", "gender", "country", "city",
            "income_level", "preferred_category", "loyalty_tier"
        )
        .join(purchase_metrics, "user_sk", "left")
        .join(interaction_metrics, "user_sk", "left")
        .join(review_metrics, "user_sk", "left")
    )

    print("[6] Handling NULL metrics...")
    customer_360 = (
        customer_360
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("total_items", coalesce(col("total_items"), lit(0)))
        .withColumn("total_spend", coalesce(col("total_spend"), lit(0)))
        .withColumn("total_interactions", coalesce(col("total_interactions"), lit(0)))
        .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
        .withColumn("total_dwell_time", coalesce(col("total_dwell_time"), lit(0)))
    )

    customer_360 = (
        customer_360
        .withColumn("source", lit("gold"))
        .withColumn("ingest_at", current_timestamp())
    )

    return customer_360


def build_product_performance(spark):
    print("\n========== BUILD PRODUCT PERFORMANCE ==========")

    print("[1] Reading Gold tables...")
    dim_product = spark.read.table("gold.dim_product")
    fact_purchase = spark.read.table("gold.fact_purchase")
    fact_interaction = spark.read.table("gold.fact_interaction")
    fact_review = spark.read.table("gold.fact_review")

    print("[2] Building purchase metrics...")
    purchase_metrics = fact_purchase.groupBy("product_sk").agg(
        sum("quantity").alias("units_sold"),
        sum("total_amount").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_sk").alias("unique_customers")
    )

    print("[3] Building interaction metrics...")
    interaction_metrics = fact_interaction.groupBy("product_sk").agg(
        count("*").alias("total_interactions")
    )

    print("[4] Building review metrics...")
    review_metrics = fact_review.groupBy("product_sk").agg(
        count("*").alias("total_reviews"),
        avg("rating").alias("avg_rating")
    )

    print("[5] Joining metrics...")
    product_performance = (
        dim_product.select(
            "product_sk", "product_id", "product_name",
            "category", "subcategory", "brand", "price"
        )
        .join(purchase_metrics, "product_sk", "left")
        .join(interaction_metrics, "product_sk", "left")
        .join(review_metrics, "product_sk", "left")
    )

    print("[6] Handling NULL metrics...")
    product_performance = (
        product_performance
        .withColumn("units_sold", coalesce(col("units_sold"), lit(0)))
        .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0)))
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("unique_customers", coalesce(col("unique_customers"), lit(0)))
        .withColumn("total_interactions", coalesce(col("total_interactions"), lit(0)))
        .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
    )

    print("[7] Building derived metrics...")
    product_performance = (
        product_performance
        .withColumn(
            "revenue_per_order",
            when(col("total_orders") > 0, col("total_revenue") / col("total_orders")).otherwise(lit(0))
        )
        .withColumn(
            "revenue_per_unit",
            when(col("units_sold") > 0, col("total_revenue") / col("units_sold")).otherwise(lit(0))
        )
        .withColumn("source", lit("gold"))
        .withColumn("ingest_at", current_timestamp())
    )

    return product_performance


def build_sales_daily(spark):
    print("\n========== BUILD SALES DAILY ==========")

    print("[1] Reading Gold tables...")
    fact_purchase = spark.read.table("gold.fact_purchase")
    dim_date = spark.read.table("gold.dim_date").select(
        "date_sk", "date", "year", "quarter", "month", "day", "day_of_week"
    )

    print("[2] Building purchase and joining metrics...")
    sales_daily = (
        fact_purchase
        .join(dim_date, "date_sk", "inner")
        .groupBy("date_sk", "date", "year", "quarter", "month", "day", "day_of_week")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("quantity").alias("total_items"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_sk").alias("unique_customers")
        )
        .withColumn("source", lit("gold"))
        .withColumn("ingest_at", current_timestamp())
    )

    return sales_daily


# ---------------- VALIDATE FUNCTIONS (ghi quality_metrics + quarantine) ----------------

def _log_check(spark, run_id, table_name, check_name, metric_value, passed, rundate):
    insert_quality_metric(
        spark, run_id=run_id, layer="mart", table_name=table_name,
        check_name=check_name, metric_value=metric_value, passed=passed, rundate=rundate
    )


def validate_customer_360(spark, run_id, df, rundate):
    print("\n========== DATA QUALITY: CUSTOMER 360 ==========")
    table_name = "customer_360"

    # 1. Grain: 1 row = 1 customer
    total_rows = df.count()
    distinct_users = df.select("user_sk").distinct().count()
    grain_diff = total_rows - distinct_users
    print(f"[DQ] Total rows: {total_rows}")
    print(f"[DQ] Distinct users: {distinct_users}")
    _log_check(spark, run_id, table_name, "grain_duplicate_user_sk", grain_diff, grain_diff == 0, rundate)
    if grain_diff != 0:
        raise ValueError("Customer 360 grain FAILED: duplicate user_sk")

    # 2. Coverage: tất cả current customers phải xuất hiện
    current_users = (
        spark.read.table("gold.dim_user")
        .filter(col("is_current") == True)
        .select("user_sk").distinct().count()
    )
    print(f"[DQ] Current users in Gold: {current_users}")
    coverage_diff = builtins.abs(total_rows - current_users)
    _log_check(spark, run_id, table_name, "coverage_vs_dim_user", coverage_diff, total_rows == current_users, rundate)
    if total_rows != current_users:
        raise ValueError(f"Customer 360 coverage FAILED: Mart={total_rows}, Gold={current_users}")

    # 3. Reconciliation: total spend
    fact_purchase = spark.read.table("gold.fact_purchase")
    mart_spend = df.selectExpr("COALESCE(SUM(total_spend), 0) AS value").first()["value"]
    gold_spend = fact_purchase.selectExpr("COALESCE(SUM(total_amount), 0) AS value").first()["value"]
    print(f"[DQ] Mart total_spend: {mart_spend}")
    print(f"[DQ] Gold total_amount: {gold_spend}")
    _log_check(spark, run_id, table_name, "spend_reconciliation_diff", mart_spend - gold_spend, mart_spend == gold_spend, rundate)
    if mart_spend != gold_spend:
        raise ValueError(f"Customer 360 spend reconciliation FAILED: Mart={mart_spend}, Gold={gold_spend}")

    # 4. Reconciliation: total items
    mart_items = df.selectExpr("COALESCE(SUM(total_items), 0) AS value").first()["value"]
    gold_items = fact_purchase.selectExpr("COALESCE(SUM(quantity), 0) AS value").first()["value"]
    print(f"[DQ] Mart total_items: {mart_items}")
    print(f"[DQ] Gold quantity: {gold_items}")
    _log_check(spark, run_id, table_name, "items_reconciliation_diff", mart_items - gold_items, mart_items == gold_items, rundate)
    if mart_items != gold_items:
        raise ValueError(f"Customer 360 items reconciliation FAILED: Mart={mart_items}, Gold={gold_items}")

    # 5. Metric consistency: total_orders không được lớn hơn total_items — quarantine dòng vi phạm
    invalid_df = df.filter(col("total_orders") > col("total_items"))
    invalid_orders = invalid_df.count()
    _log_check(spark, run_id, table_name, "metric_logic_orders_gt_items", invalid_orders, invalid_orders == 0, rundate)
    if invalid_orders > 0:
        insert_failed_records(
            spark, run_id=run_id, layer="mart", table_name=table_name,
            df_rejected=invalid_df, key_columns=["user_sk"],
            failure_reason="total_orders_gt_total_items", rundate=rundate
        )
        raise ValueError("Customer 360 metric logic FAILED: total_orders > total_items")

    print("[DQ] Customer 360: PASSED")


def validate_product_performance(spark, run_id, df, rundate):
    print("\n========== DATA QUALITY: PRODUCT PERFORMANCE ==========")
    table_name = "product_performance"

    # 1. Grain: 1 row = 1 product
    total_rows = df.count()
    distinct_products = df.select("product_sk").distinct().count()
    grain_diff = total_rows - distinct_products
    print(f"[DQ] Total rows: {total_rows}")
    print(f"[DQ] Distinct products: {distinct_products}")
    _log_check(spark, run_id, table_name, "grain_duplicate_product_sk", grain_diff, grain_diff == 0, rundate)
    if grain_diff != 0:
        raise ValueError("Product Performance grain FAILED: duplicate product_sk")

    # 2. Coverage
    gold_products = spark.read.table("gold.dim_product").select("product_sk").distinct().count()
    print(f"[DQ] Products in Gold: {gold_products}")
    coverage_diff = builtins.abs(total_rows - gold_products)
    _log_check(spark, run_id, table_name, "coverage_vs_dim_product", coverage_diff, total_rows == gold_products, rundate)
    if total_rows != gold_products:
        raise ValueError(f"Product Performance coverage FAILED: Mart={total_rows}, Gold={gold_products}")

    # 3. Revenue reconciliation
    fact_purchase = spark.read.table("gold.fact_purchase")
    mart_revenue = df.selectExpr("COALESCE(SUM(total_revenue), 0) AS value").first()["value"]
    gold_revenue = fact_purchase.selectExpr("COALESCE(SUM(total_amount), 0) AS value").first()["value"]
    print(f"[DQ] Mart revenue: {mart_revenue}")
    print(f"[DQ] Gold revenue: {gold_revenue}")
    _log_check(spark, run_id, table_name, "revenue_reconciliation_diff", mart_revenue - gold_revenue, mart_revenue == gold_revenue, rundate)
    if mart_revenue != gold_revenue:
        raise ValueError(f"Product revenue reconciliation FAILED: Mart={mart_revenue}, Gold={gold_revenue}")

    # 4. Units reconciliation
    mart_units = df.selectExpr("COALESCE(SUM(units_sold), 0) AS value").first()["value"]
    gold_units = fact_purchase.selectExpr("COALESCE(SUM(quantity), 0) AS value").first()["value"]
    print(f"[DQ] Mart units: {mart_units}")
    print(f"[DQ] Gold units: {gold_units}")
    _log_check(spark, run_id, table_name, "units_reconciliation_diff", mart_units - gold_units, mart_units == gold_units, rundate)
    if mart_units != gold_units:
        raise ValueError(f"Product units reconciliation FAILED: Mart={mart_units}, Gold={gold_units}")

    # 5. Metric consistency — quarantine dòng vi phạm
    invalid_df = df.filter(col("unique_customers") > col("total_orders"))
    invalid_customers = invalid_df.count()
    _log_check(spark, run_id, table_name, "metric_logic_customers_gt_orders", invalid_customers, invalid_customers == 0, rundate)
    if invalid_customers > 0:
        insert_failed_records(
            spark, run_id=run_id, layer="mart", table_name=table_name,
            df_rejected=invalid_df, key_columns=["product_sk"],
            failure_reason="unique_customers_gt_total_orders", rundate=rundate
        )
        raise ValueError("Product Performance metric logic FAILED: unique_customers > total_orders")

    # 6. Derived metric consistency — quarantine dòng vi phạm
    invalid_rev_df = df.filter(
        (col("units_sold") > 0) & (col("revenue_per_unit") != col("total_revenue") / col("units_sold"))
    )
    invalid_revenue_per_unit = invalid_rev_df.count()
    _log_check(spark, run_id, table_name, "derived_revenue_per_unit_mismatch", invalid_revenue_per_unit, invalid_revenue_per_unit == 0, rundate)
    if invalid_revenue_per_unit > 0:
        insert_failed_records(
            spark, run_id=run_id, layer="mart", table_name=table_name,
            df_rejected=invalid_rev_df, key_columns=["product_sk"],
            failure_reason="revenue_per_unit_mismatch", rundate=rundate
        )
        raise ValueError("Product Performance revenue_per_unit FAILED")

    print("[DQ] Product Performance: PASSED")


def validate_sales_daily(spark, run_id, df, rundate):
    print("\n========== DATA QUALITY: SALES DAILY ==========")
    table_name = "sales_daily"

    # 1. Grain
    total_rows = df.count()
    distinct_dates = df.select("date").distinct().count()
    grain_diff = total_rows - distinct_dates
    print(f"[DQ] Total rows: {total_rows}")
    print(f"[DQ] Distinct dates: {distinct_dates}")
    _log_check(spark, run_id, table_name, "grain_duplicate_date", grain_diff, grain_diff == 0, rundate)
    if grain_diff != 0:
        raise ValueError("Sales Daily grain FAILED: duplicate date")

    # 2. Coverage
    fact_purchase = spark.read.table("gold.fact_purchase")
    gold_dates = (
        fact_purchase.join(spark.read.table("gold.dim_date"), "date_sk", "inner")
        .select("date").distinct().count()
    )
    print(f"[DQ] Dates in Gold: {gold_dates}")
    coverage_diff = builtins.abs(total_rows - gold_dates)
    _log_check(spark, run_id, table_name, "coverage_vs_fact_purchase", coverage_diff, total_rows == gold_dates, rundate)
    if total_rows != gold_dates:
        raise ValueError(f"Sales Daily coverage FAILED: Mart={total_rows}, Gold={gold_dates}")

    # 3. Revenue reconciliation
    mart_revenue = df.selectExpr("COALESCE(SUM(total_revenue), 0) AS value").first()["value"]
    gold_revenue = fact_purchase.selectExpr("COALESCE(SUM(total_amount), 0) AS value").first()["value"]
    print(f"[DQ] Mart revenue: {mart_revenue}")
    print(f"[DQ] Gold revenue: {gold_revenue}")
    _log_check(spark, run_id, table_name, "revenue_reconciliation_diff", mart_revenue - gold_revenue, mart_revenue == gold_revenue, rundate)
    if mart_revenue != gold_revenue:
        raise ValueError(f"Sales revenue reconciliation FAILED: Mart={mart_revenue}, Gold={gold_revenue}")

    # 4. Quantity reconciliation
    mart_items = df.selectExpr("COALESCE(SUM(total_items), 0) AS value").first()["value"]
    gold_items = fact_purchase.selectExpr("COALESCE(SUM(quantity), 0) AS value").first()["value"]
    print(f"[DQ] Mart items: {mart_items}")
    print(f"[DQ] Gold items: {gold_items}")
    _log_check(spark, run_id, table_name, "quantity_reconciliation_diff", mart_items - gold_items, mart_items == gold_items, rundate)
    if mart_items != gold_items:
        raise ValueError(f"Sales quantity reconciliation FAILED: Mart={mart_items}, Gold={gold_items}")

    print("[DQ] Sales Daily: PASSED")


# ---------------- DISPATCH / WRITE ----------------

def build_mart(spark, table_name):
    if table_name == "customer_360":
        return build_customer_360(spark)
    elif table_name == "product_performance":
        return build_product_performance(spark)
    elif table_name == "sales_daily":
        return build_sales_daily(spark)
    else:
        raise ValueError(f"Unsupported mart table: {table_name}")


def validate_mart(spark, run_id, df, table_name, rundate):
    if table_name == "customer_360":
        validate_customer_360(spark, run_id, df, rundate)
    elif table_name == "product_performance":
        validate_product_performance(spark, run_id, df, rundate)
    elif table_name == "sales_daily":
        validate_sales_daily(spark, run_id, df, rundate)
    else:
        raise ValueError(f"Unsupported mart table: {table_name}")


def write_mart(df, table_name):
    target_table = f"mart.{table_name}"
    print(f"\n[WRITE] {target_table}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f"[Mart] {target_table}: {df.count()} rows")


ALL_MART_TABLES = ["customer_360", "product_performance", "sales_daily"]


def parse_args():
    parser = argparse.ArgumentParser(description="Build một bảng Mart")
    parser.add_argument("--table", required=True, choices=ALL_MART_TABLES)
    return parser.parse_args()


def main():
    args = parse_args()
    table_name = args.table
    layer = "mart"

    run_id = generate_run_id(layer, table_name)
    started_at = datetime.now()
    rundate = str(date.today())

    spark = create_spark_session()
    row_count = 0

    try:
        df = build_mart(spark, table_name)

        # Validate TRƯỚC khi ghi — mart chỉ được cập nhật nếu dữ liệu đã reconcile đúng với Gold.
        # Mọi check ở đây là BLOCKING theo thiết kế gốc: sai lệch số liệu ở mart là lỗi nghiêm trọng,
        # không nên âm thầm ghi đè dashboard bằng số liệu sai.
        validate_mart(spark, run_id, df, table_name, rundate)

        write_mart(df, table_name)
        row_count = df.count()

        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name,
            status="PASS", started_at=started_at, finished_at=finished_at,
            input_rows=row_count, output_rows=row_count, rejected_rows=0, rundate=rundate,
        )
        print(f"[AUDIT] SUCCESS run_id={run_id}")

    except Exception as e:
        finished_at = datetime.now()
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name,
            status="FAIL", started_at=started_at, finished_at=finished_at,
            input_rows=row_count, output_rows=0, rejected_rows=None,
            error_message=str(e)[:2000], rundate=rundate,
        )
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()