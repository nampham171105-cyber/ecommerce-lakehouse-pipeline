from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.functions import col, row_number, desc
from job_control import insert_quality_metric, insert_failed_records


# ============================================================
# SCHEMA (Bronze) — dimension: Validity
# ============================================================
def validate_schema(
    spark: SparkSession,
    run_id: str,
    df: DataFrame,
    expected_columns: set,
    layer: str,
    table_name: str,
    rundate: str,
    severity: str = "BLOCKING"
):
    actual_columns = set(df.columns)
    missing = expected_columns - actual_columns
    extra = actual_columns - expected_columns
    passed = len(missing) == 0

    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="schema_missing_columns", metric_value=len(missing),
        passed=passed, rundate=rundate
    )

    if extra:
        print(f"[SCHEMA WARNING] {layer}.{table_name}: có cột thừa không mong đợi {extra}")

    if not passed:
        msg = f"[SCHEMA VALIDATION FAILED] {layer}.{table_name}: thiếu cột {missing}"
        if severity == "BLOCKING":
            raise ValueError(msg)
        print(f"[SCHEMA WARNING - non blocking] {msg}")

    print(f"[VALIDATION] {layer}.{table_name}: schema OK ({len(actual_columns)} cột)")


# ============================================================
# MIN ROW COUNT (Bronze) — dimension: Completeness
# ============================================================
def validate_minimum_row_count(
    spark: SparkSession,
    run_id: str,
    df: DataFrame,
    min_rows: int,
    layer: str,
    table_name: str,
    rundate: str,
    severity: str = "BLOCKING"
):
    count = df.count()
    passed = count >= min_rows

    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="min_row_count", metric_value=count,
        passed=passed, rundate=rundate
    )

    print(f"[VALIDATION] {layer}.{table_name}: {count} rows (ngưỡng tối thiểu: {min_rows})")
    if not passed:
        msg = (
            f"[VALIDATION FAILED] {layer}.{table_name}: chỉ có {count} dòng, "
            f"thấp hơn ngưỡng tối thiểu {min_rows} — nghi ngờ ingest bị thiếu"
        )
        if severity == "BLOCKING":
            raise ValueError(msg)
        print(f"[VALIDATION WARNING - non blocking] {msg}")


# ============================================================
# UNIQUENESS sau dedupe (Silver) — dimension: Integrity
# ============================================================
def validate_no_duplicates(
    spark: SparkSession,
    run_id: str,
    df: DataFrame,
    key_columns: list,
    layer: str,
    table_name: str,
    rundate: str
):
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    dup_count = total - distinct
    passed = dup_count == 0

    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="no_duplicates", metric_value=dup_count,
        passed=passed, rundate=rundate
    )

    if not passed:
        raise ValueError(
            f"[VALIDATION FAILED] {layer}.{table_name}: vẫn còn {dup_count} dòng trùng theo "
            f"{key_columns} sau khi dedup — logic dedupe_by_key chưa xử lý triệt để"
        )
    print(f"[VALIDATION] {layer}.{table_name}: không còn duplicate theo {key_columns} — OK")


# ============================================================
# ROW LOSS khi làm sạch (Silver) — dimension: Completeness
# So sánh trước/sau clean_fn, quarantine phần bị mất nếu có key_columns
# ============================================================
def validate_row_count(
    spark: SparkSession,
    run_id: str,
    df_before: DataFrame,
    df_after: DataFrame,
    layer: str,
    table_name: str,
    rundate: str,
    max_loss_pct: float = 5.0,
    key_columns: list = None,
    severity: str = "BLOCKING"
):
    count_before = df_before.count()
    count_after = df_after.count()
    loss_pct = (count_before - count_after) / count_before * 100 if count_before > 0 else 0
    passed = loss_pct <= max_loss_pct

    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="row_loss_pct", metric_value=round(loss_pct, 2),
        passed=passed, rundate=rundate
    )

    print(f"[VALIDATION] {layer}.{table_name}: {count_before} -> {count_after} rows ({loss_pct:.2f}% loss)")

    # Quarantine phần dòng bị mất — nếu biết key_columns thì anti-join để lấy lại record bị loại
    if key_columns and count_before > count_after:
        df_rejected = df_before.join(df_after, on=key_columns, how="left_anti")
        insert_failed_records(
            spark, run_id=run_id, layer=layer, table_name=table_name,
            df_rejected=df_rejected, key_columns=key_columns,
            failure_reason="dropped_during_cleaning_or_dedupe", rundate=rundate
        )

    if not passed:
        msg = (
            f"[VALIDATION FAILED] {layer}.{table_name}: mất {loss_pct:.2f}% dữ liệu, "
            f"vượt ngưỡng cho phép {max_loss_pct}%"
        )
        if severity == "BLOCKING":
            raise ValueError(msg)
        print(f"[VALIDATION WARNING - non blocking] {msg}")

    return df_after


# ============================================================
# BUSINESS RULE (Silver) — dimension: Validity
# Quarantine luôn các dòng vi phạm, dù có vượt ngưỡng raise hay không
# ============================================================
def validate_business_rule(
    spark: SparkSession,
    run_id: str,
    df: DataFrame,
    rule_name: str,
    condition,
    key_columns: list,
    layer: str,
    table_name: str,
    rundate: str,
    max_violation_pct: float = 1.0,
    severity: str = "BLOCKING"
):
    total = df.count()
    df_violations = df.filter(~condition)
    violation_count = df_violations.count()
    violation_pct = violation_count / total * 100 if total > 0 else 0
    passed = violation_pct <= max_violation_pct

    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name=rule_name, metric_value=round(violation_pct, 2),
        passed=passed, rundate=rundate
    )

    print(f"[VALIDATION] {layer}.{table_name}.{rule_name}: {violation_count} vi phạm ({violation_pct:.2f}%)")

    if violation_count > 0 and key_columns:
        insert_failed_records(
            spark, run_id=run_id, layer=layer, table_name=table_name,
            df_rejected=df_violations, key_columns=key_columns,
            failure_reason=f"business_rule_violation:{rule_name}", rundate=rundate
        )

    if not passed:
        msg = (
            f"[VALIDATION FAILED] {layer}.{table_name}.{rule_name}: {violation_pct:.2f}% vi phạm, "
            f"vượt ngưỡng {max_violation_pct}%"
        )
        if severity == "BLOCKING":
            raise ValueError(msg)
        print(f"[VALIDATION WARNING - non blocking] {msg}")


# ============================================================
# REFERENTIAL INTEGRITY (Gold) — dimension: Integrity
# fact join dim có orphan (mồ côi) không
# ============================================================
def validate_referential_integrity(
    spark: SparkSession,
    run_id: str,
    df_fact: DataFrame,
    df_dim: DataFrame,
    fact_key: str,
    dim_key: str,
    layer: str,
    table_name: str,
    rundate: str,
    max_orphan_pct: float = 1.0,
    severity: str = "WARNING"
):
    total = df_fact.count()
    orphans = df_fact.join(df_dim, df_fact[fact_key] == df_dim[dim_key], "left_anti")
    orphan_count = orphans.count()
    orphan_pct = orphan_count / total * 100 if total > 0 else 0
    passed = orphan_pct <= max_orphan_pct

    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name=f"referential_integrity:{fact_key}", metric_value=round(orphan_pct, 2),
        passed=passed, rundate=rundate
    )

    print(f"[VALIDATION] {layer}.{table_name}: {orphan_count} dòng mồ côi theo {fact_key} ({orphan_pct:.2f}%)")

    if not passed:
        msg = (
            f"[VALIDATION FAILED] {layer}.{table_name}: {orphan_pct:.2f}% dòng có {fact_key} "
            f"không khớp {dim_key}, vượt ngưỡng {max_orphan_pct}%"
        )
        if severity == "BLOCKING":
            raise ValueError(msg)
        print(f"[VALIDATION WARNING - non blocking] {msg}")


# ============================================================
# DEDUPE — giữ nguyên logic cũ, KHÔNG đổi signature vì được gọi
# trực tiếp trong clean_fn() (chưa có run_id ở đó). Quarantine
# phần trùng lặp được xử lý ở validate_row_count (so before/after)
# thay vì ở đây, để tránh phải truyền spark/run_id xuyên suốt clean_fn.
# ============================================================
def dedupe_by_key(df: DataFrame, key_columns: list, order_column: str, keep: str = "latest") -> DataFrame:
    order_expr = desc(order_column) if keep == "latest" else col(order_column)
    window_spec = Window.partitionBy(*key_columns).orderBy(order_expr)

    return df \
        .withColumn("_rn", row_number().over(window_spec)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")