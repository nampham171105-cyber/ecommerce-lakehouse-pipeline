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
    expected_schema: dict,  
    layer: str,
    table_name: str,
    rundate: str,
    severity: str = "BLOCKING"
):
    # Lấy schema thực tế của df dưới dạng dict (VD: {"user_id": "string", "age": "int"})
    actual_schema = dict(df.dtypes)
    
    expected_cols = set(expected_schema.keys())
    actual_cols = set(actual_schema.keys())

    # 1. Tìm các cột thiếu và cột thừa
    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    # 2. Kiểm tra sai lệch kiểu dữ liệu (chỉ check trên các cột tồn tại ở cả 2 bên)
    type_mismatches = {}
    for col_name in expected_cols.intersection(actual_cols):
        exp_type = expected_schema[col_name]
        act_type = actual_schema[col_name]
        if exp_type != act_type:
            type_mismatches[col_name] = f"Expected: {exp_type}, Actual: {act_type}"

    # ==========================================
    # GHI METRICS VÀO DATABASE CHO TỪNG HẠNG MỤC
    # ==========================================
    
    # Check 1: Missing Columns (Mất cột)
    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="schema_missing_columns", metric_value=len(missing),
        passed=(len(missing) == 0), rundate=rundate
    )

    # Check 2: Extra Columns (Thừa cột)
    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="schema_extra_columns", metric_value=len(extra),
        passed=(len(extra) == 0), rundate=rundate
    )

    # Check 3: Type Mismatch (Sai kiểu dữ liệu)
    insert_quality_metric(
        spark, run_id=run_id, layer=layer, table_name=table_name,
        check_name="schema_type_mismatch", metric_value=len(type_mismatches),
        passed=(len(type_mismatches) == 0), rundate=rundate
    )

    # ==========================================
    # LOGIC CHẶN PIPELINE (BLOCKING)
    # ==========================================
    
    # Thừa cột thường chỉ là cảnh báo, không gây chết logic cấp bách
    if extra:
        print(f"[SCHEMA WARNING] {layer}.{table_name}: Có cột thừa {extra}")

    # Thiếu cột HOẶC sai kiểu dữ liệu là lỗi "Chí mạng" (Fatal Error)
    is_fatal = (len(missing) > 0) or (len(type_mismatches) > 0)

    if is_fatal:
        error_msgs = []
        if missing:
            error_msgs.append(f"Thiếu cột: {missing}")
        if type_mismatches:
            error_msgs.append(f"Sai Data Type: {type_mismatches}")
            
        final_msg = f"[SCHEMA VALIDATION FAILED] {layer}.{table_name} -> " + " | ".join(error_msgs)
        
        if severity == "BLOCKING":
            raise ValueError(final_msg)
        else:
            print(f"[SCHEMA WARNING - non blocking] {final_msg}")
    
    if not is_fatal:
        print(f"[VALIDATION] {layer}.{table_name}: schema OK")


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
    key_columns: str,
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
    max_loss_pct: float = 10.0,
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
    max_orphan_pct: float = 15.0,
    severity: str = "BLOCKING"
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
# DEDUPE 
# ============================================================
def dedupe_by_key(df: DataFrame, key_columns: str, order_column: str) -> DataFrame:
    window_spec = Window.partitionBy(key_columns).orderBy(desc(order_column))
    return df \
        .withColumn("_rn", row_number().over(window_spec)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")