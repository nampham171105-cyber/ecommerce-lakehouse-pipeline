from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, desc


def validate_schema(df: DataFrame, expected_columns: set, step_name: str):
    actual_columns = set(df.columns)

    missing = expected_columns - actual_columns
    extra = actual_columns - expected_columns

    if missing:
        raise ValueError(f"[SCHEMA VALIDATION FAILED] {step_name}: thiếu cột {missing}")
    if extra:
        print(f"[SCHEMA WARNING] {step_name}: có cột thừa không mong đợi {extra}")
    print(f"[VALIDATION] {step_name}: schema OK ({len(actual_columns)} cột)")


def validate_minimum_row_count(df: DataFrame, min_rows: int, step_name: str):
    count = df.count()
    print(f"[VALIDATION] {step_name}: {count} rows (ngưỡng tối thiểu: {min_rows})")
    if count < min_rows:
        raise ValueError(
            f"[VALIDATION FAILED] {step_name}: chỉ có {count} dòng, "
            f"thấp hơn ngưỡng tối thiểu {min_rows} — nghi ngờ ingest bị thiếu"
        )


def validate_no_duplicates(df: DataFrame, key_columns: list, step_name: str):
    """Silver bắt buộc KHÔNG còn duplicate theo key nghiệp vụ — khác report_duplicate_count (chỉ ghi nhận) ở Bronze."""
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    if total != distinct:
        raise ValueError(
            f"[VALIDATION FAILED] {step_name}: vẫn còn {total - distinct} dòng trùng theo {key_columns} "
            f"sau khi dedup — logic dedupe_by_key/dedupe_exact chưa xử lý triệt để"
        )
    print(f"[VALIDATION] {step_name}: không còn duplicate theo {key_columns} — OK")


def validate_row_count(df_before: DataFrame, df_after: DataFrame, step_name: str, max_loss_pct: float = 5.0):
    count_before = df_before.count()
    count_after = df_after.count()
    loss_pct = (count_before - count_after) / count_before * 100 if count_before > 0 else 0

    print(f"[VALIDATION] {step_name}: {count_before} -> {count_after} rows ({loss_pct:.2f}% loss)")

    if loss_pct > max_loss_pct:
        raise ValueError(
            f"[VALIDATION FAILED] {step_name}: mất {loss_pct:.2f}% dữ liệu, "
            f"vượt ngưỡng cho phép {max_loss_pct}%"
        )
    return df_after


# def validate_not_null(df: DataFrame, critical_columns: list, step_name: str, max_null_pct: float = 1.0):
#     total = df.count()
#     if total == 0:
#         raise ValueError(f"[VALIDATION FAILED] {step_name}: bảng rỗng, không thể validate")

#     for c in critical_columns:
#         null_count = df.filter(col(c).isNull()).count()
#         null_pct = null_count / total * 100
#         print(f"[VALIDATION] {step_name}.{c}: {null_count} nulls ({null_pct:.2f}%)")
#         if null_pct > max_null_pct:
#             raise ValueError(
#                 f"[VALIDATION FAILED] {step_name}.{c}: {null_pct:.2f}% NULL, vượt ngưỡng {max_null_pct}%"
#             )


# def validate_uniqueness(df: DataFrame, key_columns: list, step_name: str):
#     total = df.count()
#     distinct = df.select(key_columns).distinct().count()
#     if total != distinct:
#         raise ValueError(
#             f"[VALIDATION FAILED] {step_name}: {key_columns} không unique "
#             f"({total} dòng nhưng chỉ {distinct} giá trị duy nhất)"
#         )
#     print(f"[VALIDATION] {step_name}: {key_columns} unique OK ({distinct} giá trị)")


# def validate_referential_integrity(df_fact: DataFrame, df_dim: DataFrame, fact_key: str, dim_key: str, step_name: str, max_orphan_pct: float = 1.0):
#     total = df_fact.count()
#     orphans = df_fact.join(df_dim, df_fact[fact_key] == df_dim[dim_key], "left_anti")
#     orphan_count = orphans.count()
#     orphan_pct = orphan_count / total * 100 if total > 0 else 0

#     print(f"[VALIDATION] {step_name}: {orphan_count} dòng mồ côi ({orphan_pct:.2f}%)")
#     if orphan_pct > max_orphan_pct:
#         raise ValueError(
#             f"[VALIDATION FAILED] {step_name}: {orphan_pct:.2f}% dòng có {fact_key} "
#             f"không khớp {dim_key}, vượt ngưỡng {max_orphan_pct}%"
#         )


def validate_business_rule(df: DataFrame, rule_name: str, condition, step_name: str, max_violation_pct: float = 1.0):
    total = df.count()
    violation_count = df.filter(~condition).count()
    violation_pct = violation_count / total * 100 if total > 0 else 0

    print(f"[VALIDATION] {step_name}.{rule_name}: {violation_count} vi phạm ({violation_pct:.2f}%)")
    if violation_pct > max_violation_pct:
        raise ValueError(
            f"[VALIDATION FAILED] {step_name}.{rule_name}: {violation_pct:.2f}% vi phạm, "
            f"vượt ngưỡng {max_violation_pct}%"
        )


def dedupe_by_key(df: DataFrame, key_columns: list, order_column: str, keep: str = "latest") -> DataFrame:
    """Giữ lại đúng 1 bản ghi cho mỗi key, chọn theo order_column (mới nhất/cũ nhất)."""
    order_expr = desc(order_column) if keep == "latest" else col(order_column)
    window_spec = Window.partitionBy(*key_columns).orderBy(order_expr)

    return df \
        .withColumn("_rn", row_number().over(window_spec)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")