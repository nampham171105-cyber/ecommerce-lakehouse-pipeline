#!/bin/bash
set -e

echo ">>> Creating MinIO bucket..."
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb --ignore-existing local/lakehouse

echo ">>> Creating schemas and tables in Trino..."
trino --server http://trino:8080 --execute "
CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3a://lakehouse/bronze/');
CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3a://lakehouse/silver/');
CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3a://lakehouse/gold/');
CREATE SCHEMA IF NOT EXISTS delta.control WITH (location = 's3a://lakehouse/control/');
CREATE SCHEMA IF NOT EXISTS delta.mart WITH (location = 's3a://lakehouse/mart/');

CREATE TABLE IF NOT EXISTS delta.control.job_control (
    layer VARCHAR,
    table_name VARCHAR,
    watermark_column VARCHAR,
    watermark_value TIMESTAMP,
    rundate VARCHAR,
    insert_dt TIMESTAMP
)
WITH (
    location = 's3a://lakehouse/control/job_control/'
);
-- ========================================================
-- 1. QualityChecks — metadata catalog định nghĩa các rule DQ
--    (thay vì hard-code trong BRONZE_RULES / SILVER_RULES)
-- ========================================================
CREATE TABLE IF NOT EXISTS delta.control.quality_checks (
    check_id           VARCHAR,     -- có thể dùng sha2(layer||table||check_name)
    layer               VARCHAR,     -- bronze | silver | gold | mart
    table_name          VARCHAR,
    check_name          VARCHAR,     -- vd: age_in_range, price_positive
    dimension            VARCHAR,     -- completeness | accuracy | validity | consistency | integrity | timeliness
    rule_expr           VARCHAR,     -- mô tả biểu thức rule (tham khảo, logic thật nằm trong code)
    threshold_pct       DOUBLE,      -- ngưỡng % vi phạm cho phép
    severity             VARCHAR,     -- BLOCKING | WARNING
    is_active            BOOLEAN,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP
)
WITH (
    location = 's3a://lakehouse/control/quality_checks/'
);

-- ========================================================
-- 2. QualityMetrics — kết quả đo mỗi lần chạy check
-- ========================================================
CREATE TABLE IF NOT EXISTS delta.control.quality_metrics (
    metric_id      VARCHAR,
    run_id          VARCHAR,
    layer            VARCHAR,
    table_name       VARCHAR,
    check_name       VARCHAR,
    metric_value    VARCHAR,     -- để VARCHAR, cast khi query (tránh lệch kiểu số giữa các check)
    passed           VARCHAR,     -- 'true' / 'false'
    rundate          VARCHAR,
    created_at       TIMESTAMP
)
WITH (
    location = 's3a://lakehouse/control/quality_metrics/'
);

-- ========================================================
-- 3. FailedRecords — log record bị loại (quarantine)
-- ========================================================
CREATE TABLE IF NOT EXISTS delta.control.failed_records (
    failed_record_id  VARCHAR,
    run_id              VARCHAR,
    layer                VARCHAR,
    table_name           VARCHAR,
    record_id            VARCHAR,     -- key nghiệp vụ ghép lại, vd concat_ws('|', ...)
    failure_reason       VARCHAR,
    rundate              VARCHAR,
    logged_at            TIMESTAMP
)
WITH (
    location = 's3a://lakehouse/control/failed_records/'
);

-- ========================================================
-- 4. AuditLog — lịch sử thực thi từng task (mọi layer)
-- ========================================================
CREATE TABLE IF NOT EXISTS delta.control.audit_log (
    audit_id       VARCHAR,
    run_id          VARCHAR,
    layer            VARCHAR,
    table_name       VARCHAR,
    status           VARCHAR,     -- PASS | PARTIAL_FAIL | FAIL
    input_rows      BIGINT,
    output_rows     BIGINT,
    rejected_rows   BIGINT,
    started_at       TIMESTAMP,
    finished_at      TIMESTAMP,
    error_message   VARCHAR,
    rundate          VARCHAR
)
WITH (
    location = 's3a://lakehouse/control/audit_log/'
);

-- Thêm các câu lệnh CREATE TABLE hoặc lệnh SQL khác của bạn vào đây
"

echo ">>> Lakehouse initialized successfully"