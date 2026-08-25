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

-- Thêm các câu lệnh CREATE TABLE hoặc lệnh SQL khác của bạn vào đây
"

echo ">>> Lakehouse initialized successfully"