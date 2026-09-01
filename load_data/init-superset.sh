#!/bin/bash
set -e

# Mặc định Superset dùng SQLite tại /app/superset_home/superset.db
# nếu không có biến SUPERSET_CONFIG_PATH / SQLALCHEMY_DATABASE_URI trỏ nơi khác.
# Không cần Postgres riêng cho metadata ở quy mô dev/local.

echo ">>> Migrating Superset metadata DB (SQLite)..."
superset db upgrade

echo ">>> Creating admin user (nếu chưa có)..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin123 \
    || true

echo ">>> Initializing roles & permissions..."
superset init

echo ">>> Starting Superset webserver..."
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger