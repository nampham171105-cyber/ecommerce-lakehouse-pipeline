# E-Commerce Data Lakehouse — Tích hợp Data Quality Framework

Một lakehouse dạng medallion (**Bronze → Silver → Gold → Mart**) chạy trên **Delta Lake**, điều phối bằng **Apache Airflow**, truy vấn qua **Trino**, và trực quan hoá bằng **Superset** — với một framework kiểm soát chất lượng dữ liệu (data quality) được thiết kế xuyên suốt từng layer, thay vì chỉ chắp vá thêm vào sau cùng.

## Vì sao làm dự án này

Đa số pipeline dữ liệu trong portfolio dừng lại ở "extract, transform, load". Dự án này đặt thêm một câu hỏi khó hơn: **làm sao biết được dữ liệu sau khi load vào là đúng?** Mỗi layer ở đây không chỉ ghi dữ liệu, mà còn ghi lại *bằng chứng* — cái gì đã được kiểm tra, cái gì pass, cái gì bị loại và vì sao — để không có con số sai nào âm thầm lọt lên dashboard.

## Kiến trúc

```
PostgreSQL (OLTP)
      │  JDBC, load incremental theo watermark
      ▼
  ┌─────────┐   check schema + row-count
  │ BRONZE  │──────────────────────────────┐
  └────┬────┘                              │
       │ na.drop + dedupe + business rule  │
       ▼                                   │
  ┌─────────┐   quarantine dòng bị loại    │
  │ SILVER  │──────────────────────────────┤    control schema (Delta)
  └────┬────┘                              │    ├── job_control      (watermark)
       │ surrogate key, SCD1/SCD2, join    │    ├── quality_checks   (catalog rule)
       ▼                                   ├───▶├── quality_metrics  (kết quả từng check)
  ┌─────────┐   check khoá ngoại orphan    │    ├── failed_records   (dòng bị quarantine)
  │  GOLD   │──────────────────────────────┤    └── audit_log        (trạng thái mỗi run)
  └────┬────┘                              │
       │ tổng hợp (aggregation)            │
       ▼                                   │
  ┌─────────┐   đối soát (reconciliation)  │
  │  MART   │      với Gold                │
  └─────────┘──────────────────────────────┘
       │
       ▼
   Trino  ──▶  Dashboard Superset
```

Toàn bộ dữ liệu lưu bằng **Delta Lake trên MinIO** (S3-compatible), quản lý bởi **Hive Metastore**, truy vấn được đồng thời từ cả Spark lẫn Trino.

## Tech stack

| Layer | Công cụ |
|---|---|
| Điều phối | Apache Airflow (song song theo từng bảng, không chặn ở cấp TaskGroup) |
| Xử lý dữ liệu | Apache Spark 3.5.1 (PySpark) |
| Định dạng lưu trữ | Delta Lake |
| Object storage | MinIO |
| Catalog | Hive Metastore |
| Query engine | Trino |
| BI / Dashboard | Apache Superset |
| Database nguồn | PostgreSQL |
| Container hoá | Docker Compose |

## Framework kiểm soát chất lượng dữ liệu

Thay vì chỉ viết `if` kiểm tra rồi cho job crash, mỗi bước validate trong pipeline này đều:

1. **Tính ra 1 metric cụ thể** (% mất dữ liệu, % vi phạm rule, % orphan, chênh lệch đối soát...)
2. **Ghi vào `control.quality_metrics`** — dù pass hay fail đều được log, không bao giờ âm thầm bỏ qua
3. **Phân loại theo severity**: `BLOCKING` (raise và dừng pipeline — dùng cho lỗi schema, vi phạm business rule, sai lệch đối soát ở mart) và `WARNING` (chỉ log rồi tiếp tục — dùng cho khoá ngoại orphan ở Gold, vì trong pipeline chạy song song, việc dimension load trễ hơn fact một nhịp là chuyện bình thường)
4. **Quarantine các dòng vi phạm** vào `control.failed_records` bằng anti-join, thay vì drop âm thầm không để lại dấu vết
5. **Ghi audit trail đầy đủ** cho mỗi lần chạy (`run_id`, số dòng input/output/bị loại, thời điểm bắt đầu/kết thúc, message lỗi) vào `control.audit_log`

Mỗi check được gán vào 1 trong 5 dimension của data quality: **validity, completeness, integrity, accuracy, consistency**.

### Một đánh đổi thiết kế đáng nói

Các bảng fact ở Gold được build song song theo từng bảng cùng lúc với Silver, không bị chặn lại chờ "tất cả dimension load xong". Điều này có nghĩa 1 fact table có thể tạm thời join phải dimension chưa kịp cập nhật (sinh ra khoá ngoại orphan). Thay vì chặn cứng cả pipeline vì tình huống race-condition vốn đã lường trước này, các check orphan được để ở mức `WARNING`: vẫn log, vẫn quarantine, vẫn theo dõi được — nhưng không làm fail job. Ngược lại, các check đối soát ở Mart luôn là `BLOCKING`, vì một con số sai lọt lên dashboard cho business là lỗi về tính đúng đắn, không phải do timing.

## Cấu trúc project

```
docker-compose.yml
docker/
  Dockerfile.spark          # Spark + Delta + S3A + Postgres JDBC
  Dockerfile.airflow        # Airflow + Spark provider + Trino client
  Dockerfile.hive-metastore
  Dockerfile.lakehouse-init
  Dockerfile.superset       # Superset + driver SQLAlchemy cho Trino
airflow/dags/
  lakehouse_pipeline.py     # DAG Bronze → Silver → Gold → Mart
  dq_utils.py               # hàm cảnh báo + giám sát DQ
spark/jobs/
  ingest_bronze.py
  transform_silver.py
  build_gold.py
  build_mart.py
spark/utils/
  job_control.py            # đọc/ghi watermark + quality_metrics/audit_log/failed_records
  validation.py              # check schema, row-loss, business rule, referential integrity
load_data/
  init_lakehouse.sh          # tạo bucket MinIO + schema Trino + các bảng control
  seed_quality_checks.sh     # ghi tài liệu các rule đang chạy vào control.quality_checks
  setup_superset_dashboard.py
```

## Cách chạy

```bash
docker compose up -d --build
```

Lệnh này khởi động Postgres (nguồn), Spark (1 master + 2 worker), MinIO, Hive Metastore, Trino, Airflow, và Superset. Sau khi mọi service healthy:

1. Load dữ liệu mẫu e-commerce vào Postgres (`load_data/init_postgres.sql`, `load_data/load_csv_to_db.sql`)
2. Trigger DAG `lakehouse_pipeline` từ Airflow UI (`localhost:8090`)
3. Truy vấn kết quả qua Trino (`localhost:8085`) hoặc xem dashboard trên Superset (`localhost:8088`)

## Tình trạng hiện tại

- ✅ Pipeline Bronze / Silver / Gold / Mart đã tích hợp đầy đủ DQ (metric, quarantine, audit log)
- ✅ Trino liên kết truy vấn được xuyên suốt mọi layer, kể cả schema `control`
- 🚧 Cảnh báo lỗi qua Airflow (Slack webhook trong `dq_utils.py`) — đã code xong, đang chờ test end-to-end
- 🚧 Dashboard DQ trên Superset (xu hướng pass-rate, độ trễ dữ liệu, thống kê failed-records) — script tự tạo dataset đã xong, đang dựng dashboard

## Những gì mình sẽ cải thiện tiếp

- Chuyển các dict rule đang hard-code (`BRONZE_RULES`, `SILVER_RULES`) vào `control.quality_checks` để thành một rule engine thực sự đọc động từ metadata
- Tách riêng số liệu "bị loại vì null" và "bị loại vì trùng key" thay vì gộp chung vào 1 con số `rejected_rows`
- Thêm SLA về độ tươi (freshness) của dữ liệu với cảnh báo tự động khi dữ liệu bị trễ, thay vì chỉ xem thụ động qua dashboard
