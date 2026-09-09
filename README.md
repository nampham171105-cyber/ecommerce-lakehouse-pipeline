# E-Commerce Data Lakehouse — Tích hợp Data Quality Framework

Thiết kế 1 data lakehouse sử dụng kiến trúc medallion (**Bronze → Silver → Gold → Mart**) chạy trên **Delta Lake**, điều phối bằng **Apache Airflow**, truy vấn qua **Trino**, và trực quan hoá bằng **Superset** — với một framework kiểm soát chất lượng dữ liệu (data quality) được thiết kế xuyên suốt từng layer, thay vì chỉ chắp vá thêm vào sau cùng.

## Vì sao làm dự án này

Đa số pipeline dữ liệu trong portfolio dừng lại ở "extract, transform, load". Dự án này đặt thêm một câu hỏi khó hơn: **làm sao biết được dữ liệu sau khi load vào là đúng?** Mỗi layer ở đây không chỉ ghi dữ liệu, mà còn ghi lại bằng chứng — cái gì đã được kiểm tra, cái gì pass, cái gì bị loại và vì sao — để không có con số sai nào âm thầm lọt lên dashboard.

## Kiến trúc
<img width="963" height="694" alt="image" src="https://github.com/user-attachments/assets/827c7866-4c9e-4d52-93bc-17131b39e87c" />

## FLow
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

## Framework kiểm soát chất lượng dữ liệu

Thay vì chỉ viết `if` kiểm tra rồi cho job crash, mỗi bước validate trong pipeline này đều:

1. **Tính ra 1 metric cụ thể** (% mất dữ liệu, % vi phạm rule, % orphan, chênh lệch đối soát...)
2. **Ghi vào `control.quality_metrics`** — dù pass hay fail đều được log, không bao giờ âm thầm bỏ qua
3. **Phân loại theo severity**: `BLOCKING` (raise và dừng pipeline — dùng cho lỗi schema, vi phạm business rule, sai lệch đối soát ở mart) và `WARNING` (chỉ log rồi tiếp tục)
4. **Quarantine các dòng vi phạm** vào `control.failed_records` bằng anti-join, thay vì drop âm thầm không để lại dấu vết
5. **Ghi audit trail đầy đủ** cho mỗi lần chạy (`run_id`, số dòng input/output/bị loại, thời điểm bắt đầu/kết thúc, message lỗi) vào `control.audit_log`

Mỗi check được gán vào 1 trong 5 dimension của data quality: **validity, completeness, integrity, accuracy, consistency**.

## Một quyết định thiết kế đáng nói

Toàn bộ pipeline dùng chiến lược high-watermark incremental load: mỗi bảng ở mỗi layer lưu lại giá trị watermark lớn nhất đã xử lý (control.job_control), và lần chạy sau chỉ đọc phần dữ liệu mới hơn giá trị đó — ở Bronze dựa trên cột nghiệp vụ (update_at), còn từ Silver trở lên dùng ingest_at do chính pipeline sinh ra.

Lựa chọn này mang lại vài lợi ích rõ rệt so với việc full-load lại toàn bộ bảng mỗi lần chạy:

- Chi phí tính toán tối thiểu: mỗi lần chạy chỉ xử lý đúng phần dữ liệu mới, không phải quét lại toàn bộ lịch sử — quan trọng khi dữ liệu nguồn ngày càng lớn theo thời gian.
- Không cần hạ tầng phức tạp: không đòi hỏi CDC, không cần Debezium hay Kafka, chỉ cần 1 cột timestamp tăng dần ở nguồn và 1 bảng control nhỏ để lưu trạng thái — dễ triển khai, dễ debug, dễ giải thích cho người khác trong team.
- Tách biệt rõ trạng thái xử lý khỏi dữ liệu nghiệp vụ: watermark được lưu tập trung trong control.job_control theo từng cặp (layer, table_name), nên có thể theo dõi tiến độ của từng bảng độc lập, dễ dàng reset lại 1 bảng cụ thể để full-load lại mà không ảnh hưởng các bảng khác.

## Airflow

<img width="1411" height="631" alt="image" src="https://github.com/user-attachments/assets/588c9731-c37a-447e-9b8d-b8504890c855" />

## Dashboard

<img width="1869" height="1307" alt="sales-overview-2026-09-08T10-39-45 594Z" src="https://github.com/user-attachments/assets/c7d7185f-4dd7-4f0c-8be2-5c1aed91f4cd" />

## Cách chạy

```bash
docker compose up -d --build
```

Lệnh này khởi động Postgres (nguồn), Spark (1 master + 2 worker), MinIO, Hive Metastore, Trino, Airflow, và Superset. Sau khi mọi service healthy:

1. Load dữ liệu mẫu e-commerce vào Postgres (`load_data/init_postgres.sql`, `load_data/load_csv_to_db.sql`)
2. Trigger DAG `lakehouse_pipeline` từ Airflow UI (`localhost:8090`)
3. Truy vấn kết quả qua Trino (`localhost:8085`) hoặc xem dashboard trên Superset (`localhost:8088`)

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
  init_postgres.sql          # tạo bảng cho nguồn
  load_csv_to_db             # load các file csv vào nguồn postgres
```

## Những gì mình sẽ cải thiện tiếp

- Chuyển các dict rule đang hard-code (`BRONZE_RULES`, `SILVER_RULES`) vào `control.quality_checks` để thành một rule engine thực sự đọc động từ metadata
- Tách riêng số liệu "bị loại vì null" và "bị loại vì trùng key" thay vì gộp chung vào 1 con số `rejected_rows`
- Thêm quarantine table để việc xử lý lỗi dễ dàng hơn
- Cảnh báo lỗi qua Airflow (Slack webhook) — sẽ hoàn thiện sau
- Tối ưu hóa pipeline - sẽ hoàn thiện sau
