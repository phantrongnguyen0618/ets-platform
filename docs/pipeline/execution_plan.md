# Kế hoạch Thực hiện Từng Hành động trong Pipeline (Sequential Execution Plan)

Tài liệu này hướng dẫn chi tiết thứ tự thực hiện và các bước kích hoạt từng thành phần trong pipeline xử lý dữ liệu Yelp từ đầu vào (Raw JSON) đến đầu ra (API/Dashboard/Power BI).

---

## 🗺️ Bản đồ Thứ tự Thực hiện Pipeline

```
[BƯỚC 1: Khởi tạo Hạ tầng] ──> [BƯỚC 2: Ingestion lên MinIO] ──> [BƯỚC 3: Kích hoạt Airflow]
                                                                        │
┌───────────────────────────────────────────────────────────────────────┘
│
▼
[BƯỚC 4: PySpark Bronze ETL] ──> [BƯỚC 5: PySpark Silver ETL] ──> [BƯỚC 6: PySpark Gold ETL]
                                                                        │
┌───────────────────────────────────────────────────────────────────────┘
│
▼
[BƯỚC 7: SQL Materialized Views] ──> [BƯỚC 8: Kiểm định Data Quality] ──> [BƯỚC 9: Phục vụ & Trực quan]
```

---

## 📋 Chi tiết các Bước Thực hiện theo Thứ tự

### Bước 1: Khởi tạo Hạ tầng (Infrastructure Initialization)
* **Mục tiêu**: Đảm bảo tất cả các container dịch vụ (PostgreSQL, MinIO, Spark, Airflow) hoạt động ổn định và sẵn sàng kết nối.
* **Hành động thực hiện**:
  1. Cấu hình các biến môi trường trong tệp `.env` (sao chép từ `.env.example`).
  2. Khởi chạy Docker Compose:
     ```bash
     docker-compose up -d
     ```
  3. Kiểm tra trạng thái sức khỏe (health checks) của các dịch vụ bằng lệnh:
     ```bash
     docker-compose ps
     ```
  4. Xác nhận các cổng truy cập hoạt động:
     - MinIO Console: `http://localhost:9001`
     - Airflow Webserver: `http://localhost:8080` (Tài khoản: `admin` / `admin`)
     - Spark WebUI: `http://localhost:8080`
     - PostgreSQL Port: `5432`

---

### Bước 2: Ingest Dữ liệu Yelp lên MinIO (Data Ingestion to Landing Zone)
* **Mục tiêu**: Đưa các tệp dữ liệu Yelp JSON từ thư mục cục bộ lên thùng chứa (bucket) raw của MinIO.
* **Hành động thực hiện**:
  1. Đảm bảo các tệp dữ liệu Yelp nằm đúng vị trí trong thư mục: `datasets/raw/` (`yelp_academic_dataset_business.json`, `yelp_academic_dataset_review.json`, v.v.).
  2. Thực hiện chạy script Python Ingestion:
     ```bash
     python src/ingestion/ingest.py
     ```
  3. **Kết quả kiểm tra**: Truy cập vào MinIO Console (`http://localhost:9001`), kiểm tra bucket `yelp-raw` xem đã xuất hiện các tệp JSON được tổ chức theo thư mục (ví dụ: `business/yelp_academic_dataset_business.json`) hay chưa.

---

### Bước 3: Lập lịch và Khởi động Airflow Orchestrator
* **Mục tiêu**: Nạp và kích hoạt DAG lập lịch toàn bộ quy trình xử lý dữ liệu.
* **Hành động thực hiện**:
  1. Đặt các tệp định nghĩa DAG vào thư mục `airflow/dags/` (ví dụ: `yelp_medallion_dag.py`).
  2. Truy cập Airflow Webserver (`http://localhost:8080`).
  3. Bật (Unpause) DAG `yelp_analytics_pipeline` trên giao diện Airflow.
  4. Trình kích hoạt (Scheduler) của Airflow sẽ tự động điều phối thứ tự chạy của các tác vụ từ Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ DWH $\rightarrow$ Data Quality.

---

### Bước 4: PySpark Bronze ETL (Raw to Parquet Landing)
* **Mục tiêu**: Đọc dữ liệu thô từ landing zone (MinIO) và lưu lại dưới dạng Parquet thô kèm siêu dữ liệu.
* **Hành động thực hiện**:
  1. Airflow kích hoạt tác vụ chạy Spark Job `src/etl/bronze_etl.py` (hoặc submit Spark job).
  2. Spark đọc các tệp JSON từ `s3a://yelp-raw/`.
  3. Áp dụng schema cơ bản (chấp nhận kiểu string cho các cột động để tránh lỗi nạp).
  4. Thêm các cột kiểm toán hệ thống: `_ingested_at` (thời gian nạp), `_source_file` (tên tệp nguồn).
  5. Ghi dữ liệu ra định dạng Parquet nén Snappy tại bucket `s3a://yelp-bronze/`.

---

### Bước 5: PySpark Silver ETL (Data Cleaning & Standardization)
* **Mục tiêu**: Loại bỏ dữ liệu rác, chuẩn hóa kiểu dữ liệu và định dạng.
* **Hành động thực hiện**:
  1. Airflow kích hoạt Spark Job `src/etl/silver_etl.py`.
  2. Đọc các tệp Parquet từ `s3a://yelp-bronze/`.
  3. Thực hiện làm sạch dữ liệu:
     - Loại bỏ trùng lặp (deduplicate) dựa trên ID (ví dụ: `business_id`, `review_id`).
     - Ép kiểu dữ liệu (casting) sang đúng định dạng chuẩn (ví dụ: `stars` $\rightarrow$ double, `review_count` $\rightarrow$ integer, `yelping_since` $\rightarrow$ timestamp).
     - Xử lý các giá trị rỗng/Null (điền giá trị mặc định hoặc loại bỏ hàng lỗi nghiêm trọng).
  4. Ghi dữ liệu sạch ra bucket `s3a://yelp-silver/`.

---

### Bước 6: PySpark Gold ETL (Analytical Modeling / DWH Load)
* **Mục tiêu**: Xây dựng mô hình dữ liệu hình sao (Star Schema) phục vụ phân tích nghiệp vụ và ghi trực tiếp vào PostgreSQL Data Warehouse.
* **Hành động thực hiện**:
  1. Airflow kích hoạt Spark Job `src/etl/gold_etl.py`.
  2. Đọc dữ liệu đã chuẩn hóa từ `s3a://yelp-silver/`.
  3. Thực hiện liên kết (Join) các tập dữ liệu, tính toán các chỉ số tổng hợp (aggregations).
  4. Tạo cấu trúc bảng Dimension và Fact:
     - `dim_business`, `dim_user`, `dim_date`
     - `fact_review`, `fact_checkin`
  5. Sử dụng kết nối JDBC để ghi đè hoặc bổ sung (upsert) các bảng này trực tiếp vào PostgreSQL DWH (`yelp_dwh`).

---

### Bước 7: Xây dựng Materialized Views & Data Marts (SQL Processing)
* **Mục tiêu**: Tạo các bảng tổng hợp sẵn và chế độ xem tối ưu hóa truy vấn cho API và Power BI.
* **Hành động thực hiện**:
  1. Chạy các lệnh SQL DDL trong thư mục `sql/views/` và `sql/indexes/` trên PostgreSQL.
  2. Định nghĩa các Materialized Views như `mv_business_ratings_by_category` (tổng hợp điểm đánh giá trung bình theo loại hình kinh doanh).
  3. Airflow kích hoạt tác vụ SQL gửi lệnh `REFRESH MATERIALIZED VIEW` để cập nhật dữ liệu mới nhất sau khi tầng Gold ETL hoàn tất.

---

### Bước 8: Kiểm định Chất lượng Dữ liệu (Data Quality & Validation Validation)
* **Mục tiêu**: Đảm bảo dữ liệu đạt tiêu chuẩn chất lượng trước khi cung cấp cho các ứng dụng đầu cuối.
* **Hành động thực hiện**:
  1. Airflow kích hoạt chạy script kiểm định `src/validation/validate.py` (sử dụng Great Expectations hoặc kiểm tra SQL tùy biến).
  2. Thực thi các quy tắc kiểm tra chất lượng dữ liệu:
     - `expect_column_values_to_not_be_null` trên các cột khóa chính (`business_id`, `user_id`).
     - `expect_column_values_to_be_between` trên cột `stars` (giá trị phải từ $1.0$ đến $5.0$).
     - Kiểm tra số lượng dòng bản ghi nạp vào không bị sụt giảm bất thường.
  3. **Kết quả**: Xuất báo cáo Data Quality. Nếu phát hiện vi phạm nghiêm trọng, hệ thống sẽ gửi cảnh báo và dừng pipeline/quarantine bản ghi lỗi.

---

### Bước 9: Phục vụ & Trực quan hóa Dữ liệu (Serving Layer)
* **Mục tiêu**: Cung cấp dữ liệu đã xử lý và kiểm định cho người dùng cuối qua API hoặc Dashboard.
* **Hành động thực hiện**:
  1. **FastAPI (REST API)**: Khởi chạy API server để nhận các yêu cầu truy vấn từ frontend:
     ```bash
     make api
     ```
  2. **React Dashboard**: Khởi chạy ứng dụng giao diện người dùng để gọi API và vẽ biểu đồ tương tác:
     ```bash
     cd frontend && npm run dev
     ```
  3. **Power BI**: Kết nối trực tiếp Power BI Desktop tới PostgreSQL (`localhost:5432`, database: `yelp_dwh`), trỏ vào các bảng Gold hoặc Materialized Views để hiển thị báo cáo quản trị.
