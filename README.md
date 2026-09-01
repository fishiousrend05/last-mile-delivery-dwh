# 🚚 Last-Mile Delivery Data Warehouse

Dự án Xây dựng Kho dữ liệu (Data Warehouse) hỗ trợ phân tích hiệu quả giao hàng chặng cuối (Last-Mile Delivery)[cite: 3]. Đây là dự án phục vụ mục tiêu Khóa luận Tốt nghiệp (Ngành Hệ thống Thông tin Quản lý) và Portfolio Data Engineering/Analytics, áp dụng nghiêm ngặt các tiêu chuẩn thiết kế theo phương pháp Kimball[cite: 1, 3].

## 1. Bài toán & Mục tiêu
Trong hoạt động giao hàng chặng cuối, hiệu quả không chỉ nằm ở việc đơn hàng có được giao thành công hay không, mà còn phụ thuộc vào thời gian hoàn thành, tỷ lệ tuân thủ SLA, và tác động của yếu tố ngoại cảnh (thời tiết, địa lý, ngày lễ)[cite: 3]. 

Tuy nhiên, dữ liệu thường nằm rải rác và thiếu tính đồng nhất. Dự án này giải quyết bài toán đó bằng cách:
* Xây dựng nền tảng dữ liệu tập trung, tích hợp dữ liệu thương mại điện tử với yếu tố ngoại cảnh[cite: 3].
* Thiết kế mô hình **Star Schema (Kimball)** áp dụng đồng thời cả 3 loại Fact Table đặc trưng[cite: 1, 2].
* Triển khai **SCD (Slowly Changing Dimension)** để theo dõi lịch sử thay đổi[cite: 1, 2].
* Ứng dụng pipeline tự động hóa (ELT) nhằm chuẩn bị sẵn sàng Data Mart cho việc phân tích đa chiều và trực quan hóa trên BI Dashboard[cite: 3].

---

## 2. Kiến trúc & Công nghệ (Tech Stack)

Hệ thống được thiết kế theo kiến trúc **ELT (Extract - Load - Transform)**[cite: 3].

* **Ingestion (Extract & Load):** Python (Faker, Requests), Orchestration bằng **Prefect**[cite: 3].
* **Transformation:** **dbt (data build tool)**[cite: 1, 3].
* **Data Warehouse:** **PostgreSQL**[cite: 1, 3].
* **Trực quan hóa (BI):** **Power BI**[cite: 3].

### Luồng Dữ Liệu (Layering)
* **`raw/`**: Lưu trữ dữ liệu thô giữ nguyên định dạng nguồn (CSV, API JSON response)[cite: 1].
* **`staging/`**: Làm sạch, chuẩn hóa kiểu dữ liệu, dedupe[cite: 1].
* **`dwh/`**: Chứa mô hình chiều (Star Schema) với các Fact và Dimension tables, sẵn sàng cho công tác báo cáo[cite: 1].

---

## 3. Chiến lược phân loại & Nguồn dữ liệu

Để đảm bảo tính toàn vẹn học thuật và nghiệp vụ, toàn bộ trường dữ liệu trong DWH được phân loại chặt chẽ thành 4 nhóm[cite: 2, 3]:

| Phân loại | Nguồn gốc / Vai trò | Ứng dụng trong dự án |
| :--- | :--- | :--- |
| **REAL** | Lấy trực tiếp từ thực tế (Olist Kaggle Dataset, Open-Meteo API, Nager.Date)[cite: 2]. | Làm cốt lõi cho báo cáo SLA và vòng đời đơn hàng[cite: 3]. |
| **DERIVED** | Dẫn xuất từ dữ liệu REAL thông qua logic SQL/Python rõ ràng (VD: SLA delay, H3 zone mapping)[cite: 2]. | Đóng vai trò là các Metric tính toán chính[cite: 2]. |
| **SYNTHETIC** | Dữ liệu giả lập bằng Faker hoặc mô phỏng xác suất (thông tin tài xế, lý do thất bại, số lần giao)[cite: 2]. | Lấp đầy khoảng trống dữ liệu để thử nghiệm mở rộng bài toán (phân tích driver & failed-delivery)[cite: 2, 3]. |
| **SYSTEM-GEN** | Cột kiểm soát nội bộ do DWH tự sinh (Surrogate keys, SCD tracking metadata)[cite: 2]. | Quản lý cấu trúc DWH[cite: 2]. |

*(Lưu ý: Các phân tích thuộc nhóm SYNTHETIC được tách biệt rõ ràng, chỉ nhằm mục đích kiểm chứng khả năng mở rộng của mô hình kiến trúc, không đại diện cho phát hiện thực tế của thị trường[cite: 3]).*

---

## 4. Mô hình Dữ liệu (Dimensional Modeling)

Điểm nhấn kỹ thuật của dự án là việc triển khai thành công cả 3 loại Fact Table kinh điển theo Kimball và xử lý các chiều dữ liệu phức tạp[cite: 1, 2].

### 4.1. The 3 Fact Tables
1. **`fact_order_lifecycle` (Accumulating Snapshot Fact) - *[Core / REAL]***: 
   * **Grain:** 1 dòng = 1 đơn hàng[cite: 2]. 
   * **Mục đích:** Cập nhật liên tục các mốc thời gian (đặt hàng, duyệt, giao cho đối tác, hoàn thành) và tính toán SLA delay[cite: 1, 2].
2. **`fact_delivery_attempts` (Transaction Fact) - *[Extended / SYNTHETIC]***: 
   * **Grain:** 1 dòng = 1 nỗ lực thử giao hàng[cite: 2]. 
   * **Mục đích:** Ghi nhận chuỗi sự kiện giao hàng (thành công/thất bại) kết hợp mô hình xác suất rủi ro từ thời tiết/khu vực. Bảng này áp dụng **Incremental Load**[cite: 1, 2].
3. **`fact_driver_daily_kpi` (Periodic Snapshot Fact) - *[Extended / SYNTHETIC]***: 
   * **Grain:** 1 dòng = 1 tài xế x 1 ngày lịch[cite: 2]. 
   * **Mục đích:** Đo lường hiệu suất làm việc hàng ngày của tài xế (ngay cả những ngày không phát sinh nỗ lực giao hàng)[cite: 2].

### 4.2. Key Dimension Tables
* **`dim_driver` (SCD Type 2):** Quản lý lịch sử thay đổi thông tin tài xế (khu vực hoạt động, trạng thái)[cite: 1, 2].
* **`dim_zone` (SCD Type 1):** Phân cụm không gian bằng thuật toán **H3 Index (Resolution 5)**, hạ cấp từ SCD2 do ranh giới địa lý thực tế hiếm khi biến động[cite: 1, 2, 3].
* **`dim_date` & `dim_weather`:** Tích hợp dữ liệu thời tiết (Open-Meteo) và phân cấp ngày lễ (Holiday seed file)[cite: 1, 2].

---

## 5. KPI & Năng lực Phân tích (Analytics Capabilities)

Kho dữ liệu này được thiết kế để trực tiếp trả lời các câu hỏi vận hành cốt lõi[cite: 2, 3]:

* **SLA & Bottlenecks:** Khu vực và giai đoạn nào trong vòng đời đơn hàng thường xuyên vi phạm SLA nhất? (Đo lường qua *SLA Breach Rate*, *Average Delivery Lag*)[cite: 2, 3].
* **Tác động ngoại cảnh:** Mức độ nghiêm trọng của thời tiết (Weather Severity Bucket) ảnh hưởng thế nào đến rủi ro giao hàng tại các Zone cụ thể?[cite: 2, 3].
* **Hiệu suất Giao hàng (Mở rộng):** Yếu tố nào làm giảm *First-Attempt Success Rate*? Tài xế có hiệu suất kém do năng lực hay do phải hoạt động trong điều kiện rủi ro (địa lý, thời tiết) cao?[cite: 2, 3].

---

## 6. Roadmap Dự án

- [x] **Phase 0 — Setup:** Khởi tạo kiến trúc dự án, cấu hình môi trường PostgreSQL & Git.
- [ ] **Phase 1 — Source & Ingestion:** Viết Python Data Pipeline (Extractors, Pandera Validators, Loaders) và điều phối bằng Prefect.
- [ ] **Phase 2 — Staging:** Cấu hình dbt, làm sạch và chuẩn hóa dữ liệu tầng raw.
- [ ] **Phase 3 — Dimensions:** Xây dựng `dim_zone` (H3), `dim_date`, `dim_weather`, và `dim_driver` (SCD2 logic)[cite: 1].
- [ ] **Phase 4 — Facts:** Triển khai 3 bảng fact (`fact_order_lifecycle`, `fact_delivery_attempts`, `fact_driver_daily_kpi`) cùng logic Incremental Load[cite: 1].
- [ ] **Phase 5 — Testing & Docs:** Áp dụng dbt tests (unique, not_null, relationship) và sinh Data Dictionary[cite: 1].
- [ ] **Phase 6 — BI Layer:** Xây dựng Dashboard báo cáo hiệu quả vận hành trên Power BI[cite: 1].
