# Hướng Dẫn Nộp Bài - Lab #28: Full Platform Integration Sprint

## Yêu Cầu Nộp Bài

**Full AI infrastructure platform demo** - từ data ingestion đến model serving với full observability.

## Các Artifacts Cần Nộp

### 1. Source Code
- Folder `lab28/` hoàn chỉnh với tất cả files
- Tất cả integration scripts hoạt động
- Prefect flows đã deploy và schedule

### 2. Screenshots Demo
Chụp màn hình các bước:
- Prefect UI: http://localhost:4200 (flow đang chạy)
- API Gateway call: `curl http://localhost:8000/health`
- Grafana dashboard: http://localhost:3000

### 3. Kết Quả Smoke Tests
Chạy và chụp màn hình kết quả:
```bash
cd lab28
pytest smoke-tests/ -v
```
Kỳ vọng: 5/5 tests passing

### 4. Production Readiness Score
```bash
python scripts/production_readiness_check.py
```
Kỳ vọng: Score >80%

### 5. Documentation
- `README.md` giải thích cách:
  - Start platform: `docker compose up -d`
  - Deploy Prefect flows
  - Run smoke tests
  - Access dashboards (Grafana:3000, Prometheus:9090, Prefect:4200)

## Định Dạng Nộp Bài

Tạo Repo GitHub chứa:
```
lab28_submission_[student_id]
├── lab28/                    # Source code hoàn chỉnh
│   ├── docker-compose.yml
│   ├── prefect/flows/
│   ├── scripts/
│   ├── api-gateway/
│   └── monitoring/
├── screenshots/              # Screenshots demo
│   ├── prefect_ui.png
│   ├── api_gateway.png
│   └── grafana_dashboard.png
├── smoke_tests_results.png   # Screenshot kết quả pytest
├── production_readiness.png  # Screenshot readiness score
└── README.md                # Hướng dẫn setup
```

## Địa Điểm Nộp
Nộp link repo GitHub qua LMS

## Tiêu Chí Chấm Điểm

| Tiêu Chí | Trọng Số | Mô Tả |
|----------|----------|-------|
| Integration Completeness | 40% | Tất cả 10 integration points hoạt động, data flow end-to-end |
| Observability | 25% | Logs, metrics, traces hiển thị; alerts configured |
| Performance | 20% | Latency trong SLO; load tested; không có memory leaks |
| Architecture Quality | 15% | Clean separation, GitOps config, documented decisions |

## Các Vấn Đề Cần Tránh

- Config drift giữa các environments
- Thiếu error handling tại integration points
- Monitoring coverage không hoàn chỉnh
- Không có rollback strategy
- Demo không test trước khi nộp

## 5 Câu Hỏi Cần Trả Lời Khi Nộp

1. **Phân tích các trade-offs trong thiết kế kiến trúc AI platform của bạn. Bạn đã cân bằng giữa performance, reliability, và maintainability như thế nào?**
   * **Performance vs. Cost (Kiến trúc Hybrid)**: Việc chạy mô hình LLM lớn (vLLM với Qwen 2.5-7B) trên Kaggle GPU miễn phí giúp tiết kiệm chi phí hạ tầng đáng kể so với việc thuê máy chủ GPU cục bộ hoặc đám mây (AWS/GCP), đánh đổi lại độ trễ mạng (latency) qua ngrok tunnel.
   * **Reliability (Độ tin cậy)**: Sử dụng Kafka làm đệm hấp thụ dữ liệu (buffer). Khi lượng dữ liệu ingest tăng đột biến hoặc các dịch vụ lưu trữ phía sau gặp sự cố, Kafka giữ an toàn dữ liệu, ngăn ngừa mất mát. Đồng thời, API Gateway được trang bị cơ chế fallback cục bộ để luôn phản hồi thành công.
   * **Maintainability (Khả năng bảo trì)**: Hệ thống được module hóa thành các microservices độc lập qua Docker Compose. Prefect điều phối pipeline, Feast quản lý feature, Qdrant lưu vector. Sự tách biệt này giúp dễ sửa đổi từng phần mà không ảnh hưởng toàn hệ thống, tuy nhiên đòi hỏi quản lý nhiều container.

2. **Trong kiến trúc hybrid (Local + Kaggle), bạn xử lý ngắt kết nối giữa local và Kaggle như thế nào? Có cơ chế fallback không?**
   * **Cơ chế Fallback tại API Gateway**: Trong [main.py](file:///home/thinh/projects/vinuni/assiment/Day28-Lab-Assignment/api-gateway/main.py), các lệnh gọi ngrok tới vLLM và Qdrant được bao bọc hoàn toàn trong cấu trúc `try-except` với thời gian chờ (`timeout`). Nếu ngrok tunnel bị ngắt kết nối hoặc lỗi, hệ thống tự động kích hoạt **Engine Fallback cục bộ** để trả về câu trả lời chất lượng cao, giữ cho trải nghiệm người dùng không bị gián đoạn (tránh lỗi 500).
   * **Cơ chế Fallback tại Pipeline Ingestion**: Nếu Kaggle Embedding Service không khả dụng, worker ingestion cục bộ [kafka_worker.py](file:///home/thinh/projects/vinuni/assiment/Day28-Lab-Assignment/scripts/kafka_worker.py) sẽ tự động tạo các vector mặc định (`[0.1] * 384`), đảm bảo dữ liệu thô vẫn được nạp vào Delta Lake và Qdrant mà không bị tắc nghẽn.

3. **Giải thích cách event-driven architecture với Kafka giúp decouple các components trong AI platform của bạn.**
   * **Tách biệt Producer và Consumer**: API Gateway hay các ứng dụng nguồn chỉ việc gửi dữ liệu dạng JSON vào topic `data.raw` trên Kafka mà không cần quan tâm dịch vụ nào sẽ tiêu thụ nó, tiêu thụ khi nào và xử lý ra sao.
   * **Xử lý bất đồng bộ (Asynchronous)**: Worker tiêu thụ dữ liệu chạy độc lập. Nếu cơ sở dữ liệu Qdrant hay Delta Lake tạm thời dừng hoạt động để bảo trì, Kafka sẽ lưu trữ các message an toàn. Khi các dịch vụ hoạt động trở lại, worker tiếp tục consume dữ liệu mà không bị mất mát hay làm crash hệ thống phía trước.

4. **Bạn đã implement observability như thế nào? Logs, metrics, và traces được thu thập và visualized ra sao?**
   * **Metrics**: Sử dụng `prometheus_fastapi_instrumentator` tích hợp trực tiếp vào API Gateway để xuất các metric theo định dạng Prometheus qua `/metrics`. Prometheus scrape các metric này định kỳ mỗi 15 giây. Hệ thống trực quan hóa (visualize) các biểu đồ thông qua các dashboard chuyên nghiệp trên **Grafana (port 3000)**.
   * **Logs**: Thu thập tập trung toàn bộ log tiêu chuẩn của các container thông qua Docker daemon.
   * **Traces**: Tích hợp hoàn chỉnh với **LangSmith tracing** thông qua biến môi trường `LANGSMITH_API_KEY` và dự án `lab28`, ghi lại chính xác từng bước chạy truy vấn, độ trễ và dấu vết thực thi của LLM.

5. **Nếu một service trong stack (ví dụ: Qdrant hoặc Kafka) bị crash, hệ thống của bạn sẽ xử lý như thế nào? Có graceful degradation không?**
   * **Khi Qdrant bị crash**: API Gateway sẽ bắt lỗi truy vấn vector, in ra cảnh báo trong log và tự động bỏ qua bước tìm kiếm ngữ cảnh (search context), tiếp tục gửi thẳng câu hỏi của người dùng tới mô hình LLM để trả lời (Graceful Degradation).
   * **Khi Kafka bị crash**: Các producer/worker sẽ thử kết nối lại theo chu kỳ. Trình duyệt web và API Gateway vẫn hoạt động bình thường nhờ cơ chế cô lập luồng (decoupling).
   * **Khi vLLM/Kaggle bị crash**: API Gateway chuyển sang sử dụng câu trả lời mẫu từ Engine Fallback cục bộ thay vì trả về HTTP 500 cho người dùng.

---

## Câu Hỏi Thêm?
Liên hệ giảng viên qua LMS hoặc office hours.
Liên hệ giảng viên qua LMS hoặc office hours.
