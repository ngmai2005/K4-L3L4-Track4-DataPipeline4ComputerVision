# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Hồ Ngọc Mai` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `02509` |
| GitHub | `ngmai2005` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04868` |

## Kết quả cuối

- `dataset_version` của bạn: `10c3610736ab`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `725 / 150 / 144`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Thời gian: A dùng ts_utc dạng ISO 8601; B dùng timestamp_ms dạng epoch mili giây.
Tốc độ: A dùng speed_mps (m/s); B dùng spd_kmh (km/h).
Chân ga: A dùng gas dạng tỷ lệ 0–1, còn B dùng throttle_pct dạng phần trăm 0–100.
Tên cột: A dùng lat, lon, acc_x, steer; B dùng latitude, longitude, ax, steering.
Mỗi nguồn có một bảng bronze để giữ nguyên cấu trúc, dễ truy vết lỗi và xử lý lại.
Các cột dữ liệu nguồn giữ dạng chữ để tránh mất dữ liệu hoặc ép kiểu sai; đến silver mới chuẩn hóa, ép kiểu và hợp nhất.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.


Hai nguồn cần được chuẩn hóa: B đổi tốc độ từ km/h sang m/s bằng cách chia 3,6, ga/phanh phần trăm chia 100, rồi ánh xạ tên cột về schema chung.
Thời gian A được parse từ ISO 8601, B từ epoch mili giây; cả hai đưa về timestamp UTC.
Độ lệch đồng hồ được điều chỉnh theo offset của từng nguồn.
Nguồn B bị lệch đồng hồ 1,5 giây; pipeline trừ offset này sau khi đổi timestamp_ms sang UTC. Bằng chứng là sample đầu của B sau hiệu chỉnh là 2025-10-08 14:30:00.009+00:00, khớp thời điểm đầu video trong sai số 0,5 giây.
Dòng lỗi vào rejects để giữ dữ liệu gốc, drive và lý do lỗi, giúp kiểm tra, sửa và chạy lại; xoá sẽ mất dấu vết và không đối soát được số dòng.
Sau làm sạch, A còn 2968 dòng và B còn 3181 dòng; mỗi drive có 53 dòng vào rejects
| Drive | Lý do | Số dòng |
|---|---|---:|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test max_gap_ms được đặt ở mức warn vì khoảng cách thời gian giữa các mẫu vượt ngưỡng chỉ cảnh báo nguy cơ gián đoạn dữ liệu, chưa nhất thiết làm dữ liệu không hợp lệ. Trong khi đó, in_range là error vì giá trị vượt phạm vi cho phép có thể ảnh hưởng trực tiếp đến độ chính xác và độ tin cậy của dữ liệu.
Trong thí nghiệm break-gate, test in_range_silver_telemetry_speed_mps__70__0 phát hiện 6 dòng vi phạm, khiến cổng chất lượng thất bại (gate_failed = true). Kết quả khi nới ngưỡng là PASS=5, WARN=1, ERROR=1; sau khi khôi phục là PASS=6, WARN=1, ERROR=0.
Điều này cho thấy quality gate không chỉ hiển thị cảnh báo mà còn chặn dữ liệu sai nghiêm trọng đi tiếp sang Gold/warehouse, trong khi vẫn cho phép pipeline tiếp tục với các cảnh báo cần theo dõi.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Trong thí nghiệm idempotency, số dòng dữ liệu trước khi chạy lại là 1.246 dòng và sau hai lần chạy lại vẫn là 1.246 dòng. Fingerprint không thay đổi (6c7480dfde7e), chứng minh pipeline có tính idempotency. Partition trip-00003 có 60 frames; khi dùng append và chạy hai lần, số dòng thành 120, cho thấy dữ liệu bị trùng.
Delta overwrite theo partition an toàn hơn xóa rồi ghi lại vì hỗ trợ giao dịch ACID và tính nguyên tử (atomicity). Nếu xảy ra lỗi giữa chừng, giao dịch chưa commit sẽ không làm mất dữ liệu cũ. Ngược lại, thao tác xóa trước rồi ghi lại có nguy cơ mất dữ liệu nếu quá trình ghi thất bại.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?
Khung hình đen (black frame) gần như không chứa thông tin hình ảnh, thường do camera bị che hoặc lỗi ghi hình. Trong khi đó, khung hình tối (dark frame) có độ sáng thấp nhưng vẫn chứa thông tin hữu ích, chẳng hạn cảnh quay ban đêm. Tôi đồng ý với việc pipeline đánh dấu hoặc loại bỏ khung hình đen, nhưng giữ lại khung hình tối để tránh mất dữ liệu quan trọng.
Dựa vào kết quả threshold.json, số khung hình được coi là trùng ở từng ngưỡng:
| dHash khác tối đa | Số khung trùng, drive A | Số khung trùng, drive B |
|---|---:|---:|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |
Tôi chọn ngưỡng 2, cũng là ngưỡng đang được sử dụng trong pipeline, vì giúp cân bằng giữa phát hiện khung hình gần trùng và hạn chế loại bỏ nhầm dữ liệu hữu ích. Ngưỡng 0 quá nghiêm ngặt, còn ngưỡng 6 có nguy cơ loại bỏ nhiều khung hình chứa thay đổi thực tế.
Ngưỡng này phụ thuộc vào tốc độ xe và FPS. Khi xe chạy chậm, các khung hình liên tiếp thường giống nhau hơn; khi xe chạy nhanh, cảnh vật thay đổi nhiều hơn. Vì vậy, cần điều chỉnh ngưỡng dựa trên tốc độ và điều kiện ghi hình để tránh mất thông tin.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Thí nghiệm có 1.019 khung hình thuộc 26 trip. Khi chia dữ liệu theo từng khung hình, cả 26/26 trip đều bị chia cắt, đồng thời xuất hiện trong cả tập train và test. Ngược lại, khi chia theo trip, 0 trip bị chia cắt.
Việc chia theo frame gây data leakage vì các khung hình trong cùng một chuyến đi có bối cảnh, cảnh vật và điều kiện ánh sáng tương tự nhau. Mô hình có thể ghi nhớ những đặc điểm đã xuất hiện trong tập train, khiến điểm kiểm thử cao một cách giả tạo và không phản ánh đúng khả năng tổng quát hóa trên chuyến đi mới. Vì vậy, tôi chọn chia theo trip để đảm bảo đánh giá khách quan.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

dataset_version được tạo bằng cách băm nội dung thay vì sử dụng thời gian hoặc số đếm, nhằm định danh phiên bản theo dữ liệu và các thành phần tham gia tính hash. Điều này giúp kiểm tra tính nhất quán và khả năng tái lập.
Dataset của tôi có dataset_version = 10c3610736ab và manifest_sha256 = 73987ad3e4b974cd55c0cf774f134fd035da99283569dbab3c4029c4535b1f7e.
Hai lần export liên tiếp với cùng đầu vào và MSSV 02509 đều cho dataset_version = 10c3610736ab. MSSV tham gia cách phân split theo trip; split thay đổi sẽ thay đổi nội dung dataset và do đó có thể tạo version khác với bạn bè.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Theo manifest.json, dataset được tạo từ các bảng Bronze raw_telemetry_a (version 0), raw_telemetry_b (version 0), trips_raw (version 0), video_files (version 1) và các bảng Silver frames (version 51), telemetry (version 0), frames_flagged (version 1), frame_telemetry (version 1).
Pipeline loại bỏ các khung hình có cờ is_black, is_blurry, is_near_dup hoặc không đồng bộ (not sync_ok). Sau đó chia dữ liệu theo trip, phân tầng theo drive và sắp xếp bằng MD5.
Kết quả tạo ra 1.019 khung hình, gồm 725 train, 150 validation và 144 test, với dataset_version = 10c3610736ab.

Nếu cần tái tạo dataset sau 3 tháng, các thông tin quan trọng là phiên bản từng bảng nguồn, SHA-256 của video đầu vào, quy tắc lọc và chia dữ liệu, mã nguồn pipeline, cấu hình xử lý và hash của manifest. Những thông tin này giúp xác định đúng dữ liệu nguồn và kiểm tra kết quả tái tạo.
Bằng chứng từ Metabase
Ảnh dashboard cũng xác nhận cách chia theo trip:
| Tập dữ liệu | Khung hình | Trip | Drive |
|---|---:|---:|---:|
| Train | 725 | 18 | 2 |
| Validation | 150 | 4 | 2 |
| Test | 144 | 4 | 2 |
| Tổng | 1.019 | 26 | 2 |
Dashboard thể hiện dữ liệu đã được phân chia thành ba tập mà không chia sẻ trip giữa các tập.
## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Pipeline được thiết kế để xử lý lỗi tạm thời bằng cơ chế retry. Tôi chủ động tạo lỗi một lần tại trip-04868; task silver_frames thất bại và các bước phụ thuộc bị chặn. Sau khi clear task lỗi, Airflow chạy lại task đó thành công và các bước downstream tiếp tục chạy đến khi toàn bộ DAG hoàn thành. Điều này cho thấy pipeline có thể phục hồi mà không cần chạy lại toàn bộ quy trình.
Có 1 mapped task của silver_frames thất bại (map index 13, trip-04868) và 7 task downstream ở trạng thái upstream_failed. Tôi clear task lỗi cùng các bước downstream bị chặn, sau đó DAG chạy lại toàn bộ thành công. Dữ liệu không bị nhân đôi; bằng chứng là thí nghiệm idempotency vẫn giữ 1.246 frames và fingerprint không đổi. dataset_version sau phục hồi vẫn là 10c3610736ab. Bằng chứng là fail_task_log.png và retry_success_graph.png.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. Chuyển video lên Object Storage (MinIO/S3) bằng multipart upload; dùng Kafka hoặc message queue cho telemetry, metadata và sự kiện file đã sẵn sàng để xử lý.
2. Xử lý video và telemetry: Xử lý tuần tự bằng Python/OpenCV trên một máy sẽ gặp giới hạn CPU, RAM và thời gian xử lý. Giải pháp là sử dụng Spark hoặc Ray để xử lý phân tán, kết hợp nhiều worker CPU/GPU nhằm tăng khả năng xử lý song song.
3. Điều phối pipeline: Airflow chạy với cấu hình đơn máy có thể bị nghẽn khi số lượng trip và task tăng cao. Giải pháp là sử dụng CeleryExecutor hoặc KubernetesExecutor, cho phép phân phối task, mở rộng worker và tự động chạy lại khi gặp lỗi.
Kết luận: Để phục vụ 500 xe/ngày, pipeline cần chuyển từ kiến trúc xử lý tập trung sang kiến trúc có lưu trữ mở rộng, xử lý phân tán và điều phối linh hoạt, đồng thời duy trì quality gate, idempotency và data lineage để đảm bảo chất lượng dữ liệu.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

SELECT split,
       COUNT(*) AS total_frames,
       COUNT(DISTINCT trip_id) AS total_trips
FROM gold.dataset_manifest
GROUP BY split
ORDER BY split;
Kết quả là 725 khung train (18 trip), 150 khung validation (4 trip) và 144 khung test (4 trip). Qua đó, có thể đánh giá mức độ cân bằng dữ liệu và hỗ trợ kiểm tra việc chia dataset theo trip nhằm hạn chế data leakage.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều tôi bất ngờ nhất là khi chia dữ liệu theo frame, cả **26/26 trip đều bị chia cắt**, gây data leakage và có thể khiến kết quả đánh giá mô hình cao giả tạo. Tôi cũng nhận thấy tầm quan trọng của quality gate và idempotency trong việc đảm bảo chất lượng dữ liệu.

Điều tôi còn chưa rõ là cách lựa chọn ngưỡng phát hiện khung hình trùng lặp tối ưu khi tốc độ xe và điều kiện ánh sáng thay đổi.

Một lỗi pipeline có thể chưa phát hiện được là **camera bị lệch góc hoặc hình ảnh bị che khuất một phần** nhưng vẫn vượt qua các bước kiểm tra chất lượng hiện tại.
