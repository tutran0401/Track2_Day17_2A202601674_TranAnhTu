# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Anh Tú  **Lớp:** E403  **Ngày:** 2026-08-17

> **⚠️ Thứ tự chạy khi chấm — quan trọng.** Phải chạy `make seed-extra` **trước**
> `make verify`. Lý do: `expected/dashboard_baseline.json` được commit sẵn trong
> repo, nên `tools/verify.py` luôn đi vào nhánh đo dashboard; nhánh đó đọc
> `data/gold_events*/`, mà thư mục này chỉ do `make seed-extra` sinh ra và không
> nằm trong Git. Chạy `make verify` trên một bản clone mới mà bỏ qua bước đó sẽ
> gặp `IOException: No files found that match the pattern`. Điều này đúng với cả
> repo gốc — bản gốc trỏ vào `data/gold_events/*.parquet` cũng không tồn tại sau
> `make setup`. Tôi không sửa được vì `tools/verify.py` và `tools/explain.py`
> thuộc danh sách file không được phép chỉnh.
>
> ```bash
> make setup && make seed-extra && make compact && make verify
> ```
>
> **Môi trường.** Windows, không có `make`. Mọi lệnh chạy trực tiếp:
> `.venv\Scripts\python.exe tools\verify.py` (kèm `PYTHONIOENCODING=utf-8`).
> `tools/verify.py` gọi dbt in-process nên kết quả không khác `make verify`.

---

## 0 · Kết quả `make verify`

Output nguyên văn, ba lượt chạy, chạy trên repo đã dọn sạch:

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 95.1s
  run 2/3 … 90.5s
  run 3/3 … 91.2s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 13/13 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```



Tổng kết: **4 / 4 tiêu chí đạt**

---



## 1 · Kích thước bảng training tăng sau mỗi lần chạy


|                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**    | 13.790 hàng thay vì 12.480 ngay ở lượt chạy đầu trên kho sạch; mỗi lần Clear Task lại thừa thêm. Không có lỗi nào được báo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **Nguyên nhân**    | Model khai `materialized='incremental'` nhưng **không khai** `unique_key`. Thiếu khoá, dbt không biết hàng nào là "cùng một hàng", nên câu lệnh nó sinh ra từ lượt thứ hai là `INSERT INTO … SELECT` chứ không phải `MERGE`. `INSERT` không idempotent: chạy lại cùng một partition là *ghi thêm*, không phải *ghi đè* — nên **mọi cơ chế retry ở tầng trên đều biến thành cơ chế nhân bản**. Nguồn còn là CDC có `op='u'`: một ticket tạo ngày D1 rồi sửa ngày D2 mang hai `_ingested_at` khác nhau nên đi qua mệnh đề `WHERE` **hai lần trong cùng một lượt chạy** — đó là 1.310 hàng thừa có sẵn dù chưa retry lần nào. Chi tiết này cũng loại `delete+insert` theo partition ngày: hai lần ghi nằm ở hai partition khác nhau, xoá partition D2 không đụng tới hàng đã ghi ở D1. Grain là **entity**, khoá tự nhiên là `ticket_id`, nên phép ghi phải khoá theo entity chứ không theo ngày. |
| **Cách khắc phục** | `gold_training_set.sql`: `unique_key='ticket_id'` + `incremental_strategy='merge'`, giữ nguyên `WHERE` theo `run_date`. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. `gold/schema.yml`: test `unique`+`not_null` trên `ticket_id`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **Bằng chứng**     | 13.790 → **12.480**, 0 ticket lặp, checksum `8dd7c98653` giống nhau cả 3 lượt.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |


Hai tham số DAG chỉ **giảm tần suất kích hoạt**, không phải root cause: sửa DAG mà
không sửa model thì lượt chạy sạch đầu tiên vẫn cho 13.790 hàng.

---



## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ


|                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**        | 8.645 / 9.100 hàng — thiếu 5,0 %, chỉ thiếu ở ngày cũ. Bảng vẫn `ỔN ĐỊNH ✓`: nó sai một cách nhất quán.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **P99 độ trễ đo được** | **2,726 ngày**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Lookback đã chọn**   | **3 ngày** — P99 làm tròn lên đơn vị partition nhỏ nhất; con số này phủ luôn max quan sát được (2,945 ngày).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| **Nguyên nhân**        | Bộ lọc `where event_date > (select max(event_date) from {{ this }})` lấy mốc là `max(event_date)` — đại lượng trên trục **thời gian sự kiện**, trong khi thứ quyết định "dữ liệu nào vừa xuất hiện trong kho" nằm trên trục **thời gian nạp**. High-water mark đặt nhầm trục, và nó **tự nâng trần của chính nó**: ngay khi một event của 08-16 được nạp, mốc nhảy lên 08-16, nên mọi bản ghi của 08-12 tới muộn sau đó vĩnh viễn không thoả `event_date > 08-16` — chúng không "mới" theo `event_date` dù hoàn toàn mới theo thời điểm nạp. Dữ liệu ấy không lỗi, không bị loại, không ghi log: nó chỉ đơn giản không bao giờ lọt qua `WHERE`. Triệu chứng chỉ hiện ở ngày cũ vì ngày mới luôn có `event_date` lớn hơn mốc trước đó. |
| **Cách khắc phục**     | `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, kèm `unique_key=['event_date','customer_id']` và `incremental_strategy='delete+insert'`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| **Bằng chứng**         | 8.645 → **9.100** (14 ngày × 650 khách hàng), checksum `3db448685c` ×3.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |


**Phân bố** `_ingested_at − event_time` **trên** `bronze_events`**:**


| P50        | P95        | **P99**        | max        | tới muộn > 1 ngày |
| ---------- | ---------- | -------------- | ---------- | ----------------- |
| 0,128 ngày | 1,814 ngày | **2,726 ngày** | 2,945 ngày | 5,05 %            |


Phân bố có **hai cụm tách biệt**, không có gì ở giữa: ~123.000 bản ghi tới trong
0–6 giờ, ~7.000 bản ghi tới ở 43–71 giờ — hai đường nạp khác nhau, nên trung bình
và P50 vô dụng ở đây. Đổi `>` thành `>=` chỉ nới thêm **một** ngày, không chạm tới
cụm 43–71 giờ.

**Vì sao chọn P99 thay vì** `max`**? Chi phí mỗi lựa chọn?**

> `max` là một quan sát đơn lẻ do đúng một bản ghi cá biệt quyết định: một bản ghi
> kẹt 9 ngày vì sự cố mạng sẽ kéo lookback lên 9 ngày vĩnh viễn để phục vụ một
> hàng. P99 là phát biểu về **phân bố**, chấp nhận công khai rằng 1 % chậm nhất bị
> bỏ sót, và đánh đổi đó thành con số kiểm chứng được.
>
> Chi phí không đối xứng: lookback **hụt** thì mất dữ liệu *trong im lặng* — đúng
> như sự cố này. Lookback **thừa** một ngày thì phải tính lại thêm một ngày ở **mọi
> lượt chạy về sau**, mãi mãi, chứ không phải một lần. Ở bảng này mỗi ngày ≈ 650
> cặp nên rẻ; trên bảng lớn đó là ranh giới giữa job 5 phút và job 2 tiếng.
>
> Chính xác về chi phí: toán tử `>=` nên window gồm cả chính mốc, tức mỗi lượt tính
> lại **bốn** partition (`max`…`max-3`). "Lookback 3 ngày" nói về *độ lùi*, không
> phải *số partition đọc lại*.

Window rộng hơn nghĩa là cùng một cặp `(event_date, customer_id)` được tính lại
nhiều lượt — nếu chỉ biết `insert` thì kết quả cộng dồn, tái tạo đúng lỗi nhiệm vụ 1.

---



## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ


|                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**    | Backend đổi `priority` sang chuỗi ngày 08-10. Pipeline **không dừng**, `dbt test` vẫn 9/9 pass, nhưng 6.606 hàng `silver_tickets` có `priority` NULL hoặc ngoài 1..4, và model phân loại dự đoán kém hẳn từ hôm đó.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| **Nguyên nhân**    | Hai lỗ hổng chồng lên nhau, và điều nguy hiểm là chúng **triệt tiêu mọi tín hiệu báo động**. Một: macro dùng `try_cast`, mà `try_cast` được thiết kế để **không bao giờ lỗi** — gặp giá trị không đổi được nó trả `NULL`. Nguồn chuyển sang `'urgent'/'high'/…` thì nó lặng lẽ nghiền toàn bộ thành NULL: không exception, không hàng nào bị loại, số hàng không đổi. Hai: `contract` đang `enforced: false` và không test nào ràng buộc miền giá trị, nên không có gì kiểm tra ở ranh giới Bronze→Silver. Kết quả: một **thay đổi schema không tương thích đi hết pipeline mà không để lại dấu vết** — mất tín hiệu mà không mất lượt chạy, và lỗi chỉ lộ ở đầu kia dưới dạng chất lượng model tụt, nơi khó truy ngược nhất. `try_cast` còn sai theo hướng ngược lại: nó chấp nhận `'0'`, `'5'`, `'-1'` vì chúng đúng là số nguyên, dù contract quy định 1..4. |
| **Cách khắc phục** | **(a)** `normalize_priority.sql`: `CASE` ba nhóm, chuẩn hoá `trim`+`lower` trước khi khớp; `priority_reject_reason` phân biệt 4 loại lỗi. **(b)** `silver_tickets.sql`: **lọc trước, xếp hạng sau** (CTE `valid` đặt trước `row_number()`). **(c)** `quarantine_tickets.sql`: `where normalize_priority(priority_raw) is null` — dùng đúng macro kia nên hai model không thể lệch. **(d)** `silver/schema.yml`: `contract.enforced: true` + `not_null` + `accepted_values [1,2,3,4]`.                                                                                                                                                                                                                                                                                                                                                                           |
| **Bằng chứng**     | `quarantine_tickets` = **312** (checksum `ebb89036fb` ×3) · `dbt test` **13/13** (gốc 9) · `priority ∈ 1..4` sạch · `silver_tickets` vẫn đủ **12.480** ticket.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |


**Ba nhóm giá trị và cách xử lý:**


| Nhóm             | Giá trị                                                                  | Số bản ghi | Xử lý                                                                                  |
| ---------------- | ------------------------------------------------------------------------ | ---------- | -------------------------------------------------------------------------------------- |
| 1 · số hợp lệ    | `'1' '2' '3' '4'`                                                        | 6.846      | giữ nguyên                                                                             |
| 2 · nhãn chuỗi   | `'urgent' 'high' 'medium' 'low'`                                         | 7.142      | **map** `urgent=1…low=4` — schema evolution: đổi *cách biểu diễn*, *ý nghĩa không đổi* |
| 3 · không hợp lệ | `'0'`49 `''`43 `'P1'`39 `'unknown'`39 `'P2'`38 `'5'`37 `NULL`35 `'-1'`32 | **312**    | **quarantine**                                                                         |


Tiêu chí phân biệt nhóm 2 và 3: *giá trị này có mang đúng thông tin của contract cũ,
chỉ khác cách biểu diễn không?* Xử lý nhóm 2 như nhóm 3 sẽ vứt 7.142 bản ghi hợp lệ.

**Vì sao thứ tự lọc/xếp hạng quyết định số hàng.** Nếu chỉ thêm điều kiện lọc vào
*cuối*, ticket nào có bản ghi **mới nhất** bị hỏng sẽ mất khỏi Silver — 12.480 tụt
xuống 12.168. Lọc **trước** rồi mới xếp hạng: ta loại *bản ghi*, không loại *ticket*,
ticket đó vẫn còn trạng thái hợp lệ từ lần cập nhật trước.

**Vì sao cần cả** `contract` **lẫn** `test`**.** `contract` ràng buộc **kiểu**: bảng đích
được tạo đúng `data_type` khai báo nên schema vật lý không trôi. Nó **không** ràng
buộc **miền giá trị** — `priority = 99` vẫn qua vì 99 đúng là integer. Thêm nữa,
dbt-duckdb ghi bằng `INSERT … SELECT` và DuckDB **ép kiểu ngầm**, nên biểu thức
VARCHAR/BIGINT ép được vẫn lọt; chỉ giá trị không ép được mới làm dừng model. Vậy
`contract` là hàng rào cho *schema đích*, không phải cho *biểu thức nguồn*.

**Câu hỏi thiết kế: chặn ở Bronze hay Silver? Vì sao không để pipeline dừng?**

> **Chặn ở Silver.** Bronze là bản sao trung thực của nguồn — giá trị lớn nhất của
> nó là *ghi lại đúng những gì nguồn đã gửi*. Nếu Bronze từ chối bản ghi lỗi thì
> bằng chứng bị huỷ ngay tại chỗ: không dựng lại được mốc 08-10, không đối chất
> được với backend, không replay được sau khi sửa logic. Ranh giới đúng để áp
> contract là chỗ dữ liệu chuyển từ *"những gì đã nhận"* sang *"những gì đã được
> khẳng định là đúng"* — tức Bronze→Silver.
>
> **Không dừng DAG,** vì cân đối quy mô: 312 / 14.300 bản ghi = 2,2 %. Dừng nghĩa
> là để 2,2 % dữ liệu hỏng chặn 12.480 ticket, 130.000 event và 31.200 chunk bình
> thường — đổi một sự cố chất lượng cục bộ lấy một sự cố ngừng dịch vụ toàn phần.
> Cách đúng là **định tuyến**: pipeline chạy tiếp, bản ghi hỏng rơi vào hàng đợi
> có tên và có lý do, ở đó nó **hữu hạn, đếm được, theo dõi được** — 312 là con số
> ai cũng kiểm tra được, khác hẳn "có gì đó sai" của tình trạng ban đầu.

---



## 4 · Bài mở rộng



### Bài A — Query dashboard chậm


|                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Nguyên nhân**    | Hai lỗi độc lập. **(1) Small-file problem:** 5.000 file Parquet tí hon, không partition. DuckDB đọc theo lô và làm tròn lên theo *từng file*, nên 5.000 file × ~1.000 = 5.000.000 đơn vị công quét cho tập chỉ có 130.683 hàng — gấp 38 lần. Đây là kiểu suy giảm *tích luỹ mà không cần ai sửa code*: chi phí tăng theo **số file**, không theo lượng dữ liệu. **(2) Predicate không sargable:** `strftime(event_time,'%Y-%m-%d') = '…'` bọc cột trong function call; engine không so được kết quả hàm với tên thư mục partition hay min/max của row group, nên buộc phải mở toàn bộ file rồi mới biết file nào có ích. |
| **Cách khắc phục** | `compact.py`: `COPY … PARTITION_BY (event_date)`, `ORDER BY customer_name, event_time`, `ROW_GROUP_SIZE 5000`. `dashboard.sql`: trỏ `gold_events_v2`, `hive_partitioning=true`, `event_date = date '2026-08-09'`.                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Bằng chứng**     | rows scanned **5.000.000 → 9.324** (**536,3×**) · files **5.000 → 14** · hash `4379e4c5d9f3` **không đổi**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |


Ba quyết định layout: **partition theo** `event_date` (14 giá trị → 14 thư mục; nếu
partition theo `customer_name` 650 giá trị thì small-file problem được dựng lại dưới
dạng khác) · **sort theo** `customer_name` để min/max row group phủ dải hẹp, vì việc
lọc theo ngày đã do partition lo · `row_group_size 5000` vì 130.683/14 ≈ 9.300
hàng mỗi ngày, mặc định 122.880 gói trọn một ngày vào *một* row group nên min/max
phủ toàn bộ miền và không lọc được gì.

> **Bẫy gặp phải:** lần đầu tôi viết `select *, cast(event_time as date) as event_date`.
> Nguồn **đã có sẵn** cột `event_date`, nên DuckDB đổi tên cột dẫn xuất thành
> `event_date_1` và partition theo nó; truy vấn lọc `event_date` khi ấy đọc cột
> trong **file** chứ không đọc đường dẫn → pruning không kích hoạt, chỉ giảm 35,7×.
> Con số đó vẫn "đạt" ngưỡng 10× nên lỗi rất dễ lọt nếu chỉ nhìn ✓/✗.



### Bài B — Consumer gặp sự cố giữa batch


|                    |                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Nguyên nhân**    | `consume()` gọi `commit()` **trước** `write_batch()`. Chết ở giữa hai lệnh: offset đã dịch qua lô hiện tại nhưng dữ liệu chưa vào kho, lần khởi động lại đọc từ lô *sau*, lô đang dở **mất vĩnh viễn**. Đó là **at-most-once**, và nó mất trong im lặng — consumer restart bình thường, chỉ có số hàng cuối là thiếu. Không phải bug trong một dòng code, mà là hệ quả của *thứ tự* hai thao tác không nguyên tử với nhau. |
| **Cách khắc phục** | Đảo thành **ghi trước, commit sau** (at-least-once) + `write_batch` idempotent bằng `insert … on conflict (event_id) do update set …`, kèm `event_id varchar primary key`. Nạp lô qua bảng tạm rồi UPSERT bằng **một** câu lệnh (executemany trên câu có `ON CONFLICT` chạy từng dòng, đo được ~7 ms/hàng).                                                                                                                |
| **Bằng chứng**     | `crash-test`: **ĐẠT ✓** — A ghi 20.000/20.000 event_id; B chết ở lô 7, offset dừng ở **3.000** (không phải 3.500 → đúng dấu hiệu offset chưa dịch qua lô dở); C ghi lại **17.000** message, bảng vẫn đúng 20.000/20.000. 500 message phát lại bị UPSERT hấp thụ.                                                                                                                                                           |


**At-most-once / at-least-once / idempotent write.** At-most-once (commit trước):
crash → mất, không bao giờ trùng. At-least-once (ghi trước): crash → không mất, đổi
lại có trùng. **Exactly-once không tồn tại ở tầng giao vận** — không thể làm "ghi dữ
liệu" và "dịch offset" nguyên tử với nhau khi chúng ở hai hệ thống khác nhau. Thứ
chọn được là at-least-once **cộng** một phép ghi idempotent: cho phép message tới hai
lần và làm lần thứ hai không đổi trạng thái, cho **hiệu ứng quan sát được** tương
đương exactly-once mà không cần một đảm bảo không tồn tại.

`DO UPDATE` **vs** `DO NOTHING` **khi replay với nội dung đã đổi.** Với message không
đổi thì hai lựa chọn như nhau. Khác biệt chỉ lộ khi nguồn sửa bản ghi rồi phát lại
**cùng** `event_id` **với nội dung mới**: `DO NOTHING` giữ bản cũ và kho **vĩnh viễn lệch
với nguồn** — lệch im lặng, không lỗi nào được báo. `DO UPDATE` ghi đè bằng bản mới
nhất (last-write-wins). Tôi chọn `DO UPDATE` vì nó đúng trong cả hai trường hợp;
`DO NOTHING` chỉ đúng khi chắc chắn message bất biến — giả định hiếm khi đúng với CDC.

---



## 5 · Tổng kết


| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên                                                                                                                                                                              |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1        | Chạy pipeline **hai lần liên tiếp** rồi so số hàng và checksum — phép thử rẻ nhất phân biệt "chạy được" với "chạy lại được". Với mỗi model incremental: *khoá là gì, dbt sinh* `INSERT` *hay* `MERGE`*?* Đọc `target/run/…` để biết chắc thay vì đoán. |
| 2        | Với mọi bộ lọc incremental: **mốc so sánh nằm trên trục thời gian nào** — sự kiện hay nạp? Rồi đo phân bố `_ingested_at − event_time` và lấy P99. Mốc đặt trên trục sự kiện sẽ mất dữ liệu tới muộn mà không báo lỗi bao giờ.                          |
| 3        | Xem phân bố giá trị các cột khoá nghiệp vụ và tìm chỗ `NULL` chiếm tỷ lệ bất thường — `NULL` thường là dấu vết của một phép ép kiểu *không chịu báo lỗi*. `dbt test` xanh **không** nghĩa là dữ liệu đúng, chỉ nghĩa là những test đã viết đều pass.   |




### Bảng tự chấm nhanh


|                                          | Của tôi                    | Kỳ vọng        | ✓   |
| ---------------------------------------- | -------------------------- | -------------- | --- |
| `gold_training_set` — số hàng / ổn định  | 12.480 · `8dd7c98653` ×3   | 12.480 · ✓     | ✓   |
| `gold_feature_daily` — số hàng / ổn định | 9.100 · `3db448685c` ×3    | 9.100 · ✓      | ✓   |
| `gold_doc_chunks` — số hàng              | 31.200                     | 31.200         | ✓   |
| `quarantine_tickets` — số hàng           | 312                        | 312            | ✓   |
| `silver_tickets` — số ticket             | 12.480                     | 12.480         | ✓   |
| `dbt test`                               | 13/13 pass                 | pass, > 9 test | ✓   |
| P99 độ trễ đo được                       | **2,726 ngày**             | (ghi số)       | ✓   |
| **Tổng verify**                          | 4/4                        | 4/4            | ✓   |
| *(mở rộng A)* rows scanned               | 5.000.000 → 9.324 (536,3×) | ≥ 10×          | ✓   |
| *(mở rộng B)* `crash-test`               | ĐẠT                        | ĐẠT            | ✓   |




### File đã sửa


| File                                       | Thay đổi                                                                             |
| ------------------------------------------ | ------------------------------------------------------------------------------------ |
| `dbt/models/gold/gold_training_set.sql`    | `unique_key='ticket_id'`, `incremental_strategy='merge'`                             |
| `dbt/models/gold/gold_feature_daily.sql`   | lookback 3 ngày, `unique_key=['event_date','customer_id']`, `delete+insert`          |
| `dbt/models/gold/schema.yml`               | `unique`+`not_null` trên `gold_training_set.ticket_id`                               |
| `dbt/macros/normalize_priority.sql`        | `CASE` ba nhóm; `priority_reject_reason` phân loại 4 kiểu lỗi                        |
| `dbt/models/silver/silver_tickets.sql`     | lọc bản ghi hỏng **trước** `row_number()`                                            |
| `dbt/models/silver/quarantine_tickets.sql` | `where normalize_priority(priority_raw) is null`                                     |
| `dbt/models/silver/schema.yml`             | `contract.enforced: true`; `not_null` + `accepted_values [1,2,3,4]`                  |
| `dags/ai_training_pipeline.py`             | `catchup=False`, `max_active_runs=1`                                                 |
| `tools/compact.py`                         | *(A)* `PARTITION_BY(event_date)`, sort, `ROW_GROUP_SIZE 5000`, assert không mất hàng |
| `queries/dashboard.sql`                    | *(A)* `gold_events_v2`, `hive_partitioning`, predicate sargable                      |
| `ingest/consumer.py`                       | *(B)* ghi trước/commit sau; UPSERT qua bảng tạm; `event_id` là `primary key`         |


Không sửa `expected/`, `seed/generate.py`, `tools/verify.py`, `tools/explain.py`,
`tools/common.py`.

### Giới hạn đã biết

Không sai trên dữ liệu của lab, nhưng đúng là điểm yếu — ghi lại để không ai tưởng
đã xử lý:

1. `merge` **không xoá.** Ticket bị `op='d'` *sau* khi đã vào Gold sẽ biến mất khỏi
  Silver, nên không còn hàng nguồn nào xoá được hàng cũ trong Gold. `unique_key` làm
   cập nhật idempotent, nó không cài đặt ngữ nghĩa xoá của CDC. *(Seed luôn xoá ngay
   trong ngày tạo, trước khi ticket vào Gold.)*
2. **Tombstone không payload sẽ bị quarantine nhầm**, vì bộ lọc `priority is not null`
  đặt trước bước xếp hạng chưa miễn trừ `op='d'`. *(Bản ghi* `op='d'` *trong seed vẫn
   mang* `priority` *hợp lệ.)*
3. **Lookback không tự phục hồi:** bảng đích *tồn tại mà rỗng* làm `max(event_date)`
  NULL, `event_date >= NULL` không khớp gì và bảng đứng yên mãi. *(*`verify` *luôn xoá
   kho trước nên luôn đi nhánh "bảng chưa tồn tại".)*
4. `consumer.py`**:** `create table if not exists` không thêm `primary key` vào bảng
  cũ; `UPSERT` ghi đè vô điều kiện nên bản phát lại *cũ* tới sau sẽ đè bản mới hơn;
   offset ghi read-modify-write không khoá nên hai consumer song song đè offset nhau.

