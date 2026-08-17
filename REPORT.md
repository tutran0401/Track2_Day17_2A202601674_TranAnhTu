# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Anh Tú  **Lớp:** E403  **Ngày:** 2026-08-17

> Chạy `make seed-extra` **trước** `make verify`: `expected/dashboard_baseline.json`
> có sẵn trong repo nên `verify.py` luôn đo dashboard, mà dữ liệu Parquet không nằm
> trong Git. Máy làm bài chạy Windows, không có `make`, nên các lệnh được gọi thẳng
> qua `.venv\Scripts\python.exe tools\<script>.py`.

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lần chạy</summary>

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

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | 13.790 hàng thay vì 12.480 ngay ở lượt chạy đầu trên kho sạch; mỗi lần Clear Task lại thừa thêm, không lỗi nào được báo. |
| **Nguyên nhân** | Incremental model không khai `unique_key`, nên dbt không có cách nhận ra hàng nào là "cùng một hàng" và sinh ra `INSERT INTO … SELECT` chứ không phải `MERGE`: chạy lại cùng một partition là *ghi thêm* thay vì *ghi đè*, nên mọi cơ chế retry ở tầng trên — Clear Task, retry của scheduler, backfill — đều biến thành cơ chế nhân bản. Nguồn còn là CDC có `op='u'`, nên một ticket tạo ngày D1 rồi sửa ngày D2 mang hai `_ingested_at` khác nhau và đi qua mệnh đề `WHERE` hai lần trong cùng một lượt chạy, ở hai partition ngày khác nhau — vì thế `delete+insert` theo partition ngày cũng không cứu được, phải khoá theo entity. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` + `incremental_strategy='merge'`, giữ nguyên `WHERE` theo `run_date`. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1` (chỉ giảm tần suất kích hoạt, không phải root cause). `dbt/models/gold/schema.yml`: thêm test `unique`+`not_null` trên `ticket_id`. |
| **Bằng chứng** | trước: 13.790 hàng (1.310 ticket lặp) · sau: 12.480 hàng, 0 ticket lặp · checksum 3 lượt: `8dd7c98653` / `8dd7c98653` / `8dd7c98653` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | 8.645 / 9.100 hàng — thiếu 5,0 %, chỉ thiếu ở những ngày đã chạy xong từ lâu. Bảng vẫn `ỔN ĐỊNH ✓`: nó sai một cách nhất quán. |
| **P99 độ trễ đo được** | **2,726 ngày** *(P50 0,128 · P95 1,814 · max 2,945 · 5,05 % tới muộn hơn một ngày)* |
| **Lookback đã chọn** | **3 ngày** — P99 làm tròn lên đơn vị partition nhỏ nhất; con số này phủ luôn max quan sát được. |
| **Nguyên nhân** | Bộ lọc `where event_date > (select max(event_date) from {{ this }})` lấy mốc là `max(event_date)`, một đại lượng trên trục **thời gian sự kiện**, trong khi thứ quyết định dữ liệu nào vừa xuất hiện trong kho lại nằm trên trục **thời gian nạp** — high-water mark đặt nhầm trục, và nó tự nâng trần của chính nó: ngay khi một event của 08-16 được nạp, mốc nhảy lên 08-16 nên mọi bản ghi của 08-12 tới muộn sau đó vĩnh viễn không thoả `event_date > 08-16`, dù chúng hoàn toàn mới theo thời điểm nạp. Dữ liệu ấy không lỗi, không bị loại, không ghi log — nó chỉ không bao giờ lọt qua `WHERE`; triệu chứng chỉ hiện ở ngày cũ vì ngày mới luôn có `event_date` lớn hơn mốc trước đó nên che mất phần đuôi. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, kèm `unique_key=['event_date','customer_id']` và `incremental_strategy='delete+insert'` để lần tính sau thay thế lần trước thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (14 ngày × 650 khách hàng) · checksum `3db448685c` giống nhau cả 3 lượt |

Phân bố `_ingested_at − event_time` có **hai cụm tách biệt**, không có gì ở giữa:
~123.000 bản ghi tới trong 0–6 giờ, ~7.000 bản ghi tới ở 43–71 giờ. Đổi `>` thành
`>=` chỉ nới thêm một ngày nên không chạm tới cụm thứ hai.

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là một quan sát đơn lẻ do đúng một bản ghi cá biệt quyết định: một bản ghi
> kẹt 9 ngày vì sự cố mạng sẽ kéo lookback lên 9 ngày vĩnh viễn để phục vụ một hàng.
> P99 là phát biểu về **phân bố**, chấp nhận công khai rằng 1 % chậm nhất bị bỏ sót
> và biến đánh đổi đó thành con số kiểm chứng được.
>
> Chi phí không đối xứng: lookback **hụt** thì mất dữ liệu *trong im lặng* — đúng
> như sự cố này; lookback **thừa** một ngày thì phải tính lại thêm một ngày ở **mọi
> lượt chạy về sau**, mãi mãi, chứ không phải một lần. Ở bảng này mỗi ngày ≈ 650 cặp
> nên rẻ, trên bảng lớn đó là ranh giới giữa job 5 phút và job 2 tiếng. Nói chính
> xác: toán tử `>=` khiến mỗi lượt tính lại **bốn** partition (`max`…`max-3`) —
> "lookback 3 ngày" nói về độ lùi, không phải số partition đọc lại.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Backend đổi `priority` sang chuỗi ngày 08-10. Pipeline không dừng, `dbt test` vẫn 9/9 pass, nhưng 6.606 hàng `silver_tickets` có `priority` NULL hoặc ngoài 1..4 và model phân loại dự đoán kém hẳn từ hôm đó. |
| **Nguyên nhân** | Macro chuẩn hoá dùng `try_cast`, mà `try_cast` được thiết kế để **không bao giờ lỗi** — gặp giá trị không đổi được nó trả `NULL`; khi nguồn chuyển sang nhãn chữ, nó nghiền toàn bộ thành NULL mà không có exception, không hàng nào bị loại, số hàng không đổi. Cùng lúc `contract` để `enforced: false` và không test nào ràng buộc miền giá trị, nên không có gì kiểm tra ở ranh giới Bronze→Silver. Hai lỗ hổng cộng lại làm một thay đổi schema không tương thích đi hết pipeline mà **không để lại dấu vết nào** — mất tín hiệu mà không mất lượt chạy, và lỗi chỉ lộ ở đầu kia dưới dạng chất lượng model tụt, nơi khó truy ngược nhất. `try_cast` còn sai theo hướng ngược lại: nó chấp nhận `'0'`, `'5'`, `'-1'` vì chúng đúng là số nguyên, dù contract quy định 1..4. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **(1) Số hợp lệ** `'1'…'4'` — 6.846 bản ghi, đúng contract cũ → giữ nguyên. **(2) Nhãn chuỗi** `urgent/high/medium/low` — 7.142 bản ghi, **schema evolution**: đổi *cách biểu diễn*, *ý nghĩa không đổi* → map về 1..4 theo tài liệu API. **(3) Không hợp lệ** `'0'`49 `''`43 `'P1'`39 `'unknown'`39 `'P2'`38 `'5'`37 `NULL`35 `'-1'`32 = **312** → quarantine. Tiêu chí phân biệt nhóm 2 và 3: *giá trị này có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn không?* Xử lý nhóm 2 như nhóm 3 sẽ vứt 7.142 bản ghi hợp lệ. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: `CASE` ba nhóm (chuẩn hoá `trim`+`lower` trước khi khớp), `priority_reject_reason` phân biệt 4 loại lỗi. `dbt/models/silver/silver_tickets.sql`: thêm CTE `cleaned`/`valid` để lọc bản ghi hỏng **trước** `row_number()` — lọc sau sẽ xoá cả ticket có bản ghi mới nhất bị hỏng và làm số ticket tụt còn 12.168. `dbt/models/silver/quarantine_tickets.sql`: `where normalize_priority(priority_raw) is null`, dùng đúng macro kia nên hai model không thể lệch nhau. `dbt/models/silver/schema.yml`: `contract.enforced: true` + `not_null` + `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (312 cặp `(ticket_id, cdc_seq)` phân biệt, khớp đúng tập sai kiểu ở nguồn) · `dbt test` **13/13** pass (bản gốc 9) · `silver_tickets` 12.480 hàng / 12.480 ticket · `priority` 0 hàng vi phạm |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver.** Bronze là bản sao trung thực của nguồn — giá trị lớn nhất của nó
> là ghi lại đúng những gì nguồn đã gửi. Nếu Bronze từ chối bản ghi lỗi thì bằng
> chứng bị huỷ ngay tại chỗ: không dựng lại được mốc 08-10, không đối chất được với
> team backend, không replay được sau khi sửa logic. Ranh giới đúng để áp contract là
> chỗ dữ liệu chuyển từ *"những gì đã nhận"* sang *"những gì đã được khẳng định là
> đúng"*, tức Bronze→Silver.
>
> **Không dừng DAG,** vì cân đối quy mô: 312 / 14.300 bản ghi = 2,2 %. Dừng nghĩa là
> để 2,2 % dữ liệu hỏng chặn 12.480 ticket, 130.000 event và 31.200 chunk bình thường
> — đổi một sự cố chất lượng cục bộ lấy một sự cố ngừng dịch vụ toàn phần. Cách đúng
> là **định tuyến**: pipeline chạy tiếp, bản ghi hỏng rơi vào hàng đợi có tên và có lý
> do, ở đó nó hữu hạn, đếm được, theo dõi được — 312 là con số ai cũng kiểm tra được,
> khác hẳn "có gì đó sai" của tình trạng ban đầu.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **A và B** |
| **Nguyên nhân** | **A:** hai lỗi cộng lại. *Small-file problem* — 5.000 file Parquet tí hon không partition, mà DuckDB đọc theo lô và làm tròn lên theo **từng file**, nên 5.000 file tốn 5.000.000 đơn vị công quét cho tập chỉ có 130.683 hàng; chi phí tăng theo *số file* chứ không theo lượng dữ liệu, nên truy vấn chậm dần mà không ai sửa dòng code nào. *Predicate không sargable* — `strftime(event_time,…)` bọc cột trong lời gọi hàm, engine không so được kết quả hàm với tên thư mục partition hay min/max của row group nên buộc phải mở toàn bộ file rồi mới biết file nào có ích. **B:** `consume()` gọi `commit()` **trước** `write_batch()`, nên chết ở giữa hai lệnh thì offset đã dịch qua lô hiện tại trong khi dữ liệu chưa vào kho, lần khởi động lại đọc từ lô sau và lô đang dở mất vĩnh viễn — ngữ nghĩa **at-most-once**, mất trong im lặng. Đây không phải bug trong một dòng code mà là hệ quả của *thứ tự* hai thao tác không nguyên tử với nhau. |
| **Cách khắc phục** | **A:** `tools/compact.py` — `COPY … PARTITION_BY (event_date)` (14 giá trị → 14 thư mục; partition theo `customer_name` 650 giá trị sẽ dựng lại small-file problem), `ORDER BY customer_name, event_time` để min/max row group phủ dải hẹp, `ROW_GROUP_SIZE 5000` vì mặc định 122.880 gói trọn một ngày vào một row group khiến min/max phủ toàn miền và mất tác dụng lọc. `queries/dashboard.sql` — trỏ `gold_events_v2`, `hive_partitioning=true`, `event_date = date '2026-08-09'`. **B:** `ingest/consumer.py` — đảo thành **ghi trước, commit sau** (at-least-once) và làm `write_batch` idempotent bằng `on conflict (event_id) do update`, kèm `event_id varchar primary key`. |
| **Bằng chứng** | **A:** rows scanned **5.000.000 → 9.324** (**536,3×**, cần ≥ 10×) · files **5.000 → 14** · result hash `4379e4c5d9f3` **không đổi**. **B:** `make crash-test` → **ĐẠT ✓** — A ghi 20.000/20.000 event_id; B chết ở lô 7, offset dừng ở 3.000 chứ không phải 3.500 (đúng dấu hiệu offset chưa dịch qua lô dở); C khởi động lại ghi 17.000 message, bảng vẫn đúng 20.000/20.000 — 500 message phát lại bị UPSERT hấp thụ. |

**Delivery semantics.** At-most-once (commit trước): crash → mất, không bao giờ trùng.
At-least-once (ghi trước): crash → không mất, đổi lại có trùng. **Exactly-once không tồn
tại ở tầng giao vận** — không thể làm "ghi dữ liệu" và "dịch offset" nguyên tử khi chúng
ở hai hệ thống khác nhau; thứ chọn được là at-least-once **cộng** một phép ghi idempotent,
cho hiệu ứng quan sát được tương đương mà không cần một đảm bảo không tồn tại.
Chọn `DO UPDATE` chứ không `DO NOTHING`: khi nguồn sửa bản ghi rồi phát lại cùng
`event_id` với nội dung mới, `DO NOTHING` giữ bản cũ và kho **vĩnh viễn lệch với nguồn**
trong im lặng, còn `DO UPDATE` cho last-write-wins nên đúng trong cả hai trường hợp.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline **hai lần liên tiếp** rồi so số hàng và checksum — phép thử rẻ nhất phân biệt "chạy được" với "chạy lại được". Với mỗi model incremental: *khoá là gì, dbt sinh `INSERT` hay `MERGE`?* Đọc `target/run/…` để biết chắc thay vì đoán. |
| 2 | Với mọi bộ lọc incremental: **mốc so sánh nằm trên trục thời gian nào** — sự kiện hay nạp? Rồi đo phân bố `_ingested_at − event_time` và lấy P99. Mốc đặt trên trục sự kiện sẽ mất dữ liệu tới muộn mà không báo lỗi bao giờ. |
| 3 | Xem phân bố giá trị các cột khoá nghiệp vụ và tìm chỗ `NULL` chiếm tỷ lệ bất thường — `NULL` thường là dấu vết của một phép ép kiểu *không chịu báo lỗi*. `dbt test` xanh **không** nghĩa là dữ liệu đúng, chỉ nghĩa là những test đã viết đều pass. |

---

### Bảng tự chấm nhanh *(theo RUBRIC.md)*

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | `8dd7c98653` ×3 | ✓ | ✓ |
| `gold_feature_daily` — số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | `3db448685c` ×3 | ✓ | ✓ |
| `gold_doc_chunks` — số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` — số hàng | 312 | 312 | ✓ |
| `silver_tickets` — số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 13/13 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,726 ngày** | (ghi số) | ✓ |
| **Tổng verify** | 4/4 | 4/4 tiêu chí | ✓ |
