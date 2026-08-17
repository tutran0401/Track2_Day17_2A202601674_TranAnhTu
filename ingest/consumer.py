#!/usr/bin/env python3
"""Consumer đọc topic `ai-events` và ghi xuống bảng stream — NHIỆM VỤ 5.

Chạy tay:
    python ingest/consumer.py --db data/crash/crash.duckdb \
        --topic data/crash/topic.jsonl --offset data/crash/offsets.json

Kịch bản sự cố (tools/crash_test.py tự lo):
    thêm --crash-at-batch 7  -> tiến trình tự chết ở lô thứ 7, y hệt kill -9.

KHUNG THỰC HIỆN — NHIỆM VỤ 5

  Chạy `make crash-test` trước. Đọc kết quả: bạn MẤT bản ghi hay bạn có bản
  ghi TRÙNG? Con số đó xác định consumer đang ở ngữ nghĩa nào.

      at-most-once   : commit offset TRƯỚC khi ghi  -> crash = mất dữ liệu
      at-least-once  : commit offset SAU khi ghi    -> crash = trùng dữ liệu
      exactly-once   : không tồn tại ở tầng giao vận

  Hai hạng mục cần xử lý, thiếu một là chưa đủ:

    (a) Thứ tự thao tác trong consume() — xem khối được đánh dấu bên dưới.
        Đổi thứ tự chuyển ngữ nghĩa từ nhóm này sang nhóm kia. Câu hỏi: nếu
        tiến trình chết ở điểm maybe_crash(), lô hiện tại đã được ghi chưa,
        offset đã dịch chưa, và lần khởi động lại sẽ đọc từ đâu?

    (b) Tính idempotent của write_batch() — đổi thứ tự ở (a) khiến một số lô
        được phát lại. Với câu lệnh INSERT hiện tại, phát lại nghĩa là gì?

            INSERT INTO <bảng> VALUES (...)
            ON CONFLICT (<cột khoá>) DO <UPDATE ... | NOTHING>

        DuckDB chỉ chấp nhận mệnh đề ON CONFLICT khi cột khoá có ràng buộc
        PRIMARY KEY hoặc UNIQUE — xem hằng DDL ngay bên dưới.

        Câu hỏi cho báo cáo: DO UPDATE và DO NOTHING khác nhau ở đâu khi một
        message được phát lại với nội dung ĐÃ ĐỔI? Bạn chọn cái nào, vì sao?
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from ingest.log_client import LogConsumer  # noqa: E402

TABLE = "bronze_events_stream"

# event_id là PRIMARY KEY: (1) DuckDB chỉ chấp nhận ON CONFLICT khi cột khoá có
# ràng buộc PRIMARY KEY / UNIQUE, và (2) chính ràng buộc này biến phép ghi thành
# idempotent — phát lại cùng một message không tạo thêm hàng.
DDL = f"""
create table if not exists {TABLE} (
    event_id      varchar primary key,
    ticket_id     varchar,
    customer_id   varchar,
    customer_name varchar,
    event_type    varchar,
    latency_ms    integer,
    event_time    timestamp,
    _ingested_at  timestamp
);
"""


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> None:
    """Ghi một lô message xuống kho — phép ghi idempotent.

    At-least-once cho phép cùng một message tới hai lần; phép ghi phải chịu
    được điều đó. UPSERT theo event_id nên ghi lại lần thứ N cho cùng một trạng
    thái như lần thứ nhất.

    Chọn DO UPDATE chứ không DO NOTHING: khi một message được phát lại với nội
    dung ĐÃ ĐỔI (nguồn sửa bản ghi rồi phát lại cùng event_id), DO NOTHING giữ
    nguyên bản cũ và kho vĩnh viễn lệch với nguồn — im lặng, không báo lỗi.
    DO UPDATE ghi đè bằng bản mới nhất, cho ngữ nghĩa last-write-wins. Với
    message không đổi thì hai lựa chọn cho kết quả như nhau, nên DO UPDATE
    đúng trong cả hai trường hợp.
    """
    # Nạp lô vào bảng tạm KHÔNG có index trước, rồi UPSERT bằng ĐÚNG MỘT câu
    # lệnh. executemany trên câu có ON CONFLICT chạy từng dòng một và phải cập
    # nhật index ở mỗi dòng — đo được ~7 ms/hàng, tức hàng phút cho 20.000
    # message. Cách này giữ nguyên ngữ nghĩa mà chỉ tốn một lần dựng index.
    con.execute(f"create temp table if not exists _batch as select * from {TABLE} limit 0")
    con.execute("delete from _batch")
    con.executemany(
        "insert into _batch values (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["event_id"], r["ticket_id"], r["customer_id"], r["customer_name"],
                r["event_type"], r["latency_ms"], r["event_time"], r["_ingested_at"],
            )
            for r in batch
        ],
    )
    # Khử trùng NGAY TRONG lô: ON CONFLICT DO UPDATE không cập nhật được cùng
    # một hàng hai lần trong một câu lệnh, nên một lô chứa hai message cùng
    # event_id sẽ làm câu lệnh lỗi. Giữ bản mới nhất theo _ingested_at.
    con.execute(f"""
        insert into {TABLE}
        select event_id, ticket_id, customer_id, customer_name,
               event_type, latency_ms, event_time, _ingested_at
        from (
            select *, row_number() over (
                partition by event_id order by _ingested_at desc
            ) as _rn
            from _batch
        )
        where _rn = 1
        on conflict (event_id) do update set
            ticket_id     = excluded.ticket_id,
            customer_id   = excluded.customer_id,
            customer_name = excluded.customer_name,
            event_type    = excluded.event_type,
            latency_ms    = excluded.latency_ms,
            event_time    = excluded.event_time,
            _ingested_at  = excluded._ingested_at
    """)


def maybe_crash(batch_no: int, crash_at: int | None) -> None:
    """Mô phỏng `kill -9`: chết ngay, không rollback, không flush."""
    if crash_at is not None and batch_no == crash_at:
        print(f"  [consumer] 💥 tiến trình bị giết ở lô {batch_no}", flush=True)
        os._exit(137)


def consume(
    db: str,
    topic: str,
    offset_file: str,
    batch_size: int = 500,
    crash_at: int | None = None,
) -> int:
    con = duckdb.connect(db)
    con.execute(DDL)

    written = 0
    with LogConsumer(topic, offset_file) as consumer:
        batch_no = 0
        while True:
            batch = consumer.poll(batch_size)
            if not batch:
                break
            batch_no += 1

            # ── at-least-once: GHI TRƯỚC, COMMIT SAU ──────────────────────
            # Thứ tự cũ (commit rồi mới ghi) là at-most-once: chết ở giữa thì
            # offset đã dịch qua lô hiện tại nhưng dữ liệu chưa nằm trong kho,
            # lần khởi động lại đọc tiếp từ lô sau và lô đó mất vĩnh viễn.
            #
            # Thứ tự dưới đây đảo lại: chết ở giữa thì dữ liệu đã ghi nhưng
            # offset chưa dịch, nên lần khởi động lại đọc LẠI lô đó. Không mất
            # dữ liệu, đổi lại có bản ghi trùng — và phần trùng do write_batch
            # xử lý bằng UPSERT theo event_id.
            #
            # Exactly-once không tồn tại ở tầng giao vận. Thứ chọn được là
            # at-least-once cộng với một phép ghi idempotent, và tổ hợp đó cho
            # hiệu ứng quan sát được tương đương exactly-once.
            write_batch(con, batch)           # ghi dữ liệu
            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
            consumer.commit()                 # ghi nhận offset
            # ─────────────────────────────────────────────────────────────

            written += len(batch)

    con.close()
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--offset", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--crash-at-batch", type=int, default=None)
    a = ap.parse_args()
    n = consume(a.db, a.topic, a.offset, a.batch_size, a.crash_at_batch)
    print(f"  [consumer] đã ghi {n:,} message")
    return 0


if __name__ == "__main__":
    sys.exit(main())
