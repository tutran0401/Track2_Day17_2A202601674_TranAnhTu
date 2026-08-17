#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# Ba quyết định, kèm lý do:
#
#   PARTITION_BY (event_date)
#       Truy vấn dashboard lọc theo HAI cột: customer_name và ngày. Chỉ một
#       trong hai được lên đường dẫn, vì mỗi cột partition thêm vào là một tầng
#       thư mục nữa và số thư mục nhân lên theo tích số lượng giá trị.
#       event_date có 14 giá trị -> 14 thư mục, engine bỏ qua 13/14 dataset
#       trước khi mở bất kỳ file nào. customer_name có 650 giá trị -> 650 thư
#       mục, mỗi thư mục vài trăm hàng: đó chính là small-file problem cũ được
#       dựng lại dưới dạng khác.
#
#   ORDER BY customer_name, event_time
#       Cột partition đã lo việc lọc theo ngày; thống kê min/max của row group
#       chỉ còn hữu ích cho customer_name. Sắp theo customer_name để các hàng
#       của cùng một khách nằm liền nhau, nhờ đó min/max của mỗi row group phủ
#       một dải hẹp và engine bỏ qua được row group không chứa 'ACME'.
#       event_time là khoá phụ, cho dữ liệu trong một khách hàng có thứ tự ổn
#       định (kết quả tái lập được giữa các lần chạy).
#
#   ROW_GROUP_SIZE = 5000
#       130.683 hàng / 14 ngày ≈ 9.300 hàng mỗi ngày. Mặc định 122.880 gói
#       trọn một ngày vào MỘT row group, min/max của nó khi đó trải từ khách
#       hàng đầu tiên tới khách hàng cuối cùng — phủ toàn bộ miền giá trị nên
#       không lọc được gì. 5.000 hàng chia mỗi ngày thành ~7 row group, mỗi
#       nhóm phủ một dải customer_name hẹp, mà vẫn đủ lớn để không rơi lại vào
#       chi phí đọc theo lô của file tí hon.
PARTITION_COL = "event_date"
ROW_GROUP_SIZE = 5_000


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")
    if n_src == 0:
        print("\n  không tìm thấy file nguồn — chạy `python seed/generate.py --extra` trước.\n")
        return 1

    src_rows = con.execute(
        f"select count(*) from read_parquet('{SRC.as_posix()}/*.parquet')"
    ).fetchone()[0]

    # Nguồn ĐÃ CÓ sẵn cột event_date. Nếu thêm một cột dẫn xuất cùng tên, DuckDB
    # sẽ đổi tên nó thành event_date_1 và partition theo cột đó — truy vấn lọc
    # `event_date = ...` khi ấy đọc cột trong FILE chứ không đọc đường dẫn, và
    # partition pruning không bao giờ kích hoạt.
    con.execute(f"""
        copy (
            select *
            from read_parquet('{SRC.as_posix()}/*.parquet')
            order by customer_name, event_time
        ) to '{DST.as_posix()}' (
            format          parquet,
            partition_by    ({PARTITION_COL}),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    dst_files = sorted(DST.glob("**/*.parquet"))
    dst_rows = con.execute(
        f"select count(*) from read_parquet('{DST.as_posix()}/**/*.parquet', "
        f"hive_partitioning = true)"
    ).fetchone()[0]

    # Nén lại mà mất hàng thì mọi con số đo được sau đó đều vô nghĩa.
    assert src_rows == dst_rows, f"mất hàng: {src_rows:,} -> {dst_rows:,}"

    print(f"  đích  : {DST}  ({len(dst_files):,} file, {dst_rows:,} hàng)")
    print(f"  layout: partition_by({PARTITION_COL}) · "
          f"order by customer_name, event_time · row_group_size {ROW_GROUP_SIZE:,}")
    print("\n  xong. Đo lại bằng: python tools/explain.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
