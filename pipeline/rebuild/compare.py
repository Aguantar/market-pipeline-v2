"""셀 대조 (docs/01-design §5): 두 DB 의 같은 raw 테이블을 (cell_key, hour) 단위로 행 수와 cityHash64 합으로 비교한다.

FINAL 로 읽어 ReplacingMergeTree 의 미머지 중복을 제거한 뒤 비교한다 (v1 docs/00 §6: 읽는 쪽이 FINAL 을 보장).
합격 = 모든 셀에서 count 와 hash 가 같다. 차이 셀은 표로 돌려준다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..schema import RawTable


@dataclass
class CellDiff:
    cell: str
    hour: str
    left_n: int
    right_n: int
    hash_equal: bool


def _cells(client, db: str, t: RawTable, from_ts: datetime, to_ts: datetime) -> dict[tuple[str, str], tuple[int, int]]:
    hcols = ", ".join(t.hash_cols)
    q = f"""
        SELECT {t.cell_key} AS cell, toString(toStartOfHour({t.time_col})) AS hour, count() AS n, sum(cityHash64({hcols})) AS h
        FROM {db}.{t.name} FINAL
        WHERE {t.time_col} >= toDateTime64('{from_ts:%Y-%m-%d %H:%M:%S.%f}', 3, 'UTC')
          AND {t.time_col} <  toDateTime64('{to_ts:%Y-%m-%d %H:%M:%S.%f}', 3, 'UTC')
        GROUP BY cell, hour
    """
    return {(r[0], r[1]): (int(r[2]), int(r[3])) for r in client.query(q).result_rows}


def compare(client, left_db: str, right_db: str, t: RawTable, from_ts: datetime, to_ts: datetime) -> tuple[list[CellDiff], int]:
    """(차이 셀 목록, 비교한 셀 수). 왼쪽=기준(라이브), 오른쪽=재구성."""
    a, b = _cells(client, left_db, t, from_ts, to_ts), _cells(client, right_db, t, from_ts, to_ts)
    diffs: list[CellDiff] = []
    for key in sorted(set(a) | set(b)):
        la, lb = a.get(key, (0, 0)), b.get(key, (0, 0))
        if la != lb:
            diffs.append(CellDiff(key[0], key[1], la[0], lb[0], la[1] == lb[1]))
    return diffs, len(set(a) | set(b))
