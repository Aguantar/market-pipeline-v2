"""ClickHouse 적재 배처 (docs/01-design §3): 500행 또는 1초.

- 삽입 실패는 메모리 큐에 두고 지수 백오프로 재시도. 큐 상한(기본 200,000행)을 넘으면 오래된 것부터 버리고 `dropped` 로 센다.
  버려도 파일에는 있으므로 rebuild 가 메운다 - 그래서 파일이 1순위, ClickHouse 가 2순위다.
- 동기 insert. 수집기는 asyncio 라 `run_in_executor` 로 부른다 (flush_async).
"""
from __future__ import annotations

import asyncio
import collections
import logging
import re
import time

import clickhouse_connect

from .schema import RawTable

log = logging.getLogger(__name__)

BATCH_ROWS = 500
BATCH_SEC = 1.0
MAX_QUEUE = 200_000


def make_client(host: str, port: int, user: str, password: str, database: str = "mp2"):
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, database=database,
                                         settings={"async_insert": 0, "insert_deduplicate": 0,
                                                   # MV(sql_parse) 가 문자열→Float64 를 정확히 반올림하도록. 기본값은 1.09431 → 1.0943100000000001 (11-24 동치 테스트에서 발견)
                                                   "precise_float_parsing": 1})


_TRANSIENT_CODES = {159, 160, 202, 203, 209, 210, 236, 241, 242, 252, 394, 439}   # timeout, too many queries/parts, network, memory limit, cancelled, read-only


def _is_data_error(e: Exception) -> bool:
    """서버가 '이 데이터는 못 넣는다' 고 답한 오류인가 (재시도해도 같은 결과). 연결·자원 오류는 False → 기존 백오프."""
    m = re.search(r"Code: (\d+)", str(e))
    if not m:
        return False
    return int(m.group(1)) not in _TRANSIENT_CODES


class CHBatcher:
    def __init__(self, client, table: RawTable, batch_rows: int = BATCH_ROWS, batch_sec: float = BATCH_SEC,
                 max_queue: int = MAX_QUEUE):
        self.client, self.table = client, table
        self.batch_rows, self.batch_sec, self.max_queue = batch_rows, batch_sec, max_queue
        self.buf: list[tuple] = []
        self.pending: collections.deque[list[tuple]] = collections.deque()   # 실패한 배치
        self.pending_rows = 0
        self.last_flush = time.monotonic()
        self.backoff = 1.0
        self.next_retry = 0.0
        self.inserted = 0
        self.failed_batches = 0
        self.dropped = 0
        self.poison = 0            # 서버가 데이터 오류로 거부한 단건 (격리·로그 뒤 버림). 파일에는 남아 있다
        self._single_fail = 0
        self.insert_ms: collections.deque[float] = collections.deque(maxlen=1000)

    def add(self, row: tuple) -> None:
        self.buf.append(row)

    def due(self) -> bool:
        return len(self.buf) >= self.batch_rows or (self.buf and time.monotonic() - self.last_flush >= self.batch_sec)

    def _insert(self, rows: list[tuple]) -> None:
        t0 = time.monotonic()
        self.client.insert(self.table.name, rows, column_names=list(self.table.columns))
        self.insert_ms.append((time.monotonic() - t0) * 1000)
        self.inserted += len(rows)

    def flush(self) -> None:
        """현재 버퍼 + 재시도 대기분을 넣는다. 실패하면 버퍼를 pending 으로 옮기고 다음 재시도 시각을 정한다."""
        now = time.monotonic()
        if self.buf:
            batch, self.buf = self.buf, []
            self.pending.append(batch)
            self.pending_rows += len(batch)
        self.last_flush = now
        if not self.pending or now < self.next_retry:
            self._trim()
            return
        while self.pending:
            batch = self.pending[0]
            try:
                self._insert(batch)
            except Exception as e:  # noqa: BLE001
                self.failed_batches += 1
                if _is_data_error(e):
                    # 독 행 격리 (11-24 05:05 사고: 한 행의 "bbp":null 이 500행 배치를 거부시키고, 그 배치가 큐 머리를 79분 막아 1,500행 드롭).
                    # 데이터 오류는 재시도해도 같으므로 반으로 쪼개 머리에 둔다. 1행까지 쪼개진 뒤에도 거부되면 그 행만 버리고 기록한다.
                    if len(batch) > 1:
                        mid = len(batch) // 2
                        self.pending.popleft()
                        self.pending.appendleft(batch[mid:])
                        self.pending.appendleft(batch[:mid])
                        self.next_retry = now          # 쪼갠 조각은 바로 다시
                        continue
                    self._single_fail += 1
                    if self._single_fail >= 2:
                        self.pending.popleft()
                        self.pending_rows -= 1
                        self.poison += 1
                        self._single_fail = 0
                        log.error("%s poison row dropped (server rejected twice): %s | row=%s",
                                  self.table.name, str(e)[:200], repr(batch[0])[:800])
                        continue
                    self.next_retry = now + 1.0
                    return
                self.next_retry = now + self.backoff
                self.backoff = min(self.backoff * 2, 30.0)
                if self.failed_batches <= 3 or self.failed_batches % 100 == 0:
                    log.warning("%s insert failed (%d batches pending, retry in %.0fs): %s",
                                self.table.name, len(self.pending), self.backoff, str(e)[:160])
                self._trim()
                return
            self.pending.popleft()
            self.pending_rows -= len(batch)
            self._single_fail = 0
        self.backoff = 1.0

    def _trim(self) -> None:
        while self.pending_rows > self.max_queue and self.pending:
            batch = self.pending.popleft()
            self.pending_rows -= len(batch)
            self.dropped += len(batch)

    async def flush_async(self) -> None:
        await asyncio.get_running_loop().run_in_executor(None, self.flush)

    def stats(self) -> dict:
        lat = sorted(self.insert_ms)
        p95 = lat[int(len(lat) * 0.95)] if lat else 0
        return {"inserted": self.inserted, "pending": self.pending_rows, "dropped": self.dropped, "poison": self.poison,
                "failed_batches": self.failed_batches, "insert_p95_ms": round(p95, 1)}
