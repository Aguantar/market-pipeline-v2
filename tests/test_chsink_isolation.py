"""배처의 독 행 격리 (11-24 05:05 사고 회귀 테스트). ClickHouse 없이 가짜 클라이언트로."""
from pipeline import chsink, schema
from pipeline.chsink import CHBatcher


class RejectingClient:
    """특정 행이 포함된 insert 를 서버 데이터 오류(Code: 6)로 거부. 나머지는 성공."""
    def __init__(self, poison_rows, transient_first=0):
        self.poison = set(poison_rows); self.inserted = []; self.calls = 0; self.transient_first = transient_first
    def insert(self, table, rows, column_names):
        self.calls += 1
        if self.transient_first > 0:
            self.transient_first -= 1
            raise RuntimeError("HTTPDriver ... Code: 210. DB::NetException: Connection refused")
        if any(r in self.poison for r in rows):
            raise RuntimeError("received ClickHouse error code 6\n Code: 6. DB::Exception: Cannot parse string 'null' as Decimal(38, 8)")
        self.inserted.extend(rows)


def _batcher(client):
    return CHBatcher(client, schema.ENVELOPES, batch_rows=1000, batch_sec=1e9)


def _drain(b, n=200):
    for _ in range(n):
        b.next_retry = 0.0
        b.flush()
        if b.pending_rows == 0:
            return


def test_poison_row_is_isolated_and_others_land():
    rows = [("upbit", "trades", 1, None, f"row{i}") for i in range(500)]
    poison = rows[137]
    c = RejectingClient({poison})
    b = _batcher(c)
    for r in rows:
        b.add(r)
    _drain(b)
    assert b.pending_rows == 0
    assert b.poison == 1 and b.dropped == 0
    assert len(c.inserted) == 499 and poison not in c.inserted
    assert c.calls < 40          # 이분 탐색: 500행이면 ~20회 안에 끝난다 (과거: 배치 통째로 무한 재시도)


def test_transient_error_keeps_batch_whole_and_backs_off():
    rows = [("upbit", "trades", 1, None, f"row{i}") for i in range(100)]
    c = RejectingClient(set(), transient_first=2)
    b = _batcher(c)
    for r in rows:
        b.add(r)
    b.flush()                                  # 1차 실패 (연결) → 쪼개지 않고 통째로 대기
    assert b.pending_rows == 100 and len(b.pending) == 1 and b.poison == 0
    _drain(b)
    assert b.pending_rows == 0 and len(c.inserted) == 100 and b.poison == 0


def test_is_data_error_classification():
    assert chsink._is_data_error(RuntimeError("Code: 6. DB::Exception: Cannot parse"))
    assert not chsink._is_data_error(RuntimeError("Code: 210. DB::NetException"))
    assert not chsink._is_data_error(RuntimeError("Code: 241. Memory limit exceeded"))
    assert not chsink._is_data_error(RuntimeError("connection refused"))
