"""KAMIS 농산물 가격 데이터 SQLite 데이터베이스 저장 모듈

수집한 데이터를 SQLite DB에 저장하고 조회하는 기능을 제공합니다.
- 자동 테이블 생성 (prices, collection_log)
- UPSERT를 통한 중복 방지
- 품목별/기간별 조회 기능
- 수집 이력 로깅
"""

import logging
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

DEFAULT_DB_PATH = Path(__file__).parent / "data" / "kamis_prices.db"

CREATE_PRICES_TABLE = """
CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    category_code TEXT,
    category_name TEXT,
    item_code TEXT NOT NULL,
    item_name TEXT NOT NULL,
    kind_code TEXT,
    kind_name TEXT,
    rank TEXT,
    rank_code TEXT,
    unit TEXT,
    price INTEGER,
    market_name TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now', '+9 hours')),
    UNIQUE(date, item_code, kind_code, rank_code, market_name)
);
"""

CREATE_COLLECTION_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS collection_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL DEFAULT (datetime('now', '+9 hours')),
    total_fetched INTEGER NOT NULL DEFAULT 0,
    new_inserted INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'success'
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);",
    "CREATE INDEX IF NOT EXISTS idx_prices_item_code ON prices(item_code);",
    "CREATE INDEX IF NOT EXISTS idx_prices_item_name ON prices(item_name);",
    "CREATE INDEX IF NOT EXISTS idx_prices_category ON prices(category_code);",
    "CREATE INDEX IF NOT EXISTS idx_prices_date_item ON prices(date, item_code);",
]

UPSERT_PRICE = """
INSERT INTO prices (date, category_code, category_name, item_code, item_name,
                    kind_code, kind_name, rank, rank_code, unit, price, market_name)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(date, item_code, kind_code, rank_code, market_name)
DO UPDATE SET
    category_name = excluded.category_name,
    item_name = excluded.item_name,
    kind_name = excluded.kind_name,
    rank = excluded.rank,
    unit = excluded.unit,
    price = excluded.price;
"""

INSERT_LOG = """
INSERT INTO collection_log (total_fetched, new_inserted, duplicates_skipped, errors, status)
VALUES (?, ?, ?, ?, ?);
"""


class KamisDatabase:
    """KAMIS 가격 데이터 SQLite 데이터베이스 관리 클래스"""

    def __init__(self, db_path: Path | str | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """데이터베이스 및 테이블 초기화"""
        with self._connect() as conn:
            conn.execute(CREATE_PRICES_TABLE)
            conn.execute(CREATE_COLLECTION_LOG_TABLE)
            for idx_sql in CREATE_INDEXES:
                conn.execute(idx_sql)
            conn.commit()
        logger.info("데이터베이스 초기화 완료: %s", self.db_path)

    def _connect(self) -> sqlite3.Connection:
        """DB 연결 생성"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def save_prices(self, price_data: list[dict]) -> dict:
        """가격 데이터를 DB에 저장 (UPSERT)

        Args:
            price_data: kamis_collector에서 수집한 정규화된 가격 데이터 리스트

        Returns:
            {"total": int, "inserted": int, "updated": int, "errors": int}
        """
        total = len(price_data)
        inserted = 0
        updated = 0
        errors = 0

        with self._connect() as conn:
            for record in price_data:
                try:
                    cursor = conn.execute(
                        UPSERT_PRICE,
                        (
                            record.get("date", ""),
                            record.get("category_code", ""),
                            record.get("category_name", ""),
                            record.get("item_code", ""),
                            record.get("item_name", ""),
                            record.get("kind_code", ""),
                            record.get("kind_name", ""),
                            record.get("rank", ""),
                            record.get("rank_code", ""),
                            record.get("unit", ""),
                            record.get("price"),
                            record.get("market_name", ""),
                        ),
                    )
                    if cursor.rowcount > 0:
                        # SQLite에서 UPSERT 시 rowcount는 항상 1
                        # lastrowid가 새로 생성된 경우 inserted, 아니면 updated
                        inserted += 1
                    else:
                        updated += 1
                except sqlite3.Error as e:
                    errors += 1
                    logger.warning("레코드 저장 실패: %s - %s", record.get("item_name"), e)

            conn.commit()

            # 수집 로그 기록
            status = "success" if errors == 0 else "partial_error"
            conn.execute(INSERT_LOG, (total, inserted, total - inserted - errors, errors, status))
            conn.commit()

        result = {
            "total": total,
            "inserted": inserted,
            "updated": updated,
            "errors": errors,
        }
        logger.info(
            "DB 저장 완료: 총 %d건 (신규 %d, 업데이트 %d, 오류 %d)",
            total, inserted, updated, errors,
        )
        return result

    def get_prices_by_item(self, item_code: str, start_date: str = "", end_date: str = "") -> list[dict]:
        """품목별 가격 데이터 조회

        Args:
            item_code: 품목 코드 (예: "411" = 사과)
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)

        Returns:
            가격 데이터 리스트
        """
        query = "SELECT * FROM prices WHERE item_code = ?"
        params: list = [item_code]

        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)

        query += " ORDER BY date DESC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def get_prices_by_date(self, date: str) -> list[dict]:
        """특정 날짜의 전체 가격 데이터 조회

        Args:
            date: 조회일 (YYYY-MM-DD)

        Returns:
            가격 데이터 리스트
        """
        query = "SELECT * FROM prices WHERE date = ? ORDER BY category_code, item_code"
        with self._connect() as conn:
            rows = conn.execute(query, (date,)).fetchall()
            return [dict(row) for row in rows]

    def get_latest_prices(self) -> list[dict]:
        """각 품목별 최신 가격 데이터 조회

        Returns:
            품목별 최신 가격 리스트
        """
        query = """
        SELECT p.* FROM prices p
        INNER JOIN (
            SELECT item_code, kind_code, rank_code, market_name, MAX(date) as max_date
            FROM prices
            WHERE price IS NOT NULL
            GROUP BY item_code, kind_code, rank_code, market_name
        ) latest
        ON p.item_code = latest.item_code
           AND p.kind_code = latest.kind_code
           AND p.rank_code = latest.rank_code
           AND p.market_name = latest.market_name
           AND p.date = latest.max_date
        ORDER BY p.category_code, p.item_code
        """
        with self._connect() as conn:
            rows = conn.execute(query).fetchall()
            return [dict(row) for row in rows]

    def get_price_trend(self, item_code: str, days: int = 30) -> list[dict]:
        """품목의 최근 N일 가격 추이 조회

        Args:
            item_code: 품목 코드
            days: 조회 기간 (일)

        Returns:
            날짜순 가격 리스트
        """
        start = (datetime.now(KST) - timedelta(days=days)).strftime("%Y-%m-%d")
        query = """
        SELECT date, item_name, kind_name, unit, price, market_name
        FROM prices
        WHERE item_code = ? AND date >= ? AND price IS NOT NULL
        ORDER BY date ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, (item_code, start)).fetchall()
            return [dict(row) for row in rows]

    def get_statistics(self) -> dict:
        """데이터베이스 통계 조회

        Returns:
            {"total_records": int, "unique_items": int, "date_range": {...}, "last_collection": {...}}
        """
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) as cnt FROM prices").fetchone()["cnt"]
            items = conn.execute("SELECT COUNT(DISTINCT item_code) as cnt FROM prices").fetchone()["cnt"]

            date_range_row = conn.execute(
                "SELECT MIN(date) as min_date, MAX(date) as max_date FROM prices"
            ).fetchone()

            last_log = conn.execute(
                "SELECT * FROM collection_log ORDER BY id DESC LIMIT 1"
            ).fetchone()

            return {
                "total_records": total,
                "unique_items": items,
                "date_range": {
                    "start": date_range_row["min_date"],
                    "end": date_range_row["max_date"],
                } if date_range_row["min_date"] else None,
                "last_collection": dict(last_log) if last_log else None,
            }

    def get_collection_history(self, limit: int = 10) -> list[dict]:
        """수집 이력 조회

        Args:
            limit: 조회 건수

        Returns:
            수집 이력 리스트
        """
        query = "SELECT * FROM collection_log ORDER BY id DESC LIMIT ?"
        with self._connect() as conn:
            rows = conn.execute(query, (limit,)).fetchall()
            return [dict(row) for row in rows]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # 테스트용 샘플 데이터
    sample_data = [
        {
            "date": "2025-01-15",
            "category_code": "400",
            "category_name": "과일류",
            "item_code": "411",
            "item_name": "사과",
            "kind_code": "01",
            "kind_name": "후지",
            "rank": "상품",
            "rank_code": "04",
            "unit": "10 개",
            "price": 25000,
            "market_name": "서울",
        },
        {
            "date": "2025-01-15",
            "category_code": "400",
            "category_name": "과일류",
            "item_code": "412",
            "item_name": "배",
            "kind_code": "01",
            "kind_name": "신고",
            "rank": "상품",
            "rank_code": "04",
            "unit": "10 개",
            "price": 35000,
            "market_name": "서울",
        },
    ]

    db = KamisDatabase()
    result = db.save_prices(sample_data)
    print(f"저장 결과: {result}")

    stats = db.get_statistics()
    print(f"DB 통계: {stats}")

    latest = db.get_latest_prices()
    print(f"최신 가격: {len(latest)}건")
    for row in latest:
        print(f"  {row['item_name']} ({row['kind_name']}): {row['price']}원/{row['unit']}")
