"""
KAMIS 데이터 SQLite 데이터베이스 레이어
- 일별/월별/연별 가격 데이터를 SQLite에 저장
- 중복 방지 (UPSERT)
- 분석용 조회 함수 제공
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join("output", "kamis_prices.db")


def get_connection(db_path=None):
    """SQLite 연결을 반환."""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn=None):
    """테이블 생성 (없으면 생성)."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,
            product_cls_code TEXT,
            product_cls_name TEXT,
            category_code TEXT,
            category_name TEXT,
            productno TEXT,
            lastest_day TEXT,
            product_name TEXT,
            item_name TEXT,
            unit TEXT,
            day1 TEXT,
            dpr1 TEXT,
            day2 TEXT,
            dpr2 TEXT,
            day3 TEXT,
            dpr3 TEXT,
            day4 TEXT,
            dpr4 TEXT,
            direction TEXT,
            value TEXT,
            UNIQUE(productno, lastest_day, item_name, product_cls_code)
        );

        CREATE TABLE IF NOT EXISTS monthly_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,
            productno TEXT,
            yyyymm TEXT,
            monthly_max TEXT,
            monthly_min TEXT,
            UNIQUE(productno, yyyymm)
        );

        CREATE TABLE IF NOT EXISTS yearly_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,
            productno TEXT,
            yyyy TEXT,
            yearly_max TEXT,
            yearly_min TEXT,
            UNIQUE(productno, yyyy)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_productno ON daily_prices(productno);
        CREATE INDEX IF NOT EXISTS idx_daily_category ON daily_prices(category_name);
        CREATE INDEX IF NOT EXISTS idx_daily_lastest_day ON daily_prices(lastest_day);
        CREATE INDEX IF NOT EXISTS idx_monthly_productno ON monthly_prices(productno);
        CREATE INDEX IF NOT EXISTS idx_yearly_productno ON yearly_prices(productno);
    """)

    conn.commit()
    if close_after:
        conn.close()


def save_daily(df, conn=None):
    """일별 가격 데이터를 DB에 저장 (중복 무시)."""
    if df.empty:
        return 0

    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    now = datetime.now().isoformat()
    inserted = 0

    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR IGNORE INTO daily_prices
                   (collected_at, product_cls_code, product_cls_name, category_code,
                    category_name, productno, lastest_day, product_name, item_name,
                    unit, day1, dpr1, day2, dpr2, day3, dpr3, day4, dpr4,
                    direction, value)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    now,
                    row.get("product_cls_code", ""),
                    row.get("product_cls_name", ""),
                    row.get("category_code", ""),
                    row.get("category_name", ""),
                    row.get("productno", ""),
                    row.get("lastest_day", ""),
                    row.get("productName", ""),
                    row.get("item_name", ""),
                    row.get("unit", ""),
                    row.get("day1", ""),
                    row.get("dpr1", ""),
                    row.get("day2", ""),
                    row.get("dpr2", ""),
                    row.get("day3", ""),
                    row.get("dpr3", ""),
                    row.get("day4", ""),
                    row.get("dpr4", ""),
                    row.get("direction", ""),
                    row.get("value", ""),
                ),
            )
            if conn.total_changes:
                inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    if close_after:
        conn.close()
    return inserted


def save_monthly(df, conn=None):
    """월별 가격 데이터를 DB에 저장."""
    if df.empty:
        return 0

    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    now = datetime.now().isoformat()
    inserted = 0

    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR REPLACE INTO monthly_prices
                   (collected_at, productno, yyyymm, monthly_max, monthly_min)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    now,
                    row.get("productno", ""),
                    row.get("yyyymm", ""),
                    row.get("monthly_max", ""),
                    row.get("monthly_min", ""),
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    if close_after:
        conn.close()
    return inserted


def save_yearly(df, conn=None):
    """연별 가격 데이터를 DB에 저장."""
    if df.empty:
        return 0

    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    now = datetime.now().isoformat()
    inserted = 0

    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR REPLACE INTO yearly_prices
                   (collected_at, productno, yyyy, yearly_max, yearly_min)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    now,
                    row.get("productno", ""),
                    row.get("yyyy", ""),
                    row.get("yearly_max", ""),
                    row.get("yearly_min", ""),
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    if close_after:
        conn.close()
    return inserted


def save_all(data_dict, conn=None):
    """일별/월별/연별 데이터를 모두 DB에 저장."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    init_db(conn)

    d = save_daily(data_dict.get("daily", pd.DataFrame()), conn)
    m = save_monthly(data_dict.get("monthly", pd.DataFrame()), conn)
    y = save_yearly(data_dict.get("yearly", pd.DataFrame()), conn)

    print(f"\n[DB 저장] 일별: {d}건 | 월별: {m}건 | 연별: {y}건")

    if close_after:
        conn.close()
    return {"daily": d, "monthly": m, "yearly": y}


# ============================================================
# 분석용 조회 함수
# ============================================================

def query_daily_prices(productno=None, category_name=None, conn=None):
    """일별 가격 데이터 조회. productno 또는 category_name으로 필터."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    query = "SELECT * FROM daily_prices WHERE 1=1"
    params = []
    if productno:
        query += " AND productno = ?"
        params.append(productno)
    if category_name:
        query += " AND category_name = ?"
        params.append(category_name)

    df = pd.read_sql_query(query, conn, params=params)
    if close_after:
        conn.close()
    return df


def query_monthly_prices(productno=None, conn=None):
    """월별 가격 데이터 조회."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    query = "SELECT * FROM monthly_prices WHERE 1=1"
    params = []
    if productno:
        query += " AND productno = ?"
        params.append(productno)
    query += " ORDER BY yyyymm"

    df = pd.read_sql_query(query, conn, params=params)
    if close_after:
        conn.close()
    return df


def query_yearly_prices(productno=None, conn=None):
    """연별 가격 데이터 조회."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    query = "SELECT * FROM yearly_prices WHERE 1=1"
    params = []
    if productno:
        query += " AND productno = ?"
        params.append(productno)
    query += " ORDER BY yyyy"

    df = pd.read_sql_query(query, conn, params=params)
    if close_after:
        conn.close()
    return df


def get_all_products(conn=None):
    """DB에 저장된 모든 품목 목록 조회."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    df = pd.read_sql_query(
        """SELECT DISTINCT productno, product_name, category_name, item_name, unit
           FROM daily_prices
           ORDER BY category_name, product_name""",
        conn,
    )
    if close_after:
        conn.close()
    return df


def get_db_stats(conn=None):
    """DB 통계 정보."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    stats = {}
    for table in ["daily_prices", "monthly_prices", "yearly_prices"]:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        stats[table] = row[0]

    if close_after:
        conn.close()
    return stats
