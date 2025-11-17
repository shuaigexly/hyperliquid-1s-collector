import sqlite3
from pathlib import Path

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS kline_1s (
    ts TEXT PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL
);
"""

def init_db(db_path: str):
    """初始化 SQLite 数据库并返回连接"""
    # 确保目录存在
    path_obj = Path(db_path)
    if path_obj.parent != Path("."):
        path_obj.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(DB_SCHEMA)
    conn.commit()
    return conn

def insert_kline(conn, ts, o, h, l, c, v):
    """插入/更新一条 1s K线"""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO kline_1s (ts, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ts, o, h, l, c, v),
    )
    conn.commit()
