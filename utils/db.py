import sqlite3
from pathlib import Path
from contextlib import contextmanager
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'db' / 'app.db'

DDL_RAW = (
    "CREATE TABLE IF NOT EXISTS raw_listings ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "source TEXT,"
    "category TEXT,"
    "title TEXT,"
    "price_raw TEXT,"
    "address_raw TEXT,"
    "image_url TEXT,"
    "link TEXT UNIQUE,"
    "page INTEGER,"
    "scraped_at TEXT DEFAULT (CURRENT_TIMESTAMP)"
    ");"
)

@contextmanager
def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.execute(DDL_RAW)
        yield conn
        conn.commit()
    finally:
        conn.close()

def fetch_all_raw():
    """Return all rows from raw_listings as a pandas DataFrame (or None if empty)."""
    with connect_db() as conn:
        try:
            df = pd.read_sql_query('SELECT * FROM raw_listings ORDER BY id DESC', conn)
            if df is not None and not df.empty:
                return df
            return None
        except Exception:
            return None

def insert_raw_many(rows):
    """Insert a list[dict] of rows into raw_listings with INSERT OR IGNORE."""
    if not rows:
        return 0

    SQL_INSERT = (
        "INSERT OR IGNORE INTO raw_listings ("
        "source, category, title, price_raw, address_raw, image_url, link, page"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )

    with connect_db() as conn:
        cur = conn.cursor()
        inserted = 0
        for r in rows:
            try:
                cur.execute(
                    SQL_INSERT,
                    (
                        r.get('source'), r.get('category'), r.get('title'), r.get('price_raw'),
                        r.get('address_raw'), r.get('image_url'), r.get('link'), r.get('page')
                    )
                )
                if cur.rowcount:
                    inserted += 1
            except Exception:
                pass
        conn.commit()
        return inserted
