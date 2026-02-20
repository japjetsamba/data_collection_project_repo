
import sqlite3
from pathlib import Path
from contextlib import contextmanager

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'db' / 'app.db'

DDL_RAW = (
    'CREATE TABLE IF NOT EXISTS raw_listings ('
    'id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'source TEXT,'
    'category TEXT,'
    'title TEXT,'
    'price_raw TEXT,'
    'address_raw TEXT,'
    'image_url TEXT,'
    'link TEXT UNIQUE,'
    'page INTEGER,'
    'scraped_at TEXT DEFAULT (CURRENT_TIMESTAMP)'
    ');'
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

def insert_raw_many(rows):
    if not rows:
        return
    with connect_db() as conn:
        cur = conn.cursor()
        for r in rows:
            try:
                cur.execute(
                    """INSERT OR IGNORE INTO raw_listings (
                        source, category, title, price_raw, address_raw, image_url, link, page
                    ) VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        r.get('source'), r.get('category'), r.get('title'), r.get('price_raw'),
                        r.get('address_raw'), r.get('image_url'), r.get('link'), r.get('page')
                    )
                )
            except Exception:
                pass
        conn.commit()

def fetch_all_raw():
    with connect_db() as conn:
        try:
            import pandas as pd
            return pd.read_sql_query('SELECT * FROM raw_listings ORDER BY id DESC', conn)
        except Exception:
            return None

# Optional utility: write df to custom table

def write_df(df, table_name: str):
    with connect_db() as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
