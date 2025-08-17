import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable, Dict, Any

DB_PATH = os.path.join("results", "pricefinder.sqlite3")

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS offers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shop TEXT NOT NULL,
    title TEXT NOT NULL,
    price REAL,
    url TEXT NOT NULL,
    hash TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_offers_price ON offers(price);
CREATE INDEX IF NOT EXISTS idx_offers_shop ON offers(shop);
"""

@contextmanager
def connect(db_path: str = DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def init_db():
    with connect() as conn:
        conn.executescript(SCHEMA)

def _offer_hash(shop: str, url: str) -> str:
    # простая дедупликация: магазин+ссылка
    return f"{shop}::{url}".lower().strip()

def save_offers(items: Iterable[Dict[str, Any]]):
    """
    items: список словарей с ключами:
      shop, title, price, url
    """
    init_db()
    with connect() as conn:
        for it in items:
            shop = (it.get("shop") or "").strip()
            title = (it.get("title") or "").strip()
            price = it.get("price")
            url = (it.get("url") or "").strip()
            h = _offer_hash(shop, url)
            try:
                conn.execute(
                    "INSERT INTO offers (shop, title, price, url, hash) VALUES (?,?,?,?,?)",
                    (shop, title, price, url, h),
                )
            except sqlite3.IntegrityError:
                # уже есть такой оффер — пропускаем
                pass

def export_to_excel(xlsx_path: str):
    # небольшая обертка, чтобы вытащить всё в excel
    import pandas as pd
    with connect() as conn:
        df = pd.read_sql_query(
            "SELECT created_at, shop, title, price, url FROM offers ORDER BY price IS NULL, price ASC",
            conn,
        )
    os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
    df.to_excel(xlsx_path, index=False)
    return len(df)
