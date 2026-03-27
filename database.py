import sqlite3
from pathlib import Path

DB_PATH = Path("data/database.db")

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS master_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article TEXT,
        name TEXT,
        normalized_name TEXT,
        price REAL,
        stock INTEGER,
        image TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS supplier_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier TEXT,
        supplier_article TEXT,
        name TEXT,
        normalized_name TEXT,
        stock INTEGER,
        price REAL,
        image_url TEXT,
        raw_json TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS supplier_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier TEXT,
        supplier_article TEXT,
        supplier_name TEXT,
        master_id INTEGER,
        match_method TEXT,
        confidence REAL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS supplier_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier TEXT UNIQUE,
        source_type TEXT,
        source_value TEXT
    )
    """)

    conn.commit()
    conn.close()
