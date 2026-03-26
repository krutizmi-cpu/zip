import sqlite3

DB_PATH = "data/database.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS master_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article TEXT,
        name TEXT,
        price REAL,
        stock INTEGER,
        image TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS supplier_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier TEXT,
        supplier_article TEXT,
        master_id INTEGER
    )
    """)

    conn.commit()
    conn.close()
