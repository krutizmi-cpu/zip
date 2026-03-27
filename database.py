import sqlite3
from pathlib import Path

DB_PATH = Path("data/database.db")

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS supplier_settings (
        supplier TEXT PRIMARY KEY,
        source_type TEXT,
        source_value TEXT,
        selected_price_tier TEXT
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
        base_price REAL,
        image_url TEXT,
        local_image TEXT,
        raw_json TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS master_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article TEXT,
        name TEXT,
        normalized_name TEXT,
        final_price REAL,
        final_stock INTEGER,
        final_image TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS product_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier TEXT,
        supplier_article TEXT,
        supplier_name TEXT,
        normalized_name TEXT,
        master_id INTEGER,
        match_method TEXT,
        confidence REAL
    )
    """)

    conn.commit()
    conn.close()

def save_supplier_setting(supplier, source_type, source_value, selected_price_tier):
    conn = get_connection()
    conn.execute("""
        INSERT INTO supplier_settings(supplier, source_type, source_value, selected_price_tier)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(supplier) DO UPDATE SET
            source_type=excluded.source_type,
            source_value=excluded.source_value,
            selected_price_tier=excluded.selected_price_tier
    """, (supplier, source_type, source_value, selected_price_tier))
    conn.commit()
    conn.close()

def get_supplier_setting(supplier):
    conn = get_connection()
    row = conn.execute("SELECT * FROM supplier_settings WHERE supplier = ?", (supplier,)).fetchone()
    conn.close()
    return dict(row) if row else None

def clear_supplier_offers(supplier):
    conn = get_connection()
    conn.execute("DELETE FROM supplier_offers WHERE supplier = ?", (supplier,))
    conn.commit()
    conn.close()

def save_supplier_offers(supplier, df):
    clear_supplier_offers(supplier)
    conn = get_connection()
    rows = []
    for _, r in df.iterrows():
        rows.append((
            supplier,
            r.get("supplier_article"),
            r.get("name"),
            r.get("normalized_name"),
            None if r.get("stock") != r.get("stock") else r.get("stock"),
            None if r.get("base_price") != r.get("base_price") else r.get("base_price"),
            r.get("image_url"),
            r.get("local_image"),
            r.to_json(force_ascii=False)
        ))
    conn.executemany("""
        INSERT INTO supplier_offers(
            supplier, supplier_article, name, normalized_name,
            stock, base_price, image_url, local_image, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

def get_all_offers():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM supplier_offers").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_supplier_offers(supplier):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM supplier_offers WHERE supplier = ?", (supplier,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def replace_master_catalog(rows):
    conn = get_connection()
    conn.execute("DELETE FROM master_catalog")
    conn.executemany("""
        INSERT INTO master_catalog(article, name, normalized_name, final_price, final_stock, final_image)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

def get_master_catalog():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM master_catalog ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def replace_mappings(rows):
    conn = get_connection()
    conn.execute("DELETE FROM product_mapping")
    conn.executemany("""
        INSERT INTO product_mapping(
            supplier, supplier_article, supplier_name, normalized_name,
            master_id, match_method, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

def get_mappings():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM product_mapping").fetchall()
    conn.close()
    return [dict(r) for r in rows]
