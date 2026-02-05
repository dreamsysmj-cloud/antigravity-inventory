import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_NAME = "inventory.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    # 1. Products (Master Info)
    # Re-creating with more columns. IF exists, we might want to drop or alter, 
    # but for this transition, let's create a fresh table logic or add columns.
    # To keep it simple for this script, we'll create tables if not exist. 
    # (Note: SQLite ALTER TABLE is limited, so for full schema change, dropping is easier if data can be reloaded)
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            haeun_code TEXT,
            hankook_code TEXT,
            daiso_codes TEXT,
            standard TEXT,
            unit_price REAL DEFAULT 0, -- 매입단가
            pack_qty INTEGER DEFAULT 1, -- 입수 (박스당 수량 등)
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Check if new columns exist (migration logic for safety)
    try: c.execute("SELECT unit_price FROM products LIMIT 1")
    except: c.execute("ALTER TABLE products ADD COLUMN unit_price REAL DEFAULT 0")
    
    try: c.execute("SELECT pack_qty FROM products LIMIT 1")
    except: c.execute("ALTER TABLE products ADD COLUMN pack_qty INTEGER DEFAULT 1")

    # 2. Sales History (Past Data)
    c.execute('''
        CREATE TABLE IF NOT EXISTS sales_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            company TEXT,
            product_id INTEGER,
            qty REAL,
            remarks TEXT, -- 비고/적요 (거래처 등)
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
    ''')
    
    # 3. Transactions (Live Crawler Data Log - Optional for now, but good to keep)
    c.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            type TEXT,
            company TEXT,
            raw_code TEXT,
            product_id INTEGER,
            qty REAL
        )
    ''')

    conn.commit()
    conn.close()

def upsert_product_strict(row):
    """
    '물류 db 파일.xlsx' 전용 로더.
    """
    conn = get_connection()
    c = conn.cursor()
    
    haeun = str(row.get('하은코드', '')).strip().replace('nan', '').replace('.0', '')
    hankook = str(row.get('한국코드', '')).strip().replace('nan', '').replace('.0', '')
    name = str(row.get('품명', '-')).strip()
    std = str(row.get('규격', '')).strip().replace('nan', '')
    
    # Try parsing price
    try: price = float(row.get('매입단가', 0))
    except: price = 0
    
    # Try parsing pack_qty (입수) if exists, else default structure
    pack_qty = 1 # Default
    
    if not haeun and not hankook:
        conn.close()
        return # No identification code

    # Logic: Search by Haeun Code first
    pid = None
    if haeun:
        c.execute("SELECT id FROM products WHERE haeun_code = ?", (haeun,))
        res = c.fetchone()
        if res: pid = res[0]
        
    if not pid and hankook:
        c.execute("SELECT id FROM products WHERE hankook_code = ?", (hankook,))
        res = c.fetchone()
        if res: pid = res[0]
        
    if pid:
        c.execute('''UPDATE products SET 
            name=?, haeun_code=?, hankook_code=?, standard=?, unit_price=?, pack_qty=?, updated_at=CURRENT_TIMESTAMP 
            WHERE id=?''', (name, haeun, hankook, std, price, pack_qty, pid))
    else:
        c.execute('''INSERT INTO products 
            (name, haeun_code, hankook_code, standard, unit_price, pack_qty) 
            VALUES (?, ?, ?, ?, ?, ?)''', (name, haeun, hankook, std, price, pack_qty))
            
    conn.commit()
    conn.close()

def insert_sales_history(row, date_str):
    conn = get_connection()
    c = conn.cursor()
    
    raw_code = str(row.get('품목코드', '')).strip().replace('.0', '')
    qty = 0
    try:
        # 수량 컬럼 찾기 (헤더에 '수량'이 명시되어 있는지 확인 필요, 보통 6~7번째)
        # 엑셀 파일 컬럼명: 일자-No., 품목코드, 품명, 규격, 단위, 수량, 단가, 공급가액 ...
        # '수량' 컬럼 사용
        qty = float(str(row.get('수량', 0)).replace(',', ''))
    except: pass
    
    if qty == 0: 
        conn.close()
        return

    # Find Product ID
    c.execute("SELECT id FROM products WHERE haeun_code = ? OR hankook_code = ?", (raw_code, raw_code))
    res = c.fetchone()
    
    if res:
        pid = res[0]
        remarks = str(row.get('적요', '')) + " " + str(row.get('비고', ''))
        
        # Check duplicate (Date + Product + Qty + Remarks combination roughly)
        # To avoid re-importing same file multiple times
        c.execute("SELECT id FROM sales_history WHERE date=? AND product_id=? AND qty=? AND remarks=?", 
                  (date_str, pid, qty, remarks.strip()))
        if not c.fetchone():
            c.execute("INSERT INTO sales_history (date, company, product_id, qty, remarks) VALUES (?, ?, ?, ?, ?)",
                      (date_str, '하은', pid, qty, remarks.strip()))
            
    conn.commit()
    conn.close()

def find_product_by_code(company, code):
    conn = get_connection()
    c = conn.cursor()
    code = str(code).strip().replace('.0', '')
    
    # Priority based lookup
    # 1. Haeun
    c.execute("SELECT id, name, standard, unit_price, pack_qty FROM products WHERE haeun_code = ?", (code,))
    res = c.fetchone()
    if not res:
        # 2. Hankook
        c.execute("SELECT id, name, standard, unit_price, pack_qty FROM products WHERE hankook_code = ?", (code,))
        res = c.fetchone()
    if not res:
        # 3. Daiso (Like search)
        c.execute("SELECT id, name, standard, unit_price, pack_qty FROM products WHERE daiso_codes LIKE ?", (f"%{code}%",))
        res = c.fetchone()
        
    conn.close()
    return res 

def get_sales_stats(product_id, start_date=None, end_date=None):
    """
    Returns (total_qty, avg_monthly_qty, avg_daily_qty)
    """
    conn = get_connection()
    c = conn.cursor()
    
    query = "SELECT date, qty FROM sales_history WHERE product_id = ?"
    params = [product_id]
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
        
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if df.empty:
        return 0, 0, 0
        
    total_qty = df['qty'].sum()
    
    # Period Calculation
    df['date'] = pd.to_datetime(df['date'])
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        days = (max_date - min_date).days + 1
        months = days / 30.0 if days > 0 else 1
        
        avg_daily = total_qty / days if days > 0 else total_qty
        avg_monthly = total_qty / months if months > 0 else total_qty
        
        return total_qty, avg_monthly, avg_daily
        
    return total_qty, 0, 0
