import pandas as pd
import os
import shutil
import database
from datetime import datetime

# Setup
database.init_db()
SOURCE_DIR = os.path.join(os.getcwd(), "source_files")
if not os.path.exists(SOURCE_DIR):
    os.makedirs(SOURCE_DIR)

# 1. Import Master Data
master_file = "물류 db 파일.xlsx"
if os.path.exists(master_file):
    print(f"Reading Master: {master_file}")
    try:
        # Header is 0 based on inspection
        df = pd.read_excel(master_file, header=0)
        df.columns = df.columns.astype(str).str.strip().str.replace('\n', '')
        
        count = 0
        for idx, row in df.iterrows():
            database.upsert_product_strict(row)
            count += 1
        print(f"✅ Updated {count} master items.")
        
        # Move file
        shutil.move(master_file, os.path.join(SOURCE_DIR, master_file))
        print("Moved master file to source_files.")
    except Exception as e:
        print(f"❌ Error master: {e}")

# 2. Import Sales History
# Find all "판매데이터" files
import glob
files = glob.glob("*판매*데이터*.xlsx")

for f in files:
    print(f"Reading History: {f}")
    try:
        # Header is 1 based on inspection
        # Columns at header 1: 일자-No., 품목코드, 품명 ... 수량
        df = pd.read_excel(f, header=1)
        
        imported = 0
        for idx, row in df.iterrows():
            date_raw = str(row.get('일자-No.', ''))
            # Parse date: "2025/12/31 -85" -> "2025-12-31"
            date_str = date_raw.split(' ')[0].strip()
            
            try:
                # Validate date format roughly
                datetime.strptime(date_str, "%Y/%m/%d")
                # Store as YYYY-MM-DD for SQLite
                date_fmt = date_str.replace('/', '-')
                
                database.insert_sales_history(row, date_fmt)
                imported += 1
            except:
                continue
                
        print(f"✅ Imported {imported} sales records from {f}")
        
        # Move file
        shutil.move(f, os.path.join(SOURCE_DIR, f))
        print(f"Moved {f} to source_files.")
        
    except Exception as e:
        print(f"❌ Error history {f}: {e}")
