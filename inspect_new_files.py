import pandas as pd
import os

files_to_check = [
    "물류 db 파일.xlsx",
    "물류 25년11월 판매데이터.xlsx",
    "물류 25년12월 판매데이터.xlsx",
]

for fname in files_to_check:
    print(f"--- Checking {fname} ---")
    if os.path.exists(fname):
        try:
            # Inspection: check first few rows to guess header
            df = pd.read_excel(fname, nrows=5) 
            print(df)
            print("Columns:", df.columns.tolist())
        except Exception as e:
            print(f"Error reading {fname}: {e}")
    else:
        print("File not found.")
    print("\n")
