import pandas as pd
import os

files = [
    "물류 db 파일.xlsx", 
    "물류 25년12월 판매데이터.xlsx" 
]

for f in files:
    print(f"=== {f} ===")
    if os.path.exists(f):
        # Read first 10 rows
        df = pd.read_excel(f, header=None, nrows=10)
        print(df)
    else:
        print("Not found")
    print("\n")
