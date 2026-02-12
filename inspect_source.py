import pandas as pd
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

file_path = "source_files/물류 db 파일.xlsx"
df = pd.read_excel(file_path)
print("All Columns:")
for col in df.columns:
    print(f"[{col}]")
