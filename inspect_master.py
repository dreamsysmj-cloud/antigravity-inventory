import pandas as pd
import os

filename = "재고현황(25년1월31일).xlsx"
file_path = os.path.join(os.getcwd(), filename)

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
else:
    print(f"Reading {filename}...")
    df = pd.read_excel(file_path, header=None, nrows=20)
    print(df)
