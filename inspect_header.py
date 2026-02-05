import pandas as pd
import os

filename = "재고현황(25년1월31일).xlsx"
file_path = os.path.join(os.getcwd(), filename)

try:
    # Try reading with header at row 3 (index 2)
    df = pd.read_excel(file_path, header=2)
    print("Columns found at header=2:")
    print(df.columns.tolist())
    
    print("\nFirst row of data:")
    print(df.iloc[0])
except Exception as e:
    print(e)
