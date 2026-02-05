import pandas as pd
import os
import database

# Initialize DB first
database.init_db()

filename = "재고현황(25년1월31일).xlsx"
file_path = os.path.join(os.getcwd(), filename)

if not os.path.exists(file_path):
    print(f"❌ '{filename}' 파일을 찾을 수 없습니다.")
else:
    print(f"📂 '{filename}' 읽는 중...")
    try:
        # Header is at index 2 (Row 3)
        df = pd.read_excel(file_path, header=2)
        
        # Clean column names (remove spaces/newlines)
        df.columns = df.columns.astype(str).str.strip().str.replace('\n', '')
        
        print(f"✅ 컬럼 확인: {df.columns.tolist()[:5]} ...")
        
        count = 0
        for idx, row in df.iterrows():
            # Skip empty rows (where Name or Codes are missing)
            if pd.isna(row.get('품명')) and pd.isna(row.get('하은코드')) and pd.isna(row.get('한국코드')):
                continue
                
            database.upsert_product_from_master(row)
            count += 1
            
            if count % 100 == 0:
                print(f"   ... {count}개 처리 중")
                
        print(f"🎉 총 {count}개 품목을 DB에 등록/업데이트 했습니다.")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
