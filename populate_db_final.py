
import pandas as pd
import os
import database

def run():
    # Initialize DB first
    database.init_db()
    
    filename = "source_files/물류 db 파일.xlsx"
    file_path = os.path.join(os.getcwd(), filename)
    
    if not os.path.exists(file_path):
        print(f"❌ '{filename}' 파일을 찾을 수 없습니다.")
        return

    print(f"📂 '{filename}' 읽는 중...")
    try:
        # Read Excel
        df = pd.read_excel(file_path)
        
        # Clean column names
        # remove spaces and newlines
        df.columns = df.columns.astype(str).str.replace('\n', '').str.replace(' ', '')
        
        print(f"✅ 컬럼 확인: {df.columns.tolist()[:10]} ...")
        
        # Rename for database.upsert_product_strict expectation
        # database.py expects: '하은코드', '한국코드', '품명', '규격', '매입단가'
        # Our columns are like: '하은코드', '한국코드', '품명', '규격', '매입단가(vat미포함)'
        
        # Mapping
        rename_map = {
            '매입단가(vat미포함)': '매입단가',
            '하은코드': '하은코드', # Already stripped spaces
            '한국코드': '한국코드',
            '품명': '품명',
            '규격': '규격',
            # Add pack_qty if available, otherwise default 1
        }
        
        # Check if '입수' or similar exists for 'pack_qty'
        # Based on inspection: '개수', '입수' not clearly seen, might be default 1
        
        df = df.rename(columns=rename_map)
        
        count = 0
        success_count = 0
        
        conn = database.get_connection() 
        # reusing connection inside loop might be faster if we refactor upsert, 
        # but database.upsert_product_strict opens/closes connection each time. 
        # For ~2000 items it's okay.
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if pd.isna(row.get('품명')) and pd.isna(row.get('하은코드')) and pd.isna(row.get('한국코드')):
                continue
                
            try:
                database.upsert_product_strict(row)
                success_count += 1
            except Exception as e:
                print(f"Error on row {idx}: {e}")
            
            count += 1
            if count % 100 == 0:
                print(f"   ... {count}개 처리 중")
                
        print(f"🎉 스캔 {count}개 / 성공 {success_count}개 DB 등록 완료.")
        
        # Veirfy count
        c = database.get_connection().cursor()
        c.execute("SELECT count(*) FROM products")
        final_count = c.fetchone()[0]
        print(f"📊 현재 DB 총 품목 수: {final_count}개")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    run()
