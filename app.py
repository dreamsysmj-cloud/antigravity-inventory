import streamlit as st
# Force update for deployment trigger
import pandas as pd
import plotly.express as px
import os
import glob
from datetime import datetime, timedelta
import database
import sqlite3

# --------------------------------------------------------------------------------
# Constants & Setup
# --------------------------------------------------------------------------------
st.set_page_config(
    page_title="물류 재고/매출 통합 관리 (DB 기반)",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load Custom CSS
def local_css(file_name):
    if os.path.exists(file_name):
        with open(file_name, encoding='utf-8') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
local_css("styles.css")

# Ensure DB is initialized
database.init_db()

# --------------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------------
@st.cache_data
def load_latest_file():
    base_dir = os.path.join(os.getcwd(), "data")
    if not os.path.exists(base_dir): return None, "데이터 폴더가 없습니다."
    
    files = glob.glob(os.path.join(base_dir, "**", "*통합데이터.xlsx"), recursive=True)
    if not files: return None, "통합 데이터 파일을 찾을 수 없습니다."
    
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0], None

def map_products_strict(df):
    """
    DB에 있는 품목만 남기고 나머지는 제거합니다.
    """
    mapped_list = []
    
    for idx, row in df.iterrows():
        company = row.get('업체')
        code = row.get('코드')
        
        # 순수 DB 조회
        product_info = database.find_product_by_code(company, code)
        
        if product_info:
            pid, name, std, price, pack_qty = product_info
            new_row = row.to_dict()
            new_row['품명(표준)'] = name
            new_row['규격(표준)'] = std
            new_row['매입단가'] = price
            new_row['입수'] = pack_qty
            new_row['PID'] = pid
            mapped_list.append(new_row)
        # else: DB에 없으면 과감히 버림 (User Request)
            
    return pd.DataFrame(mapped_list)

@st.cache_data(show_spinner=False)
def process_excel_file(file_path):
    xls = pd.ExcelFile(file_path)
    sheet_names = xls.sheet_names
    
    stock_rows = []
    sales_rows = []
    
    for sheet in sheet_names:
        try:
            raw_df = pd.read_excel(xls, sheet_name=sheet, header=None)
            
            # Header Finding
            header_idx = -1
            for idx, row in raw_df.head(10).iterrows():
                row_str = " ".join(row.astype(str).values)
                if "코드" in row_str and ("수량" in row_str or "재고" in row_str or "입수" in row_str):
                    header_idx = idx
                    break
            
            if header_idx != -1:
                df = raw_df.iloc[header_idx+1:].copy()
                df.columns = raw_df.iloc[header_idx].astype(str).str.strip()
            else:
                df = raw_df
                
            company = "기타"
            if "하은" in sheet: company = "하은"
            elif "한국" in sheet: company = "한국"
            elif "다이소" in sheet: company = "다이소"
            elif "가온" in sheet: company = "가온"
            
            # Normalize
            col_code = next((c for c in df.columns if "코드" in c), None)
            col_qty = next((c for c in df.columns if "수량" in c or "재고" in c), None)
            
            if col_code and col_qty:
                df = df.rename(columns={col_code: '코드', col_qty: '수량'})
                df['수량'] = pd.to_numeric(df['수량'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                df['업체'] = company
                
                target_df = df[['업체', '코드', '수량']].copy() # 품명은 어차피 DB에서 가져옴
                
                if "판매" in sheet or "매출" in sheet:
                    sales_rows.append(target_df)
                else:
                    stock_rows.append(target_df)
                    
        except Exception as e:
            print(f"Sheet error {sheet}: {e}")

    # Merge
    stock_df = pd.concat(stock_rows, ignore_index=True) if stock_rows else pd.DataFrame()
    sales_df = pd.concat(sales_rows, ignore_index=True) if sales_rows else pd.DataFrame()
    
    # Map to DB strict
    stock_df = map_products_strict(stock_df)
    sales_df = map_products_strict(sales_df)
    
    return stock_df, sales_df

def get_db_sales_analysis(start_date, end_date):
    """
    기간별 판매 분석 데이터 생성
    """
    conn = database.get_connection()
    
    # Products + Sales Join query
    query = f"""
        SELECT 
            p.name as 품명,
            p.standard as 규격,
            p.unit_price as 단가,
            p.pack_qty as 입수,
            SUM(s.qty) as 총판매량
        FROM sales_history s
        JOIN products p ON s.product_id = p.id
        WHERE s.date >= '{start_date}' AND s.date <= '{end_date}'
        GROUP BY p.id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return pd.DataFrame()
        
    # Stats logic
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    days = (end_dt - start_dt).days + 1
    months = days / 30.0 if days > 0 else 1
    
    df['월평균'] = df['총판매량'] / months
    df['일평균'] = df['총판매량'] / days
    
    return df

# --------------------------------------------------------------------------------
# Main UI & Navigation
# --------------------------------------------------------------------------------
st.title("📦 물류 통합 관리 (Strict Mode)")
st.markdown(f"**{datetime.now().strftime('%Y-%m-%d')}**")

# Initialize Session State for View Navigation
if 'view' not in st.session_state:
    st.session_state['view'] = '현재 재고'

# Top Navigation Buttons
c1, c2, c3, c4 = st.columns(4)
if c1.button("📦 현재 재고", use_container_width=True, type="primary" if st.session_state['view']=='현재 재고' else "secondary"):
    st.session_state['view'] = '현재 재고'
    st.rerun()
if c2.button("🗃️ 재고 DB", use_container_width=True, type="primary" if st.session_state['view']=='재고 DB' else "secondary"):
    st.session_state['view'] = '재고 DB'
    st.rerun()
if c3.button("🔄 통합데이터", use_container_width=True, type="primary" if st.session_state['view']=='통합데이터' else "secondary"):
    st.session_state['view'] = '통합데이터'
    st.rerun()
if c4.button("📈 판매 이력 분석", use_container_width=True, type="primary" if st.session_state['view']=='판매 이력 분석' else "secondary"):
    st.session_state['view'] = '판매 이력 분석'
    st.rerun()

st.markdown("---")

# --------------------------------------------------------------------------------
# View Logic
# --------------------------------------------------------------------------------

# Common Data Loading (Used in Current Inventory & Integrated Data)
def get_current_data():
    f_path, err = load_latest_file()
    if f_path:
        return process_excel_file(f_path)
    return pd.DataFrame(), pd.DataFrame()

# 1. View: 현재 재고 (Existing Logic)
if st.session_state['view'] == '현재 재고':
    st.subheader("📦 현재 재고 현황")
    
    stock_df, sales_df = get_current_data()
    
    if not stock_df.empty:
        # Summary
        c1, c2, c3 = st.columns(3)
        c1.metric("총 재고량", f"{stock_df['수량'].sum():,.0f}")
        c2.metric("총 재고금액 (추정)", f"{(stock_df['수량'] * stock_df['매입단가']).sum():,.0f}원")
        c3.metric("표시 품목 수", f"{len(stock_df):,}개")
        
        st.dataframe(
            stock_df[['품명(표준)', '규격(표준)', '업체', '수량', '매입단가', '입수']].sort_values('품명(표준)'), 
            use_container_width=True, 
            height=600
        )
    else:
        st.info("표시할 재고 데이터가 없습니다. (크롤링 파일을 로드하지 못했거나 데이터가 비어있습니다)")

# 2. View: 재고 DB (Master DB Management)
elif st.session_state['view'] == '재고 DB':
    st.subheader("🗃️ 품목 마스터 DB 관리")
    
    # Upload Toggle
    if st.toggle("📤 품목 마스터 파일 업로드 (물류 db 파일.xlsx)", value=False):
        uploaded_db = st.file_uploader("엑셀 파일 선택", type=['xlsx'], key="master_uploader")
        if uploaded_db:
            if st.button("DB 업로드 실행"):
                with st.spinner("DB 업데이트 중..."):
                    try:
                        df = pd.read_excel(uploaded_db)
                        df.columns = df.columns.astype(str).str.replace('\n', '').str.replace(' ', '')
                        
                        rename_map = {
                            '매입단가(vat미포함)': '매입단가',
                            '하은코드': '하은코드',
                            '한국코드': '한국코드',
                            '품명': '품명',
                            '규격': '규격',
                        }
                        df = df.rename(columns=rename_map)
                        
                        success_count = 0
                        progress_bar = st.progress(0)
                        total = len(df)
                        
                        for idx, row in df.iterrows():
                            if pd.isna(row.get('품명')) and pd.isna(row.get('하은코드')) and pd.isna(row.get('한국코드')):
                                continue
                            database.upsert_product_strict(row)
                            success_count += 1
                            if idx % 10 == 0: progress_bar.progress(min(idx / total, 1.0))
                        
                        progress_bar.progress(1.0)
                        st.success(f"완료! {success_count}개 품목 업데이트됨.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"오류: {e}")

    # Show DB Table
    conn = database.get_connection()
    db_df = pd.read_sql_query("SELECT * FROM products ORDER BY name", conn)
    conn.close()
    
    st.markdown(f"**총 등록 품목: {len(db_df)}개**")
    st.dataframe(db_df, use_container_width=True, height=600)

# 3. View: 통합데이터 (Crawling Data & Company Filter)
elif st.session_state['view'] == '통합데이터':
    st.subheader("🔄 통합 데이터 상세 보기 (재고/판매)")
    
    # Upload Toggle
    uploaded_crawl = None
    if st.toggle("📤 통합 데이터 파일 업로드 (크롤링 결과)", value=False):
        uploaded_crawl = st.file_uploader("엑셀 파일 선택", type=['xlsx'], key="crawl_uploader")
    
    stock_df = pd.DataFrame()
    sales_current_df = pd.DataFrame()
    
    if uploaded_crawl:
        stock_df, sales_current_df = process_excel_file(uploaded_crawl)
        st.success("업로드된 파일을 사용합니다.")
    else:
        f_path, err = load_latest_file()
        if f_path: 
            st.info(f"서버 최신 파일 사용: {os.path.basename(f_path)}")
            stock_df, sales_current_df = process_excel_file(f_path)
            
    if not stock_df.empty or not sales_current_df.empty:
        # ----------------------------------------------------------------
        # Search Bar (Reverted to Single Bar)
        # ----------------------------------------------------------------
        c_search1, c_search2 = st.columns([1, 4])
        search_cat = c_search1.selectbox("검색 기준", ["전체", "업체", "품명", "코드"], key="search_cat")
        search_kw = c_search2.text_input("검색어 입력", placeholder="검색어를 입력하세요...", key="search_kw")
        
        st.write("---")

        # ----------------------------------------------------------------
        # Data Viewer (Quick Filters)
        # ----------------------------------------------------------------
        # Radio buttons for selecting view mode
        view_options = [
            "전체 재고", "전체 판매",
            "하은 재고", "하은 판매", 
            "한국 재고", "한국 판매", 
            "다이소 재고", "다이소 판매",
            "가온 재고", "가온 판매"
        ]
        
        selected_view = st.radio("데이터 보기 선택", view_options, horizontal=True, index=0)
        
        # 1. Base Data Construction
        # Add 'Type' column for differentiation
        stock_df['구분'] = '재고'
        sales_current_df['구분'] = '판매'
        
        combined_df = pd.concat([stock_df, sales_current_df], ignore_index=True)
        
        # 2. Filter by View Selection (Quick Filter)
        target_df = pd.DataFrame()
        
        if "전체 재고" in selected_view:
            target_df = combined_df[combined_df['구분'] == '재고']
        elif "전체 판매" in selected_view:
            target_df = combined_df[combined_df['구분'] == '판매']
        else:
            # "하은 재고", "하은 판매" etc.
            parts = selected_view.split()
            v_comp = parts[0]
            v_type = parts[1]
            target_df = combined_df[
                (combined_df['업체'] == v_comp) & 
                (combined_df['구분'] == v_type)
            ]
            
        # 3. Apply Search Filter
        if search_kw:
            if search_cat == "전체":
                # Search across all columns (convert to string first)
                mask = target_df.astype(str).apply(lambda x: x.str.contains(search_kw, case=False)).any(axis=1)
                target_df = target_df[mask]
            elif search_cat == "업체":
                target_df = target_df[target_df['업체'].astype(str).str.contains(search_kw, case=False)]
            elif search_cat == "품명":
                # Use '품명(표준)' which is guaranteed from strict mapping
                target_df = target_df[target_df['품명(표준)'].astype(str).str.contains(search_kw, case=False)]
            elif search_cat == "코드":
                target_df = target_df[target_df['코드'].astype(str).str.contains(search_kw, case=False)]
        
        # Display Result
        st.markdown(f"**조회된 데이터: {len(target_df)}건**")
        
        # Columns to show
        cols = ['구분', '업체', '코드', '품명(표준)', '규격(표준)', '수량', '매입단가', '입수']
        
        st.dataframe(
            target_df[cols].sort_values(['업체', '품명(표준)']), 
            use_container_width=True, 
            height=600
        )
        
    else:
        st.warning("데이터가 없습니다.")

# 4. View: 판매 이력 분석
elif st.session_state['view'] == '판매 이력 분석':
    st.subheader("📅 기간별 판매 분석")
    st.markdown("DB에 저장된 과거 판매 이력을 바탕으로 **월평균/일평균**을 계산합니다.")
    
    c_d1, c_d2 = st.columns(2)
    start_d = c_d1.date_input("시작일", datetime(2025, 11, 1))
    end_d = c_d2.date_input("종료일", datetime.today())
    
    if start_d <= end_d:
        df_anal = get_db_sales_analysis(start_d, end_d)
        
        if not df_anal.empty:
            st.markdown(f"**{start_d} ~ {end_d} ({len(df_anal)}개 품목)**")
            st.dataframe(
                df_anal.style.format({
                    "매입단가": "{:,.0f}",
                    "총판매량": "{:,.0f}",
                    "월평균": "{:,.1f}",
                    "일평균": "{:,.1f}"
                }).background_gradient(subset=['월평균'], cmap="Greens"),
                use_container_width=True,
                height=600
            )
        else:
            st.warning("선택한 기간에 해당하는 판매 이력이 DB에 없습니다.")
    else:
        st.error("종료일이 시작일보다 빠릅니다.")
