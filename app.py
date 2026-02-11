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
    full_stock = pd.concat(stock_rows) if stock_rows else pd.DataFrame()
    full_sales = pd.concat(sales_rows) if sales_rows else pd.DataFrame()
    
    # Strict Mapping
    if not full_stock.empty:
        full_stock = map_products_strict(full_stock)
    if not full_sales.empty:
        full_sales = map_products_strict(full_sales)
        
    return full_stock, full_sales

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
# Main UI
# --------------------------------------------------------------------------------
st.title("📦 물류 통합 관리 (Strict Mode)")
st.markdown(f"**{datetime.now().strftime('%Y-%m-%d')}**")

# Sidebar
with st.sidebar:
    st.header("⚙️ 데이터 관리")
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()

# 1. Load Data
target_file = None
uploaded_file = st.sidebar.file_uploader("최신 크롤링 파일 (통합데이터)", type=['xlsx']) # Optional manual

if uploaded_file:
    target_file = uploaded_file
else:
    f_path, err = load_latest_file()
    if f_path: target_file = f_path

# Process
stock_df = pd.DataFrame()
sales_current_df = pd.DataFrame()

if target_file:
    try:
        stock_df, sales_current_df = process_excel_file(target_file)
    except PermissionError:
        st.error("❌ 엑셀 파일이 열려있습니다. 닫으세요.")

# Tabs
tab_stock, tab_analysis = st.tabs(["📦 현재 재고 (DB등록분)", "📈 판매 이력 분석"])

with tab_stock:
    if target_file and not stock_df.empty:
        st.markdown(f"사용 파일: `{os.path.basename(target_file.name if hasattr(target_file, 'name') else target_file)}`")
        
        # Summary
        c1, c2, c3 = st.columns(3)
        c1.metric("총 재고량", f"{stock_df['수량'].sum():,.0f}")
        c2.metric("총 재고금액 (추정)", f"{(stock_df['수량'] * stock_df['매입단가']).sum():,.0f}원")
        c3.metric("표시 품목 수", f"{len(stock_df):,}개")
        
        # Strict Table
        st.dataframe(
            stock_df[['품명(표준)', '규격(표준)', '업체', '수량', '매입단가', '입수']].sort_values('품명(표준)'), 
            use_container_width=True, 
            height=600
        )
    else:
        st.info("표시할 재고 데이터가 없습니다. (크롤링 파일을 확인하거나 DB에 등록된 품목인지 확인하세요)")

with tab_analysis:
    st.markdown("### 📅 기간별 판매 분석")
    st.markdown("DB에 저장된 과거 판매 이력을 바탕으로 **월평균/일평균**을 계산합니다.")
    
    c_d1, c_d2 = st.columns(2)
    start_d = c_d1.date_input("시작일", datetime(2025, 11, 1))
    end_d = c_d2.date_input("종료일", datetime.today())
    
    if start_d <= end_d:
        df_anal = get_db_sales_analysis(start_d, end_d)
        
        if not df_anal.empty:
            st.markdown(f"**{start_d} ~ {end_d} ({len(df_anal)}개 품목)**")
            
            # Format columns
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
