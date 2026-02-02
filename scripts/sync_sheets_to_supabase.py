import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
import argparse
import re
from supabase import create_client, Client

def clean_int(val):
    if pd.isna(val) or val == '' or val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    return int(re.sub(r'[^\d-]', '', str(val)) or 0)

# Configuration
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')

# Sheet IDs
SALES_SHEET_ID = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
K_PUB_SHEET_ID = '1EfxiIat1bEUXOfdyPS184yY7ublnZVoZ7P81xMIouaE'
COSTS_SHEET_ID = '1okyT7AfjOAmYwIxA-ffQb7NGnlYIRhjrF9Kc3L77Tc8'

def clean_isbn(val):
    if pd.isna(val) or val is None:
        return None
    # Handle float-like strings from Excel/Sheets (e.g., "978...0")
    s = str(val).strip()
    if '.' in s:
        s = s.split('.')[0]
    return re.sub(r'[^0-9X]', '', s)

def get_gspread_client():
    import re # Ensure re is available if needed, though it's imported at top

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def sync_store_sales():
    """Syncs '통합테이블' from the Bookstore Scraper sheet to Supabase."""
    print("Starting Store Sales Sync...")
    gc = get_gspread_client()
    sh = gc.open_by_key(SALES_SHEET_ID)
    worksheet = sh.worksheet("통합테이블")
    
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    if df.empty:
        print("No data found in store sales sheet.")
        return

    # Clean columns
    df['ISBN'] = df['ISBN'].apply(clean_isbn)
    df['날짜'] = pd.to_datetime(df['날짜']).dt.strftime('%Y-%m-%d')
    
    bookstores = ['교보계', 'YES24', '알라딘', '영풍']
    melted = df.melt(id_vars=['날짜', 'ISBN', '정가'], 
                     value_vars=[b for b in bookstores if b in df.columns], 
                     var_name='bookstore', 
                     value_name='quantity')
    
    # 🔍 Deduplicate/Aggregate before upsert
    agg_df = melted.groupby(['날짜', 'ISBN', 'bookstore']).agg({
        'quantity': 'sum',
        '정가': 'first' # Assuming price is consistent
    }).reset_index()
    
    records = []
    for _, row in agg_df.iterrows():
        records.append({
            "isbn": row['ISBN'],
            "sale_date": row['날짜'],
            "bookstore": row['bookstore'].replace('계', ''),
            "quantity": clean_int(row['quantity']),
            "price": clean_int(row['정가'])
        })
    
    upsert_to_supabase(records, "daily_sales")

def sync_k_pub_sales():
    """Syncs data from K-Publishing (문화유통) sheet to Supabase."""
    print("Starting K-Publishing Sales Sync...")
    gc = get_gspread_client()
    sh = gc.open_by_key(K_PUB_SHEET_ID)
    
    # 1. Load Dimensions and Fact
    print("Fetching sheets (agg_sales_daily, dim_books, dim_dates)...")
    sales_df = pd.DataFrame(sh.worksheet("agg_sales_daily").get_all_records())
    books_df = pd.DataFrame(sh.worksheet("dim_books").get_all_records())
    dates_df = pd.DataFrame(sh.worksheet("dim_dates").get_all_records())
    
    if sales_df.empty or books_df.empty or dates_df.empty:
        print("Required sheets are empty.")
        return
        
    # 2. Join to get ISBN and Date
    # Mapping: sales.book_id -> books.book_id (to get ISBN)
    # Mapping: sales.date_id -> dates.date_id (to get actual date)
    
    # Clean dim_books: Keep only book_id and ISBN
    books_lookup = books_df[['book_id', 'ISBN']].copy()
    books_lookup['ISBN'] = books_lookup['ISBN'].apply(clean_isbn)
    
    # Clean dim_dates: Keep only date_id and date
    dates_lookup = dates_df[['date_id', 'date']].copy()
    
    # Perform Joins
    merged = sales_df.merge(books_lookup, on='book_id', how='left')
    merged = merged.merge(dates_lookup, on='date_id', how='left')
    
    print(f"Merged {len(merged)} rows. Filtering and mapping...")
    
    # 3. Filter and Map to DB schema
    merged = merged[pd.notnull(merged['ISBN']) & pd.notnull(merged['date'])]
    
    # 🔍 Deduplicate/Aggregate: Group by (ISBN, date, bookstore) to avoid "ON CONFLICT" errors in Postgres
    # 🔍 Aggregation to prevent ON CONFLICT error
    merged['date'] = pd.to_datetime(merged['date']).dt.strftime('%Y-%m-%d')
    agg_df = merged.groupby(['ISBN', 'date']).agg({
        'total_quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    
    # 3. Filter and Map to DB schema
    records = []
    for _, row in agg_df.iterrows():
        if pd.isna(row['ISBN']) or pd.isna(row['date']):
            continue
            
        records.append({
            "isbn": row['ISBN'],
            "sale_date": row['date'],
            "bookstore": "문화유통DB",
            "quantity": int(row['total_quantity']),
            "price": int(row['total_amount'] / row['total_quantity']) if row['total_quantity'] > 0 else 0
        })
    
    print(f"Prepared {len(records)} unique records for Supabase.")
    upsert_to_supabase(records, "daily_sales")

def sync_inventory():
    """Syncs inventory data from Google Sheets to Supabase."""
    print("Starting Inventory Sync...")
    gc = get_gspread_client()
    sh = gc.open_by_key(K_PUB_SHEET_ID)
    
    print("Fetching sheets (재고현황, dim_books)...")
    inv_df = pd.DataFrame(sh.worksheet("재고현황").get_all_records())
    books_df = pd.DataFrame(sh.worksheet("dim_books").get_all_records())
    
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    gc = get_gspread_client()
    sh = gc.open_by_key(K_PUB_SHEET_ID)
    
    print("Fetching sheets (inventory, dim_books)...")
    inv_df = pd.DataFrame(sh.worksheet("재고현황").get_all_records()) # Changed to "inventory" in snippet, but keeping "재고현황" as per original
    mapping_df = pd.DataFrame(sh.worksheet("dim_books").get_all_records())
    
    if inv_df.empty or mapping_df.empty:
        print("Inventory or books sheet is empty.")
        return

    # 1. Mapping: book_id -> ISBN
    mapping = dict(zip(mapping_df['book_id'].astype(str), mapping_df['ISBN'].astype(str)))
    
    records = []
    seen_isbns = set()
    for _, row in inv_df.iterrows():
        book_id = str(row.get('book_id'))
        isbn = clean_isbn(mapping.get(book_id))
        if not isbn or isbn in seen_isbns:
            continue
            
        # Ensure book exists in master table
        ensure_book_exists(sb, isbn)
            
        # 구글 시트 헤더가 한글인 경우와 영문인 경우 모두 대응
        stock_normal = clean_int(row.get('normal_stock') or row.get('정상재고'))
        stock_return = clean_int(row.get('return_stock') or row.get('반품재고'))
        stock_hq = clean_int(row.get('hq_stock') or row.get('본사재고'))
        # stock_logistics는 시트의 '전체재고' 또는 '재고합계' 컬럼에서 가져오거나 합산
        stock_logistics = clean_int(row.get('total_stock') or row.get('전체재고'))
        
        if stock_logistics == 0:
            stock_logistics = stock_normal + stock_return + stock_hq
            
        if stock_logistics > 0:
            # 재고가 발생한 경우 books 테이블의 모든 상태 필드를 '판매중'으로 업데이트
            try:
                sb.table('books').update({
                    "status": "판매중",
                    "sales_status": "판매중",
                    "publication_stage": "출간"
                }).eq("isbn", isbn).execute()
            except Exception as e:
                print(f"Status update error for {isbn}: {e}")

        records.append({
            "isbn": isbn,
            "stock_normal": stock_normal,
            "stock_return": stock_return,
            "stock_hq": stock_hq,
            "stock_logistics": stock_logistics
        })
        seen_isbns.add(isbn)
    
    print(f"Prepared {len(records)} inventory records.")
    upsert_to_supabase(records, "inventory")

def sync_production_costs():
    """Syncs production cost data from Google Sheets ('제작비 월별 세분화작업 data') to Supabase."""
    print("Starting Production Costs Sync...")
    gc = get_gspread_client()
    sh = gc.open_by_key(COSTS_SHEET_ID)
    ws = sh.worksheet("data")
    
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    
    if df.empty:
        print("No data found in production costs sheet.")
        return

    # Clean and Map columns
    # The sheet is expected to have columns like 'ISBN', '제작월', '판수', etc.
    # We will map them to the production_costs table columns.
    
    records = []
    for _, row in df.iterrows():
        isbn = clean_isbn(row.get('ISBN') or row.get('isbn'))
        if not isbn:
            continue
            
        records.append({
            "isbn": isbn,
            "production_month": str(row.get('제작월') or row.get('제작달') or ''),
            "edition": str(row.get('판수') or row.get('판') or '1'),
            "book_title": row.get('도서명') or row.get('서명'),
            "print_qty": clean_int(row.get('발주부수') or row.get('인쇄부수')),
            "receive_qty": clean_int(row.get('입고부수')),
            "list_price": clean_int(row.get('정가')),
            "cost_paper": clean_int(row.get('용지비') or row.get('종이값')),
            "cost_ctp": clean_int(row.get('출력비') or row.get('CTP')),
            "cost_body_print": clean_int(row.get('본문인쇄비') or row.get('본문인쇄')),
            "cost_cover_print": clean_int(row.get('표지인쇄비') or row.get('표지인쇄')),
            "cost_coating": clean_int(row.get('코팅비')),
            "cost_finishing": clean_int(row.get('가공비')),
            "cost_binding": clean_int(row.get('제본비')),
            "cost_total": clean_int(row.get('제작합계') or row.get('합계')),
            "vendor_paper": row.get('용지처'),
            "vendor_body_print": row.get('인쇄처'),
            "vendor_binding": row.get('제본처')
        })

    print(f"Prepared {len(records)} production cost records.")
    upsert_to_supabase(records, "production_costs")

def upsert_to_supabase(records, table_name):
    if not records:
        print(f"No records to upsert for {table_name}.")
        return
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    on_conflict = "isbn, sale_date, bookstore" if table_name == "daily_sales" else "isbn"
    
    chunk_size = 500
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        print(f"Upserting {table_name} chunk {i//chunk_size + 1} ({len(chunk)} records)...")
        try:
            supabase.table(table_name).upsert(
                chunk, 
                on_conflict=on_conflict
            ).execute()
        except Exception as e:
            print(f"Error during upsert to {table_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync Google Sheets to Supabase.')
    parser.add_argument('--source', choices=['bookstore', 'kpub', 'costs', 'all'], required=True, 
                        help='Source of the data (bookstore, kpub, costs, or all)')
    
    args = parser.parse_args()

    if not GOOGLE_CREDENTIALS_JSON or not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing required environment variables (GOOGLE_CREDENTIALS, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY).")
        exit(1)
        
    if args.source == 'bookstore' or args.source == 'all':
        sync_store_sales()
    if args.source == 'kpub' or args.source == 'all':
        sync_k_pub_sales()
        sync_inventory()
    if args.source == 'costs' or args.source == 'all':
        sync_production_costs()

        
    print(f"Sync process for {args.source} completed.")
