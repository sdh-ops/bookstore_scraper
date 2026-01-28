import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
from supabase import create_client, Client

# Configuration
# Note: These should be set as Environment Variables in GitHub Secrets
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')

# Sheet IDs
SALES_SHEET_ID = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
K_PUB_SHEET_ID = '1EfxiIat1bEUXOfdyPS184yY7ublnZVoZ7P81xMIouaE'

def get_gspread_client():
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
    df['ISBN'] = df['ISBN'].astype(str).str.replace('.0', '', regex=False)
    # Filter for valid dates (Google Sheets dates can sometimes come as ints or strings)
    df['날짜'] = pd.to_datetime(df['날짜']).dt.strftime('%Y-%m-%d')
    
    bookstores = ['교보계', 'YES24', '알라딘']
    melted = df.melt(id_vars=['날짜', 'ISBN', '정가'], 
                     value_vars=[b for b in bookstores if b in df.columns], 
                     var_name='bookstore', 
                     value_name='quantity')
    
    melted = melted[melted['quantity'].fillna(0) > 0]
    
    # Map to DB schema
    records = []
    for _, row in melted.iterrows():
        records.append({
            "isbn": row['ISBN'],
            "sale_date": row['날짜'],
            "bookstore": row['bookstore'].replace('계', ''),
            "quantity": int(row['quantity']),
            "price": int(row['정가']) if pd.notnull(row['정가']) else 0
        })
    
    upsert_to_supabase(records)

def sync_k_pub_sales():
    """Syncs data from K-Publishing (문화유통) sheet to Supabase."""
    print("Starting K-Publishing Sales Sync...")
    # NOTE: K-Publishing structure might differ. 
    # Usually it provides 'Total Sales' or similar. 
    # If the structure is the same as the bookstore sheet, we reuse the logic.
    # For now, placeholder for specific K-Pub mapping.
    pass

def upsert_to_supabase(records):
    if not records:
        print("No records to upsert.")
        return
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Process in chunks of 1000
    chunk_size = 1000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        print(f"Upserting chunk {i//chunk_size + 1} ({len(chunk)} records)...")
        # Upsert logic - assuming (isbn, sale_date, bookstore) is the primary key or unique constraint
        try:
            res = supabase.table("daily_sales").upsert(
                chunk, 
                on_conflict="isbn, sale_date, bookstore"
            ).execute()
        except Exception as e:
            print(f"Error during upsert: {e}")

if __name__ == "__main__":
    if not GOOGLE_CREDENTIALS_JSON or not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing required environment variables.")
        exit(1)
        
    sync_store_sales()
    # sync_k_pub_sales() # Add when structure is confirmed
    print("Sync process completed.")
