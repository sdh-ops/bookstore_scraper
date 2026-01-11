import os
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


SPREADSHEET_ID = "1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s"


def get_creds():
    path = "credentials.json"
    if os.getenv("GOOGLE_CREDENTIALS"):
        with open(path, "w") as f:
            f.write(os.getenv("GOOGLE_CREDENTIALS"))
    return Credentials.from_service_account_file(
        path,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ],
    )


class KyoboScraper:

    def __init__(self, user, pw):
        self.user = user
        self.pw = pw
        self.driver = None
        self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "downloads"))
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            print(f"Created download dir: {self.download_dir}")

    def setup(self):
        opts = webdriver.ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
        }
        opts.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts,
        )

    def login(self):
        self.driver.get("https://scm.kyobobook.co.kr/scm/login.action")
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "ipt_userId"))
        )
        time.sleep(2)
        
        # Find and fill username using ID
        user_field = self.driver.find_element(By.ID, "ipt_userId")
        user_field.clear()
        user_field.send_keys(self.user)
        time.sleep(0.5)
        
        # Find and fill password using ID
        pw_field = self.driver.find_element(By.ID, "ipt_password")
        pw_field.clear()
        pw_field.send_keys(self.pw)
        time.sleep(0.5)
        
        # Click login button using ID
        login_btn = self.driver.find_element(By.ID, "btn_login")
        login_btn.click()
        
        time.sleep(5)
        return "login" not in self.driver.current_url

    def get_missing_dates(self):
        client = gspread.authorize(get_creds())
        sheet = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sheet.worksheet("교보문고")
            data = ws.get_all_values()
        except:
            data = []

        existing = set()
        if len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            if "날짜" in df.columns:
                existing = set(df["날짜"].dropna().tolist())

        korea = pytz.timezone("Asia/Seoul")
        start = korea.localize(datetime(2025, 9, 1))
        yesterday = datetime.now(korea) - timedelta(days=1)

        out = []
        cur = start
        while cur <= yesterday:
            s = cur.strftime("%Y-%m-%d")
            if s not in existing:
                out.append(s)
            cur += timedelta(days=1)
        return out

    def scrape_date(self, date_str):
        self.driver.get("https://scm.kyobobook.co.kr/scm/page.action?pageID=saleStockInfo")
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "sel_strDateFrom_input"))
        )
        time.sleep(2)

        ymd = date_str.replace("-", "")
        
        # Set start date
        start_input = self.driver.find_element(By.ID, "sel_strDateFrom_input")
        self.driver.execute_script("arguments[0].value = '';", start_input)
        self.driver.execute_script("arguments[0].value = arguments[1];", start_input, ymd)
        time.sleep(0.3)
        
        # Set end date
        end_input = self.driver.find_element(By.ID, "sel_strDateTo_input")
        self.driver.execute_script("arguments[0].value = '';", end_input)
        self.driver.execute_script("arguments[0].value = arguments[1];", end_input, ymd)
        time.sleep(0.5)

        # Click search button
        search_btn = self.driver.find_element(By.ID, "btn_search")
        self.driver.execute_script("arguments[0].click();", search_btn)
        
        # Wait 25 seconds for data to load (required by Kyobo)
        print(f"Waiting 25 seconds for data to load...")
        time.sleep(25)

        # Extract table data directly
        print("Extracting table data...")
        try:
            # Find table (adjust selector based on actual page structure)
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            if not tables:
                print("No tables found")
                return None
            
            # Use the first data table (skip navigation tables)
            table = None
            for t in tables:
                try:
                    # Check if table has data rows
                    tbody = t.find_element(By.TAG_NAME, "tbody")
                    rows = tbody.find_elements(By.TAG_NAME, "tr")
                    if len(rows) > 0:
                        table = t
                        break
                except:
                    continue
            
            if not table:
                print("No data table found")
                return None
            
            # Extract headers
            thead = table.find_element(By.TAG_NAME, "thead")
            header_row = thead.find_element(By.TAG_NAME, "tr")
            headers = [cell.text.strip() for cell in header_row.find_elements(By.TAG_NAME, "th")]
            headers = [h for h in headers if h]  # Remove empty headers
            print(f"Headers: {len(headers)} - {', '.join(headers[:5])}...")
            
            # Extract data rows
            tbody = table.find_element(By.TAG_NAME, "tbody")
            data_rows = []
            for row in tbody.find_elements(By.TAG_NAME, "tr"):
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    if any(cell for cell in row_data):
                        data_rows.append(row_data)
            
            print(f"Data rows: {len(data_rows)}")
            
            if not data_rows:
                print("No data rows found")
                return None
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"DataFrame created: {len(df)} rows x {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            print(f"Error extracting table data: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def upload_to_google_sheets(self, df, date_str):
        if df is None or len(df) == 0:
            print("No data to upload")
            return False
        
        # Clean data
        df = df.dropna(how="all")
        df = df[~df.apply(lambda r: any("합계" in str(c) or "합 계" in str(c) for c in r.values), axis=1)]
        if "ISBN" in df.columns:
            df = df[df["ISBN"].notna() & (df["ISBN"] != "")]
        df = df.fillna("").infer_objects(copy=False)

        # Rename columns
        rename_map = {"상품명": "도서명", "출판일자": "발행일"}
        df.rename(columns=rename_map, inplace=True)
        
        # Add metadata
        upload_date = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")
        updated_at = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
        df.insert(0, "날짜", date_str)
        df.insert(0, "업로드날짜", upload_date)
        df["UpdatedAt"] = updated_at

        # Connect to Google Sheets
        client = gspread.authorize(get_creds())
        sheet = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sheet.worksheet("교보문고")
        except:
            ws = sheet.add_worksheet(title="교보문고", rows="1000", cols="20")

        # Merge with existing data
        existing = ws.get_all_values()
        if existing and len(existing) > 1:
            existing_df = pd.DataFrame(existing[1:], columns=existing[0])
            existing_df = existing_df.replace("", pd.NA).dropna(how="all").fillna("")
            combined = pd.concat([existing_df, df], ignore_index=True)
        else:
            combined = df

        # Remove data older than 3 years
        if "업로드날짜" in combined.columns:
            three_years_ago = (datetime.now(pytz.timezone("Asia/Seoul")) - timedelta(days=365*3)).strftime("%Y-%m-%d")
            combined = combined[combined["업로드날짜"] >= three_years_ago]

        # Update sheet
        ws.clear()
        combined = combined.astype(str)
        ws.update(values=[combined.columns.tolist()], range_name="A1")
        if len(combined) > 0:
            ws.update(values=combined.values.tolist(), range_name="A2")
        
        print(f"Uploaded {len(combined)} rows to Google Sheets")
        return True

    def close(self):
        if self.driver:
            self.driver.quit()


def main():
    bot = KyoboScraper(os.getenv("KYOBO_ID"), os.getenv("KYOBO_PASSWORD"))
    bot.setup()
    if not bot.login():
        print("LOGIN FAILED")
        return

    dates = bot.get_missing_dates()
    print("DATES TO FETCH:", dates)

    for d in dates:
        print(f"FETCH {d}")
        df = bot.scrape_date(d)
        if df is None or len(df) == 0:
            print(f"SKIP {d} - no data extracted")
            continue
        
        print(f"UPLOAD DataFrame with {len(df)} rows")
        bot.upload_to_google_sheets(df, d)

    bot.close()


if __name__ == "__main__":
    main()
