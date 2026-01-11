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
        start = korea.localize(datetime(2026, 1, 1))
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

        # Click Excel download button
        excel_btn = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, "btn_ExcelDown"))
        )
        self.driver.execute_script("arguments[0].click();", excel_btn)
        
        # Wait for download to complete
        print(f"Waiting for download in: {self.download_dir}")
        for i in range(30):
            files = [f for f in os.listdir(self.download_dir) if f.endswith(('.xls', '.xlsx')) and not f.endswith('.crdownload')]
            if files:
                print(f"Found {len(files)} file(s) after {i+1}s")
                return True
            time.sleep(1)
        
        print(f"Download timeout - no file found in {self.download_dir}")
        return False

    def upload_to_google_sheets(self, excel_path, date_str):
        df_raw = pd.read_excel(excel_path, header=None)
        header_idx = None
        for i, row in df_raw.iterrows():
            if any("ISBN" in str(cell) for cell in row.values):
                header_idx = i
                break
        if header_idx is None:
            return False

        headers = [str(h).strip() for h in df_raw.iloc[header_idx].values if pd.notna(h) and str(h).strip()]
        valid_cols = [i for i, h in enumerate(df_raw.iloc[header_idx].values) if pd.notna(h) and str(h).strip()]
        
        df = df_raw.iloc[header_idx + 1:, valid_cols].copy()
        df.columns = headers
        df = df.dropna(how="all")
        df = df[~df.apply(lambda r: any("합계" in str(c) or "합 계" in str(c) for c in r.values), axis=1)]
        if "ISBN" in df.columns:
            df = df[df["ISBN"].notna() & (df["ISBN"] != "")]
        df = df.fillna("")

        rename_map = {"상품명": "도서명", "출판일자": "발행일"}
        df.rename(columns=rename_map, inplace=True)
        
        upload_date = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")
        df.insert(0, "날짜", date_str)
        df.insert(0, "업로드날짜", upload_date)

        client = gspread.authorize(get_creds())
        sheet = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sheet.worksheet("교보문고")
        except:
            ws = sheet.add_worksheet(title="교보문고", rows="1000", cols="20")

        existing = ws.get_all_values()
        if existing and len(existing) > 1:
            existing_df = pd.DataFrame(existing[1:], columns=existing[0])
            existing_df = existing_df.replace("", pd.NA).dropna(how="all").fillna("")
            combined = pd.concat([existing_df, df], ignore_index=True)
        else:
            combined = df

        if "업로드날짜" in combined.columns:
            three_years_ago = (datetime.now(pytz.timezone("Asia/Seoul")) - timedelta(days=365*3)).strftime("%Y-%m-%d")
            combined = combined[combined["업로드날짜"] >= three_years_ago]

        ws.clear()
        combined = combined.astype(str)
        ws.update(values=[combined.columns.tolist()], range_name="A1")
        if len(combined) > 0:
            ws.update(values=combined.values.tolist(), range_name="A2")
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
        if not bot.scrape_date(d):
            print(f"SKIP {d} - scrape failed")
            continue
        
        # Find downloaded file
        files = [f for f in os.listdir(bot.download_dir) if f.endswith((".xls", ".xlsx"))]
        if files:
            files.sort(key=lambda x: os.path.getmtime(os.path.join(bot.download_dir, x)), reverse=True)
            excel_path = os.path.join(bot.download_dir, files[0])
            print(f"UPLOAD {excel_path}")
            bot.upload_to_google_sheets(excel_path, d)
            # Clean up downloaded file
            try:
                os.remove(excel_path)
                print(f"REMOVED {excel_path}")
            except:
                pass
        else:
            print("NO EXCEL FILE FOUND")

    bot.close()


if __name__ == "__main__":
    main()
