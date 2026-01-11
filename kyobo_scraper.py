"""
援먮낫臾멸퀬 SCM 濡쒓렇??諛??먮ℓ ?곗씠???ㅽ겕??
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime, timedelta
import pytz
import time
import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

def _locate_credentials_file():
    env_json = os.getenv('GOOGLE_CREDENTIALS')
    if env_json:
        path = os.path.join(os.getcwd(), 'credentials.json')
        try:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(env_json)
            return path
        except Exception:
            pass

    candidates = [
        os.path.join(os.getcwd(), 'credentials.json'),
        os.path.join(os.path.dirname(__file__), 'credentials.json'),
        os.path.join(os.path.dirname(__file__), '..', 'credentials.json')
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


class KyoboScraper:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.driver = None
        self.download_dir = os.path.join(os.getcwd(), "downloads")
        self.default_download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        
        # ?ㅼ슫濡쒕뱶 ?대뜑 ?앹꽦
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            print(f"?ㅼ슫濡쒕뱶 ?대뜑 ?앹꽦: {self.download_dir}")
    
    def get_missing_dates_from_sheet(self):
        """援ш??쒗듃?먯꽌 留덉?留??좎쭨 ?뺤씤 ??鍮좎쭊 ?좎쭨??怨꾩궛"""
        try:
            print("\n=== 鍮좎쭊 ?좎쭨 ?뺤씤 ===")
            
            # 援ш? ?쒗듃 ?곌껐
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            creds_path = _locate_credentials_file()
            if not creds_path:
                raise FileNotFoundError('credentials.json not found; set GOOGLE_CREDENTIALS secret or upload credentials.json')
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            
            # 援먮낫臾멸퀬 ?쒗듃 ?뺤씤
            valid_dates = []  # 珥덇린??
            last_date = None
            
            try:
                worksheet = spreadsheet.worksheet("援먮낫臾멸퀬")
                existing_data = worksheet.get_all_values()
                
                if existing_data and len(existing_data) > 1:
                    # 議고쉶湲곌컙 而щ읆?먯꽌 媛??理쒓렐 ?좎쭨 李얘린
                    df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                    
                    if '?좎쭨' in df.columns:
                        dates = df['?좎쭨'].tolist()
                        # ?좎쭨 ?뺤떇 ?꾪꽣留?
                        valid_dates = [d for d in dates if d and len(d) == 10 and '-' in d]
                        
                        if valid_dates:
                            last_date_str = max(valid_dates)
                            last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
                            # timezone 異붽?
                            korea_tz = pytz.timezone('Asia/Seoul')
                            last_date = korea_tz.localize(last_date)
                            print(f"??援ш??쒗듃 留덉?留??곗씠?? {last_date_str}")
                        else:
                            # ?곗씠?곌? ?놁쑝硫?2026-01-01遺??
                            korea_tz = pytz.timezone('Asia/Seoul')
                            last_date = korea_tz.localize(datetime(2025, 12, 31))
                            print(f"???곗씠???놁쓬, 2026-01-01遺???쒖옉")
                    else:
                        korea_tz = pytz.timezone('Asia/Seoul')
                        last_date = korea_tz.localize(datetime(2025, 12, 31))
                        print(f"???좎쭨 而щ읆 ?놁쓬, 2026-01-01遺???쒖옉")
                else:
                    # ?쒗듃媛 鍮꾩뼱?덉쑝硫?2026-01-01遺??
                    korea_tz = pytz.timezone('Asia/Seoul')
                    last_date = korea_tz.localize(datetime(2025, 12, 31))
                    print(f"???쒗듃 鍮꾩뼱?덉쓬, 2026-01-01遺???쒖옉")
            except:
                # 援먮낫臾멸퀬 ?쒗듃媛 ?놁쑝硫?2026-01-01遺??
                korea_tz = pytz.timezone('Asia/Seoul')
                last_date = korea_tz.localize(datetime(2025, 12, 31))
                print(f"??援먮낫臾멸퀬 ?쒗듃 ?놁쓬, 2026-01-01遺???쒖옉")
            
            # 2026-01-01遺???댁젣源뚯? 紐⑤뱺 ?좎쭨 ?앹꽦
            korea_tz = pytz.timezone('Asia/Seoul')
            start_date = datetime(2026, 1, 1)
            today = datetime.now(korea_tz).replace(tzinfo=None)
            yesterday = today - timedelta(days=1)
            
            # 紐⑤뱺 ?좎쭨 ?앹꽦
            all_dates = []
            current = start_date
            while current <= yesterday:
                all_dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            
            # 鍮좎쭊 ?좎쭨 = 紐⑤뱺 ?좎쭨 - ?쒗듃???덈뒗 ?좎쭨
            existing_dates_set = set(valid_dates) if valid_dates else set()
            missing_dates = [d for d in all_dates if d not in existing_dates_set]
            missing_dates.sort()
            
            if missing_dates:
                print(f"??鍮좎쭊 ?좎쭨: {len(missing_dates)}??)
                for date in missing_dates:
                    print(f"  - {date}")
            else:
                print("??鍮좎쭊 ?좎쭨 ?놁쓬 (理쒖떊 ?곹깭)")
            
            return missing_dates
            
        except Exception as e:
            print(f"?좎쭨 ?뺤씤 ?ㅻ쪟: {str(e)}")
            import traceback
            traceback.print_exc()
            # ?ㅻ쪟 ???댁젣 ?좎쭨留?諛섑솚
            korea_tz = pytz.timezone('Asia/Seoul')
            yesterday = datetime.now(korea_tz) - timedelta(days=1)
            return [yesterday.strftime('%Y-%m-%d')]
        
    def setup_driver(self):
        """Chrome ?쒕씪?대쾭 ?ㅼ젙"""
        options = webdriver.ChromeOptions()
        # Enable headless in CI or when HEADLESS env is set
        if os.getenv('GITHUB_ACTIONS') or os.getenv('CI') or os.getenv('HEADLESS') == '1':
            try:
                options.add_argument('--headless=new')
            except Exception:
                options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.page_load_strategy = 'normal'
        
        # ?ㅼ슫濡쒕뱶 寃쎈줈 ?ㅼ젙
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            # ChromeDriverManager 罹먯떆 ?ъ슜 ?먮뒗 ?쒖뒪??PATH??chromedriver ?ъ슜
            try:
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=options
                )
            except:
                # ChromeDriverManager ?ㅽ뙣 ???쒖뒪?쒖쓽 chromedriver ?ъ슜
                print("??ChromeDriverManager ?ㅽ뙣, ?쒖뒪??chromedriver ?ъ슜")
                self.driver = webdriver.Chrome(options=options)
            
            self.driver.maximize_window()
            print("??Chrome ?쒕씪?대쾭 ?ㅼ젙 ?꾨즺")
        except Exception as e:
            print(f"?쒕씪?대쾭 ?ㅼ젙 ?ㅻ쪟: {str(e)}")
            raise
                    
                    # 醫낅즺???ㅼ젙 - 湲곗〈 媛?吏?곌퀬 ?덈줈 ?낅젰
                    end_field = date_inputs[1]
                    print(f"醫낅즺???꾨뱶 ?꾩옱 媛? '{end_field.get_attribute('value')}'")
                    # 湲곗〈 媛??꾩쟾????젣
                    self.driver.execute_script("arguments[0].value = '';", end_field)
                    time.sleep(0.3)
                    # ??媛??낅젰
                    self.driver.execute_script("arguments[0].value = arguments[1];", end_field, yesterday_str)
                    print(f"??醫낅즺???ㅼ젙: {yesterday_str} (id: {end_field.get_attribute('id')})")
                    time.sleep(0.5)
                else:
                    print(f"??議고쉶湲곌컙 ?됱뿉??異⑸텇??input ?꾨뱶瑜?李얠? 紐삵뻽?듬땲?? {len(date_inputs)}媛?)
                    
            except Exception as e:
                print(f"議고쉶湲곌컙 ??李얘린 ?ㅽ뙣: {str(e)}")
                print("\n?섏씠吏??紐⑤뱺 ??援ъ“ ?뺤씤 以?..")
                
                # 紐⑤뱺 tr ?붿냼 ?뺤씤
                all_rows = self.driver.find_elements(By.XPATH, "//tr")
                for idx, row in enumerate(all_rows[:20]):
                    row_text = row.text[:100] if row.text else ""
                    if row_text:
                        print(f"  Row {idx}: {row_text}")
            
            time.sleep(1)
            
            time.sleep(1)
            
            # 4. 議고쉶 踰꾪듉 ?대┃
            print("\n議고쉶 踰꾪듉 ?대┃ 以?..")
            
            # 癒쇱? 紐⑤뱺 踰꾪듉 異쒕젰 (?붾쾭源?
            all_buttons = self.driver.find_elements(By.XPATH, "//a | //button")
            print(f"\n?섏씠吏??紐⑤뱺 踰꾪듉/留곹겕 ?뺤씤 以?.. (珥?{len(all_buttons)}媛?")
            
            議고쉶_buttons = []
            for idx, btn in enumerate(all_buttons):
                btn_text = btn.text.strip()
                if '議고쉶' in btn_text:
                    btn_class = btn.get_attribute('class') or ''
                    btn_id = btn.get_attribute('id') or ''
                    print(f"  [{idx}] text='{btn_text}', class='{btn_class}', id='{btn_id}'")
                    議고쉶_buttons.append(btn)
            
            # 'btn blue' ?대옒?ㅻ? 媛吏?議고쉶 踰꾪듉 李얘린 (?뚮???議고쉶 踰꾪듉)
            search_button_found = False
            for btn in 議고쉶_buttons:
                btn_class = btn.get_attribute('class') or ''
                btn_text = btn.text.strip()
                
                # 'blue'媛 ?대옒?ㅼ뿉 ?ы븿?섍퀬 ?띿뒪?멸? '議고쉶'??踰꾪듉
                if 'blue' in btn_class and btn_text == '議고쉶':
                    print(f"\n???뚮???議고쉶 踰꾪듉 李얠쓬! (class: {btn_class})")
                    try:
                        self.driver.execute_script("arguments[0].click();", btn)
                        print("??議고쉶 踰꾪듉 ?대┃ ?깃났")
                        
                        # ?곗씠??濡쒕뵫 ?湲?- 議고쉶?댁뿭 ?뚯씠釉붿씠 ?낅뜲?댄듃???뚭퉴吏 湲곕떎由?
                        print("議고쉶 寃곌낵 濡쒕뵫 ?湲?以?..")
                        time.sleep(30)  # 珥덇린 ?湲??쒓컙 30珥덈줈 利앷?
                        
                        # 議고쉶?댁뿭 ?뚯씠釉붿뿉 ?곗씠?곌? ?덈뒗吏 ?뺤씤
                        for i in range(10):  # 異붽? 10珥??湲?
                            try:
                                # ISBN 而щ읆???덈뒗 ??李얘린 (?곗씠?곌? ?덈떎???섎?)
                                data_rows = self.driver.find_elements(By.XPATH, "//table//tr[td]")
                                if len(data_rows) > 1:  # ?ㅻ뜑 ?몄뿉 ?곗씠???됱씠 ?덉쑝硫?
                                    print(f"??議고쉶 寃곌낵 濡쒕뵫 ?꾨즺! (?곗씠???? {len(data_rows)}媛?")
                                    break
                            except:
                                pass
                            time.sleep(1)
                            print(f"  異붽? ?湲?以?.. ({i+1}珥?")
                        
                        time.sleep(3)  # 異붽? ?덉젙???湲?
                        print("???곗씠??濡쒕뵫 ?꾨즺 - ?묒? ?ㅼ슫濡쒕뱶 以鍮?)
                        search_button_found = True
                        break
                    except Exception as e:
                        print(f"?대┃ ?ㅽ뙣: {str(e)}")
            
            if not search_button_found:
                print("??blue ?대옒??議고쉶 踰꾪듉??李얠? 紐삵뻽?듬땲??")
            
            # 5. ?묒? ?ㅼ슫濡쒕뱶 踰꾪듉 ?대┃
            print("\n?묒? ?ㅼ슫濡쒕뱶 踰꾪듉 李얜뒗 以?..")
            try:
                # ?묒??ㅼ슫 踰꾪듉 李얘린 - 紐⑤떖???ロ옄 ?뚭퉴吏 ?湲?
                time.sleep(2)
                
                excel_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), '?묒??ㅼ슫')] | //button[contains(text(), '?묒??ㅼ슫')]"))
                )
                
                # JavaScript濡??대┃
                self.driver.execute_script("arguments[0].click();", excel_button)
                print("???묒? ?ㅼ슫濡쒕뱶 踰꾪듉 ?대┃")
                
                # ?ㅼ슫濡쒕뱶 ?꾨즺 ?湲?
                print("?ㅼ슫濡쒕뱶 ?꾨즺 ?湲?以?..")
                time.sleep(10)  # ?ㅼ슫濡쒕뱶 ?湲??쒓컙 利앷?
                
                # ?ㅼ슫濡쒕뱶???뚯씪 ?뺤씤 - ???대뜑 紐⑤몢 ?뺤씤
                downloaded_files = []
                
                # 1. ?ㅼ젙???ㅼ슫濡쒕뱶 ?대뜑 ?뺤씤
                if os.path.exists(self.download_dir):
                    downloaded_files = [f for f in os.listdir(self.download_dir) if f.endswith(('.xls', '.xlsx'))]
                    if downloaded_files:
                        print(f"???ㅼ젙???ㅼ슫濡쒕뱶 ?대뜑?먯꽌 ?뚯씪 諛쒓껄")
                
                # 2. Chrome 湲곕낯 ?ㅼ슫濡쒕뱶 ?대뜑 ?뺤씤
                if not downloaded_files and os.path.exists(self.default_download_dir):
                    all_files = os.listdir(self.default_download_dir)
                    # 理쒓렐 10珥??대궡???앹꽦???묒? ?뚯씪 李얘린
                    import time as time_module
                    current_time = time_module.time()
                    for file in all_files:
                        if file.endswith(('.xls', '.xlsx')):
                            file_path = os.path.join(self.default_download_dir, file)
                            if current_time - os.path.getmtime(file_path) < 15:  # 15珥??대궡
                                downloaded_files.append(file)
                                print(f"??Chrome 湲곕낯 ?ㅼ슫濡쒕뱶 ?대뜑?먯꽌 ?뚯씪 諛쒓껄")
                                break
                
                if downloaded_files:
                    print(f"\n???묒? ?뚯씪 ?ㅼ슫濡쒕뱶 ?꾨즺!")
                    for file in downloaded_files:
                        # ?뚯씪???대뒓 ?대뜑???덈뒗吏 ?뺤씤
                        if os.path.exists(os.path.join(self.download_dir, file)):
                            file_path = os.path.join(self.download_dir, file)
                        else:
                            file_path = os.path.join(self.default_download_dir, file)
                        
                        file_size = os.path.getsize(file_path)
                        print(f"  - {file} ({file_size:,} bytes)")
                        print(f"    ?꾩튂: {file_path}")
                    return True
                else:
                    print("???ㅼ슫濡쒕뱶???묒? ?뚯씪??李얠쓣 ???놁뒿?덈떎.")
                    return False
                    
            except Exception as e:
                print(f"?묒? ?ㅼ슫濡쒕뱶 踰꾪듉 ?대┃ ?ㅻ쪟: {str(e)}")
                # ?泥?諛⑸쾿
                try:
                    all_excel_buttons = self.driver.find_elements(By.XPATH, "//*[contains(text(), '?묒?')]")
                    print(f"李얠? ?묒? 愿??踰꾪듉: {len(all_excel_buttons)}媛?)
                    for idx, btn in enumerate(all_excel_buttons):
                        print(f"  [{idx}] text='{btn.text}', id='{btn.get_attribute('id')}'")
                        if '?묒??ㅼ슫' in btn.text:
                            self.driver.execute_script("arguments[0].click();", btn)
                            print(f"???묒??ㅼ슫 踰꾪듉 ?대┃ (?몃뜳?? {idx})")
                            time.sleep(5)
                            break
                except Exception as e2:
                    print(f"?泥?諛⑸쾿 ?ㅽ뙣: {str(e2)}")
                return False
                
        except Exception as e:
            print(f"?먮ℓ ?곗씠???ㅽ겕??以??ㅻ쪟 諛쒖깮: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def upload_to_google_sheets(self, excel_file_path, query_date):
        """援ш? ?쒗듃???곗씠???낅줈??""
        try:
            print("\n=== 援ш? ?쒗듃 ?낅줈???쒖옉 ===")
            
            # 1. ?묒? ?뚯씪 ?쎄린 - 源⑤걮?섍쾶 泥섎━
            print(f"?묒? ?뚯씪 ?쎄린: {excel_file_path}")
            
            # ?묒? ?뚯씪 ?꾩껜 ?쎄린 (?ㅻ뜑 ?놁씠)
            df_raw = pd.read_excel(excel_file_path, header=None)
            print(f"???묒? ?먮낯 ?곗씠?? {len(df_raw)}??x {len(df_raw.columns)}??)
            
            # "ISBN" ?ㅻ뜑媛 ?덈뒗 ??李얘린
            header_row_idx = None
            for idx, row in df_raw.iterrows():
                row_values = [str(x) for x in row.values if pd.notna(x) and str(x).strip() != '']
                row_str = ' '.join(row_values)
                if 'ISBN' in row_str and '?곹뭹紐? in row_str:
                    header_row_idx = idx
                    print(f"???ㅻ뜑 ??諛쒓껄: {idx}??)
                    break
            
            if header_row_idx is None:
                print("???ㅻ뜑瑜?李얠쓣 ???놁뒿?덈떎.")
                return False
            
            # ?ㅻ뜑 異붿텧 - 鍮?而щ읆 ?쒓굅
            headers_raw = df_raw.iloc[header_row_idx].tolist()
            
            # ?좏슚???ㅻ뜑留?異붿텧 (nan???꾨땶 寃?
            valid_col_indices = []
            clean_headers = []
            for i, header in enumerate(headers_raw):
                if pd.notna(header) and str(header).strip() != '':
                    valid_col_indices.append(i)
                    clean_headers.append(str(header).strip())
            
            print(f"???좏슚??而щ읆: {len(clean_headers)}媛?)
            print(f"  而щ읆紐? {', '.join(clean_headers[:5])}...")
            
            # ?곗씠????異붿텧 (?ㅻ뜑 ?ㅼ쓬 ?됰???
            data_rows = df_raw.iloc[header_row_idx + 1:, valid_col_indices].copy()
            data_rows.columns = clean_headers
            data_rows = data_rows.reset_index(drop=True)
            
            print(f"??珥덇린 ?곗씠??濡쒕뱶: {len(data_rows)}??)
            
            # "??怨? ???쒓굅
            mask = data_rows.apply(lambda row: any('??怨? in str(cell) or '?⑷퀎' in str(cell) for cell in row.values), axis=1)
            data_rows = data_rows[~mask]
            print(f"???⑷퀎 ???쒓굅 ?? {len(data_rows)}??)
            
            # 紐⑤뱺 ???鍮꾩뼱?덇굅??nan?????쒓굅
            data_rows = data_rows.dropna(how='all')
            print(f"??鍮????쒓굅 ?? {len(data_rows)}??)
            
            # ISBN 而щ읆??鍮꾩뼱?덈뒗 ???쒓굅 (?곗씠?곌? ?녿뒗 ??
            if 'ISBN' in data_rows.columns:
                data_rows = data_rows[data_rows['ISBN'].notna() & (data_rows['ISBN'] != '')]
                print(f"??ISBN ?녿뒗 ???쒓굅 ?? {len(data_rows)}??)
            
            # NaN 媛믪쓣 鍮?臾몄옄?대줈 蹂??
            df = data_rows.fillna('')
            
            # 2. 移쇰읆紐??듭씪
            rename_dict = {
                '?곹뭹紐?: '?꾩꽌紐?,
                '異쒗뙋?쇱옄': '諛쒗뻾??,
                '議고쉶湲곌컙': '?좎쭨'
            }
            for old_name, new_name in rename_dict.items():
                if old_name in df.columns:
                    df.rename(columns={old_name: new_name}, inplace=True)
                    print(f"??移쇰읆紐?蹂寃? {old_name} ??{new_name}")
            
            # 3. ?낅줈?쒕궇吏? ?좎쭨 而щ읆 異붽? (留??욎뿉)
            upload_date = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
            df.insert(0, '?좎쭨', query_date)
            df.insert(0, '?낅줈?쒕궇吏?, upload_date)
            print(f"???낅줈?쒕궇吏?{upload_date}), ?좎쭨({query_date}) 而щ읆 異붽?")
            
            # 3. 援ш? ?쒗듃 ?곌껐
            print("援ш? ?쒗듃 ?곌껐 以?..")
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            creds_path = _locate_credentials_file()
            if not creds_path:
                raise FileNotFoundError('credentials.json not found; set GOOGLE_CREDENTIALS secret or upload credentials.json')
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            # 4. ?ㅽ봽?덈뱶?쒗듃 ?닿린
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            print("??援ш? ?쒗듃 ?곌껐 ?꾨즺")
            
            # 5. "援먮낫臾멸퀬" ?쒗듃 媛?몄삤湲??먮뒗 ?앹꽦
            try:
                worksheet = spreadsheet.worksheet("援먮낫臾멸퀬")
                print("??湲곗〈 '援먮낫臾멸퀬' ?쒗듃 李얠쓬")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="援먮낫臾멸퀬", rows="1000", cols="20")
                print("??'援먮낫臾멸퀬' ?쒗듃 ?앹꽦")
            
            # ?곗씠??寃???섑뻾
            try:
                validation_warnings = self.validate_data_integrity(df, query_date, worksheet)
            except AttributeError:
                validation_warnings = []
            except Exception as e:
                print(f"???곗씠??寃??以??ㅻ쪟: {str(e)}")
                validation_warnings = []
            
            # 6. 湲곗〈 ?곗씠??媛?몄삤湲?
            existing_data = worksheet.get_all_values()
            
            if existing_data and len(existing_data) > 1:
                # ?ㅻ뜑? ?곗씠??遺꾨━
                existing_headers = existing_data[0]
                existing_rows = existing_data[1:]
                
                # DataFrame?쇰줈 蹂??
                existing_df = pd.DataFrame(existing_rows, columns=existing_headers)
                
                # 鍮????쒓굅
                existing_df = existing_df.replace('', pd.NA).dropna(how='all').fillna('')
                
                print(f"??湲곗〈 ?곗씠?? {len(existing_df)}??)
                
                # ???곗씠?곗? 蹂묓빀
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                print(f"???곗씠??蹂묓빀: {len(combined_df)}??)
            else:
                combined_df = df
                print("??泥??곗씠???낅줈??)
            
            # 7. 3???댁긽???곗씠????젣
            if '?낅줈?쒕궇吏? in combined_df.columns:
                three_years_ago = (datetime.now(pytz.timezone('Asia/Seoul')) - timedelta(days=365*3)).strftime('%Y-%m-%d')
                original_len = len(combined_df)
                combined_df = combined_df[combined_df['?낅줈?쒕궇吏?] >= three_years_ago]
                removed = original_len - len(combined_df)
                if removed > 0:
                    print(f"??3???댁긽???곗씠??{removed}????젣")
            
            # 8. ?쒗듃 ?낅뜲?댄듃
            print("援ш? ?쒗듃 ?낅뜲?댄듃 以?..")
            worksheet.clear()
            
            # ?곗씠?곕? 臾몄옄?대줈 蹂?섑븯???덉쟾?섍쾶 泥섎━
            combined_df = combined_df.astype(str)
            
            # ?ㅻ뜑? ?곗씠??遺꾨━
            headers = combined_df.columns.tolist()
            data = combined_df.values.tolist()
            
            # ?ㅻ뜑 ?곌린
            worksheet.update(values=[headers], range_name='A1')
            
            # ?곗씠???곌린 (?덈뒗 寃쎌슦留?
            if data:
                worksheet.update(values=data, range_name='A2')
            
            print(f"??援ш? ?쒗듃 ?낅뜲?댄듃 ?꾨즺: {len(combined_df)}??)
            print(f"???쒗듃 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={worksheet.id}")
            
            return True
            
        except Exception as e:
            print(f"援ш? ?쒗듃 ?낅줈???ㅻ쪟: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def close(self):
        """?쒕씪?대쾭 醫낅즺"""
        if self.driver:
            print("釉뚮씪?곗?瑜?5珥???醫낅즺?⑸땲??..")
            time.sleep(5)
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None


def main():
    # 援먮낫臾멸퀬 濡쒓렇???뺣낫 (?섍꼍 蹂???곗꽑)
    import os
    USERNAME = os.getenv('KYOBO_ID', '1058745036')
    PASSWORD = os.getenv('KYOBO_PASSWORD', 'then325325@')
    
    scraper = KyoboScraper(USERNAME, PASSWORD)
    
    try:
        # ?쒕씪?대쾭 ?ㅼ젙
        scraper.setup_driver()
        
        # 鍮좎쭊 ?좎쭨 ?뺤씤
        missing_dates = scraper.get_missing_dates_from_sheet()
        
        if not missing_dates:
            print("\n??紐⑤뱺 ?곗씠?곌? 理쒖떊 ?곹깭?낅땲??")
            return
        
        print(f"\n?뱥 珥?{len(missing_dates)}?쇱쓽 ?곗씠?곕? ?섏쭛?⑸땲??")
        
        # 濡쒓렇???쒕룄
        success = scraper.login()
        
        if success:
            print("\n濡쒓렇?몄씠 ?깃났?곸쑝濡??꾨즺?섏뿀?듬땲??")
            
            success_count = 0
            failed_dates = []
            
            # 媛??좎쭨蹂꾨줈 ?ㅽ겕??
            for i, date in enumerate(missing_dates, 1):
                print(f"\n{'='*60}")
                print(f"?뱟 [{i}/{len(missing_dates)}] {date} ?곗씠???섏쭛")
                print(f"{'='*60}")
                
                try:
                    # ?먮ℓ ?곗씠???ㅽ겕??
                    scrape_success = scraper.scrape_sales_data(date)
                    
                    if scrape_success:
                        # ?ㅼ슫濡쒕뱶???뚯씪 李얘린
                        downloaded_file = None
                        
                        # 1. ?ㅼ젙???ㅼ슫濡쒕뱶 ?대뜑 ?뺤씤
                        import time as time_module
                        current_time = time_module.time()
                        
                        for file in os.listdir(scraper.download_dir):
                            if file.endswith(('.xls', '.xlsx')) and '援먮낫臾멸퀬' in file:
                                file_path = os.path.join(scraper.download_dir, file)
                                if current_time - os.path.getmtime(file_path) < 30:  # 30珥??대궡
                                    downloaded_file = file_path
                                    break
                        
                        # 2. Chrome 湲곕낯 ?ㅼ슫濡쒕뱶 ?대뜑?먯꽌 李얘린
                        if not downloaded_file:
                            for file in os.listdir(scraper.default_download_dir):
                                if file.endswith(('.xls', '.xlsx')) and '援먮낫臾멸퀬' in file:
                                    file_path = os.path.join(scraper.default_download_dir, file)
                                    if current_time - os.path.getmtime(file_path) < 30:  # 30珥??대궡
                                        downloaded_file = file_path
                                        break
                        
                        if downloaded_file:
                            print(f"\n?ㅼ슫濡쒕뱶 ?뚯씪 諛쒓껄: {downloaded_file}")
                            # 援ш? ?쒗듃 ?낅줈??
                            if scraper.upload_to_google_sheets(downloaded_file, date):
                                success_count += 1
                                print(f"??{date} ?곗씠???낅줈???꾨즺!")
                            else:
                                failed_dates.append(date)
                                print(f"??{date} ?곗씠???낅줈???ㅽ뙣")
                        else:
                            failed_dates.append(date)
                            print(f"\n??{date} ?ㅼ슫濡쒕뱶 ?뚯씪??李얠쓣 ???놁뒿?덈떎.")
                    else:
                        failed_dates.append(date)
                        print(f"??{date} ?곗씠???ㅽ겕???ㅽ뙣")
                        
                except Exception as e:
                    failed_dates.append(date)
                    print(f"??{date} 泥섎━ 以??ㅻ쪟: {str(e)}")
                    continue
            
            # 寃곌낵 ?붿빟
            print(f"\n{'='*60}")
            print("?뱤 ?곗씠???섏쭛 ?꾨즺!")
            print(f"{'='*60}")
            print(f"???깃났: {success_count}??)
            if failed_dates:
                print(f"???ㅽ뙣: {len(failed_dates)}??)
                print(f"   ?ㅽ뙣???좎쭨: {', '.join(failed_dates)}")
            print(f"{'='*60}\n")
        else:
            print("\n濡쒓렇?몄뿉 ?ㅽ뙣?덉뒿?덈떎.")
            
    except Exception as e:
        print(f"?ㅻ쪟 諛쒖깮: {str(e)}")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
