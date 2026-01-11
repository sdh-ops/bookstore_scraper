from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
from datetime import datetime, timedelta
import pytz
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import glob

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


class YoungpoongScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.download_dir = None
    
    def get_missing_dates_from_sheet(self):
        """구글시트에서 마지막 날짜 확인 후 빠진 날짜들 계산"""
        try:
            print("\n=== 빠진 날짜 확인 ===")
            
            # 구글 시트 연결
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            creds_path = _locate_credentials_file()
            if not creds_path:
                raise FileNotFoundError('credentials.json not found; set GOOGLE_CREDENTIALS secret or upload credentials.json')
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            
            # 영풍문고 시트 확인
            valid_dates = []  # 초기화
            last_date = None
            
            try:
                worksheet = spreadsheet.worksheet("영풍문고")
                existing_data = worksheet.get_all_values()
                
                if existing_data and len(existing_data) > 1:
                    # 조회기간 컬럼에서 가장 최근 날짜 찾기
                    df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                    
                    if '날짜' in df.columns:
                        dates = df['날짜'].tolist()
                        # 날짜 형식 필터링
                        valid_dates = [d for d in dates if d and len(d) == 10 and '-' in d]
                        
                        if valid_dates:
                            last_date_str = max(valid_dates)
                            last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
                            # timezone 추가
                            korea_tz = pytz.timezone('Asia/Seoul')
                            last_date = korea_tz.localize(last_date)
                            print(f"✓ 구글시트 마지막 데이터: {last_date_str}")
                        else:
                            # 데이터가 없으면 2026-01-01부터
                            korea_tz = pytz.timezone('Asia/Seoul')
                            last_date = korea_tz.localize(datetime(2025, 12, 31))
                            print(f"✓ 데이터 없음, 2026-01-01부터 시작")
                    else:
                        korea_tz = pytz.timezone('Asia/Seoul')
                        last_date = korea_tz.localize(datetime(2025, 12, 31))
                        print(f"✓ 날짜 컬럼 없음, 2026-01-01부터 시작")
                else:
                    # 시트가 비어있으면 2026-01-01부터
                    korea_tz = pytz.timezone('Asia/Seoul')
                    last_date = korea_tz.localize(datetime(2025, 12, 31))
                    print(f"✓ 시트 비어있음, 2026-01-01부터 시작")
            except:
                # 영풍문고 시트가 없으면 2026-01-01부터
                korea_tz = pytz.timezone('Asia/Seoul')
                last_date = korea_tz.localize(datetime(2025, 12, 31))
                print(f"✓ 영풍문고 시트 없음, 2026-01-01부터 시작")
            
            # 2025-09-01부터 어제까지 모든 날짜 생성
            korea_tz = pytz.timezone('Asia/Seoul')
            start_date = datetime(2025, 9, 1)
            today = datetime.now(korea_tz).replace(tzinfo=None)
            yesterday = today - timedelta(days=1)
            
            # 모든 날짜 생성
            all_dates = []
            current = start_date
            while current <= yesterday:
                all_dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            
            # 빠진 날짜 = 모든 날짜 - 시트에 있는 날짜
            existing_dates_set = set(valid_dates) if valid_dates else set()
            missing_dates = [d for d in all_dates if d not in existing_dates_set]
            missing_dates.sort()
            
            if missing_dates:
                print(f"✓ 빠진 날짜: {len(missing_dates)}일")
                for date in missing_dates:
                    print(f"  - {date}")
            else:
                print("✓ 빠진 날짜 없음 (최신 상태)")
            
            return missing_dates
            
        except Exception as e:
            print(f"날짜 확인 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            # 오류 시 어제 날짜만 반환
            korea_tz = pytz.timezone('Asia/Seoul')
            yesterday = datetime.now(korea_tz) - timedelta(days=1)
            return [yesterday.strftime('%Y-%m-%d')]
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        chrome_options = Options()
        if os.getenv('GITHUB_ACTIONS') or os.getenv('CI') or os.getenv('HEADLESS') == '1':
            try:
                chrome_options.add_argument('--headless=new')
            except Exception:
                chrome_options.add_argument('--headless')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        # 다운로드 폴더 설정
        self.download_dir = os.path.join(os.path.dirname(__file__), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
        except:
            print("⚠ ChromeDriverManager 실패, 시스템 chromedriver 사용")
            self.driver = webdriver.Chrome(options=chrome_options)
        
        self.wait = WebDriverWait(self.driver, 10)
        print("✓ Chrome 드라이버 설정 완료")
        
    def login(self, user_id, password):
        """영풍문고 SCM 로그인"""
        try:
            print("영풍문고 SCM 페이지로 이동 중...")
            self.driver.get("https://ypscm.ypbooks.co.kr/")
            time.sleep(3)
            
            print(f"현재 URL: {self.driver.current_url}")
            
            # 페이지의 모든 input 필드 확인
            print("\n페이지의 input 필드들:")
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            for i, inp in enumerate(inputs[:10]):
                try:
                    name = inp.get_attribute('name')
                    id_attr = inp.get_attribute('id')
                    type_attr = inp.get_attribute('type')
                    placeholder = inp.get_attribute('placeholder')
                    print(f"  [{i}] name={name}, id={id_attr}, type={type_attr}, placeholder={placeholder}")
                except:
                    pass
            
            print("\n로그인 필드 찾는 중...")
            
            # 아이디 입력
            id_field = None
            id_selectors = [
                (By.NAME, "userId"),
                (By.ID, "userId"),
                (By.NAME, "id"),
                (By.ID, "id"),
                (By.CSS_SELECTOR, "input[type='text']"),
            ]
            
            for by, selector in id_selectors:
                try:
                    id_field = self.driver.find_element(by, selector)
                    if id_field:
                        print(f"✓ 아이디 필드 찾음: {by}={selector}")
                        break
                except:
                    pass
            
            if not id_field:
                print("⚠ 아이디 필드를 찾을 수 없습니다.")
                return False
            
            id_field.clear()
            id_field.send_keys(user_id)
            print(f"✓ 아이디 입력: {user_id}")
            
            # 비밀번호 입력
            pw_field = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
            pw_field.clear()
            pw_field.send_keys(password)
            print("✓ 비밀번호 입력")
            
            # 로그인 버튼 클릭
            login_button_selectors = [
                (By.XPATH, "//button[contains(text(), '로그인')]"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//input[@type='submit']"),
                (By.XPATH, "//button"),
            ]
            
            login_button = None
            for by, selector in login_button_selectors:
                try:
                    login_button = self.driver.find_element(by, selector)
                    if login_button:
                        print(f"✓ 로그인 버튼 찾음")
                        break
                except:
                    pass
            
            if not login_button:
                print("⚠ 로그인 버튼을 찾을 수 없습니다.")
                return False
            
            login_button.click()
            print("✓ 로그인 버튼 클릭")
            
            time.sleep(3)
            
            print(f"현재 URL: {self.driver.current_url}")
            print("✓ 로그인 성공!")
            
            return True
            
        except Exception as e:
            print(f"로그인 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def scrape_sales_data(self, target_date_str=None):
        """판매 데이터 스크랩"""
        try:
            print(f"\n=== 영풍문고 판매 데이터 스크랩 시작 (날짜: {target_date_str if target_date_str else '어제'}) ===")
            
            # 페이지 새로고침 (두 번째 날짜부터 필요)
            if target_date_str:
                print("페이지 새로고침...")
                self.driver.refresh()
                time.sleep(3)
            
            # 1. 판매현황 메뉴 클릭
            print("판매현황 메뉴 찾는 중...")
            time.sleep(2)
            
            # 여러 가능한 선택자 시도
            sales_menu_selectors = [
                "//a[contains(text(), '판매현황')]",
                "//a[contains(text(), '판매내역')]",
                "//span[contains(text(), '판매현황')]",
                "//li[contains(text(), '판매현황')]",
            ]
            
            sales_menu = None
            for selector in sales_menu_selectors:
                try:
                    sales_menu = self.driver.find_element(By.XPATH, selector)
                    if sales_menu:
                        print(f"✓ 판매현황 메뉴 찾음")
                        break
                except:
                    continue
            
            if not sales_menu:
                print("⚠ 판매현황 메뉴를 찾을 수 없습니다.")
                print("페이지의 모든 링크 확인:")
                links = self.driver.find_elements(By.TAG_NAME, "a")
                for i, link in enumerate(links[:20]):
                    try:
                        text = link.text.strip()
                        if text:
                            print(f"  [{i}] {text}")
                    except:
                        pass
                return None, None
            
            # 메뉴 클릭
            self.driver.execute_script("arguments[0].click();", sales_menu)
            print("✓ 판매현황 메뉴 클릭")
            time.sleep(3)
            
            # 2. 날짜 설정
            print("\n날짜 설정 중...")
            korea_tz = pytz.timezone('Asia/Seoul')
            
            if target_date_str:
                # 지정된 날짜 사용 (YYYY-MM-DD -> YYYY/MM/DD)
                query_date = datetime.strptime(target_date_str, '%Y-%m-%d')
                query_date_str = target_date_str
                date_str = query_date.strftime('%Y/%m/%d')
                print(f"조회 날짜: {target_date_str} -> {date_str}")
            else:
                # 어제 날짜 사용
                today = datetime.now(korea_tz)
                query_date = today - timedelta(days=1)
                query_date_str = query_date.strftime('%Y-%m-%d')
                date_str = query_date.strftime('%Y/%m/%d')
                print(f"오늘: {today.strftime('%Y-%m-%d')}")
                print(f"조회 날짜 (어제): {query_date_str} -> {date_str}")
            
            time.sleep(2)
            
            # 날짜 입력 필드 찾기
            date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='text']")
            print(f"\n페이지의 text input 필드: {len(date_inputs)}개")
            
            # 시작일/종료일 설정
            for i, inp in enumerate(date_inputs):
                try:
                    name = inp.get_attribute('name')
                    id_attr = inp.get_attribute('id')
                    value = inp.get_attribute('value')
                    print(f"  [{i}] name={name}, id={id_attr}, value={value}")
                    
                    # 날짜 관련 필드로 보이면 설정
                    if value and len(value) == 10 and '-' in value:
                        inp.clear()
                        inp.send_keys(query_date_str)
                        print(f"✓ 날짜 설정: {query_date_str}")
                except:
                    pass
            
            time.sleep(2)
            
            # 3. 검색 버튼 클릭
            print("\n검색 버튼 찾는 중...")
            
            # 여러 방법으로 검색 버튼 찾기 (ID 우선)
            search_button = None
            search_methods = [
                # 1. ID로 직접 찾기 (가장 정확)
                (By.ID, "btnSearch_ByBook"),
                (By.CSS_SELECTOR, "#btnSearch_ByBook"),
                # 2. Class 조합
                (By.CSS_SELECTOR, "a.k-button-icontext.k-button-iconcontext"),
                # 3. XPath
                (By.XPATH, "//a[@id='btnSearch_ByBook']"),
                (By.XPATH, "//a[contains(@class, 'k-button-iconcontext') and contains(text(), '검색')]"),
            ]
            
            for by, selector in search_methods:
                try:
                    elements = self.driver.find_elements(by, selector)
                    for elem in elements:
                        if elem.is_displayed() and elem.is_enabled():
                            search_button = elem
                            print(f"✓ 검색 버튼 찾음 ({by}={selector})")
                            break
                    if search_button:
                        break
                except Exception as e:
                    continue
            
            if not search_button:
                print("⚠ 검색 버튼을 찾을 수 없습니다.")
                return None, None
            
            # 검색 버튼 클릭 (여러 방법 시도)
            print("검색 버튼 클릭 시도 중...")
            click_success = False
            
            # 방법 1: 버튼이 완전히 로드될 때까지 대기
            try:
                time.sleep(1)
                # 버튼으로 스크롤
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_button)
                time.sleep(0.5)
                print("  - 검색 버튼으로 스크롤")
            except:
                pass
            
            # 방법 2: ActionChains 클릭
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                actions = ActionChains(self.driver)
                actions.move_to_element(search_button).click().perform()
                print("✓ 검색 버튼 클릭 성공 (ActionChains)")
                click_success = True
            except Exception as e:
                print(f"  - ActionChains 클릭 실패: {str(e)}")
            
            # 방법 3: JavaScript 클릭
            if not click_success:
                try:
                    self.driver.execute_script("arguments[0].click();", search_button)
                    print("✓ 검색 버튼 클릭 성공 (JavaScript)")
                    click_success = True
                except Exception as e:
                    print(f"  - JavaScript 클릭 실패: {str(e)}")
            
            # 방법 4: 일반 클릭
            if not click_success:
                try:
                    search_button.click()
                    print("✓ 검색 버튼 클릭 성공 (일반 클릭)")
                    click_success = True
                except Exception as e:
                    print(f"  - 일반 클릭 실패: {str(e)}")
            
            if not click_success:
                print("⚠ 모든 클릭 방법 실패")
                return None, None
            
            # 검색 결과 로딩 대기 (10초 - 더 길게)
            print("검색 결과 로딩 대기 중... (10초)")
            time.sleep(10)
            
            # 검색 결과 확인 (그리드가 로드되었는지 확인)
            try:
                grid = self.driver.find_element(By.CSS_SELECTOR, ".k-grid")
                print("✓ 검색 결과 그리드 확인됨")
            except:
                print("⚠ 검색 결과 그리드를 찾을 수 없습니다 - 검색이 실행되지 않았을 수 있습니다")
                # 스크린샷 저장
                try:
                    screenshot_path = os.path.join(self.download_folder, f"search_failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                    self.driver.save_screenshot(screenshot_path)
                    print(f"  스크린샷 저장: {screenshot_path}")
                except:
                    pass
                return None
            
            # 4. 테이블에서 데이터 직접 추출
            print("\n테이블 데이터 추출 중...")
            try:
                # k-grid 내의 테이블 찾기
                table = self.driver.find_element(By.CSS_SELECTOR, ".k-grid table")
                print("✓ 테이블 찾음")
                
                # 헤더 추출
                thead = table.find_element(By.TAG_NAME, "thead")
                header_row = thead.find_element(By.TAG_NAME, "tr")
                headers = [cell.text.strip() for cell in header_row.find_elements(By.TAG_NAME, "th")]
                headers = [h for h in headers if h]  # 빈 헤더 제거
                print(f"✓ 헤더: {len(headers)}개 - {', '.join(headers[:5])}...")
                
                # 데이터 추출
                tbody = table.find_element(By.TAG_NAME, "tbody")
                data_rows = []
                for row in tbody.find_elements(By.TAG_NAME, "tr"):
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if cells:
                        row_data = [cell.text.strip() for cell in cells]
                        # 빈 행이 아니면 추가
                        if any(cell for cell in row_data):
                            data_rows.append(row_data)
                
                print(f"✓ 데이터 행: {len(data_rows)}행")
                
                if not data_rows:
                    print("⚠ 테이블에 데이터가 없습니다.")
                    return None
                
                # DataFrame 생성
                df = pd.DataFrame(data_rows, columns=headers)
                print(f"✓ DataFrame 생성: {len(df)}행 x {len(df.columns)}열")
                
                return df
                
            except Exception as e:
                print(f"⚠ 테이블 데이터 추출 오류: {str(e)}")
                import traceback
                traceback.print_exc()
                return None
            
        except Exception as e:
            print(f"데이터 스크랩 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None
    
    def upload_to_google_sheets(self, df, query_date):
        """구글 시트에 데이터 업로드 (DataFrame 직접 사용)"""
        try:
            print("\n=== 구글 시트 업로드 시작 ===")
            
            if df is None or len(df) == 0:
                print("⚠ 업로드할 데이터가 없습니다.")
                return False
            
            print(f"✓ DataFrame: {len(df)}행 x {len(df.columns)}열")
            
            # 1. 데이터 정제
            # 합계 행 제거
            df = df[~df.apply(lambda r: any('합계' in str(c) or '합 계' in str(c) or '총' in str(c) for c in r.values), axis=1)]
            df = df.dropna(how='all')
            
            # ISBN 컬럼이 있으면 ISBN 없는 행 제거
            if 'ISBN' in df.columns:
                df = df[df['ISBN'].notna() & (df['ISBN'] != '')]
            
            df = df.fillna('').infer_objects(copy=False)
            
            print(f"✓ 정제 후: {len(df)}행")
            
            print(f"✓ 정제 후: {len(df)}행")
            
            # 2. 칼럼명 통일
            rename_dict = {
                '바코드': 'ISBN',
                '출판사명': '출판사',
                '상품명': '도서명',
            }
            df.rename(columns=rename_dict, inplace=True)
            print(f"✓ 칼럼명 통일 완료")
            
            # 3. 업로드날짜, 날짜, UpdatedAt 추가
            upload_date = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
            updated_at = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')
            df.insert(0, '날짜', query_date)
            df.insert(0, '업로드날짜', upload_date)
            df['UpdatedAt'] = updated_at
            print(f"✓ 업로드날짜({upload_date}), 날짜({query_date}) 컬럼 추가")
            
            # 4. 구글 시트 연결
            print("구글 시트 연결 중...")
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            creds_path = _locate_credentials_file()
            if not creds_path:
                raise FileNotFoundError('credentials.json not found; set GOOGLE_CREDENTIALS secret or upload credentials.json')
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            # 5. 스프레드시트 열기
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            print("✓ 구글 시트 연결 완료")
            
            # 6. "영풍문고" 시트 가져오기 또는 생성
            try:
                worksheet = spreadsheet.worksheet("영풍문고")
                print("✓ 기존 '영풍문고' 시트 찾음")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="영풍문고", rows="1000", cols="20")
                print("✓ '영풍문고' 시트 생성")
            
            # 데이터 검수 수행
            try:
                validation_warnings = self.validate_data_integrity(df, query_date, worksheet)
            except AttributeError:
                validation_warnings = []
            except Exception as e:
                print(f"⚠ 데이터 검수 중 오류: {str(e)}")
                validation_warnings = []
            
            # 7. 기존 데이터 가져오기
            existing_data = worksheet.get_all_values()
            
            if existing_data and len(existing_data) > 1:
                existing_headers = existing_data[0]
                existing_rows = existing_data[1:]
                existing_df = pd.DataFrame(existing_rows, columns=existing_headers)
                existing_df = existing_df.replace('', pd.NA).dropna(how='all').fillna('')
                
                print(f"✓ 기존 데이터: {len(existing_df)}행")
                
                # 컬럼명이 다른 경우 처리
                if set(df.columns) != set(existing_df.columns):
                    print(f"⚠ 컬럼명 불일치 - 기존: {list(existing_df.columns)}, 새: {list(df.columns)}")
                    for col in existing_df.columns:
                        if col not in df.columns:
                            df[col] = ''
                    df = df[existing_df.columns]
                    print(f"✓ 컬럼명 맞춤 완료")
                
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                print(f"✓ 데이터 병합: {len(combined_df)}행")
            else:
                combined_df = df
                print("✓ 첫 데이터 업로드")
            
            # 8. 3년 이상된 데이터 삭제
            if '업로드날짜' in combined_df.columns:
                three_years_ago = (datetime.now(pytz.timezone('Asia/Seoul')) - timedelta(days=365*3)).strftime('%Y-%m-%d')
                original_len = len(combined_df)
                combined_df = combined_df[combined_df['업로드날짜'] >= three_years_ago]
                removed = original_len - len(combined_df)
                if removed > 0:
                    print(f"✓ 3년 이상된 데이터 {removed}행 삭제")
            
            # 날짜 정렬
            if '날짜' in combined_df.columns:
                combined_df = combined_df.sort_values(by='날짜', ascending=True)
                combined_df = combined_df.reset_index(drop=True)
                print(f"✓ 날짜순 정렬 완료")
            
            # 9. 시트 업데이트
            print("구글 시트 업데이트 중...")
            worksheet.clear()
            
            combined_df = combined_df.fillna('').astype(str)
            
            headers = combined_df.columns.tolist()
            data = combined_df.values.tolist()
            
            worksheet.update(values=[headers], range_name='A1')
            
            if data:
                worksheet.update(values=data, range_name='A2')
            
            print(f"✓ 구글 시트 업데이트 완료: {len(combined_df)}행")
            print(f"✓ 시트 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={worksheet.id}")
            
            return True
            
        except Exception as e:
            print(f"구글 시트 업로드 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def close(self):
        """브라우저 종료"""
        if self.driver:
            print("\n브라우저를 5초 후 종료합니다...")
            time.sleep(5)
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

if __name__ == "__main__":
    # 영풍문고 로그인 정보 (환경 변수 우선)
    import os
    USERNAME = os.getenv('YOUNGPOONG_ID', '1058745036')
    PASSWORD = os.getenv('YOUNGPOONG_PASSWORD', 'then325!')
    
    scraper = YoungpoongScraper()
    
    try:
        # 1. 드라이버 설정
        scraper.setup_driver()
        
        # 2. 빠진 날짜 확인
        missing_dates = scraper.get_missing_dates_from_sheet()
        
        if not missing_dates:
            print("\n✅ 모든 데이터가 최신 상태입니다!")
        else:
            print(f"\n📋 총 {len(missing_dates)}일의 데이터를 수집합니다.")
            
            # 3. 로그인
            if scraper.login(USERNAME, PASSWORD):
                print("\n로그인이 성공적으로 완료되었습니다!")
                
                success_count = 0
                failed_dates = []
                
                # 4. 각 날짜별로 스크랩
                for i, date in enumerate(missing_dates, 1):
                    print(f"\n{'='*60}")
                    print(f"📅 [{i}/{len(missing_dates)}] {date} 데이터 수집")
                    print(f"{'='*60}")
                    
                    try:
                        # 판매 데이터 스크랩 (DataFrame 반환)
                        df = scraper.scrape_sales_data(date)
                        
                        # 구글 시트 업로드
                        if df is not None and len(df) > 0:
                            if scraper.upload_to_google_sheets(df, date):
                                success_count += 1
                                print(f"✅ {date} 데이터 업로드 완료!")
                            else:
                                failed_dates.append(date)
                                print(f"⚠ {date} 데이터 업로드 실패")
                        else:
                            failed_dates.append(date)
                            print(f"⚠ {date} 데이터 스크랩 실패 (데이터 없음)")
                            
                    except Exception as e:
                        failed_dates.append(date)
                        print(f"⚠ {date} 처리 중 오류: {str(e)}")
                        continue
                
                # 5. 결과 요약
                print(f"\n{'='*60}")
                print("📊 데이터 수집 완료!")
                print(f"{'='*60}")
                print(f"✅ 성공: {success_count}일")
                if failed_dates:
                    print(f"⚠ 실패: {len(failed_dates)}일")
                    print(f"   실패한 날짜: {', '.join(failed_dates)}")
                print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        scraper.close()
