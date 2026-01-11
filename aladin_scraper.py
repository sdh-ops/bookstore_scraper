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


class AladinScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
    
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
            
            # 알라딘 시트 확인
            valid_dates = []  # 초기화
            last_date = None
            
            try:
                worksheet = spreadsheet.worksheet("알라딘")
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
                        print(f"✓ 조회기간 컬럼 없음, 2026-01-01부터 시작")
                else:
                    # 시트가 비어있으면 2026-01-01부터
                    korea_tz = pytz.timezone('Asia/Seoul')
                    last_date = korea_tz.localize(datetime(2025, 12, 31))
                    print(f"✓ 시트 비어있음, 2026-01-01부터 시작")
            except:
                # 알라딘 시트가 없으면 2026-01-01부터
                korea_tz = pytz.timezone('Asia/Seoul')
                last_date = korea_tz.localize(datetime(2025, 12, 31))
                print(f"✓ 알라딘 시트 없음, 2026-01-01부터 시작")
            
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
        download_dir = os.path.join(os.path.dirname(__file__), 'downloads')
        os.makedirs(download_dir, exist_ok=True)
        
        prefs = {
            "download.default_directory": download_dir,
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
    
    def validate_data_integrity(self, df, target_date, worksheet):
        """데이터 무결성 검수"""
        warnings = []
        
        try:
            # 1. 중복 날짜 검사
            existing_data = worksheet.get_all_values()
            if existing_data and len(existing_data) > 1:
                existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                if '조회기간' in existing_df.columns:
                    if target_date in existing_df['조회기간'].values:
                        warnings.append(f"⚠ 중복: {target_date} 데이터가 이미 존재합니다")
            
            # 2. 데이터 유사도 검사 (최근 3일치와 비교)
            if existing_data and len(existing_data) > 4:
                recent_data = existing_data[-3:]
                recent_isbns = set()
                for row in recent_data:
                    if len(row) > 3:  # ISBN 컬럼 존재
                        recent_isbns.add(row[3])
                
                current_isbns = set(df['ISBN'].values) if 'ISBN' in df.columns else set()
                similarity = len(current_isbns & recent_isbns) / len(current_isbns) if current_isbns else 0
                
                if similarity > 0.95:
                    warnings.append(f"⚠ 유사도 높음: 최근 데이터와 {similarity*100:.1f}% 유사")
            
            # 3. 데이터 품질 검사
            if df.empty:
                warnings.append("⚠ 빈 데이터")
            elif '판매권수' in df.columns:
                sales_sum = df['판매권수'].astype(str).str.replace(',', '').astype(float).sum()
                if sales_sum == 0:
                    warnings.append("⚠ 모든 판매수량이 0")
            
            # 경고 출력
            if warnings:
                print("\n=== 🔍 데이터 검수 결과 ===")
                for w in warnings:
                    print(w)
                print("=" * 50)
            
        except Exception as e:
            print(f"검수 중 오류: {str(e)}")
        
        return warnings
        
    def login(self, user_id, password):
        """알라딘 공급자 로그인"""
        try:
            print("알라딘 공급자 페이지로 이동 중...")
            self.driver.get("https://www.aladin.co.kr/supplier/wmain.aspx")
            time.sleep(3)
            
            print("페이지 로딩 대기 중...")
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
            
            # 아이디 입력 필드 찾기
            id_field = None
            id_selectors = [
                (By.NAME, "SupplierId"),
                (By.ID, "SupplierId"),
                (By.NAME, "txtID"),
                (By.ID, "txtID"),
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
            pw_field = self.driver.find_element(By.NAME, "Password")
            pw_field.clear()
            pw_field.send_keys(password)
            print("✓ 비밀번호 입력")
            
            # 로그인 버튼 클릭
            login_button_selectors = [
                (By.CSS_SELECTOR, "input[type='image']"),
                (By.CSS_SELECTOR, "input[type='submit']"),
                (By.XPATH, "//input[@type='image']"),
            ]
            
            login_button = None
            for by, selector in login_button_selectors:
                try:
                    login_button = self.driver.find_element(by, selector)
                    if login_button:
                        print(f"✓ 로그인 버튼 찾음: {by}={selector}")
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
            print(f"\n=== 알라딘 판매 데이터 스크랩 시작 (날짜: {target_date_str if target_date_str else '어제'}) ===")
            
            # 1. 판매 통계 메뉴 클릭
            print("판매 통계 메뉴 찾는 중...")
            time.sleep(2)
            
            # 여러 가능한 선택자 시도
            sales_menu_selectors = [
                "//a[contains(text(), '판매 통계')]",
                "//a[contains(text(), '판매통계')]",
                "//td[@class='menu']//a[contains(text(), '판매')]",
                "//a[@href and contains(@href, '판매')]",
            ]
            
            sales_menu = None
            for selector in sales_menu_selectors:
                try:
                    sales_menu = self.driver.find_element(By.XPATH, selector)
                    if sales_menu:
                        print(f"✓ 판매 통계 메뉴 찾음: {selector}")
                        break
                except:
                    continue
            
            if not sales_menu:
                print("⚠ 판매 통계 메뉴를 찾을 수 없습니다.")
                print("페이지의 모든 링크 확인:")
                links = self.driver.find_elements(By.TAG_NAME, "a")
                for i, link in enumerate(links[:20]):
                    try:
                        text = link.text.strip()
                        href = link.get_attribute('href')
                        if text:
                            print(f"  [{i}] {text} -> {href}")
                    except:
                        pass
                return False
            
            # 메뉴 클릭
            self.driver.execute_script("arguments[0].click();", sales_menu)
            print("✓ 판매 통계 메뉴 클릭")
            time.sleep(3)
            
            # 2. 날짜 설정
            print("\n날짜 설정 중...")
            
            if target_date_str:
                # 지정된 날짜 사용 (YYYY-MM-DD)
                query_date = datetime.strptime(target_date_str, '%Y-%m-%d')
                print(f"조회 날짜: {target_date_str}")
            else:
                # 어제 날짜 사용
                korea_tz = pytz.timezone('Asia/Seoul')
                today = datetime.now(korea_tz)
                query_date = (today - timedelta(days=1))
                print(f"오늘: {today.strftime('%Y-%m-%d')}")
                print(f"조회 날짜 (어제): {query_date.strftime('%Y-%m-%d')}")
            
            query_year = str(query_date.year)
            query_month = str(query_date.month)
            query_day = str(query_date.day)
            print(f"설정값: {query_year}년 {query_month}월 {query_day}일")
            
            time.sleep(2)  # 페이지 로딩 대기
            
            # 페이지의 모든 select 요소 확인
            print("\n페이지의 select 요소들:")
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            for i, sel in enumerate(selects):
                try:
                    name = sel.get_attribute('name')
                    print(f"  [{i}] name={name}, options={len(sel.find_elements(By.TAG_NAME, 'option'))}개")
                except:
                    pass
            
            # select 요소들을 순서대로 가져오기
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            
            if len(selects) >= 6:
                # 시작 날짜 (첫 3개 select)
                print("\n시작 날짜 설정:")
                try:
                    start_year = Select(selects[0])
                    start_year.select_by_value(query_year)
                    print(f"✓ 시작 년도: {query_year}")
                except Exception as e:
                    print(f"시작 년도 선택 실패: {e}")
                
                time.sleep(0.3)
                
                try:
                    start_month = Select(selects[1])
                    start_month.select_by_value(query_month)
                    print(f"✓ 시작 월: {query_month}")
                except Exception as e:
                    print(f"시작 월 선택 실패: {e}")
                
                time.sleep(0.3)
                
                try:
                    start_day = Select(selects[2])
                    start_day.select_by_value(query_day)
                    print(f"✓ 시작 일: {query_day}")
                except Exception as e:
                    print(f"시작 일 선택 실패: {e}")
                
                # 종료 날짜 (다음 3개 select)
                print("\n종료 날짜 설정:")
                time.sleep(0.5)
                
                try:
                    end_year = Select(selects[3])
                    end_year.select_by_value(query_year)
                    print(f"✓ 종료 년도: {query_year}")
                except Exception as e:
                    print(f"종료 년도 선택 실패: {e}")
                
                time.sleep(0.3)
                
                try:
                    end_month = Select(selects[4])
                    end_month.select_by_value(query_month)
                    print(f"✓ 종료 월: {query_month}")
                except Exception as e:
                    print(f"종료 월 선택 실패: {e}")
                
                time.sleep(0.3)
                
                try:
                    end_day = Select(selects[5])
                    end_day.select_by_value(query_day)
                    print(f"✓ 종료 일: {query_day}")
                except Exception as e:
                    print(f"종료 일 선택 실패: {e}")
            else:
                print(f"⚠ select 요소가 충분하지 않습니다. (발견: {len(selects)}개)")
            
            time.sleep(1)
            
            # 3. 조회 버튼 클릭
            print("\n조회 버튼 찾는 중...")
            query_button_selectors = [
                "//input[@type='submit' and @value='조회']",
                "//input[@type='button' and @value='조회']",
                "//button[contains(text(), '조회')]",
                "//a[contains(text(), '조회')]",
            ]
            
            query_button = None
            for selector in query_button_selectors:
                try:
                    query_button = self.driver.find_element(By.XPATH, selector)
                    if query_button:
                        print(f"✓ 조회 버튼 찾음")
                        break
                except:
                    continue
            
            if query_button:
                self.driver.execute_script("arguments[0].click();", query_button)
                print("✓ 조회 버튼 클릭")
                time.sleep(5)
                
                # 데이터 테이블 파싱
                print("\n=== 테이블 데이터 파싱 ===")
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                
                # 4번째 테이블이 실제 데이터 테이블
                if len(tables) >= 4:
                    data_table = tables[3]  # 0-based index
                    
                    # 테이블의 모든 행 가져오기
                    rows = data_table.find_elements(By.TAG_NAME, "tr")
                    print(f"✓ 데이터 테이블 발견: {len(rows)}행")
                    
                    # 헤더와 데이터 분리
                    table_data = []
                    for i, row in enumerate(rows):
                        cells = row.find_elements(By.TAG_NAME, "td") + row.find_elements(By.TAG_NAME, "th")
                        if cells:
                            cell_texts = []
                            for cell in cells:
                                text = cell.text.strip()
                                # "추이", "경향" 링크는 제외
                                if text and text not in ['추이', '경향']:
                                    cell_texts.append(text)
                            
                            # 유효한 데이터만 추가 (8개 컬럼: 출판사, 도서명, ISBN, 저자, 정가, 판매권수)
                            if len(cell_texts) >= 6:
                                # "총 계" 행은 제외
                                if '총 계' not in ' '.join(cell_texts) and '총계' not in ' '.join(cell_texts):
                                    table_data.append(cell_texts[:6])  # 처음 6개 컬럼만
                    
                    if table_data:
                        print(f"✓ 파싱된 데이터: {len(table_data)}행")
                        
                        # DataFrame 생성
                        df = pd.DataFrame(table_data[1:], columns=table_data[0])  # 첫 행은 헤더
                        print(f"✓ DataFrame 생성: {len(df)}행 x {len(df.columns)}열")
                        print(f"  컬럼: {', '.join(df.columns.tolist())}")
                        
                        return df, query_date.strftime('%Y-%m-%d')
                    else:
                        print("⚠ 파싱된 데이터가 없습니다.")
                        return None, None
                else:
                    print("⚠ 데이터 테이블을 찾을 수 없습니다.")
                    return None, None
            else:
                print("⚠ 조회 버튼을 찾을 수 없습니다.")
                return None, None
            
        except Exception as e:
            print(f"데이터 스크랩 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None
    
    def upload_to_google_sheets(self, df, query_date):
        """구글 시트에 데이터 업로드"""
        try:
            print("\n=== 구글 시트 업로드 시작 ===")
            
            if df is None or df.empty:
                print("⚠ 업로드할 데이터가 없습니다.")
                return False
            
            # 1. 데이터 정제 - 정가와 판매권수에서 단위 제거
            if '정가' in df.columns:
                # "10,000원" -> "10000"
                df['정가'] = df['정가'].astype(str).str.replace('원', '').str.replace(',', '').str.strip()
                print(f"✓ 정가 단위 제거 완료")
            
            if '판매권수' in df.columns:
                # "123권" -> "123"
                df['판매권수'] = df['판매권수'].astype(str).str.replace('권', '').str.replace(',', '').str.strip()
                print(f"✓ 판매권수 단위 제거 완료")
            
            # 2. 칼럼명 통일
            rename_dict = {
                '조회기간': '날짜'
            }
            for old_name, new_name in rename_dict.items():
                if old_name in df.columns:
                    df.rename(columns={old_name: new_name}, inplace=True)
                    print(f"✓ 칼럼명 변경: {old_name} → {new_name}")
            
            # 3. 업로드날짜, 날짜 컬럼 추가 (맨 앞에)
            upload_date = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
            df.insert(0, '날짜', query_date)
            df.insert(0, '업로드날짜', upload_date)
            print(f"✓ 업로드날짜({upload_date}), 날짜({query_date}) 컬럼 추가")
            print(f"✓ 업로드할 데이터: {len(df)}행")
            
            # 2. 구글 시트 연결
            print("구글 시트 연결 중...")
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            creds_path = _locate_credentials_file()
            if not creds_path:
                raise FileNotFoundError('credentials.json not found; set GOOGLE_CREDENTIALS secret or upload credentials.json')
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            # 3. 스프레드시트 열기
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            print("✓ 구글 시트 연결 완료")
            
            # 4. "알라딘" 시트 가져오기 또는 생성
            try:
                worksheet = spreadsheet.worksheet("알라딘")
                print("✓ 기존 '알라딘' 시트 찾음")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="알라딘", rows="1000", cols="20")
                print("✓ '알라딘' 시트 생성")
            
            # 데이터 검수 수행
            try:
                validation_warnings = self.validate_data_integrity(df, query_date, worksheet)
            except AttributeError:
                # validate_data_integrity 함수가 없는 경우 (이전 버전)
                validation_warnings = []
            except Exception as e:
                print(f"⚠ 데이터 검수 중 오류: {str(e)}")
                validation_warnings = []
            
            # 5. 기존 데이터 가져오기
            existing_data = worksheet.get_all_values()
            
            if existing_data and len(existing_data) > 1:
                # 헤더와 데이터 분리
                existing_headers = existing_data[0]
                existing_rows = existing_data[1:]
                
                # DataFrame으로 변환
                existing_df = pd.DataFrame(existing_rows, columns=existing_headers)
                
                # 빈 행 제거
                existing_df = existing_df.replace('', pd.NA).dropna(how='all').fillna('')
                
                print(f"✓ 기존 데이터: {len(existing_df)}행")
                
                # 새 데이터와 병합
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                print(f"✓ 데이터 병합: {len(combined_df)}행")
            else:
                combined_df = df
                print("✓ 첫 데이터 업로드")
            
            # 6. 3년 이상된 데이터 삭제
            if '업로드날짜' in combined_df.columns:
                three_years_ago = (datetime.now(pytz.timezone('Asia/Seoul')) - timedelta(days=365*3)).strftime('%Y-%m-%d')
                original_len = len(combined_df)
                combined_df = combined_df[combined_df['업로드날짜'] >= three_years_ago]
                removed = original_len - len(combined_df)
                if removed > 0:
                    print(f"✓ 3년 이상된 데이터 {removed}행 삭제")
                        # Sort by date
            if '날짜' in combined_df.columns:
                combined_df = combined_df.sort_values('날짜').reset_index(drop=True)
                        # 7. 시트 업데이트
            print("구글 시트 업데이트 중...")
            worksheet.clear()
            
            # 데이터를 문자열로 변환하여 안전하게 처리
            combined_df = combined_df.fillna('').astype(str)
            
            # 헤더와 데이터 분리
            headers = combined_df.columns.tolist()
            data = combined_df.values.tolist()
            
            # 헤더 쓰기
            worksheet.update(values=[headers], range_name='A1')
            
            # 데이터 쓰기 (있는 경우만)
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
    # 알라딘 로그인 정보 (환경 변수 우선)
    import os
    USERNAME = os.getenv('ALADIN_ID', '1058745036')
    PASSWORD = os.getenv('ALADIN_PASSWORD', '45036')
    
    scraper = AladinScraper()
    
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
                        # 판매 데이터 스크랩
                        df, _ = scraper.scrape_sales_data(date)
                        
                        # 구글 시트 업로드
                        if df is not None:
                            if scraper.upload_to_google_sheets(df, date):
                                success_count += 1
                                print(f"✅ {date} 데이터 업로드 완료!")
                            else:
                                failed_dates.append(date)
                                print(f"⚠ {date} 데이터 업로드 실패")
                        else:
                            failed_dates.append(date)
                            print(f"⚠ {date} 데이터 스크랩 실패")
                            
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
