# -*- coding: utf-8 -*-
"""
교보문고 SCM 웹사이트 스크래핑
- Selenium으로 로그인 및 데이터 다운로드
- Google Sheets에 업로드
"""

import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import pytz


class KyoboScraper:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.driver = None
        
        # 다운로드 폴더 설정
        self.download_dir = os.path.join(os.path.expanduser('~'), 'Downloads', 'kyobo_data')
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            print(f"다운로드 폴더 생성: {self.download_dir}")
        
        # Chrome 기본 다운로드 폴더
        self.default_download_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
    
    def validate_data_integrity(self, df, query_date, worksheet):
        """데이터 무결성 검증"""
        warnings = []
        
        # 1. 필수 컬럼 존재 여부
        required_columns = ['ISBN', '도서명', '발행일', '출판사']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            warnings.append(f"필수 컬럼 누락: {', '.join(missing_columns)}")
        
        # 2. ISBN 중복 검사
        if 'ISBN' in df.columns:
            duplicates = df[df.duplicated(subset=['ISBN'], keep=False)]
            if not duplicates.empty:
                warnings.append(f"중복 ISBN 발견: {len(duplicates)}건")
        
        # 3. 날짜별 데이터 개수 확인
        try:
            existing_data = worksheet.get_all_values()
            if existing_data and len(existing_data) > 1:
                existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                if '날짜' in existing_df.columns:
                    same_date_data = existing_df[existing_df['날짜'] == query_date]
                    if not same_date_data.empty:
                        warnings.append(f"기존 데이터 존재: {query_date}에 이미 {len(same_date_data)}건 존재")
        except Exception as e:
            warnings.append(f"기존 데이터 확인 중 오류: {str(e)}")
        
        # 경고 출력
        if warnings:
            print("\n⚠️ 데이터 검증 경고:")
            for warning in warnings:
                print(f"  - {warning}")
        
        return warnings
    
    def get_missing_dates_from_sheet(self):
        """구글 시트에서 빠진 날짜 확인"""
        try:
            print("\n=== 구글 시트에서 빠진 날짜 확인 ===")
            
            # 구글 시트 연결
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            creds_paths = [
                'credentials.json',  # 현재 작업 디렉토리 (GitHub Actions)
                os.path.join(script_dir, 'credentials.json'),
                os.path.join(script_dir, '..', 'credentials.json'),
            ]
            creds_path = next((p for p in creds_paths if os.path.exists(p)), None)
            if not creds_path:
                raise FileNotFoundError("credentials.json을 찾을 수 없습니다")
            
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            # 스프레드시트 열기
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            
            # "교보문고" 시트 가져오기
            try:
                worksheet = spreadsheet.worksheet("교보문고")
                print("✓ '교보문고' 시트 찾음")
                # 기존 데이터 가져오기
                existing_data = worksheet.get_all_values()
            except gspread.WorksheetNotFound:
                # 시트가 없으면 2026-01-01부터 수집하도록 처리
                print("✓ '교보문고' 시트 없음, 2026-01-01부터 시작")
                existing_data = []

            if not existing_data or len(existing_data) <= 1:
                print("✓ 시트가 비어있음 또는 데이터 부족 - 2026-01-01부터 시작")
                valid_dates = []
            else:
                # DataFrame으로 변환
                df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                # 우선 '조회기간'을 사용하고, 없으면 '날짜' 사용
                if '조회기간' in df.columns:
                    dates_raw = df['조회기간'].tolist()
                    print("  조회기간 컬럼 사용")
                elif '날짜' in df.columns:
                    dates_raw = df['날짜'].tolist()
                    print("  날짜 컬럼 사용")
                else:
                    dates_raw = []
                    print("  날짜 관련 컬럼 없음 - 2026-01-01부터 시작")

                # YYYY-MM-DD 형식의 날짜만 추출
                valid_dates = []
                for d in dates_raw:
                    d_str = str(d).strip()
                    if len(d_str) == 10 and d_str.count('-') == 2:
                        try:
                            datetime.strptime(d_str, '%Y-%m-%d')
                            valid_dates.append(d_str)
                        except:
                            pass
                print(f"✓ 기존 날짜 개수: {len(valid_dates)}개")
                if valid_dates:
                    print(f"  최근 날짜: {max(valid_dates)}")
                    print(f"  가장 오래된 날짜: {min(valid_dates)}")
            
            # DataFrame으로 변환
            df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            
            # '날짜' 컬럼이 있는지 확인
            if '날짜' not in df.columns:
                print("⚠️ '날짜' 컬럼이 없음 - 어제 날짜 반환")
                korea_tz = pytz.timezone('Asia/Seoul')
                yesterday = datetime.now(korea_tz) - timedelta(days=1)
                return [yesterday.strftime('%Y-%m-%d')]
            
            # 날짜 컬럼 추출 및 정제
            dates_raw = df['날짜'].tolist()
            
            # YYYY-MM-DD 형식의 날짜만 추출
            valid_dates = []
            for d in dates_raw:
                d_str = str(d).strip()
                if len(d_str) == 10 and d_str.count('-') == 2:
                    try:
                        datetime.strptime(d_str, '%Y-%m-%d')
                        valid_dates.append(d_str)
                    except:
                        pass
            
            print(f"✓ 기존 날짜 개수: {len(valid_dates)}개")
            if valid_dates:
                print(f"  최근 날짜: {max(valid_dates)}")
                print(f"  가장 오래된 날짜: {min(valid_dates)}")
            
            # 2026-01-01부터 어제까지 모든 날짜 생성
            korea_tz = pytz.timezone('Asia/Seoul')
            start_date = datetime(2026, 1, 1)
            today = datetime.now(korea_tz).replace(tzinfo=None)
            yesterday = today - timedelta(days=1)

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
                print(f"✓ 빠진 날짜: {len(missing_dates)}개")
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
        options = webdriver.ChromeOptions()
        # GitHub Actions에서는 headless 모드 필수
        if os.getenv('GITHUB_ACTIONS'):
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.page_load_strategy = 'normal'
        
        # 다운로드 경로 설정
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            # ChromeDriverManager 캐시 사용 또는 시스템 PATH의 chromedriver 사용
            try:
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=options
                )
            except:
                # ChromeDriverManager 실패 시 시스템의 chromedriver 사용
                print("✓ ChromeDriverManager 실패, 시스템 chromedriver 사용")
                self.driver = webdriver.Chrome(options=options)
            
            self.driver.maximize_window()
            print("✓ Chrome 드라이버 설정 완료")
        except Exception as e:
            print(f"드라이버 설정 오류: {str(e)}")
            raise

    def safe_click(self, element, timeout=5):
        """robust click helper: scroll, try native click, then JS click, then ancestor click"""
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        except Exception:
            pass

        # try native click if element has size
        try:
            size = element.size if element else None
            if size and size.get('width', 0) > 0 and size.get('height', 0) > 0:
                try:
                    element.click()
                    return True
                except Exception:
                    pass
        except Exception:
            pass

        # try JS click
        try:
            self.driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            pass

        # try clicking ancestor nodes
        try:
            el = element
            for _ in range(4):
                parent = self.driver.execute_script("return arguments[0].parentNode;", el)
                if not parent:
                    break
                try:
                    self.driver.execute_script("arguments[0].click();", parent)
                    return True
                except Exception:
                    el = parent
        except Exception:
            pass

        return False
        
    def login(self):
        """교보문고 SCM 로그인"""
        try:
            print("교보문고 SCM 로그인 페이지로 이동 중..")
            self.driver.get("https://scm.kyobobook.co.kr/scm/login.action")
            
            # 페이지 로딩 대기
            print("페이지 로딩 대기중..")
            time.sleep(3)
            
            print("\n로그인 필드 찾는 중..")
            # 여러 방법으로 아이디 필드 찾기
            username_field = None
            
            # 1. placeholder로 찾기
            try:
                username_field = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@placeholder='ID(사업자등록번호)' or contains(@placeholder, '사업자')]"))
                )
                print("✓ 아이디 필드 찾음 (placeholder)")
            except:
                pass
            
            # 2. ID로 찾기
            if not username_field:
                possible_ids = ["loginId", "id", "userId", "user_id", "username"]
                for field_id in possible_ids:
                    try:
                        username_field = self.driver.find_element(By.ID, field_id)
                        print(f"✓ 아이디 필드 찾음 (ID: {field_id})")
                        break
                    except:
                        continue
            
            # 3. name으로 찾기
            if not username_field:
                try:
                    username_field = self.driver.find_element(By.NAME, "loginId")
                    print("✓ 아이디 필드 찾음 (NAME: loginId)")
                except:
                    pass
            
            # 4. type=text의 첫번째 필드
            if not username_field:
                try:
                    inputs = self.driver.find_elements(By.XPATH, "//input[@type='text']")
                    if inputs:
                        username_field = inputs[0]
                        print("✓ 아이디 필드 찾음 (type=text)")
                except:
                    pass
            
            if not username_field:
                # 모든 input 필드 출력 (디버깅)
                inputs = self.driver.find_elements(By.TAG_NAME, "input")
                print(f"\n페이지의 input 필드들:")
                for i, inp in enumerate(inputs[:10]):  # 처음 10개만
                    print(f"  {i+1}. type={inp.get_attribute('type')}, name={inp.get_attribute('name')}, id={inp.get_attribute('id')}, placeholder={inp.get_attribute('placeholder')}")
                raise Exception("아이디 입력 필드를 찾을 수 없습니다.")
            
            print("로그인 정보 입력 중..")
            username_field.clear()
            username_field.send_keys(self.username)
            time.sleep(0.5)
            
            # 비밀번호 입력 - 여러 방법으로 시도
            password_field = None
            try:
                # placeholder로 찾기
                password_field = self.driver.find_element(By.XPATH, "//input[@placeholder='비밀번호' or @type='password']")
                print("✓ 비밀번호 필드 찾음")
            except:
                try:
                    # ID로 찾기
                    password_field = self.driver.find_element(By.ID, "loginPassword")
                    print("✓ 비밀번호 필드 찾음 (ID)")
                except:
                    # type=password로 찾기
                    password_field = self.driver.find_element(By.XPATH, "//input[@type='password']")
                    print("✓ 비밀번호 필드 찾음 (type)")
            
            password_field.clear()
            password_field.send_keys(self.password)
            time.sleep(0.5)
            print("✓ 아이디/비밀번호 입력 완료")
            
            # 로그인 버튼 찾기
            print("로그인 버튼 찾는 중..")
            login_button = None
            
            # 1. 텍스트로 찾기
            try:
                login_button = self.driver.find_element(By.XPATH, "//button[contains(text(), '로그인')] | //a[contains(text(), '로그인')] | //input[@value='로그인']")
                print("✓ 로그인 버튼 찾음 (텍스트)")
            except:
                pass
            
            # 2. submit 타입으로 찾기
            if not login_button:
                try:
                    login_button = self.driver.find_element(By.XPATH, "//button[@type='submit'] | //input[@type='submit']")
                    print("✓ 로그인 버튼 찾음 (submit)")
                except:
                    pass
            
            # 3. onclick 이벤트로 찾기
            if not login_button:
                try:
                    login_button = self.driver.find_element(By.XPATH, "//*[contains(@onclick, 'login') or contains(@onclick, 'Login')]")
                    print("✓ 로그인 버튼 찾음 (onclick)")
                except:
                    pass
            
            if not login_button:
                raise Exception("로그인 버튼을 찾을 수 없습니다.")
            
            print("로그인 버튼 클릭...")
            login_button.click()
            
            print("로그인 처리 대기중..")
            time.sleep(5)
            
            # 로그인 성공 확인
            current_url = self.driver.current_url
            print(f"현재 URL: {current_url}")
            
            if "login" not in current_url:
                print("✓ 로그인 성공!")
                return True
            else:
                print("✗ 로그인 실패. 페이지를 확인해주세요.")
                print(f"페이지 제목: {self.driver.title}")
                return False
                
        except Exception as e:
            print(f"로그인 중 오류 발생: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def scrape_sales_data(self, target_date_str=None):
        """판매 데이터 스크랩"""
        try:
            print(f"\n=== 판매 데이터 스크랩 시작 (날짜: {target_date_str if target_date_str else '어제'}) ===")
            
            # 1. 판매정보 메뉴 찾기 및 클릭
            print("판매정보 메뉴 찾는 중..")
            sales_menu = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(text(), '판매정보')]"))
            )
            
            # headless 모드 대응: 안전한 클릭 시도
            print("판매정보 메뉴 클릭 시도..")
            if self.safe_click(sales_menu):
                print("✓ 판매정보 메뉴 클릭")
            else:
                print("✗ 판매정보 메뉴 클릭 실패")
            time.sleep(2)
            
            # 2. 판매조회 서브메뉴 클릭
            print("판매조회 메뉴 찾는 중..")
            sales_inquiry = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(text(), '판매조회')]"))
            )
            # 안전한 클릭 시도
            if self.safe_click(sales_inquiry):
                print("✓ 판매조회 메뉴 클릭")
            else:
                print("✗ 판매조회 메뉴 클릭 실패")
            time.sleep(3)
            
            # 3. 조회기간 설정
            print("\n조회기간 설정 중..")
            
            if target_date_str:
                # 지정된 날짜 사용 (YYYY-MM-DD -> YYYYMMDD)
                query_date = datetime.strptime(target_date_str, '%Y-%m-%d')
                yesterday_str = query_date.strftime('%Y%m%d')
                print(f"조회 날짜: {target_date_str} -> {yesterday_str}")
            else:
                # 어제 날짜 사용
                korea_tz = pytz.timezone('Asia/Seoul')
                today = datetime.now(korea_tz)
                yesterday = today - timedelta(days=1)
                yesterday_str = yesterday.strftime('%Y%m%d')
                print(f"오늘 날짜 (한국시간): {today.strftime('%Y-%m-%d')}")
                print(f"조회 날짜 (어제): {yesterday.strftime('%Y-%m-%d')} -> {yesterday_str}")
            
            # 페이지가 완전히 로드될때까지 대기
            time.sleep(3)
            
            # 페이지 HTML 출력(디버깅용)
            page_source = self.driver.page_source
            
            # "조회기간" 텍스트를 포함하는 행 찾기
            print("\n조회기간 행 찾기 중..")
            try:
                # 조회기간 텍스트가 있는 th 또는 td 요소 찾기
                date_label = self.driver.find_element(By.XPATH, "//*[contains(text(), '조회기간')]")
                print(f"✓ 조회기간 레이블 찾음: {date_label.tag_name}")
                
                # 해당 행(tr)의 모든 input 필드 찾기 - 여러 방법 시도
                date_row = date_label.find_element(By.XPATH, "./ancestor::tr")
                
                # 방법 1: tr 안의 모든 input
                date_inputs = date_row.find_elements(By.TAG_NAME, "input")
                # type이 text인 것만 필터링
                date_inputs = [inp for inp in date_inputs if inp.get_attribute('type') == 'text' and inp.is_displayed()]
                
                print(f"조회기간 행의 input 필드: {len(date_inputs)}개")
                for idx, inp in enumerate(date_inputs):
                    print(f"  [{idx}] id='{inp.get_attribute('id')}', name='{inp.get_attribute('name')}', class='{inp.get_attribute('class')}', type='{inp.get_attribute('type')}'")
                
                # 만약 찾지 못하면 following-sibling 시도
                if len(date_inputs) < 2:
                    print("following-sibling 방법 시도...")
                    date_inputs = date_label.find_elements(By.XPATH, "./following-sibling::*/descendant::input[@type='text']")
                    date_inputs = [inp for inp in date_inputs if inp.is_displayed()]
                    print(f"다시 찾은 필드: {len(date_inputs)}개")
                
                if len(date_inputs) >= 2:
                    # 시작일 설정 - 기존 값 지우고 새로 입력
                    start_field = date_inputs[0]
                    print(f"시작일 필드 현재 값: '{start_field.get_attribute('value')}'")
                    # 기존 값 완전히 지우기
                    self.driver.execute_script("arguments[0].value = '';", start_field)
                    time.sleep(0.3)
                    # 새 값 입력
                    self.driver.execute_script("arguments[0].value = arguments[1];", start_field, yesterday_str)
                    print(f"✓ 시작일 설정: {yesterday_str} (id: {start_field.get_attribute('id')})")
                    time.sleep(0.5)
                    
                    # 종료일 설정 - 기존 값 지우고 새로 입력
                    end_field = date_inputs[1]
                    print(f"종료일 필드 현재 값: '{end_field.get_attribute('value')}'")
                    # 기존 값 완전히 지우기
                    self.driver.execute_script("arguments[0].value = '';", end_field)
                    time.sleep(0.3)
                    # 새 값 입력
                    self.driver.execute_script("arguments[0].value = arguments[1];", end_field, yesterday_str)
                    print(f"✓ 종료일 설정: {yesterday_str} (id: {end_field.get_attribute('id')})")
                    time.sleep(0.5)
                else:
                    print(f"✗ 조회기간 행에서 충분한 input 필드를 찾지 못했습니다: {len(date_inputs)}개")
                    
            except Exception as e:
                print(f"조회기간 행 찾기 실패: {str(e)}")
                print("\n페이지의 모든 행 구조 확인 중..")
                
                # 모든 tr 요소 확인
                all_rows = self.driver.find_elements(By.XPATH, "//tr")
                for idx, row in enumerate(all_rows[:20]):
                    row_text = row.text[:100] if row.text else ""
                    if row_text:
                        print(f"  Row {idx}: {row_text}")
            
            time.sleep(1)
            
            time.sleep(1)
            
            # 4. 조회 버튼 클릭
            print("\n조회 버튼 클릭 중..")
            
            # 먼저 모든 버튼 출력 (디버깅)
            all_buttons = self.driver.find_elements(By.XPATH, "//a | //button")
            print(f"\n페이지의 모든 버튼/링크 확인 중.. (총 {len(all_buttons)}개)")
            
            조회_buttons = []
            for idx, btn in enumerate(all_buttons):
                btn_text = btn.text.strip()
                if '조회' in btn_text:
                    btn_class = btn.get_attribute('class') or ''
                    btn_id = btn.get_attribute('id') or ''
                    print(f"  [{idx}] text='{btn_text}', class='{btn_class}', id='{btn_id}'")
                    조회_buttons.append(btn)
            
            # 'btn blue' 클래스를 가진 조회 버튼 찾기 (실제 조회 버튼)
            search_button_found = False
            for btn in 조회_buttons:
                btn_class = btn.get_attribute('class') or ''
                btn_text = btn.text.strip()
                
                # 'blue'가 클래스에 포함되고 텍스트가 '조회'인 버튼
                if 'blue' in btn_class and btn_text == '조회':
                    print(f"\n✓ 실제 조회 버튼 찾음! (class: {btn_class})")
                    try:
                        if self.safe_click(btn):
                            print("✓ 조회 버튼 클릭 성공")
                        else:
                            raise Exception("조회 버튼 클릭 시도 실패")

                        # 데이터 로딩 대기 - 조회영역 테이블이 업데이트될때까지 기다림
                        print("조회 결과 로딩 대기중..")
                        time.sleep(30)  # 초기 대기시간 30초로 증가
                        
                        # 조회영역 테이블에 데이터가 있는지 확인
                        for i in range(10):  # 추가 10회 대기
                            try:
                                # ISBN 컬럼이 있는 행 찾기 (데이터가 있다는 의미)
                                data_rows = self.driver.find_elements(By.XPATH, "//table//tr[td]")
                                if len(data_rows) > 1:  # 헤더 외에 데이터 행이 있으면
                                    print(f"✓ 조회 결과 로딩 완료! (데이터 행: {len(data_rows)}개)")
                                    break
                            except:
                                pass
                            time.sleep(1)
                            print(f"  추가 대기중.. ({i+1}회)")
                        
                        time.sleep(3)  # 추가 안정화 대기
                        print("✓ 데이터 로딩 완료 - 엑셀 다운로드 준비")
                        search_button_found = True
                        break
                    except Exception as e:
                        print(f"클릭 실패: {str(e)}")
            
            if not search_button_found:
                print("✗ blue 클래스 조회 버튼을 찾지 못했습니다.")
            
            # 5. 엑셀 다운로드 버튼 클릭
            print("\n엑셀 다운로드 버튼 찾는 중..")
            try:
                # 엑셀다운 버튼 찾기 - 모달이 열릴때까지 대기
                time.sleep(2)
                
                excel_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), '엑셀다운')] | //button[contains(text(), '엑셀다운')]"))
                )
                
                # 안전한 클릭으로 엑셀 다운로드 시도
                if self.safe_click(excel_button):
                    print("✓ 엑셀 다운로드 버튼 클릭")
                else:
                    print("✗ 엑셀 다운로드 버튼 클릭 실패")
                
                # 다운로드 완료 대기
                print("다운로드 완료 대기중..")
                time.sleep(10)  # 다운로드 대기시간 증가
                
                # 다운로드된 파일 확인 - 두 폴더 모두 확인
                downloaded_files = []
                
                # 1. 설정된 다운로드 폴더 확인
                if os.path.exists(self.download_dir):
                    downloaded_files = [f for f in os.listdir(self.download_dir) if f.endswith(('.xls', '.xlsx'))]
                    if downloaded_files:
                        print(f"✓ 설정된 다운로드 폴더에서 파일 발견")
                
                # 2. Chrome 기본 다운로드 폴더 확인
                if not downloaded_files and os.path.exists(self.default_download_dir):
                    all_files = os.listdir(self.default_download_dir)
                    # 최근 10초 이내에 생성된 엑셀 파일 찾기
                    import time as time_module
                    current_time = time_module.time()
                    for file in all_files:
                        if file.endswith(('.xls', '.xlsx')):
                            file_path = os.path.join(self.default_download_dir, file)
                            if current_time - os.path.getmtime(file_path) < 15:  # 15초 이내
                                downloaded_files.append(file)
                                print(f"✓ Chrome 기본 다운로드 폴더에서 파일 발견")
                                break
                
                if downloaded_files:
                    print(f"\n✓ 엑셀 파일 다운로드 완료!")
                    for file in downloaded_files:
                        # 파일이 어느 폴더에 있는지 확인
                        if os.path.exists(os.path.join(self.download_dir, file)):
                            file_path = os.path.join(self.download_dir, file)
                        else:
                            file_path = os.path.join(self.default_download_dir, file)
                        
                        file_size = os.path.getsize(file_path)
                        print(f"  - {file} ({file_size:,} bytes)")
                        print(f"    위치: {file_path}")
                    return True
                else:
                    print("✗ 다운로드된 엑셀 파일을 찾을 수 없습니다.")
                    return False
                    
            except Exception as e:
                print(f"엑셀 다운로드 버튼 클릭 오류: {str(e)}")
                # 대체방법
                try:
                    all_excel_buttons = self.driver.find_elements(By.XPATH, "//*[contains(text(), '엑셀')]")
                    print(f"찾은 엑셀 관련 버튼: {len(all_excel_buttons)}개")
                    for idx, btn in enumerate(all_excel_buttons):
                        print(f"  [{idx}] text='{btn.text}', id='{btn.get_attribute('id')}'")
                        if '엑셀다운' in btn.text:
                            self.driver.execute_script("arguments[0].click();", btn)
                            print(f"✓ 엑셀다운 버튼 클릭 (인덱스: {idx})")
                            time.sleep(5)
                            break
                except Exception as e2:
                    print(f"대체방법 실패: {str(e2)}")
                return False
                
        except Exception as e:
            print(f"판매 데이터 스크랩 중 오류 발생: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def upload_to_google_sheets(self, excel_file_path, query_date):
        """구글 시트에 데이터 업로드"""
        try:
            print("\n=== 구글 시트 업로드 시작 ===")
            
            # 1. 엑셀 파일 읽기 - 깨끗하게 처리
            print(f"엑셀 파일 읽기: {excel_file_path}")
            
            # 엑셀 파일 전체 읽기 (헤더 없이)
            df_raw = pd.read_excel(excel_file_path, header=None)
            print(f"✓ 엑셀 원본 데이터: {len(df_raw)}행 x {len(df_raw.columns)}열")
            
            # "ISBN" 헤더가 있는 행 찾기
            header_row_idx = None
            for idx, row in df_raw.iterrows():
                row_values = [str(x) for x in row.values if pd.notna(x) and str(x).strip() != '']
                row_str = ' '.join(row_values)
                if 'ISBN' in row_str and '상품명' in row_str:
                    header_row_idx = idx
                    print(f"✓ 헤더 행 발견: {idx}행")
                    break
            
            if header_row_idx is None:
                print("✗ 헤더를 찾을 수 없습니다.")
                return False
            
            # 헤더 추출 - 빈 컬럼 제거
            headers_raw = df_raw.iloc[header_row_idx].tolist()
            
            # 유효한 헤더만 추출 (nan이 아닌 값)
            valid_col_indices = []
            clean_headers = []
            for i, header in enumerate(headers_raw):
                if pd.notna(header) and str(header).strip() != '':
                    valid_col_indices.append(i)
                    clean_headers.append(str(header).strip())
            
            print(f"✓ 유효한 컬럼: {len(clean_headers)}개")
            print(f"  컬럼명: {', '.join(clean_headers[:5])}...")
            
            # 데이터 행만 추출 (헤더 다음 행부터)
            data_rows = df_raw.iloc[header_row_idx + 1:, valid_col_indices].copy()
            data_rows.columns = clean_headers
            data_rows = data_rows.reset_index(drop=True)
            
            print(f"✓ 초기 데이터 로드: {len(data_rows)}행")
            
            # "합계" 행 제거
            mask = data_rows.apply(lambda row: any('합계' in str(cell) or '소계' in str(cell) for cell in row.values), axis=1)
            data_rows = data_rows[~mask]
            print(f"✓ 소계 행 제거 후: {len(data_rows)}행")
            
            # 모든 값이 비어있거나 nan인 행 제거
            data_rows = data_rows.dropna(how='all')
            print(f"✓ 빈 행 제거 후: {len(data_rows)}행")
            
            # ISBN 컬럼이 비어있는 행 제거 (데이터가 아닌 행)
            if 'ISBN' in data_rows.columns:
                data_rows = data_rows[data_rows['ISBN'].notna() & (data_rows['ISBN'] != '')]
                print(f"✓ ISBN 없는 행 제거 후: {len(data_rows)}행")
            
            # NaN 값을 빈 문자열로 변환
            df = data_rows.fillna('')
            
            # 2. 칼럼명 통일
            rename_dict = {
                '상품명': '도서명',
                '출판사자': '발행일',
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
            
            # 3. 구글 시트 연결
            print("구글 시트 연결 중..")
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            creds_paths = [
                'credentials.json',  # 현재 작업 디렉토리 (GitHub Actions)
                os.path.join(script_dir, 'credentials.json'),
                os.path.join(script_dir, '..', 'credentials.json'),
            ]
            creds_path = next((p for p in creds_paths if os.path.exists(p)), None)
            if not creds_path:
                raise FileNotFoundError("credentials.json을 찾을 수 없습니다")
            
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            # 4. 스프레드시트 열기
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            print("✓ 구글 시트 연결 완료")
            
            # 5. "교보문고" 시트 가져오기 또는 생성
            try:
                worksheet = spreadsheet.worksheet("교보문고")
                print("✓ 기존 '교보문고' 시트 찾음")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="교보문고", rows="1000", cols="20")
                print("✓ '교보문고' 시트 생성")
            
            # 데이터 검증 실행
            try:
                validation_warnings = self.validate_data_integrity(df, query_date, worksheet)
            except AttributeError:
                validation_warnings = []
            except Exception as e:
                print(f"✗ 데이터 검증 중 오류: {str(e)}")
                validation_warnings = []
            
            # 6. 기존 데이터 가져오기
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
                print("✓ 새 데이터(최초 업로드)")
            
            # 7. 3년 이상된 데이터만 삭제
            if '업로드날짜' in combined_df.columns:
                three_years_ago = (datetime.now(pytz.timezone('Asia/Seoul')) - timedelta(days=365*3)).strftime('%Y-%m-%d')
                original_len = len(combined_df)
                combined_df = combined_df[combined_df['업로드날짜'] >= three_years_ago]
                removed = original_len - len(combined_df)
                if removed > 0:
                    print(f"✓ 3년 이상된 데이터 {removed}행 삭제")
            
            # 8. 시트 업데이트
            print("구글 시트 업데이트 중..")
            worksheet.clear()
            
            # 데이터를 문자열로 변환하여 안전하게 처리
            combined_df = combined_df.astype(str)
            
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
        """드라이버 종료"""
        if self.driver:
            print("브라우저를 5초 후 종료합니다..")
            time.sleep(5)
            self.driver.quit()


def main():
    # 교보문고 로그인 정보 (환경 변수 우선)
    import os
    USERNAME = os.getenv('KYOBO_ID', '1058745036')
    PASSWORD = os.getenv('KYOBO_PASSWORD', 'then325325@')
    
    scraper = KyoboScraper(USERNAME, PASSWORD)
    
    try:
        # 드라이버 설정
        scraper.setup_driver()
        
        # 빠진 날짜 확인
        missing_dates = scraper.get_missing_dates_from_sheet()
        
        if not missing_dates:
            print("\n✓ 모든 데이터가 최신 상태입니다")
            return
        
        print(f"\n총 {len(missing_dates)}개의 데이터를 수집합니다")
        
        # 로그인 시도
        success = scraper.login()
        
        if success:
            print("\n로그인이 성공적으로 완료되었습니다")
            
            success_count = 0
            failed_dates = []
            
            # 각 날짜별로 스크랩
            for i, date in enumerate(missing_dates, 1):
                print(f"\n{'='*60}")
                print(f"진행 [{i}/{len(missing_dates)}] {date} 데이터 수집")
                print(f"{'='*60}")
                
                try:
                    # 판매 데이터 스크랩
                    scrape_success = scraper.scrape_sales_data(date)
                    
                    if scrape_success:
                        # 다운로드된 파일 찾기
                        downloaded_file = None
                        
                        # 1. 설정된 다운로드 폴더 확인
                        import time as time_module
                        current_time = time_module.time()
                        
                        for file in os.listdir(scraper.download_dir):
                            if file.endswith(('.xls', '.xlsx')) and '교보문고' in file:
                                file_path = os.path.join(scraper.download_dir, file)
                                if current_time - os.path.getmtime(file_path) < 30:  # 30초 이내
                                    downloaded_file = file_path
                                    break
                        
                        # 2. Chrome 기본 다운로드 폴더에서 찾기
                        if not downloaded_file:
                            for file in os.listdir(scraper.default_download_dir):
                                if file.endswith(('.xls', '.xlsx')) and '교보문고' in file:
                                    file_path = os.path.join(scraper.default_download_dir, file)
                                    if current_time - os.path.getmtime(file_path) < 30:  # 30초 이내
                                        downloaded_file = file_path
                                        break
                        
                        if downloaded_file:
                            print(f"\n다운로드 파일 발견: {downloaded_file}")
                            # 구글 시트 업로드
                            if scraper.upload_to_google_sheets(downloaded_file, date):
                                success_count += 1
                                print(f"✓ {date} 데이터 업로드 완료!")
                            else:
                                failed_dates.append(date)
                                print(f"✗ {date} 데이터 업로드 실패")
                        else:
                            failed_dates.append(date)
                            print(f"\n✗ {date} 다운로드 파일을 찾을 수 없습니다.")
                    else:
                        failed_dates.append(date)
                        print(f"✗ {date} 데이터 스크랩 실패")
                        
                except Exception as e:
                    failed_dates.append(date)
                    print(f"✗ {date} 처리 중 오류: {str(e)}")
                    continue
            
            # 결과 요약
            print(f"\n{'='*60}")
            print("전체 데이터 수집 완료!")
            print(f"{'='*60}")
            print(f"✓ 성공: {success_count}건")
            if failed_dates:
                print(f"✗ 실패: {len(failed_dates)}건")
                print(f"   실패한 날짜: {', '.join(failed_dates)}")
            print(f"{'='*60}\n")
        else:
            print("\n로그인에 실패했습니다.")
            
    except Exception as e:
        print(f"오류 발생: {str(e)}")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
