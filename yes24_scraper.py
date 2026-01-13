from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import sys
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


class Yes24Scraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.download_dir = None
        self.external_auth_code = None

    def setup_driver(self, headless=True):
        """Chrome 드라이버 설정"""
        chrome_options = Options()
        # headless 모드 설정 (기본은 True로 하여 브라우저를 보이지 않게 실행)
        # 헤드리스 우선순위: 함수 인자 > 환경변수
        env_headless = os.getenv('HEADLESS') == '1' or os.getenv('CI') or os.getenv('GITHUB_ACTIONS')
        use_headless = bool(headless) or env_headless
        if use_headless:
            try:
                chrome_options.add_argument('--headless=new')
            except Exception:
                chrome_options.add_argument('--headless')
            # headless 환경에서 안정적으로 동작하도록 창 크기 지정
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-gpu')
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
        
        self.wait = WebDriverWait(self.driver, 15)
        print("✓ Chrome 드라이버 설정 완료")
    
    def get_missing_dates_from_sheet(self):
        """구글시트에서 빠진 날짜들 확인 (2026-01-01부터)"""
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
            
            # YES24 시트에서 이미 있는 날짜들 확인
            existing_dates = set()
            
            try:
                worksheet = spreadsheet.worksheet("YES24")
                existing_data = worksheet.get_all_values()
                
                if existing_data and len(existing_data) > 1:
                    df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                    print(f"  전체 데이터 행 수: {len(df)}")
                    
                    if '날짜' in df.columns:
                        dates = df['날짜'].tolist()
                        # 날짜 형식 필터링 (YYYY-MM-DD)
                        valid_dates = [d for d in dates if d and len(d) == 10 and '-' in d and d.count('-') == 2]
                        existing_dates = set(valid_dates)
                        
                        print(f"  유효한 날짜 개수: {len(existing_dates)}개")
                        if existing_dates:
                            print(f"  날짜 범위: {min(existing_dates)} ~ {max(existing_dates)}")
            except:
                print(f"✓ YES24 시트 없음 또는 비어있음")
            
            # 2025-09-01부터 어제까지 모든 날짜 생성
            korea_tz = pytz.timezone('Asia/Seoul')
            start_date = korea_tz.localize(datetime(2025, 9, 1))
            today = datetime.now(korea_tz)
            yesterday = today - timedelta(days=1)
            
            # 모든 날짜 생성
            all_dates = []
            current = start_date
            while current <= yesterday:
                all_dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            
            # 빠진 날짜 = 모든 날짜 - 시트에 있는 날짜
            missing_dates = [d for d in all_dates if d not in existing_dates]
            missing_dates.sort()
            
            if missing_dates:
                print(f"✓ 빠진 날짜: {len(missing_dates)}일")
                for date in missing_dates[:10]:
                    print(f"  - {date}")
                if len(missing_dates) > 10:
                    print(f"  ... 외 {len(missing_dates) - 10}일")
            else:
                print("✓ 빠진 날짜 없음 (2026-01-01부터 최신 상태)")
            
            return missing_dates
            
            while current <= yesterday:
                missing_dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            
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
        
    def login_with_sms(self, user_id="thenan1", password="thenan2525!", phone_choice=None, auth_code=None, phone_number=None, progress_callback=None, auth_wait_timeout=300):
        """YES24 SCM 로그인 (SMS 인증 포함)"""
        try:
            print("YES24 SCM 페이지로 이동 중...")
            self.driver.get("https://scm.yes24.com/")
            time.sleep(3)
            print(f"현재 URL: {self.driver.current_url}")
            
            # 아이디 입력
            print("\n아이디/비밀번호 입력 중...")
            
            # 페이지의 input 필드 확인
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            print(f"페이지의 input 필드: {len(inputs)}개")
            for i, inp in enumerate(inputs[:10]):
                try:
                    name = inp.get_attribute('name')
                    id_attr = inp.get_attribute('id')
                    type_attr = inp.get_attribute('type')
                    print(f"  [{i}] name={name}, id={id_attr}, type={type_attr}")
                except:
                    pass
            
            # 아이디 필드 찾기 (여러 방법 시도)
            id_field = None
            id_selectors = [
                (By.NAME, "userId"),
                (By.NAME, "userid"),
                (By.NAME, "id"),
                (By.ID, "userId"),
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
            pw_field = None
            pw_selectors = [
                (By.NAME, "password"),
                (By.NAME, "passwd"),
                (By.NAME, "pwd"),
                (By.ID, "password"),
                (By.CSS_SELECTOR, "input[type='password']"),
            ]
            
            for by, selector in pw_selectors:
                try:
                    pw_field = self.driver.find_element(by, selector)
                    if pw_field:
                        print(f"✓ 비밀번호 필드 찾음: {by}={selector}")
                        break
                except:
                    pass
            
            if not pw_field:
                print("⚠ 비밀번호 필드를 찾을 수 없습니다.")
                return False
            
            pw_field.clear()
            pw_field.send_keys(password)
            print("✓ 비밀번호 입력")
            
            # 로그인 버튼 클릭
            try:
                login_button = self.driver.find_element(By.XPATH, "//button[contains(text(), '로그인')]")
                login_button.click()
                print("✓ 로그인 버튼 클릭")
                time.sleep(3)
            except:
                print("⚠ 로그인 버튼을 찾을 수 없습니다.")
            
            print(f"현재 URL: {self.driver.current_url}")
            
            # SMS 인증 페이지로 이동했는지 확인
            print(f"현재 URL: {self.driver.current_url}")
            time.sleep(2)
            
            # SMS 인증 화면 감지
            if "sms" in self.driver.current_url.lower() or "SMSAuth" in self.driver.current_url:
                print("\n=== SMS 인증 화면 감지 ===")

                # 1. 휴대폰번호 선택
                # GUI 나 외부에서 전달 가능: phone_choice는 '1'/'2' 또는 실제 전화번호 문자열일 수 있음
                if progress_callback:
                    try:
                        progress_callback({'event': 'choose_phone', 'text': '휴대폰 번호를 선택하세요'})
                    except Exception:
                        pass

                # Allow selection via parameter, environment variable, or interactive prompt.
                # 우선순위: phone_number param > env PHONE_CHOICE > phone_choice numeric ('1'/'2') > phone_choice string > interactive prompt > 기본 매핑
                selected_phone = None
                # 1) explicit phone_number parameter
                if phone_number:
                    selected_phone = phone_number

                # 2) environment variable override (PHONE_CHOICE)
                if not selected_phone:
                    env_choice = os.getenv('PHONE_CHOICE')
                    if env_choice:
                        env_choice = env_choice.strip()
                        if env_choice in ('1', '2'):
                            phone_choice = env_choice
                        elif env_choice in ('01094603191', '01040435756'):
                            selected_phone = env_choice

                # 3) phone_choice parameter (numeric selection or direct number)
                if not selected_phone and phone_choice:
                    if phone_choice in ['1', '2']:
                        if phone_choice == '1':
                            selected_phone = '01094603191'
                        else:
                            selected_phone = '01040435756'
                    elif isinstance(phone_choice, str) and phone_choice.isdigit():
                        selected_phone = phone_choice

                # 4) interactive prompt when possible (local runs) or wait for web selection
                if not selected_phone:
                    # If OTP server is enabled, wait for phone selection from web UI
                    if os.getenv('USE_OTP_SERVER') == '1':
                        try:
                            from otp_server import wait_for_phone
                            print("Waiting for phone selection from web…")
                            phone_sel = wait_for_phone(timeout=auth_wait_timeout)
                            if phone_sel:
                                selected_phone = phone_sel
                                print(f"Selected phone from web: {selected_phone}")
                        except Exception as e:
                            print(f"wait_for_phone failed: {e}")

                    if not selected_phone:
                        try:
                            if sys.stdin and sys.stdin.isatty():
                                prompt = "휴대폰 선택: 1) 01094603191  2) 01040435756  (1/2, default 2): "
                                choice = input(prompt).strip()
                                if choice == '1':
                                    selected_phone = '01094603191'
                                else:
                                    selected_phone = '01040435756'
                        except Exception:
                            pass

                # 5) fallback default
                if not selected_phone:
                    selected_phone = '01040435756'

                print(f"✓ 선택된 전화번호: {selected_phone}")

                print("="*50 + "\n")

                # 2. 휴대폰번호 입력
                print(f"휴대폰 번호 자동 입력 중: {selected_phone}")
                try:
                    phone_field = None
                    phone_selectors = [
                        "input[type='tel']",
                        "input[type='text']",
                        "input[type='number']",
                        "input[name*='phone']",
                        "input[id*='phone']",
                        "input[name*='hp']",
                        "input[id*='hp']",
                        "input[placeholder*='휴대폰']",
                        "input[placeholder*='핸드폰']",
                    ]
                    for sel in phone_selectors:
                        try:
                            elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
                            for e in elems:
                                try:
                                    if e.is_displayed() and e.is_enabled():
                                        phone_field = e
                                        break
                                except:
                                    continue
                            if phone_field:
                                break
                        except:
                            continue

                    if not phone_field:
                        # last resort: any visible text-like input
                        try:
                            inputs = self.driver.find_elements(By.TAG_NAME, 'input')
                            for e in inputs:
                                t = e.get_attribute('type') or ''
                                if t.lower() in ('text', 'tel', 'number'):
                                    try:
                                        if e.is_displayed() and e.is_enabled():
                                            phone_field = e
                                            break
                                    except:
                                        continue
                        except:
                            pass

                    if phone_field:
                        phone_field.clear()
                        phone_field.send_keys(selected_phone)
                        print(f"✓ 휴대폰 번호 입력 완료")
                    else:
                        print("⚠ 휴대폰 입력 필드를 찾을 수 없습니다.")
                except Exception as e:
                    print(f"⚠ 휴대폰 번호 입력 실패: {e}")
                
                time.sleep(1)

                # 3. 인증번호 요청 버튼 클릭
                print("인증번호 요청 버튼 클릭 중...")
                try:
                    request_button = self.driver.find_element(By.XPATH, "//button[contains(text(), '인증번호 요청')]")
                    request_button.click()
                    print("✓ 인증번호 요청 완료")
                except Exception as e:
                    print(f"⚠ 인증번호 요청 버튼 클릭 실패: {e}")

                time.sleep(1)

                # 알림창 확인 버튼 자동 클릭
                print("알림창 확인 버튼 클릭 중...")
                try:
                    # "확인" 버튼 찾기
                    confirm_button = self.driver.find_element(By.XPATH, "//button[contains(text(), '확인')]")
                    confirm_button.click()
                    print("✓ 알림창 확인 완료")
                except Exception as e:
                    print(f"알림창 처리: {e}")

                time.sleep(1)

                # 4. 사용자로부터 인증번호 입력받기
                # Notify external UI that auth code was requested
                if progress_callback:
                    try:
                        progress_callback({'event': 'auth_requested', 'text': '인증번호가 요청되었습니다'})
                    except Exception:
                        pass

                # If GUI or external caller provided auth_code, use it. Otherwise wait for external_auth_code,
                # optionally wait via OTP web server (USE_OTP_SERVER=1), or prompt interactively.
                if auth_code is None:
                    # 1) check for external_auth_code set by GUI caller
                    waited = 0
                    poll_interval = 1
                    while True:
                        if self.external_auth_code:
                            auth_code = str(self.external_auth_code).strip()
                            break
                        if waited >= auth_wait_timeout:
                            break
                        time.sleep(poll_interval)
                        waited += poll_interval

                    # 2) if still no auth_code and OTP server requested, use it
                    if not auth_code and os.getenv('USE_OTP_SERVER') == '1':
                        try:
                            from otp_server import wait_for_otp
                            print("Waiting for OTP from web…")
                            otp = wait_for_otp(timeout=auth_wait_timeout)
                            if otp:
                                auth_code = str(otp).strip()
                                print(f"Received OTP from web: {auth_code}")
                        except Exception as e:
                            print(f"OTP server wait failed: {e}")

                    # 3) interactive fallback when running locally without progress_callback
                    if not auth_code and not progress_callback:
                        try:
                            auth_code = input("받은 인증번호를 입력하세요: ").strip()
                        except Exception:
                            pass

                if not auth_code:
                    print("⚠ 인증번호가 제공되지 않았습니다.")
                else:
                    print("="*50 + "\n")

                # 5. 인증번호 필드에 자동 입력
                print("인증번호 자동 입력 중...")
                try:
                    auth_field = None
                    auth_selectors = [
                        "input[placeholder*='인증']",
                        "input[name*='auth']",
                        "input[id*='auth']",
                        "input[type='tel']",
                        "input[type='number']",
                        "input[type='text']",
                        "input[placeholder*='인증번호']",
                    ]
                    for sel in auth_selectors:
                        try:
                            elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
                            for e in elems:
                                try:
                                    if e.is_displayed() and e.is_enabled():
                                        auth_field = e
                                        break
                                except:
                                    continue
                            if auth_field:
                                break
                        except:
                            continue

                    if not auth_field:
                        # fallback: consider any visible short input fields (likely OTP)
                        try:
                            inputs = self.driver.find_elements(By.TAG_NAME, 'input')
                            for e in inputs:
                                try:
                                    if not e.is_displayed() or not e.is_enabled():
                                        continue
                                    val = e.get_attribute('value') or ''
                                    t = (e.get_attribute('type') or '').lower()
                                    # likely empty and short type
                                    if t in ('tel', 'number', 'text') and len((val or '').strip()) <= 6:
                                        auth_field = e
                                        break
                                except:
                                    continue
                        except:
                            pass

                    if auth_field:
                        auth_field.clear()
                        auth_field.send_keys(auth_code)
                        print(f"✓ 인증번호 입력: {auth_code}")
                        if progress_callback:
                            try:
                                progress_callback({'event': 'auth_entered', 'text': f'인증번호 입력: {auth_code}'})
                            except Exception:
                                pass
                    else:
                        print("⚠ 인증번호 입력 필드를 찾을 수 없습니다.")
                except Exception as e:
                    print(f"⚠ 인증번호 입력 실패: {e}")
                
                time.sleep(1)

                # 6. 인증 버튼 클릭
                print("인증 버튼 클릭 중...")
                try:
                    # "인증" 버튼만 찾기 (인증번호 요청이 아닌)
                    auth_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), '인증')]")
                    # 두 번째 버튼이 "인증" 버튼 (첫 번째는 "인증번호 요청")
                    if len(auth_buttons) >= 2:
                        auth_button = auth_buttons[1]
                    else:
                        # 정확히 "인증"만 있는 버튼 찾기
                        auth_button = self.driver.find_element(By.XPATH, "//button[text()='인증' or normalize-space(text())='인증']")
                    
                    auth_button.click()
                    print("✓ 인증 버튼 클릭 완료")
                except Exception as e:
                    print(f"⚠ 인증 버튼 클릭 실패: {e}")
                    print("⚠ 모든 버튼 확인:")
                    buttons = self.driver.find_elements(By.TAG_NAME, "button")
                    for i, btn in enumerate(buttons):
                        try:
                            print(f"  [{i}] {btn.text}")
                        except:
                            pass
                
                time.sleep(2)

                # 7. "정상적으로 인증 되었습니다" 알림창 확인 버튼 클릭
                print("인증 완료 알림창 확인 중...")
                try:
                    confirm_button = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '확인')]"))
                    )
                    confirm_button.click()
                    print("✓ 알림창 확인 버튼 클릭 완료")
                except Exception as e:
                    print(f"알림창 처리: {e}")
                
                time.sleep(3)
            else:
                print("\n⚠ SMS 인증 화면을 찾을 수 없습니다.")
                print("⚠ 수동으로 SMS 인증을 완료해주세요.")
                print("작업이 완료되면 엔터를 눌러주세요...")
                input()
            
            print(f"현재 URL: {self.driver.current_url}")
            print("✓ 로그인 완료!")
            
            return True
            
        except Exception as e:
            print(f"로그인 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def scrape_sales_data(self, target_date_str):
        """판매 데이터 스크랩 - 자동화 버전"""
        try:
            print(f"\n=== YES24 판매 데이터 스크랩 (날짜: {target_date_str}) ===")
            
            # 페이지 완전히 로드될 때까지 대기
            print("페이지 로딩 대기 중...")
            time.sleep(3)
            
            print(f"현재 URL: {self.driver.current_url}")
            
            # 0. 햄버거 메뉴 확인 및 클릭
            print("햄버거 메뉴 확인 중...")
            hamburger_found = False
            
            hamburger_selectors = [
                (By.CSS_SELECTOR, "button.navbar-toggler"),
                (By.CSS_SELECTOR, ".navbar-toggler"),
                (By.XPATH, "//button[contains(@class, 'navbar-toggler')]"),
                (By.XPATH, "//button[contains(@class, 'menu-toggle')]"),
                (By.XPATH, "//button[@aria-label='Toggle navigation']"),
                (By.XPATH, "//button[contains(@class, 'menu')]"),
            ]
            
            for by, selector in hamburger_selectors:
                try:
                    hamburger_menus = self.driver.find_elements(by, selector)
                    for hamburger_menu in hamburger_menus:
                        # 요소가 DOM에 있고 실제로 보이는지 확인
                        try:
                            if hamburger_menu.is_displayed():
                                hamburger_menu.click()
                                print(f"✓ 햄버거 메뉴 클릭 완료")
                                time.sleep(2)
                                hamburger_found = True
                                break
                        except:
                            continue
                    if hamburger_found:
                        break
                except:
                    continue
            
            if not hamburger_found:
                print("✓ 햄버거 메뉴 없음, 일반 화면에서 메뉴 찾기")
            
            # 디버깅: 페이지의 모든 링크와 버튼 출력
            print("페이지 요소 확인 중...")
            try:
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                link_texts = [link.text.strip() for link in all_links if link.text.strip()]
                print(f"페이지의 링크들 (총 {len(link_texts)}개): {link_texts[:20]}")  # 처음 20개만
                
                if "통계관리" in str(link_texts):
                    print("✓ 페이지에 '통계관리' 링크 있음")
                else:
                    print("⚠ 페이지에 '통계관리' 링크 없음")
            except Exception as e:
                print(f"디버깅 오류: {e}")
            
            # 1. 통계관리 메뉴 클릭
            print("통계관리 메뉴 찾기 중...")
            stat_menu_found = False
            
            stat_selectors = [
                (By.LINK_TEXT, "통계관리"),
                (By.PARTIAL_LINK_TEXT, "통계관리"),
                (By.XPATH, "//a[contains(text(), '통계관리')]"),
                (By.XPATH, "//a[contains(., '통계관리')]"),
                (By.XPATH, "//div[contains(text(), '통계관리')]"),
                (By.XPATH, "//button[contains(text(), '통계관리')]"),
                (By.XPATH, "//*[text()='통계관리']"),
                (By.XPATH, "//*[contains(text(), '통계')]"),
            ]
            
            for by, selector in stat_selectors:
                try:
                    stat_elements = self.driver.find_elements(by, selector)
                    print(f"  {by} - {selector}: {len(stat_elements)}개 요소 발견")
                    for stat_menu in stat_elements:
                        try:
                            if stat_menu.is_displayed() and stat_menu.is_enabled():
                                print(f"    시도 중: {stat_menu.text}")
                                # JavaScript 클릭 시도
                                try:
                                    self.driver.execute_script("arguments[0].click();", stat_menu)
                                    print(f"✓ 통계관리 메뉴 클릭 (JS)")
                                except:
                                    stat_menu.click()
                                    print(f"✓ 통계관리 메뉴 클릭")
                                stat_menu_found = True
                                time.sleep(2)
                                break
                        except:
                            continue
                    if stat_menu_found:
                        break
                except:
                    continue
            
            if not stat_menu_found:
                print("⚠ 통계관리 메뉴를 찾을 수 없습니다.")
                print("⚠ 스크린샷을 확인하세요.")
                # 스크린샷 저장
                try:
                    screenshot_path = os.path.join(self.download_dir, f"debug_{target_date_str}.png")
                    self.driver.save_screenshot(screenshot_path)
                    print(f"✓ 스크린샷 저장: {screenshot_path}")
                except:
                    pass
                return None
            
            # 2. 업체매출관리 클릭
            print("업체매출관리 메뉴 찾기 중...")
            sales_menu_found = False
            
            sales_selectors = [
                (By.LINK_TEXT, "업체매출관리"),
                (By.PARTIAL_LINK_TEXT, "업체매출관리"),
                (By.XPATH, "//a[contains(text(), '업체매출관리')]"),
                (By.XPATH, "//div[contains(text(), '업체매출관리')]"),
                (By.XPATH, "//button[contains(text(), '업체매출관리')]"),
                (By.XPATH, "//*[text()='업체매출관리']"),
            ]
            
            for by, selector in sales_selectors:
                try:
                    sales_elements = self.driver.find_elements(by, selector)
                    for sales_menu in sales_elements:
                        if sales_menu.is_displayed() and sales_menu.is_enabled():
                            # JavaScript 클릭 시도
                            try:
                                self.driver.execute_script("arguments[0].click();", sales_menu)
                                print(f"✓ 업체매출관리 클릭 (JS, selector: {by})")
                            except:
                                sales_menu.click()
                                print(f"✓ 업체매출관리 클릭 (selector: {by})")
                            sales_menu_found = True
                            time.sleep(3)
                            break
                    if sales_menu_found:
                        break
                except:
                    continue
            
            if not sales_menu_found:
                print("⚠ 업체매출관리 메뉴를 찾을 수 없습니다.")
                return None
            
            # 3. 날짜 입력
            print(f"날짜 입력 중: {target_date_str}")
            try:
                # 페이지 로딩 대기
                time.sleep(2)
                
                # 날짜 형식: 2026-01-08
                date_input_value = target_date_str
                
                # 날짜 입력 필드 찾기 (여러 방법 시도)
                date_inputs = []
                
                # 방법 1: flatpickr-input 클래스 (가장 정확)
                try:
                    date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input.flatpickr-input")
                    if len(date_inputs) >= 2:
                        print(f"✓ flatpickr-input 필드 {len(date_inputs)}개 발견")
                except:
                    pass
                
                # 방법 2: gtDate 클래스
                if len(date_inputs) < 2:
                    try:
                        date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input.gtDate")
                        if len(date_inputs) >= 2:
                            print(f"✓ gtDate 필드 {len(date_inputs)}개 발견")
                    except:
                        pass
                
                # 방법 3: form-control 클래스와 날짜 관련 속성
                if len(date_inputs) < 2:
                    try:
                        date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input.form-control[name*='Date'], input.form-control[id*='Date']")
                        if len(date_inputs) >= 2:
                            print(f"✓ form-control Date 필드 {len(date_inputs)}개 발견")
                    except:
                        pass
                
                # 방법 4: type='date' 필드
                if len(date_inputs) < 2:
                    try:
                        date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='date']")
                        if len(date_inputs) >= 2:
                            print(f"✓ type='date' 필드 {len(date_inputs)}개 발견")
                    except:
                        pass
                
                if len(date_inputs) >= 2:
                    # 시작일 입력
                    from_date = date_inputs[0]
                    try:
                        # 기존 값 제거
                        from_date.clear()
                        time.sleep(0.5)
                        # JavaScript로 값 설정
                        self.driver.execute_script("arguments[0].value = arguments[1];", from_date, date_input_value)
                        print(f"✓ 시작일 입력: {date_input_value}")
                    except:
                        # 일반 입력 시도
                        from_date.send_keys(date_input_value)
                        print(f"✓ 시작일 입력 (일반): {date_input_value}")
                    
                    # 종료일 입력
                    to_date = date_inputs[1]
                    try:
                        # 기존 값 제거
                        to_date.clear()
                        time.sleep(0.5)
                        # JavaScript로 값 설정
                        self.driver.execute_script("arguments[0].value = arguments[1];", to_date, date_input_value)
                        print(f"✓ 종료일 입력: {date_input_value}")
                    except:
                        # 일반 입력 시도
                        to_date.send_keys(date_input_value)
                        print(f"✓ 종료일 입력 (일반): {date_input_value}")
                else:
                    print(f"⚠ 날짜 입력 필드를 찾을 수 없습니다. (발견: {len(date_inputs)}개)")
                    return None
                
                time.sleep(1)
            except Exception as e:
                print(f"⚠ 날짜 입력 실패: {e}")
                import traceback
                traceback.print_exc()
                return None
            
            # 4. 조회 버튼 클릭
            print("조회 버튼 클릭 중...")
            try:
                search_button_found = False
                search_selectors = [
                    (By.XPATH, "//button[contains(text(), '조회')]"),
                    (By.XPATH, "//button[text()='조회']"),
                    (By.CSS_SELECTOR, "button[class*='search']"),
                ]
                
                for by, selector in search_selectors:
                    try:
                        search_button = self.driver.find_element(by, selector)
                        if search_button.is_displayed():
                            search_button.click()
                            print("✓ 조회 버튼 클릭")
                            search_button_found = True
                            break
                    except:
                        continue
                
                if not search_button_found:
                    print("⚠ 조회 버튼을 찾을 수 없습니다.")
                    return None
                
                # 데이터 로딩 대기 (충분히 대기)
                print("데이터 로딩 대기 중... (8초)")
                time.sleep(8)
            except Exception as e:
                print(f"⚠ 조회 버튼 클릭 실패: {e}")
                return None
            
            # 5. 엑셀 다운로드 버튼 클릭
            print("엑셀 다운로드 버튼 찾기 중...")
            try:
                # 다운로드 전 파일 목록
                existing_files = set(glob.glob(os.path.join(self.download_dir, "*.xls*")))
                
                # 다운로드 버튼 찾기 (여러 방법 시도)
                download_button_found = False
                download_selectors = [
                    (By.XPATH, "//button[contains(text(), '그래프모양') or contains(@title, '다운로드')]"),
                    (By.CSS_SELECTOR, "button[class*='download']"),
                    (By.XPATH, "//a[contains(@class, 'download')]"),
                    (By.XPATH, "//button[contains(text(), '엑셀')]"),
                    (By.XPATH, "//button[contains(text(), '다운로드')]"),
                    (By.XPATH, "//a[contains(text(), '다운로드')]"),
                    # 아이콘으로 찾기
                    (By.CSS_SELECTOR, "button svg"),
                    (By.CSS_SELECTOR, "a svg"),
                ]
                
                for by, selector in download_selectors:
                    try:
                        download_buttons = self.driver.find_elements(by, selector)
                        for btn in download_buttons:
                            try:
                                if btn.is_displayed() and btn.is_enabled():
                                    # JavaScript 클릭 시도
                                    self.driver.execute_script("arguments[0].click();", btn)
                                    print(f"✓ 다운로드 버튼 클릭 (selector: {selector})")
                                    download_button_found = True
                                    break
                            except:
                                continue
                        if download_button_found:
                            break
                    except:
                        continue
                
                if not download_button_found:
                    print("⚠ 다운로드 버튼을 찾을 수 없습니다.")
                    # 스크린샷 저장
                    try:
                        screenshot_path = os.path.join(self.download_dir, f"download_page_{target_date_str}.png")
                        self.driver.save_screenshot(screenshot_path)
                        print(f"✓ 스크린샷 저장: {screenshot_path}")
                    except:
                        pass
                    return None
                
                # 다운로드 완료 대기
                print("다운로드 완료 대기 중... (5초)")
                time.sleep(5)
            except Exception as e:
                print(f"⚠ 엑셀 다운로드 실패: {e}")
                import traceback
                traceback.print_exc()
                return None
            
            # 6. 다운로드된 파일 찾기
            print("다운로드된 파일 확인 중...")
            time.sleep(2)
            
            # 새로운 파일 찾기
            current_files = set(glob.glob(os.path.join(self.download_dir, "*.xls*")))
            new_files = current_files - existing_files
            
            if new_files:
                latest_file = max(new_files, key=os.path.getctime)
                print(f"✓ 엑셀 파일 발견: {os.path.basename(latest_file)}")
                return latest_file
            
            print("⚠ 새로운 엑셀 파일을 찾을 수 없습니다.")
            return None
            
        except Exception as e:
            print(f"데이터 스크랩 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def upload_to_google_sheets(self, excel_file_path, query_date):
        """구글 시트에 데이터 업로드"""
        try:
            print("\n=== 구글 시트 업로드 시작 ===")
            
            # 1. 엑셀 파일 읽기
            print(f"엑셀 파일 읽기: {excel_file_path}")
            
            df_raw = pd.read_excel(excel_file_path, header=None)
            print(f"✓ 엑셀 원본 데이터: {len(df_raw)}행 x {len(df_raw.columns)}열")
            
            # 헤더 행 찾기
            header_row_idx = None
            for idx, row in df_raw.iterrows():
                row_values = [str(x) for x in row.values if pd.notna(x) and str(x).strip() != '']
                row_str = ' '.join(row_values)
                if 'ISBN' in row_str or '상품명' in row_str or '도서명' in row_str or '제목' in row_str:
                    header_row_idx = idx
                    print(f"✓ 헤더 행 발견: {idx}행")
                    break
            
            if header_row_idx is None:
                print("⚠ 헤더를 찾을 수 없습니다.")
                return False
            
            # 헤더 추출
            headers_raw = df_raw.iloc[header_row_idx].tolist()
            
            valid_col_indices = []
            clean_headers = []
            for i, header in enumerate(headers_raw):
                if pd.notna(header) and str(header).strip() != '':
                    valid_col_indices.append(i)
                    clean_headers.append(str(header).strip())
            
            print(f"✓ 유효한 컬럼: {len(clean_headers)}개")
            
            # 데이터 추출
            data_rows = df_raw.iloc[header_row_idx + 1:, valid_col_indices].copy()
            data_rows.columns = clean_headers
            data_rows = data_rows.reset_index(drop=True)
            
            print(f"✓ 초기 데이터 로드: {len(data_rows)}행")
            
            # 합계 행 제거
            mask = data_rows.apply(lambda row: any('합 계' in str(cell) or '합계' in str(cell) or '총' in str(cell) for cell in row.values), axis=1)
            data_rows = data_rows[~mask]
            print(f"✓ 합계 행 제거 후: {len(data_rows)}행")
            
            data_rows = data_rows.dropna(how='all')
            print(f"✓ 빈 행 제거 후: {len(data_rows)}행")
            
            df = data_rows.fillna('')
            
            # 2. 칼럼명 통일
            rename_dict = {
                'ISBN13': 'ISBN',
                '상품명': '도서명',
                '제조사': '출판사',
                '조회기간': '날짜'
            }
            for old_name, new_name in rename_dict.items():
                if old_name in df.columns:
                    df.rename(columns={old_name: new_name}, inplace=True)
                    print(f"✓ 칼럼명 변경: {old_name} → {new_name}")
            
            # 3. 업로드날짜, 날짜, UpdatedAt 추가
            upload_date = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
            updated_at = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')
            df.insert(0, '날짜', query_date)
            df.insert(0, '업로드날짜', upload_date)
            df['UpdatedAt'] = updated_at
            print(f"✓ 업로드날짜({upload_date}), 날짜({query_date}) 컬럼 추가")
            
            # 3. 구글 시트 연결
            print("구글 시트 연결 중...")
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            creds_path = _locate_credentials_file()
            if not creds_path:
                raise FileNotFoundError('credentials.json not found; set GOOGLE_CREDENTIALS secret or upload credentials.json')
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            
            spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
            spreadsheet = client.open_by_key(spreadsheet_id)
            print("✓ 구글 시트 연결 완료")
            
            # 4. "YES24" 시트
            try:
                worksheet = spreadsheet.worksheet("YES24")
                print("✓ 기존 'YES24' 시트 찾음")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="YES24", rows="1000", cols="20")
                print("✓ 'YES24' 시트 생성")
            
            # 데이터 검수 수행
            try:
                validation_warnings = self.validate_data_integrity(df, query_date, worksheet)
            except AttributeError:
                validation_warnings = []
            except Exception as e:
                print(f"⚠ 데이터 검수 중 오류: {str(e)}")
                validation_warnings = []
            
            # 5. 기존 데이터 병합
            existing_data = worksheet.get_all_values()
            
            if existing_data and len(existing_data) > 1:
                existing_headers = existing_data[0]
                existing_rows = existing_data[1:]
                existing_df = pd.DataFrame(existing_rows, columns=existing_headers)
                existing_df = existing_df.replace('', pd.NA).dropna(how='all').fillna('')
                
                print(f"✓ 기존 데이터: {len(existing_df)}행")
                
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                print(f"✓ 데이터 병합: {len(combined_df)}행")
            else:
                combined_df = df
                print("✓ 첫 데이터 업로드")
            
            # 6. 3년 데이터 관리
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
            print("\n브라우저를 10초 후 종료합니다...")
            time.sleep(10)
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

if __name__ == "__main__":
    # YES24 로그인 정보 (환경 변수 우선)
    import os
    USERNAME = os.getenv('YES24_ID', 'thenan1')
    PASSWORD = os.getenv('YES24_PASSWORD', 'thenan2525!')
    # Prefer PHONE_CHOICE (workflow input) but fall back to YES24_PHONE
    phone_env = os.getenv('PHONE_CHOICE') or os.getenv('YES24_PHONE') or os.getenv('YES24_PHONE')
    if phone_env in ('1', '2'):
        PHONE = '01094603191' if phone_env == '1' else '01040435756'
    elif phone_env and phone_env.isdigit():
        PHONE = phone_env
    else:
        PHONE = '01040435756'
    
    scraper = Yes24Scraper()
    
    try:
        # 1. 드라이버 설정
        scraper.setup_driver()
        
        # 2. 빠진 날짜 확인
        missing_dates = scraper.get_missing_dates_from_sheet()
        
        if not missing_dates:
            print("\n✅ 모든 데이터가 최신 상태입니다!")
            scraper.close()
            exit(0)
        
        print(f"\n📋 총 {len(missing_dates)}일의 데이터를 수집합니다.")
        
        # 3. 로그인 (SMS 인증)
        # Pass phone_choice so the function can handle numeric selection, full number, or web selection
        if scraper.login_with_sms(USERNAME, PASSWORD, phone_choice=os.getenv('PHONE_CHOICE') or PHONE):
            print("\n✅ 로그인 성공! 이제 각 날짜별 데이터를 수집합니다.\n")
            
            success_count = 0
            failed_dates = []
            
            # 4. 각 날짜별로 스크랩
            for i, date in enumerate(missing_dates, 1):
                print(f"\n{'='*60}")
                print(f"📅 [{i}/{len(missing_dates)}] {date} 데이터 수집")
                print(f"{'='*60}")
                
                try:
                    # 데이터 스크랩
                    excel_path = scraper.scrape_sales_data(date)
                    
                    # 구글 시트 업로드
                    if excel_path:
                        if scraper.upload_to_google_sheets(excel_path, date):
                            success_count += 1
                            print(f"✅ {date} 데이터 업로드 완료!")
                            # 업로드 완료 후 로컬 파일 삭제
                            try:
                                if os.path.exists(excel_path):
                                    os.remove(excel_path)
                                    print(f"🗑️ 로컬 파일 삭제: {excel_path}")
                            except Exception as del_e:
                                print(f"⚠ 파일 삭제 실패: {del_e}")
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
        else:
            print("\n⚠ 로그인 실패")
        
    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        scraper.close()
