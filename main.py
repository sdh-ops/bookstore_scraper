import os
import threading
import subprocess
import re
import time

# enable OTP server usage in scraper
os.environ['USE_OTP_SERVER'] = '1'


def start_flask_server():
    # run otp_server.app in background thread
    try:
        from otp_server import app
    except Exception as e:
        print(f"OTP server import failed: {e}")
        raise

    def run_app():
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

    t = threading.Thread(target=run_app, daemon=True)
    t.start()
    print("✓ OTP Flask server started on port 5000")
    return t


def start_cloudflared():
    # start cloudflared tunnel and parse public URL
    cmd = ['cloudflared', 'tunnel', '--url', 'http://127.0.0.1:5000']
    print(f"Starting cloudflared: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    public_url = None
    start_time = time.time()
    timeout = 30
    while True:
        if proc.stdout is None:
            break
        line = proc.stdout.readline()
        if not line:
            if time.time() - start_time > timeout:
                break
            time.sleep(0.1)
            continue
        line = line.strip()
        print(f"[cloudflared] {line}")
        # try to extract https URL
        m = re.search(r'(https?://[\w\.-]+\.trycloudflare\.com)', line)
        if m:
            public_url = m.group(1)
            break

    if public_url:
        print(f"✓ Public OTP URL: {public_url}")
    else:
        print("⚠ cloudflared did not return public URL quickly; check logs above")

    return proc, public_url


def run_scraper():
    # import and run Yes24Scraper logic in-process so otp_server variables are shared
    from yes24_scraper import Yes24Scraper

    USERNAME = os.getenv('YES24_ID', 'thenan1')
    PASSWORD = os.getenv('YES24_PASSWORD', 'thenan2525!')

    scraper = Yes24Scraper()
    try:
        scraper.setup_driver()
        missing_dates = scraper.get_missing_dates_from_sheet()
        if not missing_dates:
            print('No missing dates; exiting')
            return

        print(f"Collecting {len(missing_dates)} dates")

        # login; phone selection will be handled by otp_server when USE_OTP_SERVER=1
        if not scraper.login_with_sms(USERNAME, PASSWORD):
            print('Login failed')
            return

        for date in missing_dates:
            excel = scraper.scrape_sales_data(date)
            if excel:
                scraper.upload_to_google_sheets(excel, date)
    finally:
        scraper.close()


def main():
    start_flask_server()
    proc, public_url = start_cloudflared()

    if public_url:
        print('\nOTP URL (open in browser to input OTP and select phone):')
        print(public_url)

    try:
        run_scraper()
    finally:
        try:
            if proc and proc.poll() is None:
                proc.terminate()
        except Exception:
            pass


if __name__ == '__main__':
    main()
