import threading
import time
import requests

from otp_server import app, wait_for_phone, wait_for_otp, reset


def run_server():
    app.run(host='127.0.0.1', port=5001, debug=False, use_reloader=False)


def client_post():
    time.sleep(1)
    url = 'http://127.0.0.1:5001/otp'
    data = {'phone': '01094603191', 'otp': '123456'}
    r = requests.post(url, json=data)
    print('client post status', r.status_code, r.text)


if __name__ == '__main__':
    reset()
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    c = threading.Thread(target=client_post, daemon=True)
    c.start()

    print('Waiting for phone...')
    phone = wait_for_phone(timeout=5)
    print('Got phone:', phone)
    otp = wait_for_otp(timeout=5)
    print('Got otp:', otp)
