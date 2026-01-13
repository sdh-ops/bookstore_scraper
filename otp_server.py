import threading
import time
import os
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

# Shared state
_selected_phone = None
_otp_value = None
_lock = threading.Lock()
_cond = threading.Condition(_lock)

INDEX_HTML = '''
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>OTP 입력</title>
  </head>
  <body>
    <h3>YES24 OTP 입력</h3>
    <form id="otpForm" method="post" action="/otp">
      <label>전화번호 선택:</label>
      <select name="phone">
        <option value="01094603191">01094603191</option>
        <option value="01040435756">01040435756</option>
      </select>
      <br/><br/>
      <label>인증번호 (6자리):</label>
      <input name="otp" pattern="[0-9]{4,6}" required />
      <br/><br/>
      <button type="submit">전송</button>
    </form>
    <script>
      document.getElementById('otpForm').addEventListener('submit', function(e){
        e.preventDefault();
        const form = e.target;
        const data = { otp: form.otp.value, phone: form.phone.value };
        fetch('/otp', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
          .then(r => r.text()).then(() => alert('전송 완료'));
      });
    </script>
  </body>
</html>
'''


@app.route('/')
def index():
    return render_template_string(INDEX_HTML)


@app.route('/otp', methods=['POST'])
def receive_otp():
    global _otp_value, _selected_phone
    data = None
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    otp = (data.get('otp') or '').strip()
    phone = (data.get('phone') or '').strip()

    with _cond:
        if phone:
            _selected_phone = phone
            # also expose via env for compatibility
            os.environ['PHONE_CHOICE'] = phone
        if otp:
            _otp_value = otp
        _cond.notify_all()

    return 'OK'


def wait_for_otp(timeout=None):
    """Block until an OTP is submitted via /otp or timeout (seconds)."""
    global _otp_value
    deadline = None if timeout is None else (time.time() + float(timeout))
    with _cond:
        while _otp_value is None:
            remaining = None if deadline is None else max(0, deadline - time.time())
            if remaining == 0:
                break
            _cond.wait(timeout=remaining)
        return _otp_value


def wait_for_phone(timeout=None):
    """Block until a phone is selected via /otp or timeout (seconds). Returns phone string or None."""
    global _selected_phone
    deadline = None if timeout is None else (time.time() + float(timeout))
    with _cond:
        while _selected_phone is None:
            remaining = None if deadline is None else max(0, deadline - time.time())
            if remaining == 0:
                break
            _cond.wait(timeout=remaining)
        return _selected_phone


def reset():
    global _selected_phone, _otp_value
    with _cond:
        _selected_phone = None
        _otp_value = None
        try:
            del os.environ['PHONE_CHOICE']
        except Exception:
            pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
