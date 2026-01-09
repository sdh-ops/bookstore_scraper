import re

# 수정할 파일 목록
files = [
    'aladin_scraper.py',
    'youngpoong_scraper.py',
    'yes24_scraper.py'
]

old_pattern = r"creds_path = os\.path\.join\(os\.path\.dirname\(__file__\), '\.\.', 'credentials\.json'\)"
new_code = """script_dir = os.path.dirname(__file__)
            creds_paths = [os.path.join(script_dir, 'credentials.json'), os.path.join(script_dir, '..', 'credentials.json')]
            creds_path = next((p for p in creds_paths if os.path.exists(p)), creds_paths[1])"""

for filename in files:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 정규식으로 대체
        new_content = re.sub(old_pattern, new_code, content)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✓ {filename} 수정 완료")
    except Exception as e:
        print(f"✗ {filename} 수정 실패: {e}")

print("\n모든 스크래퍼 credentials 경로 수정 완료!")
