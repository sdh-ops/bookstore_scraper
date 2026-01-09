"""
모든 서점 스크래퍼 통합 실행 스크립트
순서: YES24 → 교보문고 → 알라딘 → 영풍문고
"""
import subprocess
import sys
import os
from datetime import datetime
import pytz

def run_scraper(scraper_name, script_name):
    """개별 스크래퍼 실행"""
    print(f"\n{'='*80}")
    print(f"{'='*80}")
    print(f"  🚀 {scraper_name} 스크래퍼 시작")
    print(f"{'='*80}")
    print(f"{'='*80}\n")
    
    try:
        # 가상환경의 파이썬 실행파일 경로
        venv_python = os.path.join(os.path.dirname(__file__), '.venv', 'Scripts', 'python.exe')
        python_exe = venv_python if os.path.exists(venv_python) else sys.executable
        
        # 파이썬 스크립트 실행
        result = subprocess.run(
            [python_exe, script_name],
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            print(f"\n✅ {scraper_name} 완료!")
            return True
        else:
            print(f"\n⚠ {scraper_name} 실행 중 오류 발생 (Exit Code: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"\n❌ {scraper_name} 실행 실패: {str(e)}")
        return False

def main():
    korea_tz = pytz.timezone('Asia/Seoul')
    start_time = datetime.now(korea_tz)
    
    print("\n" + "="*80)
    print("  📚 서점 판매 데이터 통합 수집 시스템")
    print("="*80)
    print(f"  시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  수집 순서: YES24 → 교보문고 → 알라딘 → 영풍문고 → 통합테이블 생성")
    print("="*80 + "\n")
    
    # 실행 결과 추적
    results = {}
    
    # 1. YES24 (SMS 인증 필요)
    print("⚠ YES24는 SMS 인증이 필요합니다.")
    print("⚠ SMS 인증번호를 받으면 터미널에 입력해주세요.\n")
    results['YES24'] = run_scraper("YES24", "yes24_scraper.py")
    
    # 2. 교보문고
    results['교보문고'] = run_scraper("교보문고", "kyobo_scraper.py")
    
    # 3. 알라딘
    results['알라딘'] = run_scraper("알라딘", "aladin_scraper.py")
    
    # 4. 영풍문고
    results['영풍문고'] = run_scraper("영풍문고", "youngpoong_scraper.py")
    
    # 5. 통합테이블 생성
    print("\n" + "="*80)
    print("  📊 통합테이블 생성 시작")
    print("="*80 + "\n")
    
    results['통합테이블'] = run_scraper("통합테이블 생성", "create_integrated_table.py")
    
    # 최종 결과 요약
    end_time = datetime.now(korea_tz)
    duration = end_time - start_time
    
    print("\n" + "="*80)
    print("="*80)
    print("  🎉 전체 작업 완료!")
    print("="*80)
    print("="*80 + "\n")
    
    print("【 실행 결과 】")
    for store, success in results.items():
        status = "✅ 성공" if success else "⚠ 실패"
        print(f"  {store:10s}: {status}")
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    print(f"\n  총 {total_count}개 작업 중 {success_count}개 성공")
    print(f"\n  시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  종료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  소요 시간: {duration}")
    print("\n" + "="*80 + "\n")
    
    # 구글 시트 링크 출력
    print("📊 구글 시트 확인:")
    print("https://docs.google.com/spreadsheets/d/1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s/edit")
    print("\n")

if __name__ == "__main__":
    main()
