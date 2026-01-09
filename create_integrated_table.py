"""
모든 서점 데이터를 통합테이블로 만드는 스크립트 (Wide Format)
조인 키: ISBN + 날짜(조회기간)
"""
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
from datetime import datetime
import pytz

def load_sheet_data(worksheet):
    """시트 데이터를 DataFrame으로 로드"""
    try:
        data = worksheet.get_all_values()
        if not data or len(data) < 2:
            return None
        
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.replace('', pd.NA).dropna(how='all')
        return df
    except Exception as e:
        print(f"시트 로드 오류: {e}")
        return None

def get_column_safe(df, *possible_names):
    """여러 가능한 칼럼명 중 존재하는 것 반환 (옛날/새 칼럼명 지원)"""
    for name in possible_names:
        if name in df.columns:
            return df[name]
    return pd.Series([''] * len(df))

def process_kyobo_data(df):
    """교보문고 데이터 처리"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    print(f"교보문고 원본 데이터: {len(df)}행")
    print(f"칼럼: {df.columns.tolist()}")
    
    result = pd.DataFrame()
    result['날짜'] = get_column_safe(df, '날짜', '조회기간')
    result['ISBN'] = get_column_safe(df, 'ISBN')
    result['도서명'] = get_column_safe(df, '도서명', '상품명')
    result['저자'] = get_column_safe(df, '저자')
    result['출판사'] = get_column_safe(df, '출판사')
    result['발행일'] = get_column_safe(df, '발행일', '출판일자')
    result['정가'] = pd.to_numeric(get_column_safe(df, '정가'), errors='coerce').fillna(0).astype(int)
    result['업로드날짜'] = get_column_safe(df, '업로드날짜')
    
    # 판매수량 분리
    result['교보_오프'] = pd.to_numeric(get_column_safe(df, '판매\n(영업점)'), errors='coerce').fillna(0).astype(int)
    result['교보_온'] = pd.to_numeric(get_column_safe(df, '판매\n(온라인)'), errors='coerce').fillna(0).astype(int)
    result['교보_법인'] = pd.to_numeric(get_column_safe(df, '판매\n(법인)'), errors='coerce').fillna(0).astype(int)
    result['교보계'] = result['교보_오프'] + result['교보_온'] + result['교보_법인']
    
    print(f"교보문고 처리 완료: {len(result)}행")
    return result


def process_aladin_data(df):
    """알라딘 데이터 처리"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    print(f"알라딘 원본 데이터: {len(df)}행")
    print(f"칼럼: {df.columns.tolist()}")
    
    result = pd.DataFrame()
    result['날짜'] = get_column_safe(df, '날짜', '조회기간')
    result['ISBN'] = get_column_safe(df, 'ISBN')
    result['도서명'] = get_column_safe(df, '도서명')
    result['저자'] = get_column_safe(df, '저자')
    result['출판사'] = get_column_safe(df, '출판사')
    result['정가'] = pd.to_numeric(get_column_safe(df, '정가'), errors='coerce').fillna(0).astype(int)
    result['업로드날짜'] = get_column_safe(df, '업로드날짜')
    result['알라딘'] = pd.to_numeric(get_column_safe(df, '판매권수', '판매건수'), errors='coerce').fillna(0).astype(int)
    
    print(f"알라딘 처리 완료: {len(result)}행")
    return result

def process_youngpoong_data(df):
    """영풍문고 데이터 처리"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    print(f"영풍문고 원본 데이터: {len(df)}행")
    print(f"칼럼: {df.columns.tolist()}")
    
    result = pd.DataFrame()
    result['날짜'] = get_column_safe(df, '날짜', '조회기간')
    result['ISBN'] = get_column_safe(df, 'ISBN', '바코드')
    result['도서명'] = get_column_safe(df, '도서명')
    result['저자'] = get_column_safe(df, '저자')
    result['출판사'] = get_column_safe(df, '출판사', '출판사명')
    result['발행일'] = get_column_safe(df, '발행일')
    result['장르'] = get_column_safe(df, '자재그룹내역', '장르')
    result['정가'] = pd.to_numeric(get_column_safe(df, '정가'), errors='coerce').fillna(0).astype(int)
    result['업로드날짜'] = get_column_safe(df, '업로드날짜')
    result['영풍'] = pd.to_numeric(get_column_safe(df, '판매수량'), errors='coerce').fillna(0).astype(int)
    
    print(f"영풍문고 처리 완료: {len(result)}행")
    return result

def process_yes24_data(df):
    """YES24 데이터 처리"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    print(f"YES24 원본 데이터: {len(df)}행")
    print(f"칼럼: {df.columns.tolist()}")
    
    result = pd.DataFrame()
    result['날짜'] = get_column_safe(df, '날짜', '조회기간')
    result['ISBN'] = get_column_safe(df, 'ISBN', 'ISBN13')
    result['도서명'] = get_column_safe(df, '도서명', '상품명')
    result['출판사'] = get_column_safe(df, '출판사', '제조사')
    result['업로드날짜'] = get_column_safe(df, '업로드날짜')
    result['UpdatedAt_yes24'] = get_column_safe(df, 'UpdatedAt')
    result['YES24'] = pd.to_numeric(get_column_safe(df, '당월판매량'), errors='coerce').fillna(0).astype(int)
    
    print(f"YES24 처리 완료: {len(result)}행")
    return result


def merge_with_fallback(kyobo_df, aladin_df, youngpoong_df, yes24_df):
    """4개 서점 데이터를 조인하고 Fallback 로직 적용"""
    print("\n데이터 병합 중...")
    
    # 1. 모든 서점 데이터를 날짜+ISBN 기준으로 Full Outer Join
    merged = kyobo_df.copy() if not kyobo_df.empty else pd.DataFrame()
    
    # 알라딘 병합
    if not aladin_df.empty:
        if merged.empty:
            merged = aladin_df.copy()
        else:
            merged = pd.merge(merged, aladin_df, on=['날짜', 'ISBN'], how='outer', suffixes=('', '_알라딘'))
    
    # 영풍문고 병합
    if not youngpoong_df.empty:
        if merged.empty:
            merged = youngpoong_df.copy()
        else:
            merged = pd.merge(merged, youngpoong_df, on=['날짜', 'ISBN'], how='outer', suffixes=('', '_영풍'))
    
    # YES24 병합
    if not yes24_df.empty:
        if merged.empty:
            merged = yes24_df.copy()
        else:
            merged = pd.merge(merged, yes24_df, on=['날짜', 'ISBN'], how='outer', suffixes=('', '_YES24'))
    
    if merged.empty:
        print("⚠ 병합할 데이터가 없습니다.")
        return pd.DataFrame()
    
    # 2. Fallback 로직으로 메타데이터 통합
    integrated = pd.DataFrame()
    integrated['날짜'] = merged['날짜']
    integrated['ISBN'] = merged['ISBN']
    
    # 도서명: 교보 → 알라딘 → 영풍 → YES24 (빈 문자열도 Fallback)
    integrated['도서명'] = merged.get('도서명', '')
    if '도서명_알라딘' in merged.columns:
        mask = (integrated['도서명'] == '') | (integrated['도서명'].isna())
        integrated.loc[mask, '도서명'] = merged.loc[mask, '도서명_알라딘']
    if '도서명_영풍' in merged.columns:
        mask = (integrated['도서명'] == '') | (integrated['도서명'].isna())
        integrated.loc[mask, '도서명'] = merged.loc[mask, '도서명_영풍']
    if '도서명_YES24' in merged.columns:
        mask = (integrated['도서명'] == '') | (integrated['도서명'].isna())
        integrated.loc[mask, '도서명'] = merged.loc[mask, '도서명_YES24']
    
    # 저자: 교보 → 알라딘 → 영풍 (빈 문자열도 Fallback)
    integrated['저자'] = merged.get('저자', '')
    if '저자_알라딘' in merged.columns:
        mask = (integrated['저자'] == '') | (integrated['저자'].isna())
        integrated.loc[mask, '저자'] = merged.loc[mask, '저자_알라딘']
    if '저자_영풍' in merged.columns:
        mask = (integrated['저자'] == '') | (integrated['저자'].isna())
        integrated.loc[mask, '저자'] = merged.loc[mask, '저자_영풍']
    
    # 발행일: 교보 → 영풍 (빈 문자열도 Fallback)
    integrated['발행일'] = merged.get('발행일', '')
    if '발행일_영풍' in merged.columns:
        mask = (integrated['발행일'] == '') | (integrated['발행일'].isna())
        integrated.loc[mask, '발행일'] = merged.loc[mask, '발행일_영풍']
    
    # 출판사: 교보 → 알라딘 → 영풍 → YES24 (빈 문자열도 Fallback)
    integrated['출판사'] = merged.get('출판사', '')
    if '출판사_알라딘' in merged.columns:
        mask = (integrated['출판사'] == '') | (integrated['출판사'].isna())
        integrated.loc[mask, '출판사'] = merged.loc[mask, '출판사_알라딘']
    if '출판사_영풍' in merged.columns:
        mask = (integrated['출판사'] == '') | (integrated['출판사'].isna())
        integrated.loc[mask, '출판사'] = merged.loc[mask, '출판사_영풍']
    if '출판사_YES24' in merged.columns:
        mask = (integrated['출판사'] == '') | (integrated['출판사'].isna())
        integrated.loc[mask, '출판사'] = merged.loc[mask, '출판사_YES24']
    
    # 장르: 영풍만 있음 (빈 문자열도 Fallback)
    integrated['장르'] = merged.get('장르', '')
    if '장르_영풍' in merged.columns:
        mask = (integrated['장르'] == '') | (integrated['장르'].isna())
        integrated.loc[mask, '장르'] = merged.loc[mask, '장르_영풍']
    
    # 정가: 교보 → 알라딘 → 영풍 (0도 Fallback)
    integrated['정가'] = pd.to_numeric(merged.get('정가', 0), errors='coerce').fillna(0)
    if '정가_알라딘' in merged.columns:
        mask = (integrated['정가'] == 0) | (integrated['정가'].isna())
        integrated.loc[mask, '정가'] = pd.to_numeric(merged.loc[mask, '정가_알라딘'], errors='coerce').fillna(0)
    if '정가_영풍' in merged.columns:
        mask = (integrated['정가'] == 0) | (integrated['정가'].isna())
        integrated.loc[mask, '정가'] = pd.to_numeric(merged.loc[mask, '정가_영풍'], errors='coerce').fillna(0)
    integrated['정가'] = integrated['정가'].astype(int)
    
    # 업로드날짜: 4개 서점 중 최신 (문자열이므로 먼저 fillna 처리)
    upload_dates = []
    for col in ['업로드날짜', '업로드날짜_알라딘', '업로드날짜_영풍', '업로드날짜_YES24']:
        if col in merged.columns:
            upload_dates.append(merged[col].fillna(''))
    if upload_dates:
        # 문자열 날짜이므로 직접 비교
        upload_df = pd.concat(upload_dates, axis=1).fillna('')
        integrated['업로드날짜'] = upload_df.apply(lambda row: max([x for x in row if x]), axis=1)
    else:
        integrated['업로드날짜'] = ''
    
    # 요일 계산
    try:
        integrated['요일'] = pd.to_datetime(integrated['날짜']).dt.day_name()
        # 한글 요일로 변환
        day_map = {
            'Monday': '월', 'Tuesday': '화', 'Wednesday': '수', 
            'Thursday': '목', 'Friday': '금', 'Saturday': '토', 'Sunday': '일'
        }
        integrated['요일'] = integrated['요일'].map(day_map).fillna('')
    except:
        integrated['요일'] = ''
    
    # 판매수량 칼럼들
    integrated['교보_오프'] = merged.get('교보_오프', 0).fillna(0).astype(int)
    integrated['교보_온'] = merged.get('교보_온', 0).fillna(0).astype(int)
    integrated['교보_법인'] = merged.get('교보_법인', 0).fillna(0).astype(int)
    integrated['교보계'] = merged.get('교보계', 0).fillna(0).astype(int)
    integrated['YES24'] = merged.get('YES24', 0).fillna(0).astype(int)
    integrated['알라딘'] = merged.get('알라딘', 0).fillna(0).astype(int)
    integrated['영풍'] = merged.get('영풍', 0).fillna(0).astype(int)
    
    # 일계 계산
    integrated['일계'] = (
        integrated['교보계'] + 
        integrated['YES24'] + 
        integrated['알라딘'] + 
        integrated['영풍']
    )
    
    # UpdatedAt_yes24 (YES24 스크래퍼에서 가져오기)
    integrated['UpdatedAt_yes24'] = merged.get('UpdatedAt_yes24', '')
    if 'UpdatedAt_yes24_YES24' in merged.columns:
        integrated['UpdatedAt_yes24'] = integrated['UpdatedAt_yes24'].fillna(merged['UpdatedAt_yes24_YES24'])
    
    # UpdatedAt 추가 (현재 한국시간 - 통합테이블 생성 시각)
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
    integrated['UpdatedAt'] = now_kst
    
    # 빈 문자열을 공백으로 변경
    integrated = integrated.fillna('')
    
    # 칼럼 순서 정렬
    final_columns = [
        '날짜', '요일', 'ISBN', '도서명', '저자', '발행일', '출판사', '장르', '정가',
        '교보_오프', '교보_온', '교보_법인', '교보계', 
        'YES24', '알라딘', '영풍', '일계',
        '업로드날짜', 'UpdatedAt_yes24', 'UpdatedAt'
    ]
    
    integrated = integrated[[col for col in final_columns if col in integrated.columns]]
    
    print(f"✓ 병합 완료: {len(integrated)}행")
    return integrated

def update_integrated_sheet(gc, spreadsheet_id, integrated_df):
    """통합테이블 시트 업데이트"""
    print("\n통합테이블 시트 업데이트 중...")
    
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
        # 통합테이블 시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet('통합테이블')
            print("✓ 기존 '통합테이블' 시트 찾음")
        except:
            worksheet = spreadsheet.add_worksheet(title='통합테이블', rows=1000, cols=20)
            print("✓ '통합테이블' 시트 생성")
        
        # 시트 내용 지우기
        worksheet.clear()
        
        # 데이터 업로드
        data = [integrated_df.columns.tolist()] + integrated_df.values.tolist()
        worksheet.update('A1', data)
        
        print(f"✓ 통합테이블 업데이트 완료: {len(integrated_df)}행")
        print(f"✓ 시트 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={worksheet.id}")
        
        return True
        
    except Exception as e:
        print(f"⚠ 통합테이블 업데이트 실패: {e}")
        return False

def main():
    print("=" * 80)
    print("통합테이블 생성 시작")
    print("=" * 80)
    
    # 구글 시트 연결
    print("\n구글 시트 연결 중...")
    
    # credentials.json 경로 찾기 (로컬과 GitHub Actions 모두 지원)
    script_dir = os.path.dirname(__file__)
    credentials_paths = [
        os.path.join(script_dir, 'credentials.json'),  # 같은 폴더 (GitHub Actions)
        os.path.join(script_dir, '..', 'credentials.json'),  # 상위 폴더 (로컬)
    ]
    
    credentials_path = None
    for path in credentials_paths:
        if os.path.exists(path):
            credentials_path = path
            break
    
    if not credentials_path:
        print("❌ credentials.json 파일을 찾을 수 없습니다.")
        print(f"   확인한 경로: {credentials_paths}")
        return
    
    print(f"✓ credentials.json 찾음: {credentials_path}")
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scope)
    gc = gspread.authorize(credentials)
    
    spreadsheet_id = '1bH7D7zO56xzp555BGiVCB1Mo5cRLxqN7GkC_Tudqp8s'
    spreadsheet = gc.open_by_key(spreadsheet_id)
    print("✓ 구글 시트 연결 완료")
    
    # 각 서점 데이터 로드
    print("\n각 서점 데이터 로드 중...")
    kyobo_df = process_kyobo_data(load_sheet_data(spreadsheet.worksheet('교보문고')))
    aladin_df = process_aladin_data(load_sheet_data(spreadsheet.worksheet('알라딘')))
    youngpoong_df = process_youngpoong_data(load_sheet_data(spreadsheet.worksheet('영풍문고')))
    yes24_df = process_yes24_data(load_sheet_data(spreadsheet.worksheet('YES24')))
    
    # 데이터 병합
    integrated = merge_with_fallback(kyobo_df, aladin_df, youngpoong_df, yes24_df)
    
    if integrated.empty:
        print("⚠ 통합할 데이터가 없습니다.")
        return
    
    # 통합테이블 업데이트
    update_integrated_sheet(gc, spreadsheet_id, integrated)
    
    # 통계 출력
    print("\n" + "=" * 80)
    print("통합 결과 통계")
    print("=" * 80)
    print(f"총 행 수: {len(integrated)}개")
    print(f"총 판매수량: {integrated['일계'].sum()}권")
    print(f"마지막 업데이트: {integrated['UpdatedAt'].iloc[0] if not integrated.empty else ''}")
    print("\n서점별 판매수량:")
    print(f"  교보계: {integrated['교보계'].sum()}권 (오프:{integrated['교보_오프'].sum()} + 온:{integrated['교보_온'].sum()} + 법인:{integrated['교보_법인'].sum()})")
    print(f"  YES24: {integrated['YES24'].sum()}권")
    print(f"  알라딘: {integrated['알라딘'].sum()}권")
    print(f"  영풍문고: {integrated['영풍'].sum()}권")
    print("=" * 80)

if __name__ == "__main__":
    main()

