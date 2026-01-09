# 서점 판매 데이터 자동 수집 시스템

교보문고, 알라딘, 영풍문고, YES24 네 곳의 서점에서 판매 데이터를 자동으로 수집하고 구글 시트에 업로드하는 시스템입니다.

## 🎯 주요 기능

- **자동 스크래핑**: 교보문고, 알라딘, 영풍문고 (매일 오전 5:30 자동 실행)
- **통합테이블**: 모든 서점 데이터를 하나의 테이블로 병합
- **날짜 추적**: 빠진 날짜 자동 확인 및 수집 (2026-01-01부터)
- **데이터 검증**: 중복 체크, 유사도 분석, 품질 검사
- **한국시간 기준**: 모든 날짜/시간 한국시간 표시

## 📊 수집 데이터

### 개별 서점 시트
- **교보문고**: ISBN, 도서명, 저자, 출판사, 발행일, 정가, 판매수량(오프/온/법인)
- **알라딘**: ISBN, 도서명, 저자, 출판사, 정가, 판매권수
- **영풍문고**: ISBN, 도서명, 저자, 출판사, 발행일, 장르, 정가, 판매수량
- **YES24**: ISBN, 도서명, 출판사, 당월판매량

### 통합테이블 (Wide Format)
- ISBN, 도서명, 저자, 발행일, 출판사, 장르, 정가
- 교보_오프, 교보_온, 교보_법인, 교보계
- YES24, 알라딘, 영풍, 일계
- 업로드날짜, UpdatedAt, UpdatedAt_yes24

## 🚀 설치 및 실행

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 환경 설정
루트 폴더에 `credentials.json` 파일 추가 (Google Sheets API 인증)

### 3. 개별 실행
```bash
python kyobo_scraper.py      # 교보문고
python aladin_scraper.py      # 알라딘
python youngpoong_scraper.py  # 영풍문고
python yes24_scraper.py       # YES24 (SMS 인증 필요)
```

### 4. 통합테이블 생성
```bash
python create_integrated_table.py
```

## ⚙️ GitHub Actions 자동화

매일 한국시간 오전 5:30에 자동 실행:
- 교보문고 스크래핑
- 알라딘 스크래핑
- 영풍문고 스크래핑
- 통합테이블 생성

**참고**: YES24는 SMS 인증이 필요하여 수동 실행 필요

## 🔑 필수 Secrets (GitHub)

Repository Settings → Secrets and variables → Actions

- `GOOGLE_CREDENTIALS`: credentials.json 파일 전체 내용
- `KYOBO_ID`, `KYOBO_PASSWORD`
- `ALADIN_ID`, `ALADIN_PASSWORD`
- `YOUNGPOONG_ID`, `YOUNGPOONG_PASSWORD`

## 📁 프로젝트 구조

```
서점판매스크랩/
├── kyobo_scraper.py          # 교보문고 스크래퍼
├── aladin_scraper.py          # 알라딘 스크래퍼
├── youngpoong_scraper.py      # 영풍문고 스크래퍼
├── yes24_scraper.py           # YES24 스크래퍼
├── create_integrated_table.py # 통합테이블 생성
├── requirements.txt           # Python 패키지 목록
├── credentials.json           # Google API 인증 (보안!)
└── .github/
    └── workflows/
        └── daily-scraping.yml # GitHub Actions 워크플로우
```

## 🛠️ 기술 스택

- **Python 3.11**: 메인 언어
- **Selenium**: 웹 스크래핑
- **Pandas**: 데이터 처리
- **gspread**: Google Sheets API
- **GitHub Actions**: 자동화

## 📝 라이선스

Private Repository

## 👤 작성자

더난출판사
