# DART 크롤러 (DART Crawler)

한국 금융감독원 DART(전자공시시스템) OpenAPI를 활용하여 상장기업의 공시정보 및 경영현황 데이터를 수집하는 Python 크롤러입니다.

## 📋 목차

- [주요 기능](#주요-기능)
- [설치 및 설정](#설치-및-설정)
- [파일 구조](#파일-구조)
- [사용 방법](#사용-방법)
- [API 제한사항](#api-제한사항)
- [데이터 출력](#데이터-출력)
- [주의사항](#주의사항)

## 🚀 주요 기능

- **상장기업 공시정보 수집**: 모든 상장기업의 공시정보를 일괄 수집
- **경영현황 데이터 수집**: 소액주주, 임원, 직원 현황 등 다양한 경영지표 수집
- **보수현황 분석**: 이사·감사 보수현황 및 개인별 보수지급 현황 수집
- **자동 재시작**: 중단된 작업을 자동으로 재개하여 완전한 데이터 수집 보장
- **오류 처리**: 실패한 데이터는 별도 파일로 관리하여 재처리 가능

## 🛠 설치 및 설정

### 1. 자동 설정 (권장)

**Windows:**
```bash
setup.bat
```

**macOS/Linux:**
```bash
chmod +x setup.sh
./setup.sh
```

### 2. 수동 설정

**가상환경 생성 및 활성화:**
```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화 (Windows)
venv\Scripts\activate

# 가상환경 활성화 (macOS/Linux)
source venv/bin/activate
```

**의존성 설치:**
```bash
pip install -r requirements.txt
```

**환경변수 설정:**
프로젝트 루트에 `.env` 파일을 생성하고 DART API 키를 설정합니다:

```env
DART_API_KEY=your_dart_api_key_here
```

**DART API 키 발급 방법:**
1. [DART OpenAPI 사이트](https://opendart.fss.or.kr/) 접속
2. 회원가입 후 인증키 신청
3. 발급받은 40자리 API 키를 `.env` 파일에 입력

## 📁 파일 구조

### Python 스크립트 파일

| 파일명 | 설명 | API 엔드포인트 |
|--------|------|----------------|
| `1_minority_shareholders.py` | 소액주주 현황 수집 | [소액주주현황](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019009) |
| `2_executive_status.py` | 임원현황 수집 | [임원현황](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019010) |
| `3_employee_status.py` | 직원 현황 수집 | [직원현황](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019011) |
| `4_individual_compensation_directors_auditors.py` | 이사·감사 개인별 보수현황(5억원 이상) 수집 | [개인별보수현황](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019012) |
| `5_overall_compensation_directors_auditors.py` | 이사·감사 전체 보수현황(보수지급금액) 수집 | [전체보수현황](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019013) |
| `6_individual_compensation_amounts.py` | 개인별 보수지급 금액(5억이상 상위5인) 수집 | [개인별보수금액](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019013) |
| `7_status_changes_outside_directors.py` | 사외이사 현황변동 수집 | [사외이사현황변동](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019013) |
| `8_overall_compensation_status_directors_auditors.py` | 이사·감사 전체 보수현황(주주총회 승인금액) 수집 | [전체보수승인금액](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019013) |
| `9_overall_compensation_status.py` | 이사·감사 전체 보수현황(보수지급금액 - 유형별) 수집 | [보수유형별현황](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019013) |
| `10_disclosure_info.py` | **공시정보 수집** (모든 상장기업) | [공시정보검색](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001) |

### 데이터 파일

```
data/
├── minority_shareholders.csv                    # 소액주주 현황 데이터
├── minority_shareholders_failed_data.csv        # 소액주주 현황 수집 실패 데이터
├── employee_status.csv                          # 직원 현황 데이터
├── employee_status_failed_data.csv              # 직원 현황 수집 실패 데이터
├── individual_compensation_directors_auditors_failed_data.csv  # 개인별 보수 수집 실패 데이터
├── overall_compensation_directors_auditors_failed_data.csv     # 전체 보수 수집 실패 데이터
└── disclosure_info.csv                          # 공시정보 데이터 (새로 추가)
```

### 설정 및 유틸리티 파일

| 파일명 | 설명 |
|--------|------|
| `README.md` | 프로젝트 설명서 |
| `requirements.txt` | Python 의존성 패키지 목록 |
| `.env` | 환경변수 파일 (DART API 키) |
| `.gitignore` | Git 제외 파일 목록 |
| `setup.bat` | Windows 자동 환경 설정 스크립트 |
| `setup.sh` | macOS/Linux 자동 환경 설정 스크립트 |

## 🎯 사용 방법

### 개별 스크립트 실행

```bash
# 가상환경 활성화 후
python 1_minority_shareholders.py
python 2_executive_status.py
python 3_employee_status.py
# ... 기타 스크립트들
python 10_disclosure_info.py
```

### 공시정보 수집 (권장)

```bash
python 10_disclosure_info.py
```

이 스크립트는 모든 상장기업의 공시정보를 2015년부터 2024년까지 수집합니다.

## ⚠️ API 제한사항

- **일일 요청 제한**: 20,000건
- **분당 요청 제한**: 1,000회
- **자동 지연**: 각 API 호출 후 100ms 지연으로 제한사항 준수

## 📊 데이터 출력

### 공시정보 데이터 구조 (`disclosure_info.csv`)

| 필드명 | 설명 | 예시 |
|--------|------|------|
| `corp_cls` | 법인구분 | Y(유가), K(코스닥), N(코넥스), E(기타) |
| `corp_name` | 종목명(법인명) | 삼성전자 |
| `corp_code` | 고유번호 | 00126380 |
| `stock_code` | 종목코드 | 005930 |
| `report_nm` | 보고서명 | 사업보고서 (2023.12) |
| `rcept_no` | 접수번호 | 20240315000542 |
| `flr_nm` | 공시 제출인명 | 삼성전자(주) |
| `rcept_dt` | 접수일자 | 20240315 |
| `rm` | 비고 | 유, 연, 정 등 |

### 기타 데이터 구조

각 스크립트별로 해당하는 DART API의 응답 구조를 그대로 CSV로 저장합니다.

## 🔧 주요 기능

### 1. 자동 재시작 기능
- 중단된 작업을 자동으로 감지하고 재개
- 이미 처리된 데이터는 건너뛰기
- 실패한 데이터는 별도 파일로 관리

### 2. 오류 처리
- API 제한 초과 시 자동 종료
- 네트워크 오류 시 재시도
- 실패한 회사-연도 조합 기록

### 3. 데이터 정제
- 개행문자 제거
- UTF-8 BOM 인코딩으로 한글 지원
- 실시간 CSV 저장으로 메모리 효율성

## ⚠️ 주의사항

1. **API 키 보안**: `.env` 파일을 Git에 커밋하지 마세요
2. **요청 제한**: 하루 2만건 제한을 초과하지 않도록 주의
3. **데이터 용량**: 대용량 데이터 수집 시 충분한 디스크 공간 확보
4. **네트워크**: 안정적인 인터넷 연결 필요
5. **실행 시간**: 전체 데이터 수집에는 수 시간이 소요될 수 있음

## 📞 지원

- **DART OpenAPI 문서**: https://opendart.fss.or.kr/guide/detail.do
- **API 키 발급**: https://opendart.fss.or.kr/
- **문의사항**: DART 공식 고객지원

## 📄 라이선스

이 프로젝트는 교육 및 연구 목적으로 제작되었습니다. DART API 이용약관을 준수하여 사용하시기 바랍니다.
