# CSV 뷰어 웹 애플리케이션

DART 크롤러에서 생성된 CSV 파일들을 웹 브라우저에서 쉽게 확인할 수 있는 Go 웹 애플리케이션입니다.

## 기능

- 📁 **파일 목록**: data 폴더의 모든 CSV 파일을 카드 형태로 표시
- 🔍 **검색 기능**: CSV 데이터 내에서 키워드 검색
- 📄 **페이지네이션**: 대용량 데이터를 페이지별로 나누어 표시
- 💾 **다운로드**: 원본 파일을 다른 이름으로 저장
- 📱 **반응형 디자인**: 모바일과 데스크톱에서 모두 사용 가능

## 설치 및 실행

### 1. Go 설치
Go 1.21 이상이 설치되어 있어야 합니다.

### 2. 실행 방법

#### 방법 1: 배치 파일 사용 (권장)
- **개발 모드**: `run_csv_viewer.bat` - Go 소스코드로 직접 실행
- **빌드**: `build_csv_viewer.bat` - 실행 파일 생성
- **실행**: `start_csv_viewer.bat` - 빌드된 실행 파일로 실행

#### 방법 2: 수동 실행
```bash
# 의존성 설치
go mod tidy

# 개발 모드로 실행
go run main.go

# 또는 빌드 후 실행
go build -o csv_viewer.exe main.go
./csv_viewer.exe
```

### 3. 브라우저 접속
서버 시작 후 2초 뒤에 자동으로 `http://localhost:8080`이 열립니다.

## 사용법

### 메인 페이지
- data 폴더의 모든 CSV 파일이 카드 형태로 표시됩니다
- 각 카드에는 파일명, 수정일, 파일 크기가 표시됩니다
- "보기" 버튼을 클릭하면 데이터를 확인할 수 있습니다
- "다운로드" 버튼을 클릭하면 파일을 다른 이름으로 저장할 수 있습니다

### 데이터 뷰어 페이지
- **검색**: 상단의 검색창에 키워드를 입력하여 데이터를 필터링할 수 있습니다
- **페이지 크기**: 50, 100, 200, 500개씩 보기 옵션을 선택할 수 있습니다
- **페이지네이션**: 하단의 페이지 번호를 클릭하여 다른 페이지로 이동할 수 있습니다
- **다운로드**: 우상단의 다운로드 버튼으로 파일을 저장할 수 있습니다

## 지원하는 CSV 파일

현재 data 폴더에 있는 CSV 파일들:
- `disclosure_info.csv` - 공시 정보
- `disclosure_info_failed_data.csv` - 공시 정보 실패 데이터
- `employee_status.csv` - 직원 현황
- `employee_status_failed_data.csv` - 직원 현황 실패 데이터
- `individual_compensation_directors_auditors_failed_data.csv` - 개별 보상 실패 데이터
- `minority_shareholders.csv` - 소액주주 정보
- `minority_shareholders_failed_data.csv` - 소액주주 정보 실패 데이터
- `overall_compensation_directors_auditors_failed_data.csv` - 전체 보상 실패 데이터

## 기술 스택

- **Backend**: Go 1.21
- **Web Framework**: Gorilla Mux
- **Frontend**: Bootstrap 5, Font Awesome
- **Template Engine**: Go html/template

## 프로젝트 구조

```
csv-viewer/
├── main.go                    # 메인 애플리케이션
├── go.mod                     # Go 모듈 파일
├── run_csv_viewer.bat         # 개발 모드 실행 (Go 소스코드)
├── build_csv_viewer.bat       # 빌드 스크립트
├── start_csv_viewer.bat       # 빌드된 실행 파일 실행
├── csv_viewer.exe             # 빌드된 실행 파일
├── templates/                 # HTML 템플릿
│   ├── index.html            # 메인 페이지
│   └── view.html             # 데이터 뷰어 페이지
├── static/                   # 정적 파일 (CSS, JS, 이미지)
└── data/                     # CSV 데이터 파일들
```

## API 엔드포인트

- `GET /` - 메인 페이지 (파일 목록)
- `GET /view/{filename}` - CSV 파일 뷰어 페이지
- `GET /api/data/{filename}` - CSV 데이터 API (JSON)
- `GET /download/{filename}` - 파일 다운로드

## 개발자 정보

이 애플리케이션은 DART 크롤러 프로젝트의 데이터를 웹에서 쉽게 확인할 수 있도록 개발되었습니다.
