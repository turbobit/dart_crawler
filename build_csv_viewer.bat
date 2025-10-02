@echo off
echo CSV 뷰어를 빌드합니다...
echo.

REM Go가 설치되어 있는지 확인
go version >nul 2>&1
if errorlevel 1 (
    echo 오류: Go가 설치되어 있지 않습니다.
    echo https://golang.org/dl/ 에서 Go를 다운로드하여 설치해주세요.
    pause
    exit /b 1
)

REM 의존성 설치
echo 의존성을 설치합니다...
go mod tidy
if errorlevel 1 (
    echo 오류: 의존성 설치에 실패했습니다.
    pause
    exit /b 1
)

echo.
echo 프로그램을 빌드합니다...

REM 기존 실행 파일 삭제
if exist csv_viewer.exe del csv_viewer.exe

REM 빌드 실행
go build -o csv_viewer.exe main.go
if errorlevel 1 (
    echo 오류: 빌드에 실패했습니다.
    pause
    exit /b 1
)

echo.
echo ✅ 빌드가 완료되었습니다!
echo 생성된 파일: csv_viewer.exe
echo.
echo 실행하려면 csv_viewer.exe를 더블클릭하거나
echo run_csv_viewer.bat을 실행하세요.
echo.

pause
