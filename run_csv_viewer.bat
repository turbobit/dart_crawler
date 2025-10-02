@echo off
echo ========================================
echo    CSV 뷰어 서버 (개발 모드)
echo ========================================
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
echo 서버를 시작합니다 (개발 모드)...
echo 브라우저가 자동으로 열립니다...
echo.
echo 서버를 중지하려면 Ctrl+C를 누르세요.
echo.

REM 서버 실행
go run main.go

echo.
echo 서버가 종료되었습니다.
pause
