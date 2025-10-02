@echo off
echo CSV 뷰어를 시작합니다...
echo.

REM 실행 파일이 존재하는지 확인
if not exist csv_viewer.exe (
    echo 오류: csv_viewer.exe 파일이 없습니다.
    echo 먼저 build_csv_viewer.bat을 실행하여 빌드해주세요.
    echo.
    pause
    exit /b 1
)

echo 서버를 시작합니다...
echo 브라우저가 자동으로 열립니다...
echo.
echo 서버를 중지하려면 Ctrl+C를 누르세요.
echo.

REM 서버 실행
csv_viewer.exe

echo.
echo 서버가 종료되었습니다.
pause
