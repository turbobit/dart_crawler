#!/bin/bash

echo "DART 크롤러 환경 설정을 시작합니다..."

echo ""
echo "1. 가상환경 생성 중..."
python3 -m venv venv

echo ""
echo "2. 가상환경 활성화 중..."
source venv/bin/activate

echo ""
echo "3. 의존성 설치 중..."
pip install -r requirements.txt

echo ""
echo "4. .env 파일 확인 중..."
if [ ! -f .env ]; then
    echo ".env 파일이 없습니다. 생성합니다..."
    echo "DART_API_KEY=your_dart_api_key_here" > .env
    echo ""
    echo "⚠️  .env 파일에 DART API 키를 입력해주세요!"
    echo "   DART API 키 발급: https://opendart.fss.or.kr/"
else
    echo ".env 파일이 이미 존재합니다."
fi

echo ""
echo "✅ 환경 설정이 완료되었습니다!"
echo ""
echo "다음 단계:"
echo "1. .env 파일에 DART API 키 입력"
echo "2. python 10_disclosure_info.py 실행"
echo ""
