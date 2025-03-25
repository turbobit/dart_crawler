## 소액주주 현황 수집 : https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019009
## api 제약 하루 2만건 미만, 분당 1000회 미만

import datetime
import os
import requests
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import time
from dotenv import load_dotenv
from pathlib import Path

# .env 파일에서 API 키 로드
load_dotenv()
API_KEY = os.getenv('DART_API_KEY')

# 필요한 디렉토리 생성
data_dir = Path('data')
data_dir.mkdir(exist_ok=True)

def download_corp_codes():
    """회사 고유번호 목록 다운로드 및 압축 해제"""
    corp_code_file = data_dir / 'corpCode.xml'
    
    # 이미 파일이 존재하면 다운로드 생략
    if corp_code_file.exists():
        print("이미 회사 고유번호 파일이 존재합니다.")
        return corp_code_file
    
    print("회사 고유번호 목록 다운로드 중...")
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={API_KEY}"
    response = requests.get(url)


    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code}")
    
    # 압축 파일 저장
    zip_path = data_dir / 'corpCode.zip'
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    
    # 압축 해제
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(data_dir)
    
    # 압축 파일 삭제
    zip_path.unlink()
    
    print("회사 고유번호 목록 다운로드 완료")
    return corp_code_file

def parse_corp_codes(file_path):
    """회사 고유번호 XML 파일 파싱"""
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    corps = []
    for company in root.findall('list'):
        corp_code = company.findtext('corp_code')
        corp_name = company.findtext('corp_name')
        stock_code = company.findtext('stock_code')
        
        # 상장 회사만 필터링 (주식 코드가 있는 회사)
        if stock_code and stock_code.strip():
            corps.append({
                'corp_code': corp_code,
                'corp_name': corp_name,
                'stock_code': stock_code
            })
    
    return corps

def get_minority_shareholders(corp_code, year, report_code='11011'):
    """소액주주 현황 데이터 가져오기"""
    url = "https://opendart.fss.or.kr/api/mrhlSttus.json"
    params = {
        'crtfc_key': API_KEY,
        'corp_code': corp_code,
        'bsns_year': str(year),
        'reprt_code': report_code
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"API 호출 실패: {response.status_code} - {corp_code}")
        return None
    
    data = response.json()
    
    if data['status'] != '000':
        print(f"데이터 조회 실패: {data['status']} - {data['message']} - {corp_code} - {year}")
        return None
    
    return data['list']

def main():    
    # 진행 상황을 저장할 파일
    progress_file = data_dir / 'progress.csv'
    output_file = data_dir / 'minority_shareholders.csv'
    processed_companies = set()

    if output_file.exists():
        print("이미 처리된 데이터가 존재합니다.")
        # 파일을 읽어서 3번째 컬럼을 읽어서 처리된 회사 코드를 추출
        processed_companies = set(row[2] for row in pd.read_csv(output_file).values.tolist())
        # print("처리된 회사 코드:", processed_companies)
        print(f"이미 처리된 회사 코드 수: {len(processed_companies)}")

    # 회사 고유번호 목록 가져오기
    corp_code_file = download_corp_codes()
    corps = parse_corp_codes(corp_code_file)
    
    print(f"웹에 등록된 총 {len(corps)}개 상장 회사 발견")

    # processed_companies 와 corps 를 비교하여 처리되지 않은 회사 코드를 추출
    unprocessed_corps = [corp for corp in corps if corp['corp_code'] not in processed_companies]
    # print(f"처리되지 않은 회사 코드: {unprocessed_corps}")
    print(f"처리되지 않은 회사 코드 수: {len(unprocessed_corps)}")

    return
    
    # 결과 저장할 리스트
    all_data = []
    progress_data = []
        
    # 회사별로 소액주주 현황 데이터 수집
    for i, corp in enumerate(corps):
        # corp_code가 processed_companies에 있는지 확인
        if resume_scan and corp['corp_code'] in processed_companies:
            print(f"[{i+1}/{len(corps)}] {corp['corp_name']} 건너뛰기 (이미 처리됨)")
            continue
            
        print(f"[{i+1}/{len(corps)}] {corp['corp_code']} - {corp['corp_name']} 데이터 수집 중...")
        
        has_data = False
        for year in range(2015, datetime.datetime.now().year + 1):
            data = get_minority_shareholders(corp['corp_code'], year)

            if data:
                has_data = True
                for item in data:
                    item['stock_code'] = corp['stock_code']
                    all_data.append(item)
                    pd.DataFrame([item]).to_csv(output_file, index=False, encoding='utf-8-sig', mode='a', header=False)

            # API 호출 후 즉시 지연 추가
            time.sleep(0.1)  # 분당 1000회 미만을 위해 100ms 지연

        # 진행 상황 저장
        progress_data.append({
            'corp_code': corp['corp_code'],
            'corp_name': corp['corp_name'],
            'processed_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        pd.DataFrame(progress_data).to_csv(progress_file, index=False, encoding='utf-8-sig', mode='a', header=False)

        
        print(f"조회된 자료의 수: {len(all_data)}")

    # 데이터프레임으로 변환
    if all_data:
        print(f"데이터 수집 완료: {output_file}에 저장됨")
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main() 