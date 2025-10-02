## https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
## 공시정보 API - 모든 회사별 공시정보 수집
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

output_file = data_dir / 'disclosure_info.csv'
failed_file = data_dir / 'disclosure_info_failed_data.csv'
start_date = '20150101'  # 시작일 (YYYYMMDD)
end_date = '20241231'    # 종료일 (YYYYMMDD)

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

def get_data(corp_code, bgn_de, end_de, page_no=1, page_count=100):
    """공시정보 데이터 가져오기"""
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        'crtfc_key': API_KEY,
        'corp_code': corp_code,
        'bgn_de': bgn_de,
        'end_de': end_de,
        'page_no': page_no,
        'page_count': page_count,
        'sort': 'date',
        'sort_mth': 'desc'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"API 호출 실패: {response.status_code} - {corp_code}")
        return None, None
    
    data = response.json()
    
    if data['status'] != '000':
        print(f"데이터 조회 실패: {data['status']} - {data['message']} - {corp_code}")

        if data['status'] == '020':
            #요청 제한을 초과 완전히 프로그램 종료
            print("요청 제한을 초과하여 프로그램을 종료합니다.(하루 2만건 제한)")
            exit()

        if data['status'] == '013':
            #조회된 데이터가 없음
            # 아래처럼 조회 실패한데이터는 보관하고 넘어가게 하는 코드 추가,  {corp_code} 를 저장
            with open(failed_file, 'a') as f:
                f.write(f"{corp_code}\n")
            return None, None

        return None, None
    
    return data['list'], data.get('total_count', 0)

def main():    
    processed_companies = set()
    failed_companies = set()
    first_write = not output_file.exists()  # 파일이 존재하지 않으면 True

    if output_file.exists():
        print("이미 처리된 데이터가 존재합니다.")
        df = pd.read_csv(output_file, 
                        dtype={'corp_code': str},
                        header=0)
        processed_companies = set(df['corp_code'].tolist())
        print(f"이미 처리된 회사 코드 수: {len(processed_companies)}")
    

    # 회사 고유번호 목록 가져오기
    corp_code_file = download_corp_codes()
    corps = parse_corp_codes(corp_code_file)
    
    print(f"처리할 총 {len(corps)}개 상장 회사 발견")

    # processed_companies 와 corps 를 비교하여 처리되지 않은 회사 코드를 추출
    unprocessed_corps = [corp for corp in corps if corp['corp_code'] not in processed_companies]
    print(f"처리되지 않은 회사 코드 수: {len(unprocessed_corps)}")


    # failed_data.csv 파일에서 실패한 회사 코드 읽기
    try:
        with open(failed_file, 'r') as f:
            failed_data = f.readlines()
        for line in failed_data:
            corp_code = line.strip()
            if corp_code:
                failed_companies.add(corp_code)
        print(f"실패한 조회 건수: {len(failed_companies)}")
    except FileNotFoundError:
        print("failed_data.csv 파일이 없습니다. 새로 생성됩니다.")
        with open(failed_file, 'w') as f:
            pass

    # 결과 저장할 리스트
    all_data = []
        
    # 회사별로 공시정보 데이터 수집
    for i, corp in enumerate(corps):

        # corp_code가 processed_companies에 있는지 확인
        if unprocessed_corps and corp['corp_code'] in processed_companies:
            print(f"[{i+1}/{len(corps)}] {corp['corp_name']} 건너뛰기 (이미 처리됨)")
            continue

        # 실패 이력이 있는 회사 건너뛰기
        if corp['corp_code'] in failed_companies:
            print(f"[{i+1}/{len(corps)}] {corp['corp_code']} - {corp['corp_name']} 건너뛰기 (이미 실패 이력 있음)")
            continue

        print(f"[{i+1}/{len(corps)}] {corp['corp_code']} - {corp['corp_name']} 데이터 수집 중...")
        
        # 페이지별로 데이터 수집
        page_no = 1
        page_count = 100
        total_collected = 0
        
        while True:
            data, total_count = get_data(corp['corp_code'], start_date, end_date, page_no, page_count)
            
            if data is None:
                break
                
            if not data:  # 데이터가 없으면 종료
                break
                
            for item in data:
                # 모든 문자열 값에서 개행문자 제거
                for key, value in item.items():
                    if isinstance(value, str):
                        item[key] = value.replace('\n', ' ').strip()
                
                # 회사 정보 추가
                item['stock_code'] = corp['stock_code']
                item['corp_name'] = corp['corp_name']
                all_data.append(item)
                total_collected += 1
                
                # 첫 번째 쓰기일 때만 헤더 포함, 이후에는 헤더 제외
                pd.DataFrame([item]).to_csv(output_file, 
                                          index=False, 
                                          encoding='utf-8-sig', 
                                          mode='a',
                                          header=first_write)
                first_write = False  # 첫 번째 쓰기 후에는 False로 설정

            # API 호출 후 즉시 지연 추가
            time.sleep(0.1)  # 분당 1000회 미만을 위해 100ms 지연
            
            # 다음 페이지가 있는지 확인
            if len(data) < page_count or total_collected >= total_count:
                break
                
            page_no += 1
        
        print(f"  - {corp['corp_name']} 수집 완료: {total_collected}건")

    # 데이터프레임으로 변환
    if all_data:
        print(f"데이터 수집 완료: 총 {len(all_data)}건이 {output_file}에 저장됨")
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main() 