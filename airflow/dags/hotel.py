from datetime import datetime, timedelta
import pandas as pd
from geopy.distance import great_circle
import os
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.S3_hook import S3Hook

def preprocess_text(text):
    """문자열 전처리 함수"""
    if isinstance(text, str):
        return text.lower().strip()
    return str(text).strip()  # 문자열이 아닌 경우에도 처리 가능하도록

def download_files():
    """S3에서 파일 다운로드"""
    print("Downloading files from S3...")
    hook = S3Hook(aws_conn_id='s3_connection')
    bucket_name = 'team-hori-2-bucket'
    accommodations_key = 'source/source_TravelEvents/Accommodations.csv'
    hotel_list_key = 'source/source_TravelEvents/hotel_list.csv'
    events_key = 'source/source_TravelEvents/TravelEvents.csv'
    local_accommodations_path = '/tmp/Accommodations.csv'
    local_hotel_list_path = '/tmp/hotel_list.csv'
    local_events_path = '/tmp/TravelEvents.csv'
    if os.path.exists(local_accommodations_path):
        os.remove(local_accommodations_path)
    if os.path.exists(local_hotel_list_path):
        os.remove(local_hotel_list_path)
 
    s3_events_object = hook.get_key(events_key, bucket_name)
    events_content = s3_events_object.get()['Body'].read().decode('utf-8')
    with open(local_events_path, 'w') as f:
        f.write(events_content)
        
    s3_accommodations_object = hook.get_key(accommodations_key, bucket_name)
    accommodations_content = s3_accommodations_object.get()['Body'].read().decode('utf-8')
    with open(local_accommodations_path, 'w') as f:
        f.write(accommodations_content)
    
    s3_hotel_list_object = hook.get_key(hotel_list_key, bucket_name)
    hotel_list_content = s3_hotel_list_object.get()['Body'].read().decode('utf-8')
    with open(local_hotel_list_path, 'w') as f:
        f.write(hotel_list_content)

def calculate_distance(location1, location2):
    """두 위치 간의 거리 계산 (킬로미터 단위)"""
    return great_circle(location1, location2).kilometers

def parse_location(location_str):
    """위치 문자열을 튜플로 변환 (위도, 경도)"""
    try:
        location = eval(location_str)
        if isinstance(location, list) and len(location) == 2:
            return (float(location[1]), float(location[0]))  # (위도, 경도) 순서로 변환
    except:
        return None
    return None

def parse_location_from_lat_lon(latitude, longitude):
    """위도와 경도에서 위치 튜플 생성"""
    try:
        return (float(latitude), float(longitude))  # (위도, 경도) 순서
    except:
        return None

def exact_match(row, hotel_list_df):
    """정확한 일치 검색 및 위치 정보 확인"""
    hotel_list_df['name_normalized'] = hotel_list_df['hotel_name'].apply(preprocess_text)
    row['name_normalized'] = preprocess_text(row['name'])
    
    # 문자열이 아닌 값이 있는 경우 원래 데이터를 반환
    if not isinstance(row['name_normalized'], str):
        return row
    
    matching_hotels = hotel_list_df[hotel_list_df['name_normalized'] == row['name_normalized']]
    
    if not matching_hotels.empty:
        row_location = parse_location(row['location'])
        if row_location is None:
            return row
        for _, match in matching_hotels.iterrows():
            match_location = parse_location_from_lat_lon(match['latitude'], match['longitude'])
            if match_location is None:
                continue
            distance = calculate_distance(row_location, match_location)
            if distance <= 2:  # 2km 이내
                return match  # 모든 칼럼을 포함한 일치하는 행
    return row
def process_chunk(chunk, hotel_list_df):
    """청크를 처리하여 호텔 ID 및 기타 정보를 매칭"""
    result_list = []
    for _, row in chunk.iterrows():
        match = exact_match(row, hotel_list_df)
        result_row = row.to_dict()
        if match is not None:
            
            result_row.update(match.to_dict())  # 매칭된 호텔의 모든 정보를 추가
        result_list.append(result_row)
    
    return pd.DataFrame(result_list)

def process_accommodations():
    """숙소 데이터를 처리하여 호텔 ID를 매칭"""
    print("Processing accommodations...")
    accommodations_df = pd.read_csv('/tmp/Accommodations.csv')
    hotel_list_df = pd.read_csv('/tmp/hotel_list.csv', dtype=str)

    print("DataFrames loaded. Starting matching process...")

    # 데이터프레임을 청크로 나누고 병렬 처리
    num_chunks = 12  # 병렬 처리할 청크 수
    chunks = np.array_split(accommodations_df, num_chunks)
    
    # 실패 시 재시도하는 함수
    def retry_process_chunk(chunk):
        attempt = 0
        while attempt < 3:
            try:
                return process_chunk(chunk, hotel_list_df)
            except Exception as e:
                print(f"Error processing chunk: {e}. Retrying... ({attempt + 1}/3)")
                attempt += 1
        raise RuntimeError("Failed to process chunk after multiple attempts.")

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(retry_process_chunk, chunks))

    processed_df = pd.concat(results)

    for index, row in processed_df.iterrows():
        if pd.isna(row.get('hotel_id')):
            print(f"Row {index}: fail")
        else:
            print(f"Row {index}: hotel_id = {row['hotel_id']}")
    
    processed_df.to_csv('/tmp/Updated_Accommodations.csv', index=False)

def upload_file():
    """처리된 파일을 S3에 업로드"""
    hook = S3Hook(aws_conn_id='s3_connection')
    bucket_name = 'team-hori-2-bucket'
    output_key = 'source/source_TravelEvents/Updated_Accommodations.csv'
    
    attempt = 0
    while attempt < 3:
        try:
            hook.load_file(
                filename='/tmp/Updated_Accommodations.csv',
                key=output_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"File uploaded to S3 at {output_key}")
            return
        except Exception as e:
            print(f"Error uploading file: {e}. Retrying... ({attempt + 1}/3)")
            attempt += 1
    raise RuntimeError("Failed to upload file after multiple attempts.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_accommodations_process',
    default_args=default_args,
    description='Download Accommodations and Hotel List CSV, process and upload updated Accommodations CSV',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with TaskGroup('processing_group', dag=dag) as processing_group:
    process_tasks = [
        PythonOperator(
            task_id=f'process_chunk_{i}',
            python_callable=process_accommodations,
            dag=dag,
        )
        for i in range(12)
    ]

t1 = PythonOperator(
    task_id='download_files',
    python_callable=download_files,
    dag=dag,
)

t3 = PythonOperator(
    task_id='upload_file',
    python_callable=upload_file,
    dag=dag,
)

t1 >> processing_group >> t3
