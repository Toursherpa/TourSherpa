from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import os
import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

# Agoda API 설정
AGODA_API_KEY = '1929649:7cc3444d-2aa8-4302-ad2e-81a22905524d'
AGODA_API_ENDPOINT = 'http://affiliateapi7643.agoda.com/affiliateservice/lt_v1'

# Google API Key
GOOGLE_API_KEY = 'AIzaSyBtisA7MOZ4fM248MoHSqBMbz5M4m_hggY'

def fetch_accommodations(location):
    endpoint_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        'location': f"{location[1]},{location[0]}",
        'radius': 5000,
        'type': 'lodging',
        'key': GOOGLE_API_KEY
    }
    response = requests.get(endpoint_url, params=params)
    results = response.json().get('results', [])
    
    accommodations = []
    for result in results:
        accommodation_info = {
            'name': result.get('name'),
            'address': result.get('vicinity'),
            'rating': result.get('rating'),
            'user_ratings_total': result.get('user_ratings_total'),
            'place_id': result.get('place_id'),
            'types': result.get('types'),
            'geometry': result.get('geometry'),
            'icon': result.get('icon'),
            'plus_code': result.get('plus_code'),
            'reference': result.get('reference'),
            'scope': result.get('scope'),
            'opening_hours': result.get('opening_hours'),
            'photos': result.get('photos'),
            'price_level': result.get('price_level'),
        }
        accommodations.append(accommodation_info)
    return accommodations

def fetch_agoda_details(location, name):
    headers = {
        'Accept-Encoding': 'gzip,deflate',
        'Authorization': AGODA_API_KEY
    }
    params = {
        "criteria": {
            "additional": {
                "currency": "USD",
                "discountOnly": False,
                "language": "en-us",
                "occupancy": {
                    "numberOfAdult": 2,
                    "numberOfChildren": 0
                }
            },
            "checkInDate": "2024-09-01",
            "checkOutDate": "2024-09-02",
            "cityId": location  # 혹은 hotelId로 대체
        }
    }
    response = requests.post(AGODA_API_ENDPOINT, json=params, headers=headers)
    if response.status_code == 200:
        return response.json().get('results', [])
    else:
        return None

def process_locations():
    hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'team-hori-2-bucket'
    input_key = 'source/source_TravelEvents/TravelEvents.csv'
    
    # S3에서 CSV 파일 읽기
    if hook.check_for_key(input_key, bucket_name):
        s3_object = hook.get_key(input_key, bucket_name)
        content = s3_object.get()['Body'].read().decode('utf-8')
        
        # CSV 파일을 pandas DataFrame으로 읽기
        df = pd.read_csv(StringIO(content))
        
        # 위치 리스트 반환
        return df['location'].tolist()
    else:
        raise FileNotFoundError("Input file not found in S3")

def process_single_location(location):
    location = ast.literal_eval(location)
    accommodations = fetch_accommodations(location)
    all_accommodations = []
    for acc in accommodations:
        acc['location'] = location  # location 추가
        agoda_details = fetch_agoda_details(location, acc['name'])
        if agoda_details:
            for detail in agoda_details:
                detail['google_place_info'] = acc
                all_accommodations.append(detail)
    return all_accommodations

def save_results_to_s3(**context):
    hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'team-hori-2-bucket'
    output_key = 'source/source_TravelEvents/Accommodations_Details.csv'

    all_accommodations = []
    for task_id in context['task_ids']:
        task_results = context['ti'].xcom_pull(task_ids=task_id)
        all_accommodations.extend(task_results)
    
    # 결과를 DataFrame으로 변환
    result_df = pd.DataFrame(all_accommodations)
    
    # DataFrame을 CSV로 변환
    csv_buffer = StringIO()
    result_df.to_csv(csv_buffer, index=False)
    
    # 새로운 CSV 파일을 S3에 업로드
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=output_key,
        bucket_name=bucket_name,
        replace=True
    )
    
    print(f"File saved to S3 at {output_key}")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_to_agoda_to_s3_parallel',
    default_args=default_args,
    description='Fetch accommodation details from Agoda API based on locations and names from S3 CSV and save to new CSV in S3',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# 위치 목록 가져오기 태스크
t1 = PythonOperator(
    task_id='fetch_locations',
    python_callable=process_locations,
    dag=dag,
)

# 위치별로 숙박 시설 정보를 가져오는 태스크 생성
location_tasks = []

for i in range(10):  # 위치 목록의 예시 개수로 대체
    location_task = PythonOperator(
        task_id=f'process_location_{i}',
        python_callable=process_single_location,
        op_args=[f'{{{{ ti.xcom_pull(task_ids="fetch_locations")[{i}] }}}}}'],
        dag=dag,
    )
    location_tasks.append(location_task)
    t1 >> location_task

# 결과를 S3에 저장하는 태스크
t2 = PythonOperator(
    task_id='save_results_to_s3',
    python_callable=save_results_to_s3,
    provide_context=True,
    dag=dag,
)

for location_task in location_tasks:
    location_task >> t2
