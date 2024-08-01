from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import os
import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

GOOGLE_API_KEY = Variable.get("GOOGLE_API_KEY")

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
        if rating >= 4.0 and user_ratings_total >= 100:
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
        
        # 'location' 열에 있는 각 위치에 대해 숙박시설 정보 가져오기
        all_accommodations = []
        for loc_str in df['location']:
            location = ast.literal_eval(loc_str)
            accommodations = fetch_accommodations(location)
            for acc in accommodations:
                acc['location'] = location  # location 추가
                all_accommodations.append(acc)
        
        # 결과를 DataFrame으로 변환
        result_df = pd.DataFrame(all_accommodations)
        
        # DataFrame을 CSV로 변환
        csv_buffer = StringIO()
        result_df.to_csv(csv_buffer, index=False)
        
        # 새로운 CSV 파일을 S3에 업로드
        output_key = 'source/source_TravelEvents/Accommodations.csv'
        hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=output_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        print(f"File saved to S3 at {output_key}")
    else:
        print("Input file not found in S3")

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
    's3_to_google_places_to_s3',
    default_args=default_args,
    description='Fetch accommodations around locations from S3 CSV and save to new CSV in S3',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='process_locations',
    python_callable=process_locations,
    dag=dag,
)

t1
