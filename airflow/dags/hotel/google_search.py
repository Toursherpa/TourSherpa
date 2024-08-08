from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import ast
from difflib import SequenceMatcher
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

# 환경 변수 설정
GOOGLE_API_KEY = Variable.get("GOOGLE_API_KEY")
S3_BUCKET = 'team-hori-2-bucket'
S3_INPUT_KEY = 'source/source_TravelEvents/TravelEvents.csv'
S3_OUTPUT_KEY = 'source/source_TravelEvents/google_hotels.csv'
AWS_CONN_ID = 's3_connection'

# 함수 정의
def fetch_hotel_info(location):
    endpoint_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        'location': f"{location[1]},{location[0]}",
        'radius': 5000,
        'type': 'lodging',
        'key': GOOGLE_API_KEY
    }
    response = requests.get(endpoint_url, params=params)
    results = response.json().get('results', [])

    hotels = []
    for result in results:
        hotel_info = {
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
        hotels.append(hotel_info)
    return hotels

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def download_csv_from_s3(**kwargs):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_object = hook.get_key(S3_INPUT_KEY, S3_BUCKET)
    content = s3_object.get()['Body'].read().decode('utf-8')
    
    # 오늘 날짜의 디렉터리 생성
    today_date = datetime.today().strftime('%Y-%m-%d')
    local_dir = f'/tmp/{today_date}'
    os.makedirs(local_dir, exist_ok=True)
    local_events_path = os.path.join(local_dir, 'TravelEvents.csv')
    
    with open(local_events_path, 'w') as f:
        f.write(content)
    
    # 경로를 XCom에 저장
    kwargs['ti'].xcom_push(key='local_events_path', value=local_events_path)

def process_locations(**kwargs):
    local_events_path = kwargs['ti'].xcom_pull(task_ids='download_csv_from_s3', key='local_events_path')

    df = pd.read_csv(local_events_path)
    google_hotels = []
    for loc_str in df['location']:
        location = ast.literal_eval(loc_str)
        hotels = fetch_hotel_info(location)
        for hotel in hotels:
            hotel['location'] = location  # location 추가
            google_hotels.append(hotel)

    result_df = pd.DataFrame(google_hotels)
    
    # 오늘 날짜의 디렉터리 생성 및 파일 저장
    today_date = datetime.today().strftime('%Y-%m-%d')
    local_dir = f'/tmp/{today_date}'
    os.makedirs(local_dir, exist_ok=True)
    local_hotels_path = os.path.join(local_dir, 'google_hotels.csv')
    
    result_df.to_csv(local_hotels_path, index=False)
    
    # 경로를 XCom에 저장
    kwargs['ti'].xcom_push(key='local_hotels_path', value=local_hotels_path)

def upload_csv_to_s3(**kwargs):
    local_hotels_path = kwargs['ti'].xcom_pull(task_ids='process_locations', key='local_hotels_path')
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    attempt = 0
    while attempt < 3:
        try:
            hook.load_file(
                filename=local_hotels_path,
                key=S3_OUTPUT_KEY,
                bucket_name=S3_BUCKET,
                replace=True
            )
            print(f"File uploaded to S3 at {S3_OUTPUT_KEY}")
            return
        except Exception as e:
            print(f"Error uploading file: {e}. Retrying... ({attempt + 1}/3)")
            attempt += 1
    raise RuntimeError("Failed to upload file after multiple attempts.")

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
    'google_place_list',
    default_args=default_args,
    description='Fetch accommodations around locations from S3 CSV and save to new CSV in S3',
    schedule_interval=timedelta(days=5),
    catchup=False,
)

t1 = PythonOperator(
    task_id='download_csv_from_s3',
    python_callable=download_csv_from_s3,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='process_locations',
    python_callable=process_locations,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='upload_csv_to_s3',
    python_callable=upload_csv_to_s3,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3