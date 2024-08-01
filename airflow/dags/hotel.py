from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
from difflib import SequenceMatcher

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def process_accommodations():
    hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'team-hori-2-bucket'
    accommodations_key = 'source/source_TravelEvents/Accommodations.csv'
    hotel_list_key = 'source/source_TravelEvents/hotel_list.csv'
    
    # S3에서 Accommodations.csv 파일 읽기
    if hook.check_for_key(accommodations_key, bucket_name):
        s3_accommodations_object = hook.get_key(accommodations_key, bucket_name)
        accommodations_content = s3_accommodations_object.get()['Body'].read().decode('utf-8')
        
        # Accommodations.csv 파일을 pandas DataFrame으로 읽기
        accommodations_df = pd.read_csv(StringIO(accommodations_content))
        
        # S3에서 hotel_list.csv 파일 읽기
        if hook.check_for_key(hotel_list_key, bucket_name):
            s3_hotel_list_object = hook.get_key(hotel_list_key, bucket_name)
            hotel_list_content = s3_hotel_list_object.get()['Body'].read().decode('utf-8')
            
            # hotel_list.csv 파일을 pandas DataFrame으로 읽기
            hotel_list_df = pd.read_csv(StringIO(hotel_list_content))
            
            # 이름, 주소, 도시, 주 유사도 기준으로 병합
            def get_closest_match(row, hotel_list):
                max_similarity = 0
                closest_match = None
                for _, hotel in hotel_list.iterrows():
                    name_similarity = similar(row['name'], hotel['hotel_name'])
                    address_similarity = similar(row.get('address', ''), hotel['addressline1'])
                    city_similarity = similar(row.get('city', ''), hotel['city'])
                    state_similarity = similar(row.get('state', ''), hotel['state'])
                    
                    # 유사도의 가중치 합계
                    total_similarity = (name_similarity * 0.5) + (address_similarity * 0.2) + (city_similarity * 0.2) + (state_similarity * 0.1)
                    
                    if total_similarity > max_similarity:
                        max_similarity = total_similarity
                        closest_match = hotel
                
                if max_similarity > 0.8:  # 유사도 기준값 설정
                    return closest_match['hotel_id']
                else:
                    return None
            
            accommodations_df['hotel_id'] = accommodations_df.apply(lambda x: get_closest_match(x, hotel_list_df), axis=1)
            
            # 병합된 DataFrame을 CSV로 변환
            csv_buffer = StringIO()
            accommodations_df.to_csv(csv_buffer, index=False)
            
            # 새로운 CSV 파일을 S3에 업로드
            hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=accommodations_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            print(f"File saved to S3 at {accommodations_key}")
        else:
            print("hotel_list.csv file not found in S3")
    else:
        print("Accommodations.csv file not found in S3")

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
    's3_process_accommodations',
    default_args=default_args,
    description='Process Accommodations.csv and match with hotel_list.csv to add hotel_id',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='process_accommodations',
    python_callable=process_accommodations,
    dag=dag,
)

t1

