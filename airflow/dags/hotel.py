from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import ast
from difflib import SequenceMatcher
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def download_files():
    try:
        hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = 'team-hori-2-bucket'
        accommodations_key = 'source/source_TravelEvents/Accommodations.csv'
        hotel_list_key = 'source/source_TravelEvents/hotel_list.csv'
        
        # 파일 경로 설정
        local_accommodations_path = '/tmp/Accommodations.csv'
        local_hotel_list_path = '/tmp/hotel_list.csv'
        
        # 기존 파일 삭제
        if os.path.exists(local_accommodations_path):
            os.remove(local_accommodations_path)
        if os.path.exists(local_hotel_list_path):
            os.remove(local_hotel_list_path)
        
        # S3에서 Accommodations.csv 파일 다운로드
        if hook.check_for_key(accommodations_key, bucket_name):
            s3_accommodations_object = hook.get_key(accommodations_key, bucket_name)
            accommodations_content = s3_accommodations_object.get()['Body'].read().decode('utf-8')
            with open(local_accommodations_path, 'w') as f:
                f.write(accommodations_content)
            logging.info(f"Downloaded {accommodations_key} to {local_accommodations_path}")
        else:
            logging.error(f"{accommodations_key} does not exist in the S3 bucket.")
            return

        # S3에서 hotel_list.csv 파일 다운로드
        if hook.check_for_key(hotel_list_key, bucket_name):
            s3_hotel_list_object = hook.get_key(hotel_list_key, bucket_name)
            hotel_list_content = s3_hotel_list_object.get()['Body'].read().decode('utf-8')
            with open(local_hotel_list_path, 'w') as f:
                f.write(hotel_list_content)
            logging.info(f"Downloaded {hotel_list_key} to {local_hotel_list_path}")
        else:
            logging.error(f"{hotel_list_key} does not exist in the S3 bucket.")
            return
    except Exception as e:
        logging.error(f"Error in download_files: {str(e)}")

def process_accommodations():
    try:
        accommodations_df = pd.read_csv('/tmp/Accommodations.csv')
        hotel_list_df = pd.read_csv('/tmp/hotel_list.csv')
        
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
        
        # 병합된 DataFrame을 CSV로 변환하여 저장
        accommodations_df.to_csv('/tmp/Updated_Accommodations.csv', index=False)
        logging.info("Accommodations processed and saved to '/tmp/Updated_Accommodations.csv'")
    except Exception as e:
        logging.error(f"Error in process_accommodations: {str(e)}")

def upload_file():
    try:
        hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = 'team-hori-2-bucket'
        output_key = 'source/source_TravelEvents/Updated_Accommodations.csv'
        
        # 로컬에서 S3로 파일 업로드
        hook.load_file(
            filename='/tmp/Updated_Accommodations.csv',
            key=output_key,
            bucket_name=bucket_name,
            replace=True
        )
        logging.info(f"File uploaded to S3 at {output_key}")
    except Exception as e:
        logging.error(f"Error in upload_file: {str(e)}")

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
    's3_accommodations_process',
    default_args=default_args,
    description='Download Accommodations and Hotel List CSV, process and upload updated Accommodations CSV',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='download_files',
    python_callable=download_files,
    dag=dag,
)

t2 = PythonOperator(
    task_id='process_accommodations',
    python_callable=process_accommodations,
    dag=dag,
)

t3 = PythonOperator(
    task_id='upload_file',
    python_callable=upload_file,
    dag=dag,
)

t1 >> t2 >> t3
