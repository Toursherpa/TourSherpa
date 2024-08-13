from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
import requests
import re
import pytz
import time
from amadeus import Client, ResponseError
from airflow.models import Variable

kst = pytz.timezone('Asia/Seoul')

def fetch_airport_data():
    def get_lat_lng(address):
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": Variable.get('GOOGLE_API_KEY')
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                location = results[0]["geometry"]["location"]
                return location["lat"], location["lng"]
        return None

    logging.info("공항 데이터를 가져오는 중...")
    airport_list = [
        # ... (공항 리스트)
    ]

    for i, v in enumerate(airport_list):
        address = v['airport_name']

        lat, lng = get_lat_lng(address)
        airport_list[i]['airport_location'] = [lat, lng]

    logging.info("공항 데이터 가져오기 완료")
    return pd.DataFrame(airport_list)

def upload_to_s3(df, filename):
    logging.info(f"{filename} 데이터를 S3로 업로드 중...")
    csv_filename = f'/tmp/{filename}.csv'
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_bucket_name = Variable.get('s3_bucket_name')
    s3_result_key = f'source/source_flight/{filename}.csv'
    s3_hook.load_file(filename=csv_filename, key=s3_result_key, bucket_name=s3_bucket_name, replace=True)
    logging.info(f"{filename} 데이터를 S3로 업로드 완료")

def data_to_s3(macros):
    logging.info("data_to_s3 작업 시작")
    
    try:
        df = fetch_airport_data()
        logging.info("공항 데이터 생성 완료")

        upload_to_s3(df, "airport")
        logging.info("공항 데이터를 S3로 업로드 완료")
    except Exception as e:
        logging.error(f"data_to_s3에서 오류 발생: {e}")
        raise

def create_redshift_table():
    try:
        logging.info("Redshift 테이블 생성 시작")
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS flight.airport;")
        redshift_conn.commit()
        logging.info("기존 테이블 삭제 완료")
        
        cursor.execute("""
            CREATE TABLE flight.airport (
                airport_code VARCHAR(255),
                airport_name VARCHAR(255),
                airport_location VARCHAR(255),
                country_code VARCHAR(255),
                country_name VARCHAR(255)
            );
        """)
        redshift_conn.commit()
        logging.info("새 테이블 생성 완료")

        redshift_conn.close()
    except Exception as e:
        logging.error(f"create_redshift_table에서 오류 발생: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 9, tzinfo=kst),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_airport',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)

data_to_s3_task = PythonOperator(
    task_id='data_to_s3',
    python_callable=data_to_s3,
    provide_context=True,
    dag=dag,
)

create_redshift_table_task = PythonOperator(
    task_id='create_redshift_table',
    python_callable=create_redshift_table,
    provide_context=True,
    dag=dag,
)

load_to_redshift_task = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    schema='flight',
    table='airport',
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_flight/airport.csv',
    copy_options=['IGNOREHEADER 1', 'CSV'],
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
)

data_to_s3_task >> create_redshift_table_task >> load_to_redshift_task
