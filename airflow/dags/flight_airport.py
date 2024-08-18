from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests
import pytz
import pandas as pd

# 한국 시간대 설정
kst = pytz.timezone('Asia/Seoul')

# Google Maps API를 사용하여 주소에서 위도와 경도를 가져오는 함수
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

# 공항 데이터를 가져오는 함수
def fetch_airport_data():
    airport_list = [
        {'airport_code': 'NRT', 'airport_name': '나리타 국제공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'KIX', 'airport_name': '간사이 국제공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'NGO', 'airport_name': '츄부국제공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'FUK', 'airport_name': '후쿠오카 공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'CTS', 'airport_name': '신치토세 공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'OKA', 'airport_name': '나하 공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'PEK', 'airport_name': '베이징 서우두 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'PVG', 'airport_name': '상하이 푸둥 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'CAN', 'airport_name': '광저우 바이윈 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'CKG', 'airport_name': '충칭 장베이 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'HRB', 'airport_name': '하얼빈 타이핑 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'HKG', 'airport_name': '홍콩 국제 공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'CGK', 'airport_name': '수카르노 하타 국제공항', 'airport_location': [], 'country_code': 'ID', 'country_name': '인도네시아'},
        {'airport_code': 'DEL', 'airport_name': '인디라 간디 국제공항', 'airport_location': [], 'country_code': 'IN', 'country_name': '인도'},
        {'airport_code': 'TPE', 'airport_name': '타이완 타오위안 국제공항', 'airport_location': [], 'country_code': 'TW', 'country_name': '대만'},
        {'airport_code': 'KHH', 'airport_name': '가오슝 국제공항', 'airport_location': [], 'country_code': 'TW', 'country_name': '대만'},
        {'airport_code': 'KUL', 'airport_name': '쿠알라룸푸르 국제공항', 'airport_location': [], 'country_code': 'MY', 'country_name': '말레이시아'},
        {'airport_code': 'BKI', 'airport_name': '코타키나발루 국제공항', 'airport_location': [], 'country_code': 'MY', 'country_name': '말레이시아'},
        {'airport_code': 'FRA', 'airport_name': '프랑크푸르트 암마인 공항', 'airport_location': [], 'country_code': 'DE', 'country_name': '독일'},
        {'airport_code': 'MAD', 'airport_name': '마드리드 바라하스 국제공항', 'airport_location': [], 'country_code': 'ES', 'country_name': '스페인'},
        {'airport_code': 'CDG', 'airport_name': '파리 샤를드골 국제공항', 'airport_location': [], 'country_code': 'FR', 'country_name': '프랑스'},
        {'airport_code': 'LHR', 'airport_name': '히스로 공항', 'airport_location': [], 'country_code': 'GB', 'country_name': '영국'},
        {'airport_code': 'FCO', 'airport_name': '로마 피우미치노 레오나르도 다 빈치 공항', 'airport_location': [], 'country_code': 'IT', 'country_name': '이탈리아'},
        {'airport_code': 'VIE', 'airport_name': '빈 국제공항', 'airport_location': [], 'country_code': 'AT', 'country_name': '오스트리아'},
        {'airport_code': 'AMS', 'airport_name': '암스테르담 스키폴 국제공항', 'airport_location': [], 'country_code': 'NL', 'country_name': '네덜란드'},
        {'airport_code': 'HNL', 'airport_name': '호놀룰루 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'SEA', 'airport_name': '시애틀 터코마 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'LAX', 'airport_name': '로스앤젤레스 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'ORD', 'airport_name': '시카고 오헤어 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'DFW', 'airport_name': '달라스 / 포트워스 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'JFK', 'airport_name': '존 F. 케네디 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'SYD', 'airport_name': '시드니 인터내셔널 에어포트', 'airport_location': [], 'country_code': 'AU', 'country_name': '호주'},
        {'airport_code': 'BNE', 'airport_name': '브리즈번 공항', 'airport_location': [], 'country_code': 'AU', 'country_name': '호주'},
        {'airport_code': 'YVR', 'airport_name': '밴쿠버 국제공항', 'airport_location': [], 'country_code': 'CA', 'country_name': '캐나다'},
        {'airport_code': 'YYZ', 'airport_name': '토론토 피어슨 국제공항', 'airport_location': [], 'country_code': 'CA', 'country_name': '캐나다'}
    ]

    for i, v in enumerate(airport_list):
        address = v['airport_name']
        lat, lng = get_lat_lng(address)
        airport_list[i]['airport_location'] = [lat, lng]

    return pd.DataFrame(airport_list)

# S3에 데이터를 업로드하는 함수
def upload_to_s3(df, filename):
    csv_filename = f'/tmp/{filename}.csv'

    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_bucket_name = Variable.get('s3_bucket_name')
    s3_result_key = f'source/source_flight/{filename}.csv'

    s3_hook.load_file(filename=csv_filename, key=s3_result_key, bucket_name=s3_bucket_name, replace=True)

# 전체 데이터를 S3에 저장하는 함수
def data_to_s3(macros):
    logging.info("Starting data_to_s3")
    
    try:
        df = fetch_airport_data()
        logging.info("finish df")

        upload_to_s3(df, "airport")
        logging.info("finish airport to s3")
    except Exception as e:
        logging.error(f"Error in data_to_s3: {e}")

        raise

# Redshift에 테이블을 생성하는 함수
def create_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS flight.airport;")
        redshift_conn.commit()
        logging.info("drop table")
        
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
        logging.info("create table")

        redshift_conn.close()
    except Exception as e:
        logging.error(f"Error in create_redshift_table: {e}")

        raise

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 9, tzinfo=kst),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'flight_airport',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)

# 태스크 정의
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

# 태스크 실행 순서 설정
data_to_s3_task >> create_redshift_table_task >> load_to_redshift_task