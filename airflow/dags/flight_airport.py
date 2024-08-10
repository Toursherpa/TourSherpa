from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import pandas as pd 
import boto3
from bs4 import BeautifulSoup
import requests
import re

# Airflow Connection에서 S3 연결 정보 가져오기
def get_s3_connection():
    connection = BaseHook.get_connection('s3_connection')
    return connection

# Google api 를 통해 공항 위도/경도 가져오기
def fetch_airport_data():

    # Google api 요청
    def get_lat_lng(address, api_key):
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": api_key
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                location = results[0]["geometry"]["location"]
                return location["lat"], location["lng"]
        return None

    api_key = "AIzaSyDjHHvstL_C732TlF3eH5giO8XhJrwaCIc"

    # 데이터 처리
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

        lat, lng = get_lat_lng(address, api_key)
        airport_list[i]['airport_location'] = [lat, lng]

    return pd.DataFrame(airport_list)

# S3에 파일 업로드
def upload_to_s3(data):
    s3_conn = get_s3_connection()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password
    )

    bucket_name = 'team-hori-2-bucket'
    s3_client.put_object(Body=data.to_csv(index=False), Bucket=bucket_name, Key="source/source_flight/flight_airport.csv")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='flight_airport',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_airport_data',
        python_callable=fetch_airport_data
    )

    upload_data_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=[fetch_data_task.output]
    )

    fetch_data_task >> upload_data_task