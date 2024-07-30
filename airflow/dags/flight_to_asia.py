from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from amadeus import Client, ResponseError
import pandas as pd 
import boto3
import time
from datetime import datetime, timedelta

# Airflow Connection에서 S3 연결 정보 가져오기
def get_s3_connection():
    connection = BaseHook.get_connection('s3_connection')
    return connection

# Amadeus API를 통해 항공편 정보 가져오기
def fetch_flight_data():
    amadeus = Client(
        client_id = "bNPlTPOBKmXuY8b3FfUeRPGG7swBNGuV",
        client_secret = "46D2WeXiicgBgVdj"
    )

    # 항공편 데이터 요청
    response_list = []
    airport_list = ["CGK", "DEL", "TPE", "KHH", "KUL", "BKI"]
    date_list = []

    today = datetime.today()

    for i in range(100):
        date = today + timedelta(days=i)
        date_list.append(date.strftime('%Y-%m-%d'))

    for i in airport_list:
        for j in date_list:
            try:
                response = amadeus.shopping.flight_offers_search.get(
                    originLocationCode='ICN',
                    destinationLocationCode=i,
                    departureDate=j,
                    adults=1,
                    nonStop='true'
                )
            
                response_list.append(response.data)

                print("==========================================")
                if response.data:
                    response_list.append(response.data)

                    print(response.data[0]['itineraries'][0]['segments'][0]['arrival']['iataCode'])
                    print(response.data[0]['itineraries'][0]['segments'][0]['departure']['at'])
                else:
                    print(i)
                    print("비행편 없음")

                time.sleep(5)
            except ResponseError as error:
                print(error)

                return 0

    # 데이터 처리
    airport_info = {"CGK": ["수카르노 하타 국제공항", "ID", "인도네시아"], "DEL": ["인디라 간디 국제공항", "IN", "인도"], "TPE": ["타이완 타오위안 국제공항", "TW", "대만"], "KHH": ["가오슝 국제공항", "TW", "대만"], "KUL": ["쿠알라룸푸르 국제공항", "MY", "말레이시아"], "BKI": ["코타키나발루 국제공항", "MY", "말레이시아"]}

    flight_list = []

    for i in response_list:
        for j in i:
            info_dict = dict()
        
            info_dict['airline_code'] = j['itineraries'][0]['segments'][0]['carrierCode']
            info_dict['departure'] = j['itineraries'][0]['segments'][0]['departure']['iataCode']
            info_dict['departure_at'] = j['itineraries'][0]['segments'][0]['departure']['at']
            info_dict['arrival'] = j['itineraries'][0]['segments'][0]['arrival']['iataCode']
            info_dict['arrival_at'] = j['itineraries'][0]['segments'][0]['arrival']['at']
            info_dict['duration'] = j['itineraries'][0]['segments'][0]['duration'][2: ].replace("H", "시간 ").replace("M", "분")
            info_dict['seats'] = j['numberOfBookableSeats']
            info_dict['price'] = j['price']['total']
            info_dict['airport_name'] = airport_info[info_dict['arrival']][0]
            info_dict['country_code'] = airport_info[info_dict['arrival']][1]
            info_dict['country_name'] = airport_info[info_dict['arrival']][2]
        
            flight_list.append(info_dict)

    return pd.DataFrame(flight_list)

# S3에 파일 업로드
def upload_to_s3(data):
    s3_conn = get_s3_connection()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password
    )

    bucket_name = 'team-hori-2-bucket'
    s3_client.put_object(Body=data.to_csv(), Bucket=bucket_name, Key="source/source_flight/flight_to_asia.csv")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='flight_to_asia',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_flight_data',
        python_callable=fetch_flight_data
    )

    upload_data_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=[fetch_data_task.output]
    )

    fetch_data_task >> upload_data_task