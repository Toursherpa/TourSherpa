from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from amadeus import Client, ResponseError
import pandas as pd 
import boto3
from datetime import datetime

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
    response = amadeus.shopping.flight_offers_search.get(
        originLocationCode='ICN',
        destinationLocationCode='KIX',
        departureDate='2024-11-14',
        adults=1
    )

    # 데이터 처리
    flight_list = []

    for i in response.data:
        info_dict = dict()

        info_dict['name'] = i['itineraries'][0]['segments'][0]['carrierCode'] + i['itineraries'][0]['segments'][0]['number']
        info_dict['departure'] = i['itineraries'][0]['segments'][0]['departure']['iataCode']
        info_dict['departure_at'] = i['itineraries'][0]['segments'][0]['departure']['at'][11: 16]
        info_dict['arrival'] = i['itineraries'][0]['segments'][0]['arrival']['iataCode']
        info_dict['arrival_at'] = i['itineraries'][0]['segments'][0]['arrival']['at'][11: 16]
        info_dict['duration'] = i['itineraries'][0]['segments'][0]['duration'][2: ].replace("H", "시간 ").replace("M", "분")
        info_dict['seats'] = i['numberOfBookableSeats']
        info_dict['price'] = i['price']['total']
        info_dict['checked_bag'] = i['pricingOptions']['includedCheckedBagsOnly']

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
    s3_client.put_object(Body=data.to_csv(), Bucket=bucket_name, Key="flight/test_flight.csv")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='test_flight',
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