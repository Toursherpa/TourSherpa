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

# 인천국제공항 웹사이트를 통해 항공사 정보 가져오기
def fetch_airline_data():

    # 항공사 데이터 요청
    url = "https://www.airport.kr/ap/ko/dep/apAirlinesList.do"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    # 데이터 처리
    airline_list = []

    for i in soup.find_all("tr")[16: ]:
        info_dict = dict()

        info_dict['airline_code'] = re.sub(r'[\n\r\t ]', '', i.find_all("td")[4].text)
        info_dict['airline_name'] = re.sub(r'[\n\r\t ]', '', i.find_all("td")[0].text)

        airline_list.append(info_dict)

    airline_list.append({'airline_code': 'H1', 'airline_name': '한에어'})
    airline_list.append({'airline_code': '6X', 'airline_name': '에어오디샤'})
    airline_list.append({'airline_code': 'UX', 'airline_name': '에어유로파'})
    airline_list.append({'airline_code': 'AZ', 'airline_name': '알리탈리아'})
    airline_list.append({'airline_code': 'VS', 'airline_name': '버진애틀랜틱항공'})

    return pd.DataFrame(airline_list)

# S3에 파일 업로드
def upload_to_s3(data):
    s3_conn = get_s3_connection()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password
    )

    bucket_name = 'team-hori-2-bucket'
    s3_client.put_object(Body=data.to_csv(index=False), Bucket=bucket_name, Key="source/source_flight/flight_airline.csv")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='flight_airline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_airline_data',
        python_callable=fetch_airline_data
    )

    upload_data_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=[fetch_data_task.output]
    )

    fetch_data_task >> upload_data_task