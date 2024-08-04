from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd 
import boto3
from io import StringIO
from bs4 import BeautifulSoup
import requests

# Airflow Connection에서 S3 연결 정보 가져오기
def get_s3_connection():
    connection = BaseHook.get_connection('s3_connection')

    return connection

# Airflow Connection에서 Redshift 연결 정보 가져오기
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_connection')

    return hook.get_conn()

# 통합 csv 파일 만들기
def merge_csv_from_s3():
    s3_conn = get_s3_connection()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password
    )

    bucket_name = 'team-hori-2-bucket'
    file_keys = [
        'source/source_flight/flight_to_japan.csv',
        'source/source_flight/flight_to_china.csv',
        'source/source_flight/flight_to_asia.csv',
        'source/source_flight/flight_to_europe.csv',
        'source/source_flight/flight_to_usa.csv',
        'source/source_flight/flight_to_other.csv'
    ]

    # S3에서 csv 파일 읽기
    def read_csv_from_s3(bucket_name, file_key):
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')

        return pd.read_csv(StringIO(csv_content))

    dataframes = [read_csv_from_s3(bucket_name, key) for key in file_keys]

    df = pd.concat(dataframes, ignore_index=True)

    url = "https://m.stock.naver.com/marketindex/exchange/FX_EURKRW"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    er = float(soup.find_all("strong")[1].text[: -3].replace(",", ""))

    df['price'] = df['price'] * er

    pd.options.display.float_format = '{:.2f}'.format

    return df

# Redshift에 파일 업로드
def upload_to_redshift(data):
    conn = get_redshift_connection()
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE flight.flight_to (
        airline_code VARCHAR(5),
        departure VARCHAR(5),
        departure_at VARCHAR(25),
        arrival VARCHAR(5),
        arrival_at VARCHAR(25),
        duration VARCHAR(15),
        seats INTEGER,
        price NUMERIC(9, 2)
    );
    """

    cur.execute("drop table if exists flight.flight_to")
    conn.commit()

    cur.execute(create_table_query)
    conn.commit()

    for i, r in data.iterrows():
        insert_query = "INSERT INTO flight.flight_to VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"

        cur.execute(insert_query, tuple(r))
        conn.commit()

        print(tuple(r))

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='redshift_flight_to',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data_task = PythonOperator(
        task_id='merge_csv_from_s3',
        python_callable=merge_csv_from_s3
    )

    upload_data_task = PythonOperator(
        task_id='upload_to_redshift',
        python_callable=upload_to_redshift,
        op_args=[fetch_data_task.output]
    )

    fetch_data_task >> upload_data_task