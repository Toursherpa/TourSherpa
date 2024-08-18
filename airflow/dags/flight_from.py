import logging
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pytz
import requests
import re
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from amadeus import Client, ResponseError
from bs4 import BeautifulSoup

# 한국 시간대 설정
kst = pytz.timezone('Asia/Seoul')

# S3에서 데이터를 읽어오는 함수
def read_csv_from_s3(bucket_name, file_key):
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    file_content = file_obj.get()['Body'].read().decode('utf-8')

    return pd.read_csv(StringIO(file_content))

# 공항별 항공권 날짜 구하는 함수
def fetch_flight_date(startdelta, enddelta, today):
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    bucket_name = Variable.get('s3_bucket_name')
    file_key = 'source/source_flight/airports.csv'
    df = read_csv_from_s3(bucket_name, file_key)
    airport_dict = {}
    min_date = datetime.strptime(today, '%Y-%m-%d')

    for _, row in df.iterrows():
        airport_code = row['airport_code']

        if str(row['start_date']) != 'nan':
            start_date = datetime.strptime(str(row['start_date']), '%Y-%m-%d') + timedelta(days=startdelta)
            end_date = datetime.strptime(str(row['end_date']), '%Y-%m-%d') + timedelta(days=enddelta)

            if start_date < min_date:
                start_date = min_date
        else:
            continue

        delta = end_date - start_date

        if airport_code not in airport_dict:
            airport_dict[airport_code] = set()

        for i in range(delta.days + 1):
            date = start_date + timedelta(days=i)
            airport_dict[airport_code].add(date.strftime('%Y-%m-%d'))

    return airport_dict

# 항공사 데이터 가져오는 함수
def fetch_airline_data():
    url = "https://www.airport.kr/ap/ko/dep/apAirlinesList.do"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
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

# 유로 환율 가져오는 함수
def euro_data():
    url = "https://m.stock.naver.com/marketindex/exchange/FX_EURKRW"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    euro = float(soup.find_all("strong")[1].text[: -3].replace(",", ""))

    return euro

# 항공권 데이터 가져오는 함수
def fetch_flight_data(airport_dict, airline_df, euro, check):
    amadeus = Client(
        client_id = Variable.get("amadeus_id"),
        client_secret = Variable.get("amadeus_secret")
    )
    response_list = []
    count = 0

    for i in airport_dict:
        date_list = sorted(list(airport_dict[i]))

        for j in date_list:
            count += 1

            if check == 0:
                location = ['ICN', i]
            else:
                location = [i, 'ICN']

            while True:
                try:
                    response = amadeus.shopping.flight_offers_search.get(
                        originLocationCode=location[0],
                        destinationLocationCode=location[1],
                        departureDate=j,
                        adults=1,
                        nonStop='true'
                    )
                    logging.info(f'[{count}]')

                    if response.data:
                        response_list.append(response.data)

                    time.sleep(1)

                    break
                except ResponseError as error:
                    print(error)

                    time.sleep(2)

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
            
            flight_list.append(info_dict)

    flight_df = pd.DataFrame(flight_list)
    df = pd.merge(flight_df, airline_df, on='airline_code', how='inner')

    def date_change(value):
        date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return date.strftime('%y%m%d')

    def min_change(value):
        date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return int(date.strftime('%H')) * 60 + int(date.strftime('%M'))

    departure_date = []
    departure_min = []
    price_won = []

    for i in range(len(df)):
        departure_date.append(date_change(df['departure_at'][i]))
        departure_min.append(min_change(df['departure_at'][i]))
        price_won.append(int(float(df['price'][i]) * euro))

    df['departure_date'] = departure_date
    df['departure_min'] = departure_min
    df['price'] = price_won

    return df

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
    today = (macros.datetime.now().astimezone(kst)).strftime('%Y-%m-%d')
    logging.info(f"Starting data_to_s3 {today}")
    
    try:
        airport_dict = fetch_flight_date(0, 4, today)
        logging.info("finish airport_dict")

        airline_df = fetch_airline_data()
        logging.info("finish airline_df")

        euro = euro_data()
        logging.info("finish euro")

        df = fetch_flight_data(airport_dict, airline_df, euro, 1)
        logging.info("finish df")

        upload_to_s3(df, "flight_from")
        logging.info("finish flight_from to s3")
    except Exception as e:
        logging.error(f"Error in data_to_s3: {e}")

        raise

# Redshift에 테이블을 생성하는 함수
def create_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS flight.flight_from;")
        redshift_conn.commit()
        logging.info("drop table")
        
        cursor.execute("""
            CREATE TABLE flight.flight_from (
                airline_code VARCHAR(255),
                departure VARCHAR(255),
                departure_at VARCHAR(255),
                arrival VARCHAR(255),
                arrival_at VARCHAR(255),
                duration VARCHAR(255),
                seats INTEGER,
                price INTEGER,
                airline_name VARCHAR(255),
                departure_date INTEGER,
                departure_min INTEGER
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
    'flight_from',
    default_args=default_args,
    schedule_interval=None,
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
    table='flight_from',
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_flight/flight_from.csv',
    copy_options=['IGNOREHEADER 1', 'CSV'],
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
)

# 태스크 실행 순서 설정
data_to_s3_task >> create_redshift_table_task >> load_to_redshift_task