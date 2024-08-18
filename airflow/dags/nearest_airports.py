import logging
import pandas as pd
from geopy.distance import geodesic
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta, datetime
from airflow.models import Variable
from io import StringIO
import pytz

# 한국 시간대 설정
kst = pytz.timezone('Asia/Seoul')

# S3에서 데이터를 읽어오는 함수
def read_data_from_s3(macros, **context):
    today = (macros.datetime.now().astimezone(kst)).strftime('%Y-%m-%d')
    logging.info(f"Starting read_data_from_s3 {today}")

    try:
        s3_hook = S3Hook(aws_conn_id='s3_connection')
        bucket_name = Variable.get('s3_bucket_name')

        events_key = f'source/source_TravelEvents/{today}/TravelEvents.csv'
        airports_key = 'source/source_flight/flight_airport.csv'

        events_file_obj = s3_hook.get_key(key=events_key, bucket_name=bucket_name)
        airports_file_obj = s3_hook.get_key(key=airports_key, bucket_name=bucket_name)

        events_file_content = events_file_obj.get()['Body'].read().decode('utf-8')
        airports_file_content = airports_file_obj.get()['Body'].read().decode('utf-8')

        event_df = pd.read_csv(StringIO(events_file_content))
        airport_df = pd.read_csv(StringIO(airports_file_content))

        event_df[['lon', 'lat']] = pd.DataFrame(event_df['location'].apply(lambda x: eval(x)).tolist(), index=event_df.index)
        airport_df[['airport_lat', 'airport_lon']] = pd.DataFrame(airport_df['airport_location'].apply(lambda x: eval(x)).tolist(), index=airport_df.index)

        context['ti'].xcom_push(key='event_df', value=event_df.to_dict(orient='records'))
        context['ti'].xcom_push(key='airport_df', value=airport_df.to_dict(orient='records'))
        logging.info("Events and Airports data has been read from S3 and pushed to XCom.")

    except Exception as e:
        logging.error(f"Error in read_data_from_s3: {e}")

        raise

# 가장 가까운 공항을 찾는 함수
def find_nearest_airports(**context):
    try:
        event_df = pd.DataFrame(context['ti'].xcom_pull(key='event_df'))
        airport_df = pd.DataFrame(context['ti'].xcom_pull(key='airport_df'))

        def find_nearest_airport(event_lat, event_lon, event_country, airport_df):
            min_distance = float('inf')
            nearest_airport = None
            country_airports = airport_df[airport_df['country_code'] == event_country]

            for _, airport in country_airports.iterrows():
                airport_lat = airport['airport_lat']
                airport_lon = airport['airport_lon']
                distance = geodesic((event_lat, event_lon), (airport_lat, airport_lon)).kilometers

                if distance < min_distance:
                    min_distance = distance
                    nearest_airport = airport

            return nearest_airport

        results = []

        for _, event in event_df.iterrows():
            nearest_airport = find_nearest_airport(event['lat'], event['lon'], event['country'], airport_df)

            if nearest_airport is not None:
                results.append({
                    'id': event['id'],
                    'title': event['title'],
                    'country': event['country'],
                    'start_date': str(event['start_local'])[: 10],
                    'end_date': str(event['end_local'])[: 10],
                    'airport_code': nearest_airport['airport_code'],
                    'airport_name': nearest_airport['airport_name']
                })

        result_df = pd.DataFrame(results)
        csv_filename = '/tmp/nearest_airports.csv'

        result_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

        s3_hook = S3Hook('s3_connection')
        s3_result_key = 'source/source_flight/nearest_airports.csv'
        s3_bucket_name = Variable.get('s3_bucket_name')
        s3_hook.load_file(filename=csv_filename, key=s3_result_key, bucket_name=s3_bucket_name, replace=True)
        logging.info("Nearest airports data has been calculated and uploaded to S3.")

    except Exception as e:
        logging.error(f"Error in find_nearest_airports: {e}")

        raise

# Redshift에 테이블을 생성하는 함수
def preprocess_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS public.nearest_airports;")
        redshift_conn.commit()

        cursor.execute("""
            CREATE TABLE public.nearest_airports (
                id VARCHAR(255),
                title VARCHAR(255),
                country VARCHAR(255),
                start_date VARCHAR(255),
                end_date VARCHAR(255),
                airport_code VARCHAR(10),
                airport_name VARCHAR(255)
            );
        """)
        redshift_conn.commit()
        redshift_conn.close()
        logging.info(f"Redshift table nearest_airports has been dropped and recreated.")

    except Exception as e:
        logging.error(f"Error in preprocess_redshift_table: {e}")

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
    'nearest_airports_dag',
    default_args=default_args,
    description='Find nearest airports for events, save to S3 and Redshift',
    schedule_interval=None,
    catchup=False,
)

# 태스크 정의
read_data_from_s3_task = PythonOperator(
    task_id='read_data_from_s3',
    python_callable=read_data_from_s3,
    provide_context=True,
    dag=dag,
)

find_nearest_airports_task = PythonOperator(
    task_id='find_nearest_airports',
    python_callable=find_nearest_airports,
    provide_context=True,
    dag=dag,
)

preprocess_redshift_task = PythonOperator(
    task_id='preprocess_redshift_table',
    python_callable=preprocess_redshift_table,
    provide_context=True,
    dag=dag,
)

load_to_redshift_task = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    schema='public',
    table='nearest_airports',
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_flight/nearest_airports.csv',
    copy_options=['IGNOREHEADER 1', 'CSV'],
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
)

trigger_second_dag = TriggerDagRunOperator(
    task_id='trigger_second_dag',
    trigger_dag_id='flight_to',  # The ID of the second DAG
    dag=dag,
)

trigger_third_dag = TriggerDagRunOperator(
    task_id='trigger_third_dag',
    trigger_dag_id='flight_from',  # The ID of the third DAG
    dag=dag,
)

# 태스크 실행 순서 설정
read_data_from_s3_task >> find_nearest_airports_task >> preprocess_redshift_task >> load_to_redshift_task >> trigger_second_dag >> trigger_third_dag