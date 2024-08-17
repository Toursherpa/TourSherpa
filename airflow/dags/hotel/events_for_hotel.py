from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
import logging

today_date = datetime.utcnow().strftime('%Y-%m-%d')

def calculate_distance(lon1, lat1, lon2, lat2):
    return np.sqrt((lon2 - lon1) ** 2 + (lat2 - lat1) ** 2)

def find_nearby_events():
    logging.info("10km 반경 내의 이벤트를 찾기 시작합니다...")

    hotel_file_path = '/tmp/Updated_hotels.csv'
    event_file_path = '/tmp/TravelEvents.csv'
    
    logging.info(f"호텔 데이터를 로드합니다: {hotel_file_path}")
    hotels_df = pd.read_csv(hotel_file_path, usecols=['name', 'place_id', 'location'])
    events_df = pd.read_csv(event_file_path)

    logging.info("호텔 및 이벤트 데이터의 위치 정보를 파싱합니다...")
    hotels_df[['longitude', 'latitude']] = hotels_df['location'].str.strip('[]').str.split(', ', expand=True).astype(float)
    events_df[['longitude', 'latitude']] = events_df['location'].str.strip('[]').str.split(', ', expand=True).astype(float)

    logging.info("호텔과 이벤트 간의 거리를 계산합니다...")
    distances = calculate_distance(
        hotels_df['longitude'].values[:, np.newaxis], hotels_df['latitude'].values[:, np.newaxis],
        events_df['longitude'].values, events_df['latitude'].values
    )

    logging.info("각 호텔에 대해 10km 반경 내의 이벤트를 식별합니다...")
    within_radius = distances <= 0.1

    hotels_df['event_ids'] = [
        ','.join(events_df.loc[within_radius[i], 'id'].astype(str).values)
        for i in range(len(hotels_df))
    ]

    output_dir = f'/tmp/{today_date}'
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = f'{output_dir}/Updated_hotels_with_Events.csv'
    hotels_df.drop(columns=['longitude', 'latitude'], inplace=True)
    hotels_df.to_csv(output_file_path, index=False)
    logging.info(f"이벤트 ID가 포함된 업데이트된 CSV 파일이 저장되었습니다: {output_file_path}")

    upload_to_s3(output_file_path, 'team-hori-2-bucket', f'source/source_TravelEvents/{today_date}/Updated_hotels_with_Events.csv')

def upload_to_s3(file_path, bucket_name, s3_key):
    logging.info(f"파일을 S3에 업로드합니다: {file_path} -> s3://{bucket_name}/{s3_key}")
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    logging.info(f"S3에 파일 업로드 완료: s3://{bucket_name}/{s3_key}")

def create_redshift_table_if_not_exists():
    logging.info("Redshift에 테이블이 없으면 생성합니다...")
    
    redshift_conn_id = 'redshift_connection'
    schema_name = 'hotel'
    table_name = 'events_for_hotel'
    
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        Google_Place_Id VARCHAR(512) PRIMARY KEY,
        HOTELNAME VARCHAR(512),
        EventID VARCHAR(65535)
    );
    """
    
    with redshift_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
    
    logging.info(f"테이블이 존재하지 않을 경우 생성 완료: {schema_name}.{table_name}")

def update_redshift_table():
    logging.info("Redshift 테이블을 업데이트합니다...")

    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel.events_for_hotel'
    
    updated_hotels_path = f'/tmp/{today_date}/Updated_hotels_with_Events.csv'
    hotel_df = pd.read_csv(updated_hotels_path, usecols=['place_id', 'name', 'event_ids'])
    hotel_df['place_id'] = hotel_df['place_id'].astype(str)

    with PostgresHook(postgres_conn_id=redshift_conn_id).get_conn() as conn:
        with conn.cursor() as cursor:
            for index, row in hotel_df.iterrows():
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE HOTELNAME = %s", (row['name'],))
                exists = cursor.fetchone()[0]
                
                if exists:
                    cursor.execute(f"""
                        UPDATE {table_name}
                        SET EventID = %s, Google_Place_Id = %s
                        WHERE HOTELNAME = %s
                    """, (row['event_ids'], row['place_id'], row['name']))
                    logging.info(f"기존 레코드를 업데이트했습니다: {row['name']}")
                else:
                    cursor.execute(f"""
                        INSERT INTO {table_name} (EventID, HOTELNAME, Google_Place_Id)
                        VALUES (%s, %s, %s)
                    """, (row['event_ids'], row['name'], row['place_id']))
                    logging.info(f"새 레코드를 삽입했습니다: {row['name']}")
            conn.commit()

    logging.info("Redshift 테이블 업데이트가 완료되었습니다.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hotels_with_events',
    default_args=default_args,
    description='호텔 주변 10km 반경 내의 이벤트를 찾아 CSV 및 Redshift 업데이트',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

find_events_task = PythonOperator(
    task_id='find_nearby_events',
    python_callable=find_nearby_events,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_redshift_table_if_not_exists',
    python_callable=create_redshift_table_if_not_exists,
    dag=dag,
)

update_table_task = PythonOperator(
    task_id='update_redshift_table',
    python_callable=update_redshift_table,
    dag=dag,
)

find_events_task >> create_table_task >> update_table_task