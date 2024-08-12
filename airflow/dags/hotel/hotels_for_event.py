from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import numpy as np

def calculate_distance_vectorized(lon1, lat1, lon2, lat2):
    return np.sqrt((lon2 - lon1) ** 2 + (lat2 - lat1) ** 2)

def parse_location_column(df, column_name):
    df[['longitude', 'latitude']] = df[column_name].str.strip('[]').str.split(', ', expand=True).astype(float)
    return df

def load_and_prepare_data(file_path, columns, entity_name):
    try:
        df = pd.read_csv(file_path, usecols=columns)
        df = parse_location_column(df, 'location')
        logging.info(f"{entity_name} 데이터를 성공적으로 로드하고 준비했습니다.")
        return df
    except Exception as e:
        logging.error(f"{entity_name} 데이터를 로드하는 중 오류가 발생했습니다: {e}")
        raise

def find_nearby_accommodations():
    logging.info("반경 10km 이내의 숙소 찾기 작업 시작...")

    hotels_df = load_and_prepare_data('/tmp/Updated_hotels.csv', ['hotel_id', 'location', 'rating', 'name'], '숙소')
    events_df = load_and_prepare_data('/tmp/TravelEvents.csv', ['id', 'title', 'location'], '이벤트')

    high_rating_hotels_df = hotels_df[hotels_df['rating'] >= 4]

    distances = calculate_distance_vectorized(
        events_df['longitude'].values[:, np.newaxis], events_df['latitude'].values[:, np.newaxis],
        hotels_df['longitude'].values, hotels_df['latitude'].values
    )

    within_radius = distances <= 0.1

    events_df['agoda_accommodation_ids'] = pd.Series(
        within_radius.dot(hotels_df['hotel_id'].astype(str) + ',')
    ).str.rstrip(',')

    high_rating_within_radius = distances[:, high_rating_hotels_df.index] <= 0.1
    events_df['high_rating_accommodation_names'] = pd.Series(
        high_rating_within_radius.dot(high_rating_hotels_df['name'].astype(str) + ',')
    ).str.rstrip(',')

    for _, event in events_df.iterrows():
        logging.info(f"이벤트 '{event['title']}' 근처의 숙소를 찾았습니다: Agoda ID - {event['agoda_accommodation_ids']}, Google 고평점 숙소 - {event['high_rating_accommodation_names']}")

    result_file_path = '/tmp/Updated_Events_with_hotels.csv'
    events_df[['id', 'title', 'agoda_accommodation_ids', 'high_rating_accommodation_names']].to_csv(result_file_path, index=False)
    logging.info(f"숙소 ID 및 평점 4 이상인 숙소 이름이 포함된 CSV 파일이 저장되었습니다: {result_file_path}")

def create_redshift_table_if_not_exists():
    logging.info("Redshift에 테이블이 없으면 생성합니다...")

    schema_name = 'hotel'
    table_name = 'hotels_for_event'
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        EventID VARCHAR(512) PRIMARY KEY,
        Title VARCHAR(1000),
        Agoda_Hotels VARCHAR(65535),
        Google_Place_Hotels VARCHAR(65535)
    );
    """

    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
        logging.info(f"테이블 '{schema_name}.{table_name}' 생성이 완료되었습니다.")
    except Exception as e:
        logging.error(f"테이블 생성 중 오류가 발생했습니다: {e}")
        raise

def upsert_data_to_redshift():
    logging.info("Redshift 테이블을 업데이트합니다...")

    table_name = 'hotel.hotels_for_event'
    try:
        hotel_df = pd.read_csv('/tmp/Updated_Events_with_hotels.csv', usecols=['id', 'title', 'agoda_accommodation_ids', 'high_rating_accommodation_names'])
        logging.info("CSV 파일을 성공적으로 로드했습니다.")

        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for _, row in hotel_df.iterrows():
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE EventID = %s", (row['id'],))
                    exists = cursor.fetchone()[0]

                    if exists:
                        cursor.execute(f"""
                            UPDATE {table_name}
                            SET Title = %s, Agoda_Hotels = %s, Google_Place_Hotels = %s
                            WHERE EventID = %s
                        """, (row['title'], row['agoda_accommodation_ids'], row['high_rating_accommodation_names'], row['id']))
                        logging.info(f"기존 레코드를 업데이트했습니다: EventID = {row['id']}")
                    else:
                        cursor.execute(f"""
                            INSERT INTO {table_name} (EventID, Title, Agoda_Hotels, Google_Place_Hotels)
                            VALUES (%s, %s, %s, %s)
                        """, (row['id'], row['title'], row['agoda_accommodation_ids'], row['high_rating_accommodation_names']))
                        logging.info(f"새 레코드를 삽입했습니다: EventID = {row['id']}")
                conn.commit()
        logging.info("Redshift 테이블 업데이트가 완료되었습니다.")
    except Exception as e:
        logging.error(f"테이블 업데이트 중 오류가 발생했습니다: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'events_with_hotels',
    default_args=default_args,
    description='이벤트 반경 10km 내 숙소 찾기 및 업데이트된 CSV 저장',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

find_hotels_task = PythonOperator(
    task_id='find_nearby_accommodations',
    python_callable=find_nearby_accommodations,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_redshift_table_if_not_exists',
    python_callable=create_redshift_table_if_not_exists,
    dag=dag,
)

upsert_data_task = PythonOperator(
    task_id='upsert_data_to_redshift',
    python_callable=upsert_data_to_redshift,
    dag=dag,
)

find_hotels_task >> create_table_task >> upsert_data_task
