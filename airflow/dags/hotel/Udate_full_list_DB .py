from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
import logging
import os

# 기본 DAG 설정

def update_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel.hotel_list'
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    # 파일 경로 설정
    file_path = f'/tmp/{datetime.utcnow().strftime("%Y-%m-%d")}/Updated_hotels.csv'

    # 파일이 존재하는지 확인
    try:
        hotel_df = pd.read_csv(file_path, usecols=[
        'hotel_id', 'chain_id', 'chain_name', 'hotel_name', 'city', 'star_rating', 
        'longitude', 'latitude', 'checkin', 'checkout', 'number_of_reviews', 
        'rating', 'name', 'user_ratings_total', 'place_id'
        ])

        hotel_df['hotel_id'] = hotel_df['hotel_id'].astype(str)
        insert_values = []
        for index, row in hotel_df.iterrows():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE event_id = %s", (row['hotel_id'],))
            count = cursor.fetchone()[0]
            if count == 0:
                insert_values.append((
                    row['hotel_id'], row['name'], row['rating'], row['user_ratings_total'], 
                    row['place_id'], row['hotel_id'], row['chain_id'], row['chain_name'], row['hotel_name'], 
                    row['city'], row['star_rating'], row['longitude'], row['latitude'], 
                    row['checkin'], row['checkout'], row['number_of_reviews']
                ))

        if insert_values:
            args_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in insert_values)
            cursor.execute(f"INSERT INTO {table_name} (event_id, google_name, google_rating, google_user_ratings_total, google_place_id, agoda_hotel_id, agoda_chain_id, agoda_chain_name, agoda_hotel_name, agoda_city, agoda_star_rating, agoda_longitude, agoda_latitude, agoda_checkin, agoda_checkout, google_number_of_reviews) VALUES {args_str}")

        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating table: {e}")
    finally:
        cursor.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'update_hotel_Redshift',
    default_args=default_args,
    description='A DAG to update Travel Events data and save it to Redshift',
    schedule_interval='@daily',
    catchup=False,
)

update_Redshift_task = PythonOperator(
    task_id='update_table',
    python_callable=update_table,
    provide_context=True,
    dag=dag,
)

update_Redshift_task