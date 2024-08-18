from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
import logging

def create_schema_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel_list'
    schema_name = 'hotel'
    
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    drop_table = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        event_id VARCHAR(512) PRIMARY KEY,  -- Increased length
        google_name VARCHAR(1000),
        google_rating FLOAT,
        google_user_ratings_total INT,
        google_place_id VARCHAR(512),
        agoda_hotel_id VARCHAR(512),
        agoda_chain_id VARCHAR(512),
        agoda_chain_name VARCHAR(1000),
        agoda_hotel_name VARCHAR(1000),
        agoda_city VARCHAR(512),
        agoda_star_rating FLOAT,
        agoda_longitude FLOAT,
        agoda_latitude FLOAT,
        agoda_checkin VARCHAR(100),
        agoda_checkout VARCHAR(100),
        google_number_of_reviews INT
    );
    """
    cursor.execute(drop_table)
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

def update_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel.hotel_list'
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    hotel_df = pd.read_csv('/tmp/Updated_hotels.csv', usecols=[
        'hotel_id', 'chain_id', 'chain_name', 'hotel_name', 'city', 'star_rating', 
        'longitude', 'latitude', 'checkin', 'checkout', 'number_of_reviews', 
        'rating', 'name', 'user_ratings_total', 'place_id'
    ])

    hotel_df['hotel_id'] = hotel_df['hotel_id'].astype(str)

    try:
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
    'upload_hotel_Redshift',
    default_args=default_args,
    description='A DAG to update Travel Events data and save it to Redshift',
    schedule_interval='@daily',
    catchup=False,
)

upload_to_Redshift_task = PythonOperator(
    task_id='create_schema_table',
    python_callable=create_schema_table,
    provide_context=True,
    dag=dag,
)

update_Redshift_task = PythonOperator(
    task_id='update_table',
    python_callable=update_table,
    provide_context=True,
    dag=dag,
)

upload_to_Redshift_task >> update_Redshift_task