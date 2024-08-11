from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from flight_DAG import *

kst = pytz.timezone('Asia/Seoul')

def data_to_s3(macros):
    logging.info("Starting data_to_s3")
    
    try:
        df = fetch_airport_data()
        logging.info("finish df")

        upload_to_s3(df, "airport")
        logging.info("finish airport to s3")
    except Exception as e:
        logging.error(f"Error in data_to_s3: {e}")
        raise

def create_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS flight.airport;")
        redshift_conn.commit()
        logging.info("drop table")
        
        cursor.execute("""
            CREATE TABLE flight.airport (
                airport_code VARCHAR(255),
                airport_name VARCHAR(255),
                airport_location VARCHAR(255),
                country_code VARCHAR(255),
                country_name VARCHAR(255)
            );
        """)
        redshift_conn.commit()
        logging.info("create table")

        redshift_conn.close()
    except Exception as e:
        logging.error(f"Error in create_redshift_table: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 9, tzinfo=kst),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_airport',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)

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
    table='airport',
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_flight/airport.csv',
    copy_options=['IGNOREHEADER 1', 'CSV'],
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
)

data_to_s3_task >> create_redshift_table_task >> load_to_redshift_task