from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from flight_DAG import *

kst = pytz.timezone('Asia/Seoul')

def data_to_s3(macros):
    today = (macros.datetime.now().astimezone(kst)).strftime('%Y-%m-%d')
    logging.info(f"Starting data_to_s3 {today}")
    
    try:
        airport_dict = fetch_flight_date(-3, 1, today)
        logging.info(f"finish airport_dict")

        airline_df = fetch_airline_data()
        logging.info(f"finish airline_df")

        euro = euro_data()
        logging.info(f"finish euro")

        df = fetch_flight_data(airport_dict, airline_df, euro, 0)
        logging.info(f"finish df")

        upload_to_s3(df, "flight_japan_to")
        logging.info(f"finish flight_to to s3")
    except Exception as e:
        logging.error(f"Error in data_to_s3: {e}")
        raise

def create_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS flight.flight_japan_to;")
        redshift_conn.commit()
        logging.info("drop table")
        
        cursor.execute("""
            CREATE TABLE flight.flight_japan_to (
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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 9, tzinfo=kst),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_to,
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
    table='flight_japan_to',
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_flight/flight_japan_to.csv',
    copy_options=['IGNOREHEADER 1', 'CSV'],
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
)

data_to_s3_task >> create_redshift_table_task >> load_to_redshift_task