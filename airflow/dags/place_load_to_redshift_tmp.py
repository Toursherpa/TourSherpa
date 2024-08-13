import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable



def preprocess_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()
        
        # 기존 테이블 삭제 및 테이블 생성
        cursor.execute("CREATE SCHEMA IF NOT EXISTS place;")
        cursor.execute(f"DROP TABLE IF EXISTS place.events_places;")
        cursor.execute(f"""
            CREATE TABLE place.events_places (
                "Event_ID" VARCHAR(256),
                "Event_Title" VARCHAR(256),
                "Location" VARCHAR(256),
                "Place_Name" VARCHAR(256),
                "Address" VARCHAR(256),
                "Rating" FLOAT,
                "Number_of_Reviews" FLOAT,
                "Review" VARCHAR(65535),
                "Latitude" FLOAT,
                "Longitude" FLOAT,
                "Types" VARCHAR(256),
                "Opening_Hours" VARCHAR(65535),
                "Collection_Date" DATE
            );
        """)
        redshift_conn.commit()
        logging.info(f"Redshift table public.events_places has been dropped and recreated.")
        
    except Exception as e:
        logging.error(f"Error in preprocess_redshift_table: {e}")
        raise


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1) + timedelta(hours=5),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'place_load_to_redshift',
    default_args=default_args,
    description='Load data from S3 to Redshift and log results',
    schedule_interval='@daily',
    catchup=False,
)

preprocess_redshift_task = PythonOperator(
    task_id='preprocess_redshift_table',
    python_callable=preprocess_redshift_table,
    provide_context=True,
    dag=dag,
)

load_to_redshift_task = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    schema='place',
    table='events_places',
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_place/place_cafe_restaurant.csv',
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
    method = "REPLACE",
    copy_options=[
    "IGNOREHEADER 1",
    "csv",
    "NULL AS 'None'",
    "BLANKSASNULL",
    "EMPTYASNULL",
    "FILLRECORD"
]
)




preprocess_redshift_task >> load_to_redshift_task 
