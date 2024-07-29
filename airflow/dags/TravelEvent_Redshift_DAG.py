from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
from io import StringIO
import pytz
from airflow.hooks.postgres_hook import PostgresHook
import ast
import re

countrys = ['AT', 'AU', 'BR', 'CA', 'CN', 'DE', 'ES', 'FR', 'GB', 'ID', 'IN', 'IT', 'JP', 'MY', 'NL', 'TW', 'US']

kst = pytz.timezone('Asia/Seoul')
utc_now = datetime.utcnow()
kst_now = utc_now.astimezone(kst)
today = kst_now.strftime('%Y-%m-%d')


def read_data_from_s3(**kwargs):
    s3_hook = S3Hook('TravelEvent_s3_conn')
    s3_bucket_name = Variable.get('my_s3_bucket')

    s3_key = f'source/source_TravelEvents/TravelEvents.csv'
    if s3_hook.check_for_key(key=s3_key, bucket_name=s3_bucket_name):
        file_obj = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket_name)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        transformed_data = {'file_content': file_content}  # 사전 형태로 초기화
    else:
        print("파일을 찾을 수 없습니다. 건너뜁니다.")
        transformed_data = None

    kwargs['ti'].xcom_push(key='s3_data', value=transformed_data)


def extract_formatted_address(data):
    # 문자열의 작은따옴표를 큰따옴표로 변환하여 JSON 형식에 맞게 통일
    data = str(data).replace("'", '"')

    match = re.search(r'"formatted_address"\s*:\s*["\'](.*?)(?<!\\)["\'],', data)

    if match:
        # 주소를 반환
        return match.group(1).replace('\\"', '"')
    else:
        return "상세주소는 아직 미정입니다."


def transform_data(**kwargs):
    s3_data = kwargs['ti'].xcom_pull(key='s3_data', task_ids='read_data_from_s3')

    if s3_data and 'file_content' in s3_data:
        file_content = s3_data['file_content']
        df = pd.read_csv(StringIO(file_content))
        # 필요한 컬럼만 선택하고 이름 변경
        df = df[['id', 'title', 'description', 'category', 'rank', 'phq_attendance', 'start_local', 'end_local', 'location', 'geo', 'country', 'predicted_event_spend']]
        df.columns = ['EventID', 'Title', 'Description', 'Category', 'Rank', 'PhqAttendance', 'TimeStart', 'TimeEnd', 'LocationID', 'Address', 'Country', 'PredictedEventSpend']


        df['Address'] = df['Address'].apply(lambda x: extract_formatted_address(x))
        transformed_data = df.to_dict(orient='records')
    else:
        print("데이터가 올바르게 준비되지 않았습니다.")

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)


def generate_and_save_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    redshift_conn_id = 'my_redshift_connection_id'
    redshift_table = 'travel_events'

    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    if transformed_data:
        redshift_hook.insert_rows(table=redshift_table, rows=transformed_data, replace=True, columns=[
            'EventID', 'Title', 'Description', 'Category', 'Rank', 'PhqAttendance', 'TimeStart', 'TimeEnd', 'LocationID', 'Address', 'Country', 'PredictedEventSpend'
        ])


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'update_TravelEvents_Dags_to_Redshift',
    default_args=default_args,
    description='A DAG to update Travel Events data and save it to Redshift',
    schedule_interval='*/5 * * * *',  # 필요에 따라 조정
    catchup=False,
)

read_data_from_s3_task = PythonOperator(
    task_id='read_data_from_s3',
    python_callable=read_data_from_s3,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

upload_to_Redshift_task = PythonOperator(
    task_id='upload_TravelEvents_data_to_Redshift',
    python_callable=generate_and_save_data,
    provide_context=True,
    dag=dag,
)

read_data_from_s3_task >> transform_data_task >> upload_to_Redshift_task
