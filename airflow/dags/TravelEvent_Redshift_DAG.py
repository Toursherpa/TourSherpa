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
from googletrans import Translator
import time
import ast
import re

countrys = ['AT', 'AU', 'CA', 'CN', 'DE', 'ES', 'FR', 'GB', 'ID', 'IN', 'IT', 'JP', 'MY', 'NL', 'TW', 'US']

kst = pytz.timezone('Asia/Seoul')
utc_now = datetime.utcnow()
kst_now = utc_now.astimezone(kst)
today = kst_now.strftime('%Y-%m-%d')


def read_data_from_s3(**kwargs):
    s3_hook = S3Hook('s3_connection')
    s3_bucket_name = Variable.get('s3_bucket_name')

    s3_key = f'source/source_TravelEvents/{today}/TravelEvents.csv'
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

def extract_formatted_region(data):
    # 문자열의 작은따옴표를 큰따옴표로 변환하여 JSON 형식에 맞게 통일
    data = str(data).replace("'", '"')

    match = re.search(r'"region"\s*:\s*["\'](.*?)(?<!\\)["\']\}\}', data)

    if match:
        # 주소를 반환
        return match.group(1).replace('\\"', '"')
    else:
        return "none"

def transform_data(**kwargs):
    s3_data = kwargs['ti'].xcom_pull(key='s3_data', task_ids='read_data_from_s3')

    if s3_data and 'file_content' in s3_data:
        file_content = s3_data['file_content']
        df = pd.read_csv(StringIO(file_content))
        # 필요한 컬럼만 선택하고 이름 변경
        df = df[['id', 'title', 'description', 'category', 'rank', 'phq_attendance', 'start_local', 'end_local', 'location', 'geo', 'geo', 'country', 'predicted_event_spend']]
        df.columns = ['EventID', 'Title', 'Description', 'Category', 'Rank', 'PhqAttendance', 'TimeStart', 'TimeEnd', 'LocationID', 'Address', 'Region', 'Country', 'PredictedEventSpend']
        category_mapping = {
            'sports': '스포츠',
            'festivals': '축제',
            'expos': '박람회',
            'concerts': '콘서트'
        }

        df['Category'] = df['Category'].map(category_mapping).fillna(df['Category'])
        df['Description'] = df['Description'].apply(lambda x: remove_source_info(x))
        df['Region'] = df['Region'].apply(lambda x: extract_formatted_region(x))
        df['Address'] = df['Address'].apply(lambda x: extract_formatted_address(x))
        df['Title'] = df['Title'].apply(lambda x: x[:1000])  # Title 컬럼을 최대 1000자로 제한
        df['Description'] = df['Description'].apply(lambda x: x[:5000])  # Description 컬럼을 최대 5000자로 제한
        df['LocationID'] = df['LocationID'].apply(lambda x: x[:1000])  # LocationID 컬럼을 최대 1000자로 제한
        df['Address'] = df['Address'].apply(lambda x: x[:2000])  # Address 컬럼을 최대 2000자로 제한

        def translate_to_korean(text):
            translator = Translator()
            try:
                translated = translator.translate(text, dest='ko').text
            except Exception as e:
                print(f"번역 중 오류 발생: {e}")
                translated = None  # 또는 필요에 따라 다른 기본값 설정
            time.sleep(0.5)
            return translated

        # 예제 실행
        text_to_translate = "This is a test sentence."
        translated_text = translate_to_korean(text_to_translate)
        if translated_text is None:
            print("번역 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        else:
            print(f"번역 결과: {translated_text}")

        # 각 필드를 번역하고 최대 길이 제한 적용
        df['Title'] = df['Title'].apply(lambda x: translate_to_korean(x))
        print("Title")
        df['Description'] = df['Description'].apply(lambda x: translate_to_korean(x))
        print("Description")
        df['Region'] = df['Region'].apply(lambda x: translate_to_korean(x))
        print("Region")

        df['Description'] = df['Description'].apply(lambda x: remove_source_info_empty(x))

        transformed_data = df.to_dict(orient='records')
    else:
        print("데이터가 올바르게 준비되지 않았습니다.")

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def remove_source_info(description):
    pattern = r'^Sourced from predicthq\.com - '
    return re.sub(pattern, '', description)

def remove_source_info_empty(description):
    pattern = r'^predicthq.com에서 소스'
    return re.sub(pattern, '상세정보 아직 없음.', description)
    
def generate_and_save_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    redshift_conn_id = 'redshift_connection'
    redshift_table = 'travel_events'

    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # Define the table schema
    create_table_sql = f"""
    CREATE TABLE {redshift_table} (
        EventID VARCHAR(256) PRIMARY KEY,
        Title VARCHAR(1000),
        Description VARCHAR(3000),
        Category VARCHAR(256),
        Rank INT,
        PhqAttendance BIGINT, -- 숫자를 저장할 수 있는 더 큰 범위의 타입으로 변경
        TimeStart VARCHAR(50), -- ISO 8601 형식의 문자열
        TimeEnd VARCHAR(50), -- ISO 8601 형식의 문자열
        LocationID VARCHAR(50),
        Address TEXT,
        Region TEXT,
        Country VARCHAR(50),
        PredictedEventSpend FLOAT
    );
    """

    # Drop table if it exists
    drop_table_sql = f"DROP TABLE IF EXISTS {redshift_table};"

    try:
        # Drop the existing table
        cursor.execute(drop_table_sql)
        conn.commit()

        # Create a new table
        cursor.execute(create_table_sql)
        conn.commit()

        if transformed_data:
            for record in transformed_data:
                columns = ', '.join(record.keys())
                values_placeholders = ', '.join(['%s'] * len(record))
                sql = f"""
                    INSERT INTO {redshift_table} ({columns})
                    VALUES ({values_placeholders});
                """
                try:
                    cursor.execute(sql, tuple(record.values()))
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting record {record}: {e}")
                    conn.rollback()
    except Exception as e:
        print(f"Error creating or dropping table: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()



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
    schedule_interval=None,
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
