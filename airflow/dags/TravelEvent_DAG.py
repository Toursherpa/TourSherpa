from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import requests
import pytz

countrys=['AT', 'AU', 'BR', 'CA', 'CN', 'DE', 'ES', 'FR', 'GB', 'ID', 'IN', 'IT', 'JP', 'MY', 'NL', 'TW', 'US']
categories = [
    "expos",
    "concerts",
    "festivals",
    "sports",
]

kst = pytz.timezone('Asia/Seoul')
utc_now = datetime.utcnow()
kst_now = utc_now.astimezone(kst)
today = kst_now.strftime('%Y-%m-%d')
future_date = datetime.today() + timedelta(days=90)
future_date_str = future_date.strftime('%Y-%m-%d')

def fetch_data_setting(country, category):
    ACCESS_TOKEN = Variable.get('predicthq_ACCESS_TOKEN')
    response = requests.get(
        url="https://api.predicthq.com/v1/events/",
        headers={
          "Authorization": f"Bearer {ACCESS_TOKEN}",
          "Accept": "application/json"
        },
        params={
            "country":country,
            "active.gte":today,
            "active.lte":future_date_str,
            "category":category,
            'limit': 2000,
            "rank.gte":85,
            "sort":"rank"
        }
    )
    data = response.json()
    return data
def fetch_and_upload_data():
    combined_df = pd.DataFrame()
    for country in countrys:
        for category in categories:
            fetch_data = fetch_data_setting(country, category)
            df = pd.DataFrame(fetch_data["results"])
            if not df.empty:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
    if not combined_df.empty:
        combined_df = combined_df.sort_values(by=['rank', 'predicted_event_spend'], ascending=[False, False])
        combined_df.to_csv(f'/tmp/TravelEvent_data.csv', index=False, encoding='utf-8-sig')
    else:
        combined_df.to_csv(f'/tmp/TravelEvent_data.csv', index=False, encoding='utf-8-sig')
        print(f"No data fetched for. Skipping CSV creation.")
    print("Domestic data fetched and saved to '/tmp/TravelEvents_data.csv'")

def generate_and_save_data(**kwargs):
    csv_filename = f'/tmp/TravelEvent_data.csv'  # Connection ID of your S3 connection in Airflow
    s3_bucket_name = Variable.get('my_s3_bucket')
    s3_key = f'source/source_TravelEvents/TravelEvents.csv'

    s3_hook = S3Hook('TravelEvent_s3_conn')
    s3_hook.load_file(filename=csv_filename, key=s3_key, bucket_name=s3_bucket_name, replace=True)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'update_TravelEvents_Dags',
    default_args=default_args,
    description='A DAG to update parking data every days and save it to S3',
    schedule_interval='@daily',
    catchup=False,
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_data_TravelEvents',
    python_callable=fetch_and_upload_data,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_JP_TravelEvents_data',
    python_callable=generate_and_save_data,
    provide_context=True,
    dag=dag,
)

fetch_and_upload_task >> upload_to_s3_task