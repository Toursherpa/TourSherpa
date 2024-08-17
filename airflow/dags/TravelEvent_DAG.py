from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from io import StringIO
import pandas as pd
import requests
import pytz

countrys=['AT', 'AU', 'CA', 'CN', 'DE', 'ES', 'FR', 'GB', 'ID', 'IN', 'IT', 'JP', 'MY', 'NL', 'TW', 'US']
categories = [
    "expos",
    "concerts",
    "festivals",
    "sports",
]

#시간대 한국으로 설정
kst = pytz.timezone('Asia/Seoul')
utc_now = datetime.utcnow()
kst_now = utc_now.astimezone(kst)
today = kst_now.strftime('%Y-%m-%d')
future_date = datetime.today() + timedelta(days=90)
yesterday = datetime.today() - timedelta(days=1)
future_date_str = future_date.strftime('%Y-%m-%d')
yesterday_str = yesterday.strftime('%Y-%m-%d')

#가져올 데이터 설정
def fetch_data_setting(country, category):
    ACCESS_TOKEN = Variable.get('predicthq_ACCESS_TOKEN')
    response1 = requests.get(
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
            "sort":"rank",
        }
    )
    response2 = requests.get(
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
            "sort":"rank",
            "updated.gte":yesterday_str
        }
    )
    data1 = response1.json()
    data2 = response2.json()
    return [data1, data2]

#새로운 데이터와 새로 업데이트된 행사 데이터 가져오기
def fetch_and_upload_data(**kwargs):
    combined_df = pd.DataFrame()
    for country in countrys:
        for category in categories:
            fetch_data = fetch_data_setting(country, category)[0]
            df = pd.DataFrame(fetch_data["results"])
            if not df.empty:
                combined_df = pd.concat([combined_df, df], ignore_index=True)

    if not combined_df.empty:
        combined_df = combined_df.sort_values(by=['rank', 'predicted_event_spend'], ascending=[False, False])
        combined_df['update_type'] = 0  
        combined_df.to_csv(f'/tmp/TravelEvent_data.csv', index=False, encoding='utf-8-sig')
    else:
        combined_df.to_csv(f'/tmp/TravelEvent_data.csv', index=False, encoding='utf-8-sig')
        print(f"No data fetched for. Skipping CSV creation.")

    print("Domestic data fetched and saved to '/tmp/TravelEvents_data.csv'")

    combined_up_df = pd.DataFrame()
    for country in countrys:
        for category in categories:
            fetch_up_data = fetch_data_setting(country, category)[1]
            up_df = pd.DataFrame(fetch_up_data["results"])
            if not up_df.empty:
                combined_up_df = pd.concat([combined_up_df, up_df], ignore_index=True)

    if not combined_up_df.empty:
        combined_up_df = combined_up_df.sort_values(by=['rank', 'predicted_event_spend'], ascending=[False, False])
        combined_up_df['update_type'] = 1 
        combined_up_df.to_csv(f'/tmp/UP_TravelEvent_data.csv', index=False, encoding='utf-8-sig')
    else:
        combined_up_df.to_csv(f'/tmp/UP_TravelEvent_data.csv', index=False, encoding='utf-8-sig')
        print(f"No data fetched for. Skipping CSV creation.")

    print("Domestic data fetched and saved to '/tmp/UP_TravelEvents_data.csv'")

    kwargs['ti'].xcom_push(key='combined_df_path', value='/tmp/TravelEvent_data.csv')
    kwargs['ti'].xcom_push(key='combined_up_df_path', value='/tmp/UP_TravelEvent_data.csv')

#전날 데이터 가져오기
def read_data_from_s3(**kwargs):
    s3_hook = S3Hook('s3_connection')
    s3_bucket_name = Variable.get('s3_bucket_name')

    s3_key = f'source/source_TravelEvents/{yesterday_str}/TravelEvents.csv'
    if s3_hook.check_for_key(key=s3_key, bucket_name=s3_bucket_name):
        file_obj = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket_name)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        transformed_data = {'file_content': file_content}  
    else:
        print("파일을 찾을 수 없습니다. 건너뜁니다.")
        transformed_data = None

    kwargs['ti'].xcom_push(key='s3_data', value=transformed_data)

'''
전날 데이터와 최신 데이터로 update_type을 기반으로 한 update csv 파일 업데이트
0-동일한 데이터, 변화 X
1-행사 위치가 아닌 다른 데이터의 변화 O
2-행사 위치를 포함한 데이터의 변화 O
3-새로운 EventID 생성됨 O 
'''
def update_combined_df(**kwargs):
    ti = kwargs['ti']
    combined_df_path = ti.xcom_pull(task_ids='fetch_data_TravelEvents', key='combined_df_path')
    combined_up_df_path = ti.xcom_pull(task_ids='fetch_data_TravelEvents', key='combined_up_df_path')

    combined_df = pd.read_csv(combined_df_path)
    combined_up_df = pd.read_csv(combined_up_df_path)

    s3_data = kwargs['ti'].xcom_pull(key='s3_data', task_ids='read_data_from_s3')

    if s3_data and 'file_content' in s3_data:
        file_content = s3_data['file_content']
        pre_df = pd.read_csv(StringIO(file_content))
        pre_df['update_type'] = 0  

        combined_ids = set(combined_df['id'])
        pre_ids = set(pre_df['id'])
        new_data = combined_df[combined_df['id'].isin(combined_ids - pre_ids)]
        new_data['update_type'] = 3

        merged_df = pd.merge(combined_up_df, pre_df[['id', 'location']], on='id', how='left', suffixes=('', '_pre'))
        merged_df['update_type'] = merged_df.apply(
            lambda row: 2 if row['location'] != row['location_pre'] else row['update_type'],
            axis=1
        )

        updated_combined_up_df = pd.concat([merged_df, new_data], ignore_index=True)
        if not updated_combined_up_df.empty:
            updated_combined_up_df = updated_combined_up_df.sort_values(by=['rank', 'predicted_event_spend'],
                                                                        ascending=[False, False])
            updated_combined_up_df.to_csv(f'/tmp/NEWUP_TravelEvent_data.csv', index=False, encoding='utf-8-sig')
        else:
            updated_combined_up_df.to_csv(f'/tmp/NEWUP_TravelEvent_data.csv', index=False, encoding='utf-8-sig')
            print("No new data fetched. Skipping CSV creation.")

        print("New data fetched and saved to '/tmp/NEWUP_TravelEvent_data.csv'")
    else:
        print("No previous data found in S3. Skipping update.")


#update csv 파일과 최신 데이터 S3에 저장
def generate_and_save_data(**kwargs):
    csv_filename = f'/tmp/TravelEvent_data.csv'
    csv_up_filename = f'/tmp/NEWUP_TravelEvent_data.csv'
    s3_bucket_name = Variable.get('s3_bucket_name')
    s3_key = f'source/source_TravelEvents/{today}/TravelEvents.csv'
    s3_up_key = f'source/source_TravelEvents/{today}/UP_TravelEvents.csv'

    s3_hook = S3Hook('s3_connection')
    s3_hook.load_file(filename=csv_filename, key=s3_key, bucket_name=s3_bucket_name, replace=True)
    s3_hook.load_file(filename=csv_up_filename, key=s3_up_key, bucket_name=s3_bucket_name, replace=True)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'update_TravelEvents_Dags',
    default_args=default_args,
    description='A DAG to update parking data every days and save it to S3',
    schedule_interval='1 15 * * *',
    catchup=False,
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_data_TravelEvents',
    python_callable=fetch_and_upload_data,
    dag=dag,
)

read_data_from_s3_task = PythonOperator(
    task_id='read_data_from_s3',
    python_callable=read_data_from_s3,
    provide_context=True,
    dag=dag,
)

update_combined_df = PythonOperator(
    task_id='update_combined_data',
    python_callable=update_combined_df,
    provide_context=True,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_TravelEvents_data',
    python_callable=generate_and_save_data,
    provide_context=True,
    dag=dag,
)

#행사 정보가 필요한 다른 Dag들 트리거로 실행
trigger_second_dag = TriggerDagRunOperator(
    task_id='trigger_second_dag',
    trigger_dag_id='update_TravelEvents_Dags_to_Redshift',
    dag=dag,
)

trigger_third_dag = TriggerDagRunOperator(
    task_id='trigger_third_dag',
    trigger_dag_id='nearest_airports_dag',
    dag=dag,
)

trigger_fourth_dag = TriggerDagRunOperator(
    task_id='trigger_fourth_dag',
    trigger_dag_id='place_update',
    dag=dag,
)

trigger_fifth_dag = TriggerDagRunOperator(
    task_id='trigger_fifth_dag',
    trigger_dag_id='google_hotel_list',
    dag=dag,
)

fetch_and_upload_task >> read_data_from_s3_task >> update_combined_df >> upload_to_s3_task >> trigger_second_dag >> trigger_third_dag >> trigger_fourth_dag >> trigger_fifth_dag
