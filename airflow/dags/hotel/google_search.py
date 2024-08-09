from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import ast
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import time
import logging

def download_csv_from_s3(aws_conn_id, s3_bucket, local_path, s3_key):
    logging.info(f"download_csv_from_s3 시작 - S3 Bucket: {s3_bucket}, Key: {s3_key}, Local Path: {local_path}")
    hook = S3Hook(aws_conn_id=aws_conn_id)
    if not hook.check_for_key(s3_key, s3_bucket):
        logging.error(f"S3 키 {s3_key} 가 버킷 {s3_bucket} 내에 존재하지 않습니다.")
        raise AirflowException(f"S3 키 {s3_key} 가 버킷 {s3_bucket} 내에 존재하지 않습니다.")

    s3_object = hook.get_key(s3_key, s3_bucket)
    content = s3_object.get()['Body'].read().decode('utf-8')

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'w') as f:
        f.write(content)
    logging.info(f"S3에서 {s3_key} 파일을 성공적으로 다운로드하여 {local_path}에 저장했습니다.")

def fetch_hotel_info(location, google_api_key):
    logging.info(f"fetch_hotel_info 시작 - Location: {location}")
    params = {
        'location': f"{location[1]},{location[0]}",
        'radius': 5000,
        'rankby': 'prominence',
        'type': 'lodging',
        'key': google_api_key
    }
    endpoint_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    hotels = []
    page_count = 0

    while True:
        response = requests.get(endpoint_url, params=params)
        hotels.extend(response.json().get('results', []))
        next_page_token = response.json().get('next_page_token')
        if not next_page_token or page_count >= 2:
            break
        time.sleep(2)
        params['pagetoken'] = next_page_token
        page_count += 1

    logging.info(f"{location} 위치에 대해 {len(hotels)}개의 호텔 정보를 가져왔습니다.")
    return hotels

def upload_to_s3(local_path, s3_bucket, s3_key, aws_conn_id):
    logging.info(f"upload_to_s3 시작 - Local Path: {local_path}, S3 Bucket: {s3_bucket}, S3 Key: {s3_key}")
    hook = S3Hook(aws_conn_id=aws_conn_id)
    for attempt in range(3):
        try:
            hook.load_file(filename=local_path, key=s3_key, bucket_name=s3_bucket, replace=True)
            logging.info(f"{local_path} 파일을 S3 버킷 {s3_bucket}의 {s3_key}로 업로드했습니다.")
            return
        except Exception as e:
            logging.warning(f"파일 업로드 중 오류 발생: {e}. 재시도 중... ({attempt + 1}/3)")
    logging.error(f"여러 번의 시도 끝에 파일 업로드에 실패했습니다. 경로: {local_path}, S3 키: {s3_key}, 버킷: {s3_bucket}")
    raise RuntimeError(f"여러 번의 시도 끝에 파일 업로드에 실패했습니다. 경로: {local_path}, S3 키: {s3_key}, 버킷: {s3_bucket}")

def find_latest_google_hotels_file(ti, aws_conn_id, s3_bucket):
    logging.info("find_latest_google_hotels_file 시작")
    last_update = ''
    for days_back in range(90):
        date_to_try = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        google_hotels_s3_key = f'source/source_TravelEvents/{date_to_try}/google_hotels.csv'
        google_hotels_local_path = f'/tmp/{date_to_try}/google_hotels.csv'

        try:
            logging.info(f"{date_to_try}의 google_hotels.csv 파일 다운로드 시도 중...")
            download_csv_from_s3(aws_conn_id, s3_bucket, google_hotels_local_path, google_hotels_s3_key)
            last_update = date_to_try
            logging.info(f"{date_to_try}에 날짜가 찍힌 최신 파일을 성공적으로 찾았습니다.")
            break
        except AirflowException:
            logging.info(f"{date_to_try} 날짜의 파일을 찾지 못했습니다. 이전 날짜를 시도합니다.")
            continue
    
    logging.info(f"find_latest_google_hotels_file 완료 - 마지막 업데이트 날짜: {last_update}")
    ti.xcom_push(key='last_update', value=last_update)

def merge_up_travel_events(ti, aws_conn_id, s3_bucket, current_date):
    logging.info("merge_up_travel_events 시작")
    last_update = ti.xcom_pull(key='last_update', task_ids='find_latest_google_hotels_file')
    logging.info(f"가져온 last_update 값: {last_update}")

    if not last_update:
        logging.warning("최신 google_hotels.csv 파일을 찾지 못했습니다. 백업 파일을 사용합니다.")
        events_s3_key = 'source/source_TravelEvents/TravelEvents.csv'
        events_local_path = '/tmp/TravelEvents.csv'

        try:
            download_csv_from_s3(aws_conn_id, s3_bucket, events_local_path, events_s3_key)
            combined_df = pd.read_csv(events_local_path)
            logging.info(f"백업 파일 {events_s3_key}을 성공적으로 다운로드하여 사용합니다.")
        except AirflowException:
            logging.error(f"백업 파일 {events_s3_key}을 찾지 못했습니다. 프로세스를 종료합니다.")
            return
    else:
        last_update_date = datetime.strptime(last_update, '%Y-%m-%d')
        current_date = last_update_date
        events_dataframes = []

        while current_date <= datetime.utcnow():
            date_to_try = current_date.strftime('%Y-%m-%d')
            events_s3_key = f'source/source_TravelEvents/{date_to_try}/UP_TravelEvents.csv'
            events_local_path = f'/tmp/{date_to_try}/UP_TravelEvents.csv'

            try:
                download_csv_from_s3(aws_conn_id, s3_bucket, events_local_path, events_s3_key)
                df = pd.read_csv(events_local_path)
                events_dataframes.append(df)
                logging.info(f"{date_to_try} 날짜의 UP_TravelEvents.csv 파일을 성공적으로 다운로드하여 병합했습니다.")
            except AirflowException:
                logging.info(f"{date_to_try} 날짜의 UP_TravelEvents.csv 파일을 찾지 못했습니다.")
            
            current_date += timedelta(days=1)

        combined_df = pd.concat(events_dataframes) if events_dataframes else pd.DataFrame()

    combined_df_path = f'/tmp/{current_date}/combined_up_travel_events.csv'
    combined_df.to_csv(combined_df_path, index=False)
    logging.info(f"combined_up_travel_events.csv 파일이 {combined_df_path}에 저장되었습니다.")

def fetch_and_merge_hotel_info(current_date, google_api_key):
    logging.info(f"fetch_and_merge_hotel_info 시작 - Current Date: {current_date}")
    combined_df_path = f'/tmp/{current_date}/combined_up_travel_events.csv'
    if not os.path.exists(combined_df_path):
        logging.error("combined_up_travel_events.csv 파일이 존재하지 않습니다.")
        return

    combined_df = pd.read_csv(combined_df_path)
    google_hotels = []

    for loc_str in combined_df['location']: 
        try:
            location = ast.literal_eval(loc_str)
            hotels = fetch_hotel_info(location, google_api_key)
            for hotel in hotels:
                hotel['location'] = location
                google_hotels.append(hotel)
        except Exception as e:
            logging.warning(f"위치 {loc_str}에서 호텔 정보를 가져오는 중 오류 발생: {e}")

    result_df = pd.DataFrame(google_hotels)
    result_df_path = f'/tmp/{current_date}/fetched_google_hotels.csv'
    result_df.to_csv(result_df_path, index=False)
    logging.info(f"fetched_google_hotels.csv 파일이 {result_df_path}에 저장되었습니다.")

def merge_final_results(current_date, aws_conn_id, s3_bucket):
    logging.info(f"merge_final_results 시작 - Current Date: {current_date}")
    fetched_hotels_path = f'/tmp/{current_date}/fetched_google_hotels.csv'
    if not os.path.exists(fetched_hotels_path):
        logging.error("fetched_google_hotels.csv 파일이 존재하지 않습니다.")
        return

    result_df = pd.read_csv(fetched_hotels_path)
    last_hotels_s3_key = f'source/source_TravelEvents/{current_date}/google_hotels.csv'
    last_hotels_local_path = f'/tmp/{current_date}/last_google_hotels.csv'

    try:
        download_csv_from_s3(aws_conn_id, s3_bucket, last_hotels_local_path, last_hotels_s3_key)
        last_hotels_df = pd.read_csv(last_hotels_local_path)
        result_df = pd.concat([last_hotels_df, result_df], ignore_index=True)
        logging.info("기존 google_hotels.csv 파일과 새로운 데이터를 병합했습니다.")
    except AirflowException:
        logging.info(f"{last_hotels_s3_key} 파일을 찾지 못했으므로 병합을 생략합니다.")

    final_result_path = f'/tmp/{current_date}/final_google_hotels.csv'
    result_df.to_csv(final_result_path, index=False)
    logging.info(f"final_google_hotels.csv 파일이 {final_result_path}에 저장되었습니다.")

def upload_final_result(current_date, aws_conn_id, s3_bucket):
    logging.info(f"upload_final_result 시작 - Current Date: {current_date}")
    local_hotels_path = f'/tmp/{current_date}/final_google_hotels.csv'
    main_hotels_s3_key = 'source/source_TravelEvents/google_hotels.csv'

    if os.path.exists(local_hotels_path):
        upload_to_s3(local_hotels_path, s3_bucket, main_hotels_s3_key, aws_conn_id)

def backup_file_processing(aws_conn_id, s3_bucket, google_api_key, current_date):
    logging.info("backup_file_processing 시작")
    events_s3_key = 'source/source_TravelEvents/TravelEvents.csv'
    events_local_path = '/tmp/TravelEvents.csv'

    try:
        download_csv_from_s3(aws_conn_id, s3_bucket, events_local_path, events_s3_key)
        events_df = pd.read_csv(events_local_path)
        logging.info(f"백업 파일 {events_s3_key}을 성공적으로 다운로드했습니다.")
    except AirflowException:
        logging.error(f"백업 파일 {events_s3_key}을 찾지 못했습니다.")
        return

    if not events_df.empty:
        google_hotels = []
        for loc_str in events_df['location']:
            try:
                location = ast.literal_eval(loc_str)
                hotels = fetch_hotel_info(location, google_api_key)
                for hotel in hotels:
                    hotel['location'] = location
                    google_hotels.append(hotel)
            except Exception as e:
                logging.warning(f"위치 {loc_str}에서 호텔 정보를 가져오는 중 오류 발생: {e}")

        result_df = pd.DataFrame(google_hotels)
        local_dir = f'/tmp/{current_date}'
        os.makedirs(local_dir, exist_ok=True)
        local_hotels_path = os.path.join(local_dir, 'google_hotels.csv')

        if not result_df.empty:
            result_df.to_csv(local_hotels_path, index=False)
            upload_to_s3(local_hotels_path, s3_bucket, 'source/source_TravelEvents/google_hotels.csv', aws_conn_id)

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'google_hotel_list',
    default_args=default_args,
    description='S3 CSV 파일에서 위치를 가져와 호텔 목록을 검색하고, 새로운 CSV 파일을 S3에 저장하는 작업',
    schedule_interval=timedelta(days=5),
    catchup=False,
)

# 태스크 정의
find_latest_file_task = PythonOperator(
    task_id='find_latest_google_hotels_file',
    python_callable=find_latest_google_hotels_file,
    op_kwargs={'aws_conn_id': 's3_connection', 's3_bucket': 'team-hori-2-bucket'},
    dag=dag,
    provide_context=True  # XCom을 사용하기 위해 설정
)

merge_up_travel_events_task = PythonOperator(
    task_id='merge_up_travel_events',
    python_callable=merge_up_travel_events,
    op_kwargs={'aws_conn_id': 's3_connection', 's3_bucket': 'team-hori-2-bucket', 'current_date': datetime.utcnow().strftime("%Y-%m-%d")},
    dag=dag,
    provide_context=True  # XCom을 사용하기 위해 설정
)

fetch_and_merge_hotel_info_task = PythonOperator(
    task_id='fetch_and_merge_hotel_info',
    python_callable=fetch_and_merge_hotel_info,
    op_kwargs={'current_date': datetime.utcnow().strftime("%Y-%m-%d"), 'google_api_key': Variable.get("GOOGLE_API_KEY")},
    dag=dag,
)

merge_final_results_task = PythonOperator(
    task_id='merge_final_results',
    python_callable=merge_final_results,
    op_kwargs={'current_date': datetime.utcnow().strftime("%Y-%m-%d"), 'aws_conn_id': 's3_connection', 's3_bucket': 'team-hori-2-bucket'},
    dag=dag,
)

upload_final_result_task = PythonOperator(
    task_id='upload_final_result',
    python_callable=upload_final_result,
    op_kwargs={'current_date': datetime.utcnow().strftime("%Y-%m-%d"), 'aws_conn_id': 's3_connection', 's3_bucket': 'team-hori-2-bucket'},
    dag=dag,
)

backup_file_processing_task = PythonOperator(
    task_id='backup_file_processing',
    python_callable=backup_file_processing,
    op_kwargs={'aws_conn_id': 's3_connection', 's3_bucket': 'team-hori-2-bucket', 'google_api_key': Variable.get("GOOGLE_API_KEY"), 'current_date': datetime.utcnow().strftime("%Y-%m-%d")},
    dag=dag,
)

# 태스크 순서 정의
find_latest_file_task >> merge_up_travel_events_task >> fetch_and_merge_hotel_info_task >> merge_final_results_task >> upload_final_result_task
merge_final_results_task >> backup_file_processing_task