import logging
import ast
import pandas as pd
import requests
from io import StringIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta, timezone
from airflow.utils.dates import days_ago
from airflow.models import Variable

'''
Dag for Collecting restaurant data with specific update_type handling

01. Read events list from S3
02. Request restaurant list with Google Place API
03. S3 Load
04. Redshift COPY csv file from S3

'''




def read_events_from_s3(**context):
    logging.info("Starting read_events_from_s3")
    try:
        # AWS 연결
        s3_hook = S3Hook(aws_conn_id='s3_connection')
        bucket_name = Variable.get('s3_bucket_name')
        collection_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        s3_key = f'source/source_TravelEvents/{collection_date}/UP_TravelEvents.csv'

        # S3에서 CSV 파일 가져오기
        file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        
        # CSV 파일을 DataFrame으로 변환
        df = pd.read_csv(StringIO(file_content))
        df = df[['id', 'title', 'location', 'update_type']]
        df['location'] = df['location'].apply(lambda loc: f"{ast.literal_eval(loc)[1]},{ast.literal_eval(loc)[0]}")
        
        # DataFrame을 XCom으로 푸시
        context['ti'].xcom_push(key='place_location', value=df.to_dict(orient='records'))
        logging.info("Event's ID, Title, location, update_type and preprocessed Location have been extracted and pushed to XCom.")
    
    except Exception as e:
        logging.error(f"Error in read_events_from_s3: {e}")
        raise




def fetch_places_data(**context):
    try:
        # Event 데이터 Xcom으로 불러오기
        locations = context['ti'].xcom_pull(key='place_location') 
        
        # 데이터가 None이면 에러를 발생시켜 문제를 확인
        if locations is None:
            raise ValueError("No data found in XCom for key 'place_location'")
        
        # 데이터 수집 설정
        logging.info("Starting fetch_places_data")
        places_per_location = 1
        places_data = []
        collection_date = datetime.now(timezone.utc).strftime('%Y-%m-%d') # 데이터 수집 날짜 추가

        # Google Place API Request
        def get_places(location, place_type, radius=3000):
            url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            
            params = {
                'location': location,
                'radius': radius,
                'type': place_type,
                'key': Variable.get('GOOGLE_API_KEY')
            }
            response = requests.get(url, params=params)
            response.raise_for_status()

            return response.json()

        # Google Place API로 데이터 수집
        for record in locations:
            update_type = record['update_type']

            if update_type in [2, 3]:  # '2' 및 '3' 유형에 대해서만 검색
                location = record['location']
                event_id = record['id']
                event_title = record['title']

                places_result = get_places(location, 'restaurant')
                places = places_result['results']  # 모든 restaurant 데이터를 수집
                places = places_result['results'][:places_per_location]

                for place in places:
                    place_id = place['place_id']
                    details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                    details_params = {
                        'place_id': place_id,
                        'language': 'ko',  # 가져올 데이터 언어 설정
                        'key': Variable.get('GOOGLE_API_KEY')
                    }
                    details_response = requests.get(details_url, params=details_params)
                    details_response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
                    place_details = details_response.json()
                    place_name = place_details['result']['name']
                    place_address = place_details['result'].get('formatted_address', None)
                    place_rating = place_details['result'].get('rating', None)
                    place_user_ratings_total = place_details['result'].get('user_ratings_total', None)
                    place_reviews = place_details['result'].get('reviews', [])
                    place_lat = place_details['result']['geometry']['location']['lat']
                    place_lng = place_details['result']['geometry']['location']['lng']
                    place_types = ', '.join(place_details['result'].get('types', []))
                    place_opening_hours = place_details['result'].get('opening_hours', {}).get('weekday_text', None)

                    place_opening_hours_str = place_opening_hours if place_opening_hours else ''
                    place_review = place_reviews[0]['text'] if place_reviews else 'No reviews'
                    place_review = place_review.replace('\n', ' ')  # 줄바꿈 문자를 공백으로 대체

                    places_data.append({
                        'Event_ID': event_id,
                        'Event_Title': event_title,
                        'Location': location,
                        'Place_Name': place_name,
                        'Address': place_address,
                        'Rating': place_rating,
                        'Number_of_Reviews': place_user_ratings_total,
                        'Review': place_review,
                        'Latitude': place_lat,
                        'Longitude': place_lng,
                        'Types': place_types,
                        'Opening_Hours': place_opening_hours_str,
                        'Collection_Date': collection_date,
                        'Update_Type': update_type  # 추가된 업데이트 타입
                    })

        # 데이터 .csv로 저장
        df_places = pd.DataFrame(places_data)
        csv_filename = f'/tmp/UP_place_{collection_date}.csv'
        df_places.to_csv(csv_filename, index=False, encoding='utf-8-sig')

        # S3 적재 
        s3_bucket_name = Variable.get('s3_bucket_name')
        s3_key = f'source/source_place/{collection_date}/UP_place.csv'
        s3_hook = S3Hook('s3_connection')
        s3_hook.load_file(filename=csv_filename, key=s3_key, bucket_name=s3_bucket_name, replace=True)

        context['ti'].xcom_push(key='place_results', value=df_places.to_dict(orient='records'))
        logging.info(f"Places data has been extracted and saved to S3 at {s3_key}.")
        
    except Exception as e:
        logging.error(f"Error in fetch_places_data: {e}")
        raise




def preprocess_redshift_table(**context):
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()

        # 새로 수집된 데이터 불러오기
        places_data = context['ti'].xcom_pull(key='place_results')

        if places_data is None:
            raise ValueError("No place data found in XCom for key 'place_results'")

        for record in places_data:
            event_id = record['Event_ID']
            update_type = record['Update_Type']

            if update_type == 2:
                # 기존 데이터를 삭제하고 새로 추가
                cursor.execute(f"DELETE FROM place.events_places WHERE Event_ID = '{event_id}';")

            if update_type in [2, 3]:
                # 새로운 데이터 추가
                cursor.execute("""
                    INSERT INTO place.events_places (
                        "Event_ID", "Event_Title", "Location", "Place_Name", "Address", "Rating", 
                        "Number_of_Reviews", "Review", "Latitude", "Longitude", "Types", 
                        "Collection_Date"
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    record['Event_ID'], record['Event_Title'], record['Location'], 
                    record['Place_Name'], record['Address'], record['Rating'], 
                    record['Number_of_Reviews'], record['Review'], record['Latitude'], 
                    record['Longitude'], record['Types'], record['Collection_Date']
                ))
                logging.info(f"Inserted new record for Event_ID: {event_id}")

        redshift_conn.commit()
        logging.info("Redshift table place.events_places has been updated with new data.")
        
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
    'place_update',
    default_args=default_args,
    description='Extract restaurant data using Google Places API, save to S3 and Redshift with specific update_type handling',
    schedule_interval='@daily',
    catchup=False,
)

# # 선행 DAG의 완료를 감지
# wait_for_travelEvents = ExternalTaskSensor(
#     task_id='place_wait_for_travelEvents',
#     external_dag_id='update_TravelEvents_Dags',  # 선행 DAG의 ID
#     external_task_id='upload_TravelEvents_data',  # 선행 DAG의 마지막 태스크 ID
#     allowed_states=['success'],  # 성공 상태일 때 진행
#     failed_states=['failed', 'skipped'],  # 실패 시 멈춤
#     mode='poke',  # poke 모드 사용
#     timeout=600  # 최대 기다릴 시간(초)
# )

read_events_from_s3_task = PythonOperator(
    task_id='read_UP_events_csv_from_s3',
    python_callable=read_events_from_s3,
    provide_context=True,
    dag=dag,
)

fetch_places_data_task = PythonOperator(
    task_id='fetch_UP_places_data',
    python_callable=fetch_places_data,
    provide_context=True,
    dag=dag,
)

preprocess_redshift_task = PythonOperator(
    task_id='preprocess_UP_redshift_table',
    python_callable=preprocess_redshift_table,
    provide_context=True,
    dag=dag,
)

#wait_for_nearest_task >> read_events_from_s3_task >> fetch_places_data_task >> preprocess_redshift_task
read_events_from_s3_task >> fetch_places_data_task >> preprocess_redshift_task