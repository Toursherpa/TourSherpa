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
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable

'''
Dag for Collecting cafe, restaurant data

01. Read events list from S3
02. Request cafe,restaurant list with Google Place API
03. S3 Load
04. Redshift COPY csv file from S3

'''




def read_events_from_s3(**context):
    logging.info("Starting read_events_from_s3")
    try:
        # AWS 연결
        s3_hook = S3Hook(aws_conn_id='s3_connection')
        bucket_name = Variable.get('s3_bucket_name')

        # S3에서 CSV 파일 가져오기
        file_obj = s3_hook.get_key(key='source/source_TravelEvents/TravelEvents.csv', bucket_name=bucket_name)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        
        # CSV 파일을 DataFrame으로 변환
        df = pd.read_csv(StringIO(file_content))
        df = df[['id', 'title', 'location']]
        df['location'] = df['location'].apply(lambda loc: f"{ast.literal_eval(loc)[1]},{ast.literal_eval(loc)[0]}")
        
        # DataFrame을 XCom으로 푸시
        context['ti'].xcom_push(key='place_location', value=df.to_dict(orient='records'))
        logging.info("Event's ID, Title, location and preprocessed Location have been extracted and pushed to XCom.")
    
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
        places_per_location = 5   # cafe, restaurant 수집 갯수
        places_data = []
        collection_date = datetime.now().strftime('%Y-%m-%d')  # 데이터 수집 날짜 추가


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

            location = record['location']
            event_id = record['id']
            event_title = record['title']

            for place_type in ['restaurant', 'cafe']:

                places_result = get_places(location, place_type)
                places = places_result['results'][:places_per_location]

                for place in places:
                    place_id = place['place_id']
                    details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                    details_params = {
                        'place_id': place_id,
                        'language': 'ko', # 가져올 데이터 언어 설정
                        'key': Variable.get('GOOGLE_API_KEY')
                    }
                    details_response = requests.get(details_url, params=details_params)
                    details_response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
                    place_details = details_response.json()
                    place_name = place_details['result']['name']
                    place_address = place_details['result'].get('formatted_address', 'N/A')
                    place_rating = place_details['result'].get('rating', 'N/A')
                    place_user_ratings_total = place_details['result'].get('user_ratings_total', 'N/A')
                    place_reviews = place_details['result'].get('reviews', [])
                    place_lat = place_details['result']['geometry']['location']['lat']
                    place_lng = place_details['result']['geometry']['location']['lng']
                    place_types = ', '.join(place_details['result'].get('types', []))
                    place_opening_hours = place_details['result'].get('opening_hours', {}).get('weekday_text', 'N/A')

                    place_opening_hours_str = place_opening_hours if place_opening_hours else ''
                    place_review = place_reviews[0]['text'] if place_reviews else 'No reviews'
                    place_review = place_review.replace('\n', ' ')  # 줄바꿈 문자를 공백으로 대체

                    places_data.append({
                        'Event ID': event_id,
                        'Event Title': event_title,
                        'Location': location,
                        'Place Name': place_name,
                        'Address': place_address,
                        'Rating': place_rating,
                        'Number of Reviews': place_user_ratings_total,
                        'Review': place_review,
                        'Latitude': place_lat,
                        'Longitude': place_lng,
                        'Types': place_types,
                        'Opening Hours': place_opening_hours_str,
                        'Collection Date': collection_date
                    })

        # 데이터 .csv로 저장
        df_places = pd.DataFrame(places_data)
        csv_filename = '/tmp/places_data.csv'
        df_places.to_csv(csv_filename, index=False, encoding='utf-8-sig')

        # S3 적재 
        s3_bucket_name = Variable.get('s3_bucket_name')
        s3_key = 'source/source_place/place_cafe_restaurant.csv'
        s3_hook = S3Hook('s3_connection')
        s3_hook.load_file(filename=csv_filename, key=s3_key, bucket_name=s3_bucket_name, replace=True)

        context['ti'].xcom_push(key='place_results', value=df_places.to_dict(orient='records'))
        logging.info("Places data has been extracted and saved to S3.")
        
    except Exception as e:
        logging.error(f"Error in fetch_places_data: {e}")
        raise
    

def preprocess_redshift_table():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()
        
        # 기존 테이블 삭제 및 테이블 생성
        cursor.execute(f"DROP TABLE IF EXISTS {Variable.get('redshift_schema_places')}.{Variable.get('redshift_table_places')};")
        cursor.execute(f"""
            CREATE TABLE {Variable.get('redshift_schema_places')}.{Variable.get('redshift_table_places')} (
                "Event ID" VARCHAR(256),
                "Event Title" VARCHAR(256),
                "Location" VARCHAR(256),
                "Place Name" VARCHAR(256),
                "Address" VARCHAR(256),
                "Rating" FLOAT,
                "Number of Reviews" INT,
                "Review" VARCHAR(65535),
                "Latitude" FLOAT,
                "Longitude" FLOAT,
                "Types" VARCHAR(256),
                "Opening Hours" VARCHAR(65535),
                "Collection Date" DATE
            );
        """)
        redshift_conn.commit()
        logging.info(f"Redshift table {Variable.get('redshift_table_places')} has been dropped and recreated.")
        
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
    'place_cafe_restaurant',
    default_args=default_args,
    description='Extract places data(Cafe,Restaurant) using Google Places API, save to S3 and Redshift',
    schedule_interval='@daily',
    catchup=False,
)


read_events_from_s3_task = PythonOperator(
    task_id='read_events_csv_from_s3',
    python_callable=read_events_from_s3,
    provide_context=True,
    dag=dag,
)

fetch_places_data_task = PythonOperator(
    task_id='fetch_places_data',
    python_callable=fetch_places_data,
    provide_context=True,
    dag=dag,
)

preprocess_redshift_task = PythonOperator(
    task_id='preprocess_redshift_table',
    python_callable=preprocess_redshift_table,
    provide_context=True,
    dag=dag,
)

load_to_redshift_task = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    schema=Variable.get('redshift_schema_places'),
    table=Variable.get('redshift_table_places'),
    s3_bucket=Variable.get('s3_bucket_name'),
    s3_key='source/source_place/place_cafe_restaurant.csv',
    copy_options=['IGNOREHEADER 1', 'csv'],
    aws_conn_id='s3_connection',
    redshift_conn_id='redshift_connection',
    dag=dag,
)


read_events_from_s3_task >> fetch_places_data_task >> preprocess_redshift_task >> load_to_redshift_task
