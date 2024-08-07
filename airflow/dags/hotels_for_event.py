from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

def calculate_distance(lon1, lat1, lon2, lat2):
    """
    두 지점의 거리를 계산합니다.
    """
    return ((lon2 - lon1)**2 + (lat2 - lat1)**2)**0.5

def find_accommodations():
    print("반경 10km 이내의 숙소 찾기 시작...")
    # 숙소 데이터 로드 (hotel_id, location만 포함)
    accommodations_df = pd.read_csv('/tmp/Updated_Accommodations.csv', usecols=['hotel_id', 'location', 'rating', 'name'])
    accommodations_df['longitude'] = accommodations_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    accommodations_df['latitude'] = accommodations_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))
    google_accommodations_df =pd.read_csv('/tmp/Accommodations.csv', usecols=['location', 'rating', 'name'])
    
    high_rating_accommodations_df = google_accommodations_df[google_accommodations_df['rating'] >= 4]
    high_rating_accommodations_df['longitude'] = high_rating_accommodations_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    high_rating_accommodations_df['latitude'] = high_rating_accommodations_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))
    events_df = pd.read_csv('/tmp/TravelEvents.csv', usecols=['id', 'title', 'location'])
    
    # 이벤트 데이터프레임에 숙소 ID를 저장할 새로운 열 추가
    events_df['agoda_accommodation_ids'] = ''
    events_df['high_rating_accommodation_names'] = ''

    # 이벤트 데이터프레임에서 위치 정보 파싱
    events_df['longitude'] = events_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    events_df['latitude'] = events_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))

    # 각 이벤트에 대해 반경 10km 이내의 숙소 찾기
    for index, event in events_df.iterrows():
        nearby_accommodations = []
        high_rating_accommodations =[]
        for _, accommodation in accommodations_df.iterrows():
            distance = calculate_distance(event['longitude'], event['latitude'], accommodation['longitude'], accommodation['latitude'])
            if distance <= 0.1:  # 단순한 거리 계산이므로 10km에 해당하는 임의의 기준값 0.1 사용
                nearby_accommodations.append(accommodation['hotel_id'])
        if nearby_accommodations:
            events_df.at[index, 'agoda_accommodation_ids'] = ','.join(map(str, nearby_accommodations))
        else:
            events_df.at[index, 'agoda_accommodation_ids'] = ','.join(map(str, nearby_accommodations))
        for _, accommodation in high_rating_accommodations_df.iterrows():
            distance = calculate_distance(event['longitude'], event['latitude'], accommodation['longitude'], accommodation['latitude'])
            if distance <= 0.1:  # 단순한 거리 계산이므로 10km에 해당하는 임의의 기준값 0.1 사용
                high_rating_accommodations.append(accommodation['name'])
        
        events_df.at[index, 'high_rating_accommodation_names'] = ','.join(high_rating_accommodations)
            

    # id와 name, accommodation_ids, high_rating_accommodation_names 열만 포함한 데이터프레임 저장
    result_df = events_df[['id', 'title', 'agoda_accommodation_ids', 'high_rating_accommodation_names']]
    result_df.to_csv('/tmp/Updated_Events_with_Accommodations.csv', index=False)
    print("숙소 ID 및 평점 4 이상인 숙소 이름이 포함된 업데이트된 CSV 파일이 저장되었습니다.")
    
def create_schema_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotels_for_event'
    schema_name = 'hotel'
    
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    drop_table= f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
    # Define the table schema
    create_table_sql = f"""

    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        EventID VARCHAR(512) PRIMARY KEY,  -- Increased length
        Title VARCHAR(1000),
        Agoda_Hotels VARCHAR(65535),
        Google_Place_Hotels VARCHAR(65535)
    );

    """
    cursor.execute(drop_table)
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
def update_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel.hotels_for_event'
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    hotel_df=pd.read_csv('/tmp/Updated_Events_with_Accommodations.csv', usecols=['id', 'title', 'agoda_accommodation_ids', 'high_rating_accommodation_names'])
    print(hotel_df)
    
    for index, row in hotel_df.iterrows():
        # Check if the record exists
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE EventID = %s", (row['id'],))
        exists = cursor.fetchone()[0]
        
        if exists:
            # Update the existing record
            cursor.execute(f"""
                UPDATE {table_name}
                SET Title = %s,
                    Agoda_Hotels = %s,
                    Google_Place_Hotels = %s
                WHERE EventID = %s
            """, (row['title'], row['agoda_accommodation_ids'], row['high_rating_accommodation_names'], row['id']))
        else:
            # Insert the new record
            cursor.execute(f"""
                INSERT INTO {table_name} (EventID, Title, Agoda_Hotels, Google_Place_Hotels)
                VALUES (%s, %s, %s, %s)
            """, (row['id'], row['title'], row['agoda_accommodation_ids'], row['high_rating_accommodation_names']))
    
    conn.commit()
    cursor.close()
    conn.close()
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'events_with_accommodations',
    default_args=default_args,
    description='이벤트 반경 10km 내 숙소 찾기 및 업데이트된 CSV 저장',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='find_accommodations',
    python_callable=find_accommodations,
    dag=dag,
)

t2= PythonOperator(
    task_id='create_schema_table',
    python_callable=create_schema_table,
    provide_context=True,
    dag=dag,
)

t3=PythonOperator(
    task_id='update_table',
    python_callable=update_table,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3
