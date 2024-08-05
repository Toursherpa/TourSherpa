from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
def calculate_distance(lon1, lat1, lon2, lat2):

    return ((lon2 - lon1)**2 + (lat2 - lat1)**2)**0.5

def find_events():
    print("반경 10km 이내의 이벤트 찾기 시작...")
    # 숙소 데이터 로드 (hotel_id, longitude, latitude만 포함)
    accommodations_df = pd.read_csv('/tmp/Accommodations.csv', usecols=['name', 'place_id','location'] )
    events_df = pd.read_csv('/tmp/TravelEvents.csv')
    
    # 숙소 데이터프레임에 이벤트 ID를 저장할 새로운 열 추가
    accommodations_df['event_ids'] = ''
    # 숙소 데이터프레임에 위치 정보 파싱
    accommodations_df['longitude'] = accommodations_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    accommodations_df['latitude'] = accommodations_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))
    # 이벤트 데이터프레임에서 위치 정보 파싱
    events_df['longitude'] = events_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    events_df['latitude'] = events_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))

    # 'id' 열이 있는지 확인
    if 'id' not in events_df.columns:
        raise KeyError("TravelEvents.csv 파일에 'id' 열이 없습니다. 올바른 열 이름을 사용하고 있는지 확인하세요.")
    
    # 각 숙소에 대해 반경 10km 이내의 이벤트 찾기
    for index, accommodation in accommodations_df.iterrows():
        nearby_events = []
        for _, event in events_df.iterrows():
            distance = calculate_distance(accommodation['longitude'], accommodation['latitude'], event['longitude'], event['latitude'])
            if distance <= 0.1:  # 단순한 거리 계산이므로 10km에 해당하는 임의의 기준값 0.1 사용
                nearby_events.append(event['id'])
        if nearby_events:
            accommodations_df.at[index, 'event_ids'] = ','.join(map(str, nearby_events))
        else:
            accommodations_df.at[index, 'event_ids'] = ''

    # 업데이트된 숙소 데이터를 새로운 CSV 파일에 저장
    accommodations_df=accommodations_df.drop(columns=['longitude', 'latitude'])
    accommodations_df.to_csv('/tmp/Updated_Accommodations_with_Events.csv', index=False)
    print("이벤트 ID가 포함된 업데이트된 CSV 파일이 저장되었습니다.")
    
    
def create_schema_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'events_for_hotel'
    schema_name = 'hotel'
    
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    drop_table= f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
    
    # Define the table schema
    create_table_sql = f"""

    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        Google_Place_Id VARCHAR(512) PRIMARY KEY,
        HOTELNAME VARCHAR(512),
        EventID VARCHAR(65535)
    );

    """
    cursor.execute(drop_table)
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    
    
def update_table(**kwargs):
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel.events_for_hotel'
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    hotel_df=pd.read_csv('/tmp/Updated_Accommodations_with_Events.csv', usecols=(['place_id', 'name','event_ids']))
    hotel_df['place_id'] = hotel_df['place_id'].astype(str)
    print(hotel_df)
    
    for index, row in hotel_df.iterrows():
        # Check if the record exists
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE HOTELNAME = %s", (row['name'],))
        exists = cursor.fetchone()[0]
        
        if exists:
            # Update the existing record
            cursor.execute(f"""
                UPDATE {table_name}
                SET EventID = %s, Google_Place_Id = %s
                WHERE HOTELNAME = %s
            """, (row['event_ids'],row['place_id'],row['name']))
        else:
            # Insert the new record
            cursor.execute(f"""
                INSERT INTO {table_name} (EventID,HOTELNAME, Google_Place_Id)
                VALUES (%s, %s,%s)
            """, (row['event_ids'],row['name'], row['place_id']))
    
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'accommodations_with_events',
    default_args=default_args,
    description='숙소 반경 10km 내 이벤트 찾기 및 업데이트된 CSV 저장',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='find_events',
    python_callable=find_events,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_schema_table',
    python_callable=create_schema_table,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='update_table',
    python_callable=update_table,
    provide_context=True,
    dag=dag,
)
