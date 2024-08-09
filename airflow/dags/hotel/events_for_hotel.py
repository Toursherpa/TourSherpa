from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

today_date = datetime.utcnow().strftime('%Y-%m-%d')

# Function to calculate the distance between two coordinates
def calculate_distance(lon1, lat1, lon2, lat2):
    return ((lon2 - lon1)**2 + (lat2 - lat1)**2)**0.5

# Function to find events within a 10km radius of hotels
def find_events():
    print("Starting to find events within a 10km radius...")

    # Load hotel data
    hotels_df = pd.read_csv(f'/tmp/{today_date}/Updated_hotels.csv', usecols=['name', 'place_id', 'location'])
    events_df = pd.read_csv(f'/tmp/{today_date}/TravelEvents.csv')

    # Add a new column for event IDs in the hotel dataframe
    hotels_df['event_ids'] = ''

    # Parse location data in hotel and event dataframes
    hotels_df['longitude'] = hotels_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    hotels_df['latitude'] = hotels_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))
    events_df['longitude'] = events_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[0]))
    events_df['latitude'] = events_df['location'].apply(lambda x: float(x.strip('[]').split(', ')[1]))

    # Check if 'id' column exists in event dataframe
    if 'id' not in events_df.columns:
        raise KeyError("The 'id' column is missing in TravelEvents.csv. Please ensure the correct column name is used.")

    # Find events within a 10km radius for each hotel
    for index, accommodation in hotels_df.iterrows():
        nearby_events = [
            event['id'] for _, event in events_df.iterrows()
            if calculate_distance(accommodation['longitude'], accommodation['latitude'], event['longitude'], event['latitude']) <= 0.1
        ]
        hotels_df.at[index, 'event_ids'] = ','.join(map(str, nearby_events)) if nearby_events else ''

    # Save the updated hotel data to a new CSV file
    updated_file_path = f'/tmp/{today_date}/Updated_hotels_with_Events.csv'
    hotels_df.drop(columns=['longitude', 'latitude'], inplace=True)
    hotels_df.to_csv(updated_file_path, index=False)
    print("Updated CSV file with event IDs saved.")

    # Upload the file to S3
    upload_to_s3(updated_file_path, 'team-hori-2-bucket', f'source/source_TravelEvents/{today_date}/Updated_hotels_with_Events.csv')

def upload_to_s3(file_path, bucket_name, s3_key):
    """Upload a file to S3."""
    hook = S3Hook(aws_conn_id='s3_connection')
    hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"File uploaded to S3 at {s3_key}")

# Function to create the schema and table in Redshift
def create_schema_table():
    redshift_conn_id = 'redshift_connection'
    table_name = 'events_for_hotel'
    schema_name = 'hotel'
    
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    # Define the table schema and drop if exists
    create_table_sql = f"""
    DROP TABLE IF EXISTS {schema_name}.{table_name};
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        Google_Place_Id VARCHAR(512) PRIMARY KEY,
        HOTELNAME VARCHAR(512),
        EventID VARCHAR(65535)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Function to update the table in Redshift with new data
def update_table():
    redshift_conn_id = 'redshift_connection'
    table_name = 'hotel.events_for_hotel'
    
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    hotel_df = pd.read_csv(f'/tmp/{today_date}/Updated_hotels_with_Events.csv', usecols=['place_id', 'name', 'event_ids'])
    hotel_df['place_id'] = hotel_df['place_id'].astype(str)
    
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
            """, (row['event_ids'], row['place_id'], row['name']))
        else:
            # Insert the new record
            cursor.execute(f"""
                INSERT INTO {table_name} (EventID, HOTELNAME, Google_Place_Id)
                VALUES (%s, %s, %s)
            """, (row['event_ids'], row['name'], row['place_id']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Default arguments for the DAG
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

# Define the DAG
dag = DAG(
    'hotels_with_events',
    default_args=default_args,
    description='Find events within a 10km radius of hotels and update CSV and Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the tasks
t1 = PythonOperator(
    task_id='find_events',
    python_callable=find_events,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_schema_table',
    python_callable=create_schema_table,
    dag=dag,
)

t3 = PythonOperator(
    task_id='update_table',
    python_callable=update_table,
    dag=dag,
)

# Set the task dependencies
t1 >> t2 >> t3