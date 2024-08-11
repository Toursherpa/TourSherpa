from datetime import datetime, timedelta
import pandas as pd
from geopy.distance import great_circle
import os
import numpy as np
import stat
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.task_group import TaskGroup
import logging
today_date = datetime.utcnow().strftime('%Y-%m-%d')

def preprocess_text(text):
    """문자열 전처리 함수"""
    if isinstance(text, str):
        return text.lower().strip()
    return str(text).strip()  # 문자열이 아닌 경우에도 처리 가능하도록

def download_files():
    """S3에서 파일 다운로드"""
    print("S3에서 파일을 확인하고 필요한 경우 다운로드합니다...")
    hook = S3Hook(aws_conn_id='s3_connection')
    bucket_name = 'team-hori-2-bucket'
    google_hotels_key = 'source/source_TravelEvents/google_hotels.csv'
    hotel_list_key = 'source/source_TravelEvents/hotel_list.csv'
    events_key = 'source/source_TravelEvents/TravelEvents.csv'

    # 오늘 날짜의 디렉터리 생성
    local_dir = f'/tmp/{today_date}'
    os.makedirs(local_dir, exist_ok=True)
    print(f"{local_dir} 디렉터리를 생성했습니다.")

    # 로컬 파일 경로 설정
    local_google_hotels_path = os.path.join(local_dir, 'google_hotels.csv')
    local_hotel_list_path = os.path.join(local_dir, 'hotel_list.csv')
    local_events_path = os.path.join(local_dir, 'TravelEvents.csv')

    # 파일이 없거나 손상된 경우 다운로드
    def download_if_needed(local_path, s3_key=None):
        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            if s3_key:
                print(f"S3에서 {local_path} 파일을 다운로드 중...")
                s3_object = hook.get_key(s3_key, bucket_name)
                content = s3_object.get()['Body'].read().decode('utf-8')
                with open(local_path, 'w') as f:
                    f.write(content)
            # 파일 권한 설정
            os.chmod(local_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
            print(f"{local_path} 파일이 성공적으로 다운로드되었습니다.")
        else:
            print(f"{local_path} 파일이 이미 존재하며 비어있지 않습니다.")

    download_if_needed(local_google_hotels_path, s3_key=google_hotels_key)
    download_if_needed(local_hotel_list_path, s3_key=hotel_list_key)
    download_if_needed(local_events_path, s3_key=events_key)

def calculate_distance(location1, location2):
    """두 위치 간의 거리 계산 (킬로미터 단위)"""
    distance = great_circle(location1, location2).kilometers
    print(f"두 위치 간의 거리를 계산했습니다: {distance} km")
    return distance

def parse_location(location_str):
    """위치 문자열을 튜플로 변환 (위도, 경도)"""
    try:
        location = eval(location_str)
        if isinstance(location, list) and len(location) == 2:
            parsed_location = (float(location[1]), float(location[0]))  # (위도, 경도) 순서로 변환
            print(f"위치 문자열을 변환했습니다: {location_str} -> {parsed_location}")
            return parsed_location
    except:
        print(f"위치 문자열 변환 실패: {location_str}")
        return None
    return None

def parse_location_from_lat_lon(latitude, longitude):
    """위도와 경도에서 위치 튜플 생성"""
    try:
        location = (float(latitude), float(longitude))  # (위도, 경도) 순서
        print(f"위도와 경도에서 위치 튜플을 생성했습니다: ({latitude}, {longitude}) -> {location}")
        return location
    except:
        print(f"위치 튜플 생성 실패: 위도={latitude}, 경도={longitude}")
        return None

def exact_match(row, hotel_chunk_df):
    """정확한 일치 검색 및 위치 정보 확인"""
    hotel_chunk_df['name_normalized'] = hotel_chunk_df['hotel_name'].apply(preprocess_text)
    row['name_normalized'] = preprocess_text(row['name'])
    
    # 문자열이 아닌 값이 있는 경우 원래 데이터를 반환
    if not isinstance(row['name_normalized'], str):
        print(f"이름 필드가 유효하지 않아 원본 행을 반환합니다: {row['name']}")
        return row
    
    matching_hotels = hotel_chunk_df[hotel_chunk_df['name_normalized'] == row['name_normalized']]
    
    if not matching_hotels.empty:
        row_location = parse_location(row['location'])
        if row_location is None:
            print(f"위치 정보를 파싱할 수 없어 원본 행을 반환합니다: {row['location']}")
            return row
        for _, match in matching_hotels.iterrows():
            match_location = parse_location_from_lat_lon(match['latitude'], match['longitude'])
            if match_location is None:
                continue
            distance = calculate_distance(row_location, match_location)
            if distance <= 2:  # 2km 이내
                print(f"매칭된 호텔을 찾았습니다: {match['hotel_name']} (거리: {distance} km)")
                match_row = row.to_dict()
                match_row.update(match.to_dict())
                return match_row  # 매칭된 호텔의 모든 정보를 추가하여 반환
    print(f"매칭된 호텔을 찾지 못했습니다: {row['name']}")
    return row.to_dict()

def process_chunk(hotel_chunk_df_path, google_hotels_df_path, chunk_index, total_chunks):
    """청크를 처리하여 호텔 ID 및 기타 정보를 매칭"""
    print(f"{chunk_index + 1}/{total_chunks} 청크를 처리합니다...")
    try:
        google_hotels_df = pd.read_csv(google_hotels_df_path)
    except:
        google_hotels_df = pd.DataFrame()
    hotel_chunk_df = pd.read_csv(hotel_chunk_df_path)
    
    result_list = []
    total_rows = len(google_hotels_df)
    for i, (_, row) in enumerate(google_hotels_df.iterrows()):
        match = exact_match(row, hotel_chunk_df)
        result_list.append(match)
        
        if i % 10 == 0:  # 진행 상황을 10개의 행마다 로그에 출력
            print(f"청크 {chunk_index+1}/{total_chunks}: {i+1}/{total_rows} 행을 처리했습니다.")
    
    result_df = pd.DataFrame(result_list)
    result_df.to_csv(f'/tmp/{today_date}/processed_chunk_{chunk_index}.csv', index=False)
    os.chmod(f'/tmp/{today_date}/processed_chunk_{chunk_index}.csv', stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
    print(f"{chunk_index + 1}/{total_chunks} 청크 처리가 완료되었습니다.")

def process_hotels():
    """숙소 데이터를 처리하여 호텔 ID를 매칭"""
    print("Google 호텔 데이터를 처리합니다...")
    google_hotels_path = f'/tmp/{today_date}/google_hotels.csv'
    hotel_list_path = f'/tmp/{today_date}/hotel_list.csv'
    try:
        google_hotels_df = pd.read_csv(google_hotels_path)
    except:
        google_hotels_df = pd.DataFrame()
    hotel_list_df = pd.read_csv(hotel_list_path, dtype=str)

    print("데이터 프레임이 로드되었습니다. 매칭 프로세스를 시작합니다...")

    # hotel_list_df를 청크로 나누고 병렬 처리
    num_chunks = 50  # 병렬 처리할 청크 수
    hotel_chunks = np.array_split(hotel_list_df, num_chunks)

    # 각 청크를 파일로 저장
    chunk_paths = []
    for i, chunk in enumerate(hotel_chunks):
        chunk_path = f'/tmp/{today_date}/hotel_chunk_{i}.csv'
        chunk.to_csv(chunk_path, index=False)
        os.chmod(chunk_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
        chunk_paths.append(chunk_path)
        print(f"{i+1}/{num_chunks} 청크가 파일로 저장되었습니다: {chunk_path}")

    total_chunks = len(hotel_chunks)
    print(f"총 {total_chunks} 개의 청크로 분할되었습니다.")
    return total_chunks, chunk_paths

def merge_chunks(total_chunks):
    """병렬 처리된 청크들을 병합"""
    print(f"{total_chunks} 개의 청크를 병합합니다...")
    processed_chunks = []
    
    for i in range(total_chunks):
        file_path = f'/tmp/{today_date}/processed_chunk_{i}.csv'
        try:
            if os.path.getsize(file_path) > 0:
                df = pd.read_csv(file_path)
                processed_chunks.append(df)
            else:
                logging.warning(f"{file_path} 파일이 비어 있어 건너뜁니다.")
        except pd.errors.EmptyDataError:
            logging.warning(f"{file_path} 파일을 읽을 수 없어 건너뜁니다.")

    # 병합 작업
    try:
        if processed_chunks:
            combined_df = pd.concat(processed_chunks, ignore_index=True)
            processed_df = combined_df.drop_duplicates(subset=['place_id'], keep='first')
        else:
            processed_df = pd.DataFrame()

        # 병합된 데이터 저장
        output_file_path = f'/tmp/{today_date}/Updated_hotels.csv'
        processed_df.to_csv(output_file_path, index=False)
        os.chmod(output_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
        print("병합이 완료되었습니다. 최종 데이터가 Updated_hotels.csv 파일에 저장되었습니다.")
        
    except Exception as e:
        logging.error(f"병합 작업 중 오류 발생: {e}")
        processed_df = pd.DataFrame()

    # 청크 파일 삭제
    for i in range(total_chunks):
        processed_chunk_path = f'/tmp/{today_date}/processed_chunk_{i}.csv'
        hotel_chunk_path = f'/tmp/{today_date}/hotel_chunk_{i}.csv'
        try:
            os.remove(processed_chunk_path)
            print(f"{processed_chunk_path} 파일이 삭제되었습니다.")
        except Exception as e:
            logging.warning(f"{processed_chunk_path} 파일을 삭제하는 중 오류 발생: {e}")
        try:
            os.remove(hotel_chunk_path)
            print(f"{hotel_chunk_path} 파일이 삭제되었습니다.")
        except Exception as e:
            logging.warning(f"{hotel_chunk_path} 파일을 삭제하는 중 오류 발생: {e}")
def upload_file():
    """처리된 파일을 S3에 업로드"""
    print("S3에 처리된 파일을 업로드합니다...")
    hook = S3Hook(aws_conn_id='s3_connection')
    bucket_name = 'team-hori-2-bucket'
    output_key = f'source/source_TravelEvents/{today_date}/Updated_hotels.csv'
    
    local_file_path = f'/tmp/{today_date}/Updated_hotels.csv'
    
    attempt = 0
    while attempt < 3:
        try:
            hook.load_file(
                filename=local_file_path,
                key=output_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"파일이 S3에 업로드되었습니다: {output_key}")
            return
        except Exception as e:
            print(f"파일 업로드 중 오류 발생: {e}. 재시도 중... ({attempt + 1}/3)")
            attempt += 1
    raise RuntimeError("여러 번 시도 후에도 파일 업로드에 실패했습니다.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 24),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hotel_append_agoda_data',
    default_args=default_args,
    schedule_interval=timedelta(days=5),
    catchup=False,
)

t1 = PythonOperator(
    task_id='download_files',
    python_callable=download_files,
    dag=dag,
)

def create_processing_tasks(total_chunks, chunk_paths):
    with TaskGroup("process_chunks", dag=dag) as process_chunks:
        for i in range(total_chunks):
            PythonOperator(
                task_id=f'process_chunk_{i}',
                python_callable=process_chunk,
                op_args=[chunk_paths[i], f'/tmp/{today_date}/google_hotels.csv', i, total_chunks],
                dag=dag,
            )
    return process_chunks

t2 = PythonOperator(
    task_id='process_hotels',
    python_callable=process_hotels,
    dag=dag,
)

t3 = PythonOperator(
    task_id='merge_chunks',
    python_callable=merge_chunks,
    op_args=[50],  # Assuming 50 chunks
    dag=dag,
)

t4 = PythonOperator(
    task_id='upload_file',
    python_callable=upload_file,
    dag=dag,
)

# 다운로드 -> 청크 생성 및 병렬 처리 태스크 생성 -> 병합 -> 업로드
t1 >> t2 >> create_processing_tasks(50, [f'/tmp/{today_date}/hotel_chunk_{i}.csv' for i in range(50)]) >> t3 >> t4