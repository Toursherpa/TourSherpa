from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
import requests
import re
import pytz
import time
from amadeus import Client, ResponseError
from airflow.models import Variable
import logging

def read_csv_from_s3(bucket_name, file_key):
    logging.info("S3에서 CSV 파일을 읽는 중...")
    s3_hook = S3Hook(aws_conn_id='s3_connection')

    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    file_content = file_obj.get()['Body'].read().decode('utf-8')

    logging.info("CSV 파일 읽기 완료")
    return pd.read_csv(StringIO(file_content))

def fetch_flight_date(startdelta, enddelta, today):
    logging.info("항공편 날짜 데이터를 가져오는 중...")
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    bucket_name = Variable.get('s3_bucket_name')
    file_key = 'source/source_flight/nearest_airports.csv'

    df = read_csv_from_s3(bucket_name, file_key)

    airport_dict = {}

    min_date = datetime.strptime(today, '%Y-%m-%d')

    for _, row in df.iterrows():
        airport_code = row['airport_code']

        if str(row['start_date']) != 'nan':
            start_date = datetime.strptime(str(row['start_date']), '%Y-%m-%d') + timedelta(days=startdelta)
            end_date = datetime.strptime(str(row['end_date']), '%Y-%m-%d') + timedelta(days=enddelta)

            if start_date < min_date:
                start_date = min_date
        else:
            continue

        delta = end_date - start_date

        if airport_code not in airport_dict:
            airport_dict[airport_code] = set()

        for i in range(delta.days + 1):
            date = start_date + timedelta(days=i)
            airport_dict[airport_code].add(date.strftime('%Y-%m-%d'))

    logging.info("항공편 날짜 데이터 가져오기 완료")
    return airport_dict

def fetch_airline_data():
    logging.info("항공사 데이터를 가져오는 중...")
    url = "https://www.airport.kr/ap/ko/dep/apAirlinesList.do"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    airline_list = []

    for i in soup.find_all("tr")[16: ]:
        info_dict = dict()

        info_dict['airline_code'] = re.sub(r'[\n\r\t ]', '', i.find_all("td")[4].text)
        info_dict['airline_name'] = re.sub(r'[\n\r\t ]', '', i.find_all("td")[0].text)

        airline_list.append(info_dict)

    airline_list.append({'airline_code': 'H1', 'airline_name': '한에어'})
    airline_list.append({'airline_code': '6X', 'airline_name': '에어오디샤'})
    airline_list.append({'airline_code': 'UX', 'airline_name': '에어유로파'})
    airline_list.append({'airline_code': 'AZ', 'airline_name': '알리탈리아'})
    airline_list.append({'airline_code': 'VS', 'airline_name': '버진애틀랜틱항공'})

    logging.info("항공사 데이터 가져오기 완료")
    return pd.DataFrame(airline_list)

def euro_data():
    logging.info("유로 환율 데이터를 가져오는 중...")
    url = "https://m.stock.naver.com/marketindex/exchange/FX_EURKRW"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    euro = float(soup.find_all("strong")[1].text[: -3].replace(",", ""))

    logging.info(f"유로 환율: {euro}")
    return euro

def fetch_flight_data(airport_dict, airline_df, euro, check):
    logging.info("항공편 데이터를 Amadeus API에서 가져오는 중...")
    amadeus = Client(
        client_id = Variable.get("amadeus_id"),
        client_secret = Variable.get("amadeus_secret")
    )
    
    response_list = []
    count = 0

    for i in airport_dict:
        if i not in ('NRT', 'KIX', 'NGO', 'FUK', 'CTS', 'OKA'):
            continue
            
        date_list = sorted(list(airport_dict[i]))

        for j in date_list:
            count += 1

            if check == 0:
                location = ['ICN', i]
            else:
                location = [i, 'ICN']

            while True:
                try:
                    response = amadeus.shopping.flight_offers_search.get(
                        originLocationCode=location[0],
                        destinationLocationCode=location[1],
                        departureDate=j,
                        adults=1,
                        nonStop='true'
                    )

                    if response.data:
                        response_list.append(response.data)

                    time.sleep(1)

                    break
                except ResponseError as error:
                    logging.error(f"Amadeus API 오류: {error}")

                    time.sleep(2)

    flight_list = []

    for i in response_list:
        for j in i:
            info_dict = dict()
        
            info_dict['airline_code'] = j['itineraries'][0]['segments'][0]['carrierCode']
            info_dict['departure'] = j['itineraries'][0]['segments'][0]['departure']['iataCode']
            info_dict['departure_at'] = j['itineraries'][0]['segments'][0]['departure']['at']
            info_dict['arrival'] = j['itineraries'][0]['segments'][0]['arrival']['iataCode']
            info_dict['arrival_at'] = j['itineraries'][0]['segments'][0]['arrival']['at']
            info_dict['duration'] = j['itineraries'][0]['segments'][0]['duration'][2: ].replace("H", "시간 ").replace("M", "분")
            info_dict['seats'] = j['numberOfBookableSeats']
            info_dict['price'] = j['price']['total']
            
            flight_list.append(info_dict)

    flight_df = pd.DataFrame(flight_list)
    df = pd.merge(flight_df, airline_df, on='airline_code', how='inner')

    def date_change(value):
        date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return date.strftime('%y%m%d')

    def min_change(value):
        date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return int(date.strftime('%H')) * 60 + int(date.strftime('%M'))

    departure_date = []
    departure_min = []
    price_won = []

    for i in range(len(df)):
        departure_date.append(date_change(df['departure_at'][i]))
        departure_min.append(min_change(df['departure_at'][i]))
        price_won.append(int(float(df['price'][i]) * euro))

    df['departure_date'] = departure_date
    df['departure_min'] = departure_min
    df['price'] = price_won

    logging.info("항공편 데이터 처리 완료")
    return df

def fetch_airport_data():
    logging.info("공항 데이터를 가져오는 중...")
    def get_lat_lng(address):
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": Variable.get('GOOGLE_API_KEY')
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                location = results[0]["geometry"]["location"]
                return location["lat"], location["lng"]
        return None

    airport_list = [
        # ... (공항 리스트는 생략했습니다)
    ]

    for i, v in enumerate(airport_list):
        address = v['airport_name']

        lat, lng = get_lat_lng(address)
        airport_list[i]['airport_location'] = [lat, lng]

    logging.info("공항 데이터 가져오기 완료")
    return pd.DataFrame(airport_list)

def upload_to_s3(df, filename):
    logging.info(f"S3에 {filename} 데이터를 업로드하는 중...")
    csv_filename = f'/tmp/{filename}.csv'
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_bucket_name = Variable.get('s3_bucket_name')
    s3_result_key = f'source/source_flight/{filename}.csv'
    s3_hook.load_file(filename=csv_filename, key=s3_result_key, bucket_name=s3_bucket_name, replace=True)

    logging.info(f"S3에 {filename} 데이터 업로드 완료")