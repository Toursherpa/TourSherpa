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

def read_csv_from_s3(bucket_name, file_key):
    s3_hook = S3Hook(aws_conn_id='s3_connection')

    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    file_content = file_obj.get()['Body'].read().decode('utf-8')

    return pd.read_csv(StringIO(file_content))

def fetch_flight_date(startdelta, enddelta, today):
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

    return airport_dict

def fetch_airline_data():
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

    return pd.DataFrame(airline_list)

def euro_data():
    url = "https://m.stock.naver.com/marketindex/exchange/FX_EURKRW"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    euro = float(soup.find_all("strong")[1].text[: -3].replace(",", ""))

    return euro

def fetch_flight_data(airport_dict, airline_df, euro, check):
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
                    else:

                    time.sleep(1)

                    break
                except ResponseError as error:
                    print(error)

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

    return df

def fetch_airport_data():
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
        {'airport_code': 'NRT', 'airport_name': '나리타 국제공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'KIX', 'airport_name': '간사이 국제공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'NGO', 'airport_name': '츄부국제공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'FUK', 'airport_name': '후쿠오카 공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'CTS', 'airport_name': '신치토세 공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'OKA', 'airport_name': '나하 공항', 'airport_location': [], 'country_code': 'JP', 'country_name': '일본'},
        {'airport_code': 'PEK', 'airport_name': '베이징 서우두 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'PVG', 'airport_name': '상하이 푸둥 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'CAN', 'airport_name': '광저우 바이윈 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'CKG', 'airport_name': '충칭 장베이 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'HRB', 'airport_name': '하얼빈 타이핑 국제공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'HKG', 'airport_name': '홍콩 국제 공항', 'airport_location': [], 'country_code': 'CN', 'country_name': '중국'},
        {'airport_code': 'CGK', 'airport_name': '수카르노 하타 국제공항', 'airport_location': [], 'country_code': 'ID', 'country_name': '인도네시아'},
        {'airport_code': 'DEL', 'airport_name': '인디라 간디 국제공항', 'airport_location': [], 'country_code': 'IN', 'country_name': '인도'},
        {'airport_code': 'TPE', 'airport_name': '타이완 타오위안 국제공항', 'airport_location': [], 'country_code': 'TW', 'country_name': '대만'},
        {'airport_code': 'KHH', 'airport_name': '가오슝 국제공항', 'airport_location': [], 'country_code': 'TW', 'country_name': '대만'},
        {'airport_code': 'KUL', 'airport_name': '쿠알라룸푸르 국제공항', 'airport_location': [], 'country_code': 'MY', 'country_name': '말레이시아'},
        {'airport_code': 'BKI', 'airport_name': '코타키나발루 국제공항', 'airport_location': [], 'country_code': 'MY', 'country_name': '말레이시아'},
        {'airport_code': 'FRA', 'airport_name': '프랑크푸르트 암마인 공항', 'airport_location': [], 'country_code': 'DE', 'country_name': '독일'},
        {'airport_code': 'MAD', 'airport_name': '마드리드 바라하스 국제공항', 'airport_location': [], 'country_code': 'ES', 'country_name': '스페인'},
        {'airport_code': 'CDG', 'airport_name': '파리 샤를드골 국제공항', 'airport_location': [], 'country_code': 'FR', 'country_name': '프랑스'},
        {'airport_code': 'LHR', 'airport_name': '히스로 공항', 'airport_location': [], 'country_code': 'GB', 'country_name': '영국'},
        {'airport_code': 'FCO', 'airport_name': '로마 피우미치노 레오나르도 다 빈치 공항', 'airport_location': [], 'country_code': 'IT', 'country_name': '이탈리아'},
        {'airport_code': 'VIE', 'airport_name': '빈 국제공항', 'airport_location': [], 'country_code': 'AT', 'country_name': '오스트리아'},
        {'airport_code': 'AMS', 'airport_name': '암스테르담 스키폴 국제공항', 'airport_location': [], 'country_code': 'NL', 'country_name': '네덜란드'},
        {'airport_code': 'HNL', 'airport_name': '호놀룰루 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'SEA', 'airport_name': '시애틀 터코마 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'LAX', 'airport_name': '로스앤젤레스 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'ORD', 'airport_name': '시카고 오헤어 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'DFW', 'airport_name': '달라스 / 포트워스 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'JFK', 'airport_name': '존 F. 케네디 국제공항', 'airport_location': [], 'country_code': 'US', 'country_name': '미국'},
        {'airport_code': 'SYD', 'airport_name': '시드니 인터내셔널 에어포트', 'airport_location': [], 'country_code': 'AU', 'country_name': '호주'},
        {'airport_code': 'BNE', 'airport_name': '브리즈번 공항', 'airport_location': [], 'country_code': 'AU', 'country_name': '호주'},
        {'airport_code': 'YVR', 'airport_name': '밴쿠버 국제공항', 'airport_location': [], 'country_code': 'CA', 'country_name': '캐나다'},
        {'airport_code': 'YYZ', 'airport_name': '토론토 피어슨 국제공항', 'airport_location': [], 'country_code': 'CA', 'country_name': '캐나다'}
    ]

    for i, v in enumerate(airport_list):
        address = v['airport_name']

        lat, lng = get_lat_lng(address)
        airport_list[i]['airport_location'] = [lat, lng]

    return pd.DataFrame(airport_list)

def upload_to_s3(df, filename):
    csv_filename = f'/tmp/{filename}.csv'
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_bucket_name = Variable.get('s3_bucket_name')
    s3_result_key = f'source/source_flight/{filename}.csv'
    s3_hook.load_file(filename=csv_filename, key=s3_result_key, bucket_name=s3_bucket_name, replace=True)