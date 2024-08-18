from django.shortcuts import render, get_object_or_404
from django.conf import settings
import csv
import pandas as pd
from django.http import HttpResponse
from django_filters.views import FilterView
from django.db.models import Count, Avg
from .models import HotelsForEvent, EventsForHotel, TravelEvent, HotelList, FlightTo, FlightFrom, Airport, \
    NearestAirport,PlaceforEvent
from .forms import EventFilterForm
from collections import OrderedDict
from chartkick.django import ColumnChart, BarChart
import urllib.parse
import requests
import json
from json.decoder import JSONDecodeError
from datetime import datetime, timedelta
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import logging

country_list = {'오스트리아': 'AT', '호주': 'AU', '캐나다': 'CA', '중국': 'CN', '독일': 'DE', '스페인': 'ES', '프랑스': 'FR', '영국': 'GB',
                '인도네시아': 'ID', '인도': 'IN', '이탈리아': 'IT', '일본': 'JP', '말레이시아': 'MY', '네덜란드': 'NL', '대만': 'TW',
                '미국': 'US'}

#국가코드 데이터 국가명으로 변환
def translate_country(event_list, country_name_map):
    for event in event_list:
        event.Country = country_name_map.get(event.Country, event.Country)
        event.TimeStartFormatted = format_date(event.TimeStart)
        event.TimeEndFormatted = format_date(event.TimeEnd)

def dashboard(request):
    #가장 인기 있는 top_events 추출
    top_events = TravelEvent.objects.order_by('-Rank', '-PhqAttendance')[:3]
    #가장 일찍 열린 recent_events 추출
    recent_events = TravelEvent.objects.exclude(TimeStart='none').order_by('TimeStart')[:3]
    #가장 빨리 끝나는 earliest_end_events 추출
    earliest_end_events = TravelEvent.objects.exclude(TimeEnd='none').order_by('TimeEnd')[:3]
    countries = TravelEvent.objects.values('Country').annotate(total_events=Count('EventID')).order_by('-total_events')[:5]
    categories = TravelEvent.objects.values('Category').annotate(total_events=Count('EventID'))
    country_name_map = {code: name for name, code in country_list.items()}

    translate_country(top_events, country_name_map)
    translate_country(recent_events, country_name_map)
    translate_country(earliest_end_events, country_name_map)

    flights_data = get_average_price_per_country()

    #국가별 행사수 데이터
    countries_data = {
        'labels': [country_name_map.get(country['Country'], country['Country']) for country in countries],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(0, 128, 255, 0.2)', 
            'borderColor': 'rgba(0, 128, 255, 1)', 
            'data': [country['total_events'] for country in countries],
        }]
    }
    #카테고리별 행사수 데이터
    categories_data = {
        'labels': [category['Category'] for category in categories],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)', 
            'borderColor': 'rgba(255, 99, 132, 1)',  
            'data': [category['total_events'] for category in categories],
        }]
    }
    context = {
        'flights_data': flights_data,
        'categories_data': categories_data,
        'countries_data': countries_data,
        'top_events': top_events,
        'recent_events': recent_events,
        'earliest_end_events': earliest_end_events,
    }
    return render(request, 'maps/dashboard.html', context)

def search_results(request):
    query = request.GET.get('q', '')  # 검색어를 담는 파라미터 'q' 지정
    result_events = TravelEvent.objects.filter(Title__icontains=query).order_by('-Rank', '-PhqAttendance')
    return render(request, 'maps/search_result.html', {'result_events': result_events})

def country(request, country):
    country_code = country_list.get(country, '')
    queryset = TravelEvent.objects.filter(Country=country_code)
    #해당 국가 행사들 데이터 추출출
    country_events = TravelEvent.objects.filter(Country=country_code).order_by('-Rank', '-PhqAttendance')

    top_regions = queryset.values('Region').annotate(total_events=Count('EventID')).order_by('-total_events')[:5]
    categories = queryset.values('Category').annotate(total_events=Count('EventID'))

    #지역별 행사수 데이터
    regions_data = {
        'labels': [region['Region'] for region in top_regions],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)', 
            'borderColor': 'rgba(255, 99, 132, 1)', 
            'data': [region['total_events'] for region in top_regions],
        }]
    }
    #카테고리별 행사수 데이터
    categories_data = {
        'labels': [category['Category'] for category in categories],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)',
            'borderColor': 'rgba(255, 99, 132, 1)',
            'data': [category['total_events'] for category in categories],
        }]
    }
    context = {
        'country_events': country_events,
        'country': country,
        'categories_data': categories_data,
        'regions_data': regions_data,
    }
    return render(request, 'maps/country.html', context)


def event_detail(request, country, event_id):
    event = get_object_or_404(TravelEvent, EventID=event_id)
    hotels_data = get_object_or_404(HotelsForEvent, EventID=event_id)
    nearest_airport = get_object_or_404(NearestAirport, id=event_id)


    flight_to = ''
    flight_from = ''
    flight_state = ''

    if event.TimeStart != 'NaN':
        to_start_date = (datetime.strptime(event.TimeStart, "%Y-%m-%dT%H:%M:%S") - timedelta(days=3)).strftime('%Y-%m-%d')
        to_end_date = (datetime.strptime(event.TimeEnd, "%Y-%m-%dT%H:%M:%S") + timedelta(days=1)).strftime('%Y-%m-%d')
        from_start_date = (datetime.strptime(event.TimeStart, "%Y-%m-%dT%H:%M:%S")).strftime('%Y-%m-%d')
        from_end_date = (datetime.strptime(event.TimeEnd, "%Y-%m-%dT%H:%M:%S") + timedelta(days=4)).strftime('%Y-%m-%d')

        flight_to = FlightTo.objects.filter(departure_at__range=[to_start_date, to_end_date],
                                            arrival=nearest_airport.airport_code)
        flight_from = FlightFrom.objects.filter(departure_at__range=[from_start_date, from_end_date],
                                            departure=nearest_airport.airport_code)
    else:
        flight_state = " [아직 행사 일정이 정해지지 않았습니다!]"

    # Google_Place_Hotels를 쉼표로 구분된 문자열로 가정하고 리스트로 변환
    google_place_hotels = hotels_data.Google_Place_Hotels.split(',') if hotels_data.Google_Place_Hotels else ['None']

    # HotelList에 있는지 확인
    google_place_hotels_filtered = [hotel for hotel in google_place_hotels if '/' not in hotel]
    hotel_list = [hotel for hotel in HotelList.objects.filter(google_name__in=google_place_hotels_filtered)]


    # place data - cafe, Restaurant
    place_cafe_resturant = PlaceforEvent.objects.filter(event_id=event_id) 
    
    # 데이터 변환
    modified_places = []
    for place in place_cafe_resturant:
        modified_place = {
            'place_name': place.place_name,
            'types': place.types,
            'rating': place.rating,
            'address': place.address,
            'review': place.review,
            # number_of_reviews를 정수로 변환
            'number_of_reviews': int(place.number_of_reviews) if place.number_of_reviews else 0,
        }
        modified_places.append(modified_place)


    context = {
        'event': event,
        'country': country,
        'hotel_list': hotel_list,
        'nearest_airport': nearest_airport,
        'flight_to': flight_to,
        'flight_from': flight_from,
        'flight_state': flight_state,
        'place_cafe_resturant' : modified_places
    }
    return render(request, 'maps/event_detail.html', context)



logger = logging.getLogger('agoda')

def get_agoda_hotel_image(hotel_id, api_key, site_id):
    url = "http://affiliateapi7643.agoda.com/affiliateservice/lt_v1"
    
    headers = {
        "Authorization": f"{site_id}:{api_key}",
        "Accept-Encoding": "gzip,deflate",
        "Content-Type": "application/json"
    }
    try:
        hotel_id = int(float(hotel_id))  
    except ValueError:
        logger.error(f"Invalid hotel_id: {hotel_id} cannot be converted to integer.")
    data = {
        "criteria": {
            "additional": {
                "currency": "USD",
                "language": "en-us",
                "occupancy": {
                    "numberOfAdult": 2,
                    "numberOfChildren": 0
                }
            },
            "checkInDate": "2024-08-15",
            "checkOutDate": "2024-08-16",
            "hotelId": [hotel_id]
        }
    }
    
    logger.debug(f"Sending request to Agoda API: URL={url}, Data={data}, Headers={headers}")
    
    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 200:
        try:
            hotel_data = response.json()
            logger.debug(f"Received response from Agoda API: {hotel_data}")
            if "results" in hotel_data and len(hotel_data["results"]) > 0:
                image_url = hotel_data["results"][0].get("imageURL")
                logger.debug(f"Extracted image URL: {image_url}")
                return image_url
            else:
                logger.warning("No results found in the API response.")
                return None
        except ValueError as e:
            logger.error(f"Failed to decode JSON: {str(e)}")
            return None
    else:
        logger.error(f"API request failed with status code {response.status_code}: {response.text}")
        return None

def hotel_detail(request, hotel_name):
    hotel = get_object_or_404(HotelList, google_name=hotel_name)
    events_for_hotel = get_object_or_404(EventsForHotel, HOTELNAME=hotel_name)
    api_key = ''
    site_id = ''
    events = events_for_hotel.EventID.split(',') if events_for_hotel.EventID else ['None']
    event_list = [event for event in TravelEvent.objects.filter(EventID__in=events)]

    # Agoda API를 통해 호텔 이미지 가져오기
    agoda_image_url = get_agoda_hotel_image(hotel.agoda_hotel_id,api_key, site_id)

    context = {
        'hotel': hotel,
        'event_list': event_list,
        'image_url': agoda_image_url,  # 이미지 URL을 컨텍스트에 추가
    }
    return render(request, 'maps/hotel_detail.html', context)


@csrf_exempt
def check_availability(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        api_key = ""
        site_id = ''
        hotel_id = int(float(data['hotel_id']))  # 문자열을 정수로 변환하기 전에 float으로 변환
        check_in_date = data['check_in_date']
        check_out_date = data['check_out_date']

        result = check_hotel_availability(api_key, site_id, hotel_id, check_in_date, check_out_date)
        if isinstance(result, list) and len(result) > 0:
            landing_url = result[0].get('landingURL', '')
            return JsonResponse({'landingURL': landing_url, 'hotelId': hotel_id, 'check_in_date': check_in_date, 'check_out_date': check_out_date})
        else:
            return JsonResponse({
                'error': 'No available rooms found for the specified dates.',
                'hotelId': hotel_id,
                'check_in_date': check_in_date,
                'check_out_date': check_out_date,
                'result': result
            })
    return JsonResponse({'error': 'Invalid request method.'})    
#체크인/ 체크아웃 데이터를 받아 예약가능 여부 확인 및 상세정보    

def check_hotel_availability(api_key, site_id, hotel_id, check_in_date, check_out_date):
    url = "http://affiliateapi7643.agoda.com/affiliateservice/lt_v1"

    headers = {
        "Accept-Encoding": "gzip,deflate",
        "Authorization": f"{site_id}:{api_key}",
        "Content-Type": "application/json"  # Content-Type 헤더 추가
    }

    payload = {
        "criteria": {
            "additional": {
                "currency": "USD",
                "discountOnly": False,
                "language": "en-us",
                "occupancy": {
                    "numberOfAdult": 2,
                    "numberOfChildren": 0
                }
            },
            "checkInDate": check_in_date,
            "checkOutDate": check_out_date,
            "hotelId": [hotel_id]
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            return data["results"]
        else:
            return "No available rooms found for the specified dates."
    else:
        return f"Error: {response.status_code} - {response.text}"


def get_average_price_per_country():
    flight_to_avg_prices = FlightTo.objects.values('arrival').annotate(avg_price=Avg('price'))
    flight_from_avg_prices = FlightFrom.objects.values('departure').annotate(avg_price=Avg('price'))

    country_prices = {}

    # FlightTo 모델을 기반으로 가격 집계
    for flight_to in flight_to_avg_prices:
        airport = Airport.objects.filter(airport_code=flight_to['arrival']).first()
        if airport:
            country = airport.country_name
            if country not in country_prices:
                country_prices[country] = []
            country_prices[country].append(flight_to['avg_price'])

    # FlightFrom 모델을 기반으로 가격 집계
    for flight_from in flight_from_avg_prices:
        airport = Airport.objects.filter(airport_code=flight_from['departure']).first()
        if airport:
            country = airport.country_name
            if country not in country_prices:
                country_prices[country] = []
            country_prices[country].append(flight_from['avg_price'])

    # 각 국가별 평균 가격 계산
    avg_prices_per_country = {
        country: round(sum(prices) / len(prices), 2) for country, prices in country_prices.items() if prices
    }
    top_countries = sorted(avg_prices_per_country.items(), key=lambda item: item[1])[:5]
    top_countries_data = dict(top_countries)

    # 데이터 변환
    countries_data = {
        'labels': list(top_countries_data.keys()),
        'datasets': [{
            'label': 'Average Price',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)',  # 색상 예시, 원하는 대로 수정 가능
            'borderColor': 'rgba(255, 99, 132, 1)',
            'data': list(top_countries_data.values()),
        }]
    }

    return countries_data

