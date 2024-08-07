from django.shortcuts import render, get_object_or_404
from django.conf import settings
import csv
import pandas as pd
from django.http import HttpResponse
from django_filters.views import FilterView
from django.db.models import Count
from .models import HotelsForEvent, EventsForHotel, TravelEvent, HotelList, FlightTo, FlightFrom, Airline, Airport, NearestAirport
from .forms import EventFilterForm
from collections import OrderedDict
from chartkick.django import ColumnChart, BarChart
import urllib.parse
import requests
import json
from json.decoder import JSONDecodeError
from datetime import datetime, timedelta

country_list = {'오스트리아': 'AT', '호주': 'AU', '캐나다': 'CA', '중국': 'CN', '독일': 'DE', '스페인': 'ES', '프랑스': 'FR', '영국': 'GB', '인도네시아': 'ID', '인도': 'IN', '이탈리아': 'IT', '일본': 'JP', '말레이시아': 'MY', '네덜란드': 'NL', '대만': 'TW', '미국': 'US'}



def dashboard(request):
    top_events = TravelEvent.objects.order_by('-Rank', '-PhqAttendance')[:3]
    countries = TravelEvent.objects.values('Country').annotate(total_events=Count('EventID')).order_by('-total_events')[:5]
    categories = TravelEvent.objects.values('Category').annotate(total_events=Count('EventID'))
    recent_events = TravelEvent.objects.exclude(TimeStart='none').order_by('TimeStart')[:3]
    earliest_end_events = TravelEvent.objects.exclude(TimeEnd='none').order_by('TimeEnd')[:3]

    countries_data = {
        'labels': [country['Country'] for country in countries],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(0, 128, 255, 0.2)',  # 색상 예시, 원하는 대로 수정 가능
            'borderColor': 'rgba(0, 128, 255, 1)',      # 색상 예시, 원하는 대로 수정 가능
            'data': [country['total_events'] for country in countries],
        }]
    }
    # BarChart 생성
    categories_data = {
        'labels': [category['Category'] for category in categories],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)',  # 색상 예시, 원하는 대로 수정 가능
            'borderColor': 'rgba(255, 99, 132, 1)',      # 색상 예시, 원하는 대로 수정 가능
            'data': [category['total_events'] for category in categories],
        }]
    }
    context = {
        'categories_data': categories_data,
        'countries_data': countries_data,
        'top_events': top_events,
        'recent_events': recent_events,
        'earliest_end_events': earliest_end_events,
    }
    return render(request, 'maps/dashboard.html', context)

def country(request, country):
    country_code = country_list.get(country, '')
    queryset = TravelEvent.objects.filter(Country=country_code)

    country_events = TravelEvent.objects.filter(Country=country_code).order_by('-Rank', '-PhqAttendance')

    top_regions = queryset.values('Region').annotate(total_events=Count('EventID')).order_by('-total_events')[:5]
    categories = queryset.values('Category').annotate(total_events=Count('EventID'))

    regions_data = {
        'labels': [region['Region'] for region in top_regions],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)',  # 색상 예시, 원하는 대로 수정 가능
            'borderColor': 'rgba(255, 99, 132, 1)',      # 색상 예시, 원하는 대로 수정 가능
            'data': [region['total_events'] for region in top_regions],
        }]
    }
    categories_data = {
        'labels': [category['Category'] for category in categories],
        'datasets': [{
            'label': 'Number of Events',
            'backgroundColor': 'rgba(255, 99, 132, 0.2)',  # 색상 예시, 원하는 대로 수정 가능
            'borderColor': 'rgba(255, 99, 132, 1)',      # 색상 예시, 원하는 대로 수정 가능
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

    date_obj = datetime.strptime(event.TimeStart, "%Y-%m-%dT%H:%M:%S")
    
    end_date = date_obj.strftime('%Y-%m-%d')
    start_date = (date_obj - timedelta(days=3)).strftime('%Y-%m-%d')

    flight_to = FlightTo.objects.filter(departure_at__range=[start_date, end_date], arrival=nearest_airport.airport_code)

    # Google_Place_Hotels를 쉼표로 구분된 문자열로 가정하고 리스트로 변환
    google_place_hotels = hotels_data.Google_Place_Hotels.split(',') if hotels_data.Google_Place_Hotels else ['None']

    # HotelList에 있는지 확인
    google_place_hotels_filtered = [hotel for hotel in google_place_hotels if '/' not in hotel]
    hotel_list = [hotel for hotel in HotelList.objects.filter(google_name__in=google_place_hotels_filtered)]

    context = {
        'event': event,
        'country': country,
        'hotel_list': hotel_list,
        'flight_to': flight_to,
        'd': start_date
    }
    return render(request, 'maps/event_detail.html', context)
    
def hotel_detail(request, hotel_name):
    hotel = get_object_or_404(HotelList, google_name=hotel_name)
    
    context = {
        'hotel': hotel,
        #'event_date': event_date,
    }
    return render(request, 'maps/hotel_detail.html', context)
    

