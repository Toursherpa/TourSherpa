from django.shortcuts import render, get_object_or_404
from django.conf import settings
import csv
import pandas as pd
from django.http import HttpResponse
from .models import Event
from .models import TravelEvent
from .forms import EventFilterForm
from collections import OrderedDict
import urllib.parse
import requests

country_list = {'오스트리아': 'AT', '호주': 'AU', '캐나다': 'CA', '중국': 'CN', '독일': 'DE', '스페인': 'ES', '프랑스': 'FR', '영국': 'GB', '인도네시아': 'ID', '인도': 'IN', '이탈리아': 'IT', '일본': 'JP', '말레이시아': 'MY', '네덜란드': 'NL', '대만': 'TW', '미국': 'US'}


def dashboard(request):
    top_events = TravelEvent.objects.order_by('-Rank', '-PhqAttendance')[:3]

    # 정렬된 이벤트들을 템플릿에 전달
    context = {
        'top_events': top_events,
    }
    return render(request, 'maps/dashboard.html', context)


def country(request, country):
    country_code = country_list.get(country, '')

    # 해당 국가 코드와 일치하는 TravelEvent 객체들을 가져오기
    country_events = TravelEvent.objects.filter(Country=country_code).order_by('-Rank', '-PhqAttendance')

    # 정렬된 이벤트들을 템플릿에 전달
    context = {
        'country_events': country_events,
        'country': country,  # 템플릿에 국가 이름도 전달
    }
    return render(request, 'maps/country.html', context)

def event_detail(request, country, event_id):
    event = get_object_or_404(TravelEvent, EventID=event_id)
    context = {
        'event': event,
        'country': country,
    }
    return render(request, 'maps/event_detail.html', context)

def upload_csv(request):
    if request.method == 'POST':
        csv_file = request.FILES['file']
        if not csv_file.name.endswith('.csv'):
            return HttpResponse("This is not a CSV file.")
        
        # 판다스를 사용하여 CSV 파일 읽기
        data = pd.read_csv(csv_file)

        # 데이터베이스에 데이터 저장
        for index, row in data.iterrows():
            event = Event(
                name=row['Event Name'],
                date=row['Date'],
                category=row['Category'],
                location=row['Location'],
                city=row['City']
            )
            event.save()

        return HttpResponse("CSV file data has been uploaded to the database.")
    return render(request, 'upload_csv.html')

def google_map_view(request):
    form = EventFilterForm(request.GET or None)
    api_key = settings.GOOGLE_MAPS_API_KEY
    events = Event.objects.all()
    if form.is_valid():
        category = form.cleaned_data.get('category')
        city = form.cleaned_data.get('city')

        if category:
            events = events.filter(category=category)
        if city:
            events = events.filter(city__icontains=city)

    return render(request, 'maps/google_map.html', {'google_maps_api_key': api_key,'events': events,'form': form})

def get_places(api_key, location, radius, place_type):
    url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
    params = {
        'key': api_key,
        'location': location,
        'radius': radius,
        'type': place_type
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        results = response.json().get('results', [])
        return [{'name': place['name'], 'description': place.get('vicinity', '')} for place in results]
    else:
        return []

def event_details_view(request, name, location, date, category, city):
    decoded_location = urllib.parse.unquote(location)
    api_key = settings.GOOGLE_MAPS_API_KEY
    accommodations = get_places(api_key, location, 20000, 'lodging')
    restaurants = get_places(api_key, location, 20000, 'restaurant')
    
    context = {
        'name': urllib.parse.unquote(name),
        'location': decoded_location,
        'date': date,
        'category': category,
        'city': city,
        'google_maps_api_key': api_key,
        'accommodations': accommodations,
        'restaurants': restaurants
    }
    return render(request, 'maps/event_details.html', context)

def chart(request):

    # Chart data is passed to the `dataSource` parameter, as dictionary in the form of key-value pairs.
    dataSource = OrderedDict()

    # The `chartConfig` dict contains key-value pairs data for chart attribute
    chartConfig = OrderedDict()
    chartConfig["caption"] = "Countries With Most Oil Reserves [2017-18]"
    chartConfig["subCaption"] = "In MMbbl = One Million barrels"
    chartConfig["xAxisName"] = "Country"
    chartConfig["yAxisName"] = "Reserves (MMbbl)"
    chartConfig["numberSuffix"] = "K"
    chartConfig["theme"] = "fusion"

    # The `chartData` dict contains key-value pairs data
    chartData = OrderedDict()
    chartData["Venezuela"] = 290
    chartData["Saudi"] = 260
    chartData["Canada"] = 180
    chartData["Iran"] = 140
    chartData["Russia"] = 115
    chartData["UAE"] = 100
    chartData["US"] = 30
    chartData["China"] = 30


    dataSource["chart"] = chartConfig
    dataSource["data"] = []

    # Convert the data in the `chartData` array into a format that can be consumed by FusionCharts.
    # The data for the chart should be in an array wherein each element of the array is a JSON object
    # having the `label` and `value` as keys.

    # Iterate through the data in `chartData` and insert in to the `dataSource['data']` list.
    for key, value in chartData.items():
        data = {}
        data["label"] = key
        data["value"] = value
        dataSource["data"].append(data)


    # Create an object for the column 2D chart using the FusionCharts class constructor
    # The chart data is passed to the `dataSource` parameter.
    column2D = FusionCharts("column2d", "ex1" , "600", "400", "chart-1", "json", dataSource)

    return  render(request, 'index.html', {'output' : column2D.render(), 'chartTitle': 'Simple Chart Using Array'})
