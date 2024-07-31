from django.shortcuts import render
from django.conf import settings
import csv
import pandas as pd
from django.http import HttpResponse
from .models import Event
from .models import TravelEvent
from .forms import EventFilterForm
import urllib.parse
import requests

def dashboard(request):
    top_events = TravelEvent.objects.order_by('-Rank', '-PhqAttendance')[:3]

    # 정렬된 이벤트들을 템플릿에 전달
    context = {
        'top_events': top_events,
    }
    return render(request, 'maps/dashboard.html', context)



def country(request, country):
    # 'country' 파라미터를 사용합니다
    return render(request, 'maps/country.html', {'country': country})

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
