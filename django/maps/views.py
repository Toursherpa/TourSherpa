from django.shortcuts import render
from django.conf import settings
import csv
import pandas as pd
from django.http import HttpResponse
from .models import Event
from .forms import EventFilterForm
import urllib.parse
import requests

def dashboard(request):
    return render(request, 'maps/dashboard.html')
    
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
