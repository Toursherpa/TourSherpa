from django.shortcuts import render
from django.conf import settings
import csv
import pandas as pd
from django.http import HttpResponse
from .models import Event
from .forms import EventFilterForm

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


def event_details_view(request, name, location, date, category, city):
    context = {
        'name': urllib.parse.unquote(name),
        'location': urllib.parse.unquote(location),
        'date': date,
        'category': category,
        'city': city,
        'google_maps_api_key': settings.GOOGLE_MAPS_API_KEY
    }
    return render(request, 'maps/event_details.html', context)

