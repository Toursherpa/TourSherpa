import requests
from django.conf import settings


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
api_key = 'AIzaSyBtisA7MOZ4fM248MoHSqBMbz5M4m_hggY'
location = "37.5665,126.9780"  # 서울의 예시 좌표
radius = 1500  # 반경 1500미터
place_type = "restaurant"  # 'restaurant' 유형의 장소 검색

# 함수 호출
places = get_places(api_key, location, radius, place_type)
print(places)
