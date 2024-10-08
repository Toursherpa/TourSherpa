from django.urls import path, re_path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard, name='dashboard'),
    path('dashboard/<str:country>/<str:event_id>/', views.event_detail, name='event_detail'),
    path('hotel/<str:hotel_name>/', views.hotel_detail, name='hotel_detail'),
    path('dashboard/<str:country>/', views.country, name='country_Event'),
    path('search/', views.search_results, name='search_results'),
    path('check_availability/', views.check_availability, name='check_availability'),  # 추가된 경로
]


