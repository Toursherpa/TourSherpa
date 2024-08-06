from django.urls import path, re_path
from . import views
from .views import upload_csv

urlpatterns = [

    path('dashboard/', views.dashboard, name='dashboard'),
    path('charts/', views.charts, name='charts'),
    path('tables/', views.tables, name='tables'),
    path('dashboard/<str:country>/<str:event_id>/', views.event_detail, name='event_detail'),
    path('dashboard/<str:country>/', views.country, name='country_Event'),


]

