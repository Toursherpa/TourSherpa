from django.urls import path, re_path
from . import views
from .views import upload_csv

urlpatterns = [
    path('google-map/', views.google_map_view, name='google_map_view'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('charts/', views.charts, name='charts'),
    path('tables/', views.tables, name='tables'),
    path('dashboard/<str:country>/<str:event_id>/', views.event_detail, name='event_detail'),
    path('dashboard/<str:country>/', views.country, name='country_Event'),
    path('upload_csv/', upload_csv, name='upload_csv'),
    re_path(r'^event/(?P<name>.+)/(?P<location>.+)/(?P<date>.+)/(?P<category>.+)/(?P<city>.+)/$', views.event_details_view, name='event_details'),
]

