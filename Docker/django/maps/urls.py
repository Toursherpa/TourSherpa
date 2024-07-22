from django.urls import path, re_path
from . import views
from .views import upload_csv

urlpatterns = [
    path('google-map/', views.google_map_view, name='google_map_view'),
    path('upload_csv/', upload_csv, name='upload_csv'),
    re_path(r'^event/(?P<name>.+)/(?P<location>.+)/(?P<date>.+)/(?P<category>.+)/(?P<city>.+)/$', views.event_details_view, name='event_details'),
]

