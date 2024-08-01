from django.db import models

import json


class Event(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255)
    date = models.DateTimeField()
    category = models.CharField(max_length=255)
    location = models.CharField(max_length=255)
    city = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class TravelEvent(models.Model):
    EventID = models.CharField(max_length=255, primary_key=True, default='none')
    Title = models.CharField(max_length=1000, null=True, blank=True, default='none')
    Description = models.CharField(max_length=5000, blank=True, default='none')
    Category = models.CharField(max_length=255, null=True, blank=True, default='none')
    Rank = models.IntegerField(null=True, blank=True, default=0)
    PhqAttendance = models.IntegerField(null=True, blank=True, default=0)
    TimeStart = models.CharField(max_length=50, blank=True, default='none')
    TimeEnd = models.CharField(max_length=50, blank=True, default='none')
    LocationID = models.CharField(max_length=50, blank=True, default='none') # Redshift의 JSONB에 매핑
    Address = models.TextField(null=True, blank=True, default='none')
    Region = models.CharField(max_length=255, null=True, blank=True, default='none')
    Country = models.CharField(max_length=50, null=True, blank=True, default='none')
    PredictedEventSpend = models.FloatField(null=True, blank=True, default=0)

    class Meta:
        db_table = 'travel_events'

    def __str__(self):
        return self.title or self.event_id

