from django.db import models

import json


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

class HotelsForEvent(models.Model):
    EventID = models.CharField(max_length=512, primary_key=True)
    Title = models.CharField(max_length=1000, null=True, blank=True)
    Agoda_Hotels = models.TextField(null=True, blank=True)
    Google_Place_Hotels = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'hotels_for_event'

    def __str__(self):
        return self.Title or self.EventID
        
class EventsForHotel(models.Model):
    Google_Place_Id = models.CharField(max_length=512, primary_key=True)
    HOTELNAME = models.CharField(max_length=512, null=True, blank=True)
    EventID = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'events_for_hotel'

    def __str__(self):
        return self.HOTELNAME or self.Google_Place_Id
        

class HotelList(models.Model):
    event_id = models.CharField(max_length=512, primary_key=True)
    google_name = models.CharField(max_length=1000, blank=True, null=True)
    google_rating = models.FloatField(blank=True, null=True)
    google_user_ratings_total = models.IntegerField(blank=True, null=True)
    google_place_id = models.CharField(max_length=512, blank=True, null=True)
    agoda_hotel_id = models.CharField(max_length=512, blank=True, null=True)
    agoda_chain_id = models.CharField(max_length=512, blank=True, null=True)
    agoda_chain_name = models.CharField(max_length=1000, blank=True, null=True)
    agoda_hotel_name = models.CharField(max_length=1000, blank=True, null=True)
    agoda_city = models.CharField(max_length=512, blank=True, null=True)
    agoda_star_rating = models.FloatField(blank=True, null=True)
    agoda_longitude = models.FloatField(blank=True, null=True)
    agoda_latitude = models.FloatField(blank=True, null=True)
    agoda_checkin = models.CharField(max_length=100, blank=True, null=True)
    agoda_checkout = models.CharField(max_length=100, blank=True, null=True)
    google_number_of_reviews = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = 'hotel_list'

    def __str__(self):
        return self.hotel_name or self.google_name

class FlightTo(models.Model):
    airline_code = models.CharField(max_length=50, primary_key=True)
    departure = models.CharField(max_length=50)
    departure_at = models.DateTimeField()
    arrival = models.CharField(max_length=50)
    arrival_at = models.DateTimeField()
    duration = models.CharField(max_length=50)
    seats = models.IntegerField()
    price = models.IntegerField()
    airline_name = models.CharField(max_length=100)
    departure_date = models.IntegerField()
    departure_min = models.IntegerField()

    class Meta:
        db_table = 'flight_to'

    def __str__(self):
        return self.airline_code or self.arrival

class FlightFrom(models.Model):
    airline_code = models.CharField(max_length=50, primary_key=True)
    departure = models.CharField(max_length=50)
    departure_at = models.DateTimeField()
    arrival = models.CharField(max_length=50)
    arrival_at = models.DateTimeField()
    duration = models.CharField(max_length=50)
    seats = models.IntegerField()
    price = models.IntegerField()
    airline_name = models.CharField(max_length=100)
    departure_date = models.IntegerField()
    departure_min = models.IntegerField()

    class Meta:
        db_table = 'flight_from'

    def __str__(self):
        return self.airline_code or self.departure

class Airport(models.Model):
    airport_code = models.CharField(max_length=50, primary_key=True)
    airport_name = models.CharField(max_length=100)
    airport_location = models.CharField(max_length=200)
    country_code = models.CharField(max_length=50)
    country_name = models.CharField(max_length=50)

    class Meta:
        db_table = 'airport'

    def __str__(self):
        return self.airport_code or self.country_code

class NearestAirport(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    title = models.CharField(max_length=100)
    country = models.CharField(max_length=100)
    start_date = models.CharField(max_length=100)
    end_date = models.CharField(max_length=100)
    airport_code = models.CharField(max_length=100)
    airport_name = models.CharField(max_length=100)

    class Meta:
        db_table = 'nearest_airports'

    def __str__(self):
        return self.id or self.airline_code
    


class PlaceforEvent(models.Model):
    event_id = models.CharField(max_length=256, primary_key=True)  
    event_title = models.CharField(max_length=256)  
    place_name = models.CharField(max_length=256)  
    address = models.CharField(max_length=256)  
    rating = models.FloatField(null=True)  
    number_of_reviews = models.IntegerField(null=True)  
    review = models.TextField(blank=True, null=True) 
    types = models.CharField(max_length=256, blank=True, null=True)  
    opening_hours = models.TextField(blank=True, null=True)  


    class Meta:
        db_table = 'events_places_raw'  
        #unique_together = (('event_id', 'place_name'),)  

    def __str__(self):
        return self.place_name