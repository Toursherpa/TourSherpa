from django.contrib import admin

from .models import  HotelsForEvent, EventsForHotel, TravelEvent, HotelList, FlightTo, FlightFrom, Airport, NearestAirport, PlaceforEvent 



@admin.register(TravelEvent)
class TravelEventAdmin(admin.ModelAdmin):
    list_display = ('EventID', 'Title', 'Category', 'Rank', 'PhqAttendance', 'TimeStart', 'TimeEnd', 'Region', 'Country', 'PredictedEventSpend')
    search_fields = ('EventID', 'Title', 'Category', 'Region', 'Country')
    list_filter = ('Category', 'Region', 'Country')
    ordering = ('Rank',)
    
@admin.register(EventsForHotel)
class EventsForHotelAdmin(admin.ModelAdmin):
    list_display = ('Google_Place_Id', 'HOTELNAME')
    search_fields = ('Google_Place_Id', 'HOTELNAME')

@admin.register(HotelsForEvent)
class HotelsForEventAdmin(admin.ModelAdmin):
    list_display = ('EventID', 'Title')
    search_fields = ('EventID', 'Title')

@admin.register(HotelList)
class HotelListAdmin(admin.ModelAdmin):
    list_display = ('event_id', 'google_name', 'agoda_hotel_name', 'agoda_city', 'agoda_star_rating')
    search_fields = ('event_id', 'google_name', 'agoda_hotel_name')
    list_filter = ('agoda_star_rating', 'google_rating', 'agoda_city')
    ordering = ('event_id',)

@admin.register(FlightTo)
class FlightToAdmin(admin.ModelAdmin):
    list_display = ('airline_code', 'departure', 'departure_at', 'arrival', 'arrival_at', 'duration', 'seats', 'price', 'airline_name', 'departure_date', 'departure_min')
    search_fields = ('airline_code', 'departure_at', 'arrival', 'airline_name')
    list_filter = ('airline_code', 'arrival')

@admin.register(FlightFrom)
class FlightFromAdmin(admin.ModelAdmin):
    list_display = ('airline_code', 'departure', 'departure_at', 'arrival', 'arrival_at', 'duration', 'seats', 'price', 'airline_name', 'departure_date', 'departure_min')
    search_fields = ('airline_code', 'departure', 'departure_at', 'airline_name')
    list_filter = ('airline_code', 'departure')

@admin.register(Airport)
class AirportAdmin(admin.ModelAdmin):
    list_display = ('airport_code', 'airport_name', 'airport_location', 'country_code', 'country_name')
    search_fields = ('airport_code', 'airport_name', 'country_code', 'country_name')
    list_filter = ('airport_code', 'country_code')

@admin.register(NearestAirport)
class NearestAirportAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'country', 'start_date', 'end_date', 'airport_code', 'airport_name')
    search_fields = ('title', 'country', 'airport_code', 'start_date', 'end_date', 'airport_name')
    list_filter = ('country', 'airport_code')

@admin.register(PlaceforEvent)
class PlaceforEventAdmin(admin.ModelAdmin):
    list_display = ('event_id', 'event_title', 'place_name', 'address', 'rating', 'number_of_reviews')
    search_fields = ('event_id', 'event_title', 'place_name', 'address')
    list_filter = ('rating', 'number_of_reviews')
    ordering = ('event_id',)
