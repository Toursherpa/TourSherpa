from django.contrib import admin

from .models import  HotelsForEvent, EventsForHotel, TravelEvent, HotelList, FlightTo, FlightFrom, Airline, Airport, NearestAirport



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
    list_display = ('airline_code', 'departure', 'departure_at', 'arrival', 'arrival_at', 'duration', 'seats', 'price')
    search_fields = ('airline_code', 'departure_at', 'arrival')
    list_filter = ('airline_code', 'arrival')

@admin.register(FlightFrom)
class FlightFromAdmin(admin.ModelAdmin):
    list_display = ('airline_code', 'departure', 'departure_at', 'arrival', 'arrival_at', 'duration', 'seats', 'price')
    search_fields = ('airline_code', 'departure', 'departure_at')
    list_filter = ('airline_code', 'departure')

@admin.register(Airline)
class AirlineAdmin(admin.ModelAdmin):
    list_display = ('airline_code', 'airline_name')
    search_fields = ('airline_code', 'airline_name')
    list_filter = ('airline_code', 'airline_name')

@admin.register(Airport)
class AirportAdmin(admin.ModelAdmin):
    list_display = ('airport_code', 'airport_name', 'airport_location', 'country_code', 'country_name')
    search_fields = ('airport_code', 'airport_name', 'country_code', 'country_name')
    list_filter = ('airport_code', 'country_code')

@admin.register(NearestAirport)
class NearestAirportAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'country', 'airport_code', 'airport_name')
    search_fields = ('title', 'country', 'airport_code', 'airport_name')
    list_filter = ('country', 'airport_code')