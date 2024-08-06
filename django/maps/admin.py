from django.contrib import admin
from .models import HotelsForEvent, EventsForHotel, TravelEvent, HotelList



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
