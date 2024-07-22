from django.db import models


class Event(models.Model):
    name = models.CharField(max_length=255)
    date = models.DateTimeField()
    category = models.CharField(max_length=255)
    location = models.CharField(max_length=255)
    city = models.CharField(max_length=255)

    def __str__(self):
        return self.name

# Create your models here.
