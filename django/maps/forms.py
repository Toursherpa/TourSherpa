from django import forms

class EventFilterForm(forms.Form):
    CATEGORY_CHOICES = [
        ('', 'All Categories'),
        ('sports', 'Sports'),
    ]
    category = forms.ChoiceField(choices=CATEGORY_CHOICES, required=False)
    city = forms.CharField(max_length=255, required=False)
