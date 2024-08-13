import re
from django import template
from datetime import datetime

register = template.Library()

# ISO 8601 날짜 형식을 위한 정규 표현식
ISO_8601_REGEX = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')

@register.filter
def format_date_str(date_str):
    if ISO_8601_REGEX.match(date_str):
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.strftime('%Y-%m-%d %H시 %M분')
        except ValueError:
            return date_str  # 변환 실패 시 원문 반환
    return date_str

@register.filter
def format_date(date_str):
    dt = datetime.fromisoformat(date_str)
    return dt.strftime('%Y-%m-%d %H시 %M분')