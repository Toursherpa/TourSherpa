FROM python:3.11

# 필수 시스템 라이브러리 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    tzdata \
    libffi-dev\
  && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app/src
# Python 라이브러리 업데이트
RUN pip install --upgrade pip

# 의존성 파일 복사
COPY requirements.txt /app/src

# 의존성 설치
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . /app/src

# 애플리케이션 실행
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
