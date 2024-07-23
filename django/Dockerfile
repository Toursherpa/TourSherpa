# Use the latest official Python image
FROM python:3.11-slim



# Update system packages and install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libpq-dev \
    python3-dev\
    zlib1g-dev

# Set the working directory in the container
WORKDIR /app/src
COPY requirements.txt .

# Wheel 패키지 설치
RUN pip install --upgrade pip wheel setuptools
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -v -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Command to run on container start
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
