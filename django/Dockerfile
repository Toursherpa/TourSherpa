# Base image
FROM python:3.8-slim

# Install necessary system packages
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list && \
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99archive && \
    apt-get update && \
    apt-get install -y libpq-dev gcc python3-dev && \
    apt-get clean

# Set the working directory
WORKDIR /app/src

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files
COPY . .

# Set the environment variables for PostgreSQL
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# Command to run the application
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
