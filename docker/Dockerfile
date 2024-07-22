# Use the official Python image from the Docker Hub
FROM python:3.6.5-slim

# Set the working directory
WORKDIR /app/src

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Command to run on container start
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

