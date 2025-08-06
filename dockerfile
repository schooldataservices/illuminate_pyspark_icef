# Use the official Python image as the base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the entire application code (including modules) into the container
COPY . /app/

# Ensure proper permissions for the directories
RUN chmod -R 777 /app

CMD ["python", "/app/illuminate_pipeline.py"]


