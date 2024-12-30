# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy only requirements first to leverage Docker caching
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . /app/

# Run the script with Airflow-compatible logging
CMD ["python", "illuminate_pipeline.py"]


#To run locally
# docker run --rm \
#   -v /home/icef/powerschool/Student_Rosters.txt:/home/icef/powerschool/Student_Rosters.txt \
#   -v /home/g2015samtaylor/illuminate:/home/g2015samtaylor/illuminate \
#   -v /home/g2015samtaylor/views:/home/g2015samtaylor/views \
#   illuminate-pipeline
