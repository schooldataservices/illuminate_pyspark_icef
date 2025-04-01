# Use the official Spark image as the base image
FROM bitnami/spark:3.2.1

# Set the working directory in the container
WORKDIR /app

# Switch to root user to install dependencies
USER root

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application code (including modules) into the container
COPY . /app/

# Set up the environment variables for PySpark
ENV PYSPARK_SUBMIT_ARGS="--conf spark.pyspark.gateway.timeout=300" \
    JAVA_HOME=/opt/bitnami/java \
    SPARK_HOME=/opt/bitnami/spark

# Set the PATH to include Spark and Java binaries
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Ensure proper permissions for the directories
RUN chmod -R 777 /app

# Expose the required ports for Spark (Spark UI and Spark Master)
EXPOSE 7077 8080

# Define environment variables
ENV SAVE_PATH=/app/illuminate
ENV VIEW_PATH=/app/views
ENV YEARS_DATA=24-25
ENV START_DATE=2024-07-01
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/icef-437929.json

# Command to run the Spark job
CMD ["spark-submit", "/app/illuminate_pipeline.py"]


# docker run --rm \
#     -e SAVE_PATH=/app/illuminate \
#     -e VIEW_PATH=/app/views \
#     -e YEARS_DATA=23-24 \
#     -e START_DATE=2023-07-01 \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/app/icef-437920.json \
#     -v /home/sam/icef-437920.json:/app/icef-437920.json \
#     illuminate-pipeline:pyspark > pipeline_output.log 2>&1
