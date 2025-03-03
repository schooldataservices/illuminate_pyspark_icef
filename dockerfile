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
ENV MANUAL_CHANGES_FILE_PATH=/app/illuminate_checkpoint_manual_changes.csv
ENV YEARS_DATA=24-25
ENV START_DATE=2024-07-01

# Command to run the Spark job
CMD ["spark-submit", "/app/illuminate_pipeline.py"]


#test in airflow, if not change back to backup image

#This works below
# docker run -v /path/to/local/illuminate:/app/illuminate \
#            -v /path/to/local/views:/app/views \
#            -v /path/to/local/illuminate_checkpoint_manual_changes.csv:/app/illuminate_checkpoint_manual_changes.csv \
#            illuminate-pipeline:pyspark

