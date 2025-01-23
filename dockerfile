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

# Command to run the Spark job
CMD ["spark-submit", "/app/illuminate_pipeline.py"]


#test in airflow, if not change back to backup image

#This works below
# docker run -it \
#   -v /home/icef/powerschool/Student_Rosters.txt:/home/icef/powerschool/Student_Rosters.txt \
#   -v /home/g2015samtaylor/illuminate:/home/g2015samtaylor/illuminate \
#   -v /home/g2015samtaylor/views:/home/g2015samtaylor/views \
#   illuminate-pipeline:pyspark

