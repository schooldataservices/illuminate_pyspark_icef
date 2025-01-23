# Illuminate Data Pipeline

This repository contains a data pipeline designed to fetch and process assessment results from the Illuminate platform, using Spark for parallel data processing. The pipeline is encapsulated within a Docker container, making it portable and easy to run across various environments.

## Overview

The data pipeline fetches assessment data via API requests, processes the data, and then stores the results in specified directories for further use. It is designed to handle large datasets efficiently using Spark's distributed computing power. The pipeline performs the following key steps:

1. Fetches assessment metadata.
2. Parallelizes the fetching of assessment scores.
3. Combines results and generates views.
4. Saves the processed data locally for further use.

## Requirements

Before running the pipeline, make sure you have the following prerequisites:

- **Docker**: Required to containerize and run the pipeline.
- **Apache Spark**: The pipeline uses Spark for distributed data processing.
- **Python 3.9+**: Required for running the pipeline.
- **Required Python Libraries**: These are specified in the `requirements.txt` file.

## Setup

1. **Clone the repository** to your local machine:

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2. **Install dependencies**:
    Make sure you have a `requirements.txt` file in the root of the repository. If not, you can install dependencies manually.

    If you're using Docker, the required libraries will be installed inside the container. Otherwise, you can install them in your local environment by running:

    ```bash
    pip install -r requirements.txt
    ```

3. **Docker Configuration**:

    - The pipeline is encapsulated in a Docker container.
    - The Dockerfile uses the official Bitnami Spark image and installs the necessary Python dependencies.

## Dockerfile

The Dockerfile provided sets up an environment for running the Spark-based pipeline. Here's a breakdown of the key steps:

- **Base Image**: Uses the official Bitnami Spark image (`bitnami/spark:3.2.1`).
- **Install Dependencies**: Installs the required Python packages from `requirements.txt`.
- **Spark Configuration**: Sets up the environment variables for Spark and Java.
- **Volume Mounts**: Mounts the necessary directories for input files and output results.
- **Execution**: Runs the pipeline using `spark-submit`.

### Dockerfile Example:

```dockerfile
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
```

## Running the Pipeline

1. **Build the Docker image**:
   
   Build the Docker image using the `Dockerfile`:

   ```bash
   docker build -t illuminate-pipeline:pyspark .
   ```

2. **Run the Docker container**:
   
   You can run the Docker container with volume mounts to use local files, for example:

   ```bash
   docker run -it \
     -v /hypothetical_mounts/ : hypothetical_local_mounts/
     illuminate-pipeline:pyspark
   ```

   The above command will:
   - Mount the `Student_Rosters.txt` file for input.
   - Mount the `illuminate` and `views` directories for saving the processed results.

3. **Execution**:

   After running the container, the Spark job (`illuminate_pipeline.py`) will execute within the Docker container, fetching assessment results, processing them using Spark, and saving the results to the specified directories.

4. **Output**:

   The processed results will be stored in:
   - `/home/g2015samtaylor/illuminate/` for historical assessment results.
   - `/home/g2015samtaylor/views/` for the view of the processed assessment results.

   The file names are dynamically generated based on the `years_data` variable:
   - For `23-24` school year: `assessment_results_group_historical.csv`, `assessment_results_combined_historical.csv`, `illuminate_assessment_results_historical.csv`.
   - For `24-25` school year: `assessment_results_group.csv`, `assessment_results_combined.csv`, `illuminate_assessment_results.csv`.

## Airflow Integration

The pipeline can also be run as an **Airflow DAG** using the `DockerOperator`. This allows for automation and scheduling of the pipeline within an Airflow environment. The Airflow DAG will use the same Docker container and volume mounts to execute the Spark job.

### Example Airflow DAG:

```python
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    'spark_pipeline_dag',
    default_args=default_args,
    description='A DAG to run Spark job in Docker',
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark_pipeline = DockerOperator(
        task_id='run_spark_pipeline',
        image='illuminate-pipeline:pyspark',
        command='spark-submit /app/illuminate_pipeline.py',
        mounts=[
            {'Source': ,
            'Target': ,
            'Type': }
        ],
        environment={
            'PYSPARK_SUBMIT_ARGS': '--conf spark.pyspark.gateway.timeout=300',
        },
        dag=dag,
    )

    run_spark_pipeline
```

## Logging

Logs are captured at various stages of the pipeline:

- **API Request Logs**: Logs related to the API requests are captured using the `logging` module and displayed in stdout.
- **Spark Logs**: Spark’s internal logging will also be available, helping with debugging and monitoring the job’s progress.

## Troubleshooting

- Ensure the Spark job has enough resources (RAM and CPU) to run efficiently.
- Check the Airflow logs or Docker container logs for errors related to task failures.

