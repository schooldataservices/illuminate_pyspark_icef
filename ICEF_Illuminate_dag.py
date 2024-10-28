import sys
import os
import logging
working_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory/ICEF_Illuminate')
sys.path.append(working_dir)

import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *
from modules.config import base_url_illuminate, assessment_id_list

# Set up working directory
working_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory/ICEF_Illuminate')
sys.path.append(working_dir)


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
}

# Define the DAG
with DAG(
    dag_id='ICEF_illuminate_dag',
    default_args=args,
    description='A DAG to handle Illuminate API calls',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def log_start():
        logging.info('\n\n-------------New Illuminate Operations Logging Instance')

    def get_assessment_results():
        log_start()
        try:
            access_token, expires_in = get_access_token()

            # Fetch assessment results
            test_results_group, log_results_group = loop_through_assessment_scores(access_token, assessment_id_list, 'Group')
            test_results_standard, log_results_standard = loop_through_assessment_scores(access_token, assessment_id_list, 'Standard')
            test_results_no_standard, log_results_no_standard = loop_through_assessment_scores(access_token, assessment_id_list, 'No_Standard')

            # Combine results
            test_results = pd.concat([test_results_standard, test_results_no_standard])
            test_results = alter_test_results_frame(access_token, test_results)  # Add in grade level column

            logging.info("Assessment results fetched and processed.")
            return test_results  # Return results if needed

        except Exception as e:
            logging.error(f"Error fetching assessment results: {e}")

    task_get_assessment_results = PythonOperator(
        task_id='get_assessment_results',
        python_callable=get_assessment_results
    )

    task_get_assessment_results


#COnfigure this to send to the SFTP