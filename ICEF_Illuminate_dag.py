import sys
import os
import logging
working_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory/Illuminate')
sys.path.append(working_dir)

import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *
from modules.config import base_url_illuminate, assessment_id_list


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 28), 
    'email_on_failure': True,
    'email_on_retry': True,
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
    schedule_interval='0 5 * * 1-5',  # Every weekday (Mon-Fri) at 5:00 AM
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def log_start():
        logging.info('\n\n-------------New Illuminate Operations Logging Instance')

    def get_assessment_results(save_path):
        log_start()
        try:
            access_token, expires_in = get_access_token()
            # Fetch assessment results
            test_results_group, log_results_group = loop_through_assessment_scores(access_token, assessment_id_list, 'Group')
            test_results_standard, log_results_standard = loop_through_assessment_scores(access_token, assessment_id_list, 'Standard')
            test_results_no_standard, log_results_no_standard = loop_through_assessment_scores(access_token, assessment_id_list, 'No_Standard')
            
        
            test_results_view = create_test_results_view(test_results_standard, access_token, '23-24') #add in grade level col, string matching
            logging.info("Assessment results fetched and processed.")
            
            os.makedirs(save_path, exist_ok=True)

            send_to_local(save_path, test_results_standard, 'assessment_results_standard.csv')
            send_to_local(save_path, test_results_no_standard, 'assessment_results_no_standard.csv')
            send_to_local(save_path, test_results_group, 'assessment_results_group.csv')
            send_to_local(save_path, test_results_view, 'assessment_results_view.csv')


        except Exception as e:
            logging.error(f"Error fetching assessment results: {e}")
            raise AirflowException("Failed to fetch and process assessment results")


    # Define tasks
    task_get_assessment_results = PythonOperator(
        task_id='get_assessment_results',
        python_callable=get_assessment_results,
        op_kwargs={
            'save_path' : '/home/g2015samtaylor/illuminate'
        }
    )

    # Set task dependencies
    task_get_assessment_results
 
