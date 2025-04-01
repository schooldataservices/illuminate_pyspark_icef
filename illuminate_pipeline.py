import os
import logging
import sys
from pyspark.sql import SparkSession
from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Request Parallelization") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Configure logging to use StreamHandler for stdout
logging.basicConfig(
    level=logging.INFO,  # Adjust as needed (e.g., DEBUG, WARNING)
    format="%(asctime)s - %(message)s",  # Log format
    datefmt="%d-%b-%y %H:%M:%S",  # Date format
    handlers=[
        logging.StreamHandler(sys.stdout)  # Direct logs to stdout
    ],
    force=True  # Ensures existing handlers are replaced
)

def get_assessment_results(spark, save_path, view_path, years_data, start_date, end_date_override=None):
    logging.info('\n\n-------------New Illuminate Operations Logging Instance')

    try:
        access_token, expires_in = get_access_token()

        assessments_df, assessment_id_list = get_all_assessments_metadata(access_token)
        assessment_id_list.append('141498') #Somehow missing from shared assessments
        assessment_id_list = list(set(assessment_id_list))
        logging.info(f'Here is the length of the assessment_id_list variable {len(assessment_id_list)}')

        test_results_group, log_results_group = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'Group', start_date, end_date_override=None)
        test_results_standard, log_results_standard = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'Standard', start_date, end_date_override)
        test_results_no_standard, log_results_no_standard = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'No_Standard', start_date, end_date_override)
 
        test_results_combined = bring_together_test_results(test_results_no_standard, test_results_standard)
        test_results_view = create_test_results_view(test_results_combined, years_data)
        logging.info("Assessment results fetched and processed.")

        os.makedirs(save_path, exist_ok=True)
        logging.info(f'Sending data for {years_data} school year')
        bucket_name = "illuminatebucket-icefschools-1"
        bucket_name_views = "viewsbucket-icefschools-1"

        if years_data == '23-24':

            send_to_gcs(bucket_name,save_path="",frame=test_results_group,frame_name="assessment_results_group_historical.csv")
            send_to_gcs(bucket_name,save_path="",frame=test_results_combined,frame_name="assessment_results_combined_historical.csv")
            send_to_gcs(bucket_name_views,save_path="",frame=test_results_view,frame_name="illuminate_assessment_results_historical.csv")
         
        elif years_data == '24-25':
            send_to_gcs(bucket_name,save_path="",frame=test_results_group,frame_name="assessment_results_group.csv")
            send_to_gcs(bucket_name,save_path="",frame=test_results_combined,frame_name="assessment_results_combined.csv")
            send_to_gcs(bucket_name_views,save_path="",frame=test_results_view,frame_name="illuminate_assessment_results.csv")
        else:
            raise ValueError(f'Unexpected value for years variable data {years_data}')
        
        #send over metadata everytime
        send_to_gcs(bucket_name, save_path='', frame=assessments_df, frame_name='assessments_metadata.csv')


    except Exception as e:
        logging.error(f"Error fetching assessment results: {e}")
        raise AirflowException("Failed to fetch and process assessment results")

if __name__ == "__main__":
    get_assessment_results(spark,
                           years_data=os.getenv('YEARS_DATA', '24-25'),
                           start_date=os.getenv('START_DATE', '2024-07-01'))