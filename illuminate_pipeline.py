from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *
from modules.config import base_url_illuminate
import logging
import os
import sys
from pyspark import RDD
from pyspark.sql import SparkSession

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
        assessment_id_list = assessment_id_list[:100] #for testing
        missing_ids_from_metadata = ['114845', '141498'] # Add assessments that are not present in assessements metadata
        assessment_id_list = list(set(assessment_id_list + missing_ids_from_metadata))
        logging.info(f'Here is the length of the assessment_id_list variable {len(assessment_id_list)}')

        test_results_group, log_results_group = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'Group', start_date, end_date_override=None)
        test_results_standard, log_results_standard = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'Standard', start_date, end_date_override)
        test_results_no_standard, log_results_no_standard = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'No_Standard', start_date, end_date_override)
 
        test_results_combined = bring_together_test_results(test_results_no_standard, test_results_standard)
        test_results_view = create_test_results_view(test_results_combined, years_data) #add in grade level col, string matching
        logging.info("Assessment results fetched and processed.")

        
        os.makedirs(save_path, exist_ok=True)

        if years_data == '23-24':
            logging.info(f'Sending data for {years_data} school year')
            send_to_local(save_path, test_results_group, 'assessment_results_group_historical.csv')
            send_to_local(save_path, test_results_combined, 'assessment_results_combined_historical.csv')
            send_to_local(view_path, test_results_view, 'illuminate_assessment_results_historical.csv')
            
        elif years_data == '24-25':
            logging.info(f'Sending data for {years_data} school year')
            send_to_local(save_path, test_results_group, 'assessment_results_group.csv')
            send_to_local(save_path, test_results_combined, 'assessment_results_combined.csv')
            send_to_local(view_path, test_results_view, 'illuminate_assessment_results.csv')
        else:
            raise ValueError(f'Unexpected value for years variable data {years_data}')
        
        #No matter what update assessments_metadata file to display available assessments
        send_to_local(save_path, assessments_df, 'assessments_metadata,csv')
        
        


    except Exception as e:
        logging.error(f"Error fetching assessment results: {e}")
        raise AirflowException("Failed to fetch and process assessment results")


get_assessment_results(spark,
                        save_path = '/home/g2015samtaylor/illuminate',
                        view_path = '/home/g2015samtaylor/views',
                        years_data = '24-25',
                        start_date = '2024-07-01')
#end_date should default to todays_date if not specified