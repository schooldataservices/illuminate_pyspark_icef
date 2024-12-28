from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *
from modules.config import base_url_illuminate
import logging
import os
import sys


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

logging.info("\n\n------------- New Illuminate Operations Logging Instance")

def get_assessment_results(save_path):
    logging.info("\n\n------------- New Illuminate Operations Logging Instance")
   
    try:
        access_token, expires_in = get_access_token()

        assessments_df, assessment_id_list = get_all_assessments_metadata(access_token)
        # assessment_id_list = assessment_id_list[:10] #for testing
        missing_ids_from_metadata = ['114845', '141498'] # Add assessments that are not present in assessements metadata
        assessment_id_list = list(set(assessment_id_list + missing_ids_from_metadata))
    
        # Fetch assessment results
        test_results_group, log_results_group = loop_through_assessment_scores(access_token, assessment_id_list, 'Group')
        test_results_standard, log_results_standard = loop_through_assessment_scores(access_token, assessment_id_list, 'Standard')
        test_results_no_standard, log_results_no_standard = loop_through_assessment_scores(access_token, assessment_id_list, 'No_Standard')
        
        test_results_combined = bring_together_test_results(test_results_no_standard, test_results_standard)
        test_results_view = create_test_results_view(test_results_combined, '24-25') #add in grade level col, string matching
        logging.info("Assessment results fetched and processed.")
        
        os.makedirs(save_path, exist_ok=True)

        # prior_year_file_path = '/home/g2015samtaylor/backups/illuminate'
        # test_results_group = append_prior_year(prior_year_file_path, test_results_group,  'assessment_results_group_2324.csv')
        # test_results_view = append_prior_year(prior_year_file_path, test_results_view,  'assessment_results_view_2324.csv')
        # test_results_combined = append_prior_year(prior_year_file_path, test_results_combined,  'assessment_results_combined_2324.csv')

        send_to_local(save_path, test_results_group, 'assessment_results_group_2425.csv')
        send_to_local(save_path, test_results_view, 'assessment_results_view_2425.csv')
        send_to_local(save_path, test_results_combined, 'assessment_results_combined_2425.csv')


    except Exception as e:
        logging.error(f"Error fetching assessment results: {e}")
        raise AirflowException("Failed to fetch and process assessment results")

save_path = '/home/g2015samtaylor/illuminate'
get_assessment_results(save_path)