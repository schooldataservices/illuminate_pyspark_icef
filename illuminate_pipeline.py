import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/icef-437920.json"
import logging
import sys
from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *
from gcp_utils_sds import buckets
from gcp_utils_sds import yoy
import multiprocessing
import psutil



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

def get_assessment_results(years_data, start_date, end_date_override=None):
    logging.info('\n\n-------------New Illuminate Operations Logging Instance')
    logging.info(f"Available CPUs: {multiprocessing.cpu_count()}")
    logging.info(f"Available RAM: {round(psutil.virtual_memory().total / (1024 ** 3), 2)} GB")

    access_token, expires_in = get_access_token()

    assessments_metadata, assessment_id_list = get_all_assessments_metadata(access_token)
    assessment_id_list = list(set(assessment_id_list))
    logging.info(f'Here is the length of the assessment_id_list variable {len(assessment_id_list)}')

    assessment_results_group, log_results_group = parallel_get_assessment_scores_threaded(access_token, assessment_id_list, 'Group', start_date, end_date_override)
    test_results_standard, log_results_standard = parallel_get_assessment_scores_threaded(access_token, assessment_id_list, 'Standard', start_date, end_date_override)
    test_results_no_standard, log_results_no_standard = parallel_get_assessment_scores_threaded(access_token, assessment_id_list, 'No_Standard', start_date, end_date_override)

    logging.info(f'Here is the length of the assessment_results_group variable {len(assessment_results_group)}')
    logging.info(f'Here is the length of the test_results_standard variable {len(test_results_standard)}')
    logging.info(f'Here is the length of the test_results_no_standard variable {len(test_results_no_standard)}')

    if (
    len(assessment_results_group) == 0
    and len(test_results_standard) == 0
    and len(test_results_no_standard) == 0
    ):
        logging.info("All assessment result frames are empty. No results for this year yet. Exiting task successfully.")
        return  # Task ends and is marked as success. No results for this year yet. 

    assessment_results_combined = bring_together_test_results(test_results_no_standard, test_results_standard)
    illuminate_assessment_results = create_test_results_view(assessment_results_combined)
    
    assessment_results_group['year'] = years_data
    assessment_results_combined['year'] = years_data
    illuminate_assessment_results['year'] = years_data
    logging.info("Assessment results fetched and processed. Now bringing together with prior years")

    appender = yoy.YearlyDataAppender(
        project_id="icef-437920",
        dataset_id="illuminate",
        bucket_name="historicalbucket-icefschools-1"
    )

    assessment_results_group = appender.load_and_append(
        table_name="assessment_results_group",
        blob_paths_old=[
            "illuminate/assessment_results_group_23-24.csv",
            "illuminate/assessment_results_group_24-25.csv"
        ],
        current_df=assessment_results_group,
        drop_duplicate_columns=[
            col for col in assessment_results_group.columns
            if "count" not in col and col.strip().lower() != "standard_no_standard"
        ]
    )

    assessment_results_combined = appender.load_and_append(
        table_name="assessment_results_combined",
        blob_paths_old=[
            "illuminate/assessment_results_combined_23-24.csv",
            "illuminate/assessment_results_combined_24-25.csv"
        ],
        current_df=assessment_results_combined,
        drop_duplicate_columns=[
            col for col in assessment_results_combined.columns
            if "count" not in col and col.strip().lower() != "standard_no_standard"
        ]
    )

    illuminate_assessment_results = appender.load_and_append(
        table_name="illuminate_assessment_results",
        blob_paths_old=[
            "illuminate/illuminate_assessment_results_23-24.csv",
            "illuminate/illuminate_assessment_results_24-25.csv"
        ],
        current_df=illuminate_assessment_results,
        drop_duplicate_columns=[
            col for col in illuminate_assessment_results.columns
            if "count" not in col and col.strip().lower() != "standard_no_standard"
        ]
    )

    logging.info(f'Sending data for {years_data} school year')
    bucket_name = "illuminatebucket-icefschools-1"

    buckets.send_to_gcs(bucket_name, "", assessment_results_group, "assessment_results_group.csv")
    buckets.send_to_gcs(bucket_name, "", assessment_results_combined, "assessment_results_combined.csv")
    buckets.send_to_gcs(bucket_name, "", illuminate_assessment_results, "illuminate_assessment_results.csv")
    buckets.send_to_gcs(bucket_name, '', assessments_metadata, 'assessments_metadata.csv')



get_assessment_results(years_data=os.getenv('YEARS_DATA'),
                        start_date=os.getenv('START_DATE'))