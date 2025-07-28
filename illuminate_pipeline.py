import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/icef-437920.json"
import logging
import sys
from pyspark.sql import SparkSession
from modules.auth import *
from modules.assessments_endpoints import *
from modules.frame_transformations import *
from gcp_utils_sds import buckets
from gcp_utils_sds import yoy

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

def get_assessment_results(spark, years_data, start_date, end_date_override=None):
    logging.info('\n\n-------------New Illuminate Operations Logging Instance')

    access_token, expires_in = get_access_token()

    assessments_metadata, assessment_id_list = get_all_assessments_metadata(access_token)
    assessment_id_list.append('141498') #Somehow missing from shared assessments
    assessment_id_list = list(set(assessment_id_list))
    logging.info(f'Here is the length of the assessment_id_list variable {len(assessment_id_list)}')

    assessment_results_group, log_results_group = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'Group', start_date, end_date_override)
    test_results_standard, log_results_standard = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'Standard', start_date, end_date_override)
    test_results_no_standard, log_results_no_standard = parallel_get_assessment_scores(spark, access_token, assessment_id_list, 'No_Standard', start_date, end_date_override)
    spark.stop() #Release resources

    assessment_results_combined = bring_together_test_results(test_results_no_standard, test_results_standard)
    illuminate_assessment_results = create_test_results_view(assessment_results_combined, years_data)
    logging.info("Assessment results fetched and processed.Now bringing together with prior years")

    appender = yoy.YearlyDataAppender(
    project_id="icef-437920",
    dataset_id="illuminate",
    bucket_name="historicalbucket-icefschools-1"
    )

    assessment_results_group = appender.load_and_append(
        table_name="assessment_results_group",
        blob_paths_old=["illuminate/assessment_results_group_23-24.csv",
                        "illuminate/assessment_results_group_24-25.csv"],
        current_df=assessment_results_group,
        drop_duplicate_columns=[col for col in assessment_results_group.columns
        if "count" not in col and col.strip().lower() != "standard_no_standard"]
        )

    assessment_results_combined = appender.load_and_append(
        table_name="assessment_results_combined",
        blob_paths_old=["illuminate/assessment_results_combined_23-24.csv",
                        "illuminate/assessment_results_combined_24-25.csv"],
        current_df=assessment_results_combined,
        drop_duplicate_columns=[col for col in assessment_results_combined.columns
        if "count" not in col and col.strip().lower() != "standard_no_standard"]
        )

    illuminate_assessment_results = appender.load_and_append(
        table_name="illuminate_assessment_results",
        blob_paths_old=["illuminate/illuminate_assessment_results_23-24.csv",
                    "illuminate/illuminate_assessment_results_24-25.csv"],
        current_df=illuminate_assessment_results,
        drop_duplicate_columns=[col for col in illuminate_assessment_results.columns
        if "count" not in col and col.strip().lower() != "standard_no_standard"]
    )

    logging.info(f'Sending data for {years_data} school year')
    bucket_name = "illuminatebucket-icefschools-1"
    bucket_name_views = "viewsbucket-icefschools-1"

    #Send over updated data to GCS
    buckets.send_to_gcs(bucket_name,save_path="",frame=assessment_results_group,frame_name="assessment_results_group.csv")
    buckets.send_to_gcs(bucket_name,save_path="",frame=assessment_results_combined,frame_name="assessment_results_combined.csv")
    buckets.send_to_gcs(bucket_name_views,save_path="",frame=illuminate_assessment_results,frame_name="illuminate_assessment_results.csv")
    buckets.send_to_gcs(bucket_name, save_path='', frame=assessments_metadata, frame_name='assessments_metadata.csv') #send over metadata for this year everytime


get_assessment_results(spark,
                        years_data=os.getenv('YEARS_DATA', '24-25'),
                        start_date=os.getenv('START_DATE', '2025-07-01'))