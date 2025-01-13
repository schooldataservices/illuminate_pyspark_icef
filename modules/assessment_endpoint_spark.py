from .assessments_endpoints import *
from pyspark import RDD
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Request Parallelization") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")



def parallel_get_assessment_scores(access_token, assessment_id_list, standard_or_no_standard, start_date, end_date_override=None):
    """
    This function will parallelize the API calls using Spark's RDD operations.
    """

    # Define the function that will be applied in parallel to each assessment_id
    def fetch_assessment_scores(_id):
        return get_assessment_scores(access_token, _id, standard_or_no_standard, start_date, end_date_override)

    # Parallelize the assessment IDs to create an RDD
    rdd = spark.sparkContext.parallelize(assessment_id_list)

    # Use map to apply the fetch_assessment_scores function to each element of the RDD
    results = rdd.map(fetch_assessment_scores)

    # Collect the results back to the driver
    results_collected = results.collect()

    # After collect, you will have a list of tuples with (df_result, t)
    # Now you can combine them into a single DataFrame and log DataFrame
    all_results = []
    all_logs = []

    for df_result, t in results_collected:
        all_results.append(df_result)
        all_logs.append(t)

    # Concatenate all results DataFrames into a single DataFrame
    final_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
    final_logs = pd.concat(all_logs, ignore_index=True) if all_logs else pd.DataFrame()

    return final_df, final_logs
