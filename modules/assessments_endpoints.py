import requests
import pandas as pd
import json
import logging
from datetime import datetime
from .config import base_url_illuminate

current_date = datetime.now()
current_date = current_date.strftime('%Y-%m-%d')
#Currently have url_args hardcoded in each function param to be date filtered


def get_all_assessments_metadata(access_token):
    # Set the initial page and an empty DataFrame to store all results
    page = 1
    all_results = pd.DataFrame()

    # Base URL and headers for API requests
    url_ext = 'Assessments/?page={}&limit=1000'
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    #To ensure all pages are looped through properly
    while True:
        # Make the API request with the current page number
        response = requests.get(base_url_illuminate + url_ext.format(page), headers=headers)
        results = json.loads(response.content)
    
        # Check if the response is successful
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.status_code}")
            break

        # Convert the results of the current page to a DataFrame and append to all_results
        page_results = pd.DataFrame(results['results'])
        all_results = pd.concat([all_results, page_results], ignore_index=True)

        # Check if we've retrieved all pages
        if page >= results['num_pages']:
            logging.info(f'Looped through {page} pages. Results for func get_all_assessments_metadata output into DataFrame')
            break

        # Move to the next page
        page += 1
    
    return(all_results)


def get_single_assessment(access_token, _id, standard_or_no_standard):
    # Set the initial page and an empty DataFrame to store all results
    page = 1
    all_results = pd.DataFrame()

    url_args = f'?page={page}&assessment_id={_id}&limit=1000&date_taken_start=2024-07-01&date_taken_end={current_date}'

    # Determine the endpoint based on the standard_or_no_standard parameter
    if standard_or_no_standard == 'No_Standard':
        url_ext = f'AssessmentAggregateStudentResponses/{url_args}'
    elif standard_or_no_standard == 'Standard':
        url_ext = f'AssessmentAggregateStudentResponsesStandard/{url_args}'
    else:
        print('Wrong variable for standard_or_no_standard')
        return None  # Exit the function if the parameter is incorrect

    headers = {
        "Authorization": f"Bearer {access_token}"
    }


    while True:
        # Make the API request with the current page number
        response = requests.get(base_url_illuminate + url_ext, headers=headers)

        # Check if the response is successful
        if response.status_code != 200:
            logging.error(f"Error fetching page {page}: {response.status_code}")
            break

        # Parse the response content
        results = json.loads(response.content)

        # Convert the results of the current page to a DataFrame and append to all_results
        page_results = pd.DataFrame(results['results'])
        all_results = pd.concat([all_results, page_results], ignore_index=True)

        # Check if we've retrieved all pages
        if page >= results.get('num_pages', 1):  # Default to 1 if 'num_pages' is not present
            logging.info(f'Looped through {page} pages for assessment ID {_id}. Results output into DataFrame.')
            break

        # Move to the next page
        page += 1
        # Update the URL for the next page
        url_ext = f'AssessmentAggregateStudentResponses{"Standard" if standard_or_no_standard == "Standard" else ""}/{url_args}'

    return all_results



def get_assessment_scores(access_token, _id, standard_or_no_standard):
    # Initialize variables
    page = 1
    logging_list = []  # List to store logging information
    df_results_list = []  # List to collect results DataFrames
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    while True:
        # Update the URL arguments to reflect the current page number
        url_args = f'?page={page}&assessment_id={_id}&limit=1000&date_taken_start=2024-07-01&date_taken_end={current_date}'

        # Determine the endpoint based on the standard_or_no_standard parameter
        if standard_or_no_standard == 'No_Standard':
            url_ext = f'AssessmentAggregateStudentResponses/{url_args}'
        elif standard_or_no_standard == 'Standard':
            url_ext = f'AssessmentAggregateStudentResponsesStandard/{url_args}'
        elif standard_or_no_standard == 'Group':
            url_ext = f'AssessmentAggregateStudentResponsesGroup/{url_args}'
        else:
            print('Wrong variable for standard_or_no_standard')
            return None, None  # Exit the function if the parameter is incorrect

        # Log the complete URL and make the API request
        logging.info(base_url_illuminate + url_ext)
        response = requests.get(base_url_illuminate + url_ext, headers=headers)
        r = response.status_code
        logging.info(f'The status code for assessment_id {_id} is {r}')

        # Handle successful API response
        if r == 200:
            results = json.loads(response.content)
            num_results = results['num_results']
            num_pages = results['num_pages']
            logging.info(f'Here is the num of pages for {_id} id - {num_pages} pages')

            if num_results == 0:
                # Log and exit if no results are found
                d = [_id, standard_or_no_standard, r, '', num_pages, num_results]
                logging_list.append(d)
                logging.info(f'Results are NOT present for _id {_id}, num_results {num_results}, page {page}')
                break
            else:
                # Process and store the results
                logging.info(f'Results are present for _id {_id}, num_results {num_results}, page {page}')
                df_page_results = pd.DataFrame(results['results'])
                df_page_results = df_page_results.sort_values(by='date_taken')
                df_page_results.reset_index(drop=True, inplace=True)
                df_page_results['percent_correct'] = df_page_results['percent_correct'].astype(float).round().astype(int)
                df_page_results['date_taken'] = pd.to_datetime(df_page_results['date_taken'])
                df_page_results['Standard_No_Standard'] = standard_or_no_standard
                df_results_list.append(df_page_results)

                if page == 1:  # Record details from the first page if there are results
                    title = df_page_results.iloc[0]['title']
                    d = [_id, standard_or_no_standard, r, title, num_pages, num_results]
                    logging_list.append(d)
        else:
            # Log unsuccessful API call and exit
            num_pages = 0
            num_results = 0
            logging.info(f'API call was not successful for {_id}')
            d = [_id, standard_or_no_standard, r, '', num_pages, num_results]
            logging_list.append(d)
            break

        # Check if all pages have been retrieved
        if page >= num_pages:
            logging.info(f'Completed fetching for assessment ID {_id}.')
            break

        # Increment the page number for the next iteration
        page += 1

    # Concatenate all DataFrames in the list into a single DataFrame
    df_result = pd.concat(df_results_list, ignore_index=True) if df_results_list else pd.DataFrame()
    t = pd.DataFrame(logging_list, columns=['Assessment_ID', 'Standard_No_Standard', 'Status_Code', 'Assessment_Name', 'Num_of_Pages', 'Num_Of_Tests'])

    return df_result, t







def loop_through_assessment_scores(access_token, id_list, standard_or_no_standard):

    print(f'The length of the ID_list is {len(id_list)}')

    df_list = []
    t_list = []

    # Iterate over the list of IDs and append df and t to their respective lists
    for _id in id_list: #Coming from config
        df, t = get_assessment_scores(access_token, _id, standard_or_no_standard)
        df_list.append(df)
        t_list.append(t)
        
    test_results = pd.concat(df_list)
    log_results = pd.concat(t_list)
    log_results['Standard_No_Standard'] = standard_or_no_standard
    log_results['last_update'] = pd.Timestamp.today().date()
    test_results['last_update'] = pd.Timestamp.today().date()
    test_results = test_results.reset_index(drop = True)

    logging.info(f'Returning the frame and log for loop_through_assessment_scores {standard_or_no_standard}')
 
    return(test_results, log_results)


def add_missing_assessments(assessment_id_list, new_ids):
    unique_assessment_ids = set(assessment_id_list)  # Convert to set for faster lookups
    for assessment_id in new_ids:
        if assessment_id not in unique_assessment_ids:
            unique_assessment_ids.add(assessment_id)
            logging.info(f'Adding missing assessment_id - {assessment_id}')
    return list(unique_assessment_ids)  # Convert back to list


# RAW CALL
# page = 1
# base_url_illuminate = 'https://icefps.illuminateed.com/live/rest_server.php/Api/'

# access_token, expires_in = get_access_token()

# headers = {
#     "Authorization": f"Bearer {access_token}"
# }

# _id = '115939'
# standard_or_no_standard = 'Standard'

# url_ext = f'Assessment/{_id}/View/'
# url_ext = f'PoolAssessmentAggregateStudentResponses/?page={1}'

# url_args = f'?page={page}&assessment_id={_id}&limit=1000&date_taken_start=2024-07-01&date_taken_end={current_date}'

# # Determine the endpoint based on the standard_or_no_standard parameter
# if standard_or_no_standard == 'No_Standard':
#     url_ext = f'AssessmentAggregateStudentResponses/{url_args}'
# elif standard_or_no_standard == 'Standard':
#     url_ext = f'AssessmentAggregateStudentResponsesStandard/{url_args}'
# elif standard_or_no_standard == 'Group':
#     url_ext = f'AssessmentAggregateStudentResponsesGroup/{url_args}'

# response = requests.get(base_url_illuminate + url_ext, headers=headers)
# r = response.status_code