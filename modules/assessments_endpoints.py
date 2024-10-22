import requests
import pandas as pd
import json
import logging
from .config import base_url_illuminate


def get_all_assessments(access_token):
    # Set the initial page and an empty DataFrame to store all results
    page = 1
    all_results = pd.DataFrame()

    # Base URL and headers for API requests
    url_ext = 'Assessments/?page={}&limit=5000'
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
            logging.info(f'Looped through {page} pages. Results for getting all assessments output into results frame')
            break

        # Move to the next page
        page += 1
    
    return(all_results)


# def get_specific_assessment_standard(access_token, _id):
#    # Base URL and headers for API requests
   
#     url_ext = f'AssessmentAggregateStudentResponsesStandard/?page=1&assessment_id={_id}&limit=5000'

#     headers = {
#         "Authorization": f"Bearer {access_token}"
#     }

#     response = requests.get(base_url_illuminate + url_ext, headers=headers)

#     if response.status_code == 200:
#         results = json.loads(response.content)

#         num_results = results['num_results']


#     return(response)



def get_assessment_scores(access_token, _id, standard_or_no_standard):
    
    if standard_or_no_standard == 'No_Standard':
        url_ext = f'AssessmentAggregateStudentResponses/?page=1&assessment_id={_id}&limit=5000'
    elif standard_or_no_standard == 'Standard':
        url_ext = f'AssessmentAggregateStudentResponsesStandard/?page=1&assessment_id={_id}&limit=5000'
    else:
        print('Wrong variable for standard_or_no_standard')

    headers = {
    "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(base_url_illuminate + url_ext, headers=headers)
    
    logging_list = []
    df_result = pd.DataFrame()  # Initialize df_result as an empty DataFrame

    # log the status_code, first test date, last test date, and Total Number of Tests
    r = response.status_code

    if r == 200:
        # if call is successful
        results = json.loads(response.content)
        num_results = results['num_results']

        if num_results == 0:
            # assessment id has returned no results, append the following data to a list
            d = [_id, standard_or_no_standard, r, '', '', '', num_results]
            logging_list.append(d)
        else:
            df_result = pd.DataFrame(results['results'])
            df_result = df_result.sort_values(by='date_taken')
            df_result.reset_index(drop=True, inplace=True)
            df_result['percent_correct'] = df_result['percent_correct'].astype(float)
            df_result['percent_correct'] = df_result['percent_correct'].round()
            df_result['percent_correct'] = df_result['percent_correct'].astype(int)
            df_result['date_taken'] = pd.to_datetime(df_result['date_taken'])
            df_result['Standard_No_Standard'] = standard_or_no_standard

            title = df_result.iloc[0]['title']
            first_test = df_result.iloc[0]['date_taken']
            last_test = df_result.iloc[-1]['date_taken']

            d = [_id, standard_or_no_standard, r, title, first_test, last_test, num_results]
            logging_list.append(d)

    else:
        # If API call is not 200
        d = [_id, standard_or_no_standard, r, '', '', '', 0]
        logging_list.append(d)

    t = pd.DataFrame(logging_list, columns=['Assessment_ID', 'Standard_No_Standard', 'Status_Code', 'Assessment_Name', 'First_Test_Date', 'Last_Test_Date', 'Num_Of_Tests'])
    
    return(df_result, t)
    #now change df_result to append to an empty list, and same for t




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

    logging.info(f'Returniing the frame and log for loop_through_assessment_scores {standard_or_no_standard}')
 
    return(test_results, log_results)