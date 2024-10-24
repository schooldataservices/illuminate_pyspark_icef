from .assessments_endpoints import *



def alter_test_results_frame(access_token, test_results):

    #add in grade_level column
    try:
        assessments = get_all_assessments_metadata(access_token) 
        logging.info('Succesfully retrieved all assessments to add in grade levels to test_results frame')
    except Exception as e:
        logging.error(f'Unable to get all assessments due to {e}')

    grade_levels = assessments[['assessment_id', 'grade_levels']].drop_duplicates()
    test_results = pd.merge(test_results, grade_levels, on='assessment_id', how='left')
    test_results = test_results.sort_values(by='date_taken')
    return(test_results)