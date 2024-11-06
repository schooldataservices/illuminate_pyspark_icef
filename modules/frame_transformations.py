from .assessments_endpoints import *
from airflow.exceptions import AirflowException
import os



def add_in_grade_levels(test_results, access_token):

    #add in grade_level column
    try:
        assessments = get_all_assessments_metadata(access_token) 
        logging.info('Succesfully retrieved all assessments to add in grade levels to test_results frame')
    except Exception as e:
        logging.error(f'Unable to get all assessments due to {e}')
        raise AirflowException(f"Error occurred while getting API token: {e}")

    grade_levels = assessments[['assessment_id', 'grade_levels']].drop_duplicates()
    test_results = pd.merge(test_results, grade_levels, on='assessment_id', how='left')
    test_results = test_results.sort_values(by='date_taken')
    return(test_results)


#Add in the Curriculum and Unit Columns via string matching from the Assessment Name

def add_in_curriculum_unit_col(df):   #Checkpoints currently do not have anything for curriculum

    curriculum_dict = {
        'IM': 'IM',
        'Science': 'Science',
        'Into Reading': 'Into Reading',
        'History': 'History',
        'Geometry': 'Geometry',
        'English': 'English',
        'Algebra': 'Algebra'
    }

    # Initialize the Curriculum column with empty strings
    df['curriculum'] = ''

    # Loop through the dictionary to populate the Curriculum column
    for keyword, label in curriculum_dict.items():
        df.loc[df['title'].str.contains(keyword, case=False), 'curriculum'] = label
    
    df['unit'] = df['title'].str.extract(r'(Unit \d+|Module \d+)', expand=False)

    return(df)


def create_test_type_column(frame):
    frame['test_type'] = frame['title'].apply(
        lambda x: 'checkpoint' if 'checkpoint' in str(x).lower()
        else 'assessment' if 'assessment' in str(x).lower()
        else 'unknown'
    )
    return frame



def create_test_results_view(test_results, access_token, SY):

    test_results = add_in_grade_levels(test_results, access_token)
    test_results = add_in_curriculum_unit_col(test_results)

    test_results['year'] = SY

    test_results['test_type'] = ''
    test_results = create_test_type_column(test_results)

    #Cut down cols, and change naming col names
    test_results.loc[:, 'proficiency'] = test_results['performance_band_level'] + ' ' + test_results['performance_band_label'] #add in proficiency column
    test_results = test_results[[ 'assessment_id', 'year', 'date_taken', 'grade_levels', 'local_student_id', 'test_type', 'curriculum', 'unit', 'title', 'standard_code', 'percent_correct', 'performance_band_level', 'performance_band_label', 'proficiency', 'mastered', '__count', 'last_update']]

    test_results = test_results.rename(columns={'grade_levels': 'grade',
                                                'percent_correct': 'score'
                                                  })
    #changes occurs in place
    test_results.loc[test_results['grade'] == 'K', 'grade'] = 0
    
    return(test_results)


def send_to_local(save_path, frame, frame_name):
        
    if not frame.empty:
        frame.to_csv(os.path.join(save_path, frame_name), index=False)
        logging.info(f'{frame_name} saved to {save_path}')
    else:
        logging.info(f'No data present in {frame_name} file')




# Columns unique to test_results_no_standard: {'version', 'version_label'}
# Columns unique to test_results_standard: {'academic_benchmark_guid', 'standard_code', 'standard_description'}

def bring_together_test_results(test_results_no_standard, test_results_standard):

    df = pd.concat([test_results_standard, test_results_no_standard])
    df['standard_code'] = df['standard_code'].fillna('percent')

    return(df)