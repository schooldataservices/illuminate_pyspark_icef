from .assessments_endpoints import *
from airflow.exceptions import AirflowException
import os
import re



def add_in_grade_levels(test_results):

    # temp_path = r'C:\Users\samuel.taylor\Downloads\Student_Rosters.txt'
    vm_path = '/home/icef/powerschool/Student_Rosters.txt'

    GL_mapping = pd.read_table(vm_path)[['STUDENTS.Student_Number', 'STUDENTS.Grade_Level']]
    GL_mapping = GL_mapping.rename(columns={'STUDENTS.Student_Number': 'local_student_id', 
                                            'STUDENTS.Grade_Level': 'grade_levels'})
    GL_mapping['local_student_id'] = GL_mapping['local_student_id'].astype(str)
    test_results = pd.merge(test_results, GL_mapping, on='local_student_id', how='left')
    return(test_results)


def add_in_unit_col(df):

    # Extract 'Unit', 'Interim', or 'Module' with a number and populate the 'unit' column
    df['unit'] = df['title'].str.extract(r'(Interim(?: Assessment)?(?: #?\d+)?|Interim \d+|Unit \d+|Module \d+)', expand=False)

    #Exception for the IA title standalone, #Remove the assessment and hastag form the unit columns
    df.loc[df['title'].str.contains(rf'\b{re.escape("IA")}\b', case=False), 'unit'] = 'Interim 1'
    df['unit'] = df['unit'].str.replace(r'Assessment|#', '', regex=True).str.strip()
    df['unit'] = df['unit'].str.replace(r'\s+', ' ', regex=True)

    #single interim value in unit needs to align with others
    df['unit'] = df['unit'].replace('Interim', 'Interim 1')

    unit_col_sorting = {'Module 1':  '1',
                        'Module 2' : '2',
                        'Module 3' : '3',
                        'Interim 1' : '4',
                        'Unit 1' : '1',
                        'Unit 2': '2',
                        'Unit 3' : '3'}
    
    df['unit_labels'] = df['unit'].map(unit_col_sorting)

    return(df)




#Add in the Curriculum and Unit Columns via string matching from the Assessment Name
def add_in_curriculum_col(df):
    curriculum_dict = {
        #Moreso HS mapping
        'Geometry': 'Geometry',
        'English': 'English',
        'Algebra I': 'Algebra I',
        'Algebra II': 'Algebra II',
        'Algebra 1': 'Algebra I',
        'Algebra 11': 'Algebra II',
        'PreCal': 'Pre-Calculus',
        'Pre Cal': 'Pre-Calculus',
        'Statistics': 'Statistics',
        'Stats': 'Statistics',
        'Biology': 'Biology',
        'Physics': 'Physics',
        'Government': 'Government',

        #More so Elem Mapping. Less defined
        'IM': 'Math',
        'Checkpoint': 'Checkpoint',
        'Science': 'Science',
        'Into Reading': 'ELA',
        'ELA': 'ELA',
        'Math': 'Math',
        'Quantitative': 'Math',
        'Social Studies': 'History',
        'History': 'History'
    }

    # Initialize the Curriculum column with empty strings
    df['curriculum'] = ''

    # Loop through the dictionary to populate the Curriculum column
    for keyword, label in curriculum_dict.items():
        # Use word boundaries only for the 'IM' keyword to match it as a standalone word
        if keyword == 'IM' or keyword == 'Checkpoint':
            df.loc[df['title'].str.contains(rf'\b{re.escape(keyword)}\b', case=False), 'curriculum'] = label
        else:
            df.loc[df['title'].str.contains(keyword, case=False), 'curriculum'] = label

    #Impossible to string match this one - assessment id 115533 (Grade 8 Interim Assessment #1)
    df.loc[df['assessment_id'] == '115533', 'curriculum'] = 'Science'

    # Interim Assessment #1 | Grade 11 | U.S. History
    df.loc[df['assessment_id'] == '141493', 'curriculum'] = 'US History'

    df.loc[df['assessment_id'] == '141492', 'curriculum'] = 'World History'

    #Environmental Science Interim Assessment #1
    df.loc[df['assessment_id'] == '141508', 'curriculum'] = 'Environmental Science'

    #Algebra 1 IA #1 (Sequences + IM) 
    df.loc[df['assessment_id'] == '141441', 'curriculum'] = 'Algebra I'

    #Biology for Freshman, otherwise Anatomy
    df.loc[(df['assessment_id'] == '141506') & (df['grade_levels'] == 9), 'curriculum'] = 'Biology'
    df.loc[(df['assessment_id'] == '141506') & (df['grade_levels'] != 9), 'curriculum'] = 'Anatomy'

    return df



def create_test_type_column(frame):
    frame['test_type'] = ''

    frame['test_type'] = frame['title'].apply(
        lambda x: 'checkpoint' if 'checkpoint' in str(x).lower()
        else 'assessment' if 'assessment' in str(x).lower()
        else 'unknown'
    )
    return frame



def create_test_results_view(test_results, SY):

    test_results = add_in_grade_levels(test_results)
    test_results = add_in_curriculum_col(test_results)
    test_results = add_in_unit_col(test_results)
    test_results = create_test_type_column(test_results)

    test_results['year'] = SY


    #Add in proficiency col, re-order results, and change names
    test_results.loc[:, 'proficiency'] = test_results['performance_band_level'] + ' ' + test_results['performance_band_label'] #add in proficiency column
    test_results['data_source'] = 'illuminate'

    test_results = test_results[['data_source', 'assessment_id', 'year', 'date_taken', 'grade_levels', 'local_student_id', 'test_type', 'curriculum', 'unit', 'unit_labels', 'title', 'standard_code', 'percent_correct', 'performance_band_level', 'performance_band_label', 'proficiency', 'mastered', '__count']]

    #need a validation function on the percent_correct coming out to zero. Ensure there are points possible. Or could calc percent here. 

    test_results = test_results.rename(columns={'grade_levels': 'grade',
                                                'percent_correct': 'score'
                                                  })
    #changes occurs in place
    test_results.loc[test_results['grade'] == 'K', 'grade'] = 0
    #For safe measures
    test_results = test_results.drop_duplicates()
    
    return(test_results)


def append_prior_year(prior_year_file_path, frame, prior_year_file_name):

    prior_year_file_path = os.path.join(prior_year_file_path, prior_year_file_name)

    # Check if prior year file exists
    if os.path.exists(prior_year_file_path):
        logging.info(f'Prior year file found at {prior_year_file_path}. Appending data.')
        # Read the prior year file into a DataFrame
        prior_year_frame = pd.read_csv(prior_year_file_path, encoding='ISO-8859-1')
        
        # Append prior year data to the current frame
        combined_frame = pd.concat([prior_year_frame, frame], ignore_index=True)
        
        # Optional: Remove duplicate rows if needed
        combined_frame = combined_frame.drop_duplicates()
        logging.info(f'Prior year data appended successfully.')

        # Convert the column to datetime format
        combined_frame['date_taken'] = pd.to_datetime(combined_frame['date_taken'])

        # If you want to remove the time part and keep only the date
        combined_frame['date_taken'] = combined_frame['date_taken'].dt.date

    else:
        logging.warning(f'Prior year file not found at {prior_year_file_path}. Using current frame only.')
        combined_frame = frame

    return combined_frame



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
    df = df.drop_duplicates()

    return(df)


# def add_in_grade_levels(test_results, access_token):

#     #add in grade_level column
#     try:
#         assessments = get_all_assessments_metadata(access_token) 
#         logging.info('Succesfully retrieved all assessments to add in grade levels to test_results frame')
#     except Exception as e:
#         logging.error(f'Unable to get all assessments due to {e}')
#         raise AirflowException(f"Error occurred while getting API token: {e}")

#     grade_levels = assessments[['assessment_id', 'grade_levels']].drop_duplicates()
#     test_results = pd.merge(test_results, grade_levels, on='assessment_id', how='left')
#     test_results = test_results.sort_values(by='date_taken')
#     return(test_results)


#fixing historical grade, unit, and curriculum column due to shortcomings on Students Rosters being limited ot this year

# def manual_grade_unit_curriculum_fix():

#     gl_fix = pd.read_csv('/home/g2015samtaylor/airflow/git_directory/Illuminate/modules/illuminate_historical_column_fixes_2324.csv')
#     v = pd.read_csv('/home/g2015samtaylor/backups/illuminate/assessment_results_view_2324.csv')

#     #drop columns in view in order to bring in new ones

#     # subsidize cols down and change naming convention
#     gl_fix = gl_fix[['title', 'updated "grade"', 'updated "unit"', 'updated "curriculum"']]

#     gl_fix = gl_fix.rename(columns= {'updated "grade"': 'grade', 
#                             'updated "unit"': 'unit', 
#                             'updated "curriculum"': 'curriculum'})



#     # Create dictionaries for each column in gl_fix based on the title, and map to the title, retain existing values as whatever was there prior

#     col_list = ["grade", "unit", "curriculum"]

#     for col in col_list:
#         created_dict = dict(zip(gl_fix['title'], gl_fix[col]))

#         v[col] = v['title'].map(created_dict).fillna(v[col])

#     v.to_csv('//home/g2015samtaylor/views/illuminate_assessment_results_historical.csv', index=False)

#     return(v)


def merge_excel_with_assessments_master_on_title(access_token):


    fixes = pd.read_csv('/home/g2015samtaylor/airflow/git_directory/Illuminate/illuminate_historical_column_fixes_2324.csv')
    # v = pd.read_csv('/home/g2015samtaylor/views/illuminate_assessment_results_historical.csv') 

    fixes = fixes.rename(columns={'current "grade"': 'current grade',
                        'updated "grade"': 'updated grade',
                        'updated "curriculum"': 'updated curriculum'})

    fixes = fixes[['title', 'current grade', 'updated grade', 'curriculum', 'updated curriculum']]

    fixes['title'] = fixes['title'].str.strip()


    #Need sepearate string matching for current grade
    assessments_df, assessment_id_list = get_all_assessments_metadata(access_token)
    assessments_df['title'] = assessments_df['title'].str.strip()

    #Merge masters assessments frame on fixes to see what is missing 
    temp = pd.merge(assessments_df, fixes, on='title', how='right', indicator=True)

    return(temp, assessments_df)