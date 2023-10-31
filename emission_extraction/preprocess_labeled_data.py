import pandas as pd
import random
import re
import requests

random.seed(0) 

emission_labeled_data = '/Users/fiona/github/emission/qc_emission_labeled_data.xlsx'
emission_labeled_df = pd.read_excel(emission_labeled_data)
print(f"emission_labeled_df shape: {emission_labeled_df.shape}")

def take_rel_cols(df):
    relevant_cols = [
        'SECTOR',
        'COMPANY_NAME',
        'SURVEY_NAME',
        'SUBMITTED_DATE',
    ]
    patterns = '(_DISCLOSURE_STATUS|_INPUT|_SOURCE|_DATA_VERIFICATION)'
    filtered_df = df.filter(regex=patterns)

    result_df = df[relevant_cols + list(filtered_df.columns)]
    return result_df

def extract_doc_id(text):
    if pd.isna(text):
        return None  # Handle np.nan by returning None

    # Define the regex pattern to search for integers after "https://compass.esgbook.com/documents/"
    pattern1 = r'url:https://compass\.esgbook\.com/documents/(\d+)'
    # Define the regex pattern to search for "compass doc id:" and extract integers
    pattern2 = r'compass doc id:\s*(\d+)'

    text = text.lower()
    # Use re.search to find the first pattern in the text
    match = re.search(pattern1, text)

    if match:
        return int(match.group(1))
    else:
        matches = re.findall(pattern2, text)
        if matches:
            return int(matches[0])
        else:
            return None

def check_same(row):
    row_list = [int(x) for x in row if not pd.isna(x)]
    if len(set(row_list)) == 1:
        return row_list[0]
    return None

def take_rel_rows(df, input_n=2):
    # Lower and remove '_' and '-' from column names containing '_SOURCE'
    source_columns = [col for col in df.columns if '_SOURCE' in col]
    for col in source_columns:

        new_col_name = col + "_compass_doc_id"
        df[new_col_name] = None  # Initialize the new column with None values

        for index, row in df.iterrows():
            df.loc[index, new_col_name] = extract_doc_id(row[col])


    df_columns = [col for col in df.columns if '_compass_doc_id' in col]
    df_filtered = df[df_columns]

    checklist = []
    for index, row in df_filtered.iterrows():
        checklist.append(check_same(row))
    df['same_report'] = pd.Series(checklist)

    # Filter for rows where SUBMITTED_DATE > 2020
    df['SUBMITTED_DATE'] = pd.to_datetime(df['SUBMITTED_DATE'])
    df = df[df['SUBMITTED_DATE'] > '2020-01-01']

    # Filter rows where the year in 'SURVEY_YEAR' is >= 2017
    df['SURVEY_YEAR'] = df['SURVEY_NAME'].str.extract(r'(\d{4})', expand=False)
    df = df[df['SURVEY_YEAR'].astype(int) >= 2017]

    # Find unique SECTOR values
    unique_sectors = df['SECTOR'].unique()

    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame(columns=df.columns)

    # For each sector, randomly select 2 companies
    for sector in unique_sectors:
        sector_df = df[df['SECTOR'] == sector]
        if len(sector_df) >= input_n:
            sector_df = sector_df.sample(n=input_n)
        elif len(sector_df) > 0:
            sector_df = sector_df.sample(n=len(sector_df))
        result_df = pd.concat([result_df, sector_df])

    # Round SUBMITTED_DATE to the nearest minute for better comparison
    result_df['SUBMITTED_DATE'] = result_df['SUBMITTED_DATE'].dt.round('min')
    
    # Remove duplicated companies based on the latest 'SUBMITTED_DATE'
    result_df = result_df.sort_values(by='SUBMITTED_DATE', ascending=False)
    output_df = result_df.drop_duplicates(subset='COMPANY_NAME')

    return output_df

pp_emission_labeled_df = take_rel_cols(emission_labeled_df)
filtered_emission_labeled_df = take_rel_rows(pp_emission_labeled_df)
print(f"filtered_emission_labeled_df shape: {filtered_emission_labeled_df.shape}")

# save as xlsx
filtered_emission_labeled_df.to_excel('/Users/fiona/github/emission/preprocessed_emission_labeled_data.xlsx')


# extract compass compass doc id, url to download
auth_pw = 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ijd1RlhtLXI4YnFhMTF5c1dKNjNoTSJ9.eyJ1c2VyX2VtYWlsIjoiZmlvbmEubHh5QGVzZ2Jvb2suY29tIiwiaXNzIjoiaHR0cHM6Ly9lc2ctYm9vay1kYXRhLWNvbGxlY3Rpb24uZXUuYXV0aDAuY29tLyIsInN1YiI6ImVtYWlsfDY0MWM1YzRiZTc4MjJhMTQxNjVjNGJhNiIsImF1ZCI6WyJodHRwczovL2VzZ2Jvb2stZGF0YS10ZWNobm9sb2d5LmNvbSIsImh0dHBzOi8vZXNnLWJvb2stZGF0YS1jb2xsZWN0aW9uLmV1LmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2OTg3OTE2MDMsImV4cCI6MTY5OTA1MDgwMywiYXpwIjoibkllVkhxTmNJNW11dnJJY0tiYVFzdms3U1BtZlJUQXQiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIn0.ZzdMnmz-YpsxJE0GmyLkY2sPD9hzapLtc6703PHR5R-T4LQbiKSq9fsqMqU80x3GESz41bIO2ahT7bfwIZiiKahXxYzz_UeRl0Su5LCyBwz7UDXriTZe0hBdq0l6eJxK5ihvjDyrxT04a3KmlFoC8DaA8VzOvPfZjA0JA3Lx6vWwtVSs_KoE8ULsBXGL0fkw43zGlH3raYTgT7oszpEzPHCaudOKPg2Q953z5BQ8GGJqTI8wylLLu5Q9h9sc-3zJGi2eMzg0h5zuOaxzunnErpyARPbXvuZ5oZiXKz7MDQPg_Kq3p20z1KOdYahvHkZxGYquATwq2ybXBeJDR79hrw'
url_string = 'https://api.compass.esgbook.com/documents/'

headers = {
    'Authorization': auth_pw[0],
    'Content-Type': 'application/pdf',
}

def download_single_pdf(input_url: str, output_path:str, auth_headers: str):
    """
    sample url: 'https://api.compass.esgbook.com/documents/2815574'
    """
    response = requests.get(input_url, headers=auth_headers)
    doc_id = input_url.split('/')[-1]

    if response.status_code == 200:
        output_file_path = os.path.join(output_path, doc_id + '.pdf')
        print(f"output path: {output_file_path}")
        with open(output_file_path, 'wb') as f:
            f.write(response.content)
    else:
        print('Error accessing resource:', response.status_code)

unique_compass_doc_id = [int(doc_id) for doc_id in filtered_emission_labeled_df['same_report'].tolist() if not pd.isna(doc_id)]
unique_compass_doc_url = [url_string + str(doc_id) for doc_id in unique_compass_doc_id]

for i, url in enumerate(unique_compass_doc_url):
    download_single_pdf(url, "/Users/fiona/github/emission/sample_data", auth_headers=headers)
print(f"Download Completed!")
