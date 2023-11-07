import pandas as pd
import random
import os

from utils import download_single_pdf
from preprocess_emission_data import PreprocessEmissionData

random.seed(0) 

auth_pw = os.getenv("COMPASS_AUTH_PW")
headers = {
    'Authorization': auth_pw[0],
    'Content-Type': 'application/pdf',
}


emission_labeled_data = './preprocess_emission_labeled_data.xlsx'

emission_labeled_df = pd.read_excel(emission_labeled_data)
print(f"emission_labeled_df shape: {emission_labeled_df.shape}")

preprocess_emission_data = PreprocessEmissionData()

pp_emission_labeled_df = preprocess_emission_data.take_rel_cols(emission_labeled_df)
filtered_emission_labeled_df = preprocess_emission_data.take_rel_rows(pp_emission_labeled_df)
print(f"filtered_emission_labeled_df shape: {filtered_emission_labeled_df.shape}")

# save as xlsx
filtered_emission_labeled_df.to_excel('./preprocess_emission_labeled_data.xlsx')


# extract compass compass doc id, url to download
url_string = 'https://api.compass.esgbook.com/documents/'

unique_compass_doc_id = [int(doc_id) for doc_id in filtered_emission_labeled_df['same_report'].tolist() if not pd.isna(doc_id)]
unique_compass_doc_url = [url_string + str(doc_id) for doc_id in unique_compass_doc_id]

for i, url in enumerate(unique_compass_doc_url):
    download_single_pdf(url, "/Users/fiona/github/emission/sample_data", auth_headers=headers)
print(f"Download Completed!")


# filter relevant data columns for quantiphi
emission_labeled_data = './preprocess_emission_labeled_data.xlsx'
emission_labeled_df = pd.read_excel(emission_labeled_data)

filtered_df = emission_labeled_df[~emission_labeled_df['same_report'].isna()]

suffixes_to_drop = ["_SOURCE", "_DATA_VERIFICATION", "_compass_doc_id", 'same_report', 'SURVEY_YEAR']
columns_to_drop = [col for col in filtered_df.columns if col.endswith(tuple(suffixes_to_drop))]
output_df = filtered_df.drop(columns=columns_to_drop)

output_df.to_excel('./esgb_emission_labeled_data.xlsx')