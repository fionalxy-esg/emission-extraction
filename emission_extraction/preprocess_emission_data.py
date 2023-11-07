import re
import pandas as pd

class PreprocessEmissionData:
    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def check_same(row):
        row_list = [int(x) for x in row if not pd.isna(x)]
        if len(set(row_list)) == 1:
            return row_list[0]
        return None

    def take_rel_rows(self, df, input_n=2):
        # Lower and remove '_' and '-' from column names containing '_SOURCE'
        source_columns = [col for col in df.columns if '_SOURCE' in col]
        for col in source_columns:

            new_col_name = col + "_compass_doc_id"
            df[new_col_name] = None  # Initialize the new column with None values

            for index, row in df.iterrows():
                df.loc[index, new_col_name] = self.extract_doc_id(row[col])


        df_columns = [col for col in df.columns if '_compass_doc_id' in col]
        df_filtered = df[df_columns]

        checklist = []
        for index, row in df_filtered.iterrows():
            checklist.append(self.check_same(row))
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