import requests
import os

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