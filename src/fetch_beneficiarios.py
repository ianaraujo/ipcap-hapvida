import os
import time
import requests
import pandas as pd

from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
from io import BytesIO
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/"

def process_url(session, url, output_dir):
    zip_urls = []
    df_list = []

    try:
        response = session.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a"):
            href = link.get("href")
            
            if href and href.endswith(".zip"):
                zip_urls.append(os.path.join(url, href))
    
    except HTTPError as e:
        print(f"Access Error: {e}")

    # dowload the .zip files inside zip_urls
    for zurl in zip_urls:
        with session.get(zurl, stream=True) as response:
            response.raise_for_status()

            with ZipFile(BytesIO(response.content), 'r') as zip_ref:
                csv_file = zip_ref.namelist()[0]  # assuming there's only one CSV file in the zip
                
                df = pd.read_csv(
                    zip_ref.open(csv_file), sep=';', encoding='latin1', on_bad_lines='skip', 
                    usecols=['CD_OPERADORA', 'COBERTURA_ASSIST_PLAN'], 
                    dtype={'CD_OPERADORA': 'int32', 'COBERTURA_ASSIST_PLAN': 'category'}
                )
        
        df_list.append(df)

    df_all = pd.concat(df_list, ignore_index=True)

    processed_csv_path = os.path.join(output_dir, url[-6:] + '.csv')
        
    df_all.to_csv(processed_csv_path, index=False)

def find_files(output_dir, years, months):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    url_list = [BASE_URL + str(year) + str(month).zfill(2) for year in years for month in months]

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=6) as executor:
            executor.map(process_url, [session]*len(url_list), url_list, [output_dir]*len(url_list))

if __name__ == "__main__":
    start_time = time.time()
    find_files('data/', [2022, 2023], [3, 6, 9, 12])
    end_time = time.time()

    print(f"Execution time: {round((end_time - start_time)/60, 2)} minutes")