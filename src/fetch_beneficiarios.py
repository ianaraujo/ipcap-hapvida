import os
import time
import requests
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
from io import BytesIO
from zipfile import ZipFile

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/"

def download_zip(session, url):
    try:
        with session.get(url, stream=True) as response:
            response.raise_for_status()
            
            with ZipFile(BytesIO(response.content), 'r') as zip:
                csv_file = zip.namelist()[0] 
                
                df = pd.read_csv(zip.open(csv_file), sep=';', encoding='latin1', on_bad_lines='skip', usecols=['#ID_CMPT_MOVEL', 'CD_OPERADORA', 'COBERTURA_ASSIST_PLAN', 'SG_UF', 'DE_FAIXA_ETARIA', 'QT_BENEFICIARIO_ATIVO'])
                
                df['COBERTURA_ASSIST_PLAN'] = np.where(df['COBERTURA_ASSIST_PLAN'] == 'Médico-hospitalar', 1, 0)
                
                return df
    
    except HTTPError as e:
        print(f"Failed to download or process file from {url}: {e}")
        
        return None

def process_url(session, url, output_dir):
    zip_urls = []

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

    df_all = pd.concat((download_zip(session, zurl) for zurl in zip_urls if zurl is not None), ignore_index=True)

    processed_csv_path = os.path.join(output_dir, url[-6:] + '.csv')
    
    df_all.to_csv(processed_csv_path, index=False)
    
    print(f"File saved to: {processed_csv_path}")


def find_files(output_dir, years, months):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    url_list = [BASE_URL + str(year) + str(month).zfill(2) for year in years for month in months]

    with requests.Session() as session:
        for url in url_list:
            process_url(session, url, output_dir)

if __name__ == "__main__":
    start_time = time.time()
    find_files('data/beneficiarios/', range(2018, 2024), [3, 6, 9, 12])
    end_time = time.time()

    print(f"Execution time: {round((end_time - start_time)/60, 2)} minutes")
