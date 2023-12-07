import os
import time
import requests
import pandas as pd

from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
from io import BytesIO
from zipfile import ZipFile

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/"

def find_files(output_dir, years, months, url=BASE_URL):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    url_list = [BASE_URL + str(year) + str(month).zfill(2) for year in years for month in months]

    with requests.Session() as session:

        for url in url_list:

            print(f"Downloading files at {url}")

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
                        df = pd.read_csv(zip_ref.open(csv_file), sep=';', encoding='latin1', on_bad_lines='skip', usecols=['CD_OPERADORA', 'COBERTURA_ASSIST_PLAN'])

                df['COBERTURA_ASSIST_PLAN'] = df.COBERTURA_ASSIST_PLAN.astype('category')
                
                df_list.append(df)

            df_all = pd.concat(df_list, ignore_index=True)

            processed_csv_path = os.path.join(output_dir, url[-6:] + '.csv')
                
            df_all.to_csv(processed_csv_path, index=False)

if __name__ == "__main__":
    start_time = time.time()
    find_files('data/', [2022], [3, 6])
    end_time = time.time()

    print(f"Execution time: {round((end_time - start_time)/60, 2)} seconds")