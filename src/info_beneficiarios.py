import os
import requests
import tempfile
import zipfile
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/"

def find_files(start_year, end_year):
    year_url_list = [BASE_URL + str(year) for year in range(start_year, end_year + 1)]
    ym_url_list = [year_url + str(month).zfill(2) for year_url in year_url_list for month in range(1, 13)]
    zip_urls = []

    for url in ym_url_list:
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.endswith(".zip"):
                zip_urls.append(os.path.join(url, href))
    return zip_urls

def fetch_files(zip_urls):
    with tempfile.TemporaryDirectory(dir='.') as temp_dir:
        for url in zip_urls:
            try:
                response = requests.get(url)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {url}: {e}")
                continue

            zip_file_path = os.path.join(temp_dir, os.path.basename(url))

            with open(zip_file_path, 'wb') as f:
                f.write(response.content)

            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

        all_csv_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.csv')]
        df = pd.concat((pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip') for f in all_csv_files), ignore_index=True)
    
    return df

def download_data(start_year, end_year):
    zip_urls = find_files(start_year, end_year)
    df = fetch_files(zip_urls)
    
    return df

if __name__ == "__main__":

    df = download_data(2015, 2024)
    print(df.head(10))