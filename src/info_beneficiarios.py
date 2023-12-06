import os
import requests
import tempfile
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/202310"

# year_url_list = (BASE_URL + str(year) for year in range(start_year, end_year + 1))
# month_url_list = (year_url + str(month).zfill(2) for year_url in year_url_list for month in range(1, 13))

def find_files(url):

    zip_urls = []
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.endswith(".zip"):
                zip_urls.append(os.path.join(url, href))
    except HTTPError as e:
        print(f"Error accessing URL: {url}")
        print(f"Error message: {e}")
    
    return zip_urls

def fetch_files(zip_urls):
    df_list = []
    with tempfile.TemporaryDirectory(dir='.') as temp_dir:
        for url in zip_urls:
            try:
                response = requests.get(url)
                response.raise_for_status()

                zip_file_path = os.path.join(temp_dir, os.path.basename(url))

                with open(zip_file_path, 'wb') as f:
                    f.write(response.content)

                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    for member in zip_ref.namelist():
                        if member.endswith('.csv'):
                            zip_ref.extract(member, temp_dir)
                            df = pd.read_csv(os.path.join(temp_dir, member), sep=';', encoding='latin1', on_bad_lines='skip')
                            df_list.append(df)
                            os.remove(os.path.join(temp_dir, member))  # remove the csv file after processing
            except HTTPError as e:
                print(f"Error downloading file from URL: {url}")
                print(f"Error message: {e}")

    return pd.concat(df_list, ignore_index=True)

def download_data(url):
    zip_urls = find_files(url)
    df = fetch_files(zip_urls)
    return df

if __name__ == "__main__":
    df = download_data(2015, 2024)
    print(df.head(10))