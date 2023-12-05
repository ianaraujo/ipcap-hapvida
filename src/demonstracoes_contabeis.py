import os
import requests
import tempfile
import zipfile
from bs4 import BeautifulSoup

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

url_list = [BASE_URL + str(year) for year in range(2007, 2024)]


def find_files(url_list):
    zip_urls = []

    for url in url_list:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.endswith(".zip"):
                zip_urls.append(os.path.join(url, href))
    return zip_urls


zip_urls = find_files(url_list)

with tempfile.TemporaryDirectory() as temp_dir:
    for url in zip_urls:
        # Download the .zip file
        response = requests.get(url)
        zip_file_path = os.path.join(temp_dir, os.path.basename(url))

        # Save the .zip file to the temporary directory
        with open(zip_file_path, 'wb') as f:
            f.write(response.content)

        # Extract the .zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

    # Read all CSV files into a single DataFrame
    all_csv_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.csv')]
    df = pd.concat((pd.read_csv(f) for f in all_csv_files), ignore_index=True)

print(df)
