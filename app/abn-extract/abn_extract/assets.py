import requests
import os
import zipfile
from io import BytesIO
from bs4 import BeautifulSoup
from dagster import op
from typing import List

@op
def scrape_abn_website(url: str) -> List[str]:
    """
    Scrapes ABN website and finds links containing the text "public_split" these links can then be used to download the ABN bulk extract data.

    Args:
        url (str): The URL of the ABN website to scrape.

    Returns:
        A list of URLs 

    Raises:
        None
    """
    # Send a GET request to the website
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags that contain "public_split" in the href attribute
        links = soup.find_all('a', href=lambda href: href and 'public_split' in href)

        # Print the links found
        return [link['href'] for link in links]
    else:
        print(f"Failed to retrieve website content. Status code: {response.status_code}")

@op
def download_files(file_urls: List[str]) -> None:
    """
    Downloads a list of zipped files, extracts them in memory, and writes the contents to the data directory.

    Args:
        file_urls (List[str]): List of URLs of the zipped files to download.

    Returns:
        None

    Raises:
        None
    """
    data_dir = 'data'  # Relative data directory path

    # Create the data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Download each file and extract the contents in memory
    for url in file_urls:
        # Extract the filename from the URL
        filename = url.split('/')[-1]

        # Send a GET request to download the zipped file
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the contents of the zipped file in memory
            with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
                # Extract each file in the zip archive
                for file_info in zip_ref.infolist():
                    # Skip directories
                    if file_info.is_dir():
                        continue

                    # Extract the file contents
                    file_content = zip_ref.read(file_info)

                    # Determine the file path within the data directory
                    file_path = os.path.join(data_dir, file_info.filename)

                    # Write the extracted file to the data directory
                    with open(file_path, 'wb') as output_file:
                        output_file.write(file_content)

                    print(f"Extracted: {file_info.filename} from {filename}")
        else:
            print(f"Failed to download: {filename} (Status code: {response.status_code})")




if __name__ == '__main__':
    # URL of the website to scrape
    website_url = 'https://data.gov.au/data/dataset/abn-bulk-extract'

    # Call the scrape_website function with the URL
    download_files(scrape_abn_website(website_url))
