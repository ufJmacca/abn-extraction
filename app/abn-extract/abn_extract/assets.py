import requests
import os
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from bs4 import BeautifulSoup
from dagster import op, job
from typing import List, Dict
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert, delete
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors

# Import postgres_table_creation to leverage classes
import importlib.util
module_path = '/app/postgres_table_creation.py'
module_name = 'postgres_table_creation'
spec = importlib.util.spec_from_file_location(module_name, module_path)
postgres_table_creation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(postgres_table_creation)

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
def download_files(file_urls: List[str]) -> str:
    """
    Downloads a list of zipped files, extracts them in memory, and writes the contents to the data directory.

    Args:
        file_urls (List[str]): List of URLs of the zipped files to download.

    Returns:
        string of data directory

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

    return './data'

@op
def delete_records() -> None:
    """
    Deletes records from tables

    Args:
        None

    Returns:
        None

    """
    engine = create_engine('postgresql://postgres:example_password@abn-db-1/abn')

    with Session(engine) as session:
        session.execute(delete(postgres_table_creation.MainEntity))
        session.execute(delete(postgres_table_creation.LegalEntity))
        session.execute(delete(postgres_table_creation.ASICNumber))
        session.execute(delete(postgres_table_creation.GST))
        session.execute(delete(postgres_table_creation.DGR))
        session.execute(delete(postgres_table_creation.OtherEntity))
        session.execute(delete(postgres_table_creation.ABN))
        session.commit()

def element_handle(input: str) -> str:
    """
    Handles an input element, returning its text or an empty string if an exception occurs.

    Args:
        input: The input element to handle.

    Returns:
        str: The text value of the input element or an empty string.

    """
    try:
        return input.text
    except:
        return ''
    
def bulk_insert(
    abn: List[dict],
    main_entity: List[dict],
    legal_entity: List[dict],
    asic_number: List[dict],
    gst: List[dict],
    dgr: List[dict],
    other_entity: List[dict]
) -> None:
    """
    Performs a bulk insert of data into the PostgreSQL database.

    Args:
        abn (List[dict]): List of dictionaries representing ABN records to insert.
        main_entity (List[dict]): List of dictionaries representing MainEntity records to insert.
        legal_entity (List[dict]): List of dictionaries representing LegalEntity records to insert.
        asic_number (List[dict]): List of dictionaries representing ASICNumber records to insert.
        gst (List[dict]): List of dictionaries representing GST records to insert.
        dgr (List[dict]): List of dictionaries representing DGR records to insert.
        other_entity (List[dict]): List of dictionaries representing OtherEntity records to insert.

    Returns:
        None

    """
    engine = create_engine('postgresql://postgres:example_password@abn-db-1/abn')

    with Session(engine) as session:
        session.execute(insert(postgres_table_creation.ABN), abn)
        session.execute(insert(postgres_table_creation.MainEntity), main_entity)
        session.execute(insert(postgres_table_creation.LegalEntity), legal_entity)
        session.execute(insert(postgres_table_creation.ASICNumber), asic_number)
        session.execute(insert(postgres_table_creation.GST), gst)
        session.execute(insert(postgres_table_creation.DGR), dgr)
        session.execute(insert(postgres_table_creation.OtherEntity), other_entity)
        session.commit()

def generate_bulk_insert(xml: str) -> dict:
    """
    Generate a dictionary representation of provided XML record so it can be bulk insered

    Args:
        xml (str): The ABN XML data to process and add to the bulk insert.

    Returns:
        Dictionary of ABN record for bulk insert

    Raises:
        None
    """
    soup = BeautifulSoup(xml, 'xml')

    return_dict = {}

    abn = postgres_table_creation.ABN(
        abn = int(soup.ABR.ABN.text),
        abn_status = soup.ABR.ABN['status'],
        abn_status_from_date = datetime.strptime(soup.ABR.ABN['ABNStatusFromDate'], '%Y%m%d').date(),
        entity_type_indicator = soup.ABR.EntityType.EntityTypeInd.text,
        entity_type_text = soup.ABR.EntityType.EntityTypeText.text
    )

    return_dict['abn'] = [abn]

    if soup.ABR.MainEntity:
        try:
            main_entity = postgres_table_creation.MainEntity(
                abn_id = int(soup.ABR.ABN.text),
                main_entity_type = soup.ABR.MainEntity.NonIndividualName['type'],
                main_entity_name = soup.ABR.MainEntity.NonIndividualName.text,
                address_state = soup.ABR.MainEntity.BusinessAddress.AddressDetails.State.text,
                address_postcode = int(soup.ABR.MainEntity.BusinessAddress.AddressDetails.Postcode.text or '0')
            )
        except ValueError:
            main_entity = postgres_table_creation.MainEntity(
                abn_id = int(soup.ABR.ABN.text),
                main_entity_type = soup.ABR.MainEntity.NonIndividualName['type'],
                main_entity_name = soup.ABR.MainEntity.NonIndividualName.text,
                address_state = soup.ABR.MainEntity.BusinessAddress.AddressDetails.State.text,
                address_postcode = int('0')
            )
        return_dict['main_entity'] = [main_entity]

    if soup.ABR.LegalEntity:
        try:
            legal_entity = postgres_table_creation.LegalEntity(
                abn_id = int(soup.ABR.ABN.text),
                legal_entity_type = soup.ABR.LegalEntity.IndividualName['type'],
                legal_entity_name = element_handle(soup.ABR.LegalEntity.IndividualName.GivenName) + ' ' + soup.ABR.LegalEntity.IndividualName.FamilyName.text,
                address_state = soup.ABR.LegalEntity.BusinessAddress.AddressDetails.State.text,
                address_postcode = int(soup.ABR.LegalEntity.BusinessAddress.AddressDetails.Postcode.text or '0')
            )
        except ValueError:
            legal_entity = postgres_table_creation.LegalEntity(
                abn_id = int(soup.ABR.ABN.text),
                legal_entity_type = soup.ABR.LegalEntity.IndividualName['type'],
                legal_entity_name = element_handle(soup.ABR.LegalEntity.IndividualName.GivenName) + ' ' + soup.ABR.LegalEntity.IndividualName.FamilyName.text,
                address_state = soup.ABR.LegalEntity.BusinessAddress.AddressDetails.State.text,
                address_postcode = int('0')
            )
        return_dict['legal_entity'] = [legal_entity]

    if soup.ABR.ASICNumber:
        asic_number = postgres_table_creation.ASICNumber(
            abn_id = int(soup.ABR.ABN.text),
            asic_number = soup.ABR.ASICNumber.text,
            asic_type = soup.ABR.ASICNumber['ASICNumberType']
        )
        return_dict['asic_number'] = [asic_number]

    if soup.ABR.GST:
        gst = postgres_table_creation.GST(
            abn_id = int(soup.ABR.ABN.text),
            status = soup.ABR.GST['status'],
            status_from_date = datetime.strptime(soup.ABR.GST['GSTStatusFromDate'], '%Y%m%d').date()
        )
        return_dict['gst'] = [gst]

    if soup.ABR.DGR:
        return_dict['dgr'] = []
        for dgr_entry in soup.find_all('DGR'):
            return_dict['dgr'].append(postgres_table_creation.DGR(
                    abn_id = int(soup.ABR.ABN.text),
                    status_from_date = datetime.strptime(dgr_entry['DGRStatusFromDate'], '%Y%m%d').date(),
                    name = dgr_entry.NonIndividualName.NonIndividualNameText.text
                )
            )

    if soup.ABR.OtherEntity:
        return_dict['other_entity'] = []
        for other_entity_entry in soup.find_all('OtherEntity'):
            return_dict['other_entity'].append(postgres_table_creation.OtherEntity(
                abn_id = int(soup.ABR.ABN.text),
                other_entity_type = other_entity_entry.NonIndividualName['type'],
                other_entity_name = other_entity_entry.NonIndividualName.NonIndividualNameText.text
            ))

    return  return_dict

def write_to_error_file(text1: str, text2: str) -> None:
    """
    Writes the given text1 and text2 to two new lines in the error.txt file.

    Args:
        text1 (str): The first text to write.
        text2 (str): The second text to write.

    Returns:
        None

    Raises:
        None
    """
    with open('error.txt', 'a') as file:
        file.write(text1 + '\n')
        file.write(text2 + '\n')

def remove_duplicates(dictionaries: List[Dict]) -> List[Dict]:
    """
    Removes duplicates from a list of dictionaries based on the values.

    Args:
        dictionaries (List[Dict]): A list of dictionaries.

    Returns:
        List[Dict]: A new list of dictionaries with duplicates removed.

    """
    unique_dictionaries = set(tuple(sorted(d.items())) for d in dictionaries if any(d.values()))
    return [dict(items) for items in unique_dictionaries]

@op
def process_files(directory: str) -> None:
    """
    Loops through all files in a directory and sends each line with an ABR XML element to the sample_function.

    Args:
        directory (str): The directory path to process.

    Returns:
        None

    Raises:
        None
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        print(file_path)

        # Skip directories
        if os.path.isdir(file_path):
            continue

        with open(file_path, 'r') as file:
            records = {
                    'abn': [],
                    'main_entity': [],
                    'legal_entity': [],
                    'asic_number': [],
                    'gst': [],
                    'dgr': [],
                    'other_entity': []
                }

            for line in file:
                # Parse each line as XML
                try:
                    root = BeautifulSoup(line.strip(), 'xml')
                except ET.ParseError as e:
                    print(f'parse error - {e}')
                    continue

                # Check if the root element has an ABR element
                if root.find('ABR') is None:
                    continue
                
                # Iterate over the generated key-value pairs for bulk insert.
                for key, value in generate_bulk_insert(line.strip()).items():
                    # Check if the key exists in the records dictionary.
                    if key in records.keys():
                        # Iterate over the values and convert them into dictionaries, excluding private attributes.
                        for val in value:
                            records[key].append({k1: v1 for k1, v1 in val.__dict__.items() if not k1.startswith('_')})

            bulk_insert(
                abn = records['abn'],
                main_entity = records['main_entity'],
                legal_entity = records['legal_entity'],
                asic_number = records['asic_number'],
                gst = records['gst'],
                dgr = remove_duplicates(records['dgr']),
                other_entity = remove_duplicates(records['other_entity'])
            )

        os.remove(file_path)

@op
def return_url() -> str:
    """
    returns abn-bulk-extract url to parse and download file from

    Args:
        None

    Returns:
        url (str): URL to download files from

    Raises:
        None
    """
    return 'https://data.gov.au/data/dataset/abn-bulk-extract'

@job
def extract_and_load() -> None:
    delete_records()

    process_files(directory= download_files(file_urls = scrape_abn_website(url = return_url())))


if __name__ == '__main__':
    extract_and_load() 