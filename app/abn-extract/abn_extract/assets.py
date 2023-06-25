import requests
import os
import zipfile
from io import BytesIO
from bs4 import BeautifulSoup
from dagster import op
from typing import List
from datetime import datetime

# Import postgres_table_creation to leverage classes
import importlib.util
module_path = '/app/postgres_table_creation.py'
module_name = 'postgres_table_creation'
spec = importlib.util.spec_from_file_location(module_name, module_path)
postgres_table_creation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(postgres_table_creation)
postgres_table_creation.ABN()

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

def add_record_to_db(xml: str) -> None:
    soup = BeautifulSoup(xml, 'xml')
    print(soup.find_all('DGR'))

    abn = postgres_table_creation.ABN(
        abn = int(soup.ABR.ABN.text),
        abn_status = soup.ABR.ABN['status'],
        abn_status_from_date = datetime.strptime(soup.ABR.ABN['ABNStatusFromDate'], '%Y%m%d').date(),
        entity_type_indicator = soup.ABR.EntityType.EntityTypeInd.text,
        entity_type_text = soup.ABR.EntityType.EntityTypeText.text
    )

    if soup.ABR.MainEntity:
        main_enity = postgres_table_creation.MainEntity(
            abn_id = int(soup.ABR.ABN.text),
            main_entity_type = soup.ABR.MainEntity.NonIndividualName['type'],
            main_entity_name = soup.ABR.MainEntity.NonIndividualName.text,
            address_state = soup.ABR.MainEntity.BusinessAddress.AddressDetails.State.text,
            address_postcode = int(soup.ABR.MainEntity.BusinessAddress.AddressDetails.Postcode.text)
        )

    if soup.ABR.LegalEntity:
        legal_entity = postgres_table_creation.LegalEntity(
            abn_id = int(soup.ABR.ABN.text),
            legal_entity_type = soup.ABR.LegalEntity.IndividualName['type'],
            legal_entity_name = soup.ABR.LegalEntity.IndividualName.GivenName.text + ' ' + soup.ABR.LegalEntity.IndividualName.FamilyName.text,
            address_state = soup.ABR.LegalEntity.BusinessAddress.AddressDetails.State.text,
            address_postcode = int(soup.ABR.LegalEntity.BusinessAddress.AddressDetails.Postcode.text)
        )

    if soup.ABR.ASICNumber:
        asic_number = postgres_table_creation.ASICNumber(
            abn_id = int(soup.ABR.ABN.text),
            asic_number = soup.ABR.ASICNumber.text,
            asic_type = soup.ABR.ASICNumber['ASICNumberType']
        )

    if soup.ABR.GST:
        gst = postgres_table_creation.GST(
            abn_id = int(soup.ABR.ABN.text),
            status = soup.ABR.GST['status'],
            status_from_date = datetime.strptime(soup.ABR.GST['GSTStatusFromDate'], '%Y%m%d').date()
        )

    if soup.ABR.DGR:
        dgr = postgres_table_creation.DGR(
            abn_id = int(soup.ABR.ABN.text),
        )




if __name__ == '__main__':
    # URL of the website to scrape
    website_url = 'https://data.gov.au/data/dataset/abn-bulk-extract'

    # Call the scrape_website function with the URL
    # download_files(scrape_abn_website(website_url))

    sample_xml = [
        r'''<ABR recordLastUpdatedDate="20150318" replaced="N"><ABN status="CAN" ABNStatusFromDate="20150202">11001825150</ABN><EntityType><EntityTypeInd>IND</EntityTypeInd><EntityTypeText>Individual/Sole Trader</EntityTypeText></EntityType><LegalEntity><IndividualName type="LGL"><GivenName>JOE</GivenName><FamilyName>VELARDI</FamilyName></IndividualName><BusinessAddress><AddressDetails><State>VIC</State><Postcode>3043</Postcode></AddressDetails></BusinessAddress></LegalEntity><GST status="NON" GSTStatusFromDate="19000101" /><OtherEntity><NonIndividualName type="TRD"><NonIndividualNameText>JVELARDI</NonIndividualNameText></NonIndividualName></OtherEntity></ABR>'''
        , r'''<ABR recordLastUpdatedDate="20160726" replaced="N"><ABN status="ACT" ABNStatusFromDate="20000221">11001832391</ABN><EntityType><EntityTypeInd>PRV</EntityTypeInd><EntityTypeText>Australian Private Company</EntityTypeText></EntityType><MainEntity><NonIndividualName type="MN"><NonIndividualNameText>BERRY'S BAY FOODS PTY. LTD.</NonIndividualNameText></NonIndividualName><BusinessAddress><AddressDetails><State>NSW</State><Postcode>2000</Postcode></AddressDetails></BusinessAddress></MainEntity><ASICNumber ASICNumberType="undetermined">001832391</ASICNumber><GST status="CAN" GSTStatusFromDate="20160701" /><OtherEntity><NonIndividualName type="TRD"><NonIndividualNameText>BERRY'S BAY FOODS PTY LTD</NonIndividualNameText></NonIndividualName></OtherEntity></ABR>'''
        , r'''<ABR recordLastUpdatedDate="20230504" replaced="N"><ABN status="ACT" ABNStatusFromDate="20000422">11001832828</ABN><EntityType><EntityTypeInd>PUB</EntityTypeInd><EntityTypeText>Australian Public Company</EntityTypeText></EntityType><MainEntity><NonIndividualName type="MN"><NonIndividualNameText>PACIFIC GROUP OF CHRISTIAN SCHOOLS LIMITED</NonIndividualNameText></NonIndividualName><BusinessAddress><AddressDetails><State>NSW</State><Postcode>2158</Postcode></AddressDetails></BusinessAddress></MainEntity><ASICNumber ASICNumberType="undetermined">001832828</ASICNumber><GST status="ACT" GSTStatusFromDate="20000701" /><DGR DGRStatusFromDate="20000701"><NonIndividualName type="DGR"><NonIndividualNameText>PACIFIC HILLS CHRISTIAN EDUCATION LTD BUILDING FUND</NonIndividualNameText></NonIndividualName></DGR><DGR DGRStatusFromDate="20000701"><NonIndividualName type="DGR"><NonIndividualNameText>PACIFIC HILLS CHRISTIAN EDUCATION LTD LIBRARY FUND</NonIndividualNameText></NonIndividualName></DGR><DGR DGRStatusFromDate="20070531"><NonIndividualName type="DGR"><NonIndividualNameText>PACIFIC HILLS SCHOLARSHIP FUND</NonIndividualNameText></NonIndividualName></DGR><OtherEntity><NonIndividualName type="TRD"><NonIndividualNameText>PACIFIC HILLS CHRISTIAN SCHOOL</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>GLOBAL CHRISTIAN SCHOOLS NETWORK</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>HANNAHS SCHOOL SUPPLIES</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>New Hope Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>OneMaker Academy</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Berowra Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Brook Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Coast Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Group of Christian Schools</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Gulgangali Jarjums Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>PACIFIC HILLS</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>PACIFIC HILLS CHRISTIAN SCHOOL</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Hope Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Online</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Online Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Valley Christian School</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacific Vision Foundation</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacifica Cafe</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Pacifica The Missions Cafe</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Southland College</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>THE CHRISTIAN EDUCATORS FORUM</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>THE EDUCATORS' FORUM</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>THE EXCELLENCE CENTRE</NonIndividualNameText></NonIndividualName></OtherEntity><OtherEntity><NonIndividualName type="BN"><NonIndividualNameText>Valley Hope Christian School</NonIndividualNameText></NonIndividualName></OtherEntity></ABR>'''
    ]

    add_record_to_db(sample_xml[2])

