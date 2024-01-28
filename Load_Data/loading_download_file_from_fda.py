import pandas as pd
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import requests
from PIL import Image
from io import BytesIO
import os
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urljoin  # Make sure to import urljoin correctly

# PostgreSQL connection parameters
host = 'localhost'
database = 'postgres'
user = 'clinicaltrials'
password = 'clinicaltrials'

# Create the engine to connect to the PostgreSQL database
engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')

def download_image(image_url, save_folder, image_name):
    response = requests.get(image_url)
    if response.status_code == 200:
        image = Image.open(BytesIO(response.content))
        image.save(os.path.join(save_folder, image_name))

def extract_data_from_page(url, driver, ndc, save_folder):
    driver.get(url)
    time.sleep(5)  # Wait for JavaScript to load
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    metadata = {tag['name']: tag['content'] for tag in soup.find_all('meta') if 'name' in tag.attrs}
    document_title = soup.find('div', class_='DocumentTitle').get_text(strip=True) if soup.find('div', class_='DocumentTitle') else "No Document Title"
    product_label = soup.find('h1').get_text(strip=True) if soup.find('h1') else "No Product Label"
    image_elements = soup.find_all('img')
    image_urls = []
    for idx, img in enumerate(image_elements):
        if 'src' in img.attrs:
            img_url = img['src']
            if not img_url.startswith('http'):
                img_url = urljoin(url, img_url)  # Correct usage of urljoin to handle relative URLs
            image_name = f'ndc_{ndc}_{idx}.jpg'
            download_image(img_url, save_folder, image_name)
            image_urls.append(os.path.join(save_folder, image_name))  # Store the local path
    # Extracting product information from tables
    product_info = {}
    for table in soup.find_all('table', class_='contentTablePetite'):
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if cells:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                product_info[label] = value

    # Extracting marketing information
    marketing_info = {}
    for table in soup.find_all('table', class_='formTableMorePetite'):
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if cells:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                marketing_info[label] = value

    # Extracting establishment information
    establishment_info = {}
    for table in soup.find_all('table', class_='formTableMorePetite'):
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if cells:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                establishment_info[label] = value

    # Extracting distributor name
    distributor_div = soup.find('div', class_='DistributorName')
    distributor_name = distributor_div.get_text(strip=True) if distributor_div else "No Distributor Name"

    return metadata, document_title, product_label, image_urls, product_info, marketing_info,establishment_info, distributor_name

# Define the table name
table_name = 'animal_drug_product_listing'

# PostgreSQL connection parameters
host = 'localhost'
database = 'postgres'
user = 'clinicaltrials'
password = 'clinicaltrials'

# Create the engine to connect to the PostgreSQL database
engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')

# Read the data from CSV file
data = pd.read_csv('../Data/OtherDownloads/Electronic_Animal_Drug_Product_Listing_Directory.csv', encoding='unicode_escape')
data.columns = [c.lower().replace(" ", "_") for c in data.columns]
df = pd.DataFrame(data)

table_name = 'animal_drug_product_listing'

# Drop the existing table if it exists
drop_table_query = text(f"DROP TABLE IF EXISTS {table_name};")

# Define the table creation query with all necessary columns
create_table_query = text(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    ndc TEXT,
    proprietary_name TEXT,
    non_proprietary_name TEXT,
    ingredient_list TEXT,
    labeler_name TEXT,
    product_type TEXT,
    marketing_category TEXT,
    application_number_or_monograph_id TEXT,
    link TEXT,
    inactivation_date DATE,
    reactivation_date DATE,
    metadata_info JSONB,
    document_title TEXT,
    product_label TEXT,
    image_url TEXT,
    product_info JSONB,
    marketing_info JSONB,
    establishment_info JSONB,
    distributor_name TEXT
)""")

# Open a persistent connection to the database
with engine.connect() as connection:
    # Drop the existing table and recreate it
    connection.execute(drop_table_query)
    connection.execute(create_table_query)

# Set up the WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

save_folder = '../Data/OtherDownloads/Label_image/'
if not os.path.exists(save_folder):
    os.makedirs(save_folder)


# Open a persistent connection to the database
with engine.connect() as connection:
    # Create the table
    connection.execute(create_table_query)
    # Iterate over the DataFrame and update each record with additional information
    for index, row in df.iterrows():
        metadata, document_title, product_label, image_url, product_info, marketing_info, establishment_info, distributor_name = extract_data_from_page(row['link'], driver, row['ndc'], save_folder)
        # Prepare data for insertion as a dictionary
        # Convert NaN to None for date fields
        inactivation_date = row['inactivation_date'] if pd.notna(row['inactivation_date']) else None
        reactivation_date = row['reactivation_date'] if pd.notna(row['reactivation_date']) else None

        insert_data = {
            'ndc': row['ndc'],
            'proprietary_name': row['proprietary_name'],
            'non_proprietary_name': row['non_proprietary_name'],
            'ingredient_list': row['ingredient_list'],
            'labeler_name': row['labeler_name'],
            'product_type': row['product_type'],
            'marketing_category': row['marketing_category'],
            'application_number_or_monograph_id': row['application_number_or_monograph_id'],
            'link': row['link'],
            'inactivation_date': inactivation_date,
            'reactivation_date': reactivation_date,
            'metadata_info': json.dumps(metadata),
            'document_title': document_title,
            'product_label': product_label,
            'image_url': image_url,
            'product_info': json.dumps(product_info),
            'marketing_info': json.dumps(marketing_info),
            'establishment_info': json.dumps(establishment_info),
            'distributor_name': distributor_name
        }

        # Insert data into the table
        insert_query = text(f"""
        INSERT INTO {table_name} (ndc, proprietary_name, non_proprietary_name, ingredient_list, labeler_name, product_type, 
        marketing_category, application_number_or_monograph_id, link, inactivation_date, reactivation_date, metadata_info, 
        document_title, product_label, image_url, product_info, marketing_info, establishment_info, distributor_name) 
        VALUES (:ndc, :proprietary_name, :non_proprietary_name, :ingredient_list, :labeler_name, :product_type, 
        :marketing_category, :application_number_or_monograph_id, :link, :inactivation_date, :reactivation_date, :metadata_info, 
        :document_title, :product_label, :image_url, :product_info, :marketing_info, :establishment_info, :distributor_name)
        """)
        connection.execute(insert_query, insert_data)

# Close the WebDriver
driver.quit()
