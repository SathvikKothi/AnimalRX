import time
from PIL import Image
import requests
from io import BytesIO
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

def download_image(image_url, save_folder, image_name):
    response = requests.get(image_url)
    if response.status_code == 200:
        image = Image.open(BytesIO(response.content))
        image.save(os.path.join(save_folder, image_name))


# Set up the WebDriver (example with Chrome)
url = 'https://www.accessdata.fda.gov/spl/data/83f252bd-8ef6-41f7-9c84-c1214a33117f/83f252bd-8ef6-41f7-9c84-c1214a33117f.xml'
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get(url)
time.sleep(5)
# Navigate to the URL
driver.get(url)

# Wait for JavaScript to load (if necessary)
driver.implicitly_wait(10)  # Adjust the wait time as needed

# Get the HTML content after JavaScript execution
html = driver.page_source

images = driver.find_elements(By.TAG_NAME, 'img')

# Folder to save images
save_folder = '../Data/OtherDownloads/Label_image/'
if not os.path.exists(save_folder):
    os.makedirs(save_folder)

# Download images
for idx, image in enumerate(images):
    src = image.get_attribute('src')
    if src:
        download_image(src, save_folder, f'image_{idx}.png')

soup = BeautifulSoup(html, 'html.parser')
# Extracting metadata information
metadata = {tag['name']: tag['content'] for tag in soup.find_all('meta') if 'name' in tag.attrs}

# Extracting document title
document_title_div = soup.find('div', class_='DocumentTitle')
document_title = document_title_div.get_text(strip=True) if document_title_div else "No Document Title"

# Extracting product label
product_label_h1 = soup.find('h1')
product_label = product_label_h1.get_text(strip=True) if product_label_h1 else "No Product Label"

# Extracting image URL
image = soup.find('img')
image_url = image['src'] if image and 'src' in image.attrs else "No Image URL"

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

import psycopg2
import json

# Connection parameters - replace with your details
host = 'localhost'
database = 'postgres'
user = 'clinicaltrials'
password = 'clinicaltrials'

# Connect to your PostgreSQL database
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
cursor = conn.cursor()

# SQL query to create a table if it doesn't exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS product_data (
    id SERIAL PRIMARY KEY,
    metadata JSONB,
    document_title TEXT,
    product_label TEXT,
    image_url TEXT,
    product_info JSONB,
    marketing_info JSONB,
    establishment_info JSONB,
    distributor_name TEXT
)
'''

# Execute the create table query
cursor.execute(create_table_query)

# SQL query to insert data
insert_query = '''
INSERT INTO product_data (metadata, document_title, product_label, image_url, product_info, marketing_info, establishment_info, distributor_name) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

# Prepare data for insertion
data = (
    json.dumps(metadata),
    document_title,
    product_label,
    image_url,
    json.dumps(product_info),
    json.dumps(marketing_info),
    json.dumps(establishment_info),
    distributor_name
)

# Execute the insert query
cursor.execute(insert_query, data)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
# Output results
print("Metadata:", metadata)
print("Document Title:", document_title)
print("Product Label:", product_label)
print("Image URL:", image_url)
print("Product Info:", product_info)
print("Marketing Info:", marketing_info)
print("Establishment Info:", establishment_info)
print("Distributor Name:", distributor_name)
driver.quit()
