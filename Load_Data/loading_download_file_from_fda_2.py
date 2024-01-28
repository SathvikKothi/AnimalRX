import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text

# Read the data from CSV file
data = pd.read_csv('../Data/OtherDownloads/Electronic_Animal_Drug_Product_Listing_Directory.csv', encoding='unicode_escape')
data.columns = [c.lower().replace(" ", "_") for c in data.columns]
df = pd.DataFrame(data)

# Add columns to store the extracted text and image URLs
df['extracted_text'] = None
df['image_urls'] = None

# PostgreSQL connection parameters
engine = create_engine('postgresql://clinicaltrials:clinicaltrials@localhost/postgres')

# Define the table name
table_name = 'animal_drug_product_listing'

# SQL query to create table if not exists with lowercase column names
# Adjust the table structure as needed, for instance, adding columns for text and images
create_table_query = text("""
CREATE TABLE IF NOT EXISTS animal_drug_product_listing (
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
    extracted_text TEXT,
    image_urls TEXT
)""")

# Execute the create table query
with engine.connect() as connection:
    connection.execute(create_table_query)

# Function to download and parse HTML content
def parse_html(url):
    response = requests.get(url)
    print(response.status_code)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)
        print(text_content)
        images = soup.find_all('img')
        image_urls = [img['src'] for img in images if 'src' in img.attrs]
        print(image_urls)
        return text_content, '; '.join(image_urls)
    else:
        return None, None

# Iterate over the DataFrame and fetch data
for index, row in df.iterrows():
    text_content, image_urls = parse_html(row['link'])
    df.at[index, 'extracted_text'] = text_content
    df.at[index, 'image_urls'] = image_urls

# Insert data into the table
df.to_sql(table_name, con=engine, if_exists='replace', index=False)
