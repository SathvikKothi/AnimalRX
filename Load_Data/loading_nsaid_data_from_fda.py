from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import psycopg2
import requests
import os
import time

# Function to download the web page
def download_page(url, output_file):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)
    time.sleep(10)  # Wait for dynamic content to load
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(driver.page_source)
    driver.quit()

# Function to parse HTML and extract data
def parse_html_to_table(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    records = []
    for row in soup.find_all("div", class_="row ng-scope"):
        a_tag = row.find("a")
        if not a_tag:
            continue
        application = a_tag.text.strip()
        pdf_link = a_tag['href'].strip()
        divs = row.find_all("div", class_="col-sm-3 ng-binding")
        if len(divs) >= 2:
            proprietary_name = divs[0].text.strip()
            labeling_component = divs[1].text.strip()
            records.append({
                'Application': application,
                'PDF_link': pdf_link,
                'Proprietary_Name': proprietary_name,
                'Labeling_Component': labeling_component
            })
    return records

# Function to download PDFs
def download_pdfs(records, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    for record in records:
        response = requests.get(record['PDF_link'])
        print(response.status_code)
        if response.status_code == 200:
            pdf_filename = record['Application'].replace('/', '_') + '.pdf'
            pdf_path = os.path.join(download_dir, pdf_filename)
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            record['PDF_Path'] = pdf_path
"""
def download_pdfs(records, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    for record in records:
        pdf_filename = record['Application'].replace('/', '_') + '.pdf'
        pdf_path = os.path.join(download_dir, pdf_filename)

        # Check if file already exists
        if not os.path.isfile(pdf_path):
            response = requests.get(record['PDF_link'])
            if response.status_code == 200:
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)
        else:
            print(f"File already exists: {pdf_path}")

        record['PDF_Path'] = pdf_path

"""# Function to create the PostgreSQL table
def create_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=clinicaltrials password=clinicaltrials")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS nsaid_data (
        Application VARCHAR(255),
        PDF_link VARCHAR(255),
        Proprietary_Name VARCHAR(255),
        Labeling_Component VARCHAR(255),
        PDF_Path VARCHAR(255)
    )
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_into_db(records):
    conn = psycopg2.connect("host=localhost dbname=postgres user=clinicaltrials password=clinicaltrials")
    cur = conn.cursor()

    for record in records:
        cur.execute("""
        INSERT INTO nsaid_data (Application, PDF_link, Proprietary_Name, Labeling_Component, PDF_Path)
        VALUES (%s, %s, %s, %s, %s)
        """, (record['Application'], record['PDF_link'], record['Proprietary_Name'], record['Labeling_Component'], record['PDF_Path']))

    conn.commit()
    cur.close()
    conn.close()

url = 'https://animaldrugsatfda.fda.gov/adafda/views/#/nsaidLabels'
output_file = '../Data/OtherDownloads/NSAID_files/downloaded_page.html'
download_dir = '../Data/OtherDownloads/NSAID_files'
#download_page(url, output_file)
with open(output_file, 'r', encoding='utf-8') as file:
    html_content = file.read()
records = parse_html_to_table(html_content)
download_pdfs(records, download_dir)
create_table()
insert_into_db(records)
