import re
import psycopg2

conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
conn = psycopg2.connect(conn_string)
def parse_records(text):
    # Define a regular expression pattern to extract the relevant fields
    pattern = r"EA FONSI Proprietary Name:(.*?)Application:(.*?)" \
              r"Approval Type:(.*?)Date of Approval:(.*?)" \
              r"(?:Indication for Use|Effect of Supplement):(.*?)?(?=\nEA FONSI|\nBack to top|$)"

    # Parse records using regular expression
    matches = re.finditer(pattern, text, re.DOTALL)
    parsed_records = []
    for match in matches:
        # Extract data and set to None if empty
        proprietary_name = match.group(1).strip() or None

        application = match.group(2).strip() or None
        approval_type = match.group(3).strip() or None
        date_of_approval = match.group(4).strip() or None
        indication_or_effect = match.group(5).strip() if match.group(5) else None

        parsed_records.append({
            "Process_type": "EA FONSI",
            "Proprietary_Name": proprietary_name,
            "Application": application,
            "Approval_type": approval_type,
            "Date_of_Approval": date_of_approval,
            "Effect_or_Indication": indication_or_effect
        })

    return parsed_records


# Function to insert records into the PostgreSQL table
def create_table_if_not_exists(connection, drop):
    cursor = connection.cursor()
    if drop=='Y':
        drop_table= """ drop table if exists drug_brand_approval_info"""
        cursor.execute(drop_table)

    create_table = """
    CREATE TABLE if not exists drug_brand_approval_info (
    Process_type VARCHAR(100),
    Proprietary_Name VARCHAR(255),
    Application VARCHAR(100),
    Approval_type VARCHAR(255),
    Date_of_Approval DATE,
    Effect_or_Indication TEXT,
    EA_Document_link text,
    FONSI_Document_link text)
    """
    cursor.execute(create_table)
    cursor.close()

def download_data():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import requests

    # Setup headless browser
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(options=options)

    # URL to be downloaded
    url = 'https://animaldrugsatfda.fda.gov/adafda/views/#/environmentalAssessments'
    driver.get(url)

    # Wait for JavaScript to execute
    wait = WebDriverWait(driver, 20)
    wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')

    html_content = driver.page_source

    with open('../Data/OtherDownloads/output_html/EA_FONSI.html', 'w', encoding='utf-8') as file:
        file.write(html_content)

def parse_html(html):
    from bs4 import BeautifulSoup
    import re
    soup = BeautifulSoup(html, 'html.parser')
    links = {}

    for tag in soup.find_all('a', title=re.compile(r'(EA Document|FONSI Document)')):
        title = tag.get('title')
        href = tag.get('href')
        Application = re.search(r'NADA \d{3}-\d{3}', title).group()

        if 'EA Document' in title:
            link_type = 'EA_Document_link'
        else:
            link_type = 'FONSI_Document_link'

        if Application not in links:
            links[Application] = {}
        links[Application][link_type] = href
    return links

def insert_into_db(records, links, connection):
    insert_query = """
    INSERT INTO drug_brand_approval_info 
    (Process_type, Proprietary_Name, Application, Approval_type, Date_of_Approval, Effect_or_Indication, EA_Document_link, FONSI_Document_link)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor = connection.cursor()

    for record in records:
        ea_link = links.get(record['Application'], {}).get('EA_Document_link', None)
        fonsi_link = links.get(record['Application'], {}).get('FONSI_Document_link', None)

        values = (
            record['Process_type'],
            record['Proprietary_Name'],
            record['Application'],
            record['Approval_type'],
            record['Date_of_Approval'],
            record['Effect_or_Indication'],
            ea_link,
            fonsi_link
        )
        cursor.execute(insert_query, values)

    connection.commit()
    cursor.close()


#download_data()
# Read sample text from file

file = open('../Data/OtherDownloads/Manual_download_Drug_info2.txt', 'r')
sample_text = file.read()
file.close()
parsed_records = parse_records(sample_text)


file = open('../Data/OtherDownloads/output_html/EA_FONSI.html', 'r')
html_data = file.read()
file.close()
parsed_links = parse_html(html_data)

try:
    # Insert records into the database
    create_table_if_not_exists(conn, drop='Y')
    print("Parsed Records:", parsed_records)
    print("Parsed Links:", parsed_links)
    insert_into_db(parsed_records, parsed_links, conn)
except Exception as e:
    print("An error occurred:", e)
finally:
    # Close the database connection
    conn.close()

