
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

# Function to parse the HTML and extract required information
def parse_html(html):
    from bs4 import BeautifulSoup
    import re
    soup = BeautifulSoup(html, 'html.parser')
    data = []

    # Find all 'a' tags with 'title' attribute containing 'EA Document' or 'FONSI Document'
    for tag in soup.find_all('a', title=re.compile(r'(EA Document|FONSI Document)')):
        title = tag.get('title')
        href = tag.get('href')
        application_number = re.search(r'NADA \d{3}-\d{3}', title).group()
        data.append({
            'type': 'EA' if 'EA Document' in title else 'FONSI',
            'application_number': application_number,
            'link': href
        })

    return data

download_data()
# Read sample text from file
file = open('../Data/OtherDownloads/output_html/EA_FONSI.html', 'r')
html_data = file.read()
file.close()
# Parse the HTML data
parsed_data = parse_html(html_data)
print(parsed_data)


