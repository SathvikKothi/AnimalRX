from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
import time
import os


def initialize_driver():
    chromedriver_path = '/usr/local/bin/chromedriver'  # Replace with your ChromeDriver path
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    return webdriver.Chrome(service=Service(chromedriver_path), options=options)


def open_url(driver, url):
    driver.get(url)


def click_dropdown(driver):
    try:
        dropdown_toggle = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".ui-select-toggle")))
        driver.execute_script("arguments[0].scrollIntoView(true);", dropdown_toggle)
        dropdown_toggle.click()
    except ElementClickInterceptedException:
        # Using JavaScript to click if normal click is intercepted
        driver.execute_script("arguments[0].click();", dropdown_toggle)


def get_dropdown_options(driver):
    options_selector = ".ui-select-choices-row"
    WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, options_selector)))
    return driver.find_elements(By.CSS_SELECTOR, options_selector)


def save_html(driver, filename):
    page_content = driver.page_source
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(page_content)


def main():
    driver = initialize_driver()
    url = "https://animaldrugsatfda.fda.gov/adafda/views/#/foiDrugSummaries#foiApplicationInfo"
    open_url(driver, url)

    output_dir = '../Data/OtherDownloads/output_html'
    os.makedirs(output_dir, exist_ok=True)

    click_dropdown(driver)
    options = get_dropdown_options(driver)

    for index, option in enumerate(options):
        try:
            # Re-find the option within the loop to ensure validity
            option = driver.find_elements(By.CSS_SELECTOR, ".ui-select-choices-row")[index]
            driver.execute_script("arguments[0].scrollIntoView(true);", option)
            option.click()

            WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".foi")))

            save_html(driver, os.path.join(output_dir, f'page_{index + 1}.html'))

            # Reopen the dropdown for the next iteration
            click_dropdown(driver)

        except StaleElementReferenceException:
            print(f"Stale element encountered when clicking option {index + 1}")

    driver.quit()


if __name__ == "__main__":
    main()
