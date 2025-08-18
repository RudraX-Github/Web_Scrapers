from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time
import random
from collections import deque

def setup_driver():
    """Setup Selenium WebDriver with optimized Chrome settings for headless mode."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def human_like_delay(min=0.05, max=0.15):
    """Introduce minimal delay to maintain performance while simulating human behavior."""
    time.sleep(random.uniform(min, max))

def human_like_click(element, driver):
    """Simulate human-like mouse movement and click."""
    actions = ActionChains(driver)
    actions.move_to_element_with_offset(element, random.randint(2, 5), random.randint(2, 5))
    human_like_delay(0.2, 0.5)
    actions.pause(random.uniform(0.1, 0.3))
    actions.click_and_hold()
    actions.pause(random.uniform(0.05, 0.1))
    actions.release()
    actions.perform()

def select_parcel_option(driver):
    """Ensure Parcel is selected efficiently with human-like interaction."""
    try:
        parcel_radio = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='parcel'][name='PropertySearchType']"))
        )
        human_like_click(parcel_radio, driver)
        print("✓ Parcel option selected successfully")
    except Exception as e:
        print(f"Error selecting parcel option: {str(e)}")

def extract_property_data(driver):
    """Extract property details while preserving correct column order."""
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#gridResults tbody tr")))
        rows = driver.find_elements(By.CSS_SELECTOR, "#gridResults tbody tr")

        properties = deque()  # Using deque for optimized storage

        for row in rows:
            try:
                data = {
                    "Pin#": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_PIN']").text.strip(),
                    "Description": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_Description'] .pt-sr-name").text.strip(),
                    "Description1": "-",
                    "Account": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_Account']").text.strip(),
                    "Parcel": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_ParcelNumberFormatted']").text.strip(),
                    "Year": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_tyYEAR']").text.strip(),
                    "Billing Year": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_tyYEAR_BILLING']").text.strip(),
                    "Pin": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_PIN']").text.strip(),
                    "Total Tax": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_TotalTaxDisplay']").text.strip(),
                    "Balance Due": row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_BalanceDueDisplay']").text.strip()
                }

                try:
                    address_element = row.find_element(By.CSS_SELECTOR, "[aria-describedby='gridResults_Description'] .pt-sr-address")
                    data["Description1"] = address_element.text.strip() if address_element.text.strip() else "-"
                except Exception:
                    data["Description1"] = "-"

                properties.append(data)
            except Exception as e:
                print(f"Error extracting row: {str(e)}")

        return list(properties) if properties else None  # Convert deque to list for Pandas
    except Exception as e:
        print(f"Error extracting property data: {str(e)}")
        return None

def search_parcel(driver, parcel_value):
    """Optimized parcel search with efficient input handling."""
    try:
        parcel_input = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#pt-search-editor-1")))
        parcel_input.click()
        parcel_input.send_keys(Keys.CONTROL + "a")
        parcel_input.send_keys(Keys.BACKSPACE)
        human_like_delay()

        for char in parcel_value:
            parcel_input.send_keys(char)
            human_like_delay()

        search_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#pt-search-button")))
        human_like_click(search_button, driver)
        time.sleep(2)

        return extract_property_data(driver)
    except Exception as e:
        print(f"Error searching parcel {parcel_value}: {str(e)}")
        return None

def read_parcel_list(csv_path):
    """Optimized CSV reading using Pandas for faster processing."""
    print(f"\nReading parcel numbers from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=[1])
        parcel_numbers = df.iloc[:, 0].dropna().astype(str).str.strip().unique().tolist()
        print(f"✓ Loaded {len(parcel_numbers)} unique parcel numbers")
        return parcel_numbers
    except Exception as e:
        print(f"Error reading parcel list: {str(e)}")
        return None

def process_parcel(driver, parcel_value):
    """Handles searching a parcel in a separate browser window."""
    driver.get("https://marshall.countygovservices.com/Property/Property/Search")
    select_parcel_option(driver)

    try:
        print(f"Processing parcel {parcel_value}...")
        property_data = search_parcel(driver, parcel_value)

        return property_data if property_data else None
    except Exception as e:
        print(f"Error processing {parcel_value}: {str(e)}")
        return None

def parallel_processing(parcel_numbers, output_csv="optimized_results.csv"):
    """Manages parallel Selenium instances for parcel processing."""
    drivers = [setup_driver() for _ in range(6)]
    collected_data = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        for i in range(0, len(parcel_numbers), 6):  # Rotate through parcel batches
            batch = parcel_numbers[i:i+6]
            results = executor.map(process_parcel, drivers, batch)

            batch_data = []
            for data in results:
                if data:
                    batch_data.extend(data)

            collected_data.extend(batch_data)

            # Save batch data to CSV file
            if batch_data:
                df_batch = pd.DataFrame(batch_data)
                if i == 0:
                    df_batch.to_csv(output_csv, mode='w', header=True, index=False)
                else:
                    df_batch.to_csv(output_csv, mode='a', header=False, index=False)
            print(f"✅ Saved data after processing {i+12} parcels")

    for driver in drivers:
        driver.quit()

    print("\n✅ All parcels processed and saved successfully!")

if __name__ == "__main__":
    csv_path = "/D:/RNW/Project/Marshall/County_Scrapper/Parcel_List.csv"  # Use Unix-like path for Docker container
    parcel_numbers = read_parcel_list(csv_path)

    if parcel_numbers:
        parallel_processing(parcel_numbers)