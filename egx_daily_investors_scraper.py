import json
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import os
from datetime import datetime
import time as time_module
import egxtelesend as send
import asyncio
import logging
logging.basicConfig(level=logging.DEBUG)
from rich import print

def fetch_data():
    driver = uc.Chrome()
    driver.get('https://www.egx.com.eg/ar/InvestorsTypePieChart.aspx')
    wait = WebDriverWait(driver, 10, poll_frequency=0.1)

    data = {
        "fetch_time": datetime.now().strftime("%Y-%m-%d  %I:%M%p"),
        "Egyptians": {
            "Individuals & Institutions": {},
            "Individuals": {},
            "Institutions": {}
        },
        "Arabs": {
            "Individuals & Institutions": {},
            "Individuals": {},
            "Institutions": {}
        },
        "NonArabs": {
            "Individuals & Institutions": {},
            "Individuals": {},
            "Institutions": {}
        }
    }

    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody')))
        try:
            # EGYPTIANS DATA
            data["Egyptians"]["Individuals & Institutions"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr:nth-child(2) > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr:nth-child(2) > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr:nth-child(2) > td:nth-child(4)').text
            }
            data["Egyptians"]["Individuals"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr:nth-child(2) > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr:nth-child(2) > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr:nth-child(2) > td:nth-child(4)').text
            }
            data["Egyptians"]["Institutions"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality > tbody > tr:nth-child(2) > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality > tbody > tr:nth-child(2) > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality > tbody > tr:nth-child(2) > td:nth-child(4)').text
            }
        except Exception as e:
            print(f'Error happened while fetching EGYPTIANS data: {e}')
        try:
            # ARABS DATA
            data["Arabs"]["Individuals & Institutions"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr.AlternatingRowStyle > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr.AlternatingRowStyle > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr.AlternatingRowStyle > td:nth-child(4)').text
            }
            data["Arabs"]["Individuals"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr.AlternatingRowStyle > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr.AlternatingRowStyle > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr.AlternatingRowStyle > td:nth-child(4)').text
            }
            data["Arabs"]["Institutions"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality_ctl03_lblSell1').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality_ctl03_lblBuy1').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality_ctl03_lblNet1').text
            }
        except Exception as e:
            print(f'Error happened while fetching ARABS data: {e}')
        try:
            # NON-ARABS DATA
            data["NonArabs"]["Individuals & Institutions"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr:nth-child(4) > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr:nth-child(4) > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_GridView1 > tbody > tr:nth-child(4) > td:nth-child(4)').text
            }
            data["NonArabs"]["Individuals"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr:nth-child(4) > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr:nth-child(4) > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvIndByNationality > tbody > tr:nth-child(4) > td:nth-child(4)').text
            }
            data["NonArabs"]["Institutions"] = {
                "Sell value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality > tbody > tr:nth-child(4) > td:nth-child(2)').text,
                "Buy value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality > tbody > tr:nth-child(4) > td:nth-child(3)').text,
                "Net value": driver.find_element(By.CSS_SELECTOR, '#ctl00_C_ITPc_gvInstByNationality > tbody > tr:nth-child(4) > td:nth-child(4)').text
            }
        except Exception as e:
            print(f'Error happened while fetching NON-ARABS data: {e}')
    finally:
        driver.close()
        driver.quit()
        #kill_high_cpu_chrome()
    # Capture the current date and time
    
    return data
'''
def kill_high_cpu_chrome():
    threshold = 1
    chrome_processes = [proc for proc in psutil.process_iter(['pid', 'name']) if 'chrome' in proc.info['name'].lower()]

    for proc in chrome_processes:
        try:
            cpu_usage = proc.cpu_percent(interval=1)
            if cpu_usage >= threshold:
                print(f"Killing Chrome process {proc.info['name']} with PID {proc.info['pid']} using {cpu_usage}% CPU")
                proc.kill()
        except Exception as e:
            print(f"Error killing process: {e}")   
'''

def append_data_to_json(file_path, new_data):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            existing_data = json.load(file)
    else:
        existing_data = []

    if not isinstance(existing_data, list):
        existing_data = [existing_data]

    existing_data.append(new_data)

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(existing_data, file, indent=4)


script_dir = os.path.dirname(os.path.abspath(__file__))

def run_at_specific_time():
    # Fetch the data
    data = fetch_data()
    # Append the data to the JSON file
    json_file_path = os.path.join(script_dir, 'daily_egx_investors.json') # type: ignore
    append_data_to_json(json_file_path, data)
    print(f'Data appended to {json_file_path}')
#run_at_specific_time()   
if __name__ == "__main__":
    run_at_specific_time()
    time_module.sleep(1)
    asyncio.run(send.main())
